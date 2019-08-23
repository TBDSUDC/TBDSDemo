package com.tencent.tbds.tdbank.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import com.google.common.base.Splitter;
import com.tencent.tbds.tdbank.utils.Constants;
import com.tencent.tdbank.msg.TDMsg1;
import com.tencent.tube.Message;
import com.tencent.tube.client.MessageSessionFactory;
import com.tencent.tube.client.TubeClientConfig;
import com.tencent.tube.client.TubeMessageSessionFactory;
import com.tencent.tube.client.consumer.ConsumerConfig;
import com.tencent.tube.client.consumer.pullconsumer.ConsumerResult;
import com.tencent.tube.client.consumer.pullconsumer.PullMessageConsumer;
import com.tencent.tube.cluster.MasterInfo;
import com.tencent.tube.exception.TubeClientException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TubeSource extends RichParallelSourceFunction<String> {
  private static Logger LOG = Logger.getLogger(TubeSource.class);

  private transient Map<String, String> globalMap = null;
  private transient String haHosts;
  private transient String consumerGroup;
  private transient String username;
  private transient String secureId;
  private transient String secureKey;
  private transient String topic;
  private transient boolean isTdMsg;

  private transient PullMessageConsumer messagePullConsumer;
  private MessageSessionFactory messageSessionFactory;

  private volatile boolean running = true;

  long start = System.currentTimeMillis();

  @Override public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    globalMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
    haHosts = globalMap.get("tube.ha.hosts");
    consumerGroup = globalMap.get("tube.consumer.group");
    username = globalMap.get("tube.consumer.username");
    secureId = globalMap.get("tube.consumer.secureId");
    secureKey = globalMap.get("tube.consumer.secureKey");
    topic = globalMap.get("tube.consumer.bid");
    isTdMsg = Boolean.parseBoolean(globalMap.getOrDefault("tube.consumer.isTdMsg", "false"));
    LOG.info("parameter " + haHosts + "," + consumerGroup + "," + username + "," + secureId + "," + secureKey + "," + topic);

    TubeClientConfig tubeConfig = new TubeClientConfig();
    tubeConfig.setMasterInfo(new MasterInfo(haHosts));
    ConsumerConfig consumerConfig = new ConsumerConfig(consumerGroup);
    consumerConfig.setConsumeFromMaxOffset(false);
    consumerConfig.setCertificateAndAuthParams(secureId, secureKey, username);
    this.messageSessionFactory = new TubeMessageSessionFactory(tubeConfig);
    TreeSet<String> fliterSet = null;
    while (true) {
      try {
        this.messagePullConsumer =
            messageSessionFactory.createPullConsumer(consumerConfig);
        this.messagePullConsumer.subscribe(topic, fliterSet);
        messagePullConsumer.completeSubscribe();
        break;
      } catch (Exception e) {
        if (shouldPrint(10000)) {
          LOG.warn(TubeSource.class.getCanonicalName() + " error:"
              + e.getMessage(), e);
        }
        if (messagePullConsumer != null) {
          messagePullConsumer.shutdown();
        }
        Thread.sleep(1000);
      }
    }

    // wait completeSubscribe TODO
    Thread.sleep(30000);
  }

  @Override public void close() throws Exception {
    super.close();
    if (messagePullConsumer != null) {
      try {
        messagePullConsumer.shutdown();
      } catch (TubeClientException e) {
        LOG.error("TUBE_CLIENT_ERROR : " + e.getMessage());
      }
    }
  }

  @Override public void run(SourceContext<String> sourceContext) throws Exception {
    String consumerId = messagePullConsumer.getClientId();
    LOG.info("consumerId:" + consumerId);
    while (running) {
      try {
        ConsumerResult result = this.messagePullConsumer.getMessage(topic);
        if (result.isSuccess()) {
          List<Message> messageList = result.getMessageList();
          ConsumerResult confirmResult = this.messagePullConsumer
              .confirmConsume(result.getConfirmContext(), true);
          if (confirmResult.isSuccess()) {
            if (messageList != null && messageList.size() > 0) {
              processTdMsg(messageList, result.getPartitionKey(), consumerId);
//              if (isTdMsg) {
//                processTdMsg(messageList, result.getPartitionKey(), consumerId);
//              } else {
//                for (Message message : messageList) {
//                  Row row = defaultDeserializationSchema
//                      .deserialize(message.getData());
//                  if (row != null) {
//                    emitRow(row, result.getPartitionKey(), consumerId,
//                        (DefaultDeserializationSchema) defaultDeserializationSchema,
//                        cacheUnit);
//                  }
//            }
//              }
//              sourceContext.collect(new String(message.getData()));
              sourceContext.collect("123456");
            }
          } else {
            LOG.info("ConfirmConsume failure, errCode is "
                + confirmResult.getErrCode() + ",Error message is "
                + confirmResult.getErrInfo());
          }
        } else {
          LOG.warn(
              "Receive messages failure,errorCode is " + result.getErrCode()
                  + ", Error message is " + result.getErrInfo());
          if (result.getErrCode() == -1 || result.getErrCode() == 404) {
            Thread.sleep(1000);
          }
        }
      } catch (Exception e) {
        LOG.error("[consume message exception trace]:" + e.getMessage(), e);
      }
    }

  }

  @Override public void cancel() {
    running = false;
  }

  private void processTdMsg(List<Message> messageList, String partitionKey,
      String consumerId) throws Exception {
    for (Message message : messageList) {
      LOG.info("====================process message topic=" + message.getTopic() + ", attribute=" + message.getAttribute() + "===================");
      TDMsg1 tdmsg = TDMsg1.parseFrom(message.getData());
      for (String attr : tdmsg.getAttrs()) {
        LOG.info("====================process attr===================" + attr);
        Iterator<byte[]> it = tdmsg.getIterator(attr);
        if (it != null) {
          while (it.hasNext()) {
            // 这里的body即为原始的业务数据
            byte[] body = it.next();
            LOG.info("====================process body===================" + new String(body));
            // attr即为用户发送消息时所附带的属性数据，其内容为类似k1=v1&k2=v2的kv
            // 结构的字符串，其中包括bid、tid、dt等信息
            final Splitter splitter = Splitter.on("&");
            Map<String, String> attrMap = Constants.parseAttr(splitter, attr, "=");
            String tid = attrMap.get("tid");
            LOG.info("====================process tid===================" + tid + ",attrMap=" + attrMap);
            if ("dig_test".equals(tid)) {
              // 处理接口id为mytid的数据
            }
          }
        }
      }
    }
  }

  public boolean shouldPrint(int timeout) {
    if (System.currentTimeMillis() - start > timeout) {
      start = System.currentTimeMillis();
      return true;
    }
    return false;
  }
}
