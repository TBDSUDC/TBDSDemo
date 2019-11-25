package com.tencent.tbds.tdbank.source;

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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TubeSource extends RichParallelSourceFunction<String> {
    private static Logger LOG = Logger.getLogger(TubeSource.class);
    long start = System.currentTimeMillis();
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

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //通过环境变量获取tube的相关的配置
        globalMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        haHosts = globalMap.get("tube.ha.hosts");
        consumerGroup = globalMap.get("tube.consumer.group");
        username = globalMap.get("tube.consumer.username");
        secureId = globalMap.get("tube.consumer.secureId");
        secureKey = globalMap.get("tube.consumer.secureKey");
        topic = globalMap.get("tube.consumer.bid");
        isTdMsg = Boolean.parseBoolean(globalMap.getOrDefault("tube.consumer.isTdMsg", "false"));
        LOG.info("parameter " + haHosts + "," + consumerGroup + "," + username + "," + secureId + "," + secureKey + "," + topic);

        //构造tube的client消费端的配置配置
        TubeClientConfig tubeConfig = new TubeClientConfig();
        tubeConfig.setMasterInfo(new MasterInfo(haHosts));
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerGroup);
        consumerConfig.setConsumeFromMaxOffset(false);
        consumerConfig.setCertificateAndAuthParams(secureId, secureKey, username);

        //根据消费端的配置构建tube的消费session工厂
        this.messageSessionFactory = new TubeMessageSessionFactory(tubeConfig);
        TreeSet<String> fliterSet = null;
        while (true) {
            try {
                //构建获取信息的consumer
                this.messagePullConsumer =
                        messageSessionFactory.createPullConsumer(consumerConfig);
                //consumer订阅topic，和过滤器，当前过滤器为空
                this.messagePullConsumer.subscribe(topic, fliterSet);
                //完成消息的订阅
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

    @Override
    public void close() throws Exception {
        super.close();
        if (messagePullConsumer != null) {
            try {
                messagePullConsumer.shutdown();
            } catch (TubeClientException e) {
                LOG.error("TUBE_CLIENT_ERROR : " + e.getMessage());
            }
        }
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //从消费端获取client的消费id
        String consumerId = messagePullConsumer.getClientId();
        LOG.info("consumerId:" + consumerId);
        while (running) {
            try {
                //根据端口号获取消费的信息
                ConsumerResult result = this.messagePullConsumer.getMessage(topic);
                //判断消费的数据是否成功
                if (result.isSuccess()) {
                    List<Message> messageList = result.getMessageList();
                    //确认信息消费成功
                    ConsumerResult confirmResult = this.messagePullConsumer
                            .confirmConsume(result.getConfirmContext(), true);
                    //判断信息是否发送成功
                    if (confirmResult.isSuccess()) {
                        if (messageList != null && messageList.size() > 0) {
                            processTdMsg(messageList, result.getPartitionKey(), consumerId);
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

    @Override
    public void cancel() {
        running = false;
    }

    /**
     * Tube的数据消息模式是做过压缩和加密处理，所以文件是乱码需要进行处理
     * @param messageList
     * @param partitionKey
     * @param consumerId
     * @throws Exception
     */
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
