package com.tencent.tbds.tdbank;

import com.google.common.base.Splitter;
import com.tencent.tbds.tdbank.utils.Constants;
import com.tencent.tdbank.msg.TDMsg1;
import com.tencent.tube.Message;
import com.tencent.tube.client.MessageSessionFactory;
import com.tencent.tube.client.TubeClientConfig;
import com.tencent.tube.client.consumer.ConsumerConfig;
import com.tencent.tube.client.consumer.pullconsumer.ConsumerResult;
import com.tencent.tube.client.consumer.pullconsumer.PullMessageConsumer;
import com.tencent.tube.cluster.MasterInfo;

import java.util.*;


public class TubeConsumerDemo {

    private static HashMap<String, String> map = new HashMap<String, String>(7);
    private static MessageSessionFactory messageSessionFactory = null;
    private static PullMessageConsumer pullMessageConsumer = null;
    private static TreeSet<String> fliterSet = null;
    private static boolean isTdMsg;
    private static boolean running = true;


    static {
        map.put("tube.ha.hosts", "tbds-10-228-128-2:9100,tbds-10-228-128-73:9100,tbds-10-228-128-17:9100");
        map.put("tube.consumer.group", "CCTV_YSP_FTBFZL_RC_PROD_CCTV_YSP_SJZT_PROD_b_omg_video_2894_sandmliu");
        map.put("tube.consumer.username", "sandmliu");
        map.put("tube.consumer.secureId", "Lib68S2KRAFpHsKngz6AojvEbfOSaFSiTOoG");
        map.put("tube.consumer.secureKey", "ce2OWcS57FUgAPVwBRa6GcL9z4kdz9sc");
        map.put("tube.consumer.bid", "CCTV_YSP_SJZT_PROD_b_omg_video_2894");
        map.put("tube.consumer.isTdMsg", "false");
    }

    public static void main(String[] args) throws Exception {
        ConsumerConfig consumerConfig = buildConnection();

        pullMessageConsumer = messageSessionFactory.createPullConsumer(consumerConfig);
        pullMessageConsumer.subscribe(map.get("tube.consumer.bid"), fliterSet);
        isTdMsg = Boolean.parseBoolean(map.getOrDefault("tube.consumer.isTdMsg", "false"));
        pullMessageConsumer.completeSubscribe();

        //拉取数据


    }

    /**
     * 建立和tube的链接,构建tubesessionFactory
     */
    private static ConsumerConfig buildConnection() throws Exception {
        TubeClientConfig tubeConfig = new TubeClientConfig();
        tubeConfig.setMasterInfo(new MasterInfo(map.get("tube.ha.hosts")));
        ConsumerConfig consumerConfig = new ConsumerConfig(map.get("tube.consumer.group"));
        consumerConfig.setConsumeFromMaxOffset(false);
        consumerConfig.setCertificateAndAuthParams(map.get("tube.consumer.secureId"), map.get("tube.consumer.secureKey"), map.get("tube.consumer.username"));
        return consumerConfig;
    }


    public static void getDate() throws Exception {
        String consumerId = pullMessageConsumer.getClientId();
        while (running) {
            try {
                ConsumerResult result = pullMessageConsumer.getMessage(map.get("tube.consumer.bid"));
                if (result.isSuccess()) {
                    List<Message> messageList = result.getMessageList();
                    ConsumerResult confirmResult = pullMessageConsumer
                            .confirmConsume(result.getConfirmContext(), true);
                    if (confirmResult.isSuccess()) {
                        if (messageList != null && messageList.size() > 0) {
                            processTdMsg(messageList, result.getPartitionKey(), consumerId);
                        }
                    } else {
                        System.out.println("结果失败");
                    }
                } else {
                    if (result.getErrCode() == -1 || result.getErrCode() == 404) {
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private static void processTdMsg(List<Message> messageList, String partitionKey,
                                     String consumerId) throws Exception {
        for (Message message : messageList) {
            //  LOG.info("====================process message topic=" + message.getTopic() + ", attribute=" + message.getAttribute() + "===================");
            TDMsg1 tdmsg = TDMsg1.parseFrom(message.getData());
            for (String attr : tdmsg.getAttrs()) {
                //    LOG.info("====================process attr===================" + attr);
                Iterator<byte[]> it = tdmsg.getIterator(attr);
                if (it != null) {
                    while (it.hasNext()) {
                        // 这里的body即为原始的业务数据
                        byte[] body = it.next();
                        //       LOG.info("====================process body===================" + new String(body));
                        // attr即为用户发送消息时所附带的属性数据，其内容为类似k1=v1&k2=v2的kv
                        // 结构的字符串，其中包括bid、tid、dt等信息
                        final Splitter splitter = Splitter.on("&");
                        Map<String, String> attrMap = Constants.parseAttr(splitter, attr, "=");
                        String tid = attrMap.get("tid");
                        //    LOG.info("====================process tid===================" + tid + ",attrMap=" + attrMap);
                        if ("dig_test".equals(tid)) {
                            // 处理接口id为mytid的数据
                        }
                    }
                }
            }
        }
    }

}
