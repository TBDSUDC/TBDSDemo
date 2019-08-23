package com.demo;

import com.tencent.tube.Message;
import com.tencent.tube.client.MessageSessionFactory;
import com.tencent.tube.client.TubeClientConfig;
import com.tencent.tube.client.TubeMessageSessionFactory;
import com.tencent.tube.client.consumer.ConsumerConfig;
import com.tencent.tube.client.consumer.MessageConsumer;
import com.tencent.tube.client.consumer.MessageListener;
import com.tencent.tube.client.consumer.pullconsumer.PullMessageConsumer;
import com.tencent.tube.cluster.MasterInfo;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.log4j.Logger;

public class TubeConsumerDemo {
    private static Logger log = Logger.getLogger(TubeConsumerDemo.class);

    public static void main(String[] args) throws Exception {

        PullMessageConsumer messagePullConsumer = null;

        Map<String,String> map = new HashMap();
        map.put("tube.ha.hosts", "tbds-172-16-16-31:9100,tbds-172-16-16-38:9100,tbds-172-16-16-49:9100");
        map.put("tube.consumer.group", "dig_dig_test_admin");
        map.put("tube.consumer.username", "admin");
        map.put("tube.consumer.secureId", "M7rmdKryXYRYne8w7bq277yCD7hkUKKMsMot");
        map.put("tube.consumer.secureKey", "4uXZEchnNUOhsrTxw1iynPoyEt8vRk8O");
        map.put("tube.consumer.bid", "dig_test");
        map.put("tube.consumer.isTdMsg", "false");


        String haHosts = map.get("tube.ha.hosts");
        String consumerGroup = map.get("tube.consumer.group");
        String username = map.get("tube.consumer.username");
        String secureId = map.get("tube.consumer.secureId");
        String secureKey = map.get("tube.consumer.secureKey");
        String topic = map.get("tube.consumer.bid");

        TubeClientConfig tubeConfig = new TubeClientConfig();
        tubeConfig.setMasterInfo(new MasterInfo(haHosts));
        MessageSessionFactory messageFactory = new TubeMessageSessionFactory(tubeConfig);
        //2、创建MessageConsumer对象
        //消费配置对象
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerGroup);
        //启动后，从最大offset消费
        consumerConfig.setConsumeFromMaxOffset(false);
        consumerConfig.setCertificateAndAuthParams(secureId, secureKey, username);
        MessageConsumer consumer = messageFactory.createConsumer(consumerConfig);
        System.out.println("开始订阅消息");
        consumer.subscribe(topic, new MessageHander());
        System.out.println("订阅完成");
        consumer.completeSubscribe();
    }
}


/**
 * Message Hander
 */
class MessageHander implements MessageListener {
    private static Logger log = Logger.getLogger(MessageListener.class);
    public void recieveMessages(Message message) throws InterruptedException {
        System.out.println("-----------------------"+message.toString());
        log.info("==="+message.toString());
    }
    public Executor getExecutor() {
        return null;
    }

    public void stop() {
        System.out.println("监听停止");
    }
}
