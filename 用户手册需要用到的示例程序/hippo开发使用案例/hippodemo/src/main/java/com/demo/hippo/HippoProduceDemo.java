package com.demo.hippo;

import com.tencent.hippo.Message;
import com.tencent.hippo.client.producer.ProducerConfig;
import com.tencent.hippo.client.producer.SendResult;
import com.tencent.hippo.client.producer.SimpleMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  <h1>This is used for access hippo</h1>
 */
public class HippoProduceDemo {

    /**logger**/
    private static Logger logger = LoggerFactory.getLogger(HippoProduceDemo.class);

    //Hipper Controller address
    private static final String CONTROLLER_IP_LIST="="**************";:8066";
    //Hipper Borker group
    private static final String BROKER_GROUP="hippoBrokerGroup";
    //Hipper Produce Configure
    private static ProducerConfig  producerConfig;

    /***Producer****/

    //Hipper topice accessId
    private static final String ACCESS_ID="**************";
    //Hipper topice accessKey(Note: Each topic's key and id are different)
    private static final String ACCESS_KEY="="**************";";
    //Hipper access user
    private static final String USERNAME="admin";

    //topic
    private static final String[] topics = {"tpoic_001"};

    //对生产者进行配置话
    static {
        /***producer**/
        producerConfig= new ProducerConfig(CONTROLLER_IP_LIST, BROKER_GROUP);
        producerConfig.setSecretId(ACCESS_ID);
        producerConfig.setSecretKey(ACCESS_KEY);
        producerConfig.setUserName(USERNAME);
    }

    /**Testing for sending data**/
    public static void main(String[] args) throws Exception{
        //Build testing message
        SimpleMessageProducer smp = new SimpleMessageProducer(producerConfig);
        //Register topic
        for(String topic : topics){
            smp.publish(topic);
        }
        // build information.
        SendResult sendResult = null;
        for(int i=0;i<=100;i++){
            String content = "td:td1,content:内容【"+i+"】";
            Message msg = new Message("tpoic_001",content.getBytes());
            sendResult = smp.sendMessage(msg);
            if(sendResult.isSuccess()){
                System.out.println("scuccessfull id is "+i);
                logger.info("Send successfull【"+i+"】");
            }else{
                logger.error("Send fail【"+i+"】"+sendResult.getCode());
            }
        }
    }
}
