package com.demo.hippo;

import com.tencent.hippo.Message;
import com.tencent.hippo.client.MessageResult;
import com.tencent.hippo.client.consumer.ConsumerConfig;
import com.tencent.hippo.client.consumer.PullMessageConsumer;
import com.tencent.hippo.common.HippoConstants;
import com.tencent.hippo.utils.CollectionUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *  Hippo is message broker component, This is used for testing consumer
 */
@Slf4j
public class HippoConsumerDemo {

    //Hipper Controller address
    private static final String CONTROLLER_IP_LIST="="**************";:8066";
    //Hipper Borker group
    private static final String GROUP_NAME="test_group001";
    //Hipper Consumer Configure
    private static ConsumerConfig consumerConfig;
    /***Producer****/
    //Hipper topice accessKey(Note: Each topic's key and id are different)
    private static final String ACCESS_KEY="="**************";";
    //Hipper topice accessId
    private static final String ACCESS_ID="="**************";";
    //Hipper access user
    private static final String USERNAME="admin";
    //Topic
    private static final String TOPIC="tpoic_001";
    //Batch size
    private static final int BATCH_SIZE=10;
    //timeout
    private static final int TIME_OUT=10000;

    //Initial consumer configuration
    static {
        consumerConfig = new ConsumerConfig(CONTROLLER_IP_LIST,GROUP_NAME);
        consumerConfig.setSecretId(ACCESS_ID);
        consumerConfig.setSecretKey(ACCESS_KEY);
        consumerConfig.setUserName(USERNAME);
        consumerConfig.setConfirmTimeout(10000, TimeUnit.SECONDS);
    }

    /**
     * main for testing
     * @param args
     */
    public static void main(String[] args) throws Exception{
        //获取相关信息的comsumer
        PullMessageConsumer consumer = new PullMessageConsumer(consumerConfig);
        //每次接受信息的大小
        MessageResult result = null;

        boolean tag = true;

        int i=0;
        while (tag){
            result = consumer.receiveMessages(TOPIC,BATCH_SIZE,TIME_OUT);
            tag = result.isSuccess();

            List<Message> msgs = (List<Message>) result.getValue();

            if(CollectionUtils.isEmpty(msgs)){
                log.info("沉睡10秒钟");
                Thread.sleep(10000);
                continue;
            }
            for(Message msg:msgs){
                //header Information
                //System.out.println("msgheader:"+msg.getHeaders()+"这是消费的第"+i+"信息");
               // String msgBody = new String(msg.getData(), Charsets.UTF_8);
                String msgBody = new String(msg.getData()+"这是消费的第"+i+"信息");
                i++;
                log.info("msgbody:"+msgBody);
            }

            boolean confirmed = result.getContext().confirm();

           if(!confirmed){
               log.info("消费信息confirmed的值为："+confirmed);
           }else if(result.getCode()== HippoConstants.NO_MORE_MESSAGE){
               log.info("没有更多的信息消费,进程休息10瞄准");
               Thread.sleep(10000);
           }else {
               System.out.println("Hippo "+result.getCode());
               log.info("Hippo的confirm的状态code值为："+confirmed+"返回的值为："+result.getCode());
           }
        }

        //消费者关闭
        log.info("consumer 关闭");
        consumer.shutdown();
    }
}
