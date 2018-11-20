package com.tencent.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
/**
 * kafka简单消费示例
 * @author lenovo
 */
public class KafkaConsumerDemo implements Runnable{
	
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public KafkaConsumerDemo(String topic, String group,String brokerList, String secureId, String secureKey) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //加入tbds认证信息
        tbdsAuthentication(props, secureId, secureKey);
        
        consumer = new KafkaConsumer<String,String>(props);
        this.topic = topic;
    }
    
    /**
     * 加入tbds认证信息
     * @param props
     * @param secureId
     * @param secureKey
     */
    public static void tbdsAuthentication(Properties props,String secureId,String secureKey) {
        props.put("security.protocol", "SASL_TBDS");
        props.put("sasl.mechanism", "TBDS");
        props.put("sasl.tbds.secure.id",secureId);
        props.put("sasl.tbds.secure.key",secureKey);
    }

    public void run() {
      while(true){
        try {
          doWork();
          Thread.sleep(1000);
        } catch (Exception e) {
        	e.printStackTrace();
        }
      }
    }

    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at partition "+record.partition()+",offset " + record.offset());
        }
    }
    
    public static void main(String[] args) {
      if (args == null || args.length != 5) {
        System.out.println("Usage: topic group brokerlist secureId secureKey");
        System.exit(1);
      }
      KafkaConsumerDemo kafkaConsumerDemo = new KafkaConsumerDemo(args[0], args[1],args[2],args[3],args[4]);
      Thread thread = new Thread(kafkaConsumerDemo);
      thread.start();
    }
}
