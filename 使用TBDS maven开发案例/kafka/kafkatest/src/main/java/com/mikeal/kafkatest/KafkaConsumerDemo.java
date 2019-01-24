/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mikeal.kafkatest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

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
        
        confForAuthentication(props, secureId, secureKey);
        
        consumer = new KafkaConsumer<String,String>(props);
        this.topic = topic;
    }
    
    private void confForAuthentication(Properties props,String secureId,String secureKey) {
      // 设置认证参数
      props.put(TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL, TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL_AVLUE);
      props.put(TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM, TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM_VALUE);
      props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_ID,secureId);
      props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_KEY,secureKey);
    }

    public void run() {
      while(true){
        doWork();
        
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
      }
    }

    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }
    }
    
    public static void main(String[] args) {
      if (args == null || args.length != 5) {
        System.out.println("Usage: topic group brokerlist[host1:port,host2:port,host3:port] secureId secureKey");
        System.exit(1);
      }
      KafkaConsumerDemo kafkaConsumerDemo = new KafkaConsumerDemo(args[0], args[1],args[2],args[3],args[4]);
      Thread thread = new Thread(kafkaConsumerDemo);
      thread.start();
    }
}
