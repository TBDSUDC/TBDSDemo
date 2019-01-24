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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;

public class KafkaProducerDemo extends Thread {
    private final KafkaProducer<Long, String> producer;
    private final String topic;
    private final String isasync;
    private static int messageCount = 1000;
    public KafkaProducerDemo(String topic, String brokerList, String secureId, String secureKey, String isasync) {
        Properties props = new Properties();
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerList);
        //props.put("batch.num.messages","10");
        //props.put("queue.buffering.max.messages", "20");
        //linger.ms should be 0~100 ms
        props.put("linger.ms", "50");
        //props.put("block.on.buffer.full", "true");
        //props.put("max.block.ms", "100000");
        //batch.size and buffer.memory should be changed with "the kafka message size and message sending speed"
        props.put("batch.size", "16384");
        props.put("buffer.memory", "1638400");

        confForAuthentication(props, secureId, secureKey);
        
        producer = new KafkaProducer<Long, String>(props);
        this.topic = topic;
        this.isasync = isasync;
    }

    private void confForAuthentication(Properties props,String secureId,String secureKey) {
        props.put(TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL, TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL_AVLUE);
        props.put(TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM, TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM_VALUE);
        props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_ID,secureId);
        props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_KEY,secureKey);
    }

    public void run() {
        long messageNo = 0;
        while (messageNo < KafkaProducerDemo.messageCount) {
            String messageStr = java.util.UUID.randomUUID().toString() + "--" + messageNo;
            long startTime = System.currentTimeMillis();
            if ( isasync.equals("true") ) { // Send asynchronously
                producer.send(new ProducerRecord<Long, String>(topic,
                    messageNo,
                    messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else { // Send synchronously
                try {
                    producer.send(new ProducerRecord<Long, String>(topic,
                        messageNo,
                        messageStr)).get();
                    System.out.println("thread "+Thread.currentThread().getName() +" Sent message: (" + messageNo + ", " + messageStr + ")");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
        producer.flush();  //flush the profucer buffer to send all messages to kafka
    }
    
    public static void main(String[] args) {
      if (args == null || args.length != 6) {
        System.out.println("Usage: topic brokerlist[host1:port,host2:port,host3:port] secureId secureKey true/false producerCount");
        System.exit(1);
      }

      //KafkaProducerDemo.messageCount = 1000;
      int producerCount = Integer.parseInt( args[5] );
      for( int i = 0 ; i < producerCount ; i ++ ) {
          KafkaProducerDemo kafkaProducerDemo = new KafkaProducerDemo(args[0], args[1], args[2], args[3], args[4]);
          kafkaProducerDemo.run();
      }
    }
}

class DemoCallBack implements Callback {
    private final long startTime;
    private final long key;
    private final String message;

    public DemoCallBack(long startTime, long key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println(
                "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                    "), " +
                    "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}
