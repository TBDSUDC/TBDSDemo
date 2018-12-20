/*
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

package com.tencent.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public final class JavaDirectKafkaWordCount {

  public static void main(String[] args) throws Exception {
	//这两个参数可以传过来  
    String topic = args[0];  //设置topic参数,这里由开发者自行设置
    String bootstrapServers = args[1]; //设置kafka broker参数,这里由开发者自行设置
    String kafak_tbds_sasl_id=args[2]; //设定kafka的认证id
    String kafak_tbds_sasl_key=args[3]; //设定kafka的认证key

    SparkConf conf = new SparkConf();
    conf.setAppName("spark-kafka-demo");  //设置spark application的名称
    //创建Spark流应用上下文
    JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Seconds.apply(5));

    Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put("bootstrap.servers", bootstrapServers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "spark_kafka_consumer_group");  //设置kafka的消费组名称
    kafkaParams.put("auto.offset.reset", "earliest"); //设置kafka消费的offset,可以设置"latest"/"earliest",当然这里开发者也可以使用其它接口设置任意的消费起始位置offset
    kafkaParams.put("security.protocol", "SASL_TBDS"); //固定值,Kafka认证参数设置
    kafkaParams.put("sasl.mechanism", "TBDS");//固定值,Kafka认证参数设置
    kafkaParams.put("sasl.tbds.secure.id", kafak_tbds_sasl_id);
    kafkaParams.put("sasl.tbds.secure.key", kafak_tbds_sasl_key);
    kafkaParams.put("enable.auto.commit", false);


    
    Collection<String> topics = Arrays.asList(topic);

    final JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    streamingContext,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

    //压扁
    JavaDStream<String> wordsDS = stream.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, String>() {
		private static final long serialVersionUID = 1L;

		public Iterator<String> call(ConsumerRecord<String, String> r) throws Exception {
            String value = r.value();
            List<String> list = new ArrayList<String>();
            String[] arr = value.split(" ");
            for (String s : arr) {
                list.add(s);
            }
            return list.iterator();
        }
    });

  ///映射成元组
    JavaPairDStream<String, Integer> pairDS = wordsDS.mapToPair(new PairFunction<String, String, Integer>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<String, Integer>(s, 1);
        }
    });

    //聚合
    JavaPairDStream<String, Integer> countDS = pairDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;

		public Integer call(Integer v1, Integer v2) throws Exception {
            return v1 + v2;
        }
    });
    //打印
    countDS.print();

    streamingContext.start();

    streamingContext.awaitTermination();
  }
}
