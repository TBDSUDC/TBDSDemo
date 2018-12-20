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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import com.tencent.jdbc.JdbcCRUD;

public final class StreamingDirectKafkaToMysql {

  public static void main(String[] args) throws Exception {
	  
  	if (args == null || args.length != 8) {
        System.out.println("Usage: topic bootstrapServers kafak_tbds_sasl_id kafak_tbds_sasl_key ....共8个参数，见文档说明。");
        System.exit(1);
    }  

	//这两个参数可以传过来  
    String topic = args[0];  //设置topic参数,这里由开发者自行设置
    String bootstrapServers = args[1]; //设置kafka broker参数,这里由开发者自行设置
    String kafak_tbds_sasl_id=args[2]; //设定kafka的认证id
    String kafak_tbds_sasl_key=args[3]; //设定kafka的认证key
    
    String jdbcUrlIP=args[4];
    String jdbcDB=args[5];
    String	jdbcUserProd=args[6];
    String	jdbcPasswordProd=args[7];
    
    final String url="jdbc:mysql://"+jdbcUrlIP+":3306/"+jdbcDB;
    final String user=jdbcUserProd;
    final String password=jdbcPasswordProd;
    
    SparkConf conf = new SparkConf();
    conf.setAppName("StreamingDirectKafkaToMysql");  //设置spark application的名称
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
    JavaPairDStream<String, String> dd = stream.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, String> call(ConsumerRecord<String, String> cr)
				throws Exception {
			
			String [] value = cr.value().split(" ");
			String batchId =  value[0];
			return new Tuple2<String, String>(batchId,cr.value());
		}
	});
    
    JavaPairDStream<String, Iterable<String>> time2sessionsRDD = dd.groupByKey();
    
    
    JavaPairDStream<String, String> userid2PartAggrInfoRDD =time2sessionsRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {
		
    	private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, String> call(Tuple2<String, Iterable<String>> t)
				throws Exception {
			
			String batchId = t._1;
			Iterable<String> values = t._2;
			Long sunCount = 0L;
			BigDecimal sunPrice = new BigDecimal("0.00");
			
			for(String value:values){
				String [] propStr  = value.split(" ");
				String price = propStr[1];
				sunPrice = sunPrice.add(new BigDecimal(Double.valueOf(price)));
				sunCount++;
			}
			return new Tuple2<String, String>(batchId, sunCount+":"+sunPrice.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
		}
	});
    
    
    userid2PartAggrInfoRDD.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
		private static final long serialVersionUID = 1L;
		@Override
		public void call(JavaPairRDD<String, String> rdd) throws Exception {
			rdd.foreach(new VoidFunction<Tuple2<String,String>>() {
				private static final long serialVersionUID = 1L;
				@Override
				public void call(Tuple2<String, String> t)
						throws Exception {
					
					String batchId = t._1;
					String sumStr = t._2;
					
					StreamingDirectKafkaToMysql.updateBatch(url, user, password,batchId, sumStr);
					
				}
			});
			
		}
	});
    streamingContext.start();

    streamingContext.awaitTermination();
    
    streamingContext.stop();
   }
   public static void updateBatch(String url,String user,String password,String batchId,String sumStr) {
		
		System.out.println("batchId-------------------------->"+batchId+",sumStr"+sumStr);
		
		String querySql = "select count(batchId) from TBDS_TEST where batchId = ? ";
		
		String insertSql = "INSERT INTO TBDS_TEST(sunCount,sunPrice,batchId) VALUES(?,?,?) ";
		
		String updateSql = "UPDATE TBDS_TEST t set t.sunCount = t.sunCount+?, t.sunPrice = t.sunPrice+? where batchId = ? ";
		
		Object[] obj = new Object[]{sumStr.split(":")[0],sumStr.split(":")[1],batchId};
		
		int count = JdbcCRUD.executeQueryCount(url, user, password,querySql, new Object[]{batchId});
		
		if(count > 0) { 
			//如果相应批次的数据已经存在，则执行修改，sunCount = 原来的值+现在统计的值，sunPrice=原来的值+现在统计的值
			JdbcCRUD.executeUpdate(url, user, password,updateSql, obj);
		} else {
			JdbcCRUD.executeUpdate(url, user, password,insertSql, obj);
		}
	}
}
