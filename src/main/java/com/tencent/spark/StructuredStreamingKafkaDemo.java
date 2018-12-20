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

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public final class StructuredStreamingKafkaDemo {

  public static void main(String[] args) throws Exception {
	  
	  
		  //这两个参数可以传过来  
	      String topics = args[0];  //设置topic参数,这里由开发者自行设置
	      String bootstrapServers = args[1]; //设置kafka broker参数,这里由开发者自行设置
	      String kafak_tbds_sasl_id=args[2]; //设定kafka的认证id
	      String kafak_tbds_sasl_key=args[3]; //设定kafka的认证key

	      SparkSession spark = SparkSession
	        .builder()
	        .appName("JavaStructuredKafkaWordCount")
	        .getOrCreate();

	      // Create DataSet representing the stream of input lines from kafka
	      Dataset<String> lines = spark
	        .readStream()
	        .format("kafka")
	        .option("kafka.bootstrap.servers", bootstrapServers)
	        .option("subscribe", topics)
	        .option("kafka.security.protocol", "SASL_TBDS")
	        .option("kafka.sasl.mechanism", "TBDS")
	        .option("kafka.sasl.tbds.secure.id", kafak_tbds_sasl_id)
	        .option("kafka.sasl.tbds.secure.key", kafak_tbds_sasl_key)
	        .load()
	        .selectExpr("CAST(value AS STRING)")
	        .as(Encoders.STRING());

	      // Generate running word count
	      Dataset<Row> wordCounts = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
	        public Iterator<String> call(String x) {
	          return Arrays.asList(x.split(" ")).iterator();
	        }
	      }, Encoders.STRING()).groupBy("value").count();

	      // Start running the query that prints the running counts to the console
	      StreamingQuery query = wordCounts.writeStream()
	        .outputMode("complete")
	        .format("console")
	        .start();

	      query.awaitTermination();
	  
  }
}
