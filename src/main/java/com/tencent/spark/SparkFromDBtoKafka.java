package com.tencent.spark;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 使用spark-sql从db中读取数据, 处理后再回写到Kafka
 * @author lenovo
 */
public class SparkFromDBtoKafka implements Serializable {

	private static final long serialVersionUID = 1L;
    /**
     * 使用spark-sql从db中读取数据, 处理后再回写到Kafka
     */
    public void loadDBtoKafka(SparkSession sparkSession) {
        
        Dataset<Row> jdbcDF = sparkSession.read()
        	      .format("jdbc")
        	      .option("url", "jdbc:postgresql:dbserver")
        	      .option("dbtable", "schema.tablename")
        	      .option("user", "username")
        	      .option("password", "password")
        	      .load();
        
        JavaRDD<Row> jdbcRDD = jdbcDF.javaRDD();
        jdbcRDD.foreachPartition(new VoidFunction<Iterator<Row>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Iterator<Row> it) throws Exception {
				//批量处理
				while(it.hasNext()) {
					Row rot = it.next();//获取一行数据
					
				}
			}
		});;
        
    }
    public static void main(String[] args) {
    	SparkFromDBtoKafka loadDBtoKafka = new SparkFromDBtoKafka();
    	SparkSession sparkSession = SparkSession
    		      .builder()
    		      .appName("SparkFromDBtoKafka")
    		      .config("spark.some.config.option", "some-value")
    		      .getOrCreate();
    	
        System.out.println(" ---------------------- start DBtoKafka ------------------------");
    	loadDBtoKafka.loadDBtoKafka(sparkSession);
        System.out.println(" ---------------------- finish DBtoKafka -----------------------");

        sparkSession.stop();
    }
}