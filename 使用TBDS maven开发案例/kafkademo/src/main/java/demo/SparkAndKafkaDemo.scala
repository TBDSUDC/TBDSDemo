package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark 与 kafka 集成demo
  */
object SparkAndKafkaDemo {

  /**
    * Main 程序的主方法
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /**定义程序运行的名称**/
    val appName = "sparkKafkaDemo";
    val groupId = "test_group"
    val brokers = "tbds-172-16-16-4:9092";
    /**初始化spark conf的信息，采用本地运行模式**/
    val sparkConf = new SparkConf().setAppName(appName);
    val sc = new StreamingContext(sparkConf,Seconds(5));
    //定义checkpoint
    var topics = Array("test01").toSet;
    //定义kafka的参数
    val kafkaParams = collection.Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.serializer" -> classOf[org.apache.kafka.common.serialization.StringSerializer],
      "value.serializer" -> classOf[org.apache.kafka.common.serialization.StringSerializer],
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //定义消费者，忽略offset的设置了哈
    var consumerStrategy = ConsumerStrategies.Subscribe[String,String](topics,kafkaParams);

    //通过kafaUtils来进行数据的读取
    val lines = KafkaUtils.createDirectStream(sc,LocationStrategies.PreferConsistent,consumerStrategy)

    //读取的数据进行转换成单词，根据空格进行分割，然后在统计技术
    lines.foreachRDD(r => {
     r.foreach(x =>{
       print("=================================================="+x.value())
     })
    })

    sc.start()
    sc.awaitTermination()
  }
}
