package com.demo.flink.opensource;



import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Map;
import java.util.Properties;

/**
 * Kafka Source Demo
 */
public class KafkaSourceDemo {

    //测试的topic的名字
    private static  final String TEST_TOPIC="testflinktopic";
    //zookeeper的地址
    private static final String ZK_ADDRESS="tbds-172-16-0-2:2181";
    //Kafka的地址
    private static final String KAFKA_ADDRESS="tbds-172-****:6667";
    //认证信息的id和key
    private static final String  SECURRE_ID="053OGS4************************W4STm";
    private static final String SECURRE_KEY="8qFXU8************ssKF";


    public static void main(String[] args) throws Exception{

        //获取flink的环境信息
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkPoint的生成时间间隔
        env.enableCheckpointing(5000);
        //flink消费的数据窗口方式
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //配置kafka的参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",KAFKA_ADDRESS);
        properties.setProperty("zookeeper.connect",ZK_ADDRESS);
        properties.setProperty("group.id","test_group");
        properties.setProperty("auto.offset.reset","latest");
        properties.setProperty("enable.auto.commit","true");

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //认证信息
        properties.put("security.protocol", "SASL_TBDS");
        properties.put("sasl.mechanism", "TBDS");
        properties.put("sasl.tbds.secure.id", SECURRE_ID);
        properties.put("sasl.tbds.secure.key", SECURRE_KEY);


        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010<String>(TEST_TOPIC,new SimpleStringSchema(),properties);
//        consumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<String> keyedStream = env.addSource(consumer);
        keyedStream.print();//直接将从生产者接收到的数据在控制台上进行打印
        keyedStream.map(new MapFunction<String, Void>() {
            @Override
            public Void map(String s) throws Exception {
                System.out.println(s+"======"+getTopicLIst());
                return null;
            }
        });
        // execute program
        env.execute("Flink-Kafka-demo");
    }

    public static  String  getTopicLIst(){
        //配置kafka的参数
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers",KAFKA_ADDRESS);
        properties.setProperty("zookeeper.connect",ZK_ADDRESS);
        properties.setProperty("group.id","test_group");
        properties.setProperty("auto.offset.reset","latest");
        properties.setProperty("enable.auto.commit","true");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //认证信息
        properties.put("security.protocol", "SASL_TBDS");
        properties.put("sasl.mechanism", "TBDS");
        properties.put("sasl.tbds.secure.id", SECURRE_ID);
        properties.put("sasl.tbds.secure.key", SECURRE_KEY);
        KafkaConsumer consumer = new KafkaConsumer(properties);

        Map<String, PartitionInfo> topics = consumer.listTopics();
        return StringUtils.getMapToString(topics);

    }

}
//
//class CustomWatermarkEmitter implements AssignerWithPunctuatedWatermarks<String> {
//    private static final long serialVersionUID = 1L;
//
//    public long extractTimestamp(String arg0, long arg1) {
//        if (null != arg0 && arg0.contains(",")) {
//            String parts[] = arg0.split(",");
//            return Long.parseLong(parts[0]);
//        }
//        return 0;
//    }
//
//    public Watermark checkAndGetNextWatermark(String arg0, long arg1) {
//        if (null != arg0 && arg0.contains(",")) {
//            String parts[] = arg0.split(",");
//            return new Watermark(Long.parseLong(parts[0]));
//        }
//        return null;
//    }
//}
