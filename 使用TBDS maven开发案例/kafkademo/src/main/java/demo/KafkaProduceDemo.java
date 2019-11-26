package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProduceDemo {

    public static final String TOPIC = "test01";

    public static void main(String[] args) throws Exception {
        producerDemo();
    }


    private static Properties getDefaultProperties() throws IOException {
        Properties properties = new Properties();
        // 也可以把认证以及配置信息以key-value的方式写入到一个properties配置文件中，使用时直接载入
//        properties.load(new BufferedInputStream(new FileInputStream("/opt/cluster_conf/kafka/kafka_conf.properties")));

        // hard code config information
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "********:6667");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "t_topice01");

        //add authentication information directly
        properties.put("security.protocol", "SASL_PLAINTEXT");
//        properties.put("sasl.mechanism", "TBDS");
//        properties.put("sasl.tbds.secure.id", "********");
//        properties.put("sasl.tbds.secure.key", "********");

        return properties;
    }

    private static void producerDemo() throws Exception {
        Properties properties = getDefaultProperties();
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 10);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        Future<RecordMetadata> future = null;

        SimpleDateFormat format = new SimpleDateFormat("YYYYMMDD");
        String ftime = format.format(new Date());


        int sendNo = 0;

        for (int i = 0; i < 20; i++) {
            String input = ftime + ",zhangsan03," + i;
            future = producer.send(new ProducerRecord<String, String>(TOPIC, String.valueOf(i), input));
            System.out.println("send message:[topic, offset, key, value]--->[" + TOPIC + "," + future.get().offset() + "," + sendNo + "," + input + "]");
        }
    }

    private static void consumerDemo() throws Exception {
        Properties properties = getDefaultProperties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received message: [" + record.key() + ", " + record.value() + "] at offset " + record.offset());
            }
        }
    }


}
