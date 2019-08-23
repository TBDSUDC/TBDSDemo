package com.tencent.tbds.tdbank;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.tencent.tbds.tdbank.source.TubeSource;

import java.util.HashMap;

public class AppMain {
  public static void main(String[] args) throws Exception {
    HashMap<String, String> map = new HashMap<String, String>(7);
    map.put("tube.ha.hosts", "tbds-172-16-16-31:9100,tbds-172-16-16-38:9100,tbds-172-16-16-49:9100");
    map.put("tube.consumer.group", "dig_dig_test_admin");
    map.put("tube.consumer.username", "admin");
    map.put("tube.consumer.secureId", "M7rmdKryXYRYne8w7bq277yCD7hkUKKMsMot");
    map.put("tube.consumer.secureKey", "4uXZEchnNUOhsrTxw1iynPoyEt8vRk8O");
    map.put("tube.consumer.bid", "dig_test");
    map.put("tube.consumer.isTdMsg", "false");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    DataStreamSource<String> dataStreamSource = env.addSource(new TubeSource()).setParallelism(1);
    dataStreamSource.print();
    env.execute("TestTube");
  }
}
