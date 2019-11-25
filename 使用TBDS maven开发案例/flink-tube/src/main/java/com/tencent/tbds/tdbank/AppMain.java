package com.tencent.tbds.tdbank;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.tencent.tbds.tdbank.source.TubeSource;

import java.util.HashMap;

  public class AppMain {
  public static void main(String[] args) throws Exception {

      //Tube的相关的配置信息
      HashMap<String, String> map = new HashMap<String, String>(7);
      //tube 的地址
      map.put("tube.ha.hosts", "tbds-10-22xxxx:9100,tbds-10-22xxxx3:9100,tbds-1xxxx:9100");
      /**
       *  消费组的名称，
       *  需要确认：
       *    1. 此消费组需要在tbds界面进行预先申请，审批等等
       *    2. 此消费组的用户需要有此tube的topic的访问权限
       */
      map.put("tube.consumer.group", "CCTV_YSP_FTBFZL_RC_PROD_CCTV_YSP_SJZT_PROD_b_omg_video_2894_sandmliu");
      //消费的用户名
      map.put("tube.consumer.username", "sanxxxxx");
      //用户的security_id使用tube模块对应的id
      map.put("tube.consumer.secureId", "Lib68S2KRAFpHsKngz6Aojvxxxxxxxxxxxx");
      //用户的security_key使用tube模块对应的security_key
      map.put("tube.consumer.secureKey", "ce2OWcS57FUgAPxxxxxxxxxxxxxxxx");
      //Tdbank模块接入的bid的名称
      map.put("tube.consumer.bid", "CCTV_YSP_SJZT_PROD_b_omg_video_2894");
      //打包的方式
      map.put("tube.consumer.isTdMsg", "false");

      //获取flink的环境信息
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //加载配置内容
      env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(map));
      //定义数据出处理的窗口方式
      env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
      //添加数据的流入源
      DataStreamSource<String> dataStreamSource = env.addSource(new TubeSource()).setParallelism(1);
      //获取的数据流如何处理
      dataStreamSource.print();
      //任务的名称
      env.execute("TestTube111");


  }

}
