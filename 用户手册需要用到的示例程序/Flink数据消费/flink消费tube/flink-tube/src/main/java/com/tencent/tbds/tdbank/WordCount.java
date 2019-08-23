package com.tencent.tbds.tdbank;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.log4j.Logger;

import com.tencent.tbds.tdbank.filter.filterFun;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WordCount {
  public static void main(String[] args) throws Exception {
    //1、flink环境设置，本例主要使用DataStream API
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    //2、生成source
    DataStream<String> ds = env.addSource(new SourceFunction<String>() {
      private static final long serialVersionUID = 1L;
      private boolean isRunning = true;

      @Override
      public void run(SourceContext<String> ctx) throws Exception {
        // 生成数据
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (isRunning) {
          ctx.collect(sdf.format(new Date()) + "_abcd");
          ctx.collect(sdf.format(new Date()) + "_efg");
          try {
            Thread.sleep(1000);
          } catch (Exception e) {
            // TODO: handle exception
          }
        }
      }

      @Override
      public void cancel() {
        isRunning = false;
      }
    });

    //3、业务逻辑
    ds = ds.filter(new filterFun());

    ds.print();

    //5、执行任务
    env.execute("wordcount");
  }
}
