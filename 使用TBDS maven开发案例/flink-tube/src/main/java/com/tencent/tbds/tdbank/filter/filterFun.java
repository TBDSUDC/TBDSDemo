package com.tencent.tbds.tdbank.filter;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import com.tencent.tbds.tdbank.JobLog;

import java.io.Serializable;

public class filterFun extends RichFilterFunction<String> implements Serializable {
  private transient JobLog log = null;

  @Override public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    log = new JobLog();
  }

  @Override public void close() throws Exception {
    super.close();
  }

  @Override public boolean filter(String s) throws Exception {
    log.info("filter", "data: " + s);
    if (s.contains("abcd")) {
      return true;
    } else {
      return false;
    }
  }
}
