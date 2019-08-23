package com.tencent.tbds.tdbank;

import org.apache.log4j.Logger;

import java.io.Serializable;

public class JobLog implements Serializable {
  private Logger log = Logger.getLogger(this.getClass());

  public void debug(String name, String message) {
    log.debug("[" + name + "]: " + message);
  }

  public void info(String name, String message) {
    log.info("[" + name + "]: " + message);
  }

  public void fatal(String name, String message) {
    log.fatal("[" + name + "]: " + message);
  }

  public void warn(String name, String message) {
    log.warn("[" + name + "]: " + message);
  }

  public void error(String name, String message) {
    log.error("[" + name + "]: " + message);
  }
}
