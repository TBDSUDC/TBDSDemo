package com.tencent.tbds.tdbank.utils;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class Constants implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.class);
  private static final long serialVersionUID = 1L;

  //tbds
  public static final String SECERT_ID = "secretid";
  public static final String SECRET_KEY = "secretkey";
  public static final String USER_NAME = "username";

  private static final Splitter splitter = Splitter.on("&");

  public static Map<String, String> parseAttr(String attr) {
    HashMap<String, String> map = new HashMap<String, String>();
    Iterator<String> it = splitter.split(attr).iterator();
    while (it.hasNext()) {
      String s = it.next();
      int idx = s.indexOf("=");
      String k = s;
      String v = null;
      if (idx > 0) {
        k = s.substring(0, idx);
        v = s.substring(idx + 1);
      }
      map.put(k, v);
    }
    return map;
  }

  public static Map<String, String> parseAttr(Splitter splitter, String attr, String entrySplitterStr) {
    HashMap<String, String> map = new HashMap<String, String>();
    Iterator<String> it = splitter.split(attr).iterator();
    while (it.hasNext()) {
      String s = it.next();
      int idx = s.indexOf(entrySplitterStr);
      String k = s;
      String v = null;
      if (idx > 0) {
        k = s.substring(0, idx);
        v = s.substring(idx + 1);
      }
      map.put(k, v);
    }
    return map;
  }

  public static Map<String, String> parseTdMsgAttr(String attr) {
    Map<String, String> result = new HashMap<String, String>();
    if (StringUtils.isNotEmpty(attr)) {
      String[] items = attr.trim().split("&");
      if (items != null && items.length > 0) {
        for (String item : items) {
          String[] res = item.trim().split("=");
          if (res.length == 2) {
            result.put(res[0], res[1]);
          }
        }
      }
    }
    return result;
  }

  static long now = System.currentTimeMillis();

  public static boolean shouldPrint(int timeout) {
    if (System.currentTimeMillis() - now > timeout) {
      now = System.currentTimeMillis();
      return true;
    }
    return false;
  }

  public static Map<String, String> mapStringToMap(String str) {
    str = str.substring(1, str.length() - 1);
    String[] strs = str.split(",");
    Map<String, String> map = new HashMap<String, String>();
    for (String string : strs) {
      String key = string.split("=")[0];
      String value = string.split("=")[1];
      map.put(key, value);
    }
    return map;
  }

  public static Integer getInt(Object o, Integer defaultValue) {
    if (null == o) {
      return defaultValue;
    }

    if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
      return ((Number) o).intValue();
    } else if (o instanceof Long) {
      final long l = (Long) o;
      if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
        return (int) l;
      }
    } else if (o instanceof String) {
      return Integer.parseInt((String) o);
    }

    throw new IllegalArgumentException(
        "Don't know how to convert " + o + " to int");
  }

  public static void main(String[] args) {
    // Row r = new Row(3);
    // System.out.println(r.getArity());

    String attr =
        " __clientip=100.108.252.20&sid=71760266273909036&qq=2572349305&query=UmlkZSBDb3dib3kgUmlkZQ==&time=552820120&ip=183.227.145.219&type=Common&pos=4&area_idx=1&docid=11557715734914196968&action=play&text=UmlkZSBDb3dib3kgUmlkZQ==&res_type=Danqu&page=0&platform=2&remoteplace=&test_id=&bak1=0&bak2=0&bak3=&bak4=&userid=0&cmd=93&uuid=891875818&udid=5B082D269C3C47AB8DD67EA8CBFB3496&openudid=5B082D269C3C47AB8DD67EA8CBFB3496&openudid2=&chid=&ct=1&cv=80500109&os_ver=11.3&buildver=56375";
    Map<String, String> map = parseAttr(attr);
    System.out.println(map.size());
  }
}
