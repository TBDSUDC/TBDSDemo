package com.demo.flink.opensource;

import org.apache.flink.util.CollectionUtil;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.CollectionUtils;
import scala.math.Ordering;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class StringUtils {

    public static String getMapToString(Map<String, PartitionInfo> map) {

        return  map.keySet().toString();
    }
}
