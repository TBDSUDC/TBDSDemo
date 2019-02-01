***
## 工程项目说明
***
&#160; &#160; 本eclipse工程主要包含kafka、hadoop（hdfs、mr）、spark、tbds自定义工作流等开发示例，内容是基于tbds的开源组件的简单二次开发，具体示例的操作说明文档路径：https://github.com/TBDSUDC/TBDSDemo/tree/master/doc <br>
**demo清单如下：** <br>
1. kafka 生产消费示例：包含一个producer和consumer程序，一个用来向test_topic发送数据，一个用来从test_topic读取数据
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/tree/master/src/main/java/com/tencent/kafka
2. hdfs的java客户端示例：列出根目录下的所有文件与文件夹
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/tree/master/src/main/java/com/tencent/hadoop/hdfs
3. hadoop-mr开发示例：wordCount程序
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/tree/master/src/main/java/com/tencent/hadoop/mr
4. spark core的wordCount开发示例：spark wordCount程序
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/blob/master/src/main/java/com/tencent/spark/JavaWordCount.java
5. spark-streaming消费kafka示例：spark-streaming消费kafka数据的示例
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/blob/master/src/main/java/com/tencent/spark/JavaDirectKafkaWordCount.java
6. spark-streaming消费kafka的数据汇总求和（带mysql参数传递）：spark-streaming消费kafka数据，将汇总结果保存到mysql的示例
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/blob/master/src/main/java/com/tencent/spark/StreamingDirectKafkaToMysql.java 
7. StructuredStreaming高阶api消费kafka示例：使用sparkStreaming高阶api,StructuredStreaming消费kafka示例
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/blob/master/src/main/java/com/tencent/spark/StructuredStreamingKafkaDemo.java
8. 自定义tbds工作流任务类型开发示例:自定义工作流，将CTSDB的表的数据导入TBDS的HDFS的示例
   github代码路径：https://github.com/TBDSUDC/TBDSDemo/tree/master/src/main/java/com/tencent/taskrunner
