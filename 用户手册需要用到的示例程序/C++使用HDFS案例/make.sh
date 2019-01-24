#!/bin/bash  

JAVA_HOME="/usr/jdk64/jdk1.8.0_111"
JRE_HOME="/usr/jdk64/jdk1.8.0_111/jre"
HADOOP_HOME="/usr/hdp/2.2.0.0-2041/hadoop"

#CLASSPATH=${JAVA_HOME}/lib:${JRE_HOME}/lib  
#CLASSPATH=${CLASSPATH}":"`find ${HADOOP_HOME}/share/hadoop | awk '{path=path":"$0}END{print path}'` 
#CLASSPATH=${CLASSPATH}:/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml
CLASSPATH=`hadoop classpath --glob`
CLASSPATH=$CLASSPATH:/etc/hadoop/conf/core-site.xml:/etc/hadoop/conf/hdfs-site.xml
export CLASSPATH
   
rm ./write
   
#c++make指令  
g++ write.cpp -I${HADOOP_HOME}/include -L${HADOOP_HOME}/lib/native -lhdfs -Wl,-rpath=${HADOOP_HOME}/lib/native -L${JRE_HOME}/lib/amd64/server -ljvm -Wl,-rpath=${JRE_HOME}/lib/amd64/server -o write  
   
   
#hadoop fs -rm -r -f /tmp/testlibhdfs.txt

#写入的示例指令  
./write 172.16.16.8 /tmp/testlibhdfs.txt "you are a beautiful girl, I love you"
