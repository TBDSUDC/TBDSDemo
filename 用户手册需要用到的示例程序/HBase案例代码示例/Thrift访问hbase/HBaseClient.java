package com.tencent.bigdata.hbase;
import java.io.Closeable;
import java.io.IOException;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.Connection;  
import org.apache.hadoop.hbase.client.ConnectionFactory;  
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseClient { 
  
  public static Connection createconnection(String userName, String secureId, String secureKey,String zk) {
    Configuration configuration = HBaseConfiguration.create();  
    configuration.set("hbase.zookeeper.quorum", zk);  
    configuration.set("zookeeper.znode.parent", "/hbase-unsecure");  
    
    // 认证时，在创建HBase连接之前，在configuration设置以下两个参数，其他使用方法和HBase开源完全一样,hbase其他接口的使用方法类似，只需要增加以下两个认证参数即可
    conf.set("hadoop.security.authentication","tbds");
    conf.set("hadoop.security.authentication.tbds.username",userName);
    configuration.set("hbase.security.authentication.tbds.secureid", secureId);
    configuration.set("hbase.security.authentication.tbds.securekey", secureKey);  
    
    try {  
        return ConnectionFactory.createConnection(configuration);  
    } catch (IOException e) {  
        e.printStackTrace(); 
        return null;
    }
  }  
  
  public static void close(Closeable closeable){
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {e.printStackTrace();}
    }
  }

  @SuppressWarnings("deprecation")
  public static void main(String[] args) throws Exception {  
      if (args == null || args.length != 4) {
        System.out.println("Usage: userName seucreId secureKey zkInfo(eg. zkIp1:port,zkIp2:port,zkIp3:port)");
        System.exit(1);
      }
      
      Connection connection = null;
      HBaseAdmin hbaseAdmin = null;
      try {
        connection = createconnection(args[0], args[1], args[2], args[3]);
        if (connection == null) {
          return;
        }
        
        hbaseAdmin = new HBaseAdmin(connection);
        String[] tableNames = hbaseAdmin.getTableNames();
        
        System.out.println("Scanning tables...");
        for (String tableName : tableNames) {
          System.out.println("  found: " + tableName);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }finally{
        close(hbaseAdmin);
        close(connection);
      }
  }  
} 
