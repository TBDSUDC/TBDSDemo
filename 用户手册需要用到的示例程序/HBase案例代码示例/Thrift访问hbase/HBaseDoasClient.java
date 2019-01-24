package com.tencent.bigdata.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;

public class HBaseDoasClient {
  public static Connection createconnection(String secureId, String secureKey, String zk) {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", zk);
    configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

    // 认证时，在创建HBase连接之前，在configuration设置以下两个参数，其他使用方法和HBase开源完全一样,hbase其他接口的使用方法类似，只需要增加以下两个认证参数即可
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
  
  public static void main(String[] args) throws IOException {
    if (args == null || args.length != 5) {
      System.out
          .println("Usage: doasUser loginUser seucreId secureKey zkInfo(eg. zkIp1:port,zkIp2:port,zkIp3:port)");
      System.exit(1);
    }

    // 想以什么身份跑任务的用户，自己不用登录认证，自己的身份由loginUser保证真实性，由loginUser代理
    String doasUser = args[0];
    // 真正要进行认证的用户，跑任务时，不是以该用户的身份。 该用户要保障doasUser身份的真实性。整个认证的逻辑是：HBase系统对loginUser进行认证,
    // 而loginUser对doasUser进行认证
    String loginUser = args[1];

    UserGroupInformation ugi =
        UserGroupInformation.createProxyUser(doasUser,
            UserGroupInformation.createRemoteUser(loginUser));

    String secureId = args[2];
    String secureKey = args[3];
    String zk = args[4];

    ugi.doAs(new PrivilegedAction<Boolean>() {

      @SuppressWarnings("deprecation")
      @Override
      public Boolean run() {
        Connection connection = null;
        HBaseAdmin hbaseAdmin = null;
        try {
          connection = createconnection(secureId, secureKey, zk);
          if (connection == null) {
            return false;
          }

          hbaseAdmin = new HBaseAdmin(connection);
          String[] tableNames = hbaseAdmin.getTableNames();

          System.out.println("Scanning tables...");
          for (String tableName : tableNames) {
            System.out.println("  found: " + tableName);
          }

          return true;
        } catch (Exception e) {
          e.printStackTrace();
          return false;
        } finally {
          close(hbaseAdmin);
          close(connection);
        }
      }
    });
  }
}
