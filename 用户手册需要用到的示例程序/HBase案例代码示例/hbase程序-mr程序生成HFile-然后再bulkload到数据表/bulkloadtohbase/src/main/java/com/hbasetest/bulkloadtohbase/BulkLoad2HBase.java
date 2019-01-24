package com.hbasetest.bulkloadtohbase;

/**
 * Bulkload HFile to HBase
 * 
 * @author mikealzhou
 *
 *         Created by 22/10/2018
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

 public class BulkLoad2HBase {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Usage:hadoop jar <thisjarfile> com.hbasetest.bulkloadtohbase.BulkLoad2HBase <HFile_hdfs_dir> <hbase_table_name>");
            System.exit(1);
        }
        String HFile_hdfs_dir = args[0];  //HFile文件目录               
        String tableName = args[1];
        Configuration conf = new Configuration();               

       //以下变量需要替换成你使用的集群的相关信息
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        conf.set("hadoop.security.authentication","tbds");
        conf.set("hadoop_security_authentication_tbds_secureid","wPl9zgEpT4pJlxKbv2mHrpJEPt9q3q56yFnp");
        conf.set("hadoop_security_authentication_tbds_username","testuser");
        conf.set("hadoop_security_authentication_tbds_securekey","LluWnYeM2qtaxqEwzGXwFftvCvNJ1g4F");

        String zkhost="172.16.32.13:2181,172.16.32.17:2181,172.16.32.28:2181";
        String znodeparent="/hbase-unsecure";

        conf.set("hbase.zookeeper.quorum", zkhost);
        conf.set("zookeeper.znode.parent", znodeparent); 
        conf.set("hbase.security.authentication.tbds.secureid", "yNBw194FU1FlbeRhtqMVayn3ENamnT3nrXPl");
        conf.set("hbase.security.authentication.tbds.securekey", "OG9klQodEn9jH1vXWwhBOvdmLpTJCw5D");
        
        //UserGroupInformation.setConfiguration( conf );
        //UserGroupInformation.loginUserFromSubject(null);
        HTable table = new HTable(conf, tableName);
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(HFile_hdfs_dir), table);
    }
    
}