package com.hbasetest.hbasetbdsmaven; 

import java.io.IOException;
import java.util.Map;
import java.util.ArrayList;  
import java.util.List;  
import java.util.Date;
import java.text.SimpleDateFormat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.security.UserGroupInformation;

// 为云南省厅做的测试demo

public class Testhtable {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        if(args.length != 2) {
            System.err.println("Usage: hadoop jar <thisjarfile> com.hbasetest.hbaseopenmaven.Testhtable <steplen_rownumber> <rownumber>");
            System.exit(4);
        }
        
        int steplen_rownumber = Integer.parseInt(args[0]);
        int rownumber = Integer.parseInt(args[1]);
        String zkhost="10.166.114.22:2181,10.166.114.33:2181,10.166.114.44:2181";
        String znodeparent="/hbase-unsecure";
        String hbasetablename="fk_graphs:zdk_test1";
        String hbase_secureid="eXRaYMFW21tY3ymQMbL6cQc97HLUdOSetnnc";
        String hbase_securekey="1SE0zUSrgZ3KY7WPH04rfCkJ2aZYRjKE";
        String hadoop_secureid="JOxYmGQODiBqoGFvMK15OJWAQ8qMoMRezDNV";
        String hadoop_securekey="oGKEZWt5JEil2iQC1Ac25YsN9p9aAvwC";
        String username="zdk_user";

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        hbaseConf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        hbaseConf.set("hadoop.security.authentication","tbds");
        hbaseConf.set("hadoop_security_authentication_tbds_secureid",hadoop_secureid);
        hbaseConf.set("hadoop_security_authentication_tbds_username",username);
        hbaseConf.set("hadoop_security_authentication_tbds_securekey",hadoop_securekey);
        
        hbaseConf.set("hbase.zookeeper.quorum", zkhost);
        hbaseConf.set("zookeeper.znode.parent", znodeparent); 
        hbaseConf.set("hbase.security.authentication.tbds.secureid", hbase_secureid);
        hbaseConf.set("hbase.security.authentication.tbds.securekey", hbase_securekey);
        
        UserGroupInformation.setConfiguration( hbaseConf );
        UserGroupInformation.loginUserFromSubject(null);
        //HBaseAdmin admin = new HBaseAdmin(hbaseConf);
		
        HTable htable = new HTable(hbaseConf, hbasetablename); //get instance of table.
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(5242880);  //根据数据量大小来定,我这里先设置5M,非常小.
        List<Put> listput = new ArrayList<Put>();
        int init_rownumber = steplen_rownumber * rownumber;
        int end_rownumber = init_rownumber + rownumber;
        for (int i = init_rownumber ; i < end_rownumber; i++) {   //for is number of rows
            Put putRow = new Put(("rowkey_" + i).getBytes()); //按照时间设置 rowkey
            putRow.add("personal".getBytes(), "name".getBytes(), ("value-" + i + "_name").getBytes()); //set the name of column and value.
            putRow.add("personal".getBytes(), "password".getBytes(), ("value-" + i + "_password").getBytes());
            listput.add(putRow);
        }
        htable.put(listput);
		htable.close();
    }
}