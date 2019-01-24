package com.hbasetest.hbaseopenmaven; 

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class Testhtable {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        if(args.length != 4) {
            System.err.println("Usage: hadoop jar <thisjarfile> com.hbasetest.hbaseopenmaven.Testhtable <zkhost> <zkznode> <hbasetablename> <rownumber>");
            System.exit(4);
        }
        String zkhost=args[0];
        String znodeparent=args[1];
        String hbasetablename=args[2];
        int rownumber = Integer.parseInt(args[3]);
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkhost);
        hbaseConf.set("zookeeper.znode.parent", znodeparent); 
        //hbaseConf.set("hbase.zookeeper.quorum", "10.166.113.63,10.166.113.64,10.166.113.65");
        //hbaseConf.set("hbase.zookeeper.quorum", "10.141.27.43,10.141.51.189,10.141.62.99");
        //hbaseConf.set("hbase.zookeeper.property.clientPort", "2181,2181,2181");
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        HTableDescriptor htableDescriptor = new HTableDescriptor(hbasetablename.getBytes());  //set the name of table
        htableDescriptor.addFamily(new HColumnDescriptor("families1")); //set the name of column families
        htableDescriptor.addFamily(new HColumnDescriptor("families2"));
        admin.createTable(htableDescriptor); //create a table 
        HTable table = new HTable(hbaseConf, hbasetablename); //get instance of table.
        for (int i = 0; i < rownumber; i++) {   //for is number of rows
            Put putRow = new Put(("rowkey_" + i).getBytes()); //the ith rowkey
            putRow.add("families1".getBytes(), "colume1".getBytes(), ("value-" + i + ":: this is the first-colume values of families1").getBytes()); //set the name of column and value.
            putRow.add("families1".getBytes(), "colume2".getBytes(), ("value-" + i + ":: this is the second-colume values of families1").getBytes());
            putRow.add("families1".getBytes(), "colume3".getBytes(), ("value-" + i + ":: this is the third-colume values of families1").getBytes());
            putRow.add("families2".getBytes(), "colume1".getBytes(), ("value-" + i + ":: this is the first-colume values of families2").getBytes());
            putRow.add("families2".getBytes(), "colume2".getBytes(), ("value-" + i + ":: this is the second-colume values of families2").getBytes());
            putRow.add("families2".getBytes(), "colume3".getBytes(), ("value-" + i + ":: this is the third-colume values of families2").getBytes());
            table.put(putRow);
        }

        String [] colfamlies = new String [2];
        colfamlies[0] = "families1";
        colfamlies[1] = "families2";
        for (int j = 0; j < 2; j++) {
            for(Result result: table.getScanner(colfamlies[j].getBytes())){//get data of column clusters 
                for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap(colfamlies[j].getBytes()).entrySet()){//get collection of result
                    String column = new String(entry.getKey());
                    String value = new String(entry.getValue());
                    System.out.println(colfamlies[j]+"--"+column+"::"+value);
                }
            }
        }
        //admin.disableTable(hbasetablename.getBytes()); //disable the table
        //admin.deleteTable(hbasetablename.getBytes());  //drop the tbale
    }
}