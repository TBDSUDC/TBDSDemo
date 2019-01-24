package com.hbasetest.mrbulkload;

/**
 * Read DataSource from hdfs & Gemerator HFile.
 * 
 * @author mikealzhou
 * Created by 22/10/2018
 * 
 * 数据源如下所示：
 * rowkey1,avvav,131414
 * rowKey2,wvgwb,214141
 * rowKey3,afwbe,242523
 * 第1列表示rowkey,第2列表示列族cf1的col1的值,第3列表示列族cf1的col2的值.如果想要插入多个列族/多个列的值,方法类似.
 * 
 */

import java.net.URI;  
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.security.UserGroupInformation;


public class HbaseMRBulkloadTest {
    static class HFileImportMapper2 extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        
        protected final String CF_KQ = "cf1";
        protected final String COL1 = "col1";
        protected final String COL2 = "col2";


        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] datas = line.split(",");
            String row = datas[0];
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(row));
            KeyValue kv1 = new KeyValue(Bytes.toBytes(row), this.CF_KQ.getBytes(), this.COL1.getBytes(), datas[1].getBytes());
            KeyValue kv2 = new KeyValue(Bytes.toBytes(row), this.CF_KQ.getBytes(), this.COL2.getBytes(), datas[2].getBytes());
            context.write(rowkey, kv1);   //put rowkey,'cf1:col1',value;  then,  rowkey=datas[0],value=datas[1]
            context.write(rowkey, kv2);   //put rowkey,'cf1:col2',value;  then,  rowkey=datas[0],value=datas[2]
        }
    }

    public static void main(String[] args) {
        if(args.length !=3 ){
            System.err.println("Usage: hadoop jar <thisjarfile> com.hbasetest.mrbulkload.HbaseMRBulkloadTest <hdfssourfile> <hdfsoutputfile> <hbase_table_name>");
            System.exit(1);
        }
        
        String input = args[0];   //原始数据输入目录
        String output = args[1];  //数据输出目录，也就是HFile文件目录               
        String tableName = args[2];
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

        System.out.println("table : " + tableName);
        HTable table;
        try {
            try {
                FileSystem fs = FileSystem.get(URI.create(output), conf);
                fs.delete(new Path(output), true);
                fs.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            Connection conn = ConnectionFactory.createConnection(conf);
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Job job = Job.getInstance(conf);
            job.setJobName("Generate HFile");

            job.setJarByClass(HbaseMRBulkloadTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(HFileImportMapper2.class);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, new Path(output));

            HFileOutputFormat2.configureIncrementalLoad(job, table);
            try {
                job.waitForCompletion(true);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}