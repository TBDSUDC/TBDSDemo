package com.hbasetest.mrbulkloadtohbsae;

/**
 * Read DataSource from hdfs & Gemerator HFile, then bulkload HFile to HBase
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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HbaseMRHFile_Bulkload_to_HBase {
    
    public static class LoadMapper extends Mapper<Object,Text,ImmutableBytesWritable,Put>{
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if(values.length == 3){
                byte[] rowkey = Bytes.toBytes(values[0]);
                byte[] cf1_col1_value = Bytes.toBytes(values[1]);
                byte[] cf1_col2_value = Bytes.toBytes(values[2]);
                byte[] familly = Bytes.toBytes("cf1");
                byte[] column1 = Bytes.toBytes("col1");
                byte[] column2 = Bytes.toBytes("col2");
                ImmutableBytesWritable rowkeyWritable = new ImmutableBytesWritable(rowkey);
                Put testput = new Put(rowkey);
                testput.add(familly,column1,cf1_col1_value);
                testput.add(familly,column2,cf1_col2_value);
                context.write(rowkeyWritable, testput);    
            }        
            
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length !=3 ){
            System.out.println("Usage:hadoop jar <thisjarfile> com.hbasetest.com.hbasetest.mrbulkloadtohbsae.HbaseMRHFile_Bulkload_to_HBase <inputfile> <outputfile> <hbasetablename>");
            System.exit(1);
        }
        
        String in = args[0];   //原始数据输入目录
        String out = args[1];  //数据输出目录，也就是HFile文件目录
        //int unitmb =Integer.valueOf(args[2]);                
        String hbasetablename = args[2];
        
        Configuration hadoopconf = new Configuration();                
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(unitmb * 1024 * 1024));
        //conf.set("mapred.min.split.size", String.valueOf(unitmb * 1024 * 1024));
        //conf.set("mapreduce.input.fileinputformat.split.minsize.per.node", String.valueOf(unitmb * 1024 * 1024));
        //conf.set("mapreduce.input.fileinputformat.split.minsize.per.rack", String.valueOf(unitmb * 1024 * 1024));

        //以下变量需要替换成你使用的集群的相关信息
        hadoopconf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        hadoopconf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        hadoopconf.set("hadoop.security.authentication","tbds");
        hadoopconf.set("hadoop_security_authentication_tbds_secureid","wPl9zgEpT4pJlxKbv2mHrpJEPt9q3q56yFnp");
        hadoopconf.set("hadoop_security_authentication_tbds_username","testuser");
        hadoopconf.set("hadoop_security_authentication_tbds_securekey","LluWnYeM2qtaxqEwzGXwFftvCvNJ1g4F");
                
        Job job = new Job(hadoopconf);        
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));            
        job.setMapperClass(LoadMapper.class); 
        job.setReducerClass(PutSortReducer.class);     
        job.setOutputFormatClass(HFileOutputFormat2.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);        
        job.setJarByClass(HbaseMRHFile_Bulkload_to_HBase.class);
        
        Configuration hbaseconf = HBaseConfiguration.create();
        //以下变量需要替换成你使用的集群的相关信息
        String zkhost="172.16.32.13:2181,172.16.32.17:2181,172.16.32.28:2181";
        String znodeparent="/hbase-unsecure";

        hbaseconf.set("hbase.zookeeper.quorum", zkhost);
        hbaseconf.set("zookeeper.znode.parent", znodeparent); 
        hbaseconf.set("hbase.security.authentication.tbds.secureid", "yNBw194FU1FlbeRhtqMVayn3ENamnT3nrXPl");
        hbaseconf.set("hbase.security.authentication.tbds.securekey", "OG9klQodEn9jH1vXWwhBOvdmLpTJCw5D");

        HTable table = new HTable(hbaseconf,hbasetablename);
        HFileOutputFormat2.configureIncrementalLoad(job, table);     
        
        job.waitForCompletion(true);   
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseconf);
        loader.doBulkLoad(new Path(out), table);

    }

}