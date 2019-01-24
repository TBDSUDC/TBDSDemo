package com.hadooptest.hdfsmrtesttbdsauthparra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.security.UserGroupInformation;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 5) {
            System.err.println("Usage: wordcount <infile> <outfile> <secureid> <username> <securekey>");
            System.exit(2);
        }
        
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        conf.set("hadoop.security.authentication","tbds");
        conf.set("hadoop_security_authentication_tbds_secureid",args[2]);
        conf.set("hadoop_security_authentication_tbds_username",args[3]);
        conf.set("hadoop_security_authentication_tbds_securekey",args[4]);
        
        UserGroupInformation.setConfiguration( conf );
        UserGroupInformation.loginUserFromSubject(null);

        Job job = new Job(conf, "wordcountnew");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    } 
}