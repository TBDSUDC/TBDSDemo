package com.hadooptest.hdfslist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.ArrayList;
import java.util.List;

public class HdfsAccess {

    public static void main(String[] args) {
        FileSystem fs = null ;
        try {
            Configuration conf = new Configuration();
            if(args.length != 3) {
                System.err.println("Usage: <secureid> <username> <securekey>");
                System.exit(1);
            }
            
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

            conf.set("hadoop.security.authentication","tbds");
            conf.set("hadoop_security_authentication_tbds_secureid",args[0]);
            conf.set("hadoop_security_authentication_tbds_username",args[1]);
            conf.set("hadoop_security_authentication_tbds_securekey",args[2]);
            
            UserGroupInformation.setConfiguration( conf );
            UserGroupInformation.loginUserFromSubject(null);

            fs = FileSystem.get(conf) ;

            Path basePath = new Path("/");
            FileStatus[] fileStats = fs.listStatus(basePath) ;

            List<String> fileList = new ArrayList<String>() ;

            if (fileStats != null) {
                for( FileStatus fst : fileStats ){
                    System.out.println( fst.toString() );
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}