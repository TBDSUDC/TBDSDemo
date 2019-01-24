package com.tencent.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by leslizhang on 2017/9/8.
 */
public class HdfsAccess {

    public static void main(String[] args) {
        FileSystem fs = null ;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("D:\\hadoopconf\core-site.xml"));
            conf.addResource(new Path("D:\\hadoopconf\hdfs-site.xml"));

            conf.set("hadoop.security.authentication","tbds");
            conf.set("hadoop_security_authentication_tbds_secureid","JsdH9I97QWn8xVWlXeUyaQdXC2AElAtL5SsL");
            conf.set("hadoop_security_authentication_tbds_username","hdfs");
            conf.set("hadoop_security_authentication_tbds_securekey","MHU9niJhASMDWKE9OfkBpAgW37YwAqpw");

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
