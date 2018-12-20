package com.tencent.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import com.tencent.conf.ConfigurationManager;
/**
 * 客户端去操作hdfs时
 * @author lenovo
 */
public class HadoopHdfsDemo{

    public static void main(String[] args) {

        FileSystem fs = null ;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

            if(args != null && args.length == 4) {
                conf.set("hadoop.security.authentication","tbds");
                conf.set("hadoop_security_authentication_tbds_secureid",args[0]);
                conf.set("hadoop_security_authentication_tbds_username",args[1]);
                conf.set("hadoop_security_authentication_tbds_securekey",args[2]);
            }else{
            	//加入tbds的认证参数
	            conf.set("hadoop.security.authentication", ConfigurationManager.getProperty("hadoop.security.authentication"));
	            conf.set("hadoop_security_authentication_tbds_username",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_username"));
	            conf.set("hadoop_security_authentication_tbds_secureid",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_secureid"));
	            conf.set("hadoop_security_authentication_tbds_securekey",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_securekey"));
            }
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromSubject(null);

            //拿到一个文件系统操作的客户端实例对象
            fs = FileSystem.get(conf) ;
            
            
            Path basePath = new Path(args[3]);
            
//          FileStatus[] listStatus = fs.listStatus(basePath);
//    		for (FileStatus fileStatus : listStatus) {
//    			System.out.println(fileStatus.getPath()+"------》》》"+fileStatus.toString());
//    		}
//    		
    		//找到对应目录下的所有的文件
    		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(basePath, false);  //false 不递归查找
    		while(listFiles.hasNext()){
    			LocatedFileStatus next = listFiles.next();
    			String name = next.getPath().getName();
    			Path path = next.getPath();
    			System.out.println(name + "------>>>" + path.toString());
    		}
    		
        } catch (Exception e){
            e.printStackTrace();
        }
    }
	
}