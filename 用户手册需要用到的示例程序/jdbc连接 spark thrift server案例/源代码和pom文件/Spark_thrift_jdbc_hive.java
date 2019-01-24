package com.tencent.ynga.sparksqldemo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Spark_thrift_jdbc_hive {
    private static final String driveName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection conn=null; 
	
    public static void main(String args[]){
        if (args.length != 6) {
            System.err.println("Usage: need five parameters: <spark-thriftserver-ip> <spark-thriftserver-port> <dbname> <username> <password> <sqlstring>");
            System.exit(1);
        }

        String spark_thriftserver_ip = args[0];
        String spark_thriftserver_port = args[1];
        String dbname = args[2];
        String username = args[3];
        String password = args[4];
        String sqlstring = args[5];

        // for example: url="jdbc:hive2://10.116.31.243:10008/testdb"
        String url = "jdbc:hive2://"+spark_thriftserver_ip+":"+spark_thriftserver_port+"/"+dbname;

        //the content below is the main code, is similar to connecting to hiveserver2 
        try {  
            Class.forName(driveName);  
            conn=DriverManager.getConnection(url, username, password);
            Statement st=conn.createStatement();  
            System.out.println("######## spark thrift server is connected successfully! beging to excute sql ########");
            //String sqlstring="select name,age,statment,finalscore from testtable limit 10";  
            ResultSet rs=st.executeQuery(sqlstring);  
            System.out.println("######## excute sql successful! begin to print the result ########");
            // attention: the result printed should match the result!
            while(rs.next()){  
                System.out.println("the count(*) result of the table is:");
                System.out.println("###  "+rs.getString(1)+"  ###");
            }
            
            rs.close();
            st.close();
            conn.close();
            
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        } 

    }
}