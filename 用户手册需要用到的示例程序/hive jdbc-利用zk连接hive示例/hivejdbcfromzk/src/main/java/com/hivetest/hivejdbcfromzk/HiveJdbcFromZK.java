package com.hivetest.hivejdbcfromzk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcFromZK {  
    private static final String driveName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection conn=null; 
	
    public static void main(String args[]){
        if (args.length != 5) {
            System.err.println("Usage: need five parameters: com.hivetest.hivejdbcfromzk.HiveJdbcFromZK <zk-ip:port> <dbname> <username> <password> <sqlstring>");
            System.exit(1);
        }

        String zkipport = args[0];
        String dbname = args[1];
        String username = args[2];
        String password = args[3];
        String sqlstring = args[4];

        String url = "jdbc:hive2://"+zkipport+"/"+dbname+";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        try {  
            Class.forName(driveName);  
            conn=DriverManager.getConnection(url, username, password);
            Statement st=conn.createStatement();  
            System.out.println("hive connection connect successful! beging to excute sql.");
            //String sqlstring="select name,age,statment,finalscore from testtable limit 10";  
            ResultSet rs=st.executeQuery(sqlstring);  
            System.out.println("excute sql successful! begin to print the result.");
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