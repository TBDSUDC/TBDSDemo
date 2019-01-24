package com.hivetest.hivejdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcTest {  
    private static final String driveName = "org.apache.hive.jdbc.HiveDriver";
    private static Connection conn=null; 
	
    public static void main(String args[]){
        if (args.length != 5) {
            System.err.println("Usage: hadoop jar <progressjar> com.hivetest.hivejdbc.HiveJdbcTest <hiveserver2-ip> <dbname> <username> <password> <sqlstring>");
            System.exit(1);
        }

        String hiveserver2 = args[0];
        String dbname = args[1];
        String username = args[2];
        String password = args[3];
        String sqlstring = args[4];

        String url = "jdbc:hive2://"+hiveserver2+":10000/"+dbname;
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
                System.out.println(rs.getString(1)+"     "+rs.getInt(2)+"     "+rs.getString(3)+"     "+rs.getFloat(4));
            }  
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
}