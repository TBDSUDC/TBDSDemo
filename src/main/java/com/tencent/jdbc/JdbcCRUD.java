package com.tencent.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * JDBC增删改查示范类
 * @author Administrator
 */
public class JdbcCRUD {
	/**
	 * 执行增删改SQL语句
	 * @param sql 
	 * @param params
	 * @return 影响的行数
	 */
	public static int executeUpdate(String url,String user,String password,String sql, Object[] params) {
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			
			conn = DriverManager.getConnection(
					url, 
					user, 
					password); 
			
			conn.setAutoCommit(false);  
			
			pstmt = conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);  
				}
			}
			
			rtn = pstmt.executeUpdate();
			
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			try {
				if(pstmt != null) {
					pstmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		
		return rtn;
	}
	
	/**
	 * 执行查询SQL语句
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public static int executeQueryCount(String url,String user,String password,String sql, Object[] params) {
		
		int count = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");  
			conn = DriverManager.getConnection(url,user,password);  
			
			pstmt = conn.prepareStatement(sql);
			
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);   
				}
			}
			
			rs = pstmt.executeQuery();
			
			if(rs.next()) {
				count = rs.getInt(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(pstmt != null) {
					pstmt.close();
				} 
				if(conn != null) {
					conn.close();  
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
		return count;
	}
	
	
}
