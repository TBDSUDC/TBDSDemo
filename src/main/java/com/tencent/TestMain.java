package com.tencent;

import com.tencent.conf.ConfigurationManager;
import com.tencent.constant.Constants;
import com.tencent.jdbc.JDBCHelper;

public class TestMain {

	public static void main(String[] args) {
//		int a = 0;
//		if(a==0){
//			System.out.println("调用了");
//		}else{
//			System.out.println("未调用了");
//		}
//		
//		int messageCount = 50;
//    	Random rand = new Random();
//        /**
//         *通过for循环生产数据(测试最多生产100万条)
//         */
//        for (int messageNo = 1; messageNo <= messageCount && messageNo <= 1000000; messageNo++) {
//        	
//        	DecimalFormat df = new DecimalFormat("#.00");
//        	
//        	String value = "BatchId"+rand.nextInt(6) + " " + df.format(rand.nextDouble()*1000);
//        	System.out.println(value);
//        }
		
//		TestMain.updateBatch("batchId1", "10:13.00");
		
		while(true){
			try {
				Thread.sleep(5000);
				System.out.println("currentTime------>"+System.currentTimeMillis());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			
		}
	}
	
 public static void updateBatch(String batchId,String sumStr) {
		
	    String jdbcUrlIP="localhost";
	    String jdbcDB="srig";
	    String	jdbcUserProd="root";
	    String	jdbcPasswordProd="root";
	    
	    ConfigurationManager.setProperty(Constants.JDBC_URL_PROD, "jdbc:mysql://"+jdbcUrlIP+":3306/"+jdbcDB);
	    ConfigurationManager.setProperty(Constants.JDBC_USER_PROD, jdbcUserProd);
	    ConfigurationManager.setProperty(Constants.JDBC_PASSWORD_PROD, jdbcPasswordProd);
	    
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		String querySql = "select count(batchId) from TBDS_TEST where batchId = ? ";
		
		String insertSql = "INSERT INTO TBDS_TEST(sunCount,sunPrice,batchId) VALUES(?,?,?) ";
		
		String updateSql = "UPDATE TBDS_TEST t set t.sunCount = t.sunCount+?, t.sunPrice = t.sunPrice+? where batchId = ?";
		
		Object[] obj = new Object[]{sumStr.split(":")[0],sumStr.split(":")[1],batchId};
		
		int count = jdbcHelper.executeQueryCount(querySql, new Object[]{batchId});
		System.out.println(count);
		if(count > 0) { 
			//TODO 如果相应批次的数据已经存在，则执行修改，sunCount = 原来的值+现在统计的值，sunPrice=原来的值+现在统计的值
			jdbcHelper.executeUpdate(updateSql, obj);
		} else {
			jdbcHelper.executeUpdate(insertSql, obj);
		}
	}
}
