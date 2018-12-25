package com.tencent.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tencent.conf.ConfigurationManager;

public class ExportDataFromTimeSeriesDB {
	 /**
     * 向指定 URL 发送POST方法的请求
     * @param url
     * 发送请求的 URL
     * @param param
     * 请求参数，'{"docvalue_fields": ["0002"]}'。
     * @return 所代表远程资源的响应结果
     */
    public static String sendPost(String url, String param,String user,String password) {

        PrintWriter out = null;
        BufferedReader in = null;
        StringBuffer jsonString = new StringBuffer();
        try {
            URL realUrl = new URL(url);
            //打开和URL之间的连接
            HttpURLConnection conn = (HttpURLConnection)realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setRequestProperty("user-agent","Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");

            String input = user + ":" + password;
            String encoding = new sun.misc.BASE64Encoder().encode(input.getBytes());
            conn.setRequestProperty("Authorization", "Basic " + encoding);
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setRequestMethod("POST");
            //获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            //flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
            	jsonString.append(line);
            }
        } catch (Exception e) {
            System.out.println("------------------------------发送 POST,请求出现异常！");
            e.printStackTrace();
        } finally{ //使用finally块来关闭输出流、输入流
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return jsonString.toString();
    }
    
    public static void printDataToDisk(List<FieldsModel> result,String dataInput){
    	
    	PrintWriter w = null;
    	FileOutputStream out = null;
		try {
//			w = new PrintWriter("/root/kafkatest/db_data.txt");
			out = new FileOutputStream(dataInput,true);
			w = new PrintWriter(out);
	    	for(FieldsModel line:result){
	    		w.println(line.getFields());
	    	}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(w!=null){
				w.close();
			}
			if(out!=null){
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
    }
    /*
     * 生成文件路径
     */
    public static String  createCtsdbDataFile(String dataInput,int fileNum,String batchId){
    	String filePath = "";
    	try {
	    	File file=new File(dataInput);
	    	if(!file.exists()){//如果文件夹不存在
				file.mkdirs();//创建文件夹
	    	}
	    	filePath = dataInput+"/ctsdb_"+batchId+"_"+fileNum+".txt";
	    	File f = new File(filePath);
	    	if(!f.exists()){//如果文件夹不存在
	    		f.createNewFile();
	    	}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    	return filePath;
    }
    public static void main(String[] args) {
    			
    	String url = ConfigurationManager.getProperty("ctsdb.url");
    	String dataInput = ConfigurationManager.getProperty("ctsdb.dataInput");
    	String hdfsOutput = ConfigurationManager.getProperty("ctsdb.hdfsOutput");
    	String tableName = ConfigurationManager.getProperty("ctsdb.tableName");
    	String colNames = ConfigurationManager.getProperty("ctsdb.colNames");
    	//int类型，多少条数据放一个文件 n*60000
    	Integer fileDataSum = ConfigurationManager.getInteger("ctsdb.fileDataSum");
    	
    	String validDataTime = ConfigurationManager.getProperty("ctsdb.validDataTime");
    	
        String user = ConfigurationManager.getProperty("ctsdb.user");
        String password = ConfigurationManager.getProperty("ctsdb.password");
        
    	
    	if(colNames != null && colNames.trim().length() > 0){
    		colNames = colNames.trim().replaceAll(",", "\",\"");
    		
    		colNames = "\""+colNames+"\"";
    	}
    	
    	long startTime = System.currentTimeMillis();
//    	System.out.println("--------------------------------startTime:"+startTime);
    	long sumCount = 0;
    	int fileNum = 0;
    	int sumBatchNum = 0;
    	
    	String filePath = "";
    	//生成批次ID
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");//要转换的时间格式
    	Date date = null;
    	String batchId =null;
		try {
			 date = sdf.parse(sdf.format(System.currentTimeMillis()));
			 batchId = sdf.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
    	
    	//docvalue_fields 查询表的字段
    	String jsonParam = "{\"size\":60000,\"docvalue_fields\": ["+colNames+"]}";
        //发送 POST请求,10m 数据10分钟后会过期，过期后 scroll_id将变得无效。ctsdb_tbds
    	String initSql = url+"/"+tableName+"/_search?scroll="+validDataTime;
    	String batchSql = url+"/"+"_search/scroll";
    	
    	String batchParam = null;
    	
        String resultJson=ExportDataFromTimeSeriesDB.sendPost(initSql, jsonParam,user,password);
        
        System.out.println(initSql+"------------"+jsonParam);
        
        JSONObject object = JSON.parseObject(resultJson);
        String _scroll_id = object.getString("_scroll_id");
        
        JSONObject data = (JSONObject) object.get("hits");
        JSONArray jsonArray = data.getJSONArray("hits");
        List<FieldsModel> result = JSON.parseArray(jsonArray.toJSONString(), FieldsModel.class);
        
        if(result!=null  && result.size() > 0){
        	fileNum = 1;
        	filePath = ExportDataFromTimeSeriesDB.createCtsdbDataFile(dataInput, fileNum,batchId);
        	printDataToDisk(result,filePath);
        	sumCount = sumCount+result.size();
        	System.out.println("first sumCount================>"+sumCount);
        }
        while(true){
        	
        	sumBatchNum = sumBatchNum+1;
        	//测试用
        	Integer runBatchSum = ConfigurationManager.getInteger("ctsdb.runBatchSum");
        	if(runBatchSum != null && runBatchSum > 0){
        		if((sumBatchNum+1) > runBatchSum) break;
        	}
        	
        	if(sumBatchNum > 0 && sumBatchNum % fileDataSum == 0){
        		fileNum = fileNum+1;
        		filePath = ExportDataFromTimeSeriesDB.createCtsdbDataFile(dataInput, fileNum,batchId);
        	}
        	
        	batchParam = "{\"scroll\" : \""+validDataTime+"\", \"scroll_id\" : \""+_scroll_id+"\"}";
        	String batchResultJson=ExportDataFromTimeSeriesDB.sendPost(batchSql, batchParam,user,password);
        	
        	JSONObject batchObject = JSON.parseObject(batchResultJson);
        	_scroll_id = batchObject.getString("_scroll_id");
        	
            JSONObject batchdata = (JSONObject) batchObject.get("hits");
            JSONArray batchJsonArray = batchdata.getJSONArray("hits");
            List<FieldsModel> batchResult = JSON.parseArray(batchJsonArray.toJSONString(), FieldsModel.class);
            
            if(batchResult!=null && batchResult.size() > 0){
            	sumCount = sumCount+batchResult.size();
            	printDataToDisk(batchResult,filePath);
            	System.out.println("sumCount================>"+sumCount);
            }else{
            	break;
            }
        }
        System.out.println("--------------------------------共运行:【"+(System.currentTimeMillis()-startTime)+"】毫秒");
        
        //将生成的所有文件put到hdfs里面
        syncDataToHdfs(hdfsOutput, dataInput, batchId);
        
    } 
    public static void syncDataToHdfs(String hdfsOutput,String dataInput,String batchId){
    	
	    FileSystem fs = null ;
	    try {
	        Configuration conf = new Configuration();
	        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
	
	        String username = ConfigurationManager.getProperty("hadoop_security_authentication_tbds_username");
	        if(username != null && username.trim().length() > 0){
	        	//加入tbds的认证参数
	            conf.set("hadoop.security.authentication", ConfigurationManager.getProperty("hadoop.security.authentication"));
	            conf.set("hadoop_security_authentication_tbds_username",username);
	            conf.set("hadoop_security_authentication_tbds_secureid",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_secureid"));
	            conf.set("hadoop_security_authentication_tbds_securekey",ConfigurationManager.getProperty("hadoop_security_authentication_tbds_securekey"));
		        UserGroupInformation.setConfiguration(conf);
		        UserGroupInformation.loginUserFromSubject(null);
	        }
	        //拿到一个文件系统操作的客户端实例对象
	        fs = FileSystem.get(conf) ;
	        
	        File file=new File(dataInput);
	        File[] tempList = file.listFiles();
	        for (int i = 0; i < tempList.length; i++) {
		         if (tempList[i].isFile() && tempList[i].getPath().contains(batchId)) {
		        	 Path src = new Path(tempList[i].getPath());
		 			 // 要上传到hdfs的目标路径
		 			 Path dst = new Path(hdfsOutput);
		 			 fs.copyFromLocalFile(src, dst);
		         }
	         }
			fs.close();
	    } catch (Exception e){
	    	System.out.println("------------------------------数据文件上传到hdfs异常！");
	        e.printStackTrace();
	    }
    }
    /**
     * 自定义工作流调用方法
     * @param args
     */
    public static void ctsdbToHdfsTask(Map<String, String> params) throws Exception{
		
    	String url = params.get("ctsdb.url");
    	String dataInput = params.get("ctsdb.dataInput");
    	String hdfsOutput = params.get("ctsdb.hdfsOutput");
    	String tableName = params.get("ctsdb.tableName");
    	String colNames = params.get("ctsdb.colNames");
    	//int类型，多少条数据放一个文件 n*60000
    	Integer fileDataSum = 0;
    	//测试用
    	Integer runBatchSum = 0;
    	
		try {
			fileDataSum = Integer.valueOf(params.get("ctsdb.fileDataSum"));
			runBatchSum = Integer.valueOf(params.get("ctsdb.runBatchSum"));
			//至少一个批次 存放一个文件
			if(fileDataSum < 1) {
				fileDataSum = 1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
		
    	String validDataTime = params.get("ctsdb.validDataTime");
    	
        String user = params.get("ctsdb.user");
        String password = params.get("ctsdb.password");
        
    	if(colNames != null && colNames.trim().length() > 0){
    		colNames = colNames.trim().replaceAll(",", "\",\"");
    		
    		colNames = "\""+colNames+"\"";
    	}
    	
    	long startTime = System.currentTimeMillis();
//    	System.out.println("--------------------------------startTime:"+startTime);
    	long sumCount = 0;
    	int fileNum = 0;
    	int sumBatchNum = 0;
    	
    	String filePath = "";
    	//生成批次ID
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");//要转换的时间格式
    	Date date = null;
    	String batchId =null;
		try {
			 date = sdf.parse(sdf.format(System.currentTimeMillis()));
			 batchId = sdf.format(date);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
    	
    	//docvalue_fields 查询表的字段
    	String jsonParam = "{\"size\":60000,\"docvalue_fields\": ["+colNames+"]}";
        //发送 POST请求,10m 数据10分钟后会过期，过期后 scroll_id将变得无效。ctsdb_tbds
    	String initSql = url+"/"+tableName+"/_search?scroll="+validDataTime;
    	String batchSql = url+"/"+"_search/scroll";
    	
    	String batchParam = null;
    	
        String resultJson=ExportDataFromTimeSeriesDB.sendPost(initSql, jsonParam,user,password);
        
//        System.out.println(initSql+"------------"+jsonParam);
        
        JSONObject object = JSON.parseObject(resultJson);
        String _scroll_id = object.getString("_scroll_id");
        
        JSONObject data = (JSONObject) object.get("hits");
        JSONArray jsonArray = data.getJSONArray("hits");
        List<FieldsModel> result = JSON.parseArray(jsonArray.toJSONString(), FieldsModel.class);
        
        if(result!=null  && result.size() > 0){
        	fileNum = 1;
        	filePath = ExportDataFromTimeSeriesDB.createCtsdbDataFile(dataInput, fileNum,batchId);
        	printDataToDisk(result,filePath);
        	sumCount = sumCount+result.size();
        	System.out.println("first sumCount================>"+sumCount);
        }
        while(true){
        	
        	sumBatchNum = sumBatchNum+1;

        	if(runBatchSum != null && runBatchSum > 0){
        		if((sumBatchNum+1) > runBatchSum) break;
        	}
        	
        	if(sumBatchNum > 0 && sumBatchNum % fileDataSum == 0){
        		fileNum = fileNum+1;
        		filePath = ExportDataFromTimeSeriesDB.createCtsdbDataFile(dataInput, fileNum,batchId);
        	}
        	
        	batchParam = "{\"scroll\" : \""+validDataTime+"\", \"scroll_id\" : \""+_scroll_id+"\"}";
        	String batchResultJson=ExportDataFromTimeSeriesDB.sendPost(batchSql, batchParam,user,password);
        	
        	JSONObject batchObject = JSON.parseObject(batchResultJson);
        	_scroll_id = batchObject.getString("_scroll_id");
        	
            JSONObject batchdata = (JSONObject) batchObject.get("hits");
            JSONArray batchJsonArray = batchdata.getJSONArray("hits");
            List<FieldsModel> batchResult = JSON.parseArray(batchJsonArray.toJSONString(), FieldsModel.class);
            
            if(batchResult!=null && batchResult.size() > 0){
            	sumCount = sumCount+batchResult.size();
            	printDataToDisk(batchResult,filePath);
            	System.out.println("sumCount================>"+sumCount);
            }else{
            	break;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("--------------------------------生成临时文件共运行:【"+(System.currentTimeMillis()-startTime)+"】毫秒");
        
        //将生成的所有文件put到hdfs里面
        syncDataToHdfsForCtsdb(hdfsOutput, dataInput, batchId,params);
        
        System.out.println("--------------------------------上传到hdfs共运行:【"+(System.currentTimeMillis()-endTime)+"】毫秒");

    } 
    /**
     * 将生成的所有文件put到hdfs里面
     * @param hdfsOutput
     * @param dataInput
     * @param batchId
     * @param params
     */
    public static void syncDataToHdfsForCtsdb(String hdfsOutput,String dataInput,String batchId,Map<String, String> params) throws Exception{
    	
	    FileSystem fs = null ;
	    try {
	        Configuration conf = new Configuration();
	        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
	        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
	
	        String tbds_username = params.get("tbds_username");
	        String tbds_secureid = params.get("tbds_secureid");
	        String tbds_securekey = params.get("tbds_securekey");
	        if(tbds_username != null && tbds_username.trim().length() > 0){
	        	//加入tbds的认证参数
	            conf.set("hadoop.security.authentication", "tbds");
	            conf.set("hadoop_security_authentication_tbds_username",tbds_username);
	            conf.set("hadoop_security_authentication_tbds_secureid",tbds_secureid);
	            conf.set("hadoop_security_authentication_tbds_securekey",tbds_securekey);
		        UserGroupInformation.setConfiguration(conf);
		        UserGroupInformation.loginUserFromSubject(null);
	        }
	        //拿到一个文件系统操作的客户端实例对象
	        fs = FileSystem.get(conf) ;
	        
	        File file=new File(dataInput);
	        File[] tempList = file.listFiles();
	        for (int i = 0; i < tempList.length; i++) {
		         if (tempList[i].isFile() && tempList[i].getPath().contains(batchId)) {
		        	 Path src = new Path(tempList[i].getPath());
		 			 // 要上传到hdfs的目标路径
		 			 Path dst = new Path(hdfsOutput);
		 			 fs.copyFromLocalFile(src, dst);
		         }
	         }
			fs.close();
	    } catch (Exception e){
	    	System.out.println("------------------------------数据文件上传到hdfs异常！");
	        e.printStackTrace();
	        throw new Exception(e);
	    }
    }
}
