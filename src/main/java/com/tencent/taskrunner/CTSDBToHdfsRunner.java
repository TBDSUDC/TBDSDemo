package com.tencent.taskrunner;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;

import org.apache.commons.lang.StringUtils;

import com.tencent.export.ExportDataFromTimeSeriesDB;
import com.tencent.teg.dc.lhotse.newrunner.AbstractTaskRunner;
import com.tencent.teg.dc.lhotse.proto.LhotseObject.LState;
import com.tencent.teg.dc.lhotse.runner.util.CommonUtils;

/**
 * 时序数据库数据导入hdfs 
 * @author lenovo
 */
public class CTSDBToHdfsRunner extends AbstractTaskRunner{

	public CTSDBToHdfsRunner(String configFileName) {
		super(configFileName);
	}

    public static void main(String[] args) {
    	
        String configureXml = "";
        if(args == null ||args.length < 1){
            System.out.println("需要配置文件作为输入参数!");
            System.exit(2);
        }else{
        	configureXml = args[0];
        }

        //configureXml 自动生成，不用传入
        CTSDBToHdfsRunner runner = new CTSDBToHdfsRunner(configureXml);
        runner.startWork();
    }
	    
	@Override
	public void execute() throws IOException {
		
		boolean success = false;
		
		try {
            taskRuntime = this.getTask();
            
            Map<String, String> params = taskRuntime.getProperties();
            ExportDataFromTimeSeriesDB.ctsdbToHdfsTask(params);
            
            for (String key : params.keySet()){  
            	this.writeLocalLog(Level.INFO, "get "+key+"=" + params.get(key));
            }
            
            ExportDataFromTimeSeriesDB.ctsdbToHdfsTask(params);
            
            success = true;
        } catch (Exception e) {
            this.writeLocalLog(Level.SEVERE ,"执行ctsdb to hdfs runner 出现异常");

            String st = CommonUtils.stackTraceToString(e);
            this.writeLocalLog(Level.SEVERE, "Exception stackTrace: " + st);

            commitTaskAndLog(LState.RUNNING, "", "Exception: " + e.getMessage());
            throw new IOException(e);
        }finally {
            if (!success) {
                commitTaskAndLog(LState.FAILED, "", "failed");
            } else {
                commitTaskAndLog(LState.SUCCESSFUL, "", "success execute");

            }
        }
		
	}

	@Override
	public void kill() throws IOException {
		
        this.writeLocalLog(Level.INFO, "ctsdb To hdfs Runner had been kill ");
        boolean killResult = false;
        try {
            killResult = CommonUtils.killProcess(this.taskRuntime, this);
            if (killResult) {
                this.writeLocalLog(Level.SEVERE, "kill job succeed!");
                this.commitTask(LState.KILLED, "", "kill job succeed!");
            } else {
                this.writeLocalLog(Level.SEVERE, "kill job failed!");
                this.commitTask(LState.HANGED, "", "kill job failed!");
            }
        } catch (Exception e) {
            this.writeLocalLog(Level.SEVERE,
                    "kill job failed:" + CommonUtils.stackTraceToString(e));
            this.commitTask(LState.HANGED, "", "kill job failed!");
        }
	}
	
    private void commitTaskAndLog(LState state, String runtimeId, String desc) {
        try {
            if (desc != null && desc.length() > 4000) {
                desc = StringUtils.substring(desc, 0, 4000);
            }
            this.commitTask(state, runtimeId, desc);
        }catch (Exception e) {
            String st = CommonUtils.stackTraceToString(e);
            this.writeLocalLog(Level.INFO, "Log_desc :" + desc);
            this.writeLocalLog(Level.SEVERE, "Commit task failed, StackTrace: " + st);
        }
    }
}
