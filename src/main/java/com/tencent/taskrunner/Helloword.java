package com.tencent.taskrunner;

import java.io.IOException;
import java.util.logging.Level;

import org.apache.commons.lang.StringUtils;

import com.tencent.teg.dc.lhotse.newrunner.AbstractTaskRunner;
import com.tencent.teg.dc.lhotse.proto.LhotseObject.LState;
import com.tencent.teg.dc.lhotse.runner.util.CommonUtils;
public class Helloword extends AbstractTaskRunner{

    public Helloword(String configFileName) {
        super(configFileName);
    }

    private boolean success = false;

    public static void main(String[] args) {
    	
        String configure = "";
        if(args == null ||args.length < 1){
            System.out.println("需要配置文件作为输入参数!");
            System.exit(2);
        }else{
            configure = args[0];
        }

        Helloword runner = new Helloword(configure);
        runner.startWork();
    }

    @Override
    public void execute() throws IOException {

        try {
            taskRuntime = this.getTask();
            
            String p1 = taskRuntime.getProperties().get("parameter1");

            this.writeLocalLog(Level.INFO, "get parameter 1 =" + p1);

            if(!"kill".equals(p1)){
                commitTaskAndLog(LState.FAILED, "", "parameter is kill");
            }
            success = true;
        } catch (Exception e) {
            this.writeLocalLog(Level.SEVERE ,"执行HelloWord runner 出现异常");

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
        this.writeLocalLog(Level.INFO, " hello word had been kill ");
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

