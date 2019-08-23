package com.demo;

import com.tencent.tdbank.busapi.BusClientConfig;
import com.tencent.tdbank.busapi.DefaultMessageSender;
import com.tencent.tdbank.busapi.SendResult;
import com.tencent.tdbank.busapi.network.BussdkException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * 此demo用于发送信息到Tube
 *
 * 注意：
 * tdbank有一些预定义属性是有特定含义的，用户指定扩展属性时不能使用这些字段。具体有以下属性：
 * "bid" ，  "tid" ，  "dt" ，  "cp"  ，  "cnt"  ， "messageId"
 * "mt" ，  "m"   ， "sid" ，   "f"   ，   "t"   ，"NodeIP"  ，“_file_status_check”
 *
 */

@Data
@Slf4j
public class TubeProduceDemo {

    /**接入名称就是BID名称，在tbds接入服务中配置的接口名称**/
    private static final String BID = "dig_test";
    /**tid用于业务自己进行定义，可以作为一个type类型**/
    private static final String TID = "dig_test_table";
    /**0表示用系统时间做时间戳，精确到毫秒级别**/
    private static final int dt = 0;
    /**TDManager的服务器地址**/
    private static final String TD_MANAGER_ADDRESS="http://tbds-172-16-16-4:8735";
    /**是否进行外网访问，外网采用https进行消息通信**/
    private static final boolean NEED_HTTPS_VISIT = false;
    /**TLS的证书**/
    private static final String TLS_SERVER_CERT_FILE_PATH_AND_NAME="";
    /**TLS的证书的key**/
    private static final String TLS_SERVER_KEY="";
    /**网路选择，默认都写all吧**/
    private static final String NET_TAG="all";
    //pro账户的key与id,需要确定在消息中间件tube和tdbus都有对应的权限。
    private static final String USERNAME="admin";
    //tbds中此账户对应的security key
    private static final String SECURITY_KEY = "4uXZEchnNUOhsrTxw1iynPoyEt8vRk8O";
    //tbds中此账户对应的security id
    private static final String SECURITY_ID = "M7rmdKryXYRYne8w7bq277yCD7hkUKKMsMot";
    /**busClientConfig的配置内容**/
    private static BusClientConfig busClientConfig = null;


    /**
     * 根据参数信息构造BusConfigure信息
     */
    static {
        try {
            //通过本地网络获取ip地址
            InetAddress address = InetAddress.getLocalHost();
            String localHost = address.getHostAddress();
            //构造bugclientConfig
            busClientConfig = new BusClientConfig(localHost,TD_MANAGER_ADDRESS,NEED_HTTPS_VISIT,TLS_SERVER_CERT_FILE_PATH_AND_NAME,TLS_SERVER_KEY,BID,NET_TAG);
            //设置认证信息
            busClientConfig.setAuthInfo(true, false, USERNAME, SECURITY_ID, SECURITY_KEY);
            //如果超过该值但是还没有收到回包的请求，会收到ASYNC_CALLBACK_BUFFER_FULL异常
            busClientConfig.setTotalAsyncCallbackSize(30000);

        } catch (UnknownHostException e) {
            log.error("获取本地SDK执行的地址信息失败",e);
        }catch (BussdkException e){
            log.error("初始化构建BusClientConfig失败,请检查对应的配置信息",e);
        }
    }

    /**
     * 发送数据
     * @param defaultMessageSender
     */
    public static void sendData(DefaultMessageSender defaultMessageSender){
        //构造发送数据的程序，20条发送一次,总共发送100条
        List<byte[]> contents = new ArrayList<byte[]>();
        for (int i = 0; i < 100; i++) {
            String msg = "This is message【" + i + "】"+"\t"+"zhangsan+【"+i+"】"+"\t"+i;
            byte[] content = msg.getBytes();
            contents.add(content);
            if (i % 20 == 0) {
                SendResult result = defaultMessageSender.sendMessage(content, BID, TID, dt, UUID.randomUUID().toString(), 20, TimeUnit.SECONDS);
                log.info("发送数据的结果为【"+result.toString()+"】");
                System.out.println("sss发送数据的结果为【"+result.toString()+"】");
                contents.clear();
            }
        }
    }

    /**
     *  Testing
     * @param args
     */
    public static void main(String[] args){
        DefaultMessageSender defaultMessageSender = null;
        try {
            //构造一个DefaultMessageSender
            defaultMessageSender = new DefaultMessageSender(busClientConfig);
            //发送测试数据
            TubeProduceDemo.sendData(defaultMessageSender);
        } catch (Exception e) {
            log.error("DefaultMessageSender构建失败",e);
        }finally {
            try{
                defaultMessageSender.close();
            }catch (Exception e){
                log.error("关闭异常"+e.getMessage());
            }

        }
    }
}
