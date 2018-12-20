package com.tencent;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class TestClusterHostNameAndIps {
	
	public static Map<String,String> hostNameList = null;
	
	static{
		hostNameList = TestClusterHostNameAndIps.getClusterHostNameAndIps();
	}

	public static Map<String,String>  getClusterHostNameAndIps(){
		
        Map<String,String> mapList = new HashMap<String,String>();
        Runtime r = Runtime.getRuntime();
        Process p;
        
        try {
            p = r.exec("arp -a");
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String inline;
            while ((inline = br.readLine()) != null) {
            	StringTokenizer tokens = new StringTokenizer(inline);
                String x;
                InetAddress add=null;
                try {
                    add = InetAddress.getByName(x = tokens.nextToken());
                } catch (Exception e) {
                    continue;
                }
                System.out.println(add.getHostName()+"===============>"+add.getHostAddress());
                mapList.put(add.getHostName(), add.getHostAddress());
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return mapList;
    }
}
