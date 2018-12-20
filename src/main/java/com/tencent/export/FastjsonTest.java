package com.tencent.export;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.jmx.snmp.Timestamp;
import com.tencent.conf.ConfigurationManager;

/**
 * fastjson测试类
 * @author Administrator
 *
 */
public class FastjsonTest {

	public static void main(String[] args) {
		
		StringBuffer buff = new StringBuffer();
		buff.append("{").
				append("\"took\": 49,").
				append("\"timed_out\": false,").
				append("\"_shards\": {").
				append("	\"total\": 5,").
				append("	\"successful\": 5,").
				append("	\"skipped\": 0,").
				append("	\"failed\": 0").
				append("},").
				append("\"hits\": {").
				append("	\"total\": 95554151,").
				append("	\"max_score\": 1.0,").
				append("	\"hits\": [{").
				append("		\"_index\": \"ctsdb_tbds@1545062400000_1\",").
				append("		\"_type\": \"doc\",").
				append("		\"_id\": \"AWfCDQGUqSuVnzaath-L\",").
				append("		\"_score\": 1.0,").
				append("		\"fields\": {").
				append("			\"0002\": [3425]").
				append("		}").
				append("	}, {").
				append("		\"_index\": \"ctsdb_tbds@1545062400000_1\",").
				append("		\"_type\": \"doc\",").
				append("		\"_id\": \"AWfCDQGUqSuVnzaath-0\",").
				append("		\"_score\": 1.0,").
				append("		\"fields\": {").
				append("			\"0002\": [538]").
				append("		}").
				append("	}]").
				append("}").
				append("}");
		
		JSONObject object = JSON.parseObject(buff.toString());

		long sun = 0;
        JSONObject data = (JSONObject) object.get("hits");

        JSONArray jsonArray = data.getJSONArray("hits");

        List<FieldsModel> result = JSON.parseArray(jsonArray.toJSONString(), FieldsModel.class);
        
        System.out.println(result.get(0).getFields());
        
        String str_json = JSON.toJSONString(result.get(0).getFields()); 
        
//        System.out.println(object.getString("took"));
        
	}
	
}
