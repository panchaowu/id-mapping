package com.iflytek.idmapping.util;

import com.google.gson.Gson;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * Created by admin on 2017/5/4.
 */
public class IdMappingUtil {
    public static java.util.Random random = null;
    public static String Random = "Random";
    private static final int ACTIVITY_LIMIT = 100;

    public  static String getRandomString(String taskId, int taskNum) {
        if (random == null) {
            String key = taskId.split("_")[4];
            random = new Random(Integer.valueOf(key));
        }
        return Integer.toString(random.nextInt(taskNum));
    }


    public static void addMap(Map<Text,IntWritable> oneMap, Map<Text,IntWritable>sumMap) {
        for ( Map.Entry<Text, IntWritable> id :oneMap.entrySet()) {
            Text idKey = id.getKey();
            int idValue = id.getValue().get();
//            if (idValue > ACTIVITY_LIMIT) {
//                continue;
//            }
            if(sumMap.containsKey(idKey) == true) {
                if(idValue > sumMap.get(idKey).get()){
                    sumMap.put(idKey, new IntWritable(idValue));
                }
            } else{
                sumMap.put(idKey, new IntWritable(idValue));
            }
        }
    }

    public static String orc2Json(OrcStruct orcs) {
        IDs jsonIDs = new IDs();
        jsonIDs.dvcs = new ArrayList<>();
        String globalId = OrcUtil.getIDsGlobalId(orcs);
        jsonIDs.setGlobal_id(globalId);
        for(int i = 1; i <= OrcUtil.typeCount; i++) {
            Map<String,Integer> mMap = new HashMap<>();
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)OrcUtil.idsInspector.getStructFieldData(orcs,OrcUtil.idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                for(Map.Entry<Text,IntWritable> e : typeMap.entrySet()) {
                    mMap.put(e.getKey().toString(),e.getValue().get());
                }
            }
            jsonIDs.dvcs.add(mMap);
        }
        return jsonIDs.toString();
    }

    public static OrcStruct json2orc(String IDsStr) {
        OrcStruct orcs = OrcUtil.InitIDs();
        Gson gson = new Gson();
        IDs jsonIDs = gson.fromJson(IDsStr,IDs.class);
        String globalId = jsonIDs.getGlobal_id();
        OrcUtil.setIDsGlobalId(orcs,globalId);
        List<Map<String,Integer>> dvcs = jsonIDs.getDvcs();
        if(dvcs.size() != OrcUtil.typeCount) {
            System.out.println("type count is error");
            return null;
        }
        for(int i = 1; i <= OrcUtil.typeCount; i++) {
            Map<String,Integer> mMap = dvcs.get(i-1);
            Map<Text,IntWritable> typeMap = new HashMap<>();
            if(mMap != null) {
                for(Map.Entry<String,Integer> e : mMap.entrySet()) {
                    typeMap.put(new Text(e.getKey()),new IntWritable(e.getValue()));
                }
            }
            OrcUtil.idsInspector.setStructFieldData(orcs,OrcUtil.idsFields.get(i),typeMap);
        }
        return orcs;
    }
}
