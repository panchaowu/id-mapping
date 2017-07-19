//package com.iflytek.idmapping.util;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.orc.TypeDescription;
//import org.apache.orc.mapred.OrcMap;
//import org.apache.orc.mapred.OrcStruct;
//
//import java.util.HashMap;
//import java.util.Map;
//
//
///**
// * Created by admin on 2017/5/16.
// */
//public class OrcUtils {
//
//    public static Map<String,Integer> mergeIDsToMap(OrcStruct struct) {
//        int numFields = struct.getNumFields();
//        TypeDescription schema = struct.getSchema();
//        schema.getFieldNames();
//        Map<String,Integer> idsMap = new HashMap<>();
//        for(int i = 1; i <= numFields; i++) {
//            OrcMap<Text,IntWritable> orcMap = new OrcMap<>("map<string,int>");
//            Map<String,Integer> typeMap = (Map<String,Integer>)idsInspector.getStructFieldData(struct,idsFields.get(i));
//            if(typeMap != null && typeMap.size() > 0) {
//                for(Map.Entry<String,Integer> entry : typeMap.entrySet()) {
//                    idsMap.put(IdsTypes[i]+"_"+entry.getKey(),entry.getValue());
//                }
//            }
//        }
//        return idsMap;
//    }
//}
