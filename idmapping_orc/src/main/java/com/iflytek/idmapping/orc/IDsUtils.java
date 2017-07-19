//package com.iflytek.idmapping.orc;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.MapWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.orc.TypeDescription;
//import org.apache.orc.mapred.OrcMap;
//import org.apache.orc.mapred.OrcStruct;
//
//import java.lang.reflect.Type;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by admin on 2017/5/18.
// */
//public class IDsUtils {
//
//    public static int typeCount = 7;
//    public static String[] IdsTypes = {"gloabl_id","imei","mac","imsi","phone_number","idfa","openudid","android_id"};
//    public static final String IDsSchemaStr= "struct<global_id:string,imei:map<string,int>,mac:map<string,int>,imsi:map<string,int>,phone_number:map<string,int>,idfa:map<string,int>,openudid:map<string,int>,android_id:map<string,int>>";
//
//    public static OrcStruct toLowCase(OrcStruct orcs) {
//        TypeDescription schema = TypeDescription.fromString(IDsSchemaStr);
//        OrcStruct lowerOrcs = (OrcStruct) OrcStruct.createValue(schema);
//        lowerOrcs.setFieldValue(0,orcs.getFieldValue(0));
//        for(int i = 1; i <= typeCount; i++) {
//            TypeDescription mapSchema = schema.getChildren().get(i);
//            OrcMap<Text,IntWritable> srcMap = (OrcMap<Text,IntWritable>)orcs.getFieldValue(i);
//            OrcMap<Text,IntWritable> lowerMap = new OrcMap<>(mapSchema);
//            if(srcMap != null && srcMap.size() > 0) {
//                for(Map.Entry<Text,IntWritable> entry : srcMap.entrySet()) {
//                    lowerMap.put(new Text(entry.getKey().toString().toLowerCase()),entry.getValue());
//                }
//            }
//            lowerOrcs.setFieldValue(i,lowerMap);
//        }
//        return  lowerOrcs;
//    }
//
//    public static OrcMap<Text,IntWritable> mergeIDs2Map(OrcStruct orcs) {
//        TypeDescription mapDes = TypeDescription.fromString("map<string,int>");
//        OrcMap<Text,IntWritable> orcMap = new OrcMap<>(mapDes);
//        for(int i = 1; i <= typeCount; i++) {
//            OrcMap<Text,IntWritable> dvcMap = (OrcMap<Text,IntWritable>)orcs.getFieldValue(i);
//            if(dvcMap != null && dvcMap.size() > 0) {
//                for(Map.Entry<Text,IntWritable> entry : dvcMap.entrySet()) {
//                    orcMap.put(new Text(IdsTypes[i]+"_" + entry.getKey().toString()),entry.getValue());
//                }
//            }
//        }
//        return orcMap;
//    }
//
//    public static void main(String[] args) {
//        TypeDescription schema = TypeDescription.fromString(IDsSchemaStr);
//      //  OrcStruct lowerOrcs = (OrcStruct) OrcStruct.createValue(TypeDescription.fromString(IDsSchemaStr));
//        for(int i = 0; i <= 7; i++) {
//            TypeDescription mapSchema = schema.getChildren().get(i);
//            System.out.println(mapSchema.toString());
//        }
//    }
//}
