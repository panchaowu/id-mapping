package com.iflytek.idmapping.util;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.util.*;

/**
 * Created by admin on 2017/5/4.
 */
public class OrcUtil {
    public static final int typeCount = 7;

    public static String[] IdsTypes = {"gloabl_id","imei","mac","imsi","phone_number","idfa","openudid","android_id"};
    public static final String IDsOrcSchema = "struct<global_id:string,imei:map<string,int>,mac:map<string,int>,imsi:map<string,int>,phone_number:map<string,int>,idfa:map<string,int>,openudid:map<string,int>,android_id:map<string,int>>";


    public static SettableStructObjectInspector idsInspector;
    public static List<? extends StructField> idsFields;


    public static OrcSerde idsSerde;

    public static OrcStruct InitIDs() {
        OrcStruct os = (OrcStruct) idsInspector.create();
        idsInspector.setStructFieldData(os,idsFields.get(0),new Text(""));
        for(int i = 1; i <= typeCount; i++) {
            idsInspector.setStructFieldData(os,idsFields.get(i),new HashMap<Text,IntWritable>());
        }
        return os;
//        Map<String,Integer> imeiMap =(Map<String,Integer>) idsInspector.getStructFieldData(os,idsFields.get(1));
//        if(imeiMap == null) {
//            imeiMap = new HashMap<>();
//            imeiMap.put("6318268318618",1);
//            idsInspector.setStructFieldData(os,idsFields.get(1),imeiMap);
//            idsInspector.setStructFieldData(os,idsFields.get(0),imeiMap);
//        }

    }

    public static void setupIDsOrc() {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(IDsOrcSchema);
        idsInspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        idsFields= idsInspector.getAllStructFieldRefs();
    }

    public static void initialize(TaskInputOutputContext context) {
        idsSerde = new OrcSerde();
        StringBuilder colums = new StringBuilder();
        StringBuilder types = new StringBuilder();
        for (StructField field : idsFields) {
            colums.append(field.getFieldName());
            colums.append(",");
            types.append(field.getFieldObjectInspector().getTypeName());
            types.append(",");
        }
        colums.deleteCharAt(colums.length() - 1);
        types.deleteCharAt(types.length() - 1);
        Properties properties = new Properties();
        properties.put(IOConstants.COLUMNS, colums.toString());
        properties.put(IOConstants.COLUMNS_TYPES, types.toString());
        idsSerde.initialize(context.getConfiguration(), properties);
    }

    public static String getIDsGlobalId(OrcStruct struct) {
       // TypeDescription.createStruct()
        String globalId = idsInspector.getStructFieldData(struct,idsFields.get(0)).toString();
        return globalId;
    }

    public static void setIDsGlobalId(OrcStruct struct,String val) {
        idsInspector.setStructFieldData(struct,idsFields.get(0),new Text(val));
    }

    public static Map<Text,IntWritable> getIDsTypeMap(OrcStruct orcs,int index) {
        Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(orcs,idsFields.get(index));
        return typeMap;
    }

    public static void setIDsTypeMap(OrcStruct orcs,int index, Map<Text,IntWritable> typeMap) {
        idsInspector.setStructFieldData(orcs,idsFields.get(index),typeMap);
    }

    public static OrcStruct toLowerCase(OrcStruct orc) {
        OrcStruct lowerOrc = OrcUtil.InitIDs();
        //String globalId =(String) idsInspector.getStructFieldData(orc,idsFields.get(0));
        String globalId = OrcUtil.getIDsGlobalId(orc);
        idsInspector.setStructFieldData(lowerOrc,idsFields.get(0),globalId);
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(orc,idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                Map<Text,IntWritable> lowerMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(lowerOrc,idsFields.get(i));
                for(Map.Entry<Text,IntWritable> entry : typeMap.entrySet()) {
                    lowerMap.put(new Text(entry.getKey().toString().toLowerCase()),entry.getValue());
                }
            }
        }
        return  lowerOrc;
    }

    public static OrcStruct toLowerCaseAndClean(OrcStruct orc,HashSet<String> invalidDvcSet) {
        OrcStruct lowerOrc = OrcUtil.InitIDs();
        //String globalId =(String) idsInspector.getStructFieldData(orc,idsFields.get(0));
        String globalId = OrcUtil.getIDsGlobalId(orc);
        idsInspector.setStructFieldData(lowerOrc,idsFields.get(0),globalId);
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(orc,idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                Map<Text,IntWritable> lowerMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(lowerOrc,idsFields.get(i));
                for(Map.Entry<Text,IntWritable> entry : typeMap.entrySet()) {
                  //  lowerMap.put(new Text(entry.getKey().toString().toLowerCase()),entry.getValue());
                    String key =IdsTypes[i] + "_" + entry.getKey().toString().toLowerCase();
                    if(invalidDvcSet.contains(key) == false) {
                        lowerMap.put(new Text(entry.getKey().toString().toLowerCase()),entry.getValue());
                    }
                }
            }
        }
        return  lowerOrc;
    }


    public static Map<String,Integer> mergeIDsToMap(OrcStruct struct) {
        Map<String,Integer> idsMap = new HashMap<>();
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(struct,idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                for(Map.Entry<Text,IntWritable> entry : typeMap.entrySet()) {
                    idsMap.put(IdsTypes[i]+"_"+entry.getKey().toString(),entry.getValue().get());
                }
            }
        }
        return idsMap;
    }

    public static boolean isFakeIDs(OrcStruct ids) {
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> idsMap =  (Map<Text,IntWritable>) idsInspector.getStructFieldData(ids,idsFields.get(i));
            if(idsMap.size() > 10) {
                return true;
            }
        }
        return false;
    }

    public static void addIDs(OrcStruct ids1,OrcStruct idsSum) {
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> sumMap = (Map<Text,IntWritable>) idsInspector.getStructFieldData(idsSum,idsFields.get(i));
            Map<Text,IntWritable> idsMap =  (Map<Text,IntWritable>) idsInspector.getStructFieldData(ids1,idsFields.get(i));
            IdMappingUtil.addMap(idsMap,sumMap);
        }
    }

    public static String sortedIDsToString(OrcStruct orcValue) {
        StringBuffer sb = new StringBuffer();
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(orcValue,idsFields.get(i));
            sb.append(new TreeSet(typeMap.keySet()));
        }
        return sb.toString();
    }


    public static void mergeToLastestIDs(OrcStruct orc1,OrcStruct updateOrc) {
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(orc1,idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                Map<Text,IntWritable> dstMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(updateOrc,idsFields.get(i));
                updateItem(typeMap,dstMap);
            }
        }
    }

    public static void updateItem(Map<Text,IntWritable> srcMap,Map<Text,IntWritable> dstMap) {
        for(Text key : srcMap.keySet()) {
            if(dstMap.containsKey(key) == false || dstMap.get(key).get() < srcMap.get(key).get()) {
                dstMap.put(key,srcMap.get(key));
            }
        }
    }


    public static OrcStruct filterIDs(OrcStruct oldIDs,int minDay) {
        OrcStruct newIDs = OrcUtil.InitIDs();
        boolean isEmpty = true;
        for(int i = 1; i <= typeCount; i++) {
            Map<Text,IntWritable> typeMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(oldIDs,idsFields.get(i));
            if(typeMap != null && typeMap.size() > 0) {
                Map<Text,IntWritable> dstMap = (Map<Text,IntWritable>)idsInspector.getStructFieldData(newIDs,idsFields.get(i));
                for(Map.Entry<Text,IntWritable> entry : typeMap.entrySet()) {
                    // 活跃度大于某个日期或者为手机号
                    if(IdsTypes[i].equals("phone_number") || entry.getValue().get() >= minDay) {
                        dstMap.put(entry.getKey(),entry.getValue());
                        isEmpty = false;
                    }
                }
            }
        }
        if(isEmpty) {
            return null;
        }
        return newIDs;
    }

    // index 相关函数






    public static void main(String[] args) {
//        setupIDsOrc();
//        OrcStruct data = InitIDs();
//        String global = (String)idsInspector.getStructFieldData(data,idsFields.get(0));
//        Map<String,Integer> imeiMap = (Map<String,Integer>)idsInspector.getStructFieldData(data,idsFields.get(1));
//        Map<String,Integer> macMap = (Map<String,Integer>)idsInspector.getStructFieldData(data,idsFields.get(2));
//        System.out.println(global);
//        Map<Text,IntWritable> typeMap = new HashMap<>();
//        typeMap.put(new Text("nsiusi"),new IntWritable(10));
//        typeMap.put(new Text("ewuytu"),new IntWritable(10));
//        typeMap.put(new Text("fye7u67"),new IntWritable(10));
//        typeMap.put(new Text("yuq736"),new IntWritable(10));
//        typeMap.put(new Text("auyudd"),new IntWritable(10));
//        StringBuffer sb = new StringBuffer();
//        sb.append(typeMap.keySet());
//        sb.append(new TreeSet(typeMap.keySet()));
//        System.out.println(sb);
    }


}
