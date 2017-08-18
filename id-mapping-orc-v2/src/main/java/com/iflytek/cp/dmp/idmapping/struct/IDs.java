package com.iflytek.cp.dmp.idmapping.struct;

import com.google.gson.Gson;
import com.iflytek.cp.dmp.idmapping.util.IDMappingUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;

import java.util.*;

/**
 * Created by chentao on 2017/7/20.
 * IDs, 简化orcStruct处理的内部处理的结构，并且转化时同时会根据数据源和id类型做过滤
 * Hive表的结构是struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>
 */
public class IDs {
    public static class Info {
        public String src;
        public Integer datetime;
        public String model;

        public Info(String src, Integer datetime, String model) {
            this.src = src;
            this.datetime = datetime;
            this.model = model;
        }

        @Override
        public String toString() {
            return gson.toJson(this);
        }
    }

    public String globalID;
    public Map<String, Map<String, Info>> ids;
    private static Gson gson = new Gson();
    private static TypeDescription AllSchema =
            TypeDescription.fromString("struct<global_id:string," +
                    "ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
    private static TypeDescription IDsSchema =
            TypeDescription.fromString("map<string,struct<src:string,datetime:int,model:string>>");
    private static TypeDescription IDSchema =
            TypeDescription.fromString("struct<src:string,datetime:int,model:string>");

    public IDs() {
        globalID = "";
        ids = new HashMap<String, Map<String, Info>>();
        for (String type: IDMappingUtil.getValidTypeSet()) {
            ids.put(type, new HashMap<String, Info>());
        }
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    /*
    *  将对应的orcStruct转换为内部IDs结构
    *  isFilterSrc，过滤只在 mr step 1 有用，为了减少计算量而增加
    * */
    public IDs fromOrcStruct(OrcStruct in, boolean isFilterSrc) {
        Set<String> validTypeSet = IDMappingUtil.getValidTypeSet();
        Set<String> validSrcSet = IDMappingUtil.getValidSrcSet();
        globalID = (in.getFieldValue("global_id")).toString();
        OrcMap<Text, OrcMap> orcIDs = (OrcMap<Text, OrcMap>)in.getFieldValue("ids");
        // 只处理有效的ID类型，因此将不在列表的ID类型过滤了
        for (String type : validTypeSet) {
            if (! orcIDs.containsKey(new Text(type))) {
                continue;
            }
            OrcMap<Text, OrcStruct> idlist = orcIDs.get(new Text(type));
            Map<String, Info> resIDs = new HashMap<String, Info>();
            // 处理ID列表
            for (Text tmpKey : idlist.keySet()) {
                // 去除id为空字符串的id ""
                if(tmpKey.toString().equals("")) {
                    continue;
                }
                OrcStruct value = idlist.get(tmpKey);
                StringBuffer resTxt = new StringBuffer();
                Set<String> resSrc = new HashSet<String>();    // 使用set是为了去重
                // 获取数据中的源，保存在HashSet中
                String srcTxt = (value.getFieldValue("src")).toString();
                if (isFilterSrc) {
                    Set<String> srcSet = new HashSet<String>();
                    srcSet.addAll(Arrays.asList(srcTxt.split(",")));
                    // 对比有效的源和数据中的src，过滤掉无效的源
                    for (String validSrc : validSrcSet) {
                        if (srcSet.contains(validSrc)) {
                            resSrc.add(validSrc);
                        }
                    }
                    if (resSrc.size() > 0) {
                        for(String tmp: resSrc) {
                            resTxt.append(tmp + ",");
                        }
                        resTxt.deleteCharAt(resTxt.length() - 1);  // 去掉最后一个逗号
                    } else {
                        continue;  // 没有 src，过滤掉掉该id
                    }
                } else {
                    resTxt.append(srcTxt);
                }
                Integer dateTime = ((IntWritable) value.getFieldValue("datetime")).get();
                String model = (value.getFieldValue("model")).toString();
                // 所有ID都会转化为小写
                resIDs.put((tmpKey).toString().toLowerCase(), new Info(resTxt.toString(), dateTime, model));
            }
            ids.put(type, resIDs);
        }
        return this;
    }

    public synchronized OrcStruct toOrcStruct() {
        OrcStruct orcStruct = (OrcStruct) OrcStruct.createValue(AllSchema);
        OrcMap<Text, OrcMap> orcAllIDs = (OrcMap<Text, OrcMap>) orcStruct.getFieldValue("ids");
        Text orcGlobalID = new Text(globalID);
        orcStruct.setFieldValue("global_id", orcGlobalID);
        for (String idType : ids.keySet()) {
            OrcMap orcIDs = (OrcMap) OrcStruct.createValue(IDsSchema);
            Map<String, Info> tmpID = ids.get(idType);
            for (Map.Entry<String, Info> tmp : tmpID.entrySet()) {
                OrcStruct orcID = (OrcStruct) OrcStruct.createValue(IDSchema);
                orcID.setFieldValue("src", new Text(tmp.getValue().src));
                orcID.setFieldValue("datetime", new IntWritable(tmp.getValue().datetime));
                orcID.setFieldValue("model", new Text(tmp.getValue().model));
                orcIDs.put(new Text(tmp.getKey()), orcID);
            }
            orcAllIDs.put(new Text(idType), orcIDs);
        }
        return orcStruct;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public static void main(String[] args) {
        IDs ids = new IDs();
        ids.globalID = "test_global_id";

        Map<String, Info> id = new HashMap<String, Info>();
        id.put("imei_1",new Info("vcoam",20170612,"iphone 6s"));
        id.put("imei_2",new Info("sdk",20170613,"iphone 7p"));
        ids.ids.put("imei", id);

        id = new HashMap<String, Info>();
        id.put("mac_2",new Info("lingxi",20170717,"huawei nova"));
        id.put("mac_1",new Info("cmcc",20170312,"xiaomi"));
        ids.ids.put("mac", id);

        TreeSet<String> set = new TreeSet<String>();
        set.add("mac_2");
        IDMappingUtil.filterIDs(ids, set , 20170601);
//        System.out.println(ids.fromOrcStruct(ids.toOrcStruct(), true));
        System.out.println(ids);
//        System.out.println(IDMappingUtil.getAllIdSet(ids));
        String str = "imei_imei_1";
        System.out.println(str.substring(str.indexOf('_') + 1));
    }
}
