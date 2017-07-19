package com.iflytek.idmapping.Schema;

import org.apache.avro.reflect.ReflectData;

import java.util.Map;

/**
 * Created by admin on 2017/5/4.
 */
public class IDMappingSchema {

    public String Global_Id;
    public Map<String,Integer> Imei;
    public Map<String,Integer> Mac;
    public Map<String,Integer> Imsi;
    public Map<String,Integer> Phone_Number;
    public Map<String,Integer> Idfa;
    public Map<String,Integer> Openudid;
    public Map<String,Integer> Uid;
    public Map<String,Integer> Did;
    public Map<String,Integer> Android_Id;

    public static void main(String[] args) {
        String schema = ReflectData.get().getSchema(Index.class).toString();
        System.out.println(schema);
    }
}
