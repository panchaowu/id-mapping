package com.iflytek.idmapping.Schema;

import org.apache.avro.reflect.ReflectData;

/**
 * Created by admin on 2017/5/10.
 */
public class Index {
    String id;
    String global_id;
    public static void main(String[] args) {
        String schema = ReflectData.get().getSchema(Index.class).toString();
        System.out.println(schema);
    }
}
