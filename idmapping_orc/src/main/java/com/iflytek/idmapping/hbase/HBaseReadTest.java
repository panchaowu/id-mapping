package com.iflytek.idmapping.hbase;

import java.io.IOException;
import java.util.List;

/**
 * Created by admin on 2017/6/13.
 */
public class HBaseReadTest {

    public static void main(String[] args) throws IOException {
        String tableName = args[0];
        List<String> list = HBaseUtil.getAllTables();
        System.out.println("------table names-------");
        for (String tablename : list) {
            System.out.println(tablename);
        }
        System.out.println("------table data-------");
        HBaseUtil.QueryAll(tableName);
    }
}
