package com.iflytek.idmapping.hbase;

import com.google.gson.Gson;
import com.iflytek.idmapping.struct.IDsStruct;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Map;

/**
 * Created by admin on 2017/6/27.
 */
public class HBaseQuery {

    private static String indexName = "idmapping_index2hbase_1";
    private static String idsName   = "idmapping_ids2hbase_1";

    private static HTable hTableIndex = null;
    private static HTable hTableIDs   = null;

    public static String getGlobalID(String key) throws IOException {
        Get globalKeyGet = new Get(Bytes.toBytes(key));
        if (globalKeyGet == null) {
            return null;
        }
        Result keyResult = hTableIndex.get(globalKeyGet);
        if (keyResult.isEmpty()) {
            return null;
        }
        return Bytes.toString(keyResult.getValue(Bytes.toBytes("global_id"), Bytes.toBytes("value")));
    }

    public static IDsStruct getIDsResult(String key) throws IOException {
        String globalID = getGlobalID(key);
        if (globalID == null) {
            return null;
        }
        Get idsKeyGet = new Get(Bytes.toBytes(globalID));
        Result idsResult = hTableIDs.get(idsKeyGet);

        Gson gson = new Gson();
        if(idsResult != null && ! idsResult.isEmpty()) {
            byte[] tmpBytes = idsResult.getValue(Bytes.toBytes("ids"), Bytes.toBytes("value"));
            if (tmpBytes != null) {
                String tmp = Bytes.toString(tmpBytes);
                IDsStruct ids  = gson.fromJson(tmp, IDsStruct.class);
                return ids;
            }
        }
        return null;
    }

    public static void help() {
        System.out.println("Usage:");
        System.out.println("  java -cp idmapping_query.jar com.iflytek.idmapping.hbase.HBaseQuery key [indexTableName] [idsTableName]");
        System.out.println("  ---key the id which you wan to search");
        System.out.println("  ---indexTableName Optional");
        System.out.println("  ---idsTableName Optional");
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 1 && args.length != 3) {
            help();
            return;
        }
        if(args.length == 3) {
            indexName = args[1];
            idsName   = args[2];
        }
        hTableIndex = new HTable(HBaseUtil.conf,indexName);
        hTableIDs   = new HTable(HBaseUtil.conf,idsName);

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]),"UTF-8"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("rslt.txt"),"UTF-8"));
        String id;
        int count = 0;
        while ( (id = br.readLine()) != null) {
            String upperId = id.toUpperCase();
            System.out.println(upperId + ":" + count);
            IDsStruct ids = getIDsResult(upperId);
            if(ids != null) {
                Map<String,Integer> imei = ids.getImei();
                if(imei != null && imei.size() > 0) {
                    bw.write(id + "\t" + imei.toString() + "\n");
                }
            }
        }
        br.close();
        bw.close();
    }
}
