package com.iflytek.idmapping.hbase;

/**
 * Created by admin on 2017/7/3.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by
 */
public class HFileLoader {

    public static Configuration conf = null;

    public static void doBulkLoad(String pathToHFile, String tableName){
        try {
            System.out.println("chmod :" + pathToHFile);
            Runtime.getRuntime().exec("hadoop fs -chmod -R 777 " + pathToHFile);
            System.out.println("Start Bulk Load ..");
            HBaseConfiguration.addHbaseResources(conf);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
            HTable hTable = new HTable(conf, tableName);//指定表名
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);//导入数据
            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    public  static String padLeft(String s, int length)
    {
        byte[] bs = new byte[length];
        byte[] ss = s.getBytes();
        Arrays.fill(bs, (byte) (48 & 0xff));
        System.arraycopy(ss, 0, bs,length - ss.length, ss.length);
        return new String(bs);
    }

    public static byte[][] getHexSplits(String startKey, String endKey,
                                        int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];

        int lowestKey = Integer.parseInt(startKey,16);
        int highestKey = Integer.parseInt(endKey,16);
        //int range = highestKey - lowestKey;
        int increment = (highestKey - lowestKey)/numRegions;
        for (int i = 0; i < numRegions - 1; i++) {
            int key = lowestKey + (increment * (i+1));
            String hexKey = padLeft(Integer.toHexString(key),4).toUpperCase();
            //  System.out.println(hexKey);
            byte[] b = hexKey.getBytes();
            splits[i] = b;
        }
        return splits;
    }

    public static void main(String[] args) throws IOException {
        conf = HBaseUtil.conf;
        String hfilePath = args[0];
        String tableName = args[1];
        int numRegions = Integer.parseInt(args[2]);
        System.out.println("hfilePath:" + hfilePath);
        System.out.println("tableName:" + tableName);
        System.out.println("numRegions:" + numRegions);
        byte[][] splits = getHexSplits("0000","FFFF",numRegions);
        if(tableName.startsWith("idmapping_index2hbase")) {
            HBaseUtil.createIndexTable(tableName,splits);
        } else if(tableName.startsWith("idmapping_ids2hbase")) {
            HBaseUtil.createIDsTable(tableName,splits);
        } else {
            return;
        }
        System.out.println("create table:" + tableName + "  done");
        doBulkLoad(hfilePath,tableName);
    }
}