package com.iflytek.idmapping.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 2017/6/13.
 */
public class HBaseUtil {

    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //conf.set("hbase.zookeeper.quorum", "10.10.12.82,10.10.12.83,10.10.12.84");
        conf.set("hbase.zookeeper.quorum", "172.21.191.19,172.21.191.20,172.21.191.21");
        // conf.set("hbase.master", "192.168.1.100:600000");
        conf.set("mapreduce.job.queuename", "dmp");
//        conf.set("fs.permissions.umask-mode", "000");
//        conf.set("hbase.coprocessor.region.classes", "");
    }

    public static void createIndexTable(String tableName,byte[][] splits) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            System.out.println(tableName + " is exist,detele....");
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hIndexColumn = new HColumnDescriptor("global_id");
        hIndexColumn.setCompressionType(Compression.Algorithm.SNAPPY);
        hIndexColumn.setBloomFilterType(BloomType.ROWCOL);
        hIndexColumn.setMaxVersions(1);
        tableDescriptor.addFamily(hIndexColumn);
      //  tableDescriptor.addFamily(new HColumnDescriptor("global_id"));
        hAdmin.createTable(tableDescriptor,splits);
        System.out.println("create table:" + tableName + "done!");
        hAdmin.close();
    }

    public static void createIDsTable(String tableName,byte[][] splits) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if (hAdmin.tableExists(tableName)) {
            System.out.println(tableName + " is exist,detele....");
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hIDsColumn = new HColumnDescriptor("ids");
        hIDsColumn.setCompressionType(Compression.Algorithm.SNAPPY);
        hIDsColumn.setBloomFilterType(BloomType.ROWCOL);
        hIDsColumn.setMaxVersions(1);
        tableDescriptor.addFamily(hIDsColumn);
        hAdmin.createTable(tableDescriptor,splits);
        System.out.println("create table: " + tableName + " done!");
        hAdmin.close();
    }

//    private static void createTable(String tableName, String[] cfs) throws Exception {
//        Connection conn = getConnection();
//        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
//        try {
//            if (admin.tableExists(tableName)) {
//                logger.warn("Table: {} is exists!", tableName);
//                return;
//            }
//            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
//            for (int i = 0; i < cfs.length; i++) {
//                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
//                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
//                hColumnDescriptor.setMaxVersions(1);
//                tableDesc.addFamily(hColumnDescriptor);
//            }
//            admin.createTable(tableDesc);
//            logger.info("Table: {} create success!", tableName);
//        } finally {
//            admin.close();
//            closeConnect(conn);
//        }
//    }

    public static void dropTable(String tableName) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void QueryAll(String tableName) {
     //   HTablePool pool = new HTablePool(conf, 1000);
      //  HTable table = (HTable) pool.getTable(tableName);
        try {
            HTable table = new HTable(conf, tableName);
            ResultScanner rs = table.getScanner(new Scan());

            int maxCount = 100;
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
                maxCount--;
                if(maxCount <= 0) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Hbase获取所有的表信息
    public static List<String> getAllTables() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }

}
