package com.iflytek.cp.dmp.idmapping.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IDMapping 数据导入HBase的基本封装
 * 包含创建表，删除表等操作
 */
public class HBaseUtil {

    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "172.21.191.19,172.21.191.20,172.21.191.21");
        conf.set("mapreduce.job.queuename", "dmp");
    }

    /**
     * 创建IDs表，依据splits指定region
     */
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

    /**
     *  创建Index表，依据splits指定region
     * @param tableName
     * @param splits
     * @throws IOException
     */
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
        hAdmin.createTable(tableDescriptor,splits);
        System.out.println("create table:" + tableName + "done!");
        hAdmin.close();
    }

    /**
     * 依据表名将表删除
     * @param tableName
     */
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


    /**
     * Hbase获取所有的表名
     */
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
                    //   System.out.println(hTableDescriptor.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }
}
