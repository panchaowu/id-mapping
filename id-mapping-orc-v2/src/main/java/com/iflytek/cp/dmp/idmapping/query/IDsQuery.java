package com.iflytek.cp.dmp.idmapping.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

/**
 * Created by admin on 2017/1/21.
 */
public class IDsQuery {
    private static Logger logger = Logger.getLogger(IDsQuery.class);
    private String idsPath;
    private HTable idsTable = null;
    private String idsTableName = null;
    private ConnectWatcher watcher = null;
    private Configuration conf = null;
    public IDsQuery() {
        idsPath = "/idmapping/active_ids2hbase";
    }

    public IDsQuery(String idsPath,ConnectWatcher watcher) {
        this.idsPath = idsPath;
        this.watcher = watcher;
    }

    public String getIdsPath() {
        return idsPath;
    }

    public void setIdsPath(String idsPath) {
        this.idsPath = idsPath;
    }

    public void init(Configuration _conf) throws KeeperException, InterruptedException, IOException {
        this.conf = _conf;
        idsTableName = watcher.getData(idsPath, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                    try {
                        idsTableName = watcher.getData(idsPath, this);
                        idsTable.close();
                        idsTable = new HTable(conf,idsTableName);
                        logger.info("HTable IDs Changed :" + idsTableName);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        });
        System.out.println("table name:" + idsTableName);
        idsTable = new HTable(conf, idsTableName);
       // System.out.println();
        logger.info("HTable IDs  :" + idsTableName);
    }

    // 通过globalID作为key得到ids的json字符串
    public String getIDsJsonStr(String key) throws IOException {
        Get idsKeyGet = new Get(Bytes.toBytes(key));
        if(idsKeyGet == null) {
            return null;
        }
        Result idsResult = idsTable.get(idsKeyGet);
        if(idsResult == null || idsResult.isEmpty()) {
            return null;
        }
        return Bytes.toString(idsResult.getValue(Bytes.toBytes("ids"), Bytes.toBytes("value")));
    }

    public void close() {
        try {
            if(idsTable != null) {
                    idsTable.close();
                }
        } catch (IOException e) {
                e.printStackTrace();
        }
    }
}
