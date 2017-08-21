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
 * Index 查询的模块.
 * 包含初始化及基于dvcId(MD5编码)查询IDs字符串
 */
public class IndexQuery {
    private static Logger logger = Logger.getLogger(IndexQuery.class);
    private String indexPath;
    private HTable indexTable = null;
    private String indexTableName = null;
    private ConnectWatcher watcher = null;
    private Configuration conf = null;

    public IndexQuery() {
        indexPath = "/idmapping/active_index2hbase";
    }

    public IndexQuery(String idsPath,ConnectWatcher watcher) {
        this.indexPath = idsPath;
        this.watcher = watcher;
    }

    public String getIndexPath() {
        return indexPath;
    }

    public void setIndexPath(String indexPath) {
        this.indexPath = indexPath;
    }

    public void init(Configuration _conf) throws KeeperException, InterruptedException, IOException {
        this.conf = _conf;
        indexTableName = watcher.getData(indexPath, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    indexTableName = watcher.getData(indexPath, this);
                    indexTable.close();
                    indexTable = new HTable(conf,indexTableName);
                    logger.info("HTable Index Changed :" + indexTableName);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        indexTable = new HTable(conf, indexTableName);
        System.out.println("HTable Index  :" + indexTableName);
        logger.info("HTable Index  :" + indexTableName);
    }

    // 通过设备号key得到globalID，type为设备号类型，如imei,mac，暂时不用
    public String getGlobalId(String key,String type) throws IOException {
        Get globalKeyGet = new Get(Bytes.toBytes(key));
        if (globalKeyGet == null) {
            return null;
        }
        Result globalIdResult = indexTable.get(globalKeyGet);
        if (globalIdResult == null || globalIdResult.isEmpty()) {
            return null;
        }
        return Bytes.toString(globalIdResult.getValue(Bytes.toBytes("global_id"), Bytes.toBytes("value")));
    }

    public void close() {
        try {
            if(indexTable != null) {
                indexTable.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
