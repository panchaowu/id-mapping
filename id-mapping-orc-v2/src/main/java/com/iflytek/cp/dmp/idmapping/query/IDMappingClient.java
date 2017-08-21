package com.iflytek.cp.dmp.idmapping.query;

import com.google.gson.Gson;
import com.iflytek.cp.dmp.idmapping.struct.IDs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.util.*;

/*
* idmapping的客户端访问
* 查询hbase
 */
public class IDMappingClient {
    private static Logger logger = Logger.getLogger(IDMappingClient.class);
    private String zooKeeperPath;
    private boolean bInit = false;
    private Configuration conf;
    private ConnectWatcher connectWatcher = new ConnectWatcher();
    private IDsQuery idsQuey;
    private IndexQuery indexQuery;

    public IDMappingClient() {
        zooKeeperPath = "172.21.191.19,172.21.191.20,172.21.191.21";
    }

    public IDMappingClient(String zooKeeperPath) {
        this.zooKeeperPath = zooKeeperPath;
    }

    public String getZooKeeperPath() {
        return zooKeeperPath;
    }

    public void setZooKeeperPath(String zooKeeperPath) {
        this.zooKeeperPath = zooKeeperPath;
    }

    /*
    * 客户端通过zookeeper连接hbase
    * 初始化并监听，当zk断了会重连
     */
    public synchronized void init() throws IOException, InterruptedException, KeeperException {
        if(bInit == true) {
            return;
        }
        conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zooKeeperPath);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connectWatcher.connect(zooKeeperPath);
        idsQuey = new IDsQuery("/idmapping/active_ids2hbase",connectWatcher);
        idsQuey.init(conf);
        indexQuery = new IndexQuery("/idmapping/active_index2hbase",connectWatcher);
        indexQuery.init(conf);
        bInit = true;
        logger.info("idmappingclient  init successfully!");
    }

    // 通过设备号md5查询idmapping
    public IDs getIDsByMD5Key(String srcId, String srcType) throws IOException {
        String globalID = indexQuery.getGlobalId(srcId, srcType);
        if (globalID == null) {
            return null;
        }
        String idsJsonStr = idsQuey.getIDsJsonStr(globalID);
        if(idsJsonStr != null) {
            Gson gson = new Gson();
            IDs ids = gson.fromJson(idsJsonStr,IDs.class);
            return ids;
        }
        return null;
    }

    //通过设备号明文查询idmapping
    public IDs getIDsByPlainKey(String srcId,String srcType) throws IOException {
        if(srcId == null) {
            return null;
        }
        String md5Key = MD5Hash.getMD5AsHex(srcId.toUpperCase().getBytes()).toUpperCase();
        String globalID = indexQuery.getGlobalId(md5Key, srcType);
        if (globalID == null) {
            return null;
        }
        String idsJsonStr = idsQuey.getIDsJsonStr(globalID);
        if(idsJsonStr != null) {
            Gson gson = new Gson();
            IDs ids = gson.fromJson(idsJsonStr,IDs.class);
            return ids;
        }
        return null;
    }

    public  Map<String, IDs.Info> getTargetDvcMapInfo (String srcId, String srcType, String targetType) throws IOException {
        IDs idsStruct = getIDsByMD5Key(srcId,srcType);
        if(idsStruct == null) {
            return null;
        }
        Map<String,IDs.Info> targetIdMap = (Map<String,IDs.Info>)idsStruct.ids.get(targetType);
        return targetIdMap;
    }

    public synchronized void close() throws InterruptedException {
        if(connectWatcher != null) {
            connectWatcher.close();
        }
        idsQuey.close();
        indexQuery.close();
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        if(args.length == 1) {
            System.out.println("java -jar IDMappingClient srcId ");
            String srcId = args[0];
            IDMappingClient query = new IDMappingClient();
            query.init();
            System.out.println(query.getIDsByPlainKey(srcId,"imei"));
            query.close();
        } else if(args.length == 2) {
            System.out.println("java -jar IDMappingClient inputFile outputFile ");
            IDMappingClient query = new IDMappingClient();
            query.init();
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]),"UTF-8"));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1]),"UTF-8"));
            String id;
            while((id = br.readLine()) != null) {
                bw.write(id + "\t" + query.getIDsByPlainKey(id,"imei") + "\n");
            }
            br.close();
            bw.close();
            query.close();
        } else {
            System.out.println("parameter error ! \n" +
                    "java -jar IDMappingClient srcId  or \n" +
                    "java -jar IDMappingClient inputFile outputFile ");
            return;
        }
    }
}
