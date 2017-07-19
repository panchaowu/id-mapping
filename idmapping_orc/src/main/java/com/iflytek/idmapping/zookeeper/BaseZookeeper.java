package com.iflytek.idmapping.zookeeper;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class BaseZookeeper implements Watcher{

    private ZooKeeper zookeeper;
    private static final int SESSION_TIME_OUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    @Override
    public void process(WatchedEvent event) {

        if (event.getState() == KeeperState.SyncConnected) {
            System.out.println("Watch received event");
            countDownLatch.countDown();
        }
    }

    /**连接zookeeper
     * @param host
     * @throws Exception
     */
    public void connectZookeeper(String host) throws Exception{
        zookeeper = new ZooKeeper(host, SESSION_TIME_OUT, this);
        countDownLatch.await();
        System.out.println("zookeeper connection success");
    }

    /**
     * 创建节点
     * @param path
     * @param data
     * @throws Exception
     */
    public String createNode(String path,String data) throws Exception{
        return this.zookeeper.create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获取路径下所有子节点
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public List<String> getChildren(String path) throws KeeperException, InterruptedException{
        List<String> children = zookeeper.getChildren(path, false);
        return children;
    }

    /**
     * 获取节点上面的数据
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public String getData(String path) throws KeeperException, InterruptedException{
        byte[] data = zookeeper.getData(path, false, null);
        if (data == null) {
            return "";
        }
        return new String(data);
    }

    /**
     * 设置节点信息
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Stat setData(String path,String data) throws KeeperException, InterruptedException{
        Stat stat = zookeeper.setData(path, data.getBytes(), -1);
        return stat;
    }

    /**
     * 删除节点
     * @param path
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void deleteNode(String path) throws InterruptedException, KeeperException{
        zookeeper.delete(path, -1);
    }

    /**
     * 获取创建时间
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
//    public String getCTime(String path) throws KeeperException, InterruptedException{
//        Stat stat = zookeeper.exists(path, false);
//        return DateUtil.longToString(String.valueOf(stat.getCtime()));
//    }

    /**
     * 获取某个路径下孩子的数量
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Integer getChildrenNum(String path) throws KeeperException, InterruptedException{
        int childenNum = zookeeper.getChildren(path, false).size();
        return childenNum;
    }
    /**
     * 关闭连接
     * @throws InterruptedException
     */
    public void closeConnection() throws InterruptedException{
        if (zookeeper != null) {
            zookeeper.close();
        }
    }


    public static String toStringHex(String s)
    {
        byte[] baKeyword = new byte[s.length()/2];
        for(int i = 0; i < baKeyword.length; i++)
        {
            try
            {
                baKeyword[i] = (byte)(0xff & Integer.parseInt(s.substring(i*2, i*2+2),16));
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        try
        {
            s = new String(baKeyword, "utf-8");//UTF-16le:Not
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }
        return s;
    }

    public static void main(String[] args) throws Exception {
        String hex = "E5B08FE7B1B3E7BAA2E7B1B3";
        String to = toStringHex(hex);


        BaseZookeeper zoo = new BaseZookeeper();
        zoo.connectZookeeper("172.21.191.19,172.21.191.20,172.21.191.21");

        zoo.createNode("/idmapping","");

//        String idsTableName = zoo.getData("/idmapping/active_ids");
//        System.out.println("idstable:" + idsTableName);
        String newNode1 = zoo.createNode("/idmapping/active_ids2hbase","idmapping_ids2hbase_1");
        System.out.println("newNode:" + newNode1);
        String newNode2 = zoo.createNode("/idmapping/active_index2hbase","idmapping_index2hbase_1");
        System.out.println("newNode2:" + newNode2);
//        String ids2hbase = zoo.getData("/idmapping/active_ids2hbase");
//        System.out.println("ids2hbasetable:" + ids2hbase);
//        zoo.setData("/idmapping/active_ids2hbase","changed_ids2hbase");
//        String changedIdsTable = zoo.getData("/idmapping/active_ids2hbase");
//        System.out.println("changedidsTable:" + changedIdsTable);
//        zoo.deleteNode("/idmapping/active_ids2hbase");
        zoo.closeConnection();
    }
}
