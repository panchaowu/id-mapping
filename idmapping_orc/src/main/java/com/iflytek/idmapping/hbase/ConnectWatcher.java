package com.iflytek.idmapping.hbase;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by admin on 2016/9/5.
 */

public class ConnectWatcher implements Watcher{
    private static final int SESSION_TIMEOUT = 5000;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ZooKeeper zk;

    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        countDownLatch.await();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zk.setData(path, data.getBytes(), -1);
    }

    public String getData(String path, Watcher watcher) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, watcher, null);
        return new String(data);
    }

    public void close() throws InterruptedException {
        zk.close();
      //  new ACL(ZooDefs.Perms.READ,new Id("ip","329093"));
    }
}
