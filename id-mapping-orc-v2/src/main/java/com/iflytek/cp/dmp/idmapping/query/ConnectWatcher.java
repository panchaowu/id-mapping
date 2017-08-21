package com.iflytek.cp.dmp.idmapping.query;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 检测HBase连接
 */
public class ConnectWatcher implements Watcher {
    private CountDownLatch countDownLatch = new CountDownLatch( 1);
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zooKeeper;

    public void connect(String hosts) throws InterruptedException, IOException {
        zooKeeper = new ZooKeeper(hosts,SESSION_TIMEOUT,this);
     //   countDownLatch.await();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    public String getData(String path, Watcher watcher) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, watcher, null);
        return new String(data);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
