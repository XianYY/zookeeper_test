package org.xian.zookeepertest;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/26.
 */
public class ZooKeeperUtil {

    public static class MyWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            System.out.println(event.getState());
            if (event.getState() == Event.KeeperState.SyncConnected) {
                waiter.countDown();
            }
        }
    }

    private static CountDownLatch waiter = new CountDownLatch(1);


    public static ZooKeeper connect() throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("localhost", 5000, new MyWatcher());
        waiter.await();
        return zk;
    }

    public static ZooKeeper connect(MyWatcher watcher) throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("localhost", 5000, watcher);
        waiter.await();
        return zk;
    }
}
