package org.xian.zookeepertest;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/21.
 */
public class ZooKeeperTest {



    private static class LockRootWatcher implements Watcher {

        private CountDownLatch latch;

        public LockRootWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                latch.countDown();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {


        ZooKeeper zk = ZooKeeperUtil.connect();

//        requireLock(zk);

////        zk.delete("/test/1", -1);
////        zk.delete("/test", -1);
//        String createPath = zk.create("/test", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//        zk.create("/test/1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
////        System.out.println(createPath);
//
//        List<String> children = zk.getChildren("/", watcher);
//        System.out.println(children);

        zk.close();
    }




}
