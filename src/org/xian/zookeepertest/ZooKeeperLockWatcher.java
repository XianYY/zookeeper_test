package org.xian.zookeepertest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/26.
 */
public class ZooKeeperLockWatcher {

    private static ZooKeeper zk;

    private static CountDownLatch latch;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    if (event.getPath().equals("/lock")) {
                        System.out.println("lock children changed");
                        try {
                            List<String> children = zk.getChildren("/lock", false);
                            Map<String, String> childContent = new HashMap<String, String>();
                            for (String child : children) {
                                String ip = new String(zk.getData("/lock/" + child, false, null));
                                childContent.put(child, ip);
                            }
                            System.out.println(childContent);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                latch.countDown();
            }
        };
        zk = ZooKeeperUtil.connect();
        System.out.println("Watching");

        while (true) {
            latch = new CountDownLatch(1);
            zk.getChildren("/lock", watcher);
            latch.await();
        }

    }
}
