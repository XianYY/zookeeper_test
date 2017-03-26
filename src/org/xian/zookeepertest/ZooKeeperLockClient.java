package org.xian.zookeepertest;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/26.
 */
public class ZooKeeperLockClient {

    private static String pid = "";

    private static CountDownLatch latch;

    private static Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                if (event.getPath().equals("/lock")) {
                    System.out.println("locks change");
                }
            }

            latch.countDown();
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {

        pid = getPid();


        ZooKeeper zk = ZooKeeperUtil.connect();
        requireLock(zk);
        


    }


    private static String getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(name);
        // get pid
        return name.split("@")[0];
    }

    private static void requireLock(ZooKeeper zk) throws KeeperException, InterruptedException {
        if (null == zk.exists("/lock", false)) {
            zk.create("/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String lockPath = zk.create("/lock/lock-", pid.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(MessageFormat.format("Create request {0} for PID {1}", lockPath, pid));

        while(true) {
            latch = new CountDownLatch(1);
            String smallest = findSmallestNode(zk);
            System.out.println("Smallest " + smallest);
            if (lockPath.equals("/lock/" + smallest)) {
                System.out.println(MessageFormat.format("PID {0} get lock", pid));
                Thread.sleep(5000);
                System.out.println("Release lock");
                break;
            } else {
                zk.getChildren("/lock", watcher);
                System.out.println("Didn't get lock, wait...");
                latch.await();
            }
        }
    }

    private static String findSmallestNode(ZooKeeper zk) throws KeeperException, InterruptedException {
        TreeMap<Integer, String> id2Node = new TreeMap<Integer, String>();
        for (String node : zk.getChildren("/lock", false)) {
            int id = Integer.parseInt(node.substring(node.lastIndexOf("-") + 1, node.length()));
            id2Node.put(id, node);
        }
        System.out.println(id2Node);
        if (id2Node.isEmpty()) {
            return "";
        } else {
            return id2Node.firstEntry().getValue();
        }
    }
}
