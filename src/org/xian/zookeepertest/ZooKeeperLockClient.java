package org.xian.zookeepertest;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private static String findLock(ZooKeeper zk) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/lock", false);
        for (String child : children) {
            String sessionId = new String(zk.getData("/lock/" + child, false, null));
            if (sessionId.equals(String.valueOf(zk.getSessionId()))) {
                return "/lock/" + child;
            }
        }
        return "";
    }

    private static void requireLock(ZooKeeper zk) throws KeeperException, InterruptedException {
        if (null == zk.exists("/lock", false)) {
            zk.create("/lock", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        String lockPath = findLock(zk);
        if (lockPath.isEmpty()) {
            lockPath = zk.create("/lock/lock-", String.valueOf(zk.getSessionId()).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        System.out.println(MessageFormat.format("Create request {0} for PID {1}", lockPath, pid));

        while(true) {
            latch = new CountDownLatch(1);
            List<String> children = zk.getChildren("/lock", false);
            String smallest = findSmallestNode(children);
            System.out.println("Smallest " + smallest);
            if (lockPath.equals(smallest)) {
                System.out.println(MessageFormat.format("PID {0} get lock", pid));
                Thread.sleep(5000);
                System.out.println("Release lock");
                zk.delete(lockPath, -1);
                break;
            } else {
                System.out.println("Didn't get lock, wait...");
                String listenNode = findListenNode(children, lockPath);
                if (!listenNode.isEmpty()) {
                    if (zk.exists(listenNode, watcher) != null) {
                        latch.await();
                    }
                }
            }
        }
    }

    private static String findSmallestNode(List<String> children) throws KeeperException, InterruptedException {
        TreeMap<Integer, String> id2Node = new TreeMap<Integer, String>();
        for (String node : children) {
            int id = nodeIndex(node);
            id2Node.put(id, "/lock/" + node);
        }
        if (id2Node.isEmpty()) {
            return "";
        } else {
            return id2Node.firstEntry().getValue();
        }
    }

    private static int nodeIndex(String node) {
        return Integer.parseInt(node.substring(node.lastIndexOf("-") + 1, node.length()));
    }


    private static String findListenNode(List<String> children, String currentPath) {
        TreeMap<Integer, String> id2Node = new TreeMap<Integer, String>();
        for (String node : children) {
            int id = nodeIndex(node);
            id2Node.put(id, "/lock/" + node);
        }
        Map.Entry<Integer, String> entry = id2Node.lowerEntry(nodeIndex(currentPath));
        if (entry != null) {
            return entry.getValue();
        } else {
            return "";
        }
    }
}
