package org.xian.zookeepertest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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


    public static String findSmallestNode(String rootPath, List<String> children) throws KeeperException, InterruptedException {
        TreeMap<Integer, String> id2Node = new TreeMap<Integer, String>();
        for (String node : children) {
            int id = nodeIndex(node);
            id2Node.put(id, rootPath + "/" + node);
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


    public static String findPreNode(List<String> children, String currentPath) {
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


    public static String findData(ZooKeeper zk, String rootPath, String data) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(rootPath, false);
        for (String child : children) {
            String path = rootPath + "/" + child;
            byte[] nodeData = zk.getData(path, false, null);
            if (nodeData == null) {
                if (data == null) {
                    return path;
                }
            } else if (new String(nodeData).equals(data)) {
                return path;
            }
        }
        return "";
    }
}
