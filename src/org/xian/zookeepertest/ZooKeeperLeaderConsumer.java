package org.xian.zookeepertest;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/27.
 */
public class ZooKeeperLeaderConsumer {

    private static final String LEADER_ROOT = "/leader";

    private static class LeaderWatcher implements Watcher {

        private CountDownLatch latch;
        public LeaderWatcher(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeDeleted) {
                latch.countDown();
            }
        }
    }

    private static class ChildrenWatcher implements Watcher {

        private CountDownLatch latch;
        public ChildrenWatcher(CountDownLatch latch) {
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

        while (true) {
            CountDownLatch latch = new CountDownLatch(1);
            List<String> children = zk.getChildren(LEADER_ROOT, false);
            if (children.isEmpty()) {
                System.out.println("No candidate");
                zk.getChildren(LEADER_ROOT, new ChildrenWatcher(latch));
            } else {
                String leader = ZooKeeperUtil.findSmallestNode(LEADER_ROOT, children);
                String sessionId = new String(zk.getData(leader, false, null));
                System.out.println("Leader " + sessionId);
                zk.exists(leader, new LeaderWatcher(latch));
            }


            latch.await();
        }
    }
}
