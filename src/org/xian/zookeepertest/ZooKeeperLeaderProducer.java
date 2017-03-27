package org.xian.zookeepertest;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by chenxian on 17/3/27.
 */
public class ZooKeeperLeaderProducer {

    private static final String LEADER_ROOT = "/leader";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = ZooKeeperUtil.connect();

        final String sessionId = String.valueOf(zk.getSessionId());
        String nodePath = ZooKeeperUtil.findData(zk, LEADER_ROOT, sessionId);
        if (nodePath.isEmpty()) {
            zk.create(LEADER_ROOT + "/leader-", sessionId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
        System.out.println(String.format("Candidate %s is ready", sessionId));

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
}
