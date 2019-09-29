package com.ph14.fdb.zk.curator;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ph14.fdb.zk.FdbZkServerTestUtil;

public class LeaderCandidate {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderCandidate.class);

  public void run(int id, int numParticipants, List<Integer> numberOfParticipantsPerRound) throws Exception {
    int port = 21818 + id;
    ZooKeeperServer zooKeeperServer = FdbZkServerTestUtil.newFdbZkServer(port);

    CuratorFramework curator = CuratorFrameworkFactory.newClient("localhost:" + port, 10000, 5000, new RetryNTimes(10, 10));
    curator.start();
    curator.blockUntilConnected();

    long sessionId = curator.getZookeeperClient().getZooKeeper().getSessionId();

    LeaderLatch leaderLatch = new LeaderLatch(curator, "/leader-latch");

    CountDownLatch isLeaderCountdownLatch = new CountDownLatch(1);
    leaderLatch.addListener(new LeaderLatchListener() {
      @Override
      public void isLeader() {
        LOG.info("Session: {} became the leader", sessionId);
        isLeaderCountdownLatch.countDown();
      }

      @Override
      public void notLeader() {
        throw new IllegalStateException("");
      }
    });

    leaderLatch.start();

    while (leaderLatch.getParticipants().size() < numParticipants) {
      Thread.sleep(10);
    }

    // kill once elected leader to reduce participants by 1
    if (leaderLatch.await(10, TimeUnit.SECONDS)) {
      LOG.info("Session {} elected leader. {} participants", sessionId, leaderLatch.getParticipants().size());
      numberOfParticipantsPerRound.add(leaderLatch.getParticipants().size());

      leaderLatch.close();
      zooKeeperServer.shutdown();
      return;
    }

    throw new IllegalStateException("should have been elected leader");
  }

}
