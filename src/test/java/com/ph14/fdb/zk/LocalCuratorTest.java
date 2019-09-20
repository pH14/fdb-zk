package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCuratorTest {

  @Test
  public void itRunsInProcess() throws Exception {
    int clientPort = 21818; // none-standard
    int numConnections = 5000;
    int tickTime = 2000;
    String dataDirectory = System.getProperty("java.io.tmpdir");

    File dir = new File(dataDirectory).getAbsoluteFile();

//    ZooKeeperServer server = new ZooKeeperServer(dir, dir, 100);
    ZooKeeperServer server = new FdbZooKeeperServer(dir, dir, 100);

    NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();
    standaloneServerFactory.configure(new InetSocketAddress(clientPort), numConnections);

    standaloneServerFactory.startup(server); // start the server.

    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("localhost:21818", 5000, 5000, new RetryNTimes(10, 10));
    curatorFramework.start();

    CuratorFramework curatorFrameworkTwo = CuratorFrameworkFactory.newClient("localhost:21818", 10000, 5000, new RetryNTimes(10, 10));
    curatorFrameworkTwo.start();

    InterProcessSemaphoreV2 c1 = new InterProcessSemaphoreV2(curatorFramework, "/a-semaphore", 10);
    c1.acquire(2);

    LOG.info("SemaC1: {}", c1.getParticipantNodes());

    InterProcessSemaphoreV2 c2 = new InterProcessSemaphoreV2(curatorFrameworkTwo, "/a-semaphore", 10);
    c2.acquire(5);

    LOG.info("SemaC1: {}", c1.getParticipantNodes());
    LOG.info("SemaC2: {}", c1.getParticipantNodes());

    InterProcessMutex interProcessMutex = new InterProcessMutex(curatorFramework, "/a-lock");

    interProcessMutex.acquire();
    LOG.info("Locked: {}", interProcessMutex.getParticipantNodes());
    interProcessMutex.release();
    LOG.info("Released: {}", interProcessMutex.getParticipantNodes());
    interProcessMutex.acquire();
    LOG.info("Locked: {}", interProcessMutex.getParticipantNodes());

    InterProcessMutex interProcessMutex1 = new InterProcessMutex(curatorFrameworkTwo, "/a-lock");
    interProcessMutex1.acquire();
    LOG.info("Locked from 2: {}", interProcessMutex1.getParticipantNodes());
    interProcessMutex1.release();
    LOG.info("Released from 2: {}", interProcessMutex1.getParticipantNodes());
    interProcessMutex.release();
    LOG.info("Released: {}", interProcessMutex.getParticipantNodes());

    standaloneServerFactory.closeAll();
  }

  private static final Logger LOG = LoggerFactory.getLogger(LocalCuratorTest.class);

}
