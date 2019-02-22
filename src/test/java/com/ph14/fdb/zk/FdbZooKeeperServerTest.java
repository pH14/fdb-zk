package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FdbZooKeeperServerTest {

  @Test
  public void itRunsInProcess() throws Exception {
    int clientPort = 21818; // none-standard
    int numConnections = 5000;
    int tickTime = 2000;
    String dataDirectory = System.getProperty("java.io.tmpdir");

    File dir = new File(dataDirectory).getAbsoluteFile();

    ZooKeeperServer server = new FdbZooKeeperServer(dir, dir, tickTime);
    NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();
    standaloneServerFactory.configure(new InetSocketAddress(clientPort), numConnections);

    standaloneServerFactory.startup(server); // start the server.

    ZooKeeper zooKeeper = new ZooKeeper("localhost:21818", 10000, new Watcher() {
      public void process(WatchedEvent event) {
        LOG.info("Watched event: {}", event.toString());
      }
    });

    while (!zooKeeper.getState().isConnected()) {
    }

    System.out.println("Server state: " + server.serverStats());
    System.out.println("Local hostname: " + standaloneServerFactory.getLocalAddress().getAddress().getHostName());

    System.out.println("Connected: " + zooKeeper.getState().isConnected());
    System.out.println("Alive: " + zooKeeper.getState().isAlive());

    for (ServerCnxn connection : standaloneServerFactory.getConnections()) {
      System.out.println("Connections: " +  connection.toString());
    }

    Stat exists = zooKeeper.exists("/the-path", false);
    System.out.println("Exists: " + exists);

//    zooKeeper.create("/z", new byte[] { 0x7 }, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    exists = zooKeeper.exists("/a", false);

    byte[] data = zooKeeper.getData("/z", false, new Stat());
    LOG.info("Data: {}", data);

    standaloneServerFactory.closeAll();
  }

  private static final Logger LOG = LoggerFactory.getLogger(FdbZooKeeperServerTest.class);

}
