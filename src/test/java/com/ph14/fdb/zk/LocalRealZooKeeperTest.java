package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalRealZooKeeperTest {

  @Test
  public void itRunsInProcess() throws Exception {
    int clientPort = 21818; // none-standard
    int numConnections = 5000;
    int tickTime = 2000;
    String dataDirectory = System.getProperty("java.io.tmpdir");

    File dir = new File(dataDirectory).getAbsoluteFile();

    ZooKeeperServer server = new ZooKeeperServer(dir, dir, 100);
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

    zooKeeper.create("/start", "hello".getBytes(), Ids.READ_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

    Stat exists = zooKeeper.exists("/start0000000001", false);
    System.out.println("Exists: " + exists);
    exists = zooKeeper.exists("/start0000000002", false);
    System.out.println("Exists: " + exists);
    exists = zooKeeper.exists("/start0000000003", false);
    System.out.println("Exists: " + exists);
    exists = zooKeeper.exists("/", false);
    System.out.println("Exists: " + exists);

    List<String> children = zooKeeper.getChildren("/", false);
    LOG.info("Root level children: {}", children);

    standaloneServerFactory.closeAll();
  }

  private static final Logger LOG = LoggerFactory.getLogger(LocalRealZooKeeperTest.class);

}
