package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
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

    LOG.info("Children: {}", zooKeeper.getChildren("/the-path2", false));
    LOG.info("Exists: {}", zooKeeper.exists("/the-path2", false).getCversion());
    zooKeeper.delete("/the-path2/more-path", -1);
    zooKeeper.delete("/the-path2/even-more-path", -1);
//    zooKeeper.delete("/the-path2", -1);
    String s = zooKeeper.create("/the-path2", "some data goes here!".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    LOG.info("Stat: {}", zooKeeper.exists("/the-path", false));

    zooKeeper.create("/the-path2/more-path", "some data goes here!".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    zooKeeper.create("/the-path2/even-more-path", "some data goes here!".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    LOG.info("Children: {}", zooKeeper.getChildren("/the-path2", false));

    standaloneServerFactory.closeAll();
  }

  private static final Logger LOG = LoggerFactory.getLogger(FdbZooKeeperServerTest.class);

}
