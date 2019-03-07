package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;

public class StartServer {

  @Test
  public void itRunsInProcess() throws Exception {
    int clientPort = 21818; // none-standard
    int numConnections = 5000;
    String dataDirectory = System.getProperty("java.io.tmpdir");

    File dir = new File(dataDirectory).getAbsoluteFile();

    ZooKeeperServer server = new FdbZooKeeperServer(dir, dir, 100);
    NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();
    standaloneServerFactory.configure(new InetSocketAddress(clientPort), numConnections);

    standaloneServerFactory.startup(server);

    standaloneServerFactory.run();
  }

}
