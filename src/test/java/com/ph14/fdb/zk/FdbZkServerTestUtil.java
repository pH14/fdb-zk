package com.ph14.fdb.zk;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public class FdbZkServerTestUtil {

  public static ZooKeeperServer newFdbZkServer(int port) throws Exception {
    int numConnections = 5000;
    int tickTime = 2000;
    String dataDirectory = System.getProperty("java.io.tmpdir");

    File dir = new File(dataDirectory).getAbsoluteFile();

    ZooKeeperServer server = new FdbZooKeeperServer(tickTime);
    NIOServerCnxnFactory standaloneServerFactory = new NIOServerCnxnFactory();

    standaloneServerFactory.configure(new InetSocketAddress(port), numConnections);
    standaloneServerFactory.startup(server);

    return server;
  }

}
