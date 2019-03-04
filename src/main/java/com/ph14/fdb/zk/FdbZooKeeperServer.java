package com.ph14.fdb.zk;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.FDB;
import com.google.inject.Guice;
import com.ph14.fdb.zk.config.FdbZooKeeperModule;
import com.ph14.fdb.zk.session.FdbSessionTracker;

public class FdbZooKeeperServer extends ZooKeeperServer {

  private static final Logger LOG = LoggerFactory.getLogger(FdbZooKeeperServer.class);

  public FdbZooKeeperServer(File snapDir, File logDir, int tickTime) throws IOException {
    super(snapDir, logDir, tickTime);
  }

  @Override
  public void startup() {
    System.out.println("Starting up the server");
    super.startup();
  }

  @Override
  protected void createSessionTracker() {
    sessionTracker = new FdbSessionTracker(FDB.selectAPIVersion(600).open(), this);
  }

  @Override
  protected void startSessionTracker() {
//    super.startSessionTracker();
  }

  @Override
  public void expire(Session session) {
    LOG.info("Expiring session: {}", session);
    super.expire(session);
  }

  @Override
  protected void setupRequestProcessors() {
    super.setupRequestProcessors();

    FdbZooKeeperImpl fdbZooKeeper = Guice.createInjector(new FdbZooKeeperModule()).getInstance(FdbZooKeeperImpl.class);
    this.firstProcessor = new FdbRequestProcessor(sessionTracker, firstProcessor, fdbZooKeeper);
  }

}
