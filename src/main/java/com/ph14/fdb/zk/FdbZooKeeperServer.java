package com.ph14.fdb.zk;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.inject.Guice;
import com.ph14.fdb.zk.config.FdbZooKeeperModule;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.session.FdbSessionTracker;

public class FdbZooKeeperServer extends ZooKeeperServer {

  private static final Logger LOG = LoggerFactory.getLogger(FdbZooKeeperServer.class);

  public FdbZooKeeperServer(File snapDir, File logDir, int tickTime) throws IOException {
    super(snapDir, logDir, tickTime);
  }

  @Override
  public void startup() {
    System.out.println("Starting up the server");

    try (Database fdb = FDB.selectAPIVersion(600).open()) {
      fdb.run(tr -> {
        boolean rootNodeAlreadyExists = DirectoryLayer.getDefault().exists(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();

        if (!rootNodeAlreadyExists) {
          LOG.info("Creating root path '/'");
          DirectorySubspace rootSubspace = DirectoryLayer.getDefault().create(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();
          new FdbNodeWriter().createNewNode(tr, rootSubspace, new FdbNode("/", null, new byte[0], Ids.OPEN_ACL_UNSAFE));
        }

        return null;
      });
    }

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
