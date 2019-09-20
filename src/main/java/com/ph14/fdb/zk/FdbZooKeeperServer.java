package com.ph14.fdb.zk;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.OptionalLong;

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
import com.google.inject.Injector;
import com.ph14.fdb.zk.config.FdbZooKeeperModule;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.session.FdbSessionClock;
import com.ph14.fdb.zk.session.FdbSessionDataPurger;
import com.ph14.fdb.zk.session.FdbSessionManager;

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

    getZKDatabase().setlastProcessedZxid(Long.MAX_VALUE);

    super.startup();
  }

  @Override
  protected void createSessionTracker() {
    sessionTracker = new FdbSessionManager(FDB.selectAPIVersion(600).open(), Clock.systemUTC(), OptionalLong.empty());
  }

  @Override
  protected void startSessionTracker() {
  }

  @Override
  public void expire(Session session) {
    LOG.debug("Expiring session: {}", session);
    super.expire(session);
  }

  @Override
  public void loadData() {
  }

  @Override
  protected void killSession(long sessionId, long zxid) {
  }

  @Override
  protected void setupRequestProcessors() {
    super.setupRequestProcessors();

    Injector injector = Guice.createInjector(new FdbZooKeeperModule());

    FdbZooKeeperImpl fdbZooKeeper = injector.getInstance(FdbZooKeeperImpl.class);

    FdbSessionClock fdbSessionClock = new FdbSessionClock(
        injector.getInstance(Database.class),
        Clock.systemUTC(),
        OptionalLong.empty(),
        (FdbSessionManager) sessionTracker,
        injector.getInstance(FdbSessionDataPurger.class));

    fdbSessionClock.run();

    this.firstProcessor = new FdbRequestProcessor(sessionTracker, firstProcessor, getZKDatabase(), fdbZooKeeper);
  }

}
