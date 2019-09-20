package com.ph14.fdb.zk.config;

import java.time.Clock;
import java.util.OptionalLong;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;

public class FdbZooKeeperModule extends AbstractModule {

  public static final String EPHEMERAL_NODE_DIRECTORY = "ephemeral-node-directory";
  public static final String SESSION_DIRECTORY = "fdb-zk-sessions-by-id";
  public static final String SESSION_TIMEOUT_DIRECTORY = "fdb-zk-sessions-by-timeout";

  @Override
  protected void configure() {
  }

  @Provides
  Database getFdbDatabase() {
    return FDB.selectAPIVersion(600).open();
  }

  @Provides
  Clock getClock() {
    return Clock.systemUTC();
  }

  @Provides
  @Named("serverTickMillis")
  OptionalLong getServerTickMillis() {
    return OptionalLong.empty();
  }

  // TODO: @Inject the directories so the threads don't compete
  @Provides
  @Named(EPHEMERAL_NODE_DIRECTORY)
  DirectorySubspace getEphemeralNodeDirectory(Database fdb) {
    return null;
  }

}
