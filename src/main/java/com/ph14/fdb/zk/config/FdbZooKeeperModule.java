package com.ph14.fdb.zk.config;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class FdbZooKeeperModule extends AbstractModule {

  @Override
  protected void configure() {
  }

  @Provides
  Database getFdbDatabase() {
    return FDB.selectAPIVersion(600).open();
  }

}
