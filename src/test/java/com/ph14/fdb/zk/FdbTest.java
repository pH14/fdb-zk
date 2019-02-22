package com.ph14.fdb.zk;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

public class FdbTest {

  private static final Logger LOG = LoggerFactory.getLogger(FdbTest.class);

  @Test
  public void itDoes() {
    LOG.info("{}", 54 / 100);
  }

  @Test
  public void itRunsInProcess() throws Exception {
    Database fdb = FDB.selectAPIVersion(600).open();

    fdb.run(t -> {
      DirectorySubspace subspace = DirectoryLayer.getDefault().create(t, ImmutableList.of("a", "b")).join();

      LOG.info("Subspace: {}", subspace);
      LOG.info("Packed: {}", subspace.pack(Tuple.from(123)));
      LOG.info("Path: {}", subspace.getPath());

      return null;
    });

    fdb.close();
  }

}
