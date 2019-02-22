package com.ph14.fdb.zk.layer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.junit.Test;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Strings;
import com.ph14.fdb.zk.FdbBaseTest;

public class FdbNodeSerialization extends FdbBaseTest {

  @Test
  public void itWritesAndReadsFdbNodes() {
    String path = "/foo/bar/abd/wow";

    FdbNode fdbNode = new FdbNode(
        path,
        new StatPersisted(123L, 456L, System.currentTimeMillis(), Long.MAX_VALUE - System.currentTimeMillis(), 1337, 7331, 9001, 1L, 2L),
        Strings.repeat("hello this is a data block isn't that neat?", 10000).getBytes(),
        Arrays.asList(
            new ACL(123, new Id("a schema", "id!")),
            new ACL(456, new Id("another schema", "!id"))
        ));

    DirectorySubspace subspace = DirectoryLayer.getDefault().create(transaction, fdbNode.getSplitPath()).join();

    new FdbNodeWriter(subspace).serialize(fdbNode).forEach(kv -> transaction.set(kv.getKey(), kv.getValue()));

    List<KeyValue> keyValues = transaction.getRange(subspace.range()).asList().join();

    FdbNode fetchedFdbNode = new FdbNodeReader(subspace).deserialize(keyValues);

    assertThat(fetchedFdbNode).isEqualToComparingFieldByField(fdbNode);
  }

}
