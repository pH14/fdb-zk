package com.ph14.fdb.zk.layer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.StatPersisted;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

public class FdbNode {

  private final String path;
  private final StatPersisted stat;
  private final byte[] data;
  private final List<ACL> acls;

  public FdbNode(String path, StatPersisted stat, byte[] data, List<ACL> acls) {
    this.path = path;
    this.stat = stat;
    this.data = data;
    this.acls = acls;
  }

  public String getPath() {
    return path;
  }

  public List<String> getSplitPath() {
    return ImmutableList.copyOf(path.split("/"));
  }

  public StatPersisted getStat() {
    return stat;
  }

  public byte[] getData() {
    return data;
  }

  public List<ACL> getAcls() {
    return acls;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof FdbNode) {
      final FdbNode that = (FdbNode) obj;
      return Objects.equals(this.getPath(), that.getPath())
          && Objects.equals(this.getStat(), that.getStat())
          && Arrays.equals(this.getData(), that.getData())
          && Objects.equals(this.getAcls(), that.getAcls());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPath(), getStat(), getData(), getAcls());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("path", path)
        .add("stat", stat)
        .add("data", data)
        .add("acls", acls)
        .toString();
  }
}
