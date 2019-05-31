package com.ph14.fdb.zk.layer;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.MoreObjects;

public class FdbNode {

  private final String zkPath;
  private final Stat stat;
  private final byte[] data;
  private final List<ACL> acls;

  public FdbNode(String zkPath, Stat stat, byte[] data, List<ACL> acls) {
    this.zkPath = zkPath;
    this.stat = stat;
    this.data = data;
    this.acls = acls;
  }

  public String getZkPath() {
    return zkPath;
  }

  public List<String> getFdbPath() {
    return FdbPath.toFdbPath(zkPath);
  }

  public Stat getStat() {
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
      return Objects.equals(this.getZkPath(), that.getZkPath())
          && Objects.equals(this.getStat(), that.getStat())
          && Arrays.equals(this.getData(), that.getData())
          && Objects.equals(this.getAcls(), that.getAcls());
    }
    return false;
  }

  @Override
  @SuppressWarnings("ArrayHashCode")
  public int hashCode() {
    return Objects.hash(getZkPath(), getStat(), getData(), getAcls());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("zkPath", zkPath)
        .add("stat", stat)
        .add("data", data)
        .add("acls", acls)
        .toString();
  }
}
