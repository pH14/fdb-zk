package com.ph14.fdb.zk.layer;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

public class FdbPath {

  private static final Splitter SPLITTER = Splitter.on("/");
  private static final Joiner JOINER = Joiner.on("/");

  public static List<String> toFdbPath(String zkPath) {
    List<String> path = ImmutableList.copyOf(SPLITTER.split(zkPath));
    return path.subList(1, path.size());
  }

  public static String toZkPath(List<String> fdbPath) {
    return "/" + JOINER.join(fdbPath);
  }

}
