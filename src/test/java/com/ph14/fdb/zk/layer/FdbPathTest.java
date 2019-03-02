package com.ph14.fdb.zk.layer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class FdbPathTest {

  @Test
  public void itConvertsToFdbPath() {
    List<String> path = FdbPath.toFdbPath("/abc/foo/bar");
    assertThat(path).containsExactly("abc", "foo", "bar");
  }

  @Test
  public void itConvertsToZkPath() {
    String path = FdbPath.toZkPath(ImmutableList.of("abc", "foo", "bar"));
    assertThat(path).isEqualTo("/abc/foo/bar");
  }

}
