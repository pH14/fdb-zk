package com.ph14.fdb.zk;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class ByteUtilTest {

  @Test
  public void itSplitsByteArrays() {
    List<byte[]> bytes = ByteUtil.divideByteArray(new byte[] { 0x13, 0x32 }, 1);
    assertThat(bytes).containsExactly(new byte[] { 0x13 }, new byte[] { 0x32 });

    bytes = ByteUtil.divideByteArray(new byte[] { 0x13, 0x32 }, 2);
    assertThat(bytes).containsExactly(new byte[] { 0x13, 0x32 });

    bytes = ByteUtil.divideByteArray(new byte[] { 0x13, 0x32, 0x11 }, 2);
    assertThat(bytes).containsExactly(new byte[] { 0x13, 0x32 }, new byte[] { 0x11 });
  }

}
