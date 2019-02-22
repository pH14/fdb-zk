package com.ph14.fdb.zk;

import java.util.ArrayList;
import java.util.List;

public class ByteUtil {

  public static List<byte[]> divideByteArray(byte[] source, int chunkSize) {
    int numberOfChunks = (int) Math.ceil(source.length / (double) chunkSize);
    int remainingBytes = source.length;

    List<byte[]> chunks = new ArrayList<>(numberOfChunks);

    int chunkIndex = 0;
    byte[] chunk = new byte[Integer.min(remainingBytes, chunkSize)];

    for (; remainingBytes > 0; remainingBytes--) {
      if (chunkIndex == chunkSize) {
        chunks.add(chunk);
        chunk = new byte[Integer.min(remainingBytes, chunkSize)];
        chunkIndex = 0;
      }

      chunk[chunkIndex] = source[source.length - remainingBytes];
      chunkIndex++;
    }

    chunks.add(chunk);

    return chunks;
  }

}
