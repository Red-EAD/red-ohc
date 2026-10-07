package com.red.ohc.jmh;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/** Backend-specific support keeps unrelated libraries out of isolated runtime graphs. */
public final class ChronicleBenchmarkSupport {
  private ChronicleBenchmarkSupport() {}

  public static ChronicleMapStore newChronicleMap(int keyBytes, int valueBytes, long entries) {
    ChronicleMap<byte[], byte[]> map =
        ChronicleMapBuilder.of(byte[].class, byte[].class)
            .entries(entries)
            .averageKeySize(keyBytes)
            .averageValueSize(valueBytes)
            .create();
    return new ChronicleMapStore(map);
  }

  public static final class ChronicleMapStore implements AutoCloseable {
    private final ChronicleMap<byte[], byte[]> map;

    private ChronicleMapStore(ChronicleMap<byte[], byte[]> map) {
      this.map = map;
    }

    public ChronicleMap<byte[], byte[]> map() {
      return map;
    }

    @Override
    public void close() {
      map.close();
    }
  }
}
