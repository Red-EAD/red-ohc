package com.red.ohc.jmh;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mapdb.StoreDirect;
import org.mapdb.volume.ByteBufferVol;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FairBackendSupportTest {
  @Test
  public void zeroTtlKeepsEhcacheEntry() {
    try (EhcacheBenchmarkSupport.EhcacheStore store =
        EhcacheBenchmarkSupport.newEhcache(1024 * 1024, 0)) {
      byte[] key = new byte[] {1};
      store.cache().put(key, new byte[] {2, 3});
      Assert.assertEquals(store.cache().get(key), new byte[] {2, 3});
    }
  }

  @Test
  public void renamedActorMatchesLinuxComm() {
    Assert.assertEquals(CpuPerOp.procGroup("red-ohc-mainten"), "actor");
    Assert.assertEquals(CpuPerOp.procGroup("unrelated"), "other");
  }

  @Test
  public void redAdapterMaterializesFullValueAndReclaimsNativeMemory() {
    try (FairCache cache = FairCache.create("RED_OHC", 1024 * 1024, 16, 8, 3, 0)) {
      byte[] key = new byte[8];
      byte[] input = new byte[] {1, 2, 3};
      Assert.assertTrue(cache.put(key, input));
      byte[] result = cache.get(key);
      Assert.assertEquals(result, input);
      result[2] = 99;
      Assert.assertEquals(cache.get(key), input);
    }
  }

  @Test
  public void redisStartupFailureRemovesOwnedDirectory() throws Exception {
    Set<Path> before = redisDirectories();
    String previous = System.getProperty("redohc.redis.executable");
    try {
      System.setProperty("redohc.redis.executable", "/nonexistent-red-ohc-redis-executable");
      Assert.expectThrows(IllegalStateException.class,
          () -> FairCache.create("REDIS", 1024 * 1024, 16, 8, 3, 0));
    } finally {
      if (previous == null) {
        System.clearProperty("redohc.redis.executable");
      } else {
        System.setProperty("redohc.redis.executable", previous);
      }
    }
    Assert.assertEquals(redisDirectories(), before);
  }

  private static Set<Path> redisDirectories() throws Exception {
    try (Stream<Path> paths = Files.list(Path.of(System.getProperty("java.io.tmpdir")))) {
      return paths.filter(path -> path.getFileName().toString().startsWith("red-ohc-redis-"))
          .collect(Collectors.toSet());
    }
  }

  @Test
  public void mapDbUsesDirectVolume() throws Exception {
    try (MapDbBenchmarkSupport.MapDbStore store =
        MapDbBenchmarkSupport.newMapDb(16, 0)) {
      Assert.assertEquals(store.segmentCount(), MapDbBenchmarkSupport.MAPDB_SEGMENTS);
      Method volume = StoreDirect.class.getDeclaredMethod("getVolume");
      volume.setAccessible(true);
      for (org.mapdb.Store segment : store.map().getStores()) {
        Assert.assertTrue(volume.invoke(segment) instanceof ByteBufferVol);
      }
      store.map().put(new byte[] {1}, new byte[] {2, 3});
      Assert.assertEquals(store.map().get(new byte[] {1}), new byte[] {2, 3});
    }
  }
}
