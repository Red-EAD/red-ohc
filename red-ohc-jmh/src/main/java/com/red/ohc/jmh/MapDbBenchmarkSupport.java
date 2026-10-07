package com.red.ohc.jmh;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

/** Backend-specific support keeps unrelated libraries out of isolated runtime graphs. */
public final class MapDbBenchmarkSupport {
  public static final int MAPDB_SEGMENTS = 16;
  private MapDbBenchmarkSupport() {}

  public static MapDbStore newMapDb(long capacityEntries, long ttlMillis) {
    ScheduledExecutorService expiryExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "mapdb-expiry");
              thread.setDaemon(true);
              return thread;
            });
    DB db = null;
    try {
      db = DBMaker.memoryDirectDB().make();
      HTreeMap<byte[], byte[]> map =
        db.hashMap("serialized")
            .layout(MAPDB_SEGMENTS, 128, 4)
            .keySerializer(org.mapdb.Serializer.BYTE_ARRAY)
            .valueSerializer(org.mapdb.Serializer.BYTE_ARRAY)
            .expireAfterCreate(ttlMillis, TimeUnit.MILLISECONDS)
            .expireAfterUpdate(ttlMillis, TimeUnit.MILLISECONDS)
            .expireMaxSize(capacityEntries)
            .expireExecutor(expiryExecutor)
            .expireExecutorPeriod(TimeUnit.SECONDS.toMillis(1L))
            .create();
      return new MapDbStore(map, expiryExecutor, db);
    } catch (RuntimeException | Error failure) {
      expiryExecutor.shutdownNow();
      if (db != null) {
        try {
          db.close();
        } catch (Throwable cleanup) {
          failure.addSuppressed(cleanup);
        }
      }
      throw failure;
    }
  }

  public static final class MapDbStore implements AutoCloseable {
    private final HTreeMap<byte[], byte[]> map;
    private final ScheduledExecutorService expiryExecutor;
    private final DB db;

    private MapDbStore(HTreeMap<byte[], byte[]> map, ScheduledExecutorService expiryExecutor, DB db) {
      this.map = map;
      this.expiryExecutor = expiryExecutor;
      this.db = db;
    }

    public HTreeMap<byte[], byte[]> map() {
      return map;
    }

    public int segmentCount() {
      return map.getStores().length;
    }

    @Override
    public void close() {
      Throwable failure = null;
      boolean interrupted = false;
      try {
        expiryExecutor.shutdownNow();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!expiryExecutor.isTerminated()) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) {
            throw new IllegalStateException("MapDB expiry executor did not stop");
          }
          try {
            expiryExecutor.awaitTermination(remaining, TimeUnit.NANOSECONDS);
          } catch (InterruptedException interruption) {
            interrupted = true;
          }
        }
      } catch (RuntimeException | Error cleanup) {
        failure = cleanup;
      } finally {
        try {
          db.close();
        } catch (RuntimeException | Error cleanup) {
          if (failure == null) {
            failure = cleanup;
          } else {
            failure.addSuppressed(cleanup);
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (failure != null) {
        throw new IllegalStateException("MapDB teardown failed", failure);
      }
    }
  }
}
