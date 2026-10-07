package com.red.ohc.jmh;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

/** A private Redis process; never connects to or reconfigures an existing server. */
public final class RedisAdapter implements FairCache {
  private Process server;
  private JedisPooled client;
  private Path directory;
  private long ttlMillis;

  @Override
  public void open(long bytes, long entries, int keyBytes, int valueBytes, long ttl) {
    ttlMillis = ttl;
    try {
      directory = Files.createTempDirectory("red-ohc-redis-");
      int port;
      try (ServerSocket reservation = new ServerSocket(0, 0,
          java.net.InetAddress.getLoopbackAddress())) {
        port = reservation.getLocalPort();
      }
      String password = UUID.randomUUID().toString();
      server = new ProcessBuilder(System.getProperty("redohc.redis.executable", "redis-server"),
          "--bind", "127.0.0.1", "--port", Integer.toString(port), "--save", "",
          "--appendonly", "no", "--requirepass", password, "--maxmemory", Long.toString(bytes),
          "--maxmemory-policy", "allkeys-lru", "--dir", directory.toString())
          .redirectErrorStream(true).redirectOutput(directory.resolve("server.log").toFile()).start();
      client = new JedisPooled(new HostAndPort("127.0.0.1", port),
          DefaultJedisClientConfig.builder().password(password)
              .connectionTimeoutMillis(200).socketTimeoutMillis(1000).build());
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (true) {
        if (!server.isAlive()) {
          throw new IllegalStateException("owned Redis failed to start");
        }
        try {
          if ("PONG".equals(client.ping())) {
            break;
          }
        } catch (redis.clients.jedis.exceptions.JedisConnectionException waiting) {
          if (System.nanoTime() >= deadline) {
            throw new IllegalStateException("owned Redis startup timed out", waiting);
          }
        }
        Thread.sleep(20);
      }
      System.out.println("RED_OHC_REDIS_PID=" + server.pid());
      try (Jedis metadata = new Jedis(new HostAndPort("127.0.0.1", port),
          DefaultJedisClientConfig.builder().password(password).build())) {
        String version = null;
        for (String line : metadata.info("server").split("\r?\n")) {
          if (line.startsWith("redis_version:")) {
            version = line.substring("redis_version:".length());
          }
        }
        if (version == null) {
          throw new IllegalStateException("owned Redis did not report its version");
        }
        System.out.println("RED_OHC_REDIS_VERSION=" + version);
        System.out.println("RED_OHC_REDIS_MAXMEMORY=" + metadata.configGet("maxmemory").get("maxmemory"));
      }
    } catch (IOException failure) {
      throw new IllegalStateException("redis-server is required for the Redis comparison", failure);
    } catch (InterruptedException interruption) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Redis setup interrupted", interruption);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    return client.get(key);
  }

  @Override
  public boolean put(byte[] key, byte[] value) {
    String response = ttlMillis == 0 ? client.set(key, value)
        : client.set(key, value, SetParams.setParams().px(ttlMillis));
    return "OK".equals(response);
  }

  @Override
  public void close() {
    Throwable failure = null;
    if (client != null) {
      try {
        client.close();
      } catch (RuntimeException cleanup) {
        failure = cleanup;
      }
      client = null;
    }
    if (server != null) {
      Process owned = server;
      server = null;
      owned.destroy();
      try {
        if (!owned.waitFor(5, TimeUnit.SECONDS)) {
          owned.destroyForcibly();
          if (!owned.waitFor(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("owned Redis did not exit");
          }
        }
      } catch (InterruptedException interruption) {
        owned.destroyForcibly();
        Thread.currentThread().interrupt();
        if (failure != null) {
          interruption.addSuppressed(failure);
        }
        failure = interruption;
      } catch (RuntimeException cleanup) {
        if (failure != null) {
          cleanup.addSuppressed(failure);
        }
        failure = cleanup;
      }
    }
    if (directory != null) {
      try {
        Files.deleteIfExists(directory.resolve("server.log"));
        Files.delete(directory);
      } catch (IOException cleanup) {
        if (failure != null) {
          cleanup.addSuppressed(failure);
        }
        failure = cleanup;
      }
      directory = null;
    }
    if (failure != null) {
      throw new IllegalStateException("Redis teardown failed", failure);
    }
  }
}
