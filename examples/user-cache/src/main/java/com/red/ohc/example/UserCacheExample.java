package com.red.ohc.example;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.red.ohc.api.CacheSerializer;
import com.red.ohc.api.Eviction;
import com.red.ohc.api.OHCache;
import com.red.ohc.cache.OHCacheBuilder;

/** A cache shared for the lifetime of this application process. */
public final class UserCacheExample {
  private static final OHCache<String, String> USERS =
      OHCacheBuilder.<String, String>newBuilder()
          .capacity(128L * 1024 * 1024)
          .defaultTTLmillis(60_000)
          .eviction(Eviction.S3_FIFO)
          .keySerializer(new StringSerializer())
          .valueSerializer(new StringSerializer())
          .build();

  private UserCacheExample() {}

  public static void main(String[] args) {
    USERS.put("user:42", "Alice");
    require("Alice".equals(USERS.get("user:42")));
    USERS.put("user:42", "Bob");
    require("Bob".equals(USERS.get("user:42")));
    USERS.remove("user:42");
    require(USERS.get("user:42") == null);
    System.out.println("Red OHC: put/get/replace/remove succeeded");
  }

  private static void require(boolean condition) {
    if (!condition) {
      throw new IllegalStateException("unexpected cache result");
    }
  }

  public static final class StringSerializer implements CacheSerializer<String> {
    @Override
    public int serializedSize(String value) {
      return value.getBytes(StandardCharsets.UTF_8).length;
    }

    @Override
    public void serialize(String value, ByteBuffer target) {
      target.put(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String deserialize(ByteBuffer source) {
      byte[] bytes = new byte[source.remaining()];
      source.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }
}
