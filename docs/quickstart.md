# Quick start

[Documentation](README.md)

This guide runs [`UserCacheExample`](../examples/user-cache/src/main/java/com/red/ohc/example/UserCacheExample.java)
against `red-ohc-core`, then shows how to reuse that cache in an application.
The current development dependency is `1.0.0-SNAPSHOT`; stable `1.0.0` is not yet published.

## Run the example

Install a JDK 11 or later and set `JAVA_HOME` to that JDK. Check `java -version`
and the Wrapper's `./mvnw -version` output. The first build needs network access
for Maven and dependencies; a separate Maven installation is unnecessary.

From a source checkout's root, install the core and launch the standalone example:

```bash
./mvnw -B -DskipTests install
./mvnw -B -f examples/user-cache/pom.xml compile exec:exec
```

For Windows PowerShell:

```powershell
.\mvnw.cmd -B -DskipTests install
.\mvnw.cmd -B -f examples/user-cache/pom.xml compile exec:exec
```

`-DskipTests` makes this an onboarding build, not a full correctness run.
The second command uses a separate Java process. Expect this line followed by Maven's build success:

```text
Red OHC: put/get/replace/remove succeeded
```

The example uses the development library installed in your local Maven repository.
To exclude machine-specific Maven mirrors, add
`-s config/maven/settings.xml -gs config/maven/settings.xml` to both root commands.
The example POM is standalone, so those settings paths are relative to your shell's repository root.

## Complete Java program

The following code is the checked-in example. It creates one shared 128 MiB
cache, uses the default S3-FIFO selector explicitly, and gives ordinary writes a
60-second TTL. Each read returns an independently owned string.

```java
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
```

`serializedSize` returns the UTF-8 byte count, not the character count.
`serialize` advances the buffer position by exactly that count. `deserialize`
copies the borrowed bytes into an owned object; it retains no native view.
The example serializer favors readability; assess serialization allocation in your own workload.

## Use the cache in an application

Create the cache in an application singleton or equivalent startup component.
Inject the same `OHCache` into handlers. Cached data can disappear through TTL,
eviction or removal, so handle `get` returning `null` and reload from your source of truth.

```java
String name = users.get("user:42");
if (name == null) {
    name = loadUserName("42"); // Your database or service call
    if (name != null) {
        users.put("user:42", name);
    }
}
```

This recipe allows concurrent misses to load independently. The [usage guide](usage.md)
explains compute callbacks when per-key coordination is needed. Ordinary request
handling needs no maintenance flush after a successful `put`.

Choose **one** eviction target: `capacity(bytes)` for logical serialized-entry
bytes, or `maxSize(entries)` for entry count. Keep physical memory headroom; these
are asynchronous targets. A cache has process lifetime and no public shutdown
method. Reuse it instead of creating caches per request or per test case.

## Add the dependency

While developing from source, use `1.0.0-SNAPSHOT` after the local install above.
In another Maven application's POM:

```xml
<dependency>
  <groupId>io.github.red-ead</groupId>
  <artifactId>red-ohc-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

After the stable release is published to Maven Central, change the version to
`1.0.0`. No additional `<repositories>` section will be needed. Gradle consumers
will use `mavenCentral()` and `implementation("io.github.red-ead:red-ohc-core:1.0.0")`
after that publication.

To run the example against that future release, omit the local install and use:

```bash
./mvnw -B -f examples/user-cache/pom.xml -Dred-ohc.version=1.0.0 compile exec:exec
```

Continue with [usage recipes](usage.md), [builder and API contracts](api.md), and
[memory sizing and diagnosis](operations.md). The [architecture guide](architecture/ARCHITECTURE.md)
explains the runtime behind those contracts.
