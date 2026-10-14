# Quick start

[Documentation](README.md)

This guide runs [`UserCacheExample`](../examples/user-cache/src/main/java/com/red/ohc/example/UserCacheExample.java)
against `red-ohc-core`, then shows how to reuse that cache in an application.
The release version is `1.0.0`. Check [GitHub releases](https://github.com/Red-EAD/red-ohc/releases)
for publication status; the source-checkout example works before Central publication.

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

The example uses the source-built library installed in your local Maven repository.
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

After `1.0.0` is published to Maven Central, add this inside your application's
`<dependencies>`. Maven uses Central by default, so no custom repository or
download credentials are needed:

```xml
<dependency>
  <groupId>io.github.red-ead</groupId>
  <artifactId>red-ohc-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

For Gradle Kotlin DSL:

```kotlin
repositories {
    mavenCentral()
}
dependencies {
    implementation("io.github.red-ead:red-ohc-core:1.0.0")
}
```

To run the example against the published release, omit the local install:

```bash
./mvnw -B -f examples/user-cache/pom.xml compile exec:exec
```

The example defaults to `1.0.0`. To evaluate another published version, pass
`-Dred-ohc.version=<version>` to that command.

### Development snapshots

SNAPSHOTs are optional development builds and can change. For the published
`1.0.0-SNAPSHOT` development line, add this repository at project level and the
dependency inside `<dependencies>`:

```xml
<repositories>
  <repository>
    <id>central-portal-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots/</url>
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>
<dependency>
  <groupId>io.github.red-ead</groupId>
  <artifactId>red-ohc-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

No account or credentials are needed. Use `mvn -U` to refresh a cached SNAPSHOT.
Development publication requires a `-SNAPSHOT` main version and successful CI;
stable main versions are skipped. Fast consecutive merges can be consolidated
into the latest verified main. See the [publication procedure](releasing.md#development-snapshots)
for the gate and verification record.

Continue with [usage recipes](usage.md), [builder and API contracts](api.md), and
[memory sizing and diagnosis](operations.md). The [architecture guide](architecture/ARCHITECTURE.md)
explains the runtime behind those contracts.
