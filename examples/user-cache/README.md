# User cache

A standalone application creates one shared 128 MiB cache with the S3-FIFO
selector and a 60-second default TTL. Its checks cover put/get, overwrite and
remove, using a complete UTF-8 serializer.

Run these commands from the repository root. The example uses a separate JVM:

```bash
./mvnw -B -DskipTests install
./mvnw -B -f examples/user-cache/pom.xml compile exec:exec
```

Expected output:

```text
Red OHC: put/get/replace/remove succeeded
```

Prerequisites, Windows commands, dependency setup and the complete source are in
[quick start](../../docs/quickstart.md).
In a server, retain the cache at application scope. This example ends by exiting
its process; it does not expose an application cache shutdown operation.
