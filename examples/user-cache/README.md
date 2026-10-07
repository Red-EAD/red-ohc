# User cache / 用户缓存

This standalone Java process creates one shared cache with 128 MiB logical
capacity, S3-FIFO and a 60-second TTL, then verifies put/get/replace/remove.
The UTF-8 serializer owns returned strings. The process exits after the example;
the same cache should be retained at application scope in a server.

从仓库根目录运行，先将当前开发版安装到本机 Maven 仓库：

```bash
./mvnw -B -DskipTests install
./mvnw -B -f examples/user-cache/pom.xml compile exec:exec
```

After the stable release is available, use `-Dred-ohc.version=1.0.0` and omit
the local install. No custom Maven repository is needed.
