# Red OHC

[English](README.md) · [API 说明](docs/api.md) · [公平 benchmark](docs/benchmarks.md)

Red OHC 是面向 Java 11 及以上版本的序列化堆外缓存。索引由 JDK
`ConcurrentHashMap` 维护，key/value 使用 native 内存。调用线程通过独立
lifecycle lane 发布变更，一个维护 actor 负责容量淘汰、TTL 和读者退出后的回收。

当前是首个公开版本的源码候选，开发版本为 `1.0.0-SNAPSHOT`，首发计划为
`1.0.0`；目前尚无 Maven Central 发行包。完成发行后，直接添加依赖即可，
不需要配置额外的 repository：

```xml
<dependency>
  <groupId>io.github.red-ead</groupId>
  <artifactId>red-ohc-core</artifactId>
  <version>1.0.0</version>
</dependency>
```

在应用启动时创建一个缓存并共享，使用 `put/get/remove` 操作。
[完整案例](examples/user-cache/README.md) 包含 UTF-8 serializer、128 MiB 容量、
写入、查询、覆盖和删除，并提供可运行命令。

使用时需要理解以下约定：

- key 的身份取决于序列化后的字节，serializer 必须确定、线程安全，长度与实际写入一致。
- `capacity/maxSize` 是异步维护目标，并不是 RSS 或瞬时 native 分配的硬上限。
- 支持 LRU、S3-FIFO 和 TTL。过期条目在读取时逻辑不可见，物理回收可能稍后完成。
- `flushAsync()` 等待调用时捕获的生命周期与退休水位。捕获前预留的记录必须提交或取消，
  捕获后产生的写入与新 lane 不会无限扩大边界；必要的 actor 回收会扩展退休水位。
  它用于维护收敛，不提供持久化或事务语义，正常请求不需要逐次 flush。
- 缓存按进程生命周期使用，没有公开的 `close/stop/destroy`。不要每个请求创建缓存；
  `clear/flush` 不销毁缓存基础设施。测试和 benchmark 使用内部辅助类回收。
- direct-read 的 ByteBuffer 仅在回调内部有效，不可保存或交给其他线程。
- native 后端已有 Linux/macOS/Windows 实现；跨平台、跨 JDK 的完整验证仍需分别执行，
  不能把实现存在当成验证通过。

Maven Wrapper 固定 Maven 3.9.16，并检查下载 SHA-256；无需自行安装 Maven。
默认只构建核心，benchmark 使用独立 profile：

```bash
./mvnw -B clean verify
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am clean verify
./mvnw -B -Pbenchmarks -pl red-ohc-core,red-ohc-jmh -am checkstyle:check
git diff --check
```

可加 `-s config/maven/settings.xml -gs config/maven/settings.xml` 排除本机私有 Maven 配置。
API 和失败语义见 [API 文档](docs/api.md)，验证范围见 [支持说明](docs/support.md)。

公平对比覆盖 Red OHC、[snazy/OHC](https://github.com/snazy/ohc)、Ehcache、MapDB、
Chronicle Map 和 Redis。各框架使用普通 API，公开说明容量、TTL、序列化和 Redis 网络开销的差异，
不修改框架内部实现，不将未测量的性能收益写成结论。

贡献流程见 [CONTRIBUTING](CONTRIBUTING.md)，安全问题见 [SECURITY](SECURITY.md)，
发行规则见 [发布文档](docs/releasing.md)。采用 [Apache 2.0](LICENSE.txt)，
第三方保留内容见 [NOTICE](NOTICE)。
