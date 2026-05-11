# Stellflow Netty 传输层配置说明

## 1. 文档目标

本文档用于说明 `stellflow` 数据面 Netty 传输层配置的来源、覆盖顺序、部署方式以及每个配置项的含义。

当前配置对象对应代码：

- [NettyTransportConfig.java](E:\PersonalCode\JavaProject\stellflow\src\main\java\io\github\stellhub\stellflow\network\transport\NettyTransportConfig.java)

默认配置文件：

- [stellflow.yaml](E:\PersonalCode\JavaProject\stellflow\src\main\resources\stellflow.yaml)

说明：

- 当前 YAML 不只承载 `network.transport`，也承载 `storage.log` 的运行时配置
- 其中 `stellflow.storage.log.rootDir` 会影响 `LogManager` 的磁盘落盘根目录

## 2. 配置加载顺序

当前 `NettyTransportConfig.load()` 的加载优先级为：

1. 代码内默认值
2. 类路径下的 `stellflow.yaml`
3. 外部配置文件 `-Dstellflow.config.file=...`
4. 单项系统属性覆盖 `-Dstellflow.network.transport.xxx=...`

优先级从低到高，后者覆盖前者。

## 2.1 `stellflow://` 与 `grpc://` 的定位

从这一版开始，`stellflow` 在**配置层和文档层**正式引入：

- `stellflow://host:port`
- `grpc://host:port`

它们的定位是：

- `stellflow://`：表达数据面 broker endpoint
- `grpc://`：表达控制面 gRPC endpoint

需要特别注意：

- 这只是配置与元数据表达方式
- **线上 TCP 二进制协议本身并不会发送 `stellflow://` 这样的文本前缀**
- 数据面真实线缆格式仍然是：
  - `frameLength + requestHeader + requestBody`

也就是说：

- `stellflow://` 是 URI scheme
- 不是线上报文头字符串

## 3. 部署时怎么改配置

### 3.1 方式一：直接修改打包内默认配置

修改：

- [stellflow.yaml](E:\PersonalCode\JavaProject\stellflow\src\main\resources\stellflow.yaml)

适合：

- 本地开发
- 单机实验

### 3.2 方式二：使用外部配置文件

例如：

```powershell
mvn exec:java -Dexec.mainClass=io.github.stellhub.stellflow.StellflowBrokerApplication -Dstellflow.config.file=E:\deploy\stellflow.yaml
```

适合：

- 测试环境
- 生产部署
- 同一包多环境复用

### 3.3 方式三：使用单项系统属性临时覆盖

例如：

```powershell
mvn exec:java -Dexec.mainClass=io.github.stellhub.stellflow.StellflowBrokerApplication -Dstellflow.network.transport.port=19092
```

适合：

- 临时调试
- 容器化环境注入
- 启动脚本快速覆盖

## 4. 配置项说明

### 4.1 `stellflow.network.transport.host`

默认值：

```yaml
stellflow:
  network:
    transport:
      host: 0.0.0.0
```

作用：

- 控制 Broker 监听绑定的本地地址

使用方式：

- `0.0.0.0` 表示监听所有网卡
- `127.0.0.1` 适合本机联调
- 指定内网 IP 适合多网卡环境下约束监听面

注意：

- 这是“监听地址”，不是未来真正的对外广告地址体系
- 后续如果引入 `advertised host`，应和这个值分离

### 4.2 `stellflow.network.transport.port`

默认值：

```yaml
stellflow:
  network:
    transport:
      port: 9092
```

作用：

- 控制 Broker 数据面监听端口

使用方式：

- 本地单实例建议用 `9092`
- 同机多实例可改成 `19092`、`29092` 等

注意：

- 当前 `Metadata` 响应已和实际配置联动，不再写死 `9092`

### 4.3 `stellflow.network.transport.bossThreads`

默认值：

```yaml
stellflow:
  network:
    transport:
      bossThreads: 1
```

作用：

- 控制 Netty `bossGroup` 线程数
- 负责接收新连接

使用建议：

- 大多数场景 `1` 就够
- 连接建立压力特别大时可适当增大

不建议：

- 盲目调很大

### 4.4 `stellflow.network.transport.workerThreads`

默认值：

```yaml
stellflow:
  network:
    transport:
      workerThreads: 4
```

说明：

- 代码默认值是 `max(2, CPU核数/2)`
- 默认配置文件里当前给了 `4`，方便本地和测试环境行为更稳定

作用：

- 控制 Netty `workerGroup` 线程数
- 负责连接读写和 pipeline 执行

使用建议：

- 本地开发：`2-4`
- 生产环境：结合 CPU 核数和连接并发测试调优

注意：

- 这不是业务线程池大小
- 业务处理线程池目前由 `RequestDispatcher` 单独控制

### 4.5 `stellflow.network.transport.soRcvBuf`

默认值：

```yaml
stellflow:
  network:
    transport:
      soRcvBuf: 4194304
```

作用：

- 对应 socket 接收缓冲区大小

使用建议：

- 高吞吐数据面建议至少 `4 MB`
- 大 batch 或复制链路可考虑更大

影响：

- 太小会限制大批量请求接收效率
- 太大则会增加每连接的内核缓冲占用

### 4.6 `stellflow.network.transport.soSndBuf`

默认值：

```yaml
stellflow:
  network:
    transport:
      soSndBuf: 4194304
```

作用：

- 对应 socket 发送缓冲区大小

使用建议：

- 对 `Fetch`、`Replica Fetch`、大批量响应很重要
- 建议和 `soRcvBuf` 同级别起步

### 4.7 `stellflow.network.transport.maxFrameLength`

默认值：

```yaml
stellflow:
  network:
    transport:
      maxFrameLength: 67108864
```

即：

- `64 MB`

作用：

- 限制单个协议帧允许的最大长度
- 用于防止异常大包、畸形包或恶意请求直接撑爆内存

使用建议：

- 当前第一版可以保持 `64 MB`
- 如果后面 batch 进一步做大，可按压测结果调高

注意：

- 这个值必须大于请求头 + body 的最大可能值
- 但不建议无限放大

## 5. 当前默认配置文件

当前默认配置内容如下：

```yaml
stellflow:
  storage:
    log:
      rootDir: data/logs
  network:
    transport:
      host: 0.0.0.0
      port: 9092
      bossThreads: 1
      workerThreads: 4
      soRcvBuf: 4194304
      soSndBuf: 4194304
      maxFrameLength: 67108864
```

## 6. 推荐使用方式

### 额外说明：`stellflow.storage.log.rootDir`

默认值：

```yaml
stellflow:
  storage:
    log:
      rootDir: data/logs
```

作用：

- 控制 `LogManager` 的日志根目录
- 每个 `topic-partition` 会在这个目录下创建独立子目录，例如 `orders-0`

使用建议：

- 本地开发可保留默认值
- 测试环境与生产环境建议指向独立数据盘或挂载卷
- 集成测试应使用临时目录，避免污染仓库或共享环境

### 额外说明：`stellflow.storage.log.segmentBytes`

默认值：

```yaml
stellflow:
  storage:
    log:
      segmentBytes: 33554432
```

作用：

- 控制单个 `.log` segment 的最大尺寸
- active segment 达到阈值后，`UnifiedLog` 会滚动到新的 segment 文件

使用建议：

- 本地开发和小规模测试可以保持默认值
- 如果希望更频繁验证 rolling / recovery，可在测试环境调小
- 生产环境应结合批大小、磁盘吞吐和恢复时间来调优

### 额外说明：`stellflow.storage.log.indexIntervalBytes`

默认值：

```yaml
stellflow:
  storage:
    log:
      indexIntervalBytes: 4096
```

作用：

- 控制稀疏 offset index 的采样密度
- 值越小，索引更密，查找更快，但 `.index` 文件也会更大

使用建议：

- 当前第一版建议保留 `4096`
- 如果后续单批次记录很大、跨段 fetch 较多，可以结合压测再调整

### 额外说明：`stellflow.storage.log.retentionSegments`

默认值：

```yaml
stellflow:
  storage:
    log:
      retentionSegments: 8
```

作用：

- 控制单个 `topic-partition` 最多保留多少个 segment
- 超过阈值后，最早的已滚动 segment 会被清理，从而推进 `logStartOffset`

使用建议：

- 本地开发和测试可保持默认值
- 如果希望更快验证保留与清理逻辑，可临时调小
- 生产环境需要结合 segment 大小、磁盘容量和恢复窗口综合设置

### 额外说明：`stellflow.storage.log.retentionMs`

默认值：

```yaml
stellflow:
  storage:
    log:
      retentionMs: 604800000
```

作用：

- 控制 rolled segment 的时间保留窗口
- 超过这个时间窗口的旧 segment 会变成清理候选

使用建议：

- 默认值为 7 天
- 如果是开发环境或短期压测环境，可以显著调小以更快验证清理逻辑

### 额外说明：`stellflow.storage.log.retentionBytes`

默认值：

```yaml
stellflow:
  storage:
    log:
      retentionBytes: 1073741824
```

作用：

- 控制单个 `topic-partition` 的总保留字节数上限
- 超过后会优先删除最老的已滚动 segment

使用建议：

- 默认值为 `1 GB`
- 生产环境应结合磁盘预算、segment 大小和 topic 分区数一起规划

### 额外说明：`stellflow.replica.fetch.enabled`

默认值：

```yaml
stellflow:
  replica:
    fetch:
      enabled: false
```

作用：

- 控制当前 broker 是否启动 follower 后台拉取循环
- 关闭时，只保留 handler / storage / 协议能力，不主动发起 replica fetch

使用建议：

- 单机开发环境保持 `false`
- follower 节点或复制联调环境再显式打开

### 额外说明：`stellflow.replica.fetch.assignments`

默认值：

```yaml
stellflow:
  replica:
    fetch:
      assignments: ""
```

作用：

- 为当前 skeleton 版本提供静态副本抓取分配

格式：

- `topic:partition@stellflow://leaderHost:leaderPort#leaderBrokerId`

多个 assignment 用逗号分隔，例如：

```yaml
stellflow:
  replica:
    fetch:
      assignments: "orders:0@stellflow://10.0.0.12:9092#0,payments:1@stellflow://10.0.0.13:9092#2"
```

使用建议：

- 当前只用于开发、测试和复制链路联调
- 后续正式集群应由 Controller/Broker 控制面动态下发，而不是长期依赖静态配置
- 为兼容历史配置，旧的 `host:port` 形式当前仍可解析，但新配置应统一使用 `stellflow://`

### 额外说明：`stellflow.replica.fetch.pollIntervalMs`

默认值：

```yaml
stellflow:
  replica:
    fetch:
      pollIntervalMs: 500
```

作用：

- 控制 follower 后台拉取循环的轮询间隔

使用建议：

- 值越小，复制延迟越低，但请求频率越高
- 当前建议从 `100ms - 500ms` 区间起步

### 额外说明：`stellflow.replica.fetch.pipelineRoundsPerPoll`

默认值：

```yaml
stellflow:
  replica:
    fetch:
      pipelineRoundsPerPoll: 4
```

作用：

- 控制一次调度唤醒期间，在同一条长连接上连续发送多少轮 `Replica Fetch`
- 当前实现不是每轮都重建连接，而是复用同一条 TCP 连接并串行推进多轮抓取

使用建议：

- 值越大，追赶 lag 时越积极
- 但也会让单次调度占用更多网络与 CPU 时间
- 当前建议从 `2 - 8` 区间起步

### 额外说明：`stellflow.controlPlane.grpc.serverEnabled`

默认值：

```yaml
stellflow:
  controlPlane:
    grpc:
      serverEnabled: false
```

作用：

- 控制当前进程是否同时启动 Controller/Broker 的 gRPC 控制面服务端
- 开启后，这个进程可以向 broker 下发动态 replica assignments 和分区控制命令

### 额外说明：`stellflow.controlPlane.grpc.clientEnabled`

默认值：

```yaml
stellflow:
  controlPlane:
    grpc:
      clientEnabled: false
```

作用：

- 控制当前 broker 是否作为客户端连接 controller
- 开启后，broker 会注册自己并通过 gRPC watch stream 动态接收 replica assignments

使用建议：

- follower 节点或完整集群联调时开启
- 纯本地单机开发环境通常保持关闭

### 额外说明：`stellflow.controlPlane.grpc.controllerHost` / `controllerPort`

默认值：

```yaml
stellflow:
  controlPlane:
    grpc:
      controllerHost: 127.0.0.1
      controllerPort: 19093
```

作用：

- 指定 broker 侧控制面客户端连接到哪个 controller

使用建议：

- 与数据面监听端口分离
- 如果 controller 独立部署，应配置为 controller 集群暴露地址

### 额外说明：`stellflow.controlPlane.grpc.controllerEndpoint`

默认值：

```yaml
stellflow:
  controlPlane:
    grpc:
      controllerEndpoint: grpc://127.0.0.1:19093
```

作用：

- 用 scheme-aware 的形式表达 broker 应连接到哪个 controller 控制面地址

使用建议：

- 新配置优先使用这个字段
- `controllerHost` / `controllerPort` 保留为兼容覆盖项

### 额外说明：`stellflow.controlPlane.grpc.advertisedEndpoint`

默认值：

```yaml
stellflow:
  controlPlane:
    grpc:
      advertisedEndpoint: stellflow://127.0.0.1:9092
```

作用：

- 表达 broker 向 controller 广告的数据面 endpoint
- controller 后续会用它为 follower 生成 replica assignments

使用建议：

- 新配置优先使用该字段
- `advertisedHost` / `advertisedPort` 保留为兼容覆盖项

### 额外说明：`stellflow.observability.metrics.http.port`

默认值：

```yaml
stellflow:
  observability:
    metrics:
      http:
        port: 9464
```

作用：

- 暴露 Prometheus 抓取端口
- 当前端口服务的是轻量级 `/metrics` 拉取流量，而不是数据面业务流量

使用建议：

- 与数据面 `9092` 分开
- 在生产环境中单独纳入安全组、ACL 和监控系统配置

### 本地开发

- 使用仓库内默认配置
- 必要时通过 `-Dstellflow.network.transport.port=...` 覆盖端口

### 测试环境

- 使用外部 `stellflow.yaml`
- 把监听地址、端口、缓冲区放在部署目录管理

### 生产环境

- 使用外部配置文件
- 关键项通过启动脚本显式指定
- 配合压测调整：
  - `workerThreads`
  - `soRcvBuf`
  - `soSndBuf`
  - `maxFrameLength`

## 7. 后续建议

下一步建议继续把这些配置也逐步纳入配置体系：

1. 空闲连接超时
2. RequestDispatcher worker 数量
3. ResponseResponder 模式
4. 监听器名称和未来的 advertised 地址
5. 数据面与复制链路分离监听配置
