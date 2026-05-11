# Stellflow 数据面高吞吐性能设计

## 1. 文档目的

本文档用于定义 `stellflow` 数据面围绕 `500 MB/s+` 单机吞吐目标的工程化设计基线，重点覆盖：

- batch 大小设计
- socket buffer 设计
- Netty buffer 策略
- flush 策略
- zero-copy 路径
- 副本复制窗口
- benchmark 方法论

本文档面向 Broker / Client 数据面与 Broker 间复制链路，不涉及 Controller / Broker 控制面 gRPC 设计。

## 2. 目标与边界

### 2.1 性能目标

当前目标定义为：

- 单 Broker 数据面吞吐目标：`500 MB/s+`
- 吞吐优先于单条消息极致低延迟
- 优先保证批量顺序传输效率

### 2.2 设计边界

当前性能设计默认建立在以下边界之上：

- 数据面协议：自定义二进制协议
- Java 实现底层：Netty
- 存储模型：顺序日志段
- 复制模型：Leader / Follower + ISR
- 控制面：与数据面解耦，不进入主吞吐路径

### 2.3 基本判断

对于 `500 MB/s+` 目标，真正决定上限的不是“协议头多 20 字节还是少 20 字节”，而是以下六类问题：

- 是否足够批量化
- 是否减少内存拷贝
- 是否保持顺序 I/O
- 是否支持 pipeline
- 是否减少不必要 flush
- 是否让复制窗口足够大

## 3. 端到端吞吐路径

数据面高吞吐路径可以抽象为：

```text
Producer Batch
  -> Client Buffer
  -> TCP Socket
  -> Broker Netty Direct Buffer
  -> Request Decode
  -> Partition Append
  -> Log Segment / Page Cache
  -> Replica Fetch
  -> Consumer Fetch / Response Write
```

性能优化目标是让这条链路尽量接近“连续大块字节传输”，而不是“逐条消息 RPC 调用”。

## 4. Batch 大小设计

### 4.1 原则

batch 是吞吐设计的第一原则。

高吞吐场景下，单次网络传输、单次日志追加、单次副本同步都必须以 `RecordBatch` 为中心，而不是以单条消息为中心。

### 4.2 推荐分层配置

建议按以下层次定义 batch：

| 层次 | 推荐范围 | 说明 |
| --- | --- | --- |
| Producer 默认 batch | `256 KB - 1 MB` | 适合作为通用默认值 |
| 吞吐优先 batch | `1 MB - 4 MB` | 适合作为高吞吐场景主区间 |
| Replica 同步批次 | `4 MB - 16 MB` | 复制链路可使用更大批次 |

### 4.3 设计建议

- Producer 应优先聚合后发送，而不是收到一条就发一条
- Broker 应按 batch 追加，不按消息逐条落盘
- Fetch 响应也应优先返回大块连续 batch
- Replica Fetch 应优先面向大窗口批量同步

### 4.4 风险控制

batch 过大也会带来问题：

- 单请求延迟增加
- 单连接阻塞时间变长
- 内存峰值抖动增大
- 重试成本变高

因此建议：

- 通过 `batch.size` 与 `linger.ms` 联动调优
- 区分普通 Producer 与复制链路的 batch 策略

## 5. Socket Buffer 设计

### 5.1 目标

Socket buffer 的目标是：

- 减少小包发送
- 增强带宽利用率
- 提升 pipeline 吸收能力

### 5.2 推荐方向

在高吞吐场景下建议：

- `SO_SNDBUF`：至少 `4 MB` 起步
- `SO_RCVBUF`：至少 `4 MB` 起步
- 复制链路建议更高，例如 `8 MB - 16 MB`

说明：

- 最终有效值还会受操作系统内核参数限制
- 必须结合实际网卡、内核和容器环境验证

### 5.3 工程建议

- Broker 对客户端连接与复制连接使用不同配置模板
- 复制连接优先更大 buffer
- benchmark 时必须记录 OS 层最终生效值

## 6. Netty Buffer 策略

### 6.1 核心原则

Netty 的目标不是“把数据搬来搬去”，而是尽量减少内存复制与对象分配。

### 6.2 推荐策略

建议采用以下策略：

- 使用 `PooledByteBufAllocator`
- 优先使用 `DirectByteBuf`
- 尽量使用 `CompositeByteBuf` 或 gather write
- 避免把大块磁盘数据重新拼成新的堆内字节数组
- 对批量消息体尽量保持原始字节块语义

### 6.3 为什么不用 Heap Buffer 作为主路径

Heap Buffer 的主要问题：

- 更容易产生额外复制
- GC 压力更高
- 对超大吞吐持续传输场景不友好

因此建议：

- 控制面可以相对灵活
- 数据面主路径优先 direct + pooled

### 6.4 编解码建议

- 请求头单独解码
- 请求体尽量保持 slice/view 方式处理
- 避免对 `RecordBatch` 做重复反序列化与重编码

## 7. Flush 策略

### 7.1 核心原则

高吞吐系统必须避免“每写一条就 flush 一次”。

### 7.2 Producer 侧 flush

Producer 侧建议：

- 用 `linger.ms` 聚合 batch
- 不以单消息 send 完成为 flush 触发条件
- 允许有限时间换取更大 batch

### 7.3 Broker 网络 flush

Broker 响应写回建议：

- 优先按事件循环批量 flush
- 避免每个 response 都立即 `flush()`
- 对复制链路优先顺序大块发送

### 7.4 Broker 存储 flush

存储层建议：

- 使用批量刷盘策略
- 通过 `flush.messages`、`flush.ms` 控制
- 不在每个 append 后立即强制 fsync

### 7.5 风险平衡

减少 flush 可以显著提升吞吐，但也会带来：

- 延迟波动增大
- 崩溃窗口变大

因此必须区分：

- 吞吐优先场景
- 严格持久性场景

## 8. Zero-Copy 路径

### 8.1 目标

要逼近文件传输协议的吞吐效率，必须尽量接近：

```text
page cache -> socket -> NIC
```

### 8.2 推荐路径

Java / Netty 侧建议：

- 文件发送优先 `FileChannel.transferTo`
- Netty 可使用 `DefaultFileRegion`
- 对 Fetch 响应中的日志段数据优先考虑零拷贝发送

### 8.3 适用场景

zero-copy 最适合：

- Consumer Fetch
- Replica Fetch
- 大块连续日志段读取

### 8.4 不适用场景

以下场景不一定适合强行做 zero-copy：

- 需要对返回内容做大量重组
- 需要对消息做逐条过滤或转码
- 数据不连续，无法直接映射为文件区域

### 8.5 工程原则

- 尽量让磁盘数据格式和网络传输格式贴近
- 减少 Broker 中间层对消息体做变形
- Broker 更像“日志块搬运者”，而不是“消息重编码器”

## 9. 副本复制窗口设计

### 9.1 核心判断

复制链路是吞吐放大器，也是资源放大器。

如果客户端写入是 `500 MB/s`，在 `replication.factor = 3` 的情况下，Leader 内部复制流量会显著放大，因此复制窗口不能设计得过小。

### 9.2 推荐方向

副本同步建议：

- `fetch.max.bytes`：`4 MB - 16 MB`
- `max.partition.fetch.bytes`：按单分区批次放宽
- `replica.fetch.wait.max.ms`：适当等待以换更大批
- 支持多个 in-flight replica fetch

### 9.3 复制线程策略

建议：

- 按源 Leader 分组 `ReplicaFetcherThread`
- 同一个源 Broker 上多个分区共享连接但保持大窗口
- 通过 pipeline 持续拉取，而不是“拉一次等一次”

### 9.4 高水位推进影响

复制窗口过小会导致：

- ISR 追平速度慢
- 高水位推进慢
- `acks=all` 吞吐下降

因此：

- 复制窗口大小直接影响可见性推进速度
- 复制吞吐必须与客户端写入吞吐共同设计

## 10. Pipeline 与 In-Flight 设计

### 10.1 为什么必须做

如果一个连接永远是：

1. 发一个请求
2. 等一个响应
3. 再发下一个请求

那在高带宽场景下很容易被 RTT 限制。

### 10.2 建议

- Producer 支持多个 in-flight batch
- Broker Fetch 支持并发请求处理
- Replica Fetch 保持持续 pipeline
- 用 `correlationId` 做请求-响应对应

### 10.3 风险

in-flight 增加后要注意：

- 内存占用
- 重试语义
- 顺序保证
- 幂等处理

## 11. 多分区并行策略

### 11.1 基本判断

单分区串行写是必要约束，但整体吞吐必须靠多分区并行放大。

### 11.2 设计建议

- Producer 应把流量尽量分散到多个分区
- Broker 的请求处理线程池要能并行处理多分区请求
- 存储层允许多分区并发顺序追加
- 复制层支持多分区并行追平

### 11.3 目标理解

`500 MB/s+` 更合理的理解通常是：

- 单 Broker 聚合吞吐目标
- 多分区共同贡献

而不是：

- 单连接单分区必须独自达到 `500 MB/s`

## 12. Benchmark 方法论

### 12.1 原则

benchmark 必须分层测量，不能一上来就只看“总吞吐”。

### 12.2 建议分层

建议按以下顺序基准测试：

1. **内存到网络**
   Broker 不落盘，仅测协议和网络栈极限
2. **磁盘顺序写**
   不开复制，仅测存储追加极限
3. **磁盘到网络**
   测 Fetch / Replica Fetch 大块读取与发送能力
4. **完整 Produce 链路**
   Client -> Broker -> Log
5. **完整复制链路**
   Leader -> Follower
6. **端到端真实场景**
   带 `acks`、带复制、带消费者、带可观测埋点

### 12.3 Benchmark 指标

每轮测试至少记录：

- 吞吐量 `MB/s`
- 请求数 `req/s`
- 记录数 `records/s`
- P50 / P95 / P99 延迟
- CPU 使用率
- GC 时间
- 网卡带宽利用率
- 磁盘顺序写带宽
- ISR 追平速度
- 高水位推进速度

### 12.4 Benchmark 变量控制

每次测试必须明确记录：

- batch 大小
- linger 配置
- in-flight 数量
- socket buffer 大小
- 复制因子
- `acks`
- 压缩类型
- 是否 TLS
- Producer / Consumer / Replica 并发数

### 12.5 Benchmark 场景矩阵

建议至少覆盖以下场景：

| 场景 | 目的 |
| --- | --- |
| 单 Broker、无复制 | 测基础数据面上限 |
| 单 Broker、多分区 | 测并行聚合吞吐 |
| 多 Broker、带复制 | 测真实复制成本 |
| `acks=1` 与 `acks=all` 对比 | 测可靠性成本 |
| 不压缩与压缩对比 | 测 CPU 与网络权衡 |
| Direct buffer 与 Heap buffer 对比 | 测 Netty 内存策略效果 |
| zero-copy 开关对比 | 测 Fetch 与复制路径收益 |

## 13. 可观测性与性能联动

为了让 benchmark 可解释，必须同步埋点这些核心指标：

- `stellflow.request.latency`
- `stellflow.produce.bytes`
- `stellflow.fetch.bytes`
- `stellflow.log.flush.latency`
- `stellflow.replica.lag`
- `stellflow.high_watermark.advance.count`
- `stellflow.request.queue.depth`
- `stellflow.connection.count`

只有协议、存储、复制和观测一起设计，benchmark 结果才有解释力。

## 14. 当前阶段建议默认值

在实现初期，建议从如下默认值起步做第一轮测试：

| 项目 | 建议初始值 |
| --- | --- |
| Producer batch size | `1 MB` |
| Producer linger | `5 ms` |
| Socket send/recv buffer | `4 MB` |
| Replica fetch max bytes | `8 MB` |
| In-flight requests | `4 - 8` |
| Netty allocator | `PooledByteBufAllocator` |
| 主数据面 buffer | `DirectByteBuf` |

说明：

- 这些值不是最终最优值，只是第一轮高吞吐实验基线

## 15. 结论

如果 `stellflow` 的目标是把数据面吞吐推到 `500 MB/s+`，那它必须在工程上同时做到：

- 大 batch
- 大 socket buffer
- pooled direct buffer
- 批量 flush
- 尽可能 zero-copy
- 足够大的复制窗口
- 有层次的 benchmark 方法论

归根到底，高吞吐数据面不是靠某一个“神奇协议”达成的，而是靠**协议、网络、存储、复制、buffer 和 benchmark 方法一起收敛**出来的。
