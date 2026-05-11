# Stellflow Netty 数据面实现指南

## 1. 文档目的

本文档用于细化 `stellflow` 数据面在 Netty 上的实现方式，重点覆盖：

- ChannelPipeline 设计
- ByteBuf 策略
- write / flush 设计
- zero-copy 落地路径

本文档面向：

- Broker / Client 数据面通信
- Broker 间复制链路

本文档不涉及 Controller / Broker 控制面 gRPC 实现。

## 2. 设计目标

Netty 数据面实现应当满足以下目标：

- 适配自定义二进制协议
- 高吞吐优先
- 低对象分配
- 尽量减少内存拷贝
- 适合大 batch 传输
- 适合 Fetch / Replica Fetch 的大块连续字节返回

## 3. 总体实现原则

### 3.1 Netty 不是协议

要明确区分：

- Netty：Java 侧 I/O 实现框架
- 数据面协议：`stellflow` 自定义二进制协议

协议必须独立于 Netty 存在，避免把 `ByteBuf` 结构直接变成跨语言协议定义。

### 3.2 数据面与控制面分离

Netty 仅服务以下链路：

- Producer -> Broker
- Consumer -> Broker
- Replica -> Leader

控制面：

- Controller -> Broker
- Broker -> Controller

不走这条 Netty 数据面实现链路。

### 3.3 以 batch 为一等公民

Netty Pipeline 的设计目标不是“逐消息处理”，而是：

- 接收大块请求体
- 解码固定头
- 尽量以 batch 视图向后传递
- 尽量避免中间重编码

## 4. ChannelPipeline 设计

### 4.1 Broker 服务端 Pipeline

Broker 服务端推荐 Pipeline 结构：

```text
ChannelPipeline
  -> ConnectionLifecycleHandler
  -> IdleStateHandler
  -> FrameLengthDecoder
  -> RequestHeaderDecoder
  -> RequestBodyDecoder
  -> AuthHandler
  -> QuotaHandler
  -> RequestDispatchHandler
  -> ResponseEncoder
  -> FlushConsolidationHandler
```

### 4.2 各 Handler 职责

#### ConnectionLifecycleHandler

职责：

- 连接建立与关闭回调
- 注册连接上下文
- 指标埋点

#### IdleStateHandler

职责：

- 检测空闲连接
- 处理超时关闭或保活逻辑

#### FrameLengthDecoder

职责：

- 先读取固定长度前缀
- 保证只在完整帧到达后继续向下传播

建议：

- 第一版数据面协议使用 `int32 frameLength`

#### RequestHeaderDecoder

职责：

- 解码统一请求头
- 提取 `apiKey`、`apiVersion`、`headerVersion`、`correlationId`、`clientId`
- 提取 `traceId`、`spanId`、`tenantId`、`quotaKey`、`authContextId`、`trafficClass`、`trafficTag`

#### RequestBodyDecoder

职责：

- 根据 `apiKey + apiVersion` 解析请求体
- 尽量保持 `RecordBatch` 为原始字节块视图

#### AuthHandler

职责：

- 认证状态校验
- 不通过时快速失败

#### QuotaHandler

职责：

- 请求速率和流量限流
- 必要时注入节流时间

#### RequestDispatchHandler

职责：

- 将请求投递到 `RequestChannel`
- 不在 I/O 线程中执行重业务逻辑

#### ResponseEncoder

职责：

- 编码统一响应头
- 编码响应体
- 对 Fetch 响应支持 ByteBuf / FileRegion 输出

#### FlushConsolidationHandler

职责：

- 合并 flush 调用
- 减少频繁小 flush

### 4.3 Replica Fetch Pipeline

复制连接可采用独立 Pipeline 模板：

```text
ChannelPipeline
  -> ConnectionLifecycleHandler
  -> FrameLengthDecoder
  -> RequestHeaderDecoder
  -> ReplicaRequestBodyDecoder
  -> ReplicaDispatchHandler
  -> ResponseEncoder
  -> FlushConsolidationHandler
```

说明：

- 复制链路可以做更激进的 batch 和 buffer 配置
- 因为复制请求类型更收敛，解码链可比通用客户端链更轻

## 5. EventLoop 与线程模型建议

### 5.1 原则

- I/O 线程不做重业务
- 业务逻辑下沉到 `RequestHandlerPool`
- I/O 线程只负责：
  - 读
  - 帧拼装
  - 头部解码
  - 响应回写

### 5.2 建议划分

- `bossGroup`：连接接入
- `workerGroup`：读写与编码
- `requestHandlerPool`：业务处理

### 5.3 线程数建议

初始建议：

- `bossGroup`：`1`
- `workerGroup`：`CPU cores / 2` 到 `CPU cores`
- `requestHandlerPool`：按 Broker 核数和分区并发度单独调优

## 6. ByteBuf 策略

### 6.1 推荐策略

建议统一使用：

- `PooledByteBufAllocator`
- `DirectByteBuf`

原因：

- 减少堆内复制
- 降低 GC 压力
- 更适合高吞吐持续传输

### 6.2 不推荐策略

不建议在数据面主路径大量使用：

- 非池化 buffer
- heap byte array 作为主承载结构
- 每次解码都重新复制整个请求体

### 6.3 处理方式建议

- 对头部：可直接解码为轻量对象
- 对 body：优先保留 `ByteBuf slice`
- 对 `RecordBatch`：尽量延后真正解析

### 6.4 生命周期管理

必须明确：

- 哪个 Handler retain
- 哪个 Handler release
- 哪个对象只持有视图不持有所有权

建议：

- 在 request 对象中显式表达 buffer ownership
- 对异步 dispatch 的请求，进入业务池前先明确引用计数策略

## 7. Request Decode 落地建议

### 7.1 分阶段解码

推荐分两阶段：

1. 先解请求头
2. 再按 `apiKey + apiVersion` 解请求体

### 7.2 延迟解码

对大消息请求建议：

- 不在 I/O 线程里把整个 `RecordBatch` 全解析成对象树
- 尽量只解元信息
- 真正需要时在业务线程中做更深处理

### 7.3 目标

I/O 线程尽量把请求变成：

- `RequestHeader`
- `ByteBuf bodyView`

而不是：

- 巨大的深层 Java 对象图

## 8. write / flush 设计

### 8.1 核心原则

高吞吐数据面要避免：

- 每个响应都立即 `flush()`
- 大量小 write
- 频繁系统调用

### 8.2 推荐策略

- write 与 flush 分离
- 由事件循环周期性合并 flush
- 对响应批量积攒到合理阈值后再 flush

### 8.3 实现建议

- 使用 `FlushConsolidationHandler`
- 在 `ResponseSend` 层区分：
  - 普通小响应
  - Fetch 大响应
  - Replica Fetch 大响应

### 8.4 大响应处理

对于 Fetch / Replica Fetch：

- 优先大块连续 write
- 尽量避免把多个日志段内容先复制到一个新大数组
- 能用 gather write 就不用拼接复制

## 9. Zero-Copy 落地方式

### 9.1 适用目标

zero-copy 优先用于：

- Consumer Fetch
- Replica Fetch
- 直接发送磁盘日志段中的连续字节区域

### 9.2 推荐实现

推荐优先使用：

- `FileChannel.transferTo`
- Netty `DefaultFileRegion`

### 9.3 典型路径

```text
LogSegment FileChannel
  -> FileRegion
  -> Channel.write(FileRegion)
  -> kernel sendfile path
```

### 9.4 前提条件

要想真正用好 zero-copy，需要：

- 日志文件格式与网络返回格式足够接近
- 返回区域是连续文件区间
- 不需要中途逐条重写消息

### 9.5 不适用场景

不建议强行 zero-copy 的场景：

- 返回体需要重新压缩
- 需要大量过滤
- 多个不连续片段必须复杂拼装

## 10. Fetch 响应实现建议

### 10.1 优先策略

Fetch 响应建议优先两类实现：

1. 纯内存 ByteBuf 返回
   适合小批量或未落盘数据
2. 文件区域零拷贝返回
   适合大块连续日志段

### 10.2 混合响应

如果一个响应中既有内存数据又有文件数据，建议：

- 头部和元信息走 ByteBuf
- 日志段主体走 FileRegion
- 通过顺序 write 组合发送

## 11. Replica Fetch 实现建议

### 11.1 特点

复制链路比普通 Consumer Fetch 更稳定、更单一，因此更适合做吞吐优化：

- 请求类型少
- 读取语义更简单
- 返回通常更大批量

### 11.2 建议

- 复制连接单独配置更大 socket buffer
- 支持更大的 fetch window
- 优先 zero-copy
- 保持持续 pipeline

## 12. ChannelOption 建议基线

以下为数据面初始建议值：

| 配置项 | 建议值 |
| --- | --- |
| `SO_RCVBUF` | `4 MB` 起步 |
| `SO_SNDBUF` | `4 MB` 起步 |
| `TCP_NODELAY` | 视 batch 策略决定，吞吐优先可不强依赖 |
| `WRITE_BUFFER_WATER_MARK` | 需结合响应大小调优 |
| allocator | `PooledByteBufAllocator.DEFAULT` |

说明：

- 最终值必须通过 benchmark 验证
- 普通客户端连接与复制连接可使用不同参数模板

## 13. OTel 埋点建议

Netty 数据面建议至少埋点以下指标：

- `stellflow.connection.count`
- `stellflow.request.count`
- `stellflow.request.latency`
- `stellflow.request.queue.depth`
- `stellflow.response.queue.depth`
- `stellflow.request.decode.error.count`
- `stellflow.fetch.bytes`
- `stellflow.produce.bytes`

并建议在以下阶段打点：

- 连接建立
- 完整帧接收
- 头部解码完成
- 请求入队
- 响应开始 write
- 响应 flush 完成

## 14. 实现顺序建议

建议按以下顺序实现：

1. 基础 `FrameLengthDecoder`
2. `RequestHeaderDecoder`
3. `ResponseEncoder`
4. `RequestDispatchHandler`
5. pooled direct buffer 策略
6. flush consolidation
7. Fetch 大响应优化
8. zero-copy 路径
9. Replica Fetch 专用优化

## 15. 风险与注意事项

### 15.1 引用计数错误

`ByteBuf` 引用计数错误会导致：

- 内存泄漏
- 提前释放
- 难排查的随机错误

### 15.2 过早深度解码

如果在 I/O 线程中对大 batch 做过深解码，会显著拉低吞吐。

### 15.3 flush 过于频繁

过于频繁的 flush 会导致：

- 系统调用增多
- 小包增多
- 带宽利用率下降

### 15.4 zero-copy 被破坏

如果 Fetch 返回前又做了重新拼包、重新压缩或重新编码，zero-copy 收益会被大幅削弱。

## 16. 结论

`stellflow` 在 Netty 上的数据面实现，不应只是“能收发字节”，而应围绕高吞吐目标做专门设计。最关键的落点是：

- Pipeline 分层清晰
- I/O 线程不做重业务
- pooled direct buffer
- write / flush 解耦
- Fetch / Replica Fetch 优先 zero-copy

只要这几个点在实现上守住，Netty 就可以很好地支撑 `stellflow` 的高吞吐数据面目标。
