# Stellflow Produce / Fetch 消息格式规范

## 1. 文档目的

本文档用于定义 `stellflow` 数据面协议中与消息读写最核心的三部分字段级格式：

- `RecordBatch`
- `ProduceRequestBody`
- `FetchResponseBody`

本文档是 [协议规范文档](./protocol-spec.md) 的进一步细化，目标是为 Java Broker、Java Client、Golang Client 以及后续更多语言 SDK 提供统一的消息格式契约。

## 2. 适用范围

本文档适用于以下场景：

- Producer 向 Broker 发送消息批
- Broker 向 Consumer 返回消息批
- Broker 向 Follower 返回复制消息批

本文档当前只定义第一版可实现基线：

- `Produce apiKey = 2, apiVersion = 0`
- `Fetch apiKey = 3, apiVersion = 0`

与本文件配套的独立文档如下：

- [FetchRequestBody 消息格式规范](./fetch-request-format.md)
- [ProduceResponseBody 消息格式规范](./produce-response-format.md)

## 3. 总体设计原则

### 3.1 Batch 优先

`stellflow` 的消息格式以 `RecordBatch` 为一等公民，而不是逐条消息为一等公民。  
数据面传输、存储和复制都尽量围绕 batch 展开。

### 3.2 传输格式尽量贴近存储格式

为了支持高吞吐和尽可能的 zero-copy，网络中的消息批格式应尽量接近日志段中的消息批格式，减少 Broker 中途重编码。

### 3.3 第一版优先可实现

第一版消息格式优先满足：

- 高吞吐顺序追加
- 按偏移量批量读取
- 可选压缩
- 基础校验与错误处理
- 幂等写入的 Producer 身份与序列控制
- 事务写入与 `read_committed` 可见性所需核心字段

当前阶段不优先追求：

- 过于复杂的控制批变体
- 过度紧凑编码

## 4. 编码约定

除非另有说明，本节沿用 [协议规范文档](./protocol-spec.md) 中的基础编码规则：

- `int8`：1 字节有符号整数
- `int16`：2 字节有符号整数，大端序
- `int32`：4 字节有符号整数，大端序
- `int64`：8 字节有符号整数，大端序
- `bool`：1 字节，`0` 或 `1`
- `string`：`int16 length + UTF-8 bytes`
- `nullable string`：`int16 length = -1` 表示 `null`
- `bytes`：`int32 length + raw bytes`
- `nullable bytes`：`int32 length = -1` 表示 `null`
- `array<T>`：`int32 length + repeated entries`

## 5. RecordBatch 总体结构

### 5.1 结构概览

第一版 `RecordBatch` 采用如下逻辑结构：

```text
RecordBatch
  -> BatchHeader
  -> RecordSet
```

其中：

- `BatchHeader` 描述这个批次的元信息
- `RecordSet` 是该批次中一组连续 `Record`

### 5.2 RecordBatch 二进制布局

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `baseOffsetDelta` | `int32` | 是 | 相对于请求或文件基准偏移的起始 delta，第一版网络请求中可为 `0` |
| `batchLength` | `int32` | 是 | 当前 batch 从 `partitionLeaderEpoch` 到最后一条 record 的总字节数 |
| `partitionLeaderEpoch` | `int32` | 是 | 分区领导 epoch |
| `magic` | `int8` | 是 | batch 格式版本，第一版固定为 `1` |
| `crc32c` | `int32` | 是 | 从 `attributes` 字段开始到 batch 末尾的 CRC32C |
| `attributes` | `int16` | 是 | 批次属性位图 |
| `lastOffsetDelta` | `int32` | 是 | 最后一条 record 相对首条的偏移差 |
| `baseTimestamp` | `int64` | 是 | 第一条 record 的基准时间戳 |
| `maxTimestamp` | `int64` | 是 | 批次中最大时间戳 |
| `producerId` | `int64` | 是 | Producer 标识，幂等与事务生产者必须携带，普通生产者可为 `-1` |
| `producerEpoch` | `int16` | 是 | Producer epoch，用于 fencing 与重建 |
| `baseSequence` | `int32` | 是 | 批次首条 record 对应的序列号 |
| `recordCount` | `int32` | 是 | 批次中 record 数量 |
| `records` | `array<Record>` | 是 | 批次内记录集合 |

### 5.3 字段语义说明

#### `baseOffsetDelta`

- 在网络 Produce 请求中，客户端通常无法分配真实 offset，因此第一版可固定为 `0`
- 在日志文件或 Fetch 返回中，该值用于表达批次相对基准位移关系

#### `batchLength`

- 用于快速跳过 batch 和做边界检查
- 不包含最外层请求头

#### `partitionLeaderEpoch`

- 用于复制链路与元数据一致性校验
- 第一版普通客户端可以填 `-1`
- 它对应分区级 `leaderEpoch`，用于判断当前消息批是否属于预期 Leader 代次

#### `magic`

- 第一版固定为 `1`
- 后续若消息批布局发生破坏性变化，应提升 `magic`

#### `crc32c`

- 保护批次核心内容不被损坏
- 校验范围从 `attributes` 字段开始到批次末尾

#### `attributes`

推荐位图定义如下：

| 位范围 | 含义 |
| --- | --- |
| bit `0-2` | `compressionType`，`0=none`，`1=gzip`，`2=snappy`，`3=lz4`，`4=zstd` |
| bit `3` | `timestampType`，`0=createTime`，`1=logAppendTime` |
| bit `4` | `isControlBatch` |
| bit `5` | `isTransactional` |
| bit `6-15` | 保留 |

#### `lastOffsetDelta`

- 用于快速计算该批次覆盖的位移范围
- 若 `recordCount = 1`，则该值通常为 `0`

#### `baseTimestamp` / `maxTimestamp`

- 用于时间索引与返回元信息
- 若只有一条 record，则二者可以相等

#### `producerId` / `producerEpoch` / `baseSequence`

- `producerId` 用于标识幂等生产者或事务生产者
- `producerEpoch` 用于处理 producer fencing 与重建
- `baseSequence` 用于分区级去重与顺序校验
- 普通非幂等 produce 允许使用 `-1 / -1 / -1`
- 幂等 produce 与事务 produce 必须使用有效值

#### `producerEpoch` 的作用

`producerEpoch` 是同一个 `producerId` 的代次号，用于表示“当前合法的 Producer 实例已经推进到了哪一代”。

设计目的：

- 在 Producer 重启、重连、故障转移后区分新旧实例
- 支持 producer fencing，阻止旧实例继续写入
- 与 `baseSequence` 一起支撑幂等去重与顺序校验
- 保护事务生产者在重新接管后不被旧连接污染

如果没有 `producerEpoch`：

- 旧 Producer 在失效后仍可能继续写入
- 同一个 `producerId` 的新旧实例难以区分
- 幂等写入去重和事务正确性都会变得脆弱

一句话理解：

- `producerEpoch` 不是共识层的 `Term`
- 它更像 Producer 自己的 generation，用于回答“哪个 Producer 实例是当前有效的一代”

### 5.4 `leaderEpoch` 与 `producerEpoch` 的区别

| 字段 | 所属层级 | 作用 |
| --- | --- | --- |
| `leaderEpoch` | Partition 复制层 | 表示当前分区 Leader 的代次 |
| `producerEpoch` | Producer 幂等/事务层 | 表示当前 Producer 实例的代次 |

二者共同点：

- 都是“新旧代次判定号”
- 都用于防止过期角色继续生效

二者不同点：

- `leaderEpoch` 解决的是“谁是当前合法 Leader”
- `producerEpoch` 解决的是“谁是当前合法 Producer 实例”

## 6. Record 结构

### 6.1 二进制布局

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `length` | `int32` | 是 | 当前 record 总字节数 |
| `attributes` | `int8` | 是 | record 级属性位，第一版预留 |
| `timestampDelta` | `int64` | 是 | 相对 `baseTimestamp` 的时间偏移 |
| `offsetDelta` | `int32` | 是 | 相对批次首条 record 的位移偏移 |
| `key` | `nullable bytes` | 否 | 消息 key |
| `value` | `nullable bytes` | 否 | 消息 value |
| `headers` | `array<RecordHeader>` | 是 | 记录头集合 |

### 6.2 RecordHeader 结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `key` | `string` | 是 | header 键 |
| `value` | `nullable bytes` | 否 | header 值 |

### 6.3 设计说明

- 第一版不做过度紧凑编码，优先保证跨语言实现简单
- `key` 可为空，适配无 key 消息
- `headers` 为空时，`array length = 0`

## 7. ProduceRequestBody 结构

### 7.1 顶层结构

`ProduceRequestBody` 用于 `apiKey = 2, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `transactionalId` | `nullable string` | 否 | 事务生产者标识，非事务请求为 `null` |
| `acks` | `int16` | 是 | 应答级别 |
| `timeoutMs` | `int32` | 是 | 服务端等待应答的超时时间 |
| `topicData` | `array<ProduceTopicData>` | 是 | 按 topic 聚合的分区写入数据 |

### 7.2 `transactionalId` 语义

- `null` 表示普通 produce 或仅幂等 produce
- 非 `null` 表示当前请求属于某个事务生产者上下文
- Broker 应结合 `producerId / producerEpoch`、事务状态机和协调器状态共同校验

### 7.3 `acks` 语义

第一版支持以下取值：

| 值 | 说明 |
| --- | --- |
| `0` | 不等待 Broker 应答 |
| `1` | 等待 Leader 本地写入完成 |
| `-1` | 等待满足 ISR / `acks=all` 语义 |

### 7.4 ProduceTopicData

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | `string` | 是 | Topic 名称 |
| `partitions` | `array<ProducePartitionData>` | 是 | 该 topic 下的分区写入集合 |

### 7.5 ProducePartitionData

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `partition` | `int32` | 是 | 分区编号 |
| `records` | `bytes` | 是 | 一个或多个连续 `RecordBatch` 的原始字节块 |

### 7.6 `records` 字段约定

- `records` 不是单条消息，而是 `RecordBatchSet`
- 第一版建议定义为“连续 `RecordBatch` 拼接的字节序列”
- 若只有一个 batch，则 `records` 中只包含一个 `RecordBatch`

### 7.7 Produce 请求体示意

```text
ProduceRequestBody
  transactionalId = "txn-order-app-01"
  acks = -1
  timeoutMs = 30000
  topicData = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          records = RecordBatchSet(bytes)
        },
        {
          partition = 1
          records = RecordBatchSet(bytes)
        }
      ]
    }
  ]
```

## 8. 配套文档说明

为了让 `Produce` 与 `Fetch` 两条链路形成完整协议闭环，以下结构已拆分为独立文档：

- [ProduceResponseBody 消息格式规范](./produce-response-format.md)
- [FetchRequestBody 消息格式规范](./fetch-request-format.md)

## 9. FetchResponseBody 结构

### 9.1 顶层结构

`FetchResponseBody` 用于 `apiKey = 3, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `sessionId` | `int32` | 是 | 第一版会话预留，可为 `0` |
| `responses` | `array<FetchTopicResponse>` | 是 | 按 topic 聚合的分区返回数据 |

### 9.2 FetchTopicResponse

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | `string` | 是 | Topic 名称 |
| `partitions` | `array<FetchPartitionResponse>` | 是 | 分区返回集合 |

### 9.3 FetchPartitionResponse

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `partition` | `int32` | 是 | 分区编号 |
| `errorCode` | `int16` | 是 | 分区级错误码 |
| `highWatermark` | `int64` | 是 | 当前高水位 |
| `logStartOffset` | `int64` | 是 | 当前日志起始位点 |
| `lastStableOffset` | `int64` | 是 | 最后稳定位点，`read_committed` 消费可见性的上界 |
| `abortedTransactions` | `array<AbortedTransaction>` | 是 | 与当前响应区间相关的已中止事务列表，可为空 |
| `records` | `nullable bytes` | 否 | 一个或多个连续 `RecordBatch` 的原始字节块 |

### 9.4 AbortedTransaction

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `producerId` | `int64` | 是 | 已中止事务对应的 producerId |
| `firstOffset` | `int64` | 是 | 该中止事务在本次可见区间中的首个 offset |

### 9.5 `records` 字段约定

- `records` 表示 `RecordBatchSet`
- 当分区当前无可返回数据时：
  - 可返回空 bytes
  - 或返回 `null`
  - 第一版建议统一返回空 bytes，减少多语言分支复杂度

## 10. FetchResponseBody 设计说明

### 10.1 为什么返回原始 batch bytes

因为高吞吐场景下，Broker 不应对日志段数据做多余重编码。  
返回原始 `RecordBatchSet` 有以下好处：

- 更容易与日志段格式贴近
- 更容易做 zero-copy
- 更容易复用到复制链路

### 10.2 普通 Consumer 与 Replica Fetch 的差异

二者可以复用同一套 `FetchResponseBody` 布局，但语义不同：

- 普通 Consumer：
  - `records` 只能读到 `highWatermark` 或 `lastStableOffset`
  - `read_committed` 模式下需要结合 `abortedTransactions` 过滤不可见记录
- Replica Fetch：
  - `records` 可用于追平更高位点数据
  - 仍需结合 epoch 逻辑校验

## 11. RecordBatchSet 结构说明

### 11.1 定义

`RecordBatchSet` 指：

- 一个或多个连续 `RecordBatch`
- 直接按顺序拼接成的原始字节序列

### 11.2 使用场景

- `ProducePartitionData.records`
- `FetchPartitionResponse.records`
- 复制链路的批量数据传输

## 12. 错误处理与字段级约束

### 12.1 Produce 请求约束

- `acks` 必须为 `0`、`1` 或 `-1`
- `timeoutMs` 必须大于 `0`
- `topicData` 不能为空
- 每个 `ProducePartitionData.records` 不能为空

### 12.2 Fetch 响应约束

- `highWatermark >= logStartOffset`
- `lastStableOffset >= logStartOffset`
- 非事务批或 `read_uncommitted` 视图下，`lastStableOffset` 可以等于 `highWatermark`
- 若 `errorCode != NONE`，则 `records` 可为空

### 12.3 RecordBatch 约束

- `recordCount >= 1`
- `lastOffsetDelta >= 0`
- `maxTimestamp >= baseTimestamp`
- `crc32c` 必须校验通过

## 13. 当前阶段暂不展开的高级内容

为了控制实现复杂度，以下内容第一版先不展开为正式字段：

- 复杂控制批
- 增量 fetch session
- 紧凑数组与变长 zigzag 编码

这些能力后续应通过：

- 新 `apiVersion`
- 新字段扩展
- 或明确的高级协议章节

## 14. 示例：ProducePartitionData.records

逻辑上：

```text
records =
  RecordBatchSet [
    RecordBatch #1
      -> Record #1
      -> Record #2
      -> Record #3
  ]
```

在网络上传输时：

- `records` 就是一段原始 bytes
- 不再包一层 JSON、文本或额外对象标签

## 15. 示例：FetchPartitionResponse.records

逻辑上：

```text
records =
  RecordBatchSet [
    RecordBatch #17
    RecordBatch #18
  ]
```

Broker 可以：

- 直接从日志段读取连续字节
- 直接返回给 Consumer 或 Follower

## 16. 后续待补充内容

下一阶段建议继续补充：

1. 压缩批的解压与校验语义
2. 事务协调器与 `transactionalId` 的交互细节
3. 消息格式兼容性测试样例

## 17. 结论

`RecordBatch`、`ProduceRequestBody`、`FetchResponseBody` 是 `stellflow` 数据面最核心的三个消息格式对象。第一版设计的关键不是追求极致复杂度，而是优先保证：

- 跨语言可实现
- 高吞吐批量传输友好
- 与日志段格式贴近
- 支撑幂等、事务和复制高级语义
