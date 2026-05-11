# Stellflow 消息格式样例报文

## 1. 文档目的

本文档为 `stellflow` 数据面协议提供一组完整、可读、可对照实现的样例报文，覆盖以下核心对象：

- `RecordBatch`
- `ProduceRequest`
- `ProduceResponse`
- `FetchRequest`
- `FetchResponse`

本文档的目标不是替代正式字段规范，而是帮助实现者在编码、联调、抓包和测试时快速理解各字段之间的关系。

本文档配套以下规范一起使用：

- [协议规范文档](./protocol-spec.md)
- [Produce / Fetch 消息格式规范](./produce-fetch-message-format.md)
- [FetchRequestBody 消息格式规范](./fetch-request-format.md)
- [ProduceResponseBody 消息格式规范](./produce-response-format.md)

## 2. 阅读约定

本文档中的样例报文采用三层表达：

1. 逻辑对象视图
2. 字段展开视图
3. 二进制布局示意

说明：

- 二进制布局示意主要用于帮助理解字段顺序，不要求这里给出逐字节十六进制严格编码
- 第一版重点是让 Java 与 Go 两端对字段语义和顺序形成一致认识

## 3. 样例约束

为了让样例简单且稳定，本文统一使用以下前提：

- `apiVersion = 0`
- `producerId = -1`
- `producerEpoch = -1`
- `baseSequence = -1`
- `compressionType = none`
- `timestampType = createTime`

说明：

- 样例一到样例八以普通非事务批为主，便于先看清基础布局
- 这不代表 `stellflow` 不支持幂等或事务
- 文末已补充幂等与事务相关样例

## 4. 样例一：RecordBatch

### 4.1 业务含义

向 `orders` topic 的 `0` 号分区发送 2 条消息：

- 第一条：
  - `key = "order-1001"`
  - `value = {"amount":199}`
- 第二条：
  - `key = "order-1002"`
  - `value = {"amount":299}`

### 4.2 RecordBatch 逻辑对象

```text
RecordBatch
  baseOffsetDelta = 0
  batchLength = 210
  partitionLeaderEpoch = -1
  magic = 1
  crc32c = <calculated>
  attributes = 0
  lastOffsetDelta = 1
  baseTimestamp = 1746768000000
  maxTimestamp = 1746768000010
  producerId = -1
  producerEpoch = -1
  baseSequence = -1
  recordCount = 2
  records = [
    Record#0,
    Record#1
  ]
```

### 4.3 Record#0

```text
Record
  length = 58
  attributes = 0
  timestampDelta = 0
  offsetDelta = 0
  key = "order-1001"
  value = "{\"amount\":199}"
  headers = [
    ("content-type", "application/json")
  ]
```

### 4.4 Record#1

```text
Record
  length = 58
  attributes = 0
  timestampDelta = 10
  offsetDelta = 1
  key = "order-1002"
  value = "{\"amount\":299}"
  headers = [
    ("content-type", "application/json")
  ]
```

### 4.5 RecordBatch 字段顺序示意

```text
RecordBatch
  -> baseOffsetDelta
  -> batchLength
  -> partitionLeaderEpoch
  -> magic
  -> crc32c
  -> attributes
  -> lastOffsetDelta
  -> baseTimestamp
  -> maxTimestamp
  -> producerId
  -> producerEpoch
  -> baseSequence
  -> recordCount
  -> Record#0
  -> Record#1
```

## 5. 样例二：ProduceRequest

### 5.1 场景

Producer 向 Broker 发送一个 `ProduceRequest`：

- `acks = -1`
- `timeoutMs = 30000`
- topic 为 `orders`
- 分区为 `0`
- 载荷为一个 `RecordBatchSet`，其中只包含上面的 1 个 `RecordBatch`

### 5.2 Request Header 样例

```text
RequestHeader
  apiKey = 2
  apiVersion = 0
  headerVersion = 2
  correlationId = 10001
  clientId = "stellflow-java-producer"
  traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  spanId = "00f067aa0ba902b7"
  traceFlags = 1
  tenantId = "tenant-order"
  quotaKey = "quota-order-write"
  authContextId = "authctx-prod-a"
  trafficClass = 0
  trafficTag = null
  flags = 0
```

### 5.3 ProduceRequestBody 样例

```text
ProduceRequestBody
  transactionalId = null
  acks = -1
  timeoutMs = 30000
  topicData = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          records = RecordBatchSet(
            RecordBatch#1
          )
        }
      ]
    }
  ]
```

### 5.4 ProduceRequest 完整逻辑视图

```text
ProduceRequest
  frameLength = <calculated>
  header = RequestHeader(apiKey=2, apiVersion=0, correlationId=10001, ...)
  body = ProduceRequestBody(...)
```

### 5.5 ProduceRequest 二进制顺序示意

```text
frameLength
  -> apiKey
  -> apiVersion
  -> headerVersion
  -> correlationId
  -> clientId
  -> traceId
  -> spanId
  -> traceFlags
  -> tenantId
  -> quotaKey
  -> authContextId
  -> trafficClass
  -> trafficTag
  -> flags
  -> transactionalId
  -> acks
  -> timeoutMs
  -> topicData.length
    -> topic
    -> partitions.length
      -> partition
      -> records.length
      -> RecordBatchSet bytes
```

## 6. 样例三：ProduceResponse

### 6.1 场景

Broker 成功写入 `orders-0` 分区，并返回：

- 分区成功
- 起始 offset 为 `102400`
- 当前日志起始位点为 `0`
- 未使用 `logAppendTime`

### 6.2 Response Header 样例

```text
ResponseHeader
  correlationId = 10001
  headerVersion = 2
  errorCode = 0
  throttleTimeMs = 0
```

### 6.3 ProduceResponseBody 样例

```text
ProduceResponseBody
  responses = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          errorCode = 0
          baseOffset = 102400
          currentLeaderEpoch = 12
          logAppendTimeMs = -1
          logStartOffset = 0
        }
      ]
    }
  ]
```

### 6.4 ProduceResponse 完整逻辑视图

```text
ProduceResponse
  frameLength = <calculated>
  header = ResponseHeader(correlationId=10001, errorCode=0, ...)
  body = ProduceResponseBody(...)
```

## 7. 样例四：FetchRequest

### 7.1 场景

Consumer 从 `orders-0` 分区拉取数据：

- `replicaId = -1`
- `maxWaitMs = 500`
- `minBytes = 1048576`
- `maxBytes = 8388608`
- `fetchOffset = 102400`
- `partitionMaxBytes = 4194304`

### 7.2 Request Header 样例

```text
RequestHeader
  apiKey = 3
  apiVersion = 0
  headerVersion = 2
  correlationId = 10002
  clientId = "stellflow-java-consumer"
  traceId = "4bf92f3577b34da6a3ce929d0e0e4736"
  spanId = "00f067aa0ba902c1"
  traceFlags = 1
  tenantId = "tenant-order"
  quotaKey = "quota-order-read"
  authContextId = "authctx-consumer-a"
  trafficClass = 0
  trafficTag = null
  flags = 0
```

### 7.3 FetchRequestBody 样例

```text
FetchRequestBody
  replicaId = -1
  maxWaitMs = 500
  minBytes = 1048576
  maxBytes = 8388608
  isolationLevel = 0
  sessionId = 0
  topicPartitions = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          currentLeaderEpoch = -1
          fetchOffset = 102400
          logStartOffset = -1
          partitionMaxBytes = 4194304
        }
      ]
    }
  ]
```

### 7.4 FetchRequest 完整逻辑视图

```text
FetchRequest
  frameLength = <calculated>
  header = RequestHeader(apiKey=3, apiVersion=0, correlationId=10002, ...)
  body = FetchRequestBody(...)
```

## 8. 样例五：FetchResponse

### 8.1 场景

Broker 返回 `orders-0` 分区数据：

- `highWatermark = 102402`
- `logStartOffset = 0`
- `lastStableOffset = 102402`
- 返回 1 个 `RecordBatch`

### 8.2 Response Header 样例

```text
ResponseHeader
  correlationId = 10002
  headerVersion = 2
  errorCode = 0
  throttleTimeMs = 0
```

### 8.3 FetchResponseBody 样例

```text
FetchResponseBody
  sessionId = 0
  responses = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          errorCode = 0
          highWatermark = 102402
          logStartOffset = 0
          lastStableOffset = 102402
          abortedTransactions = []
          records = RecordBatchSet(
            RecordBatch#17
          )
        }
      ]
    }
  ]
```

### 8.4 FetchResponse 完整逻辑视图

```text
FetchResponse
  frameLength = <calculated>
  header = ResponseHeader(correlationId=10002, errorCode=0, ...)
  body = FetchResponseBody(...)
```

## 9. 样例六：Replica FetchRequest

### 9.1 场景

Follower Broker `2` 从 Leader 拉取 `orders-0` 分区数据：

- `replicaId = 2`
- `fetchOffset = 204800`
- `currentLeaderEpoch = 12`
- `partitionMaxBytes = 8388608`

### 9.2 Replica FetchRequestBody 样例

```text
FetchRequestBody
  replicaId = 2
  maxWaitMs = 100
  minBytes = 1
  maxBytes = 16777216
  isolationLevel = 0
  sessionId = 0
  topicPartitions = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          currentLeaderEpoch = 12
          fetchOffset = 204800
          logStartOffset = 0
          partitionMaxBytes = 8388608
        }
      ]
    }
  ]
```

### 9.3 与普通 Consumer Fetch 的差异

- `replicaId` 不再是 `-1`
- `currentLeaderEpoch` 需要显式携带
- `partitionMaxBytes` 通常更大
- 更关注吞吐和追平，而不是单次延迟

## 10. 样例七：ProduceResponse 局部失败

### 10.1 场景

同一请求中：

- `orders-0` 成功
- `orders-1` 返回 `NOT_LEADER_OR_FOLLOWER`

### 10.2 ProduceResponseBody 样例

```text
ProduceResponseBody
  responses = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          errorCode = 0
          baseOffset = 102400
          currentLeaderEpoch = 12
          logAppendTimeMs = -1
          logStartOffset = 0
        },
        {
          partition = 1
          errorCode = 9
          baseOffset = -1
          currentLeaderEpoch = 12
          logAppendTimeMs = -1
          logStartOffset = 0
        }
      ]
    }
  ]
```

## 11. 样例八：FetchResponse 空返回

### 11.1 场景

在 `maxWaitMs` 超时前，Broker 没有积累到足够数据，但请求格式和分区状态都正常。

### 11.2 FetchResponseBody 样例

```text
FetchResponseBody
  sessionId = 0
  responses = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          errorCode = 0
          highWatermark = 102402
          logStartOffset = 0
          lastStableOffset = 102402
          abortedTransactions = []
          records = empty-bytes
        }
      ]
    }
  ]
```

## 12. 字段关系速查表

### 12.1 Produce 链路

| 对象 | 关键字段 |
| --- | --- |
| Request Header | `apiKey=2`, `apiVersion`, `headerVersion`, `correlationId`, `clientId`, `traceId`, `tenantId` |
| ProduceRequestBody | `acks`, `timeoutMs`, `topicData` |
| ProducePartitionData | `partition`, `records` |
| ProduceResponseBody | `topic`, `partition`, `errorCode`, `baseOffset` |

### 12.2 Fetch 链路

| 对象 | 关键字段 |
| --- | --- |
| Request Header | `apiKey=3`, `apiVersion`, `headerVersion`, `correlationId`, `clientId`, `traceId`, `tenantId` |
| FetchRequestBody | `replicaId`, `maxWaitMs`, `minBytes`, `maxBytes`, `topicPartitions` |
| FetchPartitionRequest | `partition`, `fetchOffset`, `partitionMaxBytes` |
| FetchResponseBody | `highWatermark`, `lastStableOffset`, `records` |

## 13. 样例报文的用途

这些样例报文建议用于：

- Java / Go 编解码单元测试
- 集成测试 golden file
- 抓包字段核对
- 协议文档评审

## 14. 后续待补充内容

建议下一阶段继续补充：

1. 样例报文对应的十六进制示意
2. gzip / lz4 / zstd 压缩批样例
3. epoch 不一致的 Replica Fetch 错误样例

## 14. 样例九：幂等 ProduceRequest

### 14.1 场景

Producer 以幂等模式向 `orders-0` 发送 1 个 batch：

- `producerId = 9001`
- `producerEpoch = 3`
- `baseSequence = 1200`

### 14.2 Request Header 样例

```text
RequestHeader
  apiKey = 2
  apiVersion = 0
  headerVersion = 2
  correlationId = 10003
  clientId = "stellflow-java-idempotent-producer"
  traceId = "4bf92f3577b34da6a3ce929d0e0e4737"
  spanId = "00f067aa0ba902d1"
  traceFlags = 1
  tenantId = "tenant-order"
  quotaKey = "quota-order-write"
  authContextId = "authctx-prod-a"
  trafficClass = 1
  trafficTag = "exp-order-v2-a"
  flags = 0
```

### 14.3 RecordBatch 关键字段

```text
RecordBatch
  producerId = 9001
  producerEpoch = 3
  baseSequence = 1200
  attributes = 0
```

## 15. 样例十：事务 ProduceRequest / FetchResponse

### 15.1 事务 ProduceRequest

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
          records = RecordBatchSet(
            RecordBatch(
              attributes = bit5(isTransactional)=1
              producerId = 9001
              producerEpoch = 4
              baseSequence = 1300
            )
          )
        }
      ]
    }
  ]
```

### 15.2 `read_committed` FetchResponse

```text
FetchResponseBody
  sessionId = 0
  responses = [
    {
      topic = "orders"
      partitions = [
        {
          partition = 0
          errorCode = 0
          highWatermark = 103000
          logStartOffset = 0
          lastStableOffset = 102980
          abortedTransactions = [
            {
              producerId = 9001
              firstOffset = 102950
            }
          ]
          records = RecordBatchSet(
            RecordBatch#31,
            RecordBatch#32
          )
        }
      ]
    }
  ]
```

## 16. 结论

协议规范决定字段定义，样例报文决定实现是否容易对齐。对于 `stellflow` 这种要同时支持 Java 和 Go 客户端的系统，样例报文是非常重要的中间层文档，它能显著降低“字段都看懂了，但实现出来仍然不一致”的风险。
