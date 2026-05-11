# Stellflow FetchRequestBody 消息格式规范

## 1. 文档目的

本文档用于正式定义 `stellflow` 数据面协议中 `FetchRequestBody` 的字段级格式，作为 [Produce / Fetch 消息格式规范](./produce-fetch-message-format.md) 的配套文档，与其中的 `FetchResponseBody` 形成完整闭环。

本文档目标是为：

- Java Broker
- Java Client
- Golang Client
- Broker 间副本复制链路

提供统一的 `Fetch` 请求格式契约。

## 2. 适用范围

本文档适用于以下场景：

- Consumer 向 Broker 发起消息拉取
- Follower 向 Leader 发起副本同步拉取

当前文档定义的版本范围为：

- `apiKey = 3`
- `apiVersion = 0`

## 3. 设计目标

`FetchRequestBody` 第一版的设计目标是：

- 支持普通 Consumer 拉取
- 支持 Follower 副本同步拉取
- 支持长轮询
- 支持批量按 topic/partition 聚合请求
- 支持吞吐优先的大窗口拉取
- 支持后续扩展 fetch session、事务隔离和更复杂复制语义

## 4. 总体结构

### 4.1 顶层结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `replicaId` | `int32` | 是 | 请求来源标识，普通 Consumer 固定为 `-1` |
| `maxWaitMs` | `int32` | 是 | 最大等待时间，用于长轮询 |
| `minBytes` | `int32` | 是 | 最小返回字节数 |
| `maxBytes` | `int32` | 是 | 响应体允许返回的最大总字节数 |
| `isolationLevel` | `int8` | 是 | 隔离级别 |
| `sessionId` | `int32` | 是 | 第一版会话预留，默认 `0` |
| `topicPartitions` | `array<FetchTopicRequest>` | 是 | 按 topic 聚合的分区拉取请求 |

### 4.2 设计说明

- `FetchRequestBody` 是“按 topic -> partition 聚合”的批量拉取请求
- 第一版保持结构简单，优先服务大吞吐批量拉取
- `replicaId` 用于区分普通消费与副本同步

## 5. 顶层字段语义

### 5.1 `replicaId`

| 取值 | 语义 |
| --- | --- |
| `-1` | 普通 Consumer 请求 |
| `>= 0` | Follower 副本请求，值为请求方 brokerId |

说明：

- 第一版用 `replicaId` 来区分 Consumer Fetch 和 Replica Fetch
- 后续若引入更多内部角色，可通过新版本扩展

### 5.2 `maxWaitMs`

- 仅在当前无足够数据时生效
- Broker 可以等待至多 `maxWaitMs` 毫秒，以积累更多数据再返回
- 若超时仍无足够数据，可返回空结果

### 5.3 `minBytes`

- Broker 期望返回至少 `minBytes` 数据
- 若累计可返回数据不足，Broker 可在 `maxWaitMs` 内继续等待
- 用于减少频繁小返回

### 5.4 `maxBytes`

- Broker 返回的整个 `FetchResponseBody` 中 `records` 总量不应超过此值
- 该值是吞吐与内存控制的重要参数

### 5.5 `isolationLevel`

建议定义如下：

| 值 | 含义 |
| --- | --- |
| `0` | `read_uncommitted` |
| `1` | `read_committed` |

说明：

- 普通 Consumer 使用该字段控制 `read_uncommitted / read_committed` 可见性
- 副本同步一般不按普通事务可见性限制
- 当值为 `1` 时，Broker 应结合 `lastStableOffset` 与 `abortedTransactions` 保证事务可见性

### 5.6 `sessionId`

- 第一版作为会话扩展保留字段
- 未启用 fetch session 时固定为 `0`
- 后续可用于增量 fetch session

## 6. FetchTopicRequest 结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | `string` | 是 | Topic 名称 |
| `partitions` | `array<FetchPartitionRequest>` | 是 | 该 topic 下的分区拉取集合 |

## 7. FetchPartitionRequest 结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `partition` | `int32` | 是 | 分区编号 |
| `currentLeaderEpoch` | `int32` | 是 | 当前已知 leader epoch，未知时为 `-1` |
| `fetchOffset` | `int64` | 是 | 起始拉取位点 |
| `logStartOffset` | `int64` | 是 | 请求方已知日志起始位点，未知时为 `-1` |
| `partitionMaxBytes` | `int32` | 是 | 当前分区允许返回的最大字节数 |

## 8. FetchPartitionRequest 字段语义

### 8.1 `partition`

- 要拉取的目标分区编号

### 8.2 `currentLeaderEpoch`

- 用于 leader epoch 相关一致性校验
- 普通 Consumer 未启用该能力时可设置为 `-1`
- Follower 副本同步时建议携带已知 epoch

补充说明：

- `currentLeaderEpoch` 是请求方对“当前分区 Leader 属于哪一代”的已知判断
- Broker 可以利用它识别客户端或副本的元数据是否已经过期
- 当请求方携带的 epoch 落后于当前真实 Leader 时，服务端可返回错误、触发元数据刷新或引导截断恢复

### 8.3 `fetchOffset`

- 从哪个逻辑 offset 开始拉取
- 对 Consumer 来说，通常来自消费位点或 seek 位置
- 对 Follower 来说，通常来自本地 LEO 或截断后的追平位点

### 8.4 `logStartOffset`

- 请求方已知的日志起始位点
- 用于副本同步和位移边界辅助判断
- 普通 Consumer 不依赖时可为 `-1`

### 8.5 `partitionMaxBytes`

- 当前分区本次请求最多允许返回的字节数
- 与顶层 `maxBytes` 共同约束最终响应大小

## 9. Fetch 请求体示意

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
        },
        {
          partition = 1
          currentLeaderEpoch = -1
          fetchOffset = 204800
          logStartOffset = -1
          partitionMaxBytes = 4194304
        }
      ]
    }
  ]
```

## 10. Consumer Fetch 与 Replica Fetch 的差异

### 10.1 Consumer Fetch

典型特征：

- `replicaId = -1`
- 主要关注 `fetchOffset`
- 按 `highWatermark` 或 `lastStableOffset` 控制可见性
- `currentLeaderEpoch` 可先用 `-1`

### 10.2 Replica Fetch

典型特征：

- `replicaId >= 0`
- 必须更关注 `currentLeaderEpoch`
- 通常使用更大的 `partitionMaxBytes`
- 关注追平速度与复制窗口，而不是单次响应延迟

## 11. 长轮询语义

### 11.1 生效条件

当以下条件同时成立时，Broker 可进入等待：

- 当前可返回数据不足 `minBytes`
- 当前请求未超出 `maxWaitMs`
- 分区状态允许等待

### 11.2 提前返回条件

满足以下任一条件时可提前返回：

- 已累计数据达到或超过 `minBytes`
- 达到 `maxWaitMs`
- 出现错误或状态变化
- 分区数据新增后已满足返回条件

## 12. 字段约束

### 12.1 顶层约束

- `maxWaitMs >= 0`
- `minBytes >= 0`
- `maxBytes > 0`
- `topicPartitions` 不得为空

### 12.2 分区级约束

- `fetchOffset >= 0`
- `partitionMaxBytes > 0`
- `currentLeaderEpoch` 可为 `-1` 或合法 epoch
- `logStartOffset` 可为 `-1` 或合法 offset

### 12.3 一致性约束

- 顶层 `maxBytes` 应大于等于单分区 `partitionMaxBytes` 的合理组合上限
- Broker 不保证所有分区都返回数据，但必须逐分区给出结果或错误

## 13. 与 FetchResponseBody 的对应关系

请求与响应的核心对应关系如下：

| 请求字段 | 响应字段 | 说明 |
| --- | --- | --- |
| `partition` | `partition` | 分区级结果对应 |
| `fetchOffset` | `records` | 从指定起始位点返回 batch 数据 |
| `currentLeaderEpoch` | `errorCode` / `records` | epoch 不一致时可能触发错误或截断逻辑 |
| `partitionMaxBytes` | `records` | 限制单分区响应大小 |
| `isolationLevel` | `lastStableOffset` / `records` | 控制可见性边界 |

## 14. `leaderEpoch` 与 `Term` 的关系说明

`currentLeaderEpoch` 很容易让人联想到 Raft 的 `Term`，二者思想确实相近，但并不完全等同。

可以这样理解：

- `Term`：Controller Quorum 共识层的任期号
- `leaderEpoch`：某个 Partition Leader 的代次号

共同点：

- 都在表达“谁是更新的一代”
- 都用于识别过期视图

区别：

- `Term` 是共识组级别的领导任期
- `leaderEpoch` 是分区复制层的局部领导代次

因此：

- `leaderEpoch` 可以理解成分区级、局部语义下的 term-like generation
- 但它不是 Controller Quorum 的 `Term` 本身

## 15. 当前阶段暂不展开的高级内容

为控制复杂度，以下内容暂不在第一版 `FetchRequestBody` 中正式展开：

- 增量 fetch session 细节
- rack-aware fetch
- observer 副本专用 fetch 字段

这些内容应在后续通过新 `apiVersion` 扩展。

## 16. 后续待补充内容

建议下一阶段继续补充：

1. `FetchRequestBody` 与 `FetchResponseBody` 的样例报文
2. Replica Fetch 的 epoch 冲突处理流程
3. 长轮询实现与超时策略细化
4. Fetch session 增量协议设计

## 17. 结论

`FetchRequestBody` 的关键不是字段越多越好，而是先稳定定义好：

- 来源类型
- 起始位点
- 等待策略
- 字节窗口
- epoch 校验信息

只要这几个核心契约明确，Consumer Fetch 和 Replica Fetch 都能在同一套请求格式上稳定演进。
