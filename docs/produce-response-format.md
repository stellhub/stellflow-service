# Stellflow ProduceResponseBody 消息格式规范

## 1. 文档目的

本文档用于正式定义 `stellflow` 数据面协议中 `ProduceResponseBody` 的字段级格式，作为 [Produce / Fetch 消息格式规范](./produce-fetch-message-format.md) 的配套文档，与其中的 `ProduceRequestBody` 形成完整闭环。

本文档面向：

- Java Broker
- Java Client
- Golang Client

用于统一 `Produce` 请求成功或失败后的返回格式。

## 2. 适用范围

当前文档定义的版本范围为：

- `apiKey = 2`
- `apiVersion = 0`

## 3. 设计目标

`ProduceResponseBody` 第一版设计目标：

- 支持 topic / partition 级结果返回
- 支持逐分区错误码返回
- 支持返回写入后的起始 offset
- 支持返回与时间和日志边界相关的关键元信息
- 支持与 `acks=0/1/all` 语义配合

## 4. 总体结构

### 4.1 顶层结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `responses` | `array<ProduceTopicResponse>` | 是 | 按 topic 聚合的分区结果 |

说明：

- 顶层节流时间已在统一响应头 `throttleTimeMs` 中表达
- 顶层错误码在响应头中表达请求级失败
- 分区级结果在响应体中表达局部成功或局部失败

## 5. ProduceTopicResponse 结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | `string` | 是 | Topic 名称 |
| `partitions` | `array<ProducePartitionResponse>` | 是 | 该 topic 下的分区结果 |

## 6. ProducePartitionResponse 结构

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `partition` | `int32` | 是 | 分区编号 |
| `errorCode` | `int16` | 是 | 分区级错误码 |
| `baseOffset` | `int64` | 是 | 本次写入批的起始 offset，失败时可为 `-1` |
| `currentLeaderEpoch` | `int32` | 是 | 当前分区 leader epoch |
| `logAppendTimeMs` | `int64` | 是 | 使用 log append time 时返回该值，否则为 `-1` |
| `logStartOffset` | `int64` | 是 | 当前日志起始位点 |

## 7. 字段语义说明

### 7.1 `partition`

- 对应 `ProduceRequestBody` 中的目标分区
- 即使某个分区写入失败，也必须尽量返回该分区的结果项

### 7.2 `errorCode`

- 用于表达该分区是否写入成功
- 若值为 `NONE`，表示该分区写入成功
- 若顶层响应头已返回请求级错误，分区级结果可为空或统一错误，但第一版建议尽量保留分区级结果

### 7.3 `baseOffset`

- 表示本次写入批在该分区上的首条逻辑 offset
- 这是客户端后续确认写入位置的重要依据
- 写入失败时使用 `-1`

### 7.4 `logAppendTimeMs`

- 若 Broker 使用 `logAppendTime`，则返回实际写入时间
- 若使用 `createTime` 语义，则返回 `-1`

### 7.5 `logStartOffset`

- 返回当前日志起始位点
- 用于客户端在某些恢复或诊断场景下感知日志边界

### 7.6 `currentLeaderEpoch`

- 用于帮助客户端和副本同步端判断元数据是否过期
- 在重试、幂等去重和错误恢复时可作为辅助判断依据

## 8. ProduceResponseBody 示例

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

## 9. 分区级错误码建议

### 9.1 常见成功与失败

`ProducePartitionResponse.errorCode` 第一版重点支持以下错误码：

| errorCode | 名称 | 说明 |
| --- | --- | --- |
| `0` | `NONE` | 写入成功 |
| `8` | `LEADER_NOT_AVAILABLE` | 当前无可写 Leader |
| `9` | `NOT_LEADER_OR_FOLLOWER` | 目标节点不是可写角色 |
| `10` | `UNKNOWN_TOPIC_OR_PARTITION` | topic 或 partition 不存在 |
| `12` | `MESSAGE_TOO_LARGE` | 单条消息过大 |
| `13` | `RECORD_LIST_TOO_LARGE` | 批次过大 |
| `14` | `INVALID_RECORD` | 消息格式非法 |
| `15` | `CORRUPT_MESSAGE` | 批次数据损坏 |
| `5` | `AUTHORIZATION_FAILED` | 无写权限 |
| `6` | `THROTTLED` | 被节流 |
| `21` | `INVALID_PRODUCER_EPOCH` | producer epoch 非法或已失效 |
| `22` | `OUT_OF_ORDER_SEQUENCE_NUMBER` | 幂等序列号乱序 |
| `23` | `DUPLICATE_SEQUENCE_NUMBER` | 幂等序列号重复 |
| `25` | `INVALID_TXN_STATE` | 当前事务状态不允许写入 |
| `26` | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | 事务 ID 无写权限 |
| `27` | `PRODUCER_FENCED` | producer 已被更新的 epoch 围栏 |

### 9.2 错误码处理原则

- 顶层请求失败由响应头 `errorCode` 表达
- 单分区失败优先使用分区级 `errorCode` 表达
- 同一请求中允许部分分区成功、部分分区失败

## 10. 与 `acks` 语义的关系

### 10.1 `acks = 0`

特点：

- 客户端不要求 Broker 返回成功确认
- 实现上可允许服务端不发送完整 `ProduceResponseBody`
- 为统一协议和调试便利，第一版仍建议协议层允许标准响应格式存在，但客户端可选择忽略

### 10.2 `acks = 1`

特点：

- Leader 本地写入成功即可返回
- `baseOffset` 在本地写入成功后可立即确定

### 10.3 `acks = -1`

特点：

- 需等待满足 ISR / `acks=all` 语义
- 若超时或副本条件不满足，返回对应错误码

## 11. Topic / Partition 级返回原则

### 11.1 为什么按 topic / partition 返回

因为 `ProduceRequestBody` 本身就是按 topic / partition 聚合发送，所以响应也必须保持同样的结构分层，方便：

- 客户端逐分区匹配结果
- 多语言客户端统一解码
- 局部失败时精确处理

### 11.2 返回顺序原则

建议：

- topic 返回顺序与请求顺序一致
- partition 返回顺序与请求顺序一致

这样可以降低客户端映射复杂度。

## 12. 边界与异常场景

### 12.1 部分成功

允许以下情况出现：

- 一个 topic 中的多个分区，部分成功，部分失败
- 同一请求中不同 topic，部分成功，部分失败

### 12.2 全部失败

若请求解析成功，但所有分区都失败：

- 顶层响应头 `errorCode` 可仍为 `NONE`
- 分区级 `errorCode` 分别返回真实失败原因

若请求连基本解析都失败：

- 由顶层响应头 `errorCode` 返回请求级错误

### 12.3 不可写分区

典型场景：

- 分区 Leader 不可用
- 当前 Broker 非 Leader
- 元数据过期

此时分区级应返回：

- `LEADER_NOT_AVAILABLE`
- `NOT_LEADER_OR_FOLLOWER`

## 13. 字段约束

### 13.1 顶层约束

- `responses` 可以为空，但正常情况下应至少与请求 topic 集合对应

### 13.2 分区约束

- `partition >= 0`
- `errorCode = NONE` 时：
  - `baseOffset >= 0`
  - `currentLeaderEpoch >= 0`
  - `logStartOffset >= 0`
- `errorCode != NONE` 时：
  - `baseOffset = -1`
  - `logAppendTimeMs` 可为 `-1`

## 14. 与 ProduceRequestBody 的对应关系

| 请求字段 | 响应字段 | 说明 |
| --- | --- | --- |
| `topic` | `topic` | 按 topic 对应 |
| `partition` | `partition` | 按分区对应 |
| `records` | `baseOffset` | 返回写入后的起始 offset |
| `acks` | 响应返回时机 | 控制何时返回该结果 |
| `timeoutMs` | `errorCode` | 超时可能转化为分区级失败 |

## 15. 当前阶段暂不展开的高级内容

当前阶段暂不正式展开：

- 每批次最后 offset 单独返回
- 分区级错误详细文本

这些内容可在后续通过新 `apiVersion` 扩展。

## 16. 后续待补充内容

建议下一阶段继续补充：

1. `ProduceResponseBody` 样例报文
2. `acks=0` 的实际网络行为约束
3. 延迟完成与 `acks=all` 的错误映射规则
4. 幂等与事务错误码的更细粒度恢复建议

## 17. 结论

`ProduceResponseBody` 的核心价值在于把写入结果精确下沉到 topic / partition 级别，并稳定返回：

- 哪个分区成功
- 哪个分区失败
- 成功写入从哪个 offset 开始
- 当前日志边界是什么

只要这层契约稳定，客户端就能可靠地基于分区级结果做重试、诊断和上层确认逻辑。
