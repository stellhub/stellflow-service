# Stellflow ListOffsets 接口说明

## 1. 目标

`ListOffsets` 用于把“时间语义”转换成“可消费 offset”。

在 `stellflow` 当前设计里，它主要承担三类查询：

- `earliest`：查询当前还能读取到的最早 offset
- `latest`：查询当前日志尾部的最新 offset
- `timestamp`：按时间戳查找对应 offset

这类查询会被：

- Consumer 初次启动
- 按时间回放
- 管理工具排障
- 副本恢复或校验工具

反复使用。

## 2. 请求字段

当前字段级定义见：

- [api-versions-and-metadata-format.md](E:\PersonalCode\JavaProject\stellflow\docs\api-versions-and-metadata-format.md)
- [protocol-spec.md](E:\PersonalCode\JavaProject\stellflow\docs\protocol-spec.md)

这里重点解释分区级查询参数：

- `partition`
- `currentLeaderEpoch`
- `timestamp`
- `maxNumOffsets`

## 3. 三种查询语义

### 3.1 `earliest`

当 `timestamp = -2` 时，表示：

- 查询当前分区还能读取到的最早 offset

在当前实现里，它对应：

- `logStartOffset`

这不是“这个分区从历史上第一个消息开始的 offset”，而是：

- **经过 retention / cleanup / truncate 之后，现在仍然保留在本地日志中的最早 offset**

也就是说，如果旧 segment 已经被删除：

- earliest 会随之向前推进

### 3.2 `latest`

当 `timestamp = -1` 时，表示：

- 查询当前分区日志尾部的最新 offset

在当前实现里，它对应：

- `logEndOffset`

注意这里返回的是“下一个可写 offset”，也就是：

- 如果当前已有两条记录，offset 分别为 `0`、`1`
- 那么 `latest` 返回 `2`

这和 Kafka 风格 offset 语义一致。

### 3.3 `timestamp`

当 `timestamp >= 0` 时，表示：

- 按时间戳查找 offset

当前实现语义是：

- 返回**第一条时间戳不小于目标值**的记录 offset

也就是：

- 查的是 `>= timestamp`
- 不是 `< timestamp`
- 也不是“最接近但可能更小”的 floor 语义

这个设计更适合 MQ 的时间回放场景，因为它能直接回答：

- “从这个时间点开始，我应该从哪个 offset 开始消费”

## 4. `maxNumOffsets`

`maxNumOffsets` 表示：

- 本次最多返回多少个 offset

它的意义不是“返回多少条消息”，而是：

- 从匹配起点开始，最多返回多少个 offset 位置

当前实现支持：

- `earliest + maxNumOffsets`
- `timestamp + maxNumOffsets`
- `latest` 一般只返回一个 offset

例如：

- earliest = `0`
- 当前日志里有 `0,1,2,3`
- `maxNumOffsets = 2`

则返回：

- `[0,1]`

如果 `maxNumOffsets <= 0`，当前实现返回：

- `INVALID_REQUEST`

## 5. `currentLeaderEpoch`

`currentLeaderEpoch` 用于保护客户端和工具不要拿着过期的 leader 视图做查询。

它的作用是：

- 客户端带上自己认知中的 leader epoch
- Broker 校验这个 epoch 是否和本地分区一致

如果不一致，当前实现返回：

- `NOT_LEADER_OR_FOLLOWER`

这能避免两类问题：

- 客户端元数据过期但仍继续按旧 leader 视图访问
- 控制面刚切主时，时间查询落到过期节点

## 6. `OFFSET_OUT_OF_RANGE`

对于普通 `timestamp` 查询，如果目标时间戳已经明显超出当前本地日志可匹配范围，当前实现会返回：

- `OFFSET_OUT_OF_RANGE`

这和 `earliest/latest` 不同：

- `earliest/latest` 总能给出一个边界值
- 普通时间戳查询要求“找到一个正式的时间对应 offset”

如果整个时间窗口已经被 retention 删除，或者时间戳比当前最新记录还新，就可能返回这个错误。

## 7. 例子

假设一个分区当前状态如下：

- `logStartOffset = 5`
- `logEndOffset = 9`
- 保留的记录 offset 为 `5,6,7,8`
- 对应时间戳为：
  - `5 -> 1000`
  - `6 -> 2000`
  - `7 -> 3000`
  - `8 -> 4000`

那么：

- `timestamp = -2`
  返回 earliest = `5`
- `timestamp = -1`
  返回 latest = `9`
- `timestamp = 2500`
  返回 `7`
- `timestamp = 5000`
  返回 `OFFSET_OUT_OF_RANGE`

## 8. 为什么它重要

`ListOffsets` 看起来只是一个“查 offset”的接口，但它实际上把下面几层能力串起来了：

- `time index`
- `logStartOffset`
- `logEndOffset`
- `leaderEpoch`
- retention / truncate

所以它不是简单工具接口，而是：

- **存储层边界对外暴露的正式查询入口**

如果后面继续扩展，优先可以补：

1. 更细的错误码区分
2. 按高水位而不是按 LEO 的 latest 变体
3. 更完整的多 topic / 多 partition 管理工具支持
