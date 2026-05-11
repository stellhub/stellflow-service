# Stellflow OpenTelemetry 指标字典

## 1. 文档目的

本文档用于定义 `stellflow` 在 Broker、Controller、Client 三个层面的核心 OpenTelemetry 指标字典，作为后续埋点、仪表盘、告警与采样策略的统一依据。

本文档遵循：

- [ADR-0004 可观测性选型](./adr/ADR-0004-observability.md)
- [ADR-0007 OpenTelemetry 指标命名与标签规范](./adr/ADR-0007-opentelemetry-metrics-naming.md)

## 2. 指标总原则

### 2.1 命名原则

- 统一使用 `stellflow.` 前缀
- 统一使用点分风格命名
- 名称表达业务语义，不直接暴露内部类名

### 2.2 标签原则

- 只使用高价值、低基数标签
- 默认标签必须跨语言可复用
- 严禁在核心指标中引入高基数 `client.id`、`connection.id`、原始 IP 地址

### 2.3 单位与类型原则

- 计数：`Counter`
- 当前值：`UpDownCounter` 或 `Gauge`
- 延迟：`Histogram`
- 大小/字节量：`Counter` 或 `Histogram`

## 3. 通用标签字典

| 标签名 | 适用范围 | 说明 |
| --- | --- | --- |
| `node.id` | Broker / Controller | 节点标识 |
| `node.role` | Broker / Controller / Client | 例如 `broker`、`controller`、`producer`、`consumer` |
| `api` | Broker / Client | API 名称，例如 `Produce`、`Fetch` |
| `result` | 全局 | `success`、`error`、`timeout`、`throttled` |
| `topic` | Broker / Client | Topic 名称，默认仅在分主题观测场景使用 |
| `partition` | Broker | Partition 编号，默认仅在局部诊断指标使用 |
| `client.type` | Client | `java`、`go` 等 |
| `coordinator.type` | Broker / Controller | `group`、`transaction` |
| `replica.role` | Broker | `leader`、`follower` |

说明：

- `topic`、`partition` 只建议用于局部细粒度指标，不建议无节制铺开到所有核心全局指标。

## 4. Broker 指标字典

### 4.1 网络与请求指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.request.count` | Counter | requests | `node.id`, `api`, `result` | Broker 收到的请求总数 |
| `stellflow.request.latency` | Histogram | ms | `node.id`, `api`, `result` | 请求总处理延迟 |
| `stellflow.request.queue.depth` | Gauge | count | `node.id` | 请求队列当前深度 |
| `stellflow.response.queue.depth` | Gauge | count | `node.id` | 响应队列当前深度 |
| `stellflow.connection.count` | Gauge | count | `node.id` | 当前活跃连接数 |
| `stellflow.request.decode.error.count` | Counter | errors | `node.id`, `api` | 请求解码失败次数 |
| `stellflow.request.throttle.count` | Counter | events | `node.id`, `api` | 请求被节流次数 |
| `stellflow.request.throttle.time` | Histogram | ms | `node.id`, `api` | 请求节流时长 |

### 4.2 Produce / Fetch 核心指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.produce.request.count` | Counter | requests | `node.id`, `result` | Produce 请求数 |
| `stellflow.produce.bytes` | Counter | bytes | `node.id` | Produce 写入总字节数 |
| `stellflow.produce.records` | Counter | records | `node.id` | Produce 写入总记录数 |
| `stellflow.produce.latency` | Histogram | ms | `node.id`, `result` | Produce 请求延迟 |
| `stellflow.fetch.request.count` | Counter | requests | `node.id`, `result` | Fetch 请求数 |
| `stellflow.fetch.bytes` | Counter | bytes | `node.id` | Fetch 返回总字节数 |
| `stellflow.fetch.records` | Counter | records | `node.id` | Fetch 返回总记录数 |
| `stellflow.fetch.latency` | Histogram | ms | `node.id`, `result` | Fetch 请求延迟 |
| `stellflow.fetch.long_poll.hit.count` | Counter | hits | `node.id` | 长轮询命中次数 |
| `stellflow.fetch.long_poll.wait` | Histogram | ms | `node.id` | 长轮询等待时长 |

### 4.3 存储层指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.log.append.latency` | Histogram | ms | `node.id`, `replica.role` | 日志追加延迟 |
| `stellflow.log.flush.latency` | Histogram | ms | `node.id` | 刷盘延迟 |
| `stellflow.log.segment.roll.count` | Counter | events | `node.id` | 滚段次数 |
| `stellflow.log.segment.active.size` | Gauge | bytes | `node.id` | 活动段大小 |
| `stellflow.log.recovery.time` | Histogram | ms | `node.id` | 启动恢复耗时 |
| `stellflow.log.rebuild.index.count` | Counter | events | `node.id` | 索引重建次数 |
| `stellflow.log.cleanup.segment.count` | Counter | segments | `node.id` | 清理删除段数量 |
| `stellflow.log.start.offset` | Gauge | offset | `node.id` | 当前日志起始位点 |
| `stellflow.log.end.offset` | Gauge | offset | `node.id` | 当前日志结束位点 |

### 4.4 复制与高水位指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.replica.fetch.request.count` | Counter | requests | `node.id` | 副本同步请求数 |
| `stellflow.replica.fetch.latency` | Histogram | ms | `node.id` | 副本同步请求延迟 |
| `stellflow.replica.lag` | Gauge | bytes | `node.id`, `replica.role` | 副本滞后字节数 |
| `stellflow.replica.lag.time` | Gauge | ms | `node.id`, `replica.role` | 副本滞后时间 |
| `stellflow.isr.expand.count` | Counter | events | `node.id` | ISR 扩容次数 |
| `stellflow.isr.shrink.count` | Counter | events | `node.id` | ISR 缩容次数 |
| `stellflow.high_watermark.advance.count` | Counter | events | `node.id` | 高水位推进次数 |
| `stellflow.partition.leader.change.count` | Counter | events | `node.id` | 分区 Leader 变更次数 |

当前已落地的运行时副本抓取指标还包括一组更贴近 Prometheus 抓取端口的具体名称：

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.replica.fetch.requests` | Counter | requests | `topic`, `partition`, `leader.broker.id` | 后台 Replica Fetch 请求总数 |
| `stellflow.replica.fetch.failures` | Counter | requests | `topic`, `partition`, `leader.broker.id` | 后台 Replica Fetch 失败总数 |
| `stellflow.replica.fetch.bytes` | Counter | bytes | `topic`, `partition`, `leader.broker.id` | 后台 Replica Fetch 复制字节总数 |
| `stellflow.replica.fetch.entries` | Counter | entries | `topic`, `partition`, `leader.broker.id` | 后台 Replica Fetch 复制条目总数 |
| `stellflow.replica.fetch.lag` | Gauge | offsets | `topic`, `partition`, `leader.broker.id` | follower 相对 leader 高水位的当前滞后 |
| `stellflow.replica.fetch.last.success.timestamp` | Gauge | ms | `topic`, `partition`, `leader.broker.id` | 最近一次成功抓取时间 |

### 4.5 协调器与配额指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.coordinator.group.count` | Gauge | groups | `node.id`, `coordinator.type` | 当前管理的消费组数量 |
| `stellflow.group.rebalance.count` | Counter | events | `node.id` | 再均衡次数 |
| `stellflow.offset.commit.count` | Counter | requests | `node.id`, `result` | OffsetCommit 次数 |
| `stellflow.offset.commit.latency` | Histogram | ms | `node.id`, `result` | OffsetCommit 延迟 |
| `stellflow.quota.throttle.count` | Counter | events | `node.id`, `api` | 配额节流次数 |

## 5. Controller 指标字典

### 5.1 控制面与仲裁指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.controller.active` | Gauge | boolean | `node.id` | 当前节点是否为活动控制器 |
| `stellflow.controller.election.count` | Counter | events | `node.id` | 控制器选举次数 |
| `stellflow.controller.epoch` | Gauge | epoch | `node.id` | 当前控制器 Epoch |
| `stellflow.controller.command.count` | Counter | commands | `node.id`, `result` | 控制命令处理次数 |
| `stellflow.controller.command.latency` | Histogram | ms | `node.id`, `result` | 控制命令处理延迟 |
| `stellflow.metadata.log.append.latency` | Histogram | ms | `node.id` | 元数据日志追加延迟 |
| `stellflow.metadata.log.replay.latency` | Histogram | ms | `node.id` | 元数据日志回放延迟 |
| `stellflow.metadata.delta.broadcast.count` | Counter | events | `node.id` | 元数据增量广播次数 |

### 5.2 Broker 注册与分区状态指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.broker.registered.count` | Gauge | brokers | `node.id` | 已注册 Broker 数量 |
| `stellflow.broker.fenced.count` | Gauge | brokers | `node.id` | 被围栏 Broker 数量 |
| `stellflow.broker.heartbeat.timeout.count` | Counter | events | `node.id` | Broker 心跳超时次数 |
| `stellflow.partition.assignment.change.count` | Counter | events | `node.id` | 分区分配变化次数 |
| `stellflow.unclean.election.count` | Counter | events | `node.id` | 非清洁选举次数 |

## 6. Client 指标字典

### 6.1 Producer 指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.client.request.count` | Counter | requests | `client.type`, `api`, `result` | 客户端请求总数 |
| `stellflow.client.request.latency` | Histogram | ms | `client.type`, `api`, `result` | 客户端请求延迟 |
| `stellflow.client.metadata.refresh.count` | Counter | events | `client.type` | 元数据刷新次数 |
| `stellflow.client.metadata.refresh.latency` | Histogram | ms | `client.type` | 元数据刷新延迟 |
| `stellflow.producer.batch.size` | Histogram | bytes | `client.type` | Producer 批大小 |
| `stellflow.producer.batch.record.count` | Histogram | records | `client.type` | Producer 批记录数 |
| `stellflow.producer.send.bytes` | Counter | bytes | `client.type` | Producer 发送字节数 |
| `stellflow.producer.send.error.count` | Counter | errors | `client.type`, `result` | Producer 发送失败次数 |
| `stellflow.producer.retry.count` | Counter | retries | `client.type` | Producer 重试次数 |

### 6.2 Consumer 指标

| 指标名 | 类型 | 单位 | 推荐标签 | 说明 |
| --- | --- | --- | --- | --- |
| `stellflow.consumer.fetch.bytes` | Counter | bytes | `client.type` | Consumer 拉取字节数 |
| `stellflow.consumer.fetch.records` | Counter | records | `client.type` | Consumer 拉取记录数 |
| `stellflow.consumer.poll.latency` | Histogram | ms | `client.type` | Poll 延迟 |
| `stellflow.consumer.lag` | Gauge | records | `client.type` | 消费滞后记录数 |
| `stellflow.consumer.commit.count` | Counter | commits | `client.type`, `result` | 提交位点次数 |
| `stellflow.consumer.commit.latency` | Histogram | ms | `client.type`, `result` | 提交位点延迟 |
| `stellflow.consumer.rebalance.count` | Counter | events | `client.type` | 再均衡次数 |
| `stellflow.consumer.heartbeat.count` | Counter | heartbeats | `client.type`, `result` | 心跳次数 |

## 7. 核心告警建议基线

以下指标适合优先纳入告警体系：

| 指标名 | 告警方向 |
| --- | --- |
| `stellflow.request.latency` | 请求延迟持续升高 |
| `stellflow.request.decode.error.count` | 解码错误持续增加 |
| `stellflow.log.flush.latency` | 刷盘延迟异常 |
| `stellflow.replica.lag` | 副本滞后持续扩大 |
| `stellflow.isr.shrink.count` | ISR 频繁缩容 |
| `stellflow.controller.election.count` | 控制器频繁选举 |
| `stellflow.broker.heartbeat.timeout.count` | Broker 心跳超时增加 |
| `stellflow.consumer.lag` | 消费滞后显著堆积 |

## 8. 落地建议

### 8.1 埋点顺序

建议按以下优先级落埋点：

1. Broker 请求与存储核心指标
2. 复制、高水位与 ISR 指标
3. Controller 仲裁与元数据指标
4. Java Client 指标
5. Golang Client 指标

### 8.2 标签治理建议

- 默认仪表盘优先使用 `node.id`、`api`、`result`
- `topic`、`partition` 应只在诊断看板中局部展开
- 不同语言客户端必须使用同一批标签名，不得出现 Java 和 Go 各自命名

## 9. 后续待补充内容

以下内容建议在进入实际埋点阶段后继续补充：

1. 每个指标的采样示例代码
2. 仪表盘分组建议
3. 告警阈值建议
4. Trace 与 Metrics 的关联约定
5. Logs 与 Metrics 的语义映射

## 10. 结论

OpenTelemetry-first 的关键不只是“接入 OTel SDK”，而是先把指标字典统一下来。本文档的作用就是把 Broker、Controller、Client 的核心指标钉成统一表格，后续埋点、监控面板和跨语言 SDK 才能在同一语义基础上推进。
