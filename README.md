# Stellflow

Stellflow 是一个基于 JDK 25 实现的分布式消息队列项目，整体领域模型、运行角色和核心工程思想以 Apache Kafka 最新主线架构为参考，但**并不是对 Kafka 原始实现路径的逐项复刻**。

它的目标是：

- 保持 Kafka 风格的 Topic / Partition / Replica / ISR / Offset / Controller Quorum 体系
- 保持 Kafka 风格的高吞吐日志存储与分区复制思想
- 使用纯 Java 重新构建实现
- 在基础设施层面采用与 Kafka 不同、但更符合当前项目目标的技术路线

## 一眼看懂：Stellflow 和 Kafka 的不同

当前仓库与 Apache Kafka 的关键差异如下：

| 维度 | Kafka 原生路线 | Stellflow 当前路线 |
| --- | --- | --- |
| Broker / Client 通信 | 自定义二进制协议，Java 原生网络栈风格 | 自定义二进制协议，Java 客户端底层 Netty，Golang 客户端底层自研 |
| Broker 间数据复制 | 复用核心二进制协议 | 复用同一套二进制协议，底层统一 Netty |
| Controller / Broker 控制面通信 | Kafka 自身协议体系 | 使用 gRPC |
| Controller Quorum 一致性 | KRaft | 使用 Apache 的 Raft |
| 消息存储核心 | 自研日志段 | 自研日志段，不走 gRPC，不走 KV |
| 可观测性 | JMX-first | OpenTelemetry-first，不再支持 JMX |

如果只看一句话，可以这样理解：

> Stellflow 保留 Kafka 的核心语义和分布式日志思想，但在通信、控制面一致性实现和可观测性标准上，明确走与 Kafka 不同的现代化路线。

## 当前已确认的技术决策

1. Broker / Client 通信：自定义二进制协议，Java 客户端底层 Netty，Golang 客户端底层自研。
2. Broker 间数据复制：复用同一套二进制协议，底层使用 Netty。
3. Controller / Broker 控制面通信：使用 gRPC。
4. Controller Quorum 一致性：使用 Apache 的 Raft。
5. 消息存储核心：自研日志段，不走 gRPC，不走 KV。
6. 可观测性：使用 OpenTelemetry 标准，不再支持 JMX。
7. 配置与文档层正式使用 `stellflow://` 表达数据面 endpoint，但线上 TCP 二进制协议本身不携带文本前缀。

## 设计文档

当前仓库已经沉淀的设计文档如下：

- [概要设计](./docs/overview-design.md)
- [存储层详细设计](./docs/storage-detailed-design.md)
- [Broker 请求处理链路设计](./docs/broker-request-pipeline-design.md)
- [Controller 与 Replica 详细设计](./docs/controller-replica-detailed-design.md)
- [通信与基础设施选型调研](./docs/communication-research.md)
- [高性能协议选型与吞吐设计对照](./docs/high-performance-protocol-comparison.md)
- [数据面高吞吐性能设计](./docs/data-plane-performance-design.md)
- [压测计划](./docs/benchmark-plan.md)
- [Netty 数据面实现指南](./docs/netty-data-plane-implementation-guide.md)
- [协议规范文档](./docs/protocol-spec.md)
- [ApiVersions / Metadata 消息格式规范](./docs/api-versions-and-metadata-format.md)
- [Produce / Fetch 消息格式规范](./docs/produce-fetch-message-format.md)
- [ListOffsets 接口说明](./docs/list-offsets-interface.md)
- [FetchRequestBody 消息格式规范](./docs/fetch-request-format.md)
- [ProduceResponseBody 消息格式规范](./docs/produce-response-format.md)
- [消息格式样例报文](./docs/message-format-examples.md)
- [协议兼容性测试计划](./docs/protocol-compatibility-test-plan.md)
- [Replica Fetch 运行设计](./docs/replica-fetch-runtime-design.md)
- [OTel 指标字典](./docs/otel-metrics-dictionary.md)
- [ADR 索引](./docs/adr/README.md)

## 项目定位

Stellflow 面向以下典型场景：

- 异步解耦
- 事件分发
- 流式传输
- 高吞吐日志聚合
- 削峰填谷与任务缓冲

## 当前状态

当前仓库仍处于设计与骨架规划阶段，重点工作在于：

- 固化总体架构与模块边界
- 明确通信、存储、控制面和可观测性技术路线
- 为后续代码骨架实现提供统一设计基线
