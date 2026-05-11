# Stellflow 通信与基础设施选型调研

## 1. 文档目的

本文档用于整理 `stellflow` 在通信协议、控制面一致性、存储核心和可观测性方面的技术选型结论，并明确这些选型与 Apache Kafka 原生实现之间的差异。

本项目总体目标仍然是：

- 核心领域模型向 Kafka 对齐
- 核心工程思想向 Kafka 对齐
- 使用 Java / JDK 25 重建实现

但在基础设施层面，`stellflow` 不追求对 Kafka 原始实现路径的逐项复制，而是明确采用更适合当前工程目标的替代方案。

## 2. 调研范围

本次调研覆盖以下问题：

- Broker 与 Client 的通信协议应如何设计
- Broker 间数据复制应如何复用通信协议
- Controller 与 Broker 的控制面通信是否使用 gRPC
- Controller Quorum 的一致性协议如何实现
- 消息存储核心是否引入 gRPC 或第三方 KV
- 可观测性是否沿用 Kafka 的 JMX-first 路线

## 3. 背景与核心判断

### 3.1 Kafka 原生路径的特点

Kafka 原生实现通常具有以下特点：

- Broker / Client 通信使用自定义二进制协议
- Broker 间复制复用同一套协议模型
- Controller Quorum 使用 KRaft 路线
- 消息存储核心使用日志段与索引，而不是通用 KV
- 可观测性原生以 JMX 为主

这些设计在系统语义、吞吐路径和运行模型上非常成熟，但对于一个纯 Java 重建项目而言，部分基础设施路径可以适度替换，以降低工程复杂度或改善现代化集成体验。

### 3.2 选型原则

本项目最终采用以下判断标准：

- 热路径优先保留 Kafka 风格
- 控制面优先选择更高开发效率的现代基础设施
- 消息存储核心不做通用化抽象，不引入不必要的第三方引擎
- 可观测性采用现代标准，避免继续绑定 JVM 专属暴露方式

## 4. 方案对比

### 4.1 Broker / Client 通信

候选方案主要包括：

- 自定义二进制协议 + Java NIO / Netty
- 全量 gRPC
- HTTP/REST

结论：

- HTTP/REST 不适合作为消息队列核心数据面协议
- 全量 gRPC 虽然更利于跨语言生成代码，但会让数据面更像 RPC 系统，不利于保持 Kafka 风格的批量拉取、长轮询和高吞吐二进制请求响应模型
- 自定义二进制协议更适合 `Produce / Fetch / Metadata` 这类核心路径

最终决定：

- Broker / Client 通信继续采用自定义二进制协议
- Java 客户端底层使用 Netty
- Golang 客户端底层自研实现

### 4.2 Broker 间数据复制

候选方案主要包括：

- 复用 Broker / Client 二进制协议
- 单独定义复制协议
- 使用 gRPC streaming

结论：

- 复制链路是数据面热路径，应尽量复用核心协议语义
- 单独定义复制协议会导致编解码、限流、调试和兼容性成本上升
- gRPC streaming 虽然可实现，但并不天然适配 Kafka 式复制语义

最终决定：

- Broker 间数据复制复用同一套二进制协议
- 底层统一使用 Netty

### 4.3 Controller / Broker 控制面通信

候选方案主要包括：

- 自定义二进制协议
- gRPC
- HTTP/REST

结论：

- 控制面消息频率低于数据面，更关注接口清晰度、可维护性和演进便利性
- gRPC 更适合表达 Broker 注册、心跳、元数据增量下发、配置变更等控制命令

最终决定：

- Controller / Broker 控制面通信使用 gRPC

### 4.4 Controller Quorum 一致性

候选方案主要包括：

- 自研 Raft
- Apache Raft 实现
- 非 Raft 路线

结论：

- 自研 Raft 成本高、验证周期长
- 既然 `stellflow` 已经明确采用 Controller Quorum 设计，就应当优先复用成熟的 Apache Raft 体系能力

最终决定：

- Controller Quorum 一致性使用 Apache 的 Raft

### 4.5 消息存储核心

候选方案主要包括：

- 自研日志段 + 稀疏索引
- 基于 gRPC 的远程存储抽象
- 引入 RocksDB / LevelDB 等第三方 KV

结论：

- Kafka 风格消息队列的核心不是 KV，而是顺序追加日志
- 如果把主消息路径改成通用 KV，引擎语义、读写模型和性能优化方向都会改变
- gRPC 不应进入消息存储核心路径

最终决定：

- 消息存储核心自研日志段
- 不走 gRPC
- 不走 KV

### 4.6 可观测性

候选方案主要包括：

- 延续 Kafka 的 JMX-first
- JMX + Prometheus 转换
- OpenTelemetry-first

结论：

- Kafka 原生是 JMX-first，但这更偏向 JVM 时代的默认方案
- 对多语言生态和现代可观测平台而言，OpenTelemetry 更统一
- 新项目没有必要继续以 JMX 作为一等标准接口

最终决定：

- 可观测性统一采用 OpenTelemetry 标准
- 不再支持 JMX

## 5. 最终技术决策

本项目当前确认的技术路线如下：

1. Broker / Client 通信：自定义二进制协议，Java 客户端底层 Netty，Golang 客户端底层自研
2. Broker 间数据复制：复用同一套二进制协议，底层使用 Netty
3. Controller / Broker 控制面通信：使用 gRPC
4. Controller Quorum 一致性：使用 Apache 的 Raft
5. 消息存储核心：自研日志段，不走 gRPC，不走 KV
6. 可观测性：使用 OpenTelemetry 标准，不再支持 JMX

## 6. 与 Kafka 的关键差异

`stellflow` 与 Kafka 的主要差异不在领域模型，而在基础设施落地：

- Kafka 原生网络栈更偏向自管 NIO；`stellflow` 的 Java 通信底层选择 Netty
- Kafka 控制面是 KRaft；`stellflow` 明确落地为 Apache Raft 实现
- Kafka 原生可观测性以 JMX 为主；`stellflow` 直接转向 OpenTelemetry
- Kafka 原生 Java 客户端最完整；`stellflow` 在客户端层面提前明确 Java 与 Golang 两套不同底层实现路径

## 7. 对后续实现的约束

后续编码必须遵守以下边界：

- 不将 gRPC 引入 Broker / Client 数据面主链路
- 不为消息存储核心引入第三方 KV 依赖
- 不新增 JMX 暴露作为主指标接口
- Controller / Broker 的控制命令优先围绕 gRPC IDL 设计
- 二进制协议设计必须独立于 Netty，保证 Golang 等多语言客户端可实现

## 8. 结论

`stellflow` 的最终路线不是“逐行翻译 Kafka”，而是“保持 Kafka 核心语义与工程思想，同时在基础设施层做受控替换”。这份文档定义了当前最重要的替换边界，并作为后续网络层、控制层、存储层和可观测性实现的统一依据。

## 9. 对应 ADR

为避免后续实现阶段对关键技术决策反复摇摆，本文档中的结论已经进一步拆分为正式 ADR：

- [ADR-0001 通信协议选型](./adr/ADR-0001-communication-protocol.md)
- [ADR-0002 控制面一致性选型](./adr/ADR-0002-control-plane-consensus.md)
- [ADR-0003 存储核心选型](./adr/ADR-0003-storage-core.md)
- [ADR-0004 可观测性选型](./adr/ADR-0004-observability.md)
