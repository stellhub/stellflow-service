# Stellflow 日志设计文档

## 1. 文档目标

本文档用于定义 `stellflow` Broker 自身运行日志的设计基线，重点覆盖：

- 日志职责边界
- 日志分层
- 日志级别
- MDC 规范
- 本地滚动文件策略

本文档只描述 `stellflow` 自身运行日志，不描述对外 OTel logs 接收协议。

## 2. 设计背景

`stellflow` 自身是 OTel 可观测体系的一部分，并且未来具备 OTel logs 接收能力。  
因此 Broker 自身运行日志不应依赖自身的 OTel logs 接收链路，否则会引入以下问题：

- 启动自举依赖
- 故障时的循环放大
- 排障路径与业务路径耦合
- 接收日志组件本身日志不可独立落盘

因此本项目当前采用：

- 日志 API：`SLF4J`
- 日志实现：`Log4j2`
- 输出方式：控制台 + 本地滚动文件

## 3. 总体原则

### 3.1 自身运行日志独立于 OTel logs

Broker 自身运行日志必须满足：

- 不依赖 OTel Collector
- 不依赖 `stellflow` 自身日志接收能力
- 网络异常时仍可本地落盘
- 数据面和控制面故障时仍可排障

### 3.2 结构化上下文优先

日志正文应简洁，关键上下文通过 MDC 提供，避免把所有信息拼成一整串文本。

### 3.3 高吞吐路径谨慎打日志

`stellflow` 是高吞吐 MQ，数据面热路径不应：

- 每条消息逐条打 `INFO`
- 打大块 payload
- 打完整 batch 内容

日志应更多聚焦：

- 生命周期
- 请求级关键事件
- 异常路径
- 状态切换

## 4. 日志分层

建议按模块分层：

| 层级 | 包路径 | 主要职责 |
| --- | --- | --- |
| 启动与生命周期层 | `io.github.stellhub.stellflow` | Broker 启停、配置摘要、关闭过程 |
| 网络传输层 | `io.github.stellhub.stellflow.network.transport` | 连接建立、断开、解码失败、回写失败 |
| 协议层 | `io.github.stellhub.stellflow.network.protocol` | 协议错误、编解码异常、版本不兼容 |
| API 分发层 | `io.github.stellhub.stellflow.server.api` | 请求入队、处理异常、未实现 API |
| 存储层 | `io.github.stellhub.stellflow.storage` | 段加载、恢复、刷盘、清理、校验 |
| 复制层 | `io.github.stellhub.stellflow.replica` | leader/follower 切换、ISR 变化、复制异常 |
| 控制器层 | `io.github.stellhub.stellflow.controller` | 元数据提交、Broker 注册、围栏、选主 |

## 5. 日志级别规范

### 5.1 `ERROR`

用于：

- 当前请求失败且属于异常路径
- 线程主循环出现非预期异常
- 存储、复制、控制面关键流程失败
- 需要运维立即关注的问题

不建议：

- 用于正常分区级拒绝
- 用于客户端可预期错误码

### 5.2 `WARN`

用于：

- 非法请求
- 版本不兼容
- 未注册 API
- 回写失败
- 单次连接异常
- 限流、配额或鉴权拒绝

### 5.3 `INFO`

用于：

- Broker 启动与关闭
- SocketServer 绑定端口
- 线程池启动与关闭
- 连接建立与断开
- 重要状态切换

### 5.4 `DEBUG`

用于：

- 请求入队与出队
- API 分发路径
- 调试阶段字段级摘要

默认不建议在生产环境长期开启。

### 5.5 `TRACE`

仅用于：

- 非常细粒度协议调试
- 临时问题定位

默认不建议启用。

## 6. 当前已落地的日志点

当前仓库已经落地以下日志点：

- Broker 启动与关闭
- `SocketServer` 绑定开始与成功
- 连接建立与断开
- 请求头解码失败
- 请求体解码失败
- 未注册 API
- 请求处理异常
- 响应回写失败
- `RequestDispatcher` 与 `ResponseResponder` 生命周期

## 7. MDC 规范

### 7.1 目标

MDC 用于把请求级上下文挂到日志上，便于本地 grep、平台检索和跨模块关联。

### 7.2 当前已使用字段

| MDC Key | 说明 |
| --- | --- |
| `correlationId` | 请求-响应关联 ID |
| `clientId` | 逻辑客户端标识 |
| `traceId` | 全局追踪 ID |
| `tenantId` | 多租户标识 |
| `trafficClass` | 流量治理等级 |
| `trafficTag` | 具体灰度/实验/回放分组 |
| `apiKey` | 请求 API 类型 |
| `apiVersion` | 请求版本 |
| `connectionId` | Netty 连接标识 |

### 7.3 后续建议补充字段

后续进入存储、复制与控制器实现后，可继续补充：

- `partition`
- `topic`
- `leaderEpoch`
- `producerId`
- `producerEpoch`
- `term`
- `brokerId`

### 7.4 MDC 使用原则

- 只放稳定、可检索的上下文
- 不把大字段或 payload 放进 MDC
- 高基数字段谨慎进入默认日志模式
- 处理线程完成请求后必须清理 MDC

## 8. 本地滚动文件策略

当前配置文件为：

- [log4j2.xml](../src/main/resources/log4j2.xml)

当前策略：

- 主日志文件：`logs/stellflow.log`
- 归档目录：`logs/archive/`
- 按天滚动
- 单文件达到 `128 MB` 时滚动
- 保留最多 `30` 个归档文件

## 9. 日志格式规范

当前日志格式包含：

- 时间
- 级别
- 线程名
- Logger 名称
- `correlationId`
- `traceId`
- `clientId`
- `tenantId`
- `trafficClass`
- `trafficTag`
- 日志正文

目标是让以下排障路径直接可用：

- 按 `correlationId` 检索一次请求
- 按 `traceId` 检索整条链路
- 按 `clientId` 检索某个客户端
- 按 `tenantId` 检索某个租户
- 按 `trafficTag` 检索某一组灰度/实验流量

## 10. 不建议记录的内容

以下内容默认不应直接进入常规日志：

- 完整消息 payload
- 大批量 `RecordBatch` 原文
- 用户敏感数据
- 认证凭据
- 高频每消息级 `INFO`

如果确实需要临时定位：

- 应通过 `DEBUG` 或 `TRACE`
- 应限定范围
- 应有明确的回收计划

## 11. 与 OTel 的关系

当前设计中：

- `metrics`：走 OpenTelemetry
- `traces`：走 OpenTelemetry
- Broker 自身运行日志：走本地文件
- `stellflow` 对外 logs 接收能力：作为产品能力，不作为自身主日志通路

一句话总结：

`stellflow` 自己可以消费 OTel logs，但不依赖自己来保存自己的运行日志。

## 12. 下一步建议

建议后续继续补充：

1. 存储层日志规范
2. 控制器与复制层日志规范
3. 审计日志单独文件策略
4. 故障注入与日志回放规范
