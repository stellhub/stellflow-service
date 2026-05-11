# Stellflow 压测计划

## 1. 文档目的

本文档用于定义 `stellflow` 数据面的标准压测计划，作为后续吞吐优化、回归验证与版本对比的统一基线。

本文档重点覆盖：

- 压测机器配置
- 网络配置
- 磁盘配置
- 消息大小矩阵
- 复制因子矩阵
- 压测场景与记录项

本文档与以下文档配套使用：

- [数据面高吞吐性能设计](./data-plane-performance-design.md)
- [协议规范文档](./protocol-spec.md)
- [OTel 指标字典](./otel-metrics-dictionary.md)

## 2. 压测目标

当前压测目标分为三层：

1. 验证单 Broker 数据面是否具备 `500 MB/s+` 的设计潜力
2. 验证复制因子、`acks`、消息大小、压缩与并发度对吞吐和延迟的影响
3. 建立可重复、可回归、可比较的 benchmark 体系

## 3. 压测原则

- 所有 benchmark 必须可重复执行
- 所有 benchmark 必须明确记录环境变量
- 同一轮对比只改变一个主变量
- 测试结果必须同时记录吞吐、延迟、CPU、GC、磁盘和网络指标
- benchmark 结果必须和 OTel 指标联动解释

## 4. 压测环境分级

建议定义三类环境：

### 4.1 开发级环境

用途：

- 功能可用性验证
- 本地调优冒烟

特点：

- 单机或双机
- 不追求最终吞吐结论

### 4.2 标准 benchmark 环境

用途：

- 性能对比
- 配置调优
- PR 回归

特点：

- 独立机器
- 稳定网络
- 固定磁盘规格

### 4.3 极限吞吐环境

用途：

- 追求单 Broker 或小集群吞吐上限
- 验证 `500 MB/s+` 目标

特点：

- 高性能 CPU
- 10GbE 或以上网络
- NVMe SSD

## 5. 压测机器配置

### 5.1 推荐标准 benchmark 机器

| 角色 | 建议配置 |
| --- | --- |
| Broker | 16-32 vCPU, 64-128 GB RAM, NVMe SSD, 10GbE |
| Producer 压测机 | 16-32 vCPU, 32-64 GB RAM, 10GbE |
| Consumer 压测机 | 16-32 vCPU, 32-64 GB RAM, 10GbE |
| Controller 节点 | 8-16 vCPU, 16-32 GB RAM, SSD, 10GbE |

### 5.2 推荐极限吞吐环境

| 角色 | 建议配置 |
| --- | --- |
| Broker | 32+ vCPU, 128 GB RAM, 高性能 NVMe, 25GbE |
| Producer 压测机 | 32+ vCPU, 64+ GB RAM, 25GbE |
| Consumer 压测机 | 32+ vCPU, 64+ GB RAM, 25GbE |

### 5.3 最低建议

如果只做第一轮性能基线验证，至少保证：

- Broker 与压测机分离
- 使用 SSD 而不是机械盘
- 网络至少 `10GbE`

## 6. 网络配置

### 6.1 推荐网络基线

| 级别 | 建议 |
| --- | --- |
| 最低可用 | `10GbE` |
| 推荐 | `25GbE` |
| 极限验证 | `40GbE+` |

### 6.2 网络要求

- Broker、Producer、Consumer 尽量位于同一低延迟内网
- 避免与其它高流量业务混跑
- 关闭不必要的中间代理或透明网关
- 测试时记录 MTU、实际带宽和丢包率

### 6.3 压测必须记录的网络项

- 网卡型号
- 链路速率
- MTU
- 是否 TLS
- 平均 RTT
- 峰值带宽利用率

## 7. 磁盘配置

### 7.1 推荐磁盘基线

| 场景 | 建议 |
| --- | --- |
| 标准 benchmark | 企业级 NVMe SSD |
| 极限吞吐 | 高性能 NVMe SSD，多队列 |
| 不建议 | 机械盘作为主测试介质 |

### 7.2 磁盘要求

- 单 Broker 数据盘与系统盘尽量分离
- 日志目录单独挂载
- benchmark 前确认磁盘无额外高负载
- 必须先测磁盘顺序写和顺序读上限

### 7.3 压测必须记录的磁盘项

- 盘型
- 顺序读带宽
- 顺序写带宽
- 随机读写能力
- 文件系统类型
- 挂载参数

## 8. 基线软件配置

### 8.1 Broker

压测前至少记录：

- JDK 版本
- JVM 参数
- Netty allocator 策略
- 日志段配置
- flush 配置
- 复制配置

### 8.2 操作系统

压测前至少记录：

- 操作系统版本
- 内核版本
- `ulimit`
- `somaxconn`
- send/recv buffer 上限
- page cache 相关参数

## 9. 消息大小矩阵

消息大小必须分层测试，不能只测一种大小。

### 9.1 推荐消息大小矩阵

| 编号 | 单条消息大小 | 用途 |
| --- | --- | --- |
| M1 | `256 B` | 小消息高频场景 |
| M2 | `1 KB` | 通用业务消息 |
| M3 | `4 KB` | 中小消息 |
| M4 | `16 KB` | 中等消息 |
| M5 | `64 KB` | 大消息 |
| M6 | `256 KB` | 超大消息 |
| M7 | `1 MB` | 极限批量与块传输场景 |

### 9.2 使用方式

- 每个消息大小至少测试 `acks=1` 与 `acks=all`
- 每个消息大小至少测试无复制与带复制
- 大消息场景必须重点关注 batch 与 socket buffer 的联动

## 10. Batch 大小矩阵

建议对以下 batch 大小做组合测试：

| 编号 | Batch 大小 |
| --- | --- |
| B1 | `256 KB` |
| B2 | `512 KB` |
| B3 | `1 MB` |
| B4 | `2 MB` |
| B5 | `4 MB` |
| B6 | `8 MB` |

说明：

- Producer 和 Replica Fetch 可以使用不同 batch 组合

## 11. 复制因子矩阵

### 11.1 推荐复制因子矩阵

| 编号 | 复制因子 | 用途 |
| --- | --- | --- |
| R1 | `1` | 无复制基线 |
| R2 | `2` | 轻复制场景 |
| R3 | `3` | 标准高可用场景 |

### 11.2 与 acks 联动测试

建议至少覆盖以下组合：

| 组合 | replication.factor | acks | 目的 |
| --- | --- | --- | --- |
| C1 | `1` | `1` | 理论吞吐上限基线 |
| C2 | `2` | `1` | 弱确认复制场景 |
| C3 | `2` | `all` | 双副本可靠性成本 |
| C4 | `3` | `1` | 标准复制弱确认 |
| C5 | `3` | `all` | 标准复制强确认 |

## 12. 并发度矩阵

### 12.1 Producer 并发

| 编号 | Producer 线程 / 实例数 |
| --- | --- |
| P1 | `1` |
| P2 | `4` |
| P3 | `8` |
| P4 | `16` |
| P5 | `32` |

### 12.2 Consumer 并发

| 编号 | Consumer 线程 / 实例数 |
| --- | --- |
| C1 | `1` |
| C2 | `4` |
| C3 | `8` |
| C4 | `16` |

### 12.3 Partition 数量矩阵

| 编号 | Partition 数量 |
| --- | --- |
| T1 | `1` |
| T2 | `8` |
| T3 | `16` |
| T4 | `32` |
| T5 | `64` |

## 13. Benchmark 场景计划

### 13.1 场景 A：内存到网络极限

目标：

- 测协议与 Netty 栈上限

配置建议：

- 不落盘
- 不复制
- 大 batch

### 13.2 场景 B：磁盘顺序写极限

目标：

- 测 Broker 存储层顺序写极限

配置建议：

- 单 Broker
- 不复制
- 仅 Produce

### 13.3 场景 C：磁盘到网络 Fetch 极限

目标：

- 测大块 Fetch 返回性能

配置建议：

- 预先写满数据
- 测 Consumer Fetch 或 Replica Fetch

### 13.4 场景 D：单 Broker 数据面基线

目标：

- 测单 Broker 真实 Produce / Fetch 能力

配置建议：

- `replication.factor = 1`
- `acks=1`

### 13.5 场景 E：复制成本验证

目标：

- 测复制因子和 `acks` 对吞吐影响

配置建议：

- 测 `replication.factor = 2/3`
- 测 `acks=1/all`

### 13.6 场景 F：端到端真实场景

目标：

- 测真实业务拓扑下的系统行为

配置建议：

- Producer + Broker + Replica + Consumer
- 开启 OTel 指标
- 开启必要限流和协调器

## 14. 每轮压测必须记录的数据

### 14.1 核心性能数据

- 吞吐量 `MB/s`
- `req/s`
- `records/s`
- P50 / P95 / P99 延迟

### 14.2 资源利用率

- Broker CPU
- Producer CPU
- Consumer CPU
- GC 时间
- 内存占用
- Direct memory 占用
- 网卡带宽利用率
- 磁盘带宽利用率

### 14.3 复制与一致性相关指标

- 副本滞后
- ISR 扩缩次数
- 高水位推进速度
- `acks=all` 成本变化

### 14.4 OTel 指标联动

每轮压测建议同步抓取：

- `stellflow.request.latency`
- `stellflow.produce.bytes`
- `stellflow.fetch.bytes`
- `stellflow.log.flush.latency`
- `stellflow.replica.lag`
- `stellflow.high_watermark.advance.count`
- `stellflow.request.queue.depth`

## 15. 压测结果记录模板

建议每轮压测至少记录如下字段：

| 字段 | 说明 |
| --- | --- |
| 测试编号 | 唯一 ID |
| 日期 | 测试时间 |
| Git 提交或版本 | 便于回归对比 |
| 场景编号 | A-F |
| Broker 数量 | 集群规模 |
| Partition 数量 | 并发分区数 |
| replication.factor | 复制因子 |
| acks | 应答级别 |
| 消息大小 | 单条消息大小 |
| batch 大小 | 批大小 |
| Producer 并发 | 线程 / 实例数 |
| Consumer 并发 | 线程 / 实例数 |
| 吞吐 | MB/s |
| 延迟 | P50/P95/P99 |
| CPU | 使用率 |
| 磁盘带宽 | 读写带宽 |
| 网络带宽 | 入站/出站 |
| 备注 | 特殊现象 |

## 16. 第一阶段正式压测建议顺序

建议先执行以下顺序：

1. 单 Broker、无复制、`1 KB` 消息、`1 MB` batch
2. 单 Broker、无复制、`4 KB` / `16 KB` / `64 KB` 消息对比
3. 单 Broker、多分区、并发 Producer 提升
4. 单 Broker、Fetch 极限测试
5. 三节点、`replication.factor=3`、`acks=1`
6. 三节点、`replication.factor=3`、`acks=all`
7. zero-copy 开关对比
8. Direct buffer 与 Heap buffer 对比

## 17. 结论

`stellflow` 的压测计划必须是“机器配置、网络、磁盘、消息大小、复制因子、并发度、指标记录”一起标准化，而不是只跑一个简单吞吐测试。只有 benchmark 体系先立住，后续数据面优化才有真正可比较、可回归的依据。
