# Stellflow ADR 索引

## 1. ADR 是什么

ADR 是 `Architecture Decision Record` 的缩写，中文通常译为“架构决策记录”。

它不是大而全的设计文档，而是针对某一个关键技术问题，记录以下内容的一页或几页短文档：

- 当时面临什么问题
- 评估过哪些可选方案
- 最终选择了什么
- 为什么这样选
- 这个选择会带来什么约束和后果

ADR 的重点不是“把所有知识写全”，而是把**关键决策及其理由**固定下来，避免团队在后续实现、重构和协作中反复摇摆。

## 2. 为什么当前仓库适合开始使用 ADR

`stellflow` 当前正处于设计基线收敛阶段，很多决定一旦做出，就会直接影响：

- 协议设计
- 多语言客户端实现
- 控制面接口
- 存储抽象
- 可观测性接入方式

这些问题都不是简单的编码细节，而是会长期影响整个项目结构的“架构级决定”。因此相比只写调研文档，更适合进一步沉淀为 ADR。

## 3. 当前 ADR 列表

- [ADR-0001 通信协议选型](./ADR-0001-communication-protocol.md)
- [ADR-0002 控制面一致性选型](./ADR-0002-control-plane-consensus.md)
- [ADR-0003 存储核心选型](./ADR-0003-storage-core.md)
- [ADR-0004 可观测性选型](./ADR-0004-observability.md)
- [ADR-0005 多语言客户端协议版本策略](./ADR-0005-multi-language-protocol-versioning.md)
- [ADR-0006 元数据发现与 Broker 扩容策略](./ADR-0006-metadata-discovery-and-broker-scaling.md)
- [ADR-0007 OpenTelemetry 指标命名与标签规范](./ADR-0007-opentelemetry-metrics-naming.md)

## 4. ADR 编写约定

当前仓库建议使用如下结构：

- `状态`：Proposed / Accepted / Superseded / Deprecated
- `背景`
- `决策`
- `备选方案`
- `决策后果`
- `实现约束`

随着项目推进，后续若有新方案替代已有 ADR，不删除旧 ADR，而是将旧 ADR 状态更新为 `Superseded`，并在新 ADR 中说明替代关系。
