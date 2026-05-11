# ADR-0007 OpenTelemetry 指标命名与标签规范

## 状态

Accepted

## 背景

`stellflow` 已明确采用 OpenTelemetry 作为统一可观测性标准，并且不再支持 JMX 作为主暴露方式。

在这种前提下，如果不尽早统一指标命名与标签规范，后续会出现以下问题：

- 指标名风格不一致
- 标签维度碎片化
- 不同模块重复表达同一语义
- 多语言客户端与服务端难以统一观测口径

因此需要为 Broker、Controller、客户端以及后续工具链定义统一的 OTel 指标命名和标签规范。

## 决策

`stellflow` 的 OpenTelemetry 指标命名与标签规范采用以下规则：

1. 指标命名统一使用小写蛇形或点分风格中的一种稳定风格，当前统一采用点分风格，前缀为 `stellflow.`。
2. 指标名应体现明确语义，不在名称中堆叠过多实现细节。
3. 指标标签只保留高价值、低基数维度，避免高基数标签进入核心指标。
4. Broker、Controller、客户端共享一套基础语义词汇，避免同义不同名。
5. 计数类、字节类、延迟类、队列类指标分别采用统一后缀约定。
6. 指标定义优先面向跨语言一致性，而不是只面向 Java 单实现。

## 备选方案

### 方案 A：各模块自行命名

优点：

- 初期开发最快

缺点：

- 长期不可维护
- 查询和告警难以统一
- 不同语言实现之间容易发生语义漂移

### 方案 B：沿用 Kafka/JMX 时代命名习惯

优点：

- 对熟悉 Kafka 的开发者更直观

缺点：

- 不适合作为 OTel-first 项目的长期标准
- 会遗留过多 JVM/JMX 时代的命名习惯

### 方案 C：统一定义 OTel 风格指标规范

优点：

- 统一服务端、客户端和工具链语义
- 更利于跨语言与平台化接入
- 更利于长期演进

缺点：

- 需要在项目早期投入额外设计精力

## 决策理由

最终选择方案 C，原因如下：

- 可观测性命名一旦散开，后期治理成本极高
- `stellflow` 既有服务端又有多语言客户端，必须尽早统一语义
- OTel-first 路线天然要求统一的观测契约，而不是临时拼接命名

## 决策后果

采用该规范后：

- 所有新指标都必须进入统一命名体系
- 监控看板、告警规则和采样策略会更容易复用
- 多语言客户端可直接共享指标词汇表

建议的基础命名示例如下：

- `stellflow.request.count`
- `stellflow.request.latency`
- `stellflow.produce.bytes`
- `stellflow.fetch.bytes`
- `stellflow.replica.lag`
- `stellflow.controller.election.count`
- `stellflow.log.flush.latency`

建议的常用标签示例如下：

- `node.id`
- `topic`
- `partition`
- `api`
- `client.type`
- `result`

不建议在核心全局指标中直接使用以下高基数标签：

- `client.id`
- `connection.id`
- `request.id`
- 原始 IP 地址

## 实现约束

- 所有新增指标必须遵循统一前缀和后缀约定
- 不允许在核心指标中引入高基数标签
- 服务端与客户端共享统一指标词汇表
- 后续需要补充正式指标字典文档，作为 OTel 埋点实现依据
