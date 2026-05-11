# Stellflow 协议兼容性测试计划

## 1. 文档目的

本文档用于定义 `stellflow` 在以下实现组合之间的跨语言协议兼容性测试矩阵：

- Java Broker
- Java Client
- Go Client

本文档目标是：

- 建立统一的协议兼容性验证基线
- 提前发现字段顺序、编码方式、错误码和版本协商的不一致
- 为后续版本演进建立回归测试体系

## 2. 测试范围

当前协议兼容性测试覆盖以下层面：

1. 连接与基础握手
2. `ApiVersions` 能力协商
3. `Metadata` 查询
4. `Produce`
5. `Fetch`
6. 错误码与异常路径
7. 长轮询与空返回
8. 大 batch 与边界值
9. 幂等写入与事务可见性

## 3. 测试对象矩阵

### 3.1 当前实现角色

| 角色 | 实现语言 | 说明 |
| --- | --- | --- |
| Broker | Java | 协议服务端实现 |
| Client | Java | Java SDK |
| Client | Go | Golang SDK |

### 3.2 基础兼容矩阵

| 编号 | Broker | Client | 目标 |
| --- | --- | --- | --- |
| C1 | Java Broker | Java Client | 语言内基线验证 |
| C2 | Java Broker | Go Client | 跨语言兼容验证 |

说明：

- 当前阶段因为 Broker 只有 Java 实现，因此以 Java Broker 为中心做客户端兼容性验证
- 后续若出现 Go Broker 或 mock Broker，可扩展更多组合

## 4. 测试分层

### 4.1 层 1：编码与解码单元测试

目标：

- 验证单个对象的编码与解码一致性

对象范围：

- Request Header
- Response Header
- `RecordBatch`
- `ProduceRequestBody`
- `ProduceResponseBody`
- `FetchRequestBody`
- `FetchResponseBody`

### 4.2 层 2：Golden File 测试

目标：

- 固定一组标准样例报文
- Java 和 Go 都必须能对同一组样例完成一致编码/解码

建议来源：

- [消息格式样例报文](./message-format-examples.md)

### 4.3 层 3：端到端协议测试

目标：

- 验证真实 Broker 与 Client 间的协议互通

### 4.4 层 4：版本兼容回归测试

目标：

- 验证协议升级后旧客户端仍可工作
- 验证 `ApiVersions` 协商逻辑正确

## 5. 基础协议对象兼容测试矩阵

### 5.1 Header 兼容

| 测试项 | Java 编码 / Java 解码 | Java 编码 / Go 解码 | Go 编码 / Java 解码 |
| --- | --- | --- | --- |
| Request Header | 必测 | 必测 | 必测 |
| Response Header | 必测 | 必测 | 必测 |

### 5.2 Body 兼容

| 测试对象 | Java/Java | Java/Go | Go/Java |
| --- | --- | --- | --- |
| `ApiVersionsRequest/Response` | 必测 | 必测 | 必测 |
| `MetadataRequest/Response` | 必测 | 必测 | 必测 |
| `ProduceRequestBody` | 必测 | 必测 | 必测 |
| `ProduceResponseBody` | 必测 | 必测 | 必测 |
| `FetchRequestBody` | 必测 | 必测 | 必测 |
| `FetchResponseBody` | 必测 | 必测 | 必测 |
| `RecordBatch` | 必测 | 必测 | 必测 |

## 6. 端到端协议测试矩阵

### 6.1 能力协商

| 编号 | Broker | Client | 测试内容 |
| --- | --- | --- | --- |
| E1 | Java Broker | Java Client | `ApiVersions` 成功协商 |
| E2 | Java Broker | Go Client | `ApiVersions` 成功协商 |
| E3 | Java Broker | Java Client | 请求不支持版本时返回 `UNSUPPORTED_VERSION` |
| E4 | Java Broker | Go Client | 请求不支持版本时返回 `UNSUPPORTED_VERSION` |

### 6.2 Metadata

| 编号 | Broker | Client | 测试内容 |
| --- | --- | --- | --- |
| M1 | Java Broker | Java Client | 查询 Broker / Topic / Partition 元数据 |
| M2 | Java Broker | Go Client | 查询 Broker / Topic / Partition 元数据 |
| M3 | Java Broker | Java Client | 不存在 Topic 返回一致错误 |
| M4 | Java Broker | Go Client | 不存在 Topic 返回一致错误 |

### 6.3 Produce

| 编号 | Broker | Client | 测试内容 |
| --- | --- | --- | --- |
| P1 | Java Broker | Java Client | 单分区 Produce 成功 |
| P2 | Java Broker | Go Client | 单分区 Produce 成功 |
| P3 | Java Broker | Java Client | 多分区 Produce 成功 |
| P4 | Java Broker | Go Client | 多分区 Produce 成功 |
| P5 | Java Broker | Java Client | 局部分区失败，分区级错误码正确 |
| P6 | Java Broker | Go Client | 局部分区失败，分区级错误码正确 |
| P7 | Java Broker | Java Client | 超大消息触发 `MESSAGE_TOO_LARGE` |
| P8 | Java Broker | Go Client | 超大消息触发 `MESSAGE_TOO_LARGE` |

### 6.4 Fetch

| 编号 | Broker | Client | 测试内容 |
| --- | --- | --- | --- |
| F1 | Java Broker | Java Client | 普通 Consumer Fetch 成功 |
| F2 | Java Broker | Go Client | 普通 Consumer Fetch 成功 |
| F3 | Java Broker | Java Client | 空返回与长轮询行为正确 |
| F4 | Java Broker | Go Client | 空返回与长轮询行为正确 |
| F5 | Java Broker | Java Client | 多分区 Fetch 成功 |
| F6 | Java Broker | Go Client | 多分区 Fetch 成功 |
| F7 | Java Broker | Java Client | `OFFSET_OUT_OF_RANGE` 行为正确 |
| F8 | Java Broker | Go Client | `OFFSET_OUT_OF_RANGE` 行为正确 |

### 6.5 Replica Fetch

| 编号 | Broker | Client/Replica | 测试内容 |
| --- | --- | --- | --- |
| R1 | Java Broker | Java Replica Client | Replica Fetch 成功 |
| R2 | Java Broker | Go Replica Client | Replica Fetch 成功 |
| R3 | Java Broker | Java Replica Client | Leader epoch 不一致时返回一致行为 |
| R4 | Java Broker | Go Replica Client | Leader epoch 不一致时返回一致行为 |

### 6.6 幂等与事务

| 编号 | Broker | Client | 测试内容 |
| --- | --- | --- | --- |
| X1 | Java Broker | Java Client | 幂等 Produce 成功，`producerId / producerEpoch / baseSequence` 生效 |
| X2 | Java Broker | Go Client | 幂等 Produce 成功，`producerId / producerEpoch / baseSequence` 生效 |
| X3 | Java Broker | Java Client | 重复序列写入返回一致的重复写语义 |
| X4 | Java Broker | Go Client | 序列乱序时返回 `OUT_OF_ORDER_SEQUENCE_NUMBER` |
| X5 | Java Broker | Java Client | 事务 Produce 成功，`transactionalId` 生效 |
| X6 | Java Broker | Go Client | `read_committed` Fetch 返回一致的 `lastStableOffset` 与 `abortedTransactions` |

## 7. Golden File 测试计划

### 7.1 目标

使用一组稳定样例报文，验证不同语言实现是否遵守同一格式。

### 7.2 样例来源

建议基于以下文档中的样例生成 golden files：

- [消息格式样例报文](./message-format-examples.md)

### 7.3 建议的 golden 样例集合

| 样例编号 | 对象 |
| --- | --- |
| G1 | `RecordBatch` 单批两条记录 |
| G2 | `ProduceRequest` 单 topic 单分区 |
| G3 | `ProduceResponse` 成功返回 |
| G4 | `ProduceResponse` 局部失败返回 |
| G5 | `FetchRequest` 普通 Consumer |
| G6 | `FetchRequest` Replica Fetch |
| G7 | `FetchResponse` 返回一批数据 |
| G8 | `FetchResponse` 空返回 |
| G9 | 幂等 `ProduceRequest` |
| G10 | 事务 `FetchResponse` |

### 7.4 校验要求

对每个 golden 样例都要验证：

- Java 编码结果是否匹配 golden
- Go 编码结果是否匹配 golden
- Java 是否能解码 golden
- Go 是否能解码 golden

## 8. 字段级重点关注项

跨语言最容易出问题的点必须重点验证：

### 8.1 基础编码

- 大端序是否一致
- `nullable string` 与 `nullable bytes` 的 `-1` 语义是否一致
- `array length` 是否一致

### 8.2 Header

- `apiKey`
- `apiVersion`
- `headerVersion`
- `correlationId`
- `clientId`
- `traceId`
- `spanId`
- `traceFlags`
- `tenantId`
- `quotaKey`
- `authContextId`
- `trafficClass`
- `trafficTag`
- `flags`

### 8.3 Produce

- `acks`
- `transactionalId`
- `timeoutMs`
- topic / partition 聚合顺序
- `records` 原始字节是否保持一致
- `producerId / producerEpoch / baseSequence`

### 8.4 Fetch

- `replicaId`
- `maxWaitMs`
- `minBytes`
- `maxBytes`
- `fetchOffset`
- `partitionMaxBytes`
- `highWatermark`
- `lastStableOffset`
- `abortedTransactions`

### 8.5 RecordBatch

- `crc32c`
- `attributes` 位图
- `timestampDelta`
- `offsetDelta`
- header 数组顺序

## 9. 错误路径兼容测试

以下错误路径必须做跨语言一致性验证：

| 错误码 | 场景 |
| --- | --- |
| `UNSUPPORTED_VERSION` | 请求版本超出 Broker 支持范围 |
| `INVALID_REQUEST` | 请求体字段非法 |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic 或 Partition 不存在 |
| `NOT_LEADER_OR_FOLLOWER` | 请求发送到错误 Broker |
| `MESSAGE_TOO_LARGE` | 消息过大 |
| `OFFSET_OUT_OF_RANGE` | Fetch 位移越界 |
| `AUTHORIZATION_FAILED` | 无权限访问 |
| `INVALID_PRODUCER_EPOCH` | producer epoch 非法 |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` | 幂等写入乱序 |
| `DUPLICATE_SEQUENCE_NUMBER` | 幂等写入重复 |
| `INVALID_TXN_STATE` | 事务状态非法 |
| `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | 事务 ID 无权限 |
| `PRODUCER_FENCED` | producer 被围栏 |

## 10. 性能相关兼容测试

协议兼容不仅是“能不能解析”，也要确保在高吞吐场景下行为一致。

建议加入以下兼容场景：

| 编号 | 测试内容 |
| --- | --- |
| T1 | `1 KB` 消息、`1 MB` batch 的 Produce / Fetch |
| T2 | `64 KB` 消息、`4 MB` batch 的 Produce / Fetch |
| T3 | 多分区并发 Produce |
| T4 | 长轮询 Fetch |
| T5 | 大批量 Replica Fetch |

目标：

- 验证 Java Client 与 Go Client 在高吞吐数据面下不会因编码差异而行为不同

## 11. 版本兼容回归计划

### 11.1 当前阶段

当前阶段以 `apiVersion = 0` 为主，但测试框架必须从一开始就支持：

- Java Client 请求旧版本
- Go Client 请求旧版本
- Broker 根据 `ApiVersions` 结果协商版本

### 11.2 后续要求

当 `apiVersion = 1` 出现后，必须补以下回归：

- `Broker(v1)` + `Java Client(v0)`
- `Broker(v1)` + `Go Client(v0)`
- `Broker(v1)` + `Java Client(v1)`
- `Broker(v1)` + `Go Client(v1)`

## 12. 推荐执行顺序

建议按以下顺序建立兼容测试体系：

1. Header 编解码单测
2. `RecordBatch` 编解码单测
3. Golden file 测试
4. Java Broker + Java Client 端到端
5. Java Broker + Go Client 端到端
6. 错误路径测试
7. 性能场景兼容测试
8. 版本兼容回归测试

## 13. 结果记录模板

每轮兼容性测试建议至少记录：

| 字段 | 说明 |
| --- | --- |
| 测试编号 | 唯一 ID |
| Broker 版本 | Java Broker 版本 |
| Client 类型 | Java / Go |
| API | Produce / Fetch / Metadata / ApiVersions |
| 版本 | apiVersion |
| 样例编号 | 对应 golden 或场景编号 |
| 结果 | pass / fail |
| 错误详情 | 若失败则记录差异 |
| 备注 | 特殊现象 |

## 14. 结论

`stellflow` 的跨语言协议兼容测试，不能只停留在“Java 自己能通”。必须把 Java Broker、Java Client、Go Client 放到统一矩阵中验证字段顺序、编码规则、错误码和版本协商逻辑。只有这套测试矩阵建立起来，自定义二进制协议才真正具备长期可维护的多语言实现基础。
