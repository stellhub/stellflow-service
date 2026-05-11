# Stellflow 协议规范文档

## 1. 文档目的

本文档定义 `stellflow` 数据面自定义二进制协议的第一版规范基线，重点覆盖：

- 协议分层
- 请求头与响应头
- `apiKey` 与 `apiVersion`
- 错误码体系
- 能力协商机制
- 兼容性原则

本文档是多语言客户端、Broker 数据面与复制链路的共同契约。它必须独立于 Java、Netty、Golang 等具体实现存在。

## 2. 适用范围

本协议规范适用于以下链路：

- Broker / Client 数据面通信
- Broker 间数据复制通信

本协议**不适用于**：

- Controller / Broker 控制面 gRPC 接口

## 3. 协议设计目标

- 支持多语言客户端实现
- 支持显式版本管理
- 支持能力协商
- 支持批量请求与批量响应
- 支持清晰、稳定的错误码映射
- 支持后续滚动升级与灰度兼容

## 4. 协议分层

`stellflow` 数据面协议采用如下分层：

1. 传输层：TCP
2. Java 实现层：Netty
3. 协议层：自定义二进制请求-响应协议
4. 语义层：Produce / Fetch / Metadata / ApiVersions 等 API

说明：

- Netty 只是 Java 侧的 I/O 承载框架，不属于协议的一部分。
- Golang 或其它语言客户端必须基于同一协议规范独立实现编解码。

## 5. 连接与会话模型

### 5.1 连接模型

- 客户端与 Broker 使用长连接
- 一个连接上允许串行或有限并发发送多个请求
- 响应通过 `correlationId` 与请求对应

### 5.2 会话模型

- 协议本身不要求强会话状态
- 连接级上下文可包含认证结果、节流状态和能力缓存
- 断开连接后，客户端应支持重连与元数据刷新

## 6. 帧格式

### 6.1 总体格式

每个请求帧和响应帧都采用“长度前缀 + 头部 + 体”的格式：

```text
+----------------+----------------+----------------+
| frameLength    | header         | body           |
+----------------+----------------+----------------+
```

### 6.2 字段编码约定

建议采用以下通用编码规则：

- `int8`：1 字节有符号整数
- `int16`：2 字节有符号整数，大端序
- `int32`：4 字节有符号整数，大端序
- `int64`：8 字节有符号整数，大端序
- `bool`：1 字节，`0` 或 `1`
- `string`：`int16 length + UTF-8 bytes`
- `nullable string`：`int16 length`，`-1` 表示 `null`
- `bytes`：`int32 length + raw bytes`
- `nullable bytes`：`int32 length`，`-1` 表示 `null`
- `array<T>`：`int32 length + repeated entries`

说明：

- 第一版协议统一使用大端序，避免跨语言歧义。
- 后续如引入紧凑编码，应通过新版本显式区分，不得直接修改旧版本语义。

### 6.3 大端序说明

本文档中的“大端序”指：

- 高位字节放前面
- 低位字节放后面

也就是说，一个多字节整数在网络上传输时，先写入最高有效字节，再写入最低有效字节。  
这种写法也常被称为 `network byte order`。

示例 1：`int16 = 258`

```text
十六进制值：0x0102
大端序字节流：01 02
```

示例 2：`int32 = 16909060`

```text
十六进制值：0x01020304
大端序字节流：01 02 03 04
```

如果某个客户端错误地按小端序解码同一个 `int32`，就会把：

```text
01 02 03 04
```

误读成：

```text
04 03 02 01
```

最终得到完全错误的数值。

因此，`stellflow` 的 Java Broker、Java Client、Golang Client 在处理 `int16 / int32 / int64` 时，都必须严格按大端序编解码。

## 7. 请求头规范

### 7.1 请求头字段

每个请求必须携带统一请求头：

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `apiKey` | `int16` | 是 | API 标识 |
| `apiVersion` | `int16` | 是 | 请求使用的协议版本 |
| `headerVersion` | `int16` | 是 | 请求头版本，当前基线为 `2` |
| `correlationId` | `int32` | 是 | 请求-响应关联 ID |
| `clientId` | `nullable string` | 否 | 客户端逻辑标识 |
| `traceId` | `nullable string` | 否 | W3C Trace Context 风格的 trace id，建议使用 32 位十六进制字符串 |
| `spanId` | `nullable string` | 否 | 当前请求 span id，建议使用 16 位十六进制字符串 |
| `traceFlags` | `int8` | 是 | Trace 采样与扩展标记，建议兼容 W3C trace-flags |
| `tenantId` | `nullable string` | 否 | 多租户或业务域标识 |
| `quotaKey` | `nullable string` | 否 | 配额与限流归属键 |
| `authContextId` | `nullable string` | 否 | 鉴权上下文标识，例如代理透传、委托身份或认证上下文句柄 |
| `trafficClass` | `int8` | 是 | 流量染色与治理等级 |
| `trafficTag` | `nullable string` | 否 | 具体染色批次、实验组、回放组或诊断流量标签 |
| `flags` | `int16` | 是 | 通用扩展位图，用于压缩、内部请求、调试等轻量标记 |

### 7.2 请求头语义

- `apiKey` 决定请求语义类别
- `apiVersion` 决定同一个 `apiKey` 的字段布局和行为语义
- `headerVersion` 决定请求头自身的字段布局和扩展能力
- `correlationId` 必须由客户端生成，并在对应响应中原样返回
- `clientId` 用于限流、审计与观测，不得作为高基数全局指标标签默认暴露
- `traceId / spanId / traceFlags` 用于跨语言链路追踪、请求染色与问题定位
- `tenantId / quotaKey / authContextId / trafficClass / trafficTag` 用于多租户治理、限流、鉴权上下文透传、流量分级与具体实验标识
- `flags` 用于承载轻量级横切语义，未知标记位默认拒绝或忽略，取决于具体版本约定

### 7.3 请求头字段设计目的

虽然请求头字段数量不多，但每个字段都在解决不同的问题，职责不能互相替代。

#### `apiKey`

设计目的：

- 标识请求类别
- 让 Broker 在读取完请求头后，就能决定该使用哪条业务处理链路
- 支持同一监听端口复用多种 API

意义：

- `ApiVersions`、`Metadata`、`Produce`、`Fetch` 等请求体结构完全不同
- Broker 必须先知道“这是什么请求”，才能选择正确的 body 解码器和 handler

如果缺少：

- Broker 无法判断请求体应该按哪种格式解释
- 同一段字节在不同 API 下会有完全不同的含义
- 协议只能退化成“一种连接只支持一种请求类型”的低灵活度模型

#### `apiVersion`

设计目的：

- 标识当前请求使用的协议版本
- 支持同一个 `apiKey` 在未来增加字段、调整行为而不破坏旧客户端
- 支持滚动升级和多版本客户端并存

意义：

- 同一个 `Metadata` 或 `Fetch` 请求，在不同版本下可能有不同字段布局
- `apiVersion` 使 Broker 和 Client 能显式约定“按哪一版协议解释”

如果缺少：

- 协议一旦升级，新老客户端与 Broker 就容易发生字段错位
- 客户端和服务端只能依赖软件版本猜测协议格式，兼容性会非常脆弱
- 很难支持平滑演进

#### `correlationId`

设计目的：

- 将响应与请求一一对应
- 支持一个连接上多个 in-flight 请求并发存在
- 支持异步处理和乱序返回

意义：

- Broker 的处理延迟并不固定
- 先发的请求不一定先回，客户端必须通过 `correlationId` 识别响应归属

如果缺少：

- 客户端只能“发一个、等一个、再发下一个”
- 长连接复用能力会显著下降
- 吞吐、并发和超时重试设计都会受到限制

#### `clientId`

设计目的：

- 标识请求来源的逻辑客户端
- 支持按客户端维度做限流、审计、问题定位和观测

意义：

- `clientId` 不是认证身份，而是逻辑来源标识
- 它有助于识别“是谁在产生这类流量”

如果缺少：

- 系统仍然可以运行
- 但按客户端限流、排查慢请求、定位异常流量来源都会明显变难

补充约束：

- `clientId` 适合用于日志、追踪属性和有选择的指标维度
- 不适合作为高基数全局指标标签默认暴露，否则会放大指标存储与查询成本

#### `traceId / spanId / traceFlags`

设计目的：

- 将 Producer、Broker、Replica、Consumer 的请求链路串成统一 trace
- 支持 OpenTelemetry 落地
- 支持按请求粒度进行染色、灰度、旁路调试和问题定位

意义：

- `traceId` 标识整条链路
- `spanId` 标识当前请求跨度
- `traceFlags` 用于表达采样、调试、优先级等追踪位

如果缺少：

- 跨语言、跨进程追踪会断裂
- 线上诊断将更依赖日志模糊匹配
- 染色、灰度和流量实验只能依赖 `clientId` 或连接级信息，粒度过粗

补充约束：

- 建议兼容 W3C Trace Context 的十六进制表示
- `traceId` 和 `spanId` 不参与请求路由
- 但应作为追踪与诊断的一等上下文

#### `tenantId / quotaKey / authContextId / trafficClass / trafficTag`

设计目的：

- 支持多租户治理
- 支持按租户、业务线、逻辑账户或流量组进行配额与限流
- 支持连接级认证之外的请求级治理上下文透传
- 支持灰度流量、后台流量、控制流量等染色分类

意义：

- `tenantId` 用于描述资源归属
- `quotaKey` 用于决定配额桶归属，可与 `clientId` 不同
- `authContextId` 用于承载代理认证、委托身份或上游鉴权结果的上下文句柄
- `trafficClass` 用于表达流量治理等级，例如正常、金丝雀、后台、控制流
- `trafficTag` 用于标识具体实验批次、灰度分组、回放组或诊断流量标签

如果缺少：

- 限流、配额、审计、租户治理都只能退化到 `clientId` 或连接来源维度
- 无法稳定支持代理接入、委托调用和细粒度治理
- 染色流量与普通流量难以在请求入口处区分
- 同一客户端同时运行多组染色或实验流量时，难以识别具体属于哪一组

补充约束：

- 鉴权主体仍应以连接级认证结果为准
- `authContextId` 是透传与映射辅助字段，不应取代认证本身
- `trafficClass` 建议使用稳定枚举，不建议自由文本
- `trafficTag` 可以使用稳定字符串，但不建议作为默认全局高基数指标标签暴露

#### `flags`

设计目的：

- 为请求头预留轻量扩展位
- 承载一些横切、小粒度、但需要尽早生效的协议语义开关

意义：

- 某些扩展不值得为它单独升级 body 结构
- 使用位图可以在保持头部紧凑的同时，预留未来扩展空间

如果缺少：

- 许多小扩展只能通过修改 body 或提升 `headerVersion` 来实现
- 协议会更笨重，演进成本更高

兼容性要求：

- 未知标记位应按协议版本约定“拒绝”或“忽略”
- 不能让旧实现对未知 `flags` 产生静默误解

#### `headerVersion`

设计目的：

- 让请求头结构自身可以独立演进
- 将“头部升级”和“业务 API 升级”解耦

意义：

- `apiVersion` 管理的是 body 语义
- `headerVersion` 管理的是 header 自身布局
- 将来若头部新增字段，例如追踪、租户、扩展上下文等，不必强迫所有 API 一起升版本

如果缺少：

- 一旦 header 结构要扩展，往往只能同步修改所有 API 的版本处理逻辑
- 协议升级成本会被不必要地放大
- 头部结构会更难保持长期稳定

当前基线建议：

- 新实现统一使用 `headerVersion = 2`
- `headerVersion = 1` 仅作为早期草稿布局保留，不再建议用于正式实现

### 7.4 请求头字段职责速查表

| 字段 | 核心问题 | 缺少后的主要影响 |
| --- | --- | --- |
| `apiKey` | 这是什么请求 | 无法路由和正确解码 body |
| `apiVersion` | 这份请求按哪一版解释 | 新老版本兼容性脆弱 |
| `headerVersion` | 头部按哪一版解释 | header 无法平滑演进 |
| `correlationId` | 这个响应回给哪个请求 | 无法安全支持多 in-flight |
| `clientId` | 这是谁发来的逻辑流量 | 限流、审计、排障能力下降 |
| `traceId / spanId / traceFlags` | 这条请求链路是谁、是否被采样、是否被染色 | 跨语言追踪和精细染色能力不足 |
| `tenantId / quotaKey / authContextId / trafficClass / trafficTag` | 这条流量属于谁、按谁治理、用什么认证上下文、属于什么流量等级、属于哪一组实验 | 多租户治理、配额、代理鉴权和多组染色实验支撑能力不足 |
| `flags` | 有没有附加轻量语义 | 小扩展需要频繁改结构 |
 
### 7.5 请求头字段顺序

为确保 `headerVersion` 能在解析早期生效，当前标准请求头字段顺序固定为：

```text
apiKey
-> apiVersion
-> headerVersion
-> correlationId
-> clientId
-> traceId
-> spanId
-> traceFlags
-> tenantId
-> quotaKey
-> authContextId
-> trafficClass
-> trafficTag
-> flags
```

说明：

- 解析器应在读取完 `apiVersion` 后立即读取 `headerVersion`
- 后续 header 字段扩展必须通过提升 `headerVersion` 进行
- 不允许在相同 `headerVersion` 下隐式改变字段顺序

## 8. 响应头规范

### 8.1 响应头字段

每个响应必须携带统一响应头：

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `correlationId` | `int32` | 是 | 与请求对应 |
| `headerVersion` | `int16` | 是 | 响应头版本，当前基线为 `2` |
| `errorCode` | `int16` | 是 | 顶层错误码 |
| `throttleTimeMs` | `int32` | 是 | 节流延迟，未节流时为 `0` |

### 8.2 响应头语义

- `correlationId` 必须与请求一致
- `headerVersion` 用于响应头自身演进，并应与当前实现支持的响应头布局一致
- `errorCode` 表示请求级错误；分区级错误放在响应体中
- `throttleTimeMs` 用于 Producer、Consumer 和 Admin 类请求的节流反馈

### 8.3 响应头字段顺序

当前标准响应头字段顺序固定为：

```text
correlationId
-> headerVersion
-> errorCode
-> throttleTimeMs
```

## 9. apiKey 规范

### 9.1 第一版保留的核心 API

| apiKey | 名称 | 方向 | 说明 |
| --- | --- | --- | --- |
| `0` | `ApiVersions` | Client/Broker | 查询 Broker 支持的 API 版本范围 |
| `1` | `Metadata` | Client/Broker | 查询 Broker、Topic、Partition 元数据 |
| `2` | `Produce` | Client/Broker | 写入消息批 |
| `3` | `Fetch` | Client/Broker 或 Replica/Broker | 拉取消息批 |
| `4` | `ListOffsets` | Client/Broker | 查询位移边界 |
| `5` | `OffsetCommit` | Client/Broker | 提交消费位点 |
| `6` | `OffsetFetch` | Client/Broker | 查询消费位点 |
| `7` | `FindCoordinator` | Client/Broker | 查询协调器 |
| `8` | `Heartbeat` | Client/Broker | 消费组心跳 |
| `9` | `JoinGroup` | Client/Broker | 加入消费组 |
| `10` | `SyncGroup` | Client/Broker | 同步消费组分配 |

说明：

- 该编号是 `stellflow` 当前建议值，不要求与 Kafka 原始 `apiKey` 完全一致。
- 一旦正式发布第一版协议，已分配 `apiKey` 不得重用为不同语义。

### 9.2 apiKey 分配原则

- `0-99`：核心数据面与协调类 API
- `100-199`：复制与内部 Broker API
- `200-299`：保留扩展

## 10. apiVersion 规范

### 10.1 版本规则

- 每个 `apiKey` 独立维护自己的 `apiVersion`
- `apiVersion` 从 `0` 开始递增
- 同一 `apiKey` 的不同版本可以有字段增减和行为变更，但必须在文档中明确说明

### 10.2 兼容性原则

- 优先保证向后兼容
- 新字段优先以可选字段或尾部扩展字段增加
- 破坏性变更必须提升 `apiVersion`
- 不允许在相同 `apiVersion` 内变更字段语义

### 10.3 版本协商原则

- 客户端不能默认使用“自己支持的最高版本”
- 客户端必须先获取 Broker 支持的版本范围
- 客户端应在自身支持范围与 Broker 支持范围的交集内选择版本

## 11. Capability Negotiation 规范

### 11.1 协商入口

能力协商使用 `ApiVersions` 请求完成。

### 11.2 ApiVersionsRequest

建议字段：

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `clientSoftwareName` | `nullable string` | 否 | 例如 `stellflow-java`、`stellflow-go` |
| `clientSoftwareVersion` | `nullable string` | 否 | 客户端软件版本 |
| `supportedFeatures` | `array<string>` | 否 | 客户端声明支持的可选特性 |

### 11.3 ApiVersionsResponse

建议字段：

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `apiVersions` | `array<ApiVersionRange>` | 是 | 每个 API 的最小/最大版本 |
| `brokerSoftwareVersion` | `nullable string` | 否 | Broker 软件版本 |
| `supportedFeatures` | `array<string>` | 否 | Broker 支持的特性集合 |

其中 `ApiVersionRange` 建议结构为：

| 字段名 | 类型 | 说明 |
| --- | --- | --- |
| `apiKey` | `int16` | API 标识 |
| `minVersion` | `int16` | 最低支持版本 |
| `maxVersion` | `int16` | 最高支持版本 |

### 11.4 协商流程

1. 客户端连接任意可达 Broker。
2. 发送 `ApiVersionsRequest`。
3. Broker 返回自身支持的 `apiKey -> [minVersion, maxVersion]` 范围。
4. 客户端计算双方交集并缓存。
5. 后续每类请求使用交集中的最高可用版本。
6. 若 Broker 切换或返回 `UNSUPPORTED_VERSION`，客户端应刷新能力缓存。

## 12. 错误码规范

### 12.1 顶层错误码表

| errorCode | 名称 | 说明 |
| --- | --- | --- |
| `0` | `NONE` | 成功 |
| `1` | `UNKNOWN_SERVER_ERROR` | 未分类服务端错误 |
| `2` | `UNSUPPORTED_VERSION` | 请求版本不受支持 |
| `3` | `INVALID_REQUEST` | 请求格式非法或字段不合法 |
| `4` | `AUTHENTICATION_FAILED` | 认证失败 |
| `5` | `AUTHORIZATION_FAILED` | 鉴权失败 |
| `6` | `THROTTLED` | 请求被节流 |
| `7` | `BROKER_NOT_AVAILABLE` | Broker 不可用 |
| `8` | `LEADER_NOT_AVAILABLE` | 分区 Leader 不可用 |
| `9` | `NOT_LEADER_OR_FOLLOWER` | 节点不是目标分区的有效角色 |
| `10` | `UNKNOWN_TOPIC_OR_PARTITION` | Topic 或 Partition 不存在 |
| `11` | `OFFSET_OUT_OF_RANGE` | 位移越界 |
| `12` | `MESSAGE_TOO_LARGE` | 消息过大 |
| `13` | `RECORD_LIST_TOO_LARGE` | 批次过大 |
| `14` | `INVALID_RECORD` | 记录格式非法 |
| `15` | `CORRUPT_MESSAGE` | 数据损坏 |
| `16` | `COORDINATOR_NOT_AVAILABLE` | 协调器不可用 |
| `17` | `NOT_COORDINATOR` | 当前节点不是协调器 |
| `18` | `CONCURRENT_TRANSACTIONS` | 事务冲突或并发受限 |
| `19` | `FENCED_INSTANCE_ID` | 客户端实例被围栏 |
| `20` | `FEATURE_NOT_ENABLED` | 特性未启用 |
| `21` | `INVALID_PRODUCER_EPOCH` | producer epoch 非法或已过期 |
| `22` | `OUT_OF_ORDER_SEQUENCE_NUMBER` | 幂等序列号乱序 |
| `23` | `DUPLICATE_SEQUENCE_NUMBER` | 幂等序列号重复 |
| `24` | `TRANSACTION_COORDINATOR_FENCED` | 事务协调器上下文已失效 |
| `25` | `INVALID_TXN_STATE` | 当前事务状态不允许执行该操作 |
| `26` | `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` | 事务 ID 无访问权限 |
| `27` | `PRODUCER_FENCED` | producer 已被新的 epoch 围栏 |

### 12.2 错误码原则

- 顶层错误用于请求级失败
- 分区级错误应放在响应体中逐项返回
- 错误码一旦发布不得复用为新语义
- 错误码名称与跨语言 SDK 异常模型必须一一对应

## 13. Header 与 Body 的版本演进策略

### 13.1 HeaderVersion

- 请求头与响应头自身保留 `headerVersion`
- 当头部结构发生变化时，仅提升头部版本，不影响所有 API 的 `apiVersion`
- 当前正式建议实现基线为 `headerVersion = 2`

### 13.2 Body Version

- 请求体和响应体的结构变化由 `apiVersion` 控制
- 某个 API 的新版本不得隐式改变其它 API 的字段布局

## 14. 数据面与复制链路的复用原则

- Broker / Client 与 Broker / Replica 使用同一套协议头规范
- 复制链路复用 `Fetch` 等核心 API 语义
- 复制链路需要的额外字段应通过 API 版本扩展或内部字段扩展表达，不单独再定义完全不同的帧模型

## 15. 多语言实现约束

- 协议文档必须作为 Java、Golang 等 SDK 的共同实现依据
- 不允许直接将 Java 内部类名或 Netty 类型暴露到协议定义中
- 每个语言客户端都应有协议兼容性测试
- 所有语言客户端都必须遵守同一份错误码与能力协商规则

## 16. 后续待补充内容

以下内容作为下一阶段协议文档补充项：

1. 每个 `apiKey` 的正式请求体与响应体字段定义
2. `ProduceRecordBatch` 编码格式
3. `FetchResponse` 的分区数据结构
4. 认证与安全握手扩展
5. 协议兼容性测试矩阵

当前上述第 1-3 项已在以下文档中进一步展开：

- [ApiVersions / Metadata 消息格式规范](./api-versions-and-metadata-format.md)
- [Produce / Fetch 消息格式规范](./produce-fetch-message-format.md)
- [FetchRequestBody 消息格式规范](./fetch-request-format.md)
- [ProduceResponseBody 消息格式规范](./produce-response-format.md)

## 17. 结论

`stellflow` 的协议规范必须先于多语言 SDK 和 Broker 主链路稳定下来。对当前阶段而言，最重要的是先把 `apiKey / apiVersion / header / error code / capability negotiation` 这五个基础契约钉死。只要这层契约稳定，后续 Java、Golang 以及更多语言客户端都可以在统一边界内演进。
