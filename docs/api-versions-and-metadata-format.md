# Stellflow ApiVersions / Metadata 消息格式规范

## 1. 文档目的

本文档用于定义 `stellflow` 数据面协议中以下两个基础 API 的字段级格式：

- `ApiVersions`
- `Metadata`

这两个 API 决定了客户端如何完成：

- 能力协商
- Broker 发现
- Topic / Partition 路由发现
- Leader 定位

本文档与以下文档配套使用：

- [协议规范文档](./protocol-spec.md)
- [Produce / Fetch 消息格式规范](./produce-fetch-message-format.md)
- [消息格式样例报文](./message-format-examples.md)

## 2. 适用范围

本文档适用于：

- Java Client 与 Java Broker 的能力协商和元数据发现
- Golang Client 与 Java Broker 的能力协商和元数据发现
- 后续更多语言客户端的 bootstrap 与 metadata refresh

本文档当前只定义第一版可实现基线：

- `ApiVersions apiKey = 0, apiVersion = 0`
- `Metadata apiKey = 1, apiVersion = 0`

## 3. 设计原则

### 3.1 先协商，再访问

客户端在连接 Broker 后，应优先发送 `ApiVersionsRequest`，确认对端支持的 API 版本范围，再决定后续 `Metadata / Produce / Fetch` 等请求使用哪个 `apiVersion`。

### 3.2 元数据是路由事实来源

客户端不应长期依赖 `bootstrap servers` 作为真实数据面路由。  
`bootstrap servers` 只用于建立初始连接，后续分区路由应基于 `MetadataResponse` 的返回结果。

### 3.3 元数据布局优先简单稳定

第一版 `Metadata` 优先满足：

- Broker 列表发现
- Topic / Partition 发现
- Leader / Replica / ISR 发现
- 版本升级时易于尾部扩展

不优先引入复杂增量 session 语义。

## 4. 编码约定

除非另有说明，本节沿用 [协议规范文档](./protocol-spec.md) 中的基础编码规则：

- `int8`：1 字节有符号整数
- `int16`：2 字节有符号整数，大端序
- `int32`：4 字节有符号整数，大端序
- `int64`：8 字节有符号整数，大端序
- `bool`：1 字节，`0` 或 `1`
- `string`：`int16 length + UTF-8 bytes`
- `nullable string`：`int16 length = -1` 表示 `null`
- `array<T>`：`int32 length + repeated entries`

## 5. ApiVersionsRequestBody

### 5.1 顶层结构

`ApiVersionsRequestBody` 用于 `apiKey = 0, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `clientSoftwareName` | `nullable string` | 否 | 客户端软件名，例如 `stellflow-java-client` |
| `clientSoftwareVersion` | `nullable string` | 否 | 客户端软件版本，例如 `0.1.0-SNAPSHOT` |
| `supportedFeatures` | `array<string>` | 是 | 客户端声明支持的可选特性列表，可为空数组 |

### 5.2 字段语义

#### `clientSoftwareName`

- 用于诊断、审计和灰度排查
- 不参与协议行为判断
- 第一版建议客户端始终填写

#### `clientSoftwareVersion`

- 用于问题定位和兼容性分析
- 不应替代 `apiVersion`

#### `supportedFeatures`

- 表达客户端支持的可选能力，例如：
  - `compression.gzip`
  - `compression.lz4`
  - `fetch.long_poll`
  - `auth.sasl_plain`
  - `producer.idempotence`
  - `producer.transactions`
  - `observability.trace_context`
  - `governance.multi_tenant`
- 第一版只做信息声明，是否真正启用由服务端能力和后续 API 语义共同决定

## 6. ApiVersionsResponseBody

### 6.1 顶层结构

`ApiVersionsResponseBody` 用于 `apiKey = 0, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `apiVersions` | `array<ApiVersionRange>` | 是 | Broker 支持的 API 版本范围 |
| `brokerSoftwareName` | `nullable string` | 否 | Broker 软件名，例如 `stellflow-broker` |
| `brokerSoftwareVersion` | `nullable string` | 否 | Broker 软件版本 |
| `supportedFeatures` | `array<string>` | 是 | Broker 支持的可选特性列表，可为空数组 |

### 6.2 ApiVersionRange

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `apiKey` | `int16` | 是 | API 标识 |
| `minVersion` | `int16` | 是 | 最低支持版本 |
| `maxVersion` | `int16` | 是 | 最高支持版本 |

### 6.3 第一版建议返回的核心 API

| apiKey | 名称 | minVersion | maxVersion |
| --- | --- | --- | --- |
| `0` | `ApiVersions` | `0` | `0` |
| `1` | `Metadata` | `0` | `0` |
| `2` | `Produce` | `0` | `0` |
| `3` | `Fetch` | `0` | `0` |
| `4` | `ListOffsets` | `0` | `0` |
| `5` | `OffsetCommit` | `0` | `0` |
| `6` | `OffsetFetch` | `0` | `0` |
| `7` | `FindCoordinator` | `0` | `0` |
| `8` | `Heartbeat` | `0` | `0` |
| `9` | `JoinGroup` | `0` | `0` |
| `10` | `SyncGroup` | `0` | `0` |

### 6.4 响应语义

- `apiVersions` 必须覆盖 Broker 对外可见的全部受支持 API
- `supportedFeatures` 应只返回当前节点已启用且可对外提供的能力
- 若某特性只在部分 Broker 打开，客户端不应仅依赖第一次协商结果做永久缓存

## 7. MetadataRequestBody

### 7.1 顶层结构

`MetadataRequestBody` 用于 `apiKey = 1, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topics` | `array<MetadataTopicRequest>` | 是 | 要查询的 topic 列表，可为空 |
| `includeClusterAuthorizedOperations` | `bool` | 是 | 是否返回集群级授权信息，第一版建议固定为 `0` |
| `includeTopicAuthorizedOperations` | `bool` | 是 | 是否返回 topic 级授权信息，第一版建议固定为 `0` |
| `allowAutoTopicCreation` | `bool` | 是 | 是否允许在查询时触发自动建 topic，第一版建议固定为 `0` |

### 7.2 MetadataTopicRequest

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `topic` | `string` | 是 | Topic 名称 |

### 7.3 请求语义

- 当 `topics.length > 0` 时，表示查询指定 topic
- 当 `topics.length = 0` 时，表示查询全部 topic 的元数据
- 第一版建议客户端默认只查自己需要的 topic，避免全量元数据过大
- `allowAutoTopicCreation` 在 `stellflow` 第一版中建议始终关闭，避免元数据请求带有副作用

## 8. MetadataResponseBody

### 8.1 顶层结构

`MetadataResponseBody` 用于 `apiKey = 1, apiVersion = 0`。

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `clusterId` | `nullable string` | 否 | 集群逻辑标识 |
| `controllerId` | `int32` | 是 | 当前活动 Controller 对应的 Broker ID |
| `brokers` | `array<MetadataBroker>` | 是 | Broker 列表 |
| `topics` | `array<MetadataTopicResponse>` | 是 | Topic 元数据列表 |
| `clusterAuthorizedOperations` | `int32` | 是 | 集群授权位图，第一版可固定为 `0` |

### 8.2 MetadataBroker

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `brokerId` | `int32` | 是 | Broker ID |
| `host` | `string` | 是 | Broker 对外主机名或稳定逻辑域名 |
| `port` | `int32` | 是 | Broker 对外端口 |
| `rack` | `nullable string` | 否 | 机架或可用区信息，第一版可为空 |

### 8.3 MetadataTopicResponse

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `errorCode` | `int16` | 是 | Topic 级错误码 |
| `topic` | `string` | 是 | Topic 名称 |
| `isInternal` | `bool` | 是 | 是否为内部 topic |
| `partitions` | `array<MetadataPartitionResponse>` | 是 | Partition 元数据 |
| `topicAuthorizedOperations` | `int32` | 是 | Topic 授权位图，第一版可固定为 `0` |

### 8.4 MetadataPartitionResponse

| 字段名 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `errorCode` | `int16` | 是 | 分区级错误码 |
| `partition` | `int32` | 是 | 分区编号 |
| `leaderId` | `int32` | 是 | 当前 Leader Broker ID |
| `leaderEpoch` | `int32` | 是 | 当前 Leader Epoch |
| `replicaNodes` | `array<int32>` | 是 | 全量副本 Broker ID 列表 |
| `isrNodes` | `array<int32>` | 是 | 当前 ISR Broker ID 列表 |
| `offlineReplicaNodes` | `array<int32>` | 是 | 当前离线副本 Broker ID 列表，可为空 |

## 9. ApiVersions 设计说明

### 9.1 为什么需要独立协商 API

因为 `stellflow` 需要同时支持：

- Java Client
- Golang Client
- 后续更多语言客户端

如果没有独立的 `ApiVersions` 协商入口，客户端只能“猜” Broker 支持的版本范围，滚动升级和跨版本联调会非常脆弱。

### 9.2 协商缓存建议

客户端本地可缓存：

- `broker endpoint -> apiVersions`
- `clusterId -> supportedFeatures`

但以下场景应触发刷新：

- 首次连接
- Broker 重连
- 收到 `UNSUPPORTED_VERSION`
- 元数据刷新后发现目标 Broker 切换

## 10. Metadata 设计说明

### 10.1 为什么 Metadata 要显式返回 Broker 地址

因为客户端并不是只连 `bootstrap servers`。  
客户端在拿到 `MetadataResponse` 后，应直接连接目标 partition leader 对应的 Broker 地址。

### 10.2 为什么 Broker 地址应使用稳定逻辑地址

为了支持：

- Broker 横向扩容
- 机房搬迁
- 机器替换

`MetadataBroker.host` 不应直接暴露易变裸 IP，而应优先返回稳定域名或稳定逻辑地址。

### 10.3 `controllerId` 的作用

虽然 `stellflow` 的控制面使用 `gRPC`，但数据面客户端仍然可能需要知道集群当前控制器标识，用于：

- 诊断
- 管理接口定位
- 运维可观测性辅助

### 10.4 为什么第一版不做增量 Metadata Session

第一版优先完成：

- 正确性
- 多语言一致性
- 简单稳定的实现

增量 metadata session 虽然能减少流量，但会引入：

- 会话态维护
- 增量计算
- 断线重同步复杂度

因此建议后续通过更高 `apiVersion` 再引入。

## 11. 错误码与字段级约束

### 11.1 ApiVersions

- `apiVersions` 不能为空
- 任一 `ApiVersionRange` 必须满足 `minVersion <= maxVersion`
- 若请求 `apiVersion` 不支持，应返回顶层 `UNSUPPORTED_VERSION`

### 11.2 MetadataRequest

- `topics` 可为空，但不可为 `null`
- topic 名称不得为空字符串
- 第一版若 `allowAutoTopicCreation = 1`，Broker 可返回 `FEATURE_NOT_ENABLED` 或 `INVALID_REQUEST`

### 11.3 MetadataResponse

- `controllerId` 应存在于 `brokers` 集合中；若控制器不可见，可返回 `-1`
- 每个 `MetadataPartitionResponse.leaderId` 应在 `replicaNodes` 中
- `isrNodes` 必须是 `replicaNodes` 的子集
- `offlineReplicaNodes` 必须是 `replicaNodes` 的子集

## 12. 二进制顺序示意

### 12.1 ApiVersionsRequest

```text
frameLength
  -> apiKey
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
  -> clientSoftwareName
  -> clientSoftwareVersion
  -> supportedFeatures.length
    -> supportedFeatures[i]
```

### 12.2 ApiVersionsResponse

```text
frameLength
  -> correlationId
  -> errorCode
  -> throttleTimeMs
  -> headerVersion
  -> apiVersions.length
    -> apiKey
    -> minVersion
    -> maxVersion
  -> brokerSoftwareName
  -> brokerSoftwareVersion
  -> supportedFeatures.length
    -> supportedFeatures[i]
```

### 12.3 MetadataRequest

```text
frameLength
  -> apiKey
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
  -> topics.length
    -> topic
  -> includeClusterAuthorizedOperations
  -> includeTopicAuthorizedOperations
  -> allowAutoTopicCreation
```

### 12.4 MetadataResponse

```text
frameLength
  -> correlationId
  -> errorCode
  -> throttleTimeMs
  -> headerVersion
  -> clusterId
  -> controllerId
  -> brokers.length
    -> brokerId
    -> host
    -> port
    -> rack
  -> topics.length
    -> errorCode
    -> topic
    -> isInternal
    -> partitions.length
      -> errorCode
      -> partition
      -> leaderId
      -> leaderEpoch
      -> replicaNodes.length
        -> replicaNode
      -> isrNodes.length
        -> isrNode
      -> offlineReplicaNodes.length
        -> offlineReplicaNode
    -> topicAuthorizedOperations
  -> clusterAuthorizedOperations
```

## 13. 与后续 API 的关系

`ApiVersions` 和 `Metadata` 是后续数据面所有核心 API 的入口：

1. `ApiVersions` 决定客户端能用哪些版本
2. `Metadata` 决定客户端该连哪些 Broker、把请求发到哪里
3. `Produce / Fetch` 在此基础上完成真实读写和复制

因此这两个 API 的文档稳定性，直接决定多语言客户端能否顺利启动和路由。

## 14. 后续待补充内容

下一阶段建议继续补充：

1. `ApiVersions / Metadata` 对应的样例报文
2. `Metadata` 不存在 topic 的错误样例
3. metadata refresh 策略和客户端缓存时序图
4. 增量 metadata session 的高版本预留设计

## 15. 结论

对于 `stellflow` 这种采用自定义二进制协议、又希望支持 Java 与 Golang 多语言客户端的系统，`ApiVersions` 和 `Metadata` 不是辅助接口，而是整个数据面连接、发现与路由的起点。把这两部分字段级格式先钉死，后续的 `Produce / Fetch / Replica Fetch` 才能稳定落地。
