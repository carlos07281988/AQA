# AQAP Protocol v2 — Agent Queue Agent Communication Protocol

> 基于消息队列的 Agent 间通信协议，以 **队列即协议** 为核心理念。
> 队列自身承载路由优先级、重试语义、死信隔离 —— 不依赖 HTTP/RPC。

---

## 一、设计原则

| 原则 | 说明 |
|------|------|
| **队列即协议** | Topic = 消息类型 + 路由 + 优先级。队列就是协议接口 |
| **层间隔离** | Transport / Message / State Machine / Security 严格分层 |
| **Schema 契约** | Payload 使用 JSON Schema 或 Protobuf 定义，类型安全 |
| **显式状态机** | 每条消息的生命周期有明确的状态转移图 |
| **可插拔** | Transport 后端、序列化格式、加密策略均可替换 |
| **多语言优先** | 协议标准优先于实现，SDK 按语言独立发布 |

---

## 二、协议层次

### 2.1 Transport Layer

定义消息如何在一组 Agent 之间可靠传递。已有4种后端。

**关键改进：**

```python
class Transport(ABC):
    """统一 Transport 接口 — v2 规范化"""

    @abstractmethod
    async def connect(self) -> None: ...
    @abstractmethod
    async def disconnect(self) -> None: ...
    @abstractmethod
    async def publish(
        self, topic: str, message: Envelope, /
    ) -> None: ...
    @abstractmethod
    def subscribe(
        self, topic: str, group: str, consumer: str = ""
    ) -> AsyncGenerator[Envelope, None]: ...
    @abstractmethod
    async def ack(
        self, topic: str, message_id: str, group: str = "", /
    ) -> None: ...
    @abstractmethod
    async def nack(
        self, topic: str, message_id: str, group: str = "",
        requeue: bool = True, /
    ) -> None: ...
    @abstractmethod
    async def create_group(
        self, topic: str, group: str, /
    ) -> None: ...
    @property
    @abstractmethod
    def name(self) -> str: ...
    @abstractmethod
    async def health(self) -> dict: ...
```

**v2 变更：**
- topic 统一为 `str`（不做 Topic 枚举，保持 Transport 中立于消息语义）
- 增加 `nack()`——显式拒绝，可 requeue
- 所有参数使用 `positional-only`（`/`）防止混淆
- `publish()` 接收 `Envelope`（而非 Message）—— Transport 只关心信封

### 2.2 Message Layer — 分层信封

消息从单层变为三层结构：

```python
@dataclass
class Envelope:
    """传输层信封 — 只关心路由和可靠性"""

    message_id: str           # UUID v7
    topic: str                # 队列名
    protocol_version: str     # 协议版本号 (semver)
    payload_encoding: str     # "json" | "protobuf" | "msgpack"
    payload: bytes            # 已序列化的载荷
    signature: str = ""       # HMAC 签名 (可选)
    timestamp: float = 0      # 创建时间戳


@dataclass
class Message:
    """消息信封 — 协议语义层"""

    # ── 路由 ──
    source: str               # 发件 Agent ID
    target: str = ""          # 收件 Agent ID (空 = 广播/按 topic 路由)
    correlation_id: str = ""  # 关联消息 ID (回复链)

    # ── 追踪 ──
    trace_id: str             # 链路追踪 ID (UUID v7)
    span_id: str              # 当前处理单元 ID

    # ── 类型 ──
    type: MessageType         # 消息类型枚举

    # ── 载荷 ──
    headers: dict[str, str]   # 元数据键值对
    body: bytes               # 业务载荷 (Schema 契约校验)

    # ── 生命周期 ──
    version: int = 1          # 消息版本号 (乐观锁)
    created_at: float = 0     # unix 时间戳


@dataclass
class SchemaEnvelope:
    """Schema 层 — 业务契约"""

    schema_id: str             # Schema 标识符 (如 "task.v1")
    schema_version: str        # Schema 版本
    data: dict[str, Any]       # 验证通过的业务数据
```

**三层映射关系：**

```
Transport Layer:  Envelope { topic, payload(binary), signature }
                          │
                          ▼ 反序列化 payload
Message Layer:    Message { source, target, trace_id, body(binary) }
                          │
                          ▼ Schema 校验 body
Schema Layer:     SchemaEnvelope { schema_id, data }
```

### 2.3 State Machine Layer

每条消息在一个 Agent 内的处理有显式的状态机：

```
                    ┌──────────┐
                    │ RECEIVED │ ← 从队列取出
                    └────┬─────┘
                         │
                    ┌────▼─────┐
                    │ DECODING │ ← Envelope → Message 解析
                    └────┬─────┘
                         │
                    ┌────▼─────┐
                    │ VALIDATE │ ← Schema 校验 + 幂等检查
                    └────┬─────┘
                         │
               ┌─────────┼─────────┐
               ▼         ▼         ▼
          ┌────────┐ ┌───────┐ ┌───────┐
          │ HANDLE │ │ DROP  │ │ ERROR │
          └───┬────┘ │(校验   │ │(格式   │
              │      │ 失败)  │ │ 错误)  │
         ┌────┴────┐ └───────┘ └───────┘
         ▼         ▼
    ┌────────┐ ┌───────┐
    │ REPLY  │ │ DLQ   │
    └────┬───┘ └───────┘
         │
    ┌────▼────┐
    │ ACK'ED  │ ← 确认消费完成
    └─────────┘
```

**重试路径（如 handle 抛异常）：**

```
RECEIVED → HANDLE → (异常) → retry_count < max_retries?
    ├─ YES → RECEIVED（不 ACK，下次重新投递）
    └─ NO  → DLQ 消息生成 → 发布到 dlq topic → ACK 原消息
```

### 2.4 Schema Contract Layer

定义 Agent 间的业务契约。以 JSON Schema 为例：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "aqap:schema:task.v1",
  "title": "Quality Task",
  "type": "object",
  "properties": {
    "task_id": { "type": "string", "pattern": "^task-[a-z0-9-]+$" },
    "type": { "type": "string", "enum": ["code_review", "unit_test", "integration_test"] },
    "target": {
      "type": "object",
      "properties": {
        "repo": { "type": "string" },
        "branch": { "type": "string" },
        "commit": { "type": "string", "pattern": "^[a-f0-9]{40}$" }
      },
      "required": ["repo", "branch"]
    },
    "config": {
      "type": "object",
      "properties": {
        "timeout_seconds": { "type": "integer", "minimum": 1, "maximum": 3600 },
        "parallelism": { "type": "integer", "minimum": 1, "maximum": 16 }
      }
    }
  },
  "required": ["task_id", "type", "target"]
}
```

**Schema Registry**（新增组件）：

```python
class SchemaRegistry:
    """Schema 注册表 — 中心化或分布式"""

    def register(self, schema_id: str, schema: dict) -> None: ...
    def validate(self, schema_id: str, data: dict) -> ValidationResult: ...
    def get(self, schema_id: str) -> dict | None: ...
    def has(self, schema_id: str) -> bool: ...
    def list(self) -> list[SchemaMeta]: ...
```

**内置 Schema 包：**
- `aqap.schema.task.v1` — 检测任务
- `aqap.schema.result.v1` — 检测结果
- `aqap.schema.verdict.v1` — 评判裁决
- `aqap.schema.report.v1` — 报告
- `aqap.schema.heartbeat.v1` — 心跳
- `aqap.schema.error.v1` — 错误
- `aqap.schema.dlq.v1` — 死信元数据

---

## 三、Topic 体系

### 3.1 Topic 命名规范

```
aqap:v2:{domain}:{subtype}[:{qualifier}]
```

| 层级 | 说明 | 示例 |
|------|------|------|
| `aqap` | 系统前缀 | — |
| `v2` | 协议版本 | `v2` |
| `{domain}` | 功能域 | `agent`, `system`, `dlq`, `event` |
| `{subtype}` | 子类型 | `probe`, `judge`, `report`, `error` |
| `{qualifier}` | 可选限定 | `{agent_id}` |

**预定义 Topic：**

| Topic | 用途 | 保留策略 |
|-------|------|----------|
| `aqap:v2:agent:probe` | Probe Agent 任务分发 | 7 天 |
| `aqap:v2:agent:judge` | Judge Agent 评判请求 | 7 天 |
| `aqap:v2:agent:report` | Reporter 报告 | 7 天 |
| `aqap:v2:agent:result` | 检测结果（共用管道） | 30 天 |
| `aqap:v2:system:event` | 系统事件（上线/下线） | 3 天 |
| `aqap:v2:system:heartbeat` | 心跳 | 1 天 |
| `aqap:v2:error:dlq` | 死信队列 | 30 天 |
| `aqap:v2:error:global` | 全局错误通道 | 7 天 |
| `aqap:v2:inbox:{agent_id}` | Agent 定向收件箱 | 7 天 |
| `aqap:v2:broadcast` | 全局广播 | 1 天 |

### 3.2 消费者组策略

| 场景 | Group 命名 | 说明 |
|------|-----------|------|
| 负载均衡 | `aqap:v2:{topic}:workers` | 多个相同 Agent 平分消息 |
| 独占消费 | `aqap:v2:{topic}:{agent_id}` | 每个 Agent 独立消费全部消息 |
| 广播 | group 为空 | 每条消息投递到所有消费者 |

---

## 四、消息类型枚举

```python
class MessageType(str, Enum):
    # ── 检测流程 ──
    TASK_DISPATCH   = "aqap:task:dispatch"     # 任务分发
    TASK_RESULT     = "aqap:task:result"        # 检测结果
    TASK_CANCEL     = "aqap:task:cancel"        # 任务取消

    # ── 评判流程 ──
    JUDGE_REQUEST   = "aqap:judge:request"      # 评判请求
    JUDGE_VERDICT   = "aqap:judge:verdict"      # 评判裁决

    # ── 报告流程 ──
    REPORT_REQUEST  = "aqap:report:request"     # 报告请求
    REPORT_DELIVER  = "aqap:report:deliver"     # 报告投递

    # ── 系统 ──
    HEARTBEAT       = "aqap:system:heartbeat"   # 心跳
    REGISTER        = "aqap:system:register"    # Agent 注册
    SHUTDOWN        = "aqap:system:shutdown"    # Agent 下线
    ERROR           = "aqap:system:error"       # 错误通知
    DLQ_MESSAGE     = "aqap:system:dlq"         # 死信
```

---

## 五、安全层

### 5.1 加密

```
Payload Encryption:
  Algorithm: AES-256-GCM
  Key: HKDF(secret_key, salt="aqap-v2-payload", info=message_type)
  Nonce: 12 bytes random, prepended to ciphertext
  AAD: topic + message_id + trace_id (防止重放和路由篡改)

Envelope Signature:
  Algorithm: HMAC-SHA256
  Key: HKDF(secret_key, salt="aqap-v2-envelope", info=topic)
  Signs: message_id + topic + timestamp + payload_hash
```

### 5.2 密钥管理

```
config.yaml 不再存 secret 明文，改为：
1. 环境变量 AQAP_SECRET_KEY（主密钥）
2. 或 Kubernetes Secret volume mount
3. 或 AWS Secrets Manager / Vault 集成（通过插件）

config.yaml 仅存：
  security:
    key_source: "env"       # env | file | vault
    key_path: ""            # 文件路径（当 key_source=file）
    algorithm: "AES-256-GCM"
```

### 5.3 密钥轮换

```
┌───────────────────────────────────────────────────┐
│  Key Rotation Protocol                             │
│                                                    │
│  1. 新密钥写入 key_source（同一位置）               │
│  2. 广播 ROTATE_KEY 事件到 system:event            │
│  3. 收到事件的 Agent 主动重新加载密钥               │
│  4. 旧消息仍可用旧密钥解密（解密失败 → 尝试旧密钥） │
│  5. 所有新消息使用新密钥加密                        │
└───────────────────────────────────────────────────┘
```

---

## 六、Agent 生命周期（显式状态机）

```
      ┌──────────────────────┐
      │       CREATED        │ ← Agent 实例化
      └──────────┬───────────┘
                 │ start()
      ┌──────────▼───────────┐
      │      CONNECTING      │ ← transport.connect()
      └──────────┬───────────┘
                 │ 连接成功
      ┌──────────▼───────────┐
      │       IDLE           │ ← 心跳开始，等待任务
      └──────────┬───────────┘
                 │ 收到消息 / 定时任务触发
      ┌──────────▼───────────┐
      │      PROCESSING      │ ← 正在处理
      └──────────┬───────────┘
                 │ 完成 / 全部任务完成
      ┌──────────▼───────────┐
      │       IDLE           │
      └──────────┬───────────┘
                 │ stop()
      ┌──────────▼───────────┐
      │     DISCONNECTING    │ ← 广播下线，等待进行中任务
      └──────────┬───────────┘
                 │ 全部完成
      ┌──────────▼───────────┐
      │      TERMINATED      │
      └──────────────────────┘

错误恢复：
  IDLE/IDLE → 心跳超时或连接断开 → RECONNECTING → IDLE
```

---

## 七、传输层实现对比

| 后端 | 场景 | At-least-once | 消费者组 | 持久化 | 延迟 |
|------|------|:---:|:---:|:---:|:---:|
| **InMemory** | 测试/演示 | ❌ | ❌ | ❌ | <1ms |
| **Redis Streams** | 轻量生产 | ✅ | ✅ | ✅ | ~1ms |
| **Kafka** | 高吞吐生产 | ✅ | ✅ | ✅ | ~10ms |
| **RabbitMQ** | 企业级 | ✅ | ✅ | ✅ | ~1ms |
| **NATS JetStream** (计划) | 云原生 | ✅ | ✅ | ✅ | <1ms |

---

## 八、测试策略

### 8.1 测试金字塔

```
         ┌────────────┐
         │   E2E  │     └────────────┘
         │  Integration (20%) │
         └───────────────────────┘
         │   Unit Tests (70%) │
         └───────────────────────────┘
```

### 8.2 关键测试覆盖

| 类别 | 项目 | 说明 |
|------|------|------|
| **协议层** | Envelope 序列化/反序列化 | 所有 encoding 格式 |
| | Schema 校验 | 有效/无效/未知 schema |
| | 版本兼容性 | v1→v2 升级路径 |
| **传输层** | 连接/断开 | 正常 + 异常 |
| | at-least-once 语义 | 崩溃后验证不丢消息 |
| | 消费者组负载均衡 | 多消费者分担 |
| **状态机** | 全生命周期 | CREATED → TERMINATED |
| | 重试 + 达到上限 | 3 次重试后进 DLQ |
| | 死信重放 | 重放后队列长度正确 |
| **安全层** | 加密/解密 | AES-256-GCM |
| | HMAC 签名 | 篡改检测 |
| | 密钥轮换 | 新旧密钥兼容 |
| **多 Agent** | Probe → Judge → Reporter | 完整链路 |
| | 并发消息 | 10 个任务同时 |
| | Agent 失联发现 | 心跳超时 |

---

## 九、SDK 设计（多语言）

### 9.1 Python SDK (已存在，v2 改进)

```python
# 发布任务
client = AQAPClient(transport=redis)
await client.connect()

task = TaskSchema(
    task_id="task-abc-123",
    type="code_review",
    target=Target(repo="org/repo", branch="feature/foo"),
)
await client.dispatch(topic="aqap:v2:agent:probe", body=task)

# 消费结果
async for msg in client.consume(topic="aqap:v2:agent:result"):
    result = ResultSchema.validate(msg.body)
    print(result.score)
```

### 9.2 SDK 公共接口（所有语言）

```
┌─────────────────────────────────────────────┐
│          AQAP SDK — 公共接口契约              │
├─────────────────────────────────────────────┤
│                                              │
│  connect(config: Config) -> Client           │
│  │                                           │
│  ├─ dispatch(topic, body, headers?)           │
│  ├─ request(topic, body, timeout?) -> Message │
│  ├─ consume(topic, group?, handler?)           │
│  ├─ ack(message_id)                           │
│  ├─ nack(message_id, requeue?)                │
│  └─ close()                                   │
│                                              │
│  # Schema 工具                                │
│  Schema.validate(data) -> Result              │
│  Schema.load(schema_id) -> Schema             │
│                                              │
│  # 安全工具                                   │
│  Secret.load(key_source) -> Secret            │
│  Secret.encrypt(payload) -> bytes             │
│  Secret.decrypt(ciphertext) -> bytes          │
└─────────────────────────────────────────────┘
```

### 9.3 计划支持的语言

| 语言 | SDK 包名 | 优先级 |
|------|---------|:------:|
| Python | `aqap-sdk` | ✅ 已存在，v2 升级 |
| TypeScript | `@aqap/sdk` | 🚧 P1 |
| Go | `github.com/aqap/sdk-go` | 🚧 P2 |
| Java | `io.aqap:sdk` | 📋 P3 |
| Rust | `aqap-sdk` | 📋 P4 |

---

## 十、协议升级策略

### 10.1 版本兼容性矩阵

```
protocol_version = "2.0.0"  (semver)

major 不兼容 (v1 → v2): 需桥接 Agent
minor 向后兼容 (v2.0 → v2.1): 新字段可忽略
patch 完全兼容 (v2.0.0 → v2.0.1): 热修复
```

### 10.2 版本协商

```
Agent A                              Agent B
   │                                    │
   │  REGISTER (protocol_version=2.1.0) │
   │────────────────────────────────────►│
   │                                    │
   │  REGISTER_ACK (protocol_version)   │
   │◄────────────────────────────────────│
   │                                    │
   │  — 双方使用 min(our, their) 的  │
   │    major version 通信              │
   │                                    │
   │  — 如果 major 不兼容，A 只发      │
   │    系统事件，不入业务 Topic       │
```

### 10.3 桥接示例

```python
class ProtocolBridgeV1ToV2(Agent):
    """v1 ↔ v2 协议桥接"""

    async def handle_message(self, message: Message) -> list[Message]:
        if message.protocol_version.startswith("1."):
            # 将 v1 消息包装为 v2 Envelope
            v2_msg = self._upgrade(message)
            return [v2_msg]
        elif message.protocol_version.startswith("2."):
            v1_msg = self._downgrade(message)
            return [v1_msg]
```

---

## 十一、v1 → v2 迁移路径

### 11.1 向后兼容（推荐）

```
1. 所有现有代码保持运行，Topic 名不变
2. 新增 v2 前缀 Topic（aqap:v2:*）与旧 Topic 共存
3. ProtocolBridgeV1ToV2 Agent 桥接两个世界
4. 逐步迁移 Agent 到 v2
5. 旧 Topic 和桥接 Agent 在所有 Agent 迁移后下线
```

### 11.2 配置示例

```yaml
# config.yaml
app:
  name: "AQAP v2"
  protocol_version: "2.0.0"

transport:
  backend: "redis-streams"
  redis_url: "${REDIS_URL}"

protocol:
  # v2 核心
  schema_registry:
    backend: "embedded"  # embedded | redis | file
    auto_register: true
  state_machine:
    enabled: true
    metrics: true

  # 迁移支持
  bridge:
    v1_topic_prefix: "aqap:"        # 旧 Topic 前缀
    v2_topic_prefix: "aqap:v2:"     # 新 Topic 前缀
    enabled: true                    # 启用桥接
  
  message:
    max_body_size: 10485760          # 10 MB
    default_encoding: "json"         # json | protobuf | msgpack
    compression: "zlib"              # none | zlib | zstd

security:
  key_source: "env"                  # env | file | vault
  algorithm: "AES-256-GCM"
  envelope_hmac: true                # 启用 HMAC 签名
  key_rotation_interval: 86400       # 密钥轮换间隔（秒）
```

---

## 十二、总结

| 维度 | v1 现状 | v2 目标 |
|------|---------|---------|
| 消息结构 | 单层 Message | 三层 (Envelope + Message + Schema) |
| 状态管理 | 隐式 | 显式状态机 + 生命周期 |
| Schema | dict[str, Any] | JSON Schema / Protobuf 契约 |
| 安全 | AES-GCM, 密钥明文 | HKDF 派生 + HMAC 签名 + Vault 集成 |
| 路由 | topic 枚举 + 硬编码链 | 统一的 topic 命名空间 + 灵活路由 |
| 多语言 | Python only | Python/TS/Go/Java/Rust SDK |
| 密钥轮换 | ❌ | ✅ |
| 版本协商 | 字符串比较 | Semver + 桥接 Agent |
| DLQ | 基本实现 | 结构化死信 + 可追溯重放 |
| 并发安全 | InMemory 有竞态 | 所有 Transport 线程安全 |
