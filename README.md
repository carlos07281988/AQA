# AQA — Agent Quality Assurance

基于消息队列的 Agent 通信协议与质量保障系统。

**队列即协议** — Agent 之间没有直接引用、没有函数调用、没有 HTTP 端点。
通信的唯一原语是 **往某个 topic 发一条消息**，别的 Agent 从 topic 上消费。

---

## 一、设计哲学

```
┌────────────────────────────────────────────────────────────────────┐
│                         设计原则                                    │
│                                                                    │
│  1. 队列即协议 — MQ 就是通信契约, 不是 RPC 的传输层                │
│  2. 信封标准化 — 所有消息共享同一个 JSON 信封结构                  │
│  3. 版本化演进 — 协议版本字段保证多版本 Agent 共存                │
│  4. 全链路追踪 — trace_id 贯穿所有消息, 无死角                    │
│  5. 可插拔后端 — Transport 层屏蔽队列实现 (Redis/Kafka/内存)      │
│  6. 外部友好  — 任何语言只要拼 JSON 就能接入                      │
└────────────────────────────────────────────────────────────────────┘
```

### 为什么不用 HTTP/RPC 做 Agent 间通信

| 对比项 | HTTP/RPC | 消息队列 (本协议) |
|---|---|---|
| 耦合度 | 调用方依赖被调方的地址+接口定义 | 零依赖, 只认 topic |
| 背压 | 调用方需要自己处理超时/重试 | 队列自带 backpressure |
| 故障隔离 | 级联失败 (A 挂了→B 重试→C 雪崩) | 消费者故障不影响生产者 |
| 多语言 | 每个语言写一套 client stub | 拼 JSON 即可 |
| 热升级 | 需要服务发现 + 负载均衡 | 停消费者→升级→启动, 消息不丢 |
| 可观测 | 需要额外埋点 | 消息本身带 trace_id |

---

## 二、消息信封 (Message Envelope)

这是系统中**所有**消息的固定 JSON 结构。无论是内部 Python Agent 还是外部 Go/JS 程序, 发送的消息都必须是这个格式。

```json
{
  "type":           "TASK_DISPATCH",
  "message_id":     "a1b2c3d4e5f6g7h8",
  "source":         "cli-orchestrator",
  "target":         "",
  "topic":          "aqa:agent:probe",
  "trace_id":       "trace_dcf9a2b1",
  "correlation_id": "",
  "version":        "1.0",
  "payload":        { "task_id": "t-001", "target_svc": "svc-a" },
  "timestamp":      "2026-06-25T10:30:00+00:00"
}
```

### 字段详解

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `type` | string | ✅ | 消息语义类型, 见下文"消息类型" |
| `message_id` | string | ✅ | 消息唯一标识, 建议 `uuid4` 或 `hex(16)` |
| `source` | string | ✅ | 发送者标识。格式建议: `{语言}-{agent名}-{实例ID}` |
| `target` | string | ❌ | 目标 Agent 标识。**空字符串 `""` 表示广播** (该 topic 下所有消费者都能处理) |
| `topic` | string | ✅ | 路由主题。必须是已定义的 topic 名 |
| `trace_id` | string | ✅ | **全链路追踪 ID**。同一任务链上所有消息共享同一个 trace_id |
| `correlation_id` | string | ❌ | 关联回覆 ID。回复消息时填原始消息的 `message_id` |
| `version` | string | ✅ | 协议版本, 当前固定 `"1.0"` |
| `payload` | object | ✅ | 业务负载。**任何合法的 JSON object** |
| `timestamp` | string | ❌ | ISO-8601 格式的时间戳。发送时自动填充 |

### 消息生命周期

```
  创建 → 序列化 → publish(topic) → 队列存储 → subscribe(topic) → 反序列化 → 处理
                                                                          ↓
                                                                       ack() 或 重试
```

每条消息在队列中的存储格式是 Redis Stream 的一个 entry, key 为 `topic` 名, value 为 JSON 序列化后的字符串。

---

## 三、消息类型 (Message Types)

系统定义了 8 种消息类型, 覆盖完整的质量保障流程:

| 类型 | 语义 | 方向 | 典型 payload |
|---|---|---|---|
| `HEARTBEAT` | 心跳 | Agent → 广播 | `{"cpu": 0.3, "mem": 512, "status": "healthy"}` |
| `TASK_DISPATCH` | 下发检测任务 | 调度器 → Probe | `{"task_id": "t-001", "target": "svc-a"}` |
| `TASK_RESULT` | 检测结果 | Probe → Judge | `{"task_id": "t-001", "passed": true, "score": 0.95}` |
| `JUDGE_REQUEST` | 请求评判 | Probe → Judge | `{"task_id": "t-001", "evidences": [...]}` |
| `JUDGE_VERDICT` | 评判裁决 | Judge → Reporter | `{"task_id": "t-001", "verdict": "PASS", "score": 92}` |
| `REPORT_REQUEST` | 请求报告 | Judge → Reporter | `{"task_id": "t-001", "format": "html"}` |
| `REPORT` | 报告结果 | Reporter → 下游 | `{"report_url": "s3://...", "summary": "5/10 通过"}` |
| `ERROR` | 系统错误 | 任意 Agent | `{"code": "TIMEOUT", "message": "probe-1 超时", "trace_id": "..."}` |

### 类型扩展规则

1. 类型名使用 `SCREAMING_SNAKE_CASE`, 长度不超过 32 字符
2. 新增类型必须在本 README 中登记
3. 自定义类型前缀建议加模块名, 如 `PLUGIN_CUSTOM_CHECK`
4. 收到不识别的类型, Agent **必须**通过 `ERROR` 消息报告, 不能静默丢弃

---

## 四、Topic 系统

Topic 是消息路由的唯一依据。Agent 不关心"发给谁", 只关心"投到哪个 topic"和"订阅哪个 topic"。

### 内置 Topic

| Topic | 用途 | 消费者 |
|---|---|---|
| `aqa:broadcast` | 全局广播通道 (心跳 / 系统通知) | 所有 Agent |
| `aqa:agent:probe` | 检测任务分发 | Probe Agent |
| `aqa:agent:judge` | 评判裁决 | Judge Agent |
| `aqa:agent:reporter` | 报告生成 | Reporter Agent |
| `aqa:inbox:{agent_id}` | Agent 私有收件箱 (定向消息) | 指定 Agent |

### Topic 命名规范

```
aqa:{scope}:{name}
  │      │       └─ 具体名称, 小写字母 + 连字符
  │      └───────── 作用域 (agent / inbox / broadcast / plugin)
  └──────────────── 系统保留前缀
```

- 系统 topic 以 `aqa:` 开头
- 自定义插件 topic 建议用 `plugin:{plugin_name}:{sub_topic}`
- 外部 Agent 可以创建任意 topic, 但需避免 `aqa:` 前缀冲突

### 路由规则

```
  publish("aqa:agent:probe", msg)
              │
              ▼
    ┌─────────────────────┐
    │  Redis Stream       │
    │  aqa:agent:probe    │  ← 消息存储在这里
    └─────────────────────┘
              │
     ┌────────┴────────┐
     ▼                  ▼
  消费组 judge-group   消费组 external-go-group
  (内部 Judge Agent)   (外部 Go Agent)
```

一个 topic 可以有多个消费组, 每个消费组独立消费。内部 Judge 和外部 Go Agent 可以同时处理同一 topic 的消息。

---

## 五、全链路追踪 (Trace & Correlation)

这是协议设计中**最重要的部分**。没有它, 一条请求经过 Probe → Judge → Reporter 三个 Agent 后就无法串联了。

### trace_id — 任务链标识

```
初始消息                     trace_id = "trace_a1b2"
  TASK_DISPATCH ────────► aqa:agent:probe
                               │
                          trace_id 保持不变 ──── 透传
                               │
  TASK_RESULT    ◄──────── aqa:agent:probe
    trace_id = "trace_a1b2"
                               │
  JUDGE_VERDICT  ◄──────── aqa:agent:judge
    trace_id = "trace_a1b2"
```

**规则**: 一条任务链上所有的消息共享同一个 `trace_id`。每个 Agent 在处理消息后继续往下游发消息时,**必须透传原始的 `trace_id`**, 不能重新生成。

### correlation_id — 消息级回覆

```
Agent A 发送 message_id="msg_001"
                    │
                    ▼
Agent B 处理完回复 message_id="msg_002", correlation_id="msg_001"
                                              │
                                              └── Agent A 通过 correlation_id
                                                  知道这是 msg_001 的回复
```

**规则**: 当消息是对另一条消息的回复时, `correlation_id` 填原始消息的 `message_id`。非回复消息的 `correlation_id` 为空字符串。

### 追踪示例

```json
// 完整追踪链 (三个消息共享 trace_id)
{
  "message_id":     "msg_a1",
  "type":           "TASK_DISPATCH",
  "trace_id":       "trace_x001",       // ← 入口生成
  "correlation_id": "",
  "payload":        { "task_id": "1" }
}
→ topic: aqa:agent:probe

{
  "message_id":     "msg_b2",
  "type":           "TASK_RESULT",
  "trace_id":       "trace_x001",       // ← 透传
  "correlation_id": "msg_a1",           // ← 关联原始消息
  "payload":        { "task_id": "1", "passed": true }
}
→ topic: aqa:agent:judge

{
  "message_id":     "msg_c3",
  "type":           "JUDGE_VERDICT",
  "trace_id":       "trace_x001",       // ← 透传
  "correlation_id": "msg_b2",
  "payload":        { "task_id": "1", "verdict": "PASS" }
}
→ topic: aqa:agent:reporter
```

**存储建议**: 配合 Redis Stream 的 ACK 机制 + 外部日志系统, 可以用 `trace_id` 做索引, 查询任意一条任务的全链路数据。

---

## 六、数据流

### 标准检测流程

```sequence
CLI / 调度器
     │
     │ publish(TASK_DISPATCH) → aqa:agent:probe
     ▼
┌──────────────────────────────────────────────────┐
│                  Probe Agent                      │
│                                                   │
│  1. 订阅 aqa:agent:probe                          │
│  2. 收到 TASK_DISPATCH                            │
│  3. 执行插件链 (validator → scorer → custom)       │
│  4. 发布 TASK_RESULT → aqa:agent:judge            │
└───────────────────────┬──────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│                  Judge Agent                      │
│                                                   │
│  1. 订阅 aqa:agent:judge                          │
│  2. 收到 TASK_RESULT / JUDGE_REQUEST              │
│  3. 综合评分, 给出裁决 (PASS / FAIL / WARN)       │
│  4. 发布 JUDGE_VERDICT → aqa:agent:reporter       │
└───────────────────────┬──────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│                Reporter Agent                     │
│                                                   │
│  1. 订阅 aqa:agent:reporter                       │
│  2. 收到 JUDGE_VERDICT / REPORT_REQUEST           │
│  3. 生成报告 (HTML / JSON / Markdown)             │
│  4. 发布 REPORT → 配置的下游 topic                │
└──────────────────────────────────────────────────┘
```

### 多级流水线 (外部 Agent 插入)

```
  TASK_DISPATCH
       │
       ▼
  aqa:agent:probe ──────┬────────────────┐
       │                 │                │
  ┌────┴────┐     ┌─────┴─────┐   ┌──────┴──────┐
  │ Python  │     │  Go       │   │  JS         │
  │ Probe   │     │  Inspector│   │  Validator  │
  └────┬────┘     └─────┬─────┘   └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │ (多个消费者并行处理)
                        ▼
                 aqa:agent:judge
```

多个 Agent 可以同时消费同一个 topic。例如一个 Python Probe 做基础检测, 一个 Go Inspector 做性能检测, 一个 JS Validator 做前端检查, 三者互不干扰, 各自发布结果到 `aqa:agent:judge`。

---

## 七、外部 Agent 接入

外部 Agent (Go / JS / Java / Rust / 任意语言) 接入的核心原则:

> **不需要 AQA SDK, 不需要 AQA 内核, 不需要 Python 环境。**
> 只需要能连接队列(REDIS_URL), 会拼 JSON。

### 接入清单

| 需要 | 不需要 |
|---|---|
| Redis 客户端 (任何语言) | AQA Python 包 |
| 理解本协议的消息信封 | 任何 SDK / stub 代码 |
| 透传 `trace_id` | 了解 AQA 内部架构 |

### 最小接入示例 (伪代码)

```python
# ❌ 错误方式: 调用 Agent SDK 的 API
judge.evaluate(result.payload)

# ✅ 正确方式: 往 topic 发一条消息
redis.publish("aqa:agent:judge", {
    "type": "TASK_RESULT",
    "source": "go-inspector-v2",
    "topic": "aqa:agent:judge",
    "trace_id": received_msg.trace_id,   # ← 必须透传
    "payload": {"task_id": "t-001", "passed": true, "score": 0.97}
})
```

### 协议桥接 (Bridge)

对于无法直接连接 Redis 的遗留系统, 提供 Protocol Bridge:

```
  外部 HTTP 服务             外部 gRPC 服务
       │                          │
  HTTP POST /inspect         rpc Inspect()
       │                          │
       ▼                          ▼
  ┌──────────────────────────────────────────┐
  │           Protocol Bridge                 │
  │                                           │
  │  HTTPBridge:   HTTP POST → AQA Queue     │
  │  gRPCBridge:   gRPC call → AQA Queue     │
  │  FileBridge:   文件监控 → AQA Queue       │
  └───────────────┬──────────────────────────┘
                  │
                  ▼
           AQA Queue Bus
```

Bridge 是薄转换层, 只有**协议转换**逻辑, 不包含任何业务逻辑。

SDK 实现见 `sdk/` 目录, 包含:
- `aqa_sdk/message.py` — Python 端消息构造/解析
- `aqa_sdk/consumer.py` — 自动 ACK 的消费循环
- `aqa_sdk/bridge/` — HTTPBridge 实现
- `examples/go_agent.go` — Go 语言外部 Agent 完整示例
- `examples/js_agent.mjs` — JS/Node.js 外部 Agent 完整示例

---

## 八、版本与兼容性

### 版本策略

```
消息版本: version: "1.0"                       ← 协议版本
系统版本: app.version: "1.0.0" in config.yaml   ← 部署版本
```

- `version` 字段只在大版本不兼容时升级 (比如字段重命名、类型定义变更)
- 新增字段或新增消息类型不算不兼容变更, `version` 可保持不变
- 系统支持多个版本的 `version` 同时存在, Agent 按 `version` 字段路由到对应的处理逻辑

### 降级策略

Agent 处理消息时:
1. 检查 `version` — 如果不支持, 发布 `ERROR` 消息
2. 检查 `type` — 如果不识别, 发布 `ERROR` 消息
3. 检查 `trace_id` — 如果缺失, 主动生成一个并记日志警告

---

## 九、Transport 实现参考

| 后端 | 状态 | 依赖 |
|---|---|---|
| `InMemoryTransport` | ✅ 完整实现 (用于测试和 Demo) | 无 |
| `RedisStreamsTransport` | ✅ 完整实现 | `redis>=5.0` |
| `KafkaTransport` | ⬜ 接口已定义, 待实现 | `aiokafka>=0.10.0` |

`Transport` 抽象接口只有 4 个方法:

```python
class Transport(ABC):
    async def connect(self) -> None
    async def publish(self, topic: str, message: str) -> None
    async def subscribe(self, topic: str) -> AsyncGenerator[str, None]
    async def ack(self, topic: str, message_id: str) -> None
```

实现一个新的后端 (如 RabbitMQ / Pulsar / NATS) 只需实现这 4 个方法。

---

## 十、插件系统

插件是 Agent 处理 `payload` 时的扩展点, 不参与消息路由。

```
收到消息 → 反序列化 → 路由到 Agent → 执行插件链 → 构建回复 → 发布
                                           │
                                    ┌──────┴──────┐
                                    │ Plugin.      │
                                    │ execute(ctx) │
                                    └─────────────┘
```

插件通过 `Plugin` 基类实现:

```python
class Plugin(ABC):
    @property
    def name(self) -> str: ...

    @property
    def config_schema(self) -> dict: ...

    async def execute(self, context: dict) -> dict: ...
```

插件的注册和卸载在 `config.yaml` 中声明, 运行时通过 `PluginRegistry` 管理。

---

## 十一、快速开始

```bash
# 1. 安装
cd AQA
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# 2. 运行测试 (无外部依赖)
PYTHONPATH=. ./venv/bin/python -m pytest tests/ -v

# 3. 运行 InMemory Demo (无需 Redis)
PYTHONPATH=. ./venv/bin/python examples/demo.py

# 4. 使用真实队列
# 修改 config.yaml → transport.backend: redis-streams
# 在 code 中使用 RedisStreamsTransport 替换 InMemoryTransport
```

---

## 项目结构

```
AQA/
├── aqa/                    # AQA 内核
│   ├── core/
│   │   ├── message.py      # 消息协议实现
│   │   └── config.py       # 配置加载
│   ├── transport/
│   │   ├── base.py         # Transport ABC
│   │   ├── redis_streams.py
│   │   └── kafka_transport.py
│   ├── plugin/
│   │   ├── base.py         # Plugin ABC
│   │   └── registry.py     # 插件注册中心
│   ├── agent/
│   │   ├── base.py         # Agent 基类 (消息循环)
│   │   ├── probe.py        # 检测 Agent
│   │   ├── judge.py        # 评判 Agent
│   │   └── reporter.py     # 报告 Agent
│   └── plugins/            # 内置插件
├── sdk/                    # 外部 Agent SDK
│   ├── aqa_sdk/            #   Python SDK (独立包)
│   ├── README.md           #   协议文档 + 多语言示例
│   └── examples/
│       ├── go_agent.go     #   Go 外部 Agent 示例
│       └── js_agent.mjs    #   JS 外部 Agent 示例
├── config.yaml             # 主配置
├── tests/
└── examples/
    └── demo.py             # InMemory Demo
```
