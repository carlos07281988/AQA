# AQA SDK — Agent Quality Assurance 外部 Agent 接入工具包

## 一句话

**队列就是协议**。外部 Agent 不需要 AQA Python 内核，只需要连接同一个 Redis Streams，按 JSON 信封格式发/收消息即可。

内置 3 个全功能 Agent（Probe / Judge / Reporter）通过心跳检测实现自我健康管理，支持故障恢复。

---

## 接入方式

### 方式一：Python SDK (推荐)

```bash
pip install redis pyyaml
export PYTHONPATH=/path/to/AQA/sdk:$PYTHONPATH
```

```python
from aqa_sdk import AQAMessage, MessageType, StreamProducer, Consumer

async def handler(msg: AQAMessage):
    print(f"收到任务: {msg.payload}")
    result = msg.reply(MessageType.TASK_RESULT, {"passed": True})
    async with StreamProducer("redis://...") as p:
        await p.publish(result)

c = Consumer("redis://...", "aqa:agent:probe", handler)
await c.start()
```

### 方式二：Go 语言

```go
// 直接读写 Redis Stream
rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

// 处理消息: 解析 JSON 信封, 处理 payload, 发布结果到下一级 topic
result := AQAMessage{
    Type:    "TASK_RESULT",
    Source:  "go-inspector",
    Topic:   "aqa:agent:judge",
    Payload: map[string]any{"passed": true, "score": 0.97},
}
rdb.XAdd(ctx, &redis.XAddArgs{Stream: "aqa:agent:judge", Values: map[string]any{"json": result.ToJSON()}})
```

完整示例: `examples/go_agent.go`

### 方式三：JavaScript/Node.js

```js
import { createClient } from 'redis';
const sub = createClient({ url: REDIS_URL });

// 消费并 ACK
const msg = JSON.parse(raw.json);
// ... 处理 ...
const reply = { type: 'TASK_RESULT', source: 'js-validator', ... };
publisher.xAdd('aqa:agent:judge', '*', { json: JSON.stringify(reply) });
```

完整示例: `examples/js_agent.mjs`

### 方式四：任何语言

只要理解下面的 JSON 信封协议，任何语言都能参与。

---

## 线协议 — JSON 信封格式

这是所有 Agent 通信的唯一契约：

```json
{
  "type":           "TASK_DISPATCH",
  "message_id":     "a1b2c3d4e5f6g7h8",
  "source":         "cli-orchestrator",
  "target":         "",
  "topic":          "aqa:agent:probe",
  "trace_id":       "trace_uuid",
  "correlation_id": "",
  "version":        "1.0",
  "payload":        { "task_id": "t-001", ... },
  "timestamp":      "2026-01-01T00:00:00+00:00"
}
```

### 字段说明

| 字段 | 必填 | 说明 |
|---|---|---|
| `type` | ✅ | 消息类型 (见下) |
| `message_id` | ✅ | 唯一 ID, 建议 16 位 hex |
| `source` | ✅ | 发送者标识 (agent 名 / 语言 / 版本) |
| `target` | ❌ | 目标 Agent (空 = 广播) |
| `topic` | ✅ | 路由主题 |
| `trace_id` | ✅ | 全链路追踪 ID, **必须透传** |
| `correlation_id` | ❌ | 关联回覆 (reply 时填原始 message_id) |
| `version` | ✅ | 固定 "1.0" |
| `payload` | ✅ | 业务数据 (任意 JSON) |
| `timestamp` | ❌ | ISO-8601 |

### 消息类型

| 类型 | 语义 | 典型 topic | 示例 payload |
|---|---|---|---|
| `HEARTBEAT` | 心跳 | `aqa:broadcast` | `{"cpu": 0.3, "mem": 512}` |
| `TASK_DISPATCH` | 下发检测任务 | `aqa:agent:probe` | `{"task_id": "...", "target": "svc-a"}` |
| `TASK_RESULT` | 检测结果 | `aqa:agent:judge` | `{"task_id": "...", "passed": true, "score": 0.95}` |
| `JUDGE_REQUEST` | 请求评判 | `aqa:agent:judge` | `{"task_id": "...", "evidences": [...]}` |
| `JUDGE_VERDICT` | 评判裁决 | `aqa:agent:reporter` | `{"task_id": "...", "verdict": "PASS", "score": 92}` |
| `REPORT_REQUEST` | 请求报告 | `aqa:agent:reporter` | `{"task_id": "...", "format": "html"}` |
| `REPORT` | 报告结果 | 任意 | `{"report_url": "...", "summary": "..."}` |
| `ERROR` | 系统错误 | 任意 | `{"code": "TIMEOUT", "message": "..."}` |

### 标准 Topic

| Topic | 用途 |
|---|---|
| `aqa:broadcast` | 全局广播 (心跳/系统消息) |
| `aqa:agent:probe` | 检测任务分发 |
| `aqa:agent:judge` | 评判裁决 |
| `aqa:agent:reporter` | 报告生成 |
| `aqa:inbox:{agent_id}` | Agent 私有收件箱 |

### 数据流

```
TASK_DISPATCH ──> aqa:agent:probe ──> TASK_RESULT ──> aqa:agent:judge ──> JUDGE_VERDICT ──> aqa:agent:reporter ──> REPORT
```

外部 Agent 可以切入**任意阶段**:
- 替换 Probe: 订阅 `aqa:agent:probe`, 发布到 `aqa:agent:judge`
- 替换 Judge: 订阅 `aqa:agent:judge`, 发布到 `aqa:agent:reporter`
- 补充检测: 订阅 `aqa:agent:probe`, 也发布到 `aqa:agent:judge` (多个消费者)

---

## 架构

```
                         AQA Queue Bus (Redis Streams)
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
   aqa:agent:probe      aqa:agent:judge       aqa:agent:reporter
         │                      │                      │
    ┌────┴────┐          ┌─────┴─────┐          ┌─────┴─────┐
    │ Probe   │          │  Judge    │          │  Reporter │
    │ Agent   │          │  Agent    │          │  Agent    │
    └─────────┘          └───────────┘          └───────────┘
         │                      │                      │
    ┌────┴────┐          ┌─────┴─────┐          ┌─────┴─────┐
    │Plugin   │          │  Plugin   │          │  Plugin   │
    │Chain    │          │  Chain    │          │  Chain    │
    └─────────┘          └───────────┘          └───────────┘
                                    ↑
         ┌──────────────────────────┘
         │  外部 Agent (Go / JS / Java / ...)
    ┌────┴────┐
    │ Redis   │
    │ Stream  │ 直接读写, 无需 SDK
    └─────────┘
```

---

## 协议版本 & 兼容性

- 版本字段 `version: "1.0"` 由 SDK 自动填充
- 未来升级到 2.0 时, 系统可同时处理 1.0 和 2.0 的消息, Agent 按版本号路由
- `trace_id` 是链路追踪的唯一键, **所有转发消息必须透传 trace_id**
