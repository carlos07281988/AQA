# AQAP 工程审查报告

> 审查日期：2026-07-01
> 审查范围：全部 34 个 .py 文件, 14 个 .md 文档, 配置, 测试

---

## 一、架构评分

| 维度 | 评分 (1-5) | 说明 |
|------|:---------:|------|
| 模块化 | ★★★★½ | 层间解耦优秀，Transport/Agent/Plugin 各司其职 |
| 可测试性 | ★★★★½ | 37 项测试 + InMemoryTransport 隔离，质量高 |
| 可扩展性 | ★★★★ | 插件系统和 Transport 抽象层设计良好 |
| 安全性 | ★★★ | 加密到位，但密钥管理薄弱，无签名机制 |
| 文档 | ★★★★½ | 全面且结构清晰 |
| 代码质量 | ★★★★ | 类型注解、异常处理、日志一致性好 |
| 协议设计 | ★★★½ | 单层消息结构不足，路由隐式，缺少 Schema 契约 |

---

## 二、关键 Bug & 问题

### 🔴 Bug 1: InMemoryTransport 竞态条件

**文件**: `aqap/transport/inmemory.py`

```python
# L36-37 — _subscribers 和 _queues 被 publish/subscribe 并发访问
# 无 asyncio.Lock 保护，高并发下可能丢失消息或导致 KeyError

class InMemoryTransport(Transport):
    _subscribers: dict[str, list[asyncio.Queue]]  # ← 线程不安全
    _queues: dict[str, asyncio.Queue]              # ← 同上
```

**修复**: 增加 `_lock = asyncio.Lock()`，在 publish/subscribe/unsubscribe 时加锁。

### 🔴 Bug 2: Transport 接口签名不一致

**文件**: `aqap/transport/kafka_transport.py L87` vs `base.py L23`

```python
# base.py: publish(self, topic: str | Topic, message: Message) -> None
# kafka_transport.py: publish(self, topic: str, message: Message) -> None
#                                       ^^^^ 少了 | Topic
# subscribe() 同理 — 基类用 str|Topic, Kafka 实现用 str
# RabbitMQ 实现用 str|Topic — 三个实现三种签名
```

**修复**: 统一为 `topic: str`（Transport 层不应依赖 Topic 枚举）。

### 🟡 Bug 3: DLQ 重放丢失原始 source

**文件**: `aqap/agent/dlq_consumer.py L153`

```python
msg = Message(
    type=MessageType(original.get("type", "TASK_DISPATCH")),
    source="dlq-consumer",  # ← 硬编码！丢失原始 source
    ...
)
```

**影响**: 重放的消息看起来都来自 dlq-consumer，原始发送者信息丢失。

### 🟡 Bug 4: DLQ 死信索引无持久化

**文件**: `aqap/agent/dlq_consumer.py L104`

```python
self._dead_letters: list[dict[str, Any]] = []
```

**影响**: 进程重启后死信索引丢失，无法重放。仅能通过 Redis Stream PEL 恢复，但元数据丢失。

### 🟡 Bug 5: 幂等去重集合无过期

**文件**: `aqap/agent/base.py` (IDEMPOTENCY 相关)

```python
self._processed_ids: set[str] = set()
```

**影响**: 集合只裁剪大小（10000→5000），但没有时间维度。长期运行的 Agent 如果消息量不大但持续很久，仍可能收到 8 小时前的重复消息。

### 🟡 Bug 6: Heartbeat 连接未就绪时可能崩溃

**文件**: `aqap/agent/base.py` (heartbeat loop)

```python
async def _heartbeat_loop(self):
    while self._running:
        await asyncio.sleep(self._heartbeat_interval)
        await self._transport.publish(Topic.BROADCAST, hb_msg)  # ← 如果 transport 未连接？
```

**影响**: start() → create_task(heartbeat_loop) 与 connect() 是并发启动的。如果心跳在 connect 完成前执行，publish 可能抛异常。

### 🔵 Bug 7: test_extended.py 引用了未实现的模块

**文件**: `tests/test_extended.py`

```python
from aqap.core.state_machine import MessageStateMachine  # 不存在
from aqap.protocol_v2 import Envelope, SchemaEnvelope     # 不存在
```

**影响**: 该测试文件无法运行。这是前瞻性测试但尚未实现，应标记为 xfail 或移入 "future" 目录。

---

## 三、设计改进建议

### 3.1 必须做

1. **修复并发安全问题** — InMemoryTransport 加锁
2. **统一 Transport 接口签名** — 全部 `topic: str`
3. **修复 DLQ 重放 source** — 保存原始消息的所有元数据
4. **test_extended.py 标记 xfail** 或移到 examples/

### 3.2 建议做

5. **引入 Schema 契约** — 至少为 task/result/verdict 定义 JSON Schema
6. **密钥管理升级** — 环境变量 + 可选 Vault 集成
7. **心跳保护** — heartbeat loop 检查 transport 连接状态
8. **DLQ 索引持久化** — 写到 Redis 或 SQLite

### 3.3 锦上添花

9. **消息压缩** — body > 1KB 时自动 zlib
10. **Prometheus 指标** — 消息处理延迟、失败率、插件耗时
11. **健康检查端点** — HTTP health endpoint 用于 K8s probe
12. **多语言 SDK 规范** — 先出 TypeScript SDK

---

## 四、修复优先级

| 优先级 | 问题 | 影响范围 | 预估工时 |
|:------:|------|:--------:|:--------:|
| P0 | InMemory 竞态条件 | 测试偶尔失败 | 2 小时 |
| P0 | Transport 接口不一致 | 第三方 Transport 实现出错 | 1 小时 |
| P1 | DLQ 重放 source 丢失 | 运维可追溯性 | 1 小时 |
| P1 | test_extended 无法运行 | 开发体验 | 0.5 小时 |
| P2 | Schema 契约 | Agent 间合约 | 4 小时 |
| P2 | 密钥管理 | 安全合规 | 3 小时 |
| P3 | Prometheus 指标 | 可观测性 | 4 小时 |
| P3 | 多语言 SDK | 生态 | 40+ 小时 |
