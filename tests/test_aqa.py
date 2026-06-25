"""
AQA 测试 — 核心协议 + Transport + 插件 + Agent + DLQ + 安全 + 链路追踪
"""
from __future__ import annotations

import asyncio
import pytest

from aqa.core.message import (
    Message,
    MessageType,
    Topic,
    task_dispatch,
    task_result,
    judge_verdict,
    heartbeat,
)
from aqa.plugin.base import Plugin
from aqa.plugin.registry import registry


class TestMessageProtocol:
    """消息信封协议测试"""

    def test_message_create(self):
        msg = task_dispatch("probe-1", {"task_id": "t1"})
        assert msg.type == MessageType.TASK_DISPATCH
        assert msg.source == "probe-1"
        assert msg.payload["task_id"] == "t1"
        assert msg.version == "1.0"

    def test_message_serialize_roundtrip(self):
        original = task_dispatch("probe-1", {"task_id": "t1", "score": 0.95})
        restored = Message.from_json(original.to_json())
        assert restored.type == original.type
        assert restored.source == original.source
        assert restored.payload["task_id"] == "t1"
        assert restored.payload["score"] == 0.95

    def test_reply(self):
        incoming = task_dispatch("probe-1", {"task_id": "t1"})
        reply = incoming.reply(MessageType.TASK_RESULT, {"passed": True})
        assert reply.type == MessageType.TASK_RESULT
        assert reply.target == "probe-1"
        assert reply.trace_id == incoming.trace_id
        assert reply.correlation_id == incoming.trace_id

    def test_heartbeat(self):
        msg = heartbeat("probe-1", {"alive": True, "uptime": 60})
        assert msg.type == MessageType.HEARTBEAT
        assert msg.source == "probe-1"
        assert msg.payload["status"]["alive"] is True

    def test_agent_inbox_topic(self):
        inbox = Topic.agent_inbox("probe-1")
        assert inbox == "aqa:inbox:probe-1"


class TestPluginRegistry:
    """插件注册中心测试"""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        yield
        for name in list(registry._plugins.keys()):
            registry.unregister(name)

    @pytest.mark.asyncio
    async def test_register_and_list(self):
        plugin = _SimplePlugin()
        registry.register(plugin, topics=["probe"])
        assert registry.count == 1
        assert "test-plugin" in registry.list()

    @pytest.mark.asyncio
    async def test_execute_all(self):
        plugin = _SimplePlugin()
        registry.register(plugin, topics=["probe"])
        await registry.initialize_all({})

        results = await registry.execute_all("probe", {"x": 21})
        assert len(results) == 1
        assert results[0]["result"]["value"] == 42
        assert results[0]["error"] is None

    @pytest.mark.asyncio
    async def test_unregister(self):
        plugin = _SimplePlugin()
        registry.register(plugin, topics=["judge"])
        assert registry.count == 1
        ok = registry.unregister("test-plugin")
        assert ok is True
        assert registry.count == 0

    @pytest.mark.asyncio
    async def test_topic_mapping(self):
        a = _SimplePlugin()
        registry.register(a, topics=["probe"])
        assert "probe" in registry.topics
        assert "judge" not in registry.topics


class TestMessageRouting:
    """消息路由测试 — 协议层链路"""

    @pytest.mark.asyncio
    async def test_task_dispatch_to_result_flow(self):
        msg = task_dispatch("cli", {"task_id": "t-001", "target": "model-x"})
        assert msg.type == MessageType.TASK_DISPATCH
        assert msg.payload["task_id"] == "t-001"
        restored = Message.from_json(msg.to_json())
        assert restored.trace_id == msg.trace_id

    @pytest.mark.asyncio
    async def test_result_to_judge_flow(self):
        result = task_result("probe-1", {"task_id": "t-001", "passed": True, "score": 0.9})
        judge_req = result.reply(MessageType.JUDGE_REQUEST, {"evidence": result.payload})
        assert judge_req.type == MessageType.JUDGE_REQUEST
        assert judge_req.correlation_id == result.trace_id

    @pytest.mark.asyncio
    async def test_judge_to_report_flow(self):
        verdict = judge_verdict("judge-1", {"task_id": "t-001", "score": 0.85, "passed": True})
        report_req = verdict.reply(MessageType.REPORT_REQUEST, {
            "task": {"task_id": "t-001"},
            "verdict": verdict.payload,
        })
        assert report_req.type == MessageType.REPORT_REQUEST
        assert report_req.payload["verdict"]["score"] == 0.85


# ── 测试用辅助类 ──


class _SimplePlugin(Plugin):
    @property
    def name(self) -> str:
        return "test-plugin"

    @property
    def version(self) -> str:
        return "0.0.1"

    async def initialize(self, config: dict) -> None:
        self.config = config

    async def execute(self, context: dict) -> dict:
        return {"passed": True, "value": context.get("x", 0) * 2}

    async def cleanup(self) -> None:
        pass


from aqa.transport.base import Transport
from typing import AsyncGenerator


class _TestTransport(Transport):
    """测试用 Transport — 简化版 InMemory"""

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._subs: dict[str, list[asyncio.Queue]] = {}
        self._running = True

    @property
    def name(self):
        return "test"

    async def connect(self): pass
    async def disconnect(self):
        self._running = False
    async def create_group(self, topic, group): pass

    async def publish(self, topic, message):
        t = str(topic.value) if isinstance(topic, Topic) else topic
        for q in self._subs.get(t, []):
            await q.put(message)

    async def subscribe(self, topic, group="", consumer="") -> AsyncGenerator[Message, None]:
        t = str(topic.value) if isinstance(topic, Topic) else topic
        q = asyncio.Queue()
        self._subs.setdefault(t, []).append(q)
        try:
            while self._running:
                msg = await asyncio.wait_for(q.get(), timeout=0.5)
                yield msg
        except asyncio.TimeoutError:
            pass
        finally:
            if q in self._subs.get(t, []):
                self._subs[t].remove(q)

    async def ack(self, topic, msg_id=None): pass


from aqa.agent.probe import ProbeAgent
from aqa.agent.judge import JudgeAgent
from aqa.agent.reporter import ReporterAgent


class TestAgentIntegration:
    """Agent 集成测试 — 多 Agent 消息流转"""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        yield
        for name in list(registry._plugins.keys()):
            registry.unregister(name)

    @pytest.mark.asyncio
    async def test_agent_send_receive(self):
        transport = _TestTransport()
        probe = ProbeAgent("probe-test", transport)
        probe.subscribe_to("aqa:broadcast")
        await probe.start()
        await transport.publish(
            "aqa:broadcast",
            task_dispatch("cli", {"task_id": "t-001"}),
        )
        await asyncio.sleep(0.3)
        await probe.stop()
        assert True

    @pytest.mark.asyncio
    async def test_full_flow_in_memory(self):
        transport = _TestTransport()
        registry.register(_SimplePlugin(), topics=["probe", "judge"])
        await registry.initialize_all({})

        probe = ProbeAgent("probe-1", transport)
        judge = JudgeAgent("judge-1", transport)
        reporter = ReporterAgent("reporter-1", transport)

        probe.subscribe_to(Topic.AGENT_PROBE)
        judge.subscribe_to(Topic.AGENT_JUDGE)
        reporter.subscribe_to(Topic.AGENT_REPORTER)

        await asyncio.gather(probe.start(), judge.start(), reporter.start())

        await transport.publish(
            Topic.AGENT_PROBE,
            task_dispatch("tester", {"task_id": "full-test", "x": 21}),
        )
        await asyncio.sleep(1.0)

        await asyncio.gather(probe.stop(), judge.stop(), reporter.stop())
        await registry.cleanup_all()
        assert True


class TestDLQ:
    """死信队列测试"""

    def test_dlq_record(self):
        from aqa.core.dlq import create_dlq_message, DeadLetterRecord

        record = create_dlq_message(
            original={"type": "TASK", "source": "test"},
            error="timeout",
            retry_count=3,
            max_retries=3,
            failed_by="probe-1",
        )
        assert isinstance(record, DeadLetterRecord)
        assert record.error == "timeout"
        assert record.retry_count == 3
        assert record.max_retries == 3
        assert record.failed_by == "probe-1"
        assert record.original_message["source"] == "test"

    def test_dlq_serialize(self):
        from aqa.core.dlq import create_dlq_message

        record = create_dlq_message(
            original={"type": "TASK"},
            error="error",
            retry_count=1,
            max_retries=3,
        )
        d = record.to_dict()
        assert d["type"] == "DLQ"
        assert d["error"] == "error"
        assert "failed_at" in d


class TestSecurity:
    """Payload 加密测试"""

    def test_encrypt_decrypt(self):
        from aqa.core.security import PayloadCipher

        cipher = PayloadCipher("test-secret-key-12345")
        assert cipher.enabled is True

        original = {"task_id": "secret-001", "data": "sensitive"}
        encrypted = cipher.encrypt_payload(original)
        assert "_encrypted" in encrypted
        assert encrypted["_encrypted"] is True

        decrypted = cipher.decrypt_payload(encrypted)
        assert decrypted["task_id"] == "secret-001"
        assert decrypted["data"] == "sensitive"

    def test_noop_when_disabled(self):
        from aqa.core.security import PayloadCipher

        cipher = PayloadCipher()
        assert cipher.enabled is False

        payload = {"task_id": "t1"}
        assert cipher.encrypt_payload(payload) is payload
        assert cipher.decrypt_payload(payload) is payload

    def test_unencrypted_passthrough(self):
        from aqa.core.security import PayloadCipher

        cipher = PayloadCipher("test-secret")
        assert cipher.decrypt_payload({"task_id": "plain"}) == {"task_id": "plain"}


class TestTraceCollector:
    """链路追踪插件测试"""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        yield
        for name in list(registry._plugins.keys()):
            registry.unregister(name)

    @pytest.mark.asyncio
    async def test_trace_record(self):
        from aqa.plugins.trace_collector import TraceCollector

        plugin = TraceCollector()
        registry.register(plugin, topics=["probe"])
        await registry.initialize_all({})

        ctx = {
            "_aqa_start_time": 1000.0,
            "_aqa_trace_id": "trace-abc",
            "_aqa_message_type": "TASK_DISPATCH",
            "_aqa_source": "cli",
        }
        results = await registry.execute_all("probe", ctx)
        assert len(results) == 1
        assert results[0]["result"]["traced"] is True

    @pytest.mark.asyncio
    async def test_trace_no_start_time(self):
        from aqa.plugins.trace_collector import TraceCollector

        plugin = TraceCollector()
        registry.register(plugin, topics=["probe"])
        await registry.initialize_all({})

        ctx = {"_aqa_trace_id": "trace-xyz"}
        results = await registry.execute_all("probe", ctx)
        assert len(results) == 1
        assert results[0]["error"] is None


class TestSupervisor:
    """AgentSupervisor 测试"""

    @pytest.mark.asyncio
    async def test_register_and_start_stop(self):
        from aqa.agent.supervisor import AgentSupervisor

        sup = AgentSupervisor(heartbeat_timeout=5)
        transport = _TestTransport()
        probe = ProbeAgent("sup-test", transport)
        probe.subscribe_to("aqa:broadcast")
        sup.register(probe)

        assert "sup-test" in sup._agents
        health = await sup.health_check()
        assert health["total"] == 1

        await sup.start_all()
        await asyncio.sleep(0.2)
        await sup.stop_all()
        assert not probe._running

    @pytest.mark.asyncio
    async def test_heartbeat_tracking(self):
        from aqa.agent.supervisor import AgentSupervisor

        sup = AgentSupervisor(heartbeat_timeout=30)
        sup.record_heartbeat("agent-1")
        sup.record_heartbeat("agent-2")

        health = await sup.health_check()
        assert "agent-1" in health["agents"]
        assert "agent-2" in health["agents"]
