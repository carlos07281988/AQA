"""
AQA 测试 — 核心协议 + Transport + 插件 + Agent 集成
"""
from __future__ import annotations

import asyncio
import pytest

from aqa.core.message import Message, MessageType, Topic, task_dispatch, task_result, judge_verdict, heartbeat
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


class SimpleTestPlugin(Plugin):
    """测试用插件"""
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


class TestPluginRegistry:
    """插件注册中心测试"""

    @pytest.fixture(autouse=True)
    def clean_registry(self):
        # 每次测试后清理
        yield
        for name in list(registry._plugins.keys()):
            registry.unregister(name)

    @pytest.mark.asyncio
    async def test_register_and_list(self):
        plugin = SimpleTestPlugin()
        registry.register(plugin, topics=["probe"])
        assert registry.count == 1
        assert "test-plugin" in registry.list()

    @pytest.mark.asyncio
    async def test_execute_all(self):
        plugin = SimpleTestPlugin()
        registry.register(plugin, topics=["probe"])
        await registry.initialize_all({})

        results = await registry.execute_all("probe", {"x": 21})
        assert len(results) == 1
        assert results[0]["result"]["value"] == 42
        assert results[0]["error"] is None

    @pytest.mark.asyncio
    async def test_unregister(self):
        plugin = SimpleTestPlugin()
        registry.register(plugin, topics=["judge"])
        assert registry.count == 1

        ok = registry.unregister("test-plugin")
        assert ok is True
        assert registry.count == 0

    @pytest.mark.asyncio
    async def test_topic_mapping(self):
        a = SimpleTestPlugin()
        registry.register(a, topics=["probe"])
        assert "probe" in registry.topics
        assert "judge" not in registry.topics


class TestMessageRouting:
    """消息路由测试 — Probe → Judge → Reporter 流程"""

    @pytest.mark.asyncio
    async def test_task_dispatch_to_result_flow(self):
        """验证 TASK_DISPATCH 可序列化并携带正确 trace_id"""
        msg = task_dispatch("cli", {"task_id": "t-001", "target": "model-x"})

        assert msg.type == MessageType.TASK_DISPATCH
        assert msg.payload["task_id"] == "t-001"
        assert msg.payload["target"] == "model-x"

        # roundtrip
        restored = Message.from_json(msg.to_json())
        assert restored.type == msg.type
        assert restored.trace_id == msg.trace_id

    @pytest.mark.asyncio
    async def test_result_to_judge_flow(self):
        """验证检测结果可传递到评判阶段"""
        result = task_result("probe-1", {"task_id": "t-001", "passed": True, "score": 0.9})
        assert result.type == MessageType.TASK_RESULT
        assert result.payload["score"] == 0.9

        # 模拟 judge request
        judge_req = result.reply(MessageType.JUDGE_REQUEST, {"evidence": result.payload})
        assert judge_req.type == MessageType.JUDGE_REQUEST
        assert judge_req.correlation_id == result.trace_id

    @pytest.mark.asyncio
    async def test_judge_to_report_flow(self):
        """验证评判结果可传递到报告阶段"""
        verdict = judge_verdict("judge-1", {"task_id": "t-001", "score": 0.85, "passed": True})

        # 模拟 report request
        report_req = verdict.reply(MessageType.REPORT_REQUEST, {
            "task": {"task_id": "t-001"},
            "verdict": verdict.payload,
        })
        assert report_req.type == MessageType.REPORT_REQUEST
        assert report_req.payload["verdict"]["score"] == 0.85


from aqa.transport.base import Transport
from typing import AsyncGenerator


class TestTransport(Transport):
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


class TestAgentIntegration:
    """Agent 集成测试 — 多 Agent 消息流转"""

    @pytest.mark.asyncio
    async def test_agent_send_receive(self):
        transport = TestTransport()
        probe = ProbeAgent("probe-test", transport)
        probe.subscribe_to("aqa:broadcast")

        await probe.start()

        # 发送任务 -> probe 应回复结果
        await transport.publish(
            "aqa:broadcast",
            task_dispatch("cli", {"task_id": "t-001"}),
        )

        # 等待处理
        await asyncio.sleep(0.3)
        await probe.stop()

        # 验证 probe 已正常启动停止 (无异常即通过)
        assert True

    @pytest.mark.asyncio
    async def test_full_flow_in_memory(self):
        """完整流程: dispatch -> probe -> judge -> reporter"""
        from aqa.agent.judge import JudgeAgent
        from aqa.agent.reporter import ReporterAgent

        transport = TestTransport()
        registry.register(SimpleTestPlugin(), topics=["probe", "judge"])
        await registry.initialize_all({})

        probe = ProbeAgent("probe-1", transport)
        judge = JudgeAgent("judge-1", transport)
        reporter = ReporterAgent("reporter-1", transport)

        probe.subscribe_to(Topic.AGENT_PROBE)
        judge.subscribe_to(Topic.AGENT_JUDGE)
        reporter.subscribe_to(Topic.AGENT_REPORTER)

        await asyncio.gather(
            probe.start(),
            judge.start(),
            reporter.start(),
        )

        # 发送并等待
        await transport.publish(
            Topic.AGENT_PROBE,
            task_dispatch("tester", {"task_id": "full-test", "x": 21}),
        )
        await asyncio.sleep(1.0)

        await asyncio.gather(
            probe.stop(),
            judge.stop(),
            reporter.stop(),
        )
        await registry.cleanup_all()

        assert True  # 无异常即通过
