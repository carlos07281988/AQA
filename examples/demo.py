"""
AQA 演示 — 使用 InMemoryTransport 模拟完整检测流程
无需 Redis / Kafka, 纯内存验证架构
"""
from __future__ import annotations

import asyncio
from typing import AsyncGenerator

from aqa.core.message import Message, MessageType, Topic, task_dispatch, heartbeat
from aqa.transport.base import Transport


class InMemoryTransport(Transport):
    """
    内存 Transport — 用于测试和演示

    使用 asyncio.Queue 模拟消息队列,
    不需要 Redis / Kafka 即可验证架构
    """

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._running = True

    @property
    def name(self) -> str:
        return "in-memory"

    async def connect(self):
        print("[transport] InMemory 已就绪")

    async def disconnect(self):
        self._running = False
        print("[transport] InMemory 已关闭")

    async def create_group(self, topic: str | Topic, group: str):
        pass

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        if topic_str not in self._subscribers:
            return
        for q in self._subscribers[topic_str]:
            await q.put(message)

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqa-default",
        consumer: str = "",
    ) -> AsyncGenerator[Message, None]:
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(topic_str, []).append(q)

        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield message
                except asyncio.TimeoutError:
                    continue
        finally:
            subs = self._subscribers.get(topic_str, [])
            if q in subs:
                subs.remove(q)

    async def ack(self, topic: str | Topic, message_id: str | None = None):
        pass  # InMemory 无需 ACK


# ── 编写一个简单的自定义插件用于演示 ──
from aqa.plugin.base import Plugin


class DemoCheckPlugin(Plugin):
    """演示用检测插件 — 总是返回通过但附带虚假数据"""

    @property
    def name(self) -> str:
        return "demo-check"

    @property
    def version(self) -> str:
        return "1.0.0"

    async def initialize(self, config: dict) -> None:
        self.threshold = config.get("threshold", 0.5)

    async def execute(self, context: dict) -> dict:
        return {
            "passed": True,
            "score": 0.95,
            "message": f"Demo 检测通过 (threshold={self.threshold})",
        }

    async def cleanup(self) -> None:
        pass


from aqa.plugin.registry import registry
from aqa.agent.probe import ProbeAgent
from aqa.agent.judge import JudgeAgent
from aqa.agent.reporter import ReporterAgent


async def main():
    print("=" * 50)
    print("AQA — Agent Quality Assurance Demo")
    print("Transport: InMemory (无需 Redis)")
    print("=" * 50)

    # 初始化 Transport
    transport = InMemoryTransport()

    # 注册插件
    demo_plugin = DemoCheckPlugin()
    registry.register(demo_plugin, topics=["probe", "judge"])
    await registry.initialize_all({})

    # 创建 Agent
    probe = ProbeAgent("probe-1", transport)
    judge = JudgeAgent("judge-1", transport)
    reporter = ReporterAgent("reporter-1", transport)

    # 订阅 Topic
    probe.subscribe_to("aqa:broadcast")
    probe.subscribe_to(Topic.AGENT_PROBE)
    judge.subscribe_to(Topic.AGENT_JUDGE)
    reporter.subscribe_to(Topic.AGENT_REPORTER)

    try:
        # 启动所有 Agent
        await asyncio.gather(
            probe.start(),
            judge.start(),
            reporter.start(),
        )

        print("\n▶ 发送检测任务...")

        # 发送检测任务
        await transport.publish(
            Topic.AGENT_PROBE,
            task_dispatch(
                "demo-cli",
                {
                    "task_id": "demo-001",
                    "name": "Demo 质量检测",
                    "target": "ai-model-v1",
                    "required_fields": ["task_id", "passed", "plugin_results"],
                },
            ),
        )

        # 等待消息流转完成
        await asyncio.sleep(2.0)

        print("\n▶ 检查注册的插件...")
        print(f"  插件数: {registry.count}")
        for name, ver in registry.list().items():
            print(f"  - {name} v{ver}")

        print(f"\n  已订阅 Topic: {transport._subscribers.keys()}")

    finally:
        print("\n▶ 停止 Agent...")
        await asyncio.gather(
            probe.stop(),
            judge.stop(),
            reporter.stop(),
        )
        await registry.cleanup_all()

    print("\n✅ Demo 完成")


if __name__ == "__main__":
    asyncio.run(main())
