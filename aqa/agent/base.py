"""Agent 基类 — 所有 AQA Agent 的通用骨架"""
from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from aqa.core.message import Message, MessageType, Topic, heartbeat
from aqa.plugin.registry import registry
from aqa.transport.base import Transport


class Agent(ABC):
    """
    Agent 抽象基类

    封装通用逻辑:
    - 消息订阅循环 (subscribe → handle_message → ack)
    - 心跳保活
    - 插件发现与执行
    - 优雅关闭
    """

    def __init__(self, agent_id: str, transport: Transport, group: str = "aqa-default"):
        self.agent_id = agent_id
        self._transport = transport
        self._group = group
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._topics: list[str | Topic] = []

    # ── 子类必须实现 ──

    @property
    @abstractmethod
    def agent_type(self) -> str:
        """Agent 类型标识 (如 probe, judge, reporter)"""

    @abstractmethod
    async def handle_message(self, message: Message) -> list[Message] | None:
        """
        处理收到的消息

        返回: 需要发送的回复消息列表 (或 None)
        """

    # ── 可重写的生命周期 ──

    async def on_start(self) -> None:
        """Agent 启动钩子"""
        pass

    async def on_stop(self) -> None:
        """Agent 停止钩子"""
        pass

    # ── 订阅管理 ──

    def subscribe_to(self, topic: str | Topic):
        """订阅主题"""
        self._topics.append(topic)

    # ── 生命周期控制 ──

    async def start(self):
        """启动 Agent"""
        if self._running:
            print(f"[{self.agent_id}] 已在运行")
            return

        self._running = True
        await self._transport.connect()
        await self.on_start()

        # 注册到系统
        await self._transport.publish(
            Topic.SYSTEM_EVENTS,
            Message(
                type=MessageType.REGISTER,
                source=self.agent_id,
                payload={"agent_type": self.agent_type},
            ),
        )

        # 启动心跳
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        # 启动消息消费
        for topic in self._topics:
            task = asyncio.create_task(self._consume_loop(topic))
            self._tasks.append(task)

        print(f"[{self.agent_id}] Agent 已启动, 订阅: {self._topics}")

    async def stop(self):
        """优雅停止 Agent"""
        self._running = False

        # 发送关闭消息
        await self._transport.publish(
            Topic.SYSTEM_EVENTS,
            Message(type=MessageType.SHUTDOWN, source=self.agent_id),
        )

        # 取消所有任务
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        await self.on_stop()
        await self._transport.disconnect()
        print(f"[{self.agent_id}] Agent 已停止")

    # ── 内部循环 ──

    async def _heartbeat_loop(self):
        """定期发送心跳"""
        while self._running:
            msg = heartbeat(self.agent_id)
            await self._transport.publish(Topic.SYSTEM_EVENTS, msg)
            await asyncio.sleep(30)

    async def _consume_loop(self, topic: str | Topic):
        """消费 topic 消息循环"""
        consumer_id = f"{self.agent_id}-{uuid.uuid4().hex[:6]}"
        async for message in self._transport.subscribe(
            topic, group=self._group, consumer=consumer_id
        ):
            if not self._running:
                break

            # 过滤: 忽略自己发出的消息 (避免回声)
            if message.source == self.agent_id:
                continue

            try:
                replies = await self.handle_message(message)
                if replies:
                    for reply in replies:
                        await self._transport.publish(
                            Topic.agent_inbox(reply.target or self.agent_id),
                            reply,
                        )
            except Exception as e:
                print(f"[{self.agent_id}] 处理消息异常: {e}")
                # 发送错误通知
                error_msg = Message(
                    type=MessageType.ERROR,
                    source=self.agent_id,
                    payload={"error": str(e), "original_trace_id": message.trace_id},
                    trace_id=message.trace_id,
                )
                await self._transport.publish(Topic.SYSTEM_EVENTS, error_msg)

            # ACK 消息
            await self._transport.ack(
                topic, message.headers.get("_redis_msg_id")
            )

    # ── 插件执行便捷方法 ──

    async def run_plugins(self, topic: str, context: dict) -> list[dict]:
        """执行绑定到指定 topic 的所有插件"""
        return await registry.execute_all(topic, context)

    # ── 发送消息 ──

    async def send(self, message: Message):
        """发送消息到目标 agent 的收件箱"""
        target = message.target
        if target:
            await self._transport.publish(Topic.agent_inbox(target), message)
        else:
            # 广播
            await self._transport.publish(Topic.BROADCAST, message)
