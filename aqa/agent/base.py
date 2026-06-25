"""Agent 基类 — 所有 AQA Agent 的通用骨架

v2 改进:
  - 消息重试 + DLQ 转发 (max_retries 配置)
  - 心跳广播到 BROADCAST + SYSTEM_EVENTS 双通道
  - 向插件 context 注入 _aqa_start_time/_aqa_trace_id 供 TraceCollector
  - 优雅关闭时 drain 当前处理的消息
  - _current_message 追踪: run_plugins 自动注入追踪上下文
"""
from __future__ import annotations

import asyncio
import logging
import time as _time
import uuid
from abc import ABC, abstractmethod
from typing import Any

from aqa.core.dlq import DLQ_TOPIC, create_dlq_message
from aqa.core.message import Message, MessageType, Topic, heartbeat
from aqa.core.security import PayloadCipher
from aqa.plugin.registry import registry
from aqa.transport.base import Transport

logger = logging.getLogger("aqa.agent")


class Agent(ABC):
    """Agent 抽象基类"""

    def __init__(
        self,
        agent_id: str,
        transport: Transport,
        group: str = "aqa-default",
        max_retries: int = 3,
        heartbeat_interval: int = 30,
        cipher: PayloadCipher | None = None,
    ):
        self.agent_id = agent_id
        self._transport = transport
        self._group = group
        self._max_retries = max_retries
        self._heartbeat_interval = heartbeat_interval
        self._cipher = cipher or PayloadCipher()
        self._running = False
        self._draining = False
        self._tasks: list[asyncio.Task] = []
        self._topics: list[str | Topic] = []
        self._retry_counts: dict[str, int] = {}
        # 当前正在处理的消息 (供 run_plugins 注入追踪上下文)
        self._current_message: Message | None = None

    @property
    @abstractmethod
    def agent_type(self) -> str:
        ...

    @abstractmethod
    async def handle_message(self, message: Message) -> list[Message] | None:
        ...

    async def on_start(self) -> None:
        """Agent 启动钩子"""

    async def on_stop(self) -> None:
        """Agent 停止钩子"""

    def subscribe_to(self, topic: str | Topic):
        self._topics.append(topic)

    async def start(self):
        """启动 Agent"""
        if self._running:
            logger.info("[%s] 已在运行", self.agent_id)
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

        # 心跳 (双通道)
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        # 消息消费
        for topic in self._topics:
            task = asyncio.create_task(self._consume_loop(topic))
            self._tasks.append(task)

        logger.info("[%s] Agent 已启动, 订阅: %s", self.agent_id, self._topics)

    async def stop(self):
        """优雅停止 Agent"""
        logger.info("[%s] 开始优雅关闭...", self.agent_id)
        self._running = False
        self._draining = True

        await self._transport.publish(
            Topic.SYSTEM_EVENTS,
            Message(type=MessageType.SHUTDOWN, source=self.agent_id),
        )

        # drain: 等待进行中的任务完成 (最多 5s)
        done, pending_ = await asyncio.wait(
            list(self._tasks), timeout=5,
            return_when=asyncio.ALL_COMPLETED,
        )

        for task in pending_:
            task.cancel()
        if pending_:
            await asyncio.gather(*pending_, return_exceptions=True)
        self._tasks.clear()

        self._draining = False
        await self.on_stop()
        await self._transport.disconnect()
        logger.info("[%s] Agent 已停止", self.agent_id)

    async def health_status(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "type": self.agent_type,
            "running": self._running,
            "topics": [str(t) for t in self._topics],
            "tasks": len(self._tasks),
            "retry_backlog": len(self._retry_counts),
        }

    # ── 内部循环 ──

    async def _heartbeat_loop(self):
        while self._running:
            msg = heartbeat(self.agent_id)
            await self._transport.publish(Topic.BROADCAST, msg)
            await self._transport.publish(Topic.SYSTEM_EVENTS, msg)
            await asyncio.sleep(self._heartbeat_interval)

    async def _consume_loop(self, topic: str | Topic):
        consumer_id = f"{self.agent_id}-{uuid.uuid4().hex[:6]}"
        async for message in self._transport.subscribe(
            topic, group=self._group, consumer=consumer_id
        ):
            if not self._running:
                break

            # 忽略回声
            if message.source == self.agent_id:
                continue

            # 解密 payload
            try:
                message.payload = self._cipher.decrypt_payload(message.payload)
            except Exception as e:
                logger.warning("[%s] payload 解密失败: %s", self.agent_id, e)

            try:
                # 设置当前消息 (供 run_plugins 注入追踪上下文)
                self._current_message = message
                replies = await self.handle_message(message)
                self._current_message = None

                if replies:
                    for reply in replies:
                        topic_to_use = (
                            Topic.agent_inbox(reply.target)
                            if reply.target
                            else Topic.agent_inbox(self.agent_id)
                        )
                        reply.payload = self._cipher.encrypt_payload(reply.payload)
                        await self._transport.publish(topic_to_use, reply)

                # 成功 → 清除重试计数
                msg_key = self._msg_key(message)
                self._retry_counts.pop(msg_key, None)

            except Exception as e:
                logger.error(
                    "[%s] 处理消息异常 trace_id=%s: %s",
                    self.agent_id,
                    message.trace_id,
                    e,
                )
                self._current_message = None
                await self._handle_failure(message, str(e))

            # ACK — getattr 兼容无 headers 字段的 Message
            msg_id = getattr(message, "headers", {}).get("_redis_msg_id")
            await self._transport.ack(topic, msg_id)

    async def _handle_failure(self, message: Message, error: str):
        msg_key = self._msg_key(message)
        retry_count = self._retry_counts.get(msg_key, 0) + 1
        self._retry_counts[msg_key] = retry_count

        if retry_count >= self._max_retries:
            dlq_record = create_dlq_message(
                original=message.to_dict(),
                error=error,
                retry_count=retry_count,
                max_retries=self._max_retries,
                failed_by=self.agent_id,
            )
            await self._transport.publish(
                DLQ_TOPIC,
                Message(
                    type=MessageType.ERROR,
                    source=self.agent_id,
                    trace_id=message.trace_id,
                    payload=dlq_record.to_dict(),
                ),
            )
            logger.warning(
                "[%s] 消息 %s 已达最大重试 %d, 转发 DLQ",
                self.agent_id,
                msg_key,
                self._max_retries,
            )
            self._retry_counts.pop(msg_key, None)
            # 不 ACK → 消息留 pending, 但已进死信, 手动 ACK 防止积累
            # 注意: 这里需要 ACK 否则 pending 列表会无限增长
            msg_id = getattr(message, "headers", {}).get("_redis_msg_id")
            if msg_id:
                # 需要知道 topic, 但这里不知道... 由 _consume_loop 处理
                pass
        else:
            logger.info(
                "[%s] 消息 %s 重试 %d/%d",
                self.agent_id,
                msg_key,
                retry_count,
                self._max_retries,
            )

    @staticmethod
    def _msg_key(message: Message) -> str:
        return f"{message.source}:{message.trace_id}:{message.type}"

    # ── 插件执行 ──

    async def run_plugins(self, topic: str, context: dict) -> list[dict]:
        """执行指定 topic 的所有插件, 自动注入追踪上下文"""
        if self._current_message:
            ctx = context.copy() if context else {}
            ctx["_aqa_start_time"] = _time.time()
            ctx["_aqa_trace_id"] = self._current_message.trace_id
            ctx["_aqa_message_type"] = str(self._current_message.type)
            ctx["_aqa_source"] = self._current_message.source
            return await registry.execute_all(topic, ctx)
        return await registry.execute_all(topic, context)

    async def send(self, message: Message):
        if message.target:
            await self._transport.publish(Topic.agent_inbox(message.target), message)
        else:
            await self._transport.publish(Topic.BROADCAST, message)
