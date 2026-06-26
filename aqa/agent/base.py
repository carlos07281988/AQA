"""Agent 基类 — 所有 AQA Agent 的通用骨架

v2 改进:
  - 消息重试 + DLG 转发 (max_retries 配置)
  - 心跳广播到 BR0ADCAST + SYSTEM_EVENTS 双通道
  - 向插件 context 注入 _aqa_start_time/_aqa_trace_id 供 TraceCollector
  - 优雅关闭时 drain 当前处理的消息
  - _current_message 追踪: run_plugins 自动注入追踪上下文
  - 消息到达时 validate_message 校验
"""
from __future__ import annotations

import asyncio
import logging
import time as _time
import uuid
from abc import ABC, abstractmethod
from typing import Any

from aqa.core.dlq import DLQ_TOPIC, create_dlq_message
from aqa.core.message import (
    Message,
    MessageType,
    Topic,
    error_message,
    heartbeat,
    validate_message,
)
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
        # 最近一次收到的 message_id (用于 ack)
        self._last_topic: str = ""
        self._last_group: str = ""
        self._last_msg_id: str | None = None
        # 幂等去重 — 已处理消息 ID 缓存 (最多保留 10000 条)
        self._processed_ids: dict[str, float] = {}
        self._idempotency_max_size: int = 10000
        self._idempotency_ttl: int = 300  # 5 分钟后自动过期
        self._last_idempotency_cleanup: float = 0.0

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
        logger.info("[%s] on_start 完成", self.agent_id)

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
            Message(type=MessageType.SHUTDOWN, source=self.agent_id, payload={}),
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
            logger.debug("[%s] 心跳发送", self.agent_id)
            await asyncio.sleep(self._heartbeat_interval)

    async def _consume_loop(self, topic: str | Topic):
        consumer_id = f"{self.agent_id}-{uuid.uuid4().hex[:6]}"
        async for message in self._transport.subscribe(
            topic, group=self._group, consumer=consumer_id
        ):
            if not self._running:
                break

            logger.debug("[%s] 收到消息 trace_id=%s type=%s", self.agent_id, message.trace_id, message.type)

            # 记录当前 topic、group 和 transport 层 msg_id (用于 ack)
            self._last_topic = str(topic)
            self._last_group = self._group
            self._last_msg_id = getattr(message, "_transport_msg_id", None)

            # 忽略回声
            if message.source == self.agent_id:
                continue

            # 幂等去重 — 重复 message_id 直接跳过 (PROTOCOL.md §8.5)
            msg_id = message.message_id
            if msg_id in self._processed_ids and self._processed_ids[msg_id] > _time.time() - self._idempotency_ttl:
                logger.debug(
                    "[%s] 跳过重复消息 %s (trace=%s)",
                    self.agent_id, msg_id, message.trace_id,
                )
                continue
            self._processed_ids[msg_id] = _time.time()
            # 限制缓存大小
            if len(self._processed_ids) > self._idempotency_max_size:
                self._evict_stale_ids()

            # 校验消息格式 (PROTOCOL.md §1)
            raw = message.to_dict()
            validation_errors = validate_message(raw)
            if validation_errors:
                logger.warning(
                    "[%s] 消息校验失败 trace_id=%s: %s",
                    self.agent_id, message.trace_id, "; ".join(validation_errors),
                )
                # 校验失败 → ack 丢弃, 不重试
                continue

            # 未知类型 → 发 ERROR 消息并丢弃 (PROTOCOL.md §5.1)
            if message.type == MessageType.UNKNOWN:
                logger.warning(
                    "[%s] 收到未知类型消息 trace_id=%s, 发 ERROR",
                    self.agent_id, message.trace_id,
                )
                err = error_message(
                    source=self.agent_id,
                    code="UNKNOWN_TYPE",
                    message=f"不识别消息类型",
                    trace_id=message.trace_id,
                    original_message_id=message.message_id,
                )
                if message.source:
                    err.target = message.source
                await self._transport.publish(
                    Topic.agent_inbox(err.target) if err.target else Topic.BROADCAST,
                    err,
                )
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
                            else self._determine_reply_topic(message)
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
                should_ack = await self._handle_failure(message, str(e), str(topic))
                if should_ack:
                    self._last_msg_id = None  # 已在 _handle_failure 中 ACK，防止重复
            if self._last_msg_id:
                await self._transport.ack(topic, self._last_msg_id, self._last_group)

    def _determine_reply_topic(self, message: Message) -> str:
        """根据收到消息的 topic 决定回复发往哪个 topic"""
        topic_map = {
            Topic.AGENT_PROBE: Topic.AGENT_JUDGE,
            Topic.AGENT_JUDGE: Topic.AGENT_REPORTER,
            Topic.AGENT_REPORTER: Topic.BROADCAST,
        }
        return topic_map.get(message.topic, Topic.agent_inbox(message.source))

    def _evict_stale_ids(self) -> None:
        """清理过期的幂等去重记录"""
        now = _time.time()
        cutoff = now - self._idempotency_ttl
        stale = [mid for mid, ts in self._processed_ids.items() if ts < cutoff]
        for mid in stale:
            del self._processed_ids[mid]
        # 如果 TTL 清理后仍然超过上限, 按时间排序删除最旧的
        if len(self._processed_ids) > self._idempotency_max_size:
            sorted_ids = sorted(self._processed_ids.items(), key=lambda x: x[1])
            for mid, _ in sorted_ids[:len(sorted_ids) - self._idempotency_max_size // 2]:
                del self._processed_ids[mid]
        logger.debug("[%s] 幂等缓存清理: 移除 %d 条旧记录", self.agent_id, len(stale))
        self._last_idempotency_cleanup = _time.time()

    async def _handle_failure(self, message: Message, error: str, topic: str):
        '''处理消息处理失败。

        Returns:
            True  → 消息已转入 DLQ，调用方无需再次 ACK
            False → 消息将重试，调用方不应 ACK (留在 pending 队列)
        '''
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
            if self._last_msg_id:
                await self._transport.ack(topic, self._last_msg_id, self._last_group)
            return True
        else:
            logger.info(
                "[%s] 消息 %s 重试 %d/%d",
                self.agent_id,
                msg_key,
                retry_count,
                self._max_retries,
            )
            return False

    @staticmethod
    def _msg_key(message: Message) -> str:
        return f"{message.source}:{message.trace_id}:{message.type}"

    # ── 插件执行 ──

    async def run_plugins(self, topic: str, context: dict) -> list[dict]:
        """执行指定 topic 的所有插件, 自动注入追踪上下文"""
        ctx = context.copy() if context else {}
        if self._current_message:
            ctx["_aqa_start_time"] = _time.time()
            ctx["_aqa_trace_id"] = self._current_message.trace_id
            ctx["_aqa_message_type"] = str(self._current_message.type)
            ctx["_aqa_source"] = self._current_message.source
        return await registry.execute_all(topic, ctx)

    async def send(self, message: Message):
        if message.target:
            logger.debug("[%s] 发送 → %s (type=%s)", self.agent_id, message.target, message.type)
            await self._transport.publish(Topic.agent_inbox(message.target), message)
        else:
            logger.debug("[%s] 发送 → BROADCAST (type=%s)", self.agent_id, message.type)
            await self._transport.publish(Topic.BROADCAST, message)
