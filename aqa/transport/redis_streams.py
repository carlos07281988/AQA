"""Redis Streams Transport 实现"""
from __future__ import annotations

import json
from typing import AsyncGenerator, Optional

from aqa.transport.base import Transport
from aqa.core.message import Message, Topic


class RedisStreamsTransport(Transport):
    """
    Redis Streams 传输层

    利用 Redis Stream 的消费者组实现可靠消息投递:
    - XADD → XREADGROUP → XACK 模式
    - 每个 Agent 属于一个 consumer group
    - 支持故障转移: 挂掉的 consumer 未 ACK 的消息会被重新投递
    """

    def __init__(self, redis_url: str = "redis://127.0.0.1:6379/0"):
        self.redis_url = redis_url
        self._redis = None
        self._groups_created: set[str] = set()

    @property
    def name(self) -> str:
        return "redis-streams"

    async def _get_redis(self):
        if self._redis is None:
            import redis.asyncio as aioredis
            self._redis = aioredis.Redis.from_url(
                self.redis_url, decode_responses=True
            )
        return self._redis

    async def connect(self):
        r = await self._get_redis()
        await r.ping()
        print(f"[transport] Redis Streams 已连接: {self.redis_url}")

    async def disconnect(self):
        if self._redis:
            await self._redis.close()
            self._redis = None
            print("[transport] Redis Streams 已断开")

    async def create_group(self, topic: str | Topic, group: str):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic

        if topic_str in self._groups_created:
            return

        r = await self._get_redis()
        try:
            # MKSTREAM: 如果 stream 不存在则自动创建
            await r.xgroup_create(topic_str, group, mkstream=True)
        except Exception as e:
            # BUSYGROUP: group 已存在, 静默跳过
            if "BUSYGROUP" not in str(e):
                print(f"[transport] WARN 创建 group {group}@{topic_str}: {e}")
        self._groups_created.add(topic_str)

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        r = await self._get_redis()

        fields = message.to_dict()
        # 将 dict 展平为 field-value pairs (Redis Stream field 必须是字符串)
        flat: list[str] = []
        for k, v in fields.items():
            flat.append(k)
            flat.append(json.dumps(v, ensure_ascii=False) if not isinstance(v, str) else v)

        message_id = await r.xadd(topic_str, fields=flat, maxlen=10000)
        return message_id

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqa-default",
        consumer: str = "",
    ) -> "AsyncGenerator[Message, None]":
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        # 确保 consumer group 存在
        await self.create_group(topic_str, group)

        r = await self._get_redis()
        import asyncio

        while True:
            try:
                # XREADGROUP: 阻塞读取新消息, timeout=3s
                results = await r.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams={topic_str: ">"},
                    count=10,
                    block=3000,
                )
                if not results:
                    await asyncio.sleep(0.1)
                    continue

                # results: [(stream_name, [(message_id, {field: value}), ...])]
                for stream_name, messages in results:
                    for msg_id, fields in messages:
                        try:
                            # 将扁平字段还原为 dict
                            raw = {}
                            for k, v in fields.items():
                                try:
                                    raw[k] = json.loads(v)
                                except (json.JSONDecodeError, TypeError):
                                    raw[k] = v
                            message = Message.from_dict(raw)
                            # 携带 message_id 用于 ack
                            message.headers["_redis_msg_id"] = msg_id
                            message.headers["_redis_stream"] = stream_name
                            yield message
                        except Exception as e:
                            print(f"[transport] 消息解析失败 ({msg_id}): {e}")
                            # 解析失败的消息也要 ACK 掉避免阻塞
                            await self.ack(topic_str, msg_id)

            except Exception as e:
                print(f"[transport] XREADGROUP 错误: {e}")
                await asyncio.sleep(1)

    async def ack(self, topic: str | Topic, message_id: str | None = None):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        if message_id:
            r = await self._get_redis()
            await r.xack(topic_str, "aqa-default", message_id)

    async def ack_message(self, message: Message):
        """便捷方法: 从消息 header 中提取 stream/message_id 并 ACK"""
        stream = message.headers.get("_redis_stream")
        msg_id = message.headers.get("_redis_msg_id")
        if stream and msg_id:
            await self.ack(stream, msg_id)
