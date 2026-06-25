"""Kafka Transport 实现 (占位) — 实现 Transport 抽象接口"""
from __future__ import annotations

import json
from typing import AsyncGenerator

from aqa.transport.base import Transport
from aqa.core.message import Message, Topic


class KafkaTransport(Transport):
    """
    Kafka 传输层

    依赖 aiokafka 库:
    pip install aiokafka
    """

    def __init__(self, bootstrap_servers: str = "127.0.0.1:9092"):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None
        self._consumer = None

    @property
    def name(self) -> str:
        return "kafka"

    async def connect(self):
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        )
        await self._producer.start()
        print(f"[transport] Kafka 已连接: {self.bootstrap_servers}")

    async def disconnect(self):
        if self._producer:
            await self._producer.stop()
        if self._consumer:
            await self._consumer.stop()
        print("[transport] Kafka 已断开")

    async def create_group(self, topic: str | Topic, group: str):
        # Kafka consumer group 是消费者端概念, broker 端无需预创建
        pass

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        if self._producer:
            await self._producer.send(topic_str, value=message.to_dict())

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqa-default",
        consumer: str = "",
    ) -> "AsyncGenerator[Message, None]":
        from aiokafka import AIOKafkaConsumer

        topic_str = str(topic.value) if isinstance(topic, Topic) else topic

        self._consumer = AIOKafkaConsumer(
            topic_str,
            group_id=group,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
        )
        await self._consumer.start()

        try:
            async for msg in self._consumer:
                message = Message.from_dict(msg.value)
                message.headers["_kafka_offset"] = str(msg.offset)
                message.headers["_kafka_partition"] = str(msg.partition)
                yield message
        finally:
            pass  # consumer stop 在 disconnect 中完成

    async def ack(self, topic: str | Topic, message_id: str | None = None):
        # Kafka: enable_auto_commit 会自动提交 offset
        pass
