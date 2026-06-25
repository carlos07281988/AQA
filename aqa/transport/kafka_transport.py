"""
AQA Kafka Transport — 完整实现

依赖: aiokafka>=0.10.0
使用前需安装: pip install aiokafka
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from aqa.transport.base import Transport
from aqa.core.message import Message

logger = logging.getLogger("aqa.transport.kafka")

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaTransport(Transport):
    """Apache Kafka 后端实现

    与 Redis Streams Transport 接口完全一致, 切换后端不改 Agent 代码。

    使用要求:
        pip install aiokafka
        config.yaml → transport.backend: kafka
        kafka_servers: "127.0.0.1:9092"
    """

    def __init__(
        self,
        servers: str = "127.0.0.1:9092",
        client_id: str = "aqa-kafka",
    ):
        if not KAFKA_AVAILABLE:
            raise ImportError(
                "KafkaTransport 需要 aiokafka, 请执行: pip install aiokafka"
            )

        self._servers = servers
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None
        self._consumers: dict[str, AIOKafkaConsumer] = {}
        self._running = False

    async def connect(self) -> None:
        """建立 Kafka 连接 (创建生产者)"""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._servers,
            client_id=self._client_id,
            acks="all",
            compression_type="gzip",
        )
        await self._producer.start()
        self._running = True
        logger.info("[kafka] 已连接 %s", self._servers)

    async def disconnect(self) -> None:
        """关闭所有连接"""
        self._running = False
        for topic, consumer in self._consumers.items():
            await consumer.stop()
        self._consumers.clear()
        if self._producer:
            await self._producer.stop()
        logger.info("[kafka] 已断开")

    async def publish(self, topic: str, message: Message) -> None:
        """发布消息到 Kafka topic"""
        if not self._producer:
            raise RuntimeError("Kafka 未连接, 请先调用 connect()")
        payload = message.to_json().encode("utf-8")
        await self._producer.send_and_wait(topic, payload)

    def subscribe(
        self, topic: str, group: str = "aqa-default", consumer: str = ""
    ) -> AsyncGenerator[Message, None]:
        """订阅 Kafka topic (返回异步生成器)"""
        return self._subscribe_loop(topic, group, consumer)

    async def _subscribe_loop(
        self, topic: str, group: str, consumer_id: str
    ) -> AsyncGenerator[Message, None]:
        """内部消费循环"""
        kafka_consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._servers,
            group_id=group,
            client_id=consumer_id or f"{self._client_id}-{topic}",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
        )
        self._consumers[topic] = kafka_consumer
        await kafka_consumer.start()

        try:
            async for msg in kafka_consumer:
                if not self._running:
                    break
                try:
                    decoded = json.loads(msg.value.decode("utf-8"))
                    yield Message.from_dict(decoded)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning("[kafka] 消息解析失败 topic=%s: %s", topic, e)
        finally:
            if topic in self._consumers:
                del self._consumers[topic]
            await kafka_consumer.stop()

    async def ack(self, topic: str, message_id: str | None = None) -> None:
        """Kafka 使用自动提交偏移量, 无需手动 ACK"""
        pass

    async def create_group(self, topic: str, group: str) -> None:
        """Kafka 消费组自动创建, 无需手动"""
        pass
