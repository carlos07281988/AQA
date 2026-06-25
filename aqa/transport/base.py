"""Transport 抽象接口 — 所有队列后端的统一契约"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncGenerator, Callable

from aqa.core.message import Message, Topic


class Transport(ABC):
    """
    Transport 抽象基类

    所有消息队列后端 (Redis Streams, Kafka, RabbitMQ, InMemory)
    必须实现此接口，通过 config 切换。
    """

    @abstractmethod
    async def connect(self):
        """建立连接"""

    @abstractmethod
    async def disconnect(self):
        """关闭连接"""

    @abstractmethod
    async def publish(self, topic: str | Topic, message: Message):
        """
        发布消息到指定 topic
        - Redis Streams: XADD stream_key *
        - Kafka: produce(topic, message.to_json())
        """

    @abstractmethod
    async def subscribe(
        self, topic: str | Topic, group: str = "aqa-default", consumer: str = ""
    ) -> AsyncGenerator[Message, None]:
        """
        订阅 topic 并持续消费消息 (异步生成器)

        Redis Streams: XREADGROUP GROUP group consumer BLOCK STREAMS key >
        Kafka: consume(topic, group_id=group)
        """

    @abstractmethod
    async def ack(self, topic: str | Topic, message_id: str | None = None):
        """
        确认消息已处理 (Redis Streams XACK / Kafka commit)

        对于不需要 ack 的 transport (如 InMemory)，no-op 即可
        """

    @abstractmethod
    async def create_group(self, topic: str | Topic, group: str):
        """
        创建消费组 (Redis Streams: XGROUP CREATE / Kafka: auto)

        幂等: group 已存在则静默跳过
        """

    @property
    @abstractmethod
    def name(self) -> str:
        """transport 标识名 (用于日志/监控)"""
