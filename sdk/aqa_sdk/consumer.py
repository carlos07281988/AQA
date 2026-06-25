"""
AQA SDK — Consumer (订阅者)

自动从 Redis Stream 消费消息, 支持消费组自动 ACK、故障接管。
外部 Agent 用此模块订阅 topic 并处理消息。

使用示例:
    async def handler(msg):
        print(f"收到: task_id={msg.payload['task_id']}")
        return {"passed": True, "score": 0.95}

    consumer = Consumer(
        redis_url="redis://127.0.0.1:6379",
        topic="aqa:agent:probe",
        group="my-group",
        consumer_id="worker-1",
        handler=handler,
    )
    await consumer.start()
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Awaitable, Callable, Optional

from aqa_sdk.message import AQAMessage, MessageType, Topic, validate_message

MessageHandler = Callable[[AQAMessage], Awaitable[dict[str, Any]]]


def _get_redis() -> Any:
    """延时导入 redis, 避免未安装时报错"""
    try:
        import redis.asyncio as aioredis

        return aioredis
    except ImportError:
        raise ImportError(
            "缺少 redis 依赖: pip install redis>=5.0\n"
            "或使用纯 JSON 协议直接对接 (见 examples/)"
        )


class Consumer:
    """
    Redis Streams 消费者

    加入消费组, 循环消费消息, 支持:
    - 消费组自动负载均衡
    - Pending 超时自动 CLAIM (故障转移)
    - 死信队列 (超过 max_retries 自动丢弃)
    - 优雅退出
    """

    def __init__(
        self,
        redis_url: str,
        topic: str,
        group: str,
        consumer_id: str,
        handler: MessageHandler,
        batch_size: int = 1,
        poll_interval: float = 1.0,
        claim_interval: float = 30.0,
        max_retries: int = 3,
    ):
        self.redis_url = redis_url
        self.topic = topic or Topic.AGENT_PROBE
        self.group = group
        self.consumer_id = consumer_id
        self.handler = handler
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.claim_interval = claim_interval
        self.max_retries = max_retries
        self._redis: Any = None
        self._running = False
        self._stats: dict[str, int] = {
            "received": 0,
            "acked": 0,
            "failed": 0,
        }

    async def start(self):
        """启动消费者循环"""
        redis_mod = _get_redis()
        self._redis = redis_mod.Redis.from_url(
            self.redis_url, decode_responses=True
        )

        # 确保消费组存在
        try:
            await self._redis.xgroup_create(
                self.topic, self.group, id="0", mkstream=True
            )
        except Exception:
            pass  # 组已存在

        self._running = True
        print(f"[consumer] {self.consumer_id} 启动 @ {self.topic}")

        while self._running:
            try:
                await self._consume_once()
                await self._claim_pending()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[consumer] 循环异常: {e}")
                await asyncio.sleep(self.poll_interval)

        await self._redis.close()
        print(f"[consumer] {self.consumer_id} 已退出")

    async def stop(self):
        """停止消费者"""
        self._running = False

    async def _consume_once(self):
        """批量读取消息"""
        redis_mod = _get_redis()
        results = await self._redis.xreadgroup(
            groupname=self.group,
            consumername=self.consumer_id,
            streams={self.topic: ">"},
            count=self.batch_size,
            block=int(self.poll_interval * 1000),
        )

        if not results:
            return

        for stream_name, messages in results:
            for msg_id, msg_data in messages:
                self._stats["received"] += 1
                await self._process_message(msg_id, msg_data)

    async def _claim_pending(self):
        """接管超时未 ACK 的消息 (故障转移)"""
        redis_mod = _get_redis()
        pending = await self._redis.xpending_range(
            self.topic, self.group, min="-", max="+", count=10
        )
        if not pending:
            return

        stale_ids = [
            p["message_id"]
            for p in pending
            if p.get("times_delivered", 0) >= self.max_retries
        ]

        if stale_ids:
            # 超过重试次数 → 标记为死信
            for mid in stale_ids:
                await self._redis.xack(self.topic, self.group, mid)

        claim_ids = [
            p["message_id"]
            for p in pending
            if float(p.get("elapsed_time", 0)) >= self.claim_interval
            and p.get("times_delivered", 0) < self.max_retries
        ]

        if claim_ids:
            claimed = await self._redis.xclaim(
                self.topic,
                self.group,
                self.consumer_id,
                min_idle_time=int(self.claim_interval * 1000),
                message_ids=claim_ids,
            )
            for msg_id, msg_data in claimed:
                await self._process_message(msg_id, msg_data)

    async def _process_message(self, msg_id: str, msg_data: dict):
        """处理单条消息"""
        try:
            raw = msg_data.get("json", "")
            if isinstance(raw, bytes):
                raw = raw.decode()
            if not raw:
                # 兼容其他字段
                for v in msg_data.values():
                    if isinstance(v, (str, bytes)):
                        raw = v if isinstance(v, str) else v.decode()
                        break

            data = json.loads(raw)
            errors = validate_message(data)
            if errors:
                await self._redis.xack(self.topic, self.group, msg_id)
                return

            message = AQAMessage.from_dict(data)
            await self.handler(message)

            await self._redis.xack(self.topic, self.group, msg_id)
            self._stats["acked"] += 1

        except Exception as e:
            self._stats["failed"] += 1

    def stats(self) -> dict:
        return {**self._stats, "consumer_id": self.consumer_id, "topic": self.topic}


class StreamProducer:
    """
    Redis Streams 生产者

    外部 Agent 用此类向指定 topic 发布消息。
    独立于 aqa 包, 只有 redis 依赖。
    """

    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self._redis: Any = None

    async def connect(self):
        redis_mod = _get_redis()
        self._redis = redis_mod.Redis.from_url(
            self.redis_url, decode_responses=True
        )

    async def disconnect(self):
        if self._redis:
            await self._redis.close()

    async def publish(self, message: AQAMessage, maxlen: int = 10000) -> str:
        """发布消息到 topic 对应的 Stream"""
        if not self._redis:
            await self.connect()
        stream = message.topic or Topic.AGENT_PROBE
        raw = message.to_json()
        msg_id = await self._redis.xadd(
            stream, {"json": raw}, maxlen=maxlen, approximate=True
        )
        return msg_id

    async def send_to(
        self,
        msg_type: MessageType,
        source: str,
        target: str,
        topic: str,
        payload: dict,
        trace_id: str = "",
    ) -> str:
        """快捷发布"""
        msg = AQAMessage(
            type=msg_type,
            source=source,
            target=target,
            topic=topic,
            payload=payload,
            trace_id=trace_id or uuid.uuid4().hex[:16],
        )
        return await self.publish(msg)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()
