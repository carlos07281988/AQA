"""
AQA SDK — 消息协议层

外部 Agent 接入 AQA 系统的唯一依赖：理解 JSON 信封格式。
本模块提供 Python 端的序列化/反序列化工具。
Go/JS/Java 等语言可直接按 JSON 结构对接。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class MessageType(str, Enum):
    """消息类型 — AQA 协议的核心语义"""

    HEARTBEAT = "HEARTBEAT"
    TASK_DISPATCH = "TASK_DISPATCH"
    TASK_RESULT = "TASK_RESULT"
    JUDGE_REQUEST = "JUDGE_REQUEST"
    JUDGE_VERDICT = "JUDGE_VERDICT"
    REPORT_REQUEST = "REPORT_REQUEST"
    REPORT = "REPORT"

    # 系统消息
    ERROR = "ERROR"
    COMMAND = "COMMAND"

    def __str__(self) -> str:
        return self.value


class Topic:
    """
    标准 Topic 定义

    外部 Agent 订阅/发布时必须使用相同的 topic 字符串。
    """

    # 全局广播
    BROADCAST = "aqa:broadcast"
    # 各阶段
    AGENT_PROBE = "aqa:agent:probe"
    AGENT_JUDGE = "aqa:agent:judge"
    AGENT_REPORTER = "aqa:agent:reporter"
    # Agent 私有收件箱
    AGENT_INBOX = "aqa:inbox:"  # + agent_id

    @staticmethod
    def inbox(agent_id: str) -> str:
        return f"aqa:inbox:{agent_id}"

    @staticmethod
    def all() -> list[str]:
        return [
            Topic.BROADCAST,
            Topic.AGENT_PROBE,
            Topic.AGENT_JUDGE,
            Topic.AGENT_REPORTER,
        ]


class AQAMessage:
    """
    AQA 消息信封

    这是整个系统的**线协议格式**。
    外部 Agent 无论用什么语言，只需要构造这个 JSON 结构即可通信。

    JSON Schema:
    {
        "type": "TASK_DISPATCH | TASK_RESULT | JUDGE_VERDICT | ...",
        "message_id": "uuid",
        "source": "agent-name",
        "target": "target-agent",        # 空字符串 = 广播
        "topic": "aqa:agent:probe",      # 路由目标
        "trace_id": "uuid",              # 全链路追踪
        "correlation_id": "uuid|''",     # 回覆关联
        "version": "1.0",                # 协议版本
        "payload": { ... },              # 业务数据
        "timestamp": "ISO-8601"
    }
    """

    PROTOCOL_VERSION = "1.0"

    def __init__(
        self,
        type: MessageType,
        source: str,
        payload: dict[str, Any],
        target: str = "",
        topic: str = "",
        trace_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        message_id: Optional[str] = None,
        timestamp: Optional[str] = None,
        version: str = PROTOCOL_VERSION,
    ):
        self.message_id = message_id or uuid.uuid4().hex[:16]
        self.type = type if isinstance(type, MessageType) else MessageType(type)
        self.source = source
        self.target = target
        self.topic = topic
        self.trace_id = trace_id or uuid.uuid4().hex[:16]
        self.correlation_id = correlation_id or ""
        self.version = version
        self.payload = payload
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    def reply(self, msg_type: MessageType, payload: dict[str, Any]) -> "AQAMessage":
        """创建对此消息的回覆 (自动交换 source/target, 透传 trace_id)"""
        return AQAMessage(
            type=msg_type,
            source=self.target or self.source,
            target=self.source,
            payload=payload,
            topic=self.topic,
            trace_id=self.trace_id,
            correlation_id=self.message_id,
            version=self.version,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "message_id": self.message_id,
            "source": self.source,
            "target": self.target,
            "topic": self.topic,
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
            "version": self.version,
            "payload": self.payload,
            "timestamp": self.timestamp,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AQAMessage":
        return cls(
            type=MessageType(data["type"]),
            source=data.get("source", ""),
            payload=data.get("payload", {}),
            target=data.get("target", ""),
            topic=data.get("topic", ""),
            trace_id=data.get("trace_id"),
            correlation_id=data.get("correlation_id"),
            message_id=data.get("message_id"),
            timestamp=data.get("timestamp"),
            version=data.get("version", cls.PROTOCOL_VERSION),
        )

    @classmethod
    def from_json(cls, raw: str) -> "AQAMessage":
        return cls.from_dict(json.loads(raw))

    @classmethod
    def task_dispatch(
        cls, source: str, payload: dict[str, Any]
    ) -> "AQAMessage":
        """创建一个检测任务"""
        return cls(MessageType.TASK_DISPATCH, source, payload, topic=Topic.AGENT_PROBE)

    @classmethod
    def task_result(
        cls, source: str, payload: dict[str, Any]
    ) -> "AQAMessage":
        """任务执行结果"""
        return cls(MessageType.TASK_RESULT, source, payload, topic=Topic.AGENT_JUDGE)

    @classmethod
    def judge_verdict(
        cls, source: str, payload: dict[str, Any]
    ) -> "AQAMessage":
        """评判裁决"""
        return cls(MessageType.JUDGE_VERDICT, source, payload, topic=Topic.AGENT_REPORTER)

    @classmethod
    def heartbeat(cls, source: str, status: dict[str, Any]) -> "AQAMessage":
        """心跳"""
        return cls(MessageType.HEARTBEAT, source, status)

    def __repr__(self) -> str:
        return (
            f"<{self.type.value} "
            f"sid={self.message_id[:8]} "
            f"trace={self.trace_id[:8]} "
            f"{self.source} → {self.target or '*'} @{self.topic}>"
        )


def validate_message(data: dict[str, Any]) -> list[str]:
    """验证消息信封格式是否合法"""
    errors: list[str] = []
    required = ["type", "source", "payload", "version"]
    for field in required:
        if field not in data:
            errors.append(f"缺少必填字段: {field}")
    if "type" in data and data["type"] not in [t.value for t in MessageType]:
        errors.append(f"未知消息类型: {data['type']}")
    if "version" in data and data["version"] != AQAMessage.PROTOCOL_VERSION:
        errors.append(f"协议版本不匹配: 期望 {AQAMessage.PROTOCOL_VERSION}, 收到 {data['version']}")
    return errors
