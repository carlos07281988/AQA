"""
AQA SDK — 消息协议层

线格式严格遵循 PROTOCOL.md §1 定义。
与 core Message 序列化后必须产出相同的 JSON。
外部 Agent 在任何语言中仅需构造此 JSON 结构即可通信。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class MessageType(str, Enum):
    """消息类型 — AQA 协议的核心语义 (PROTOCOL.md §2)"""

    UNKNOWN = "UNKNOWN"

    # 生命周期
    HEARTBEAT = "HEARTBEAT"
    REGISTER = "REGISTER"
    SHUTDOWN = "SHUTDOWN"

    # 检测流程
    TASK_DISPATCH = "TASK_DISPATCH"
    TASK_RESULT = "TASK_RESULT"
    JUDGE_REQUEST = "JUDGE_REQUEST"
    JUDGE_VERDICT = "JUDGE_VERDICT"
    REPORT_REQUEST = "REPORT_REQUEST"
    REPORT_DELIVER = "REPORT_DELIVER"

    # 系统
    ERROR = "ERROR"
    LOG = "LOG"
    PLUGIN_EVENT = "PLUGIN_EVENT"

    def __str__(self) -> str:
        return self.value


class Topic:
    """
    标准 Topic 定义 (PROTOCOL.md §3)

    外部 Agent 订阅/发布时必须使用相同的 topic 字符串。
    """

    BROADCAST = "aqa:broadcast"
    AGENT_PROBE = "aqa:agent:probe"
    AGENT_JUDGE = "aqa:agent:judge"
    AGENT_REPORTER = "aqa:agent:reporter"
    SYSTEM_EVENTS = "aqa:system:events"
    PLUGIN_EVENTS = "aqa:plugin:events"
    DLQ = "aqa:dlq"

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


# ── 错误码体系 ──

class ErrorCode(str, Enum):
    """协议级错误码 (PROTOCOL.md §2.3)"""
    UNKNOWN_TYPE = "UNKNOWN_TYPE"
    VERSION_MISMATCH = "VERSION_MISMATCH"
    MALFORMED = "MALFORMED"
    ROUTING_FAILURE = "ROUTING_FAILURE"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    TIMEOUT = "TIMEOUT"

    def __str__(self) -> str:
        return self.value


class AQAMessage:
    """
    AQA 消息信封

    这是整个系统的**线协议格式** (PROTOCOL.md §1)。
    与 core Message 序列化产出完全相同的 JSON。
    外部 Agent 无论用什么语言，只需要构造这个 JSON 结构即可通信。
    """

    PROTOCOL_VERSION = "1.0"

    def __init__(
        self,
        type: MessageType,
        source: str,
        payload: dict[str, Any],
        message_id: Optional[str] = None,
        target: str = "",
        topic: str = "",
        trace_id: Optional[str] = None,
        correlation_id: str = "",
        version: str = PROTOCOL_VERSION,
        timestamp: Optional[str] = None,
    ):
        self.type = type if isinstance(type, MessageType) else self._resolve_type(type)
        self.source = source
        self.payload = payload
        self.message_id = message_id or uuid.uuid4().hex[:16]
        self.target = target
        self.topic = topic
        self.trace_id = trace_id or uuid.uuid4().hex[:16]
        self.correlation_id = correlation_id
        self.version = version
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _resolve_type(raw: str) -> MessageType:
        """尝试解析类型, 失败时返回 UNKNOWN"""
        try:
            return MessageType(raw)
        except ValueError:
            try:
                return MessageType(raw.upper())
            except ValueError:
                import logging
                logging.getLogger("aqa_sdk.message").warning("未知消息类型 '%s', 标记为 UNKNOWN", raw)
                return MessageType.UNKNOWN

    # ── 序列化 ──

    def to_dict(self) -> dict[str, Any]:
        """序列化为线格式 JSON dict — 字段顺序与 core Message 完全一致"""
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

    # ── 反序列化 ──

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AQAMessage":
        return cls(
            type=data.get("type", ""),
            source=data.get("source", ""),
            payload=data.get("payload", {}),
            message_id=data.get("message_id"),
            target=data.get("target", ""),
            topic=data.get("topic", ""),
            trace_id=data.get("trace_id"),
            correlation_id=data.get("correlation_id", ""),
            version=data.get("version", cls.PROTOCOL_VERSION),
            timestamp=data.get("timestamp"),
        )

    @classmethod
    def from_json(cls, raw: str) -> "AQAMessage":
        return cls.from_dict(json.loads(raw))

    # ── 消息构建 ──

    def reply(self, msg_type: MessageType, payload: dict[str, Any]) -> "AQAMessage":
        """
        创建对此消息的回复 (PROTOCOL.md §4)。

        规则:
        - source/target 交换
        - trace_id 透传
        - correlation_id = 原消息的 message_id
        """
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

    # ── 便捷工厂方法 ──

    @classmethod
    def task_dispatch(cls, source: str, payload: dict[str, Any]) -> "AQAMessage":
        return cls(MessageType.TASK_DISPATCH, source, payload, topic=Topic.AGENT_PROBE)

    @classmethod
    def task_result(cls, source: str, payload: dict[str, Any]) -> "AQAMessage":
        return cls(MessageType.TASK_RESULT, source, payload, topic=Topic.AGENT_JUDGE)

    @classmethod
    def judge_verdict(cls, source: str, payload: dict[str, Any]) -> "AQAMessage":
        return cls(MessageType.JUDGE_VERDICT, source, payload, topic=Topic.AGENT_REPORTER)

    @classmethod
    def heartbeat(cls, source: str, status: dict[str, Any]) -> "AQAMessage":
        return cls(MessageType.HEARTBEAT, source, status)

    @classmethod
    def error(
        cls,
        source: str,
        code: str,
        message: str,
        trace_id: str = "",
        original_message_id: str = "",
    ) -> "AQAMessage":
        """构造协议级 ERROR 消息 (PROTOCOL.md §2.3)"""
        return cls(
            MessageType.ERROR,
            source,
            {
                "code": code,
                "message": message,
                "trace_id": trace_id,
                "original_message_id": original_message_id,
            },
        )

    def __repr__(self) -> str:
        return (
            f"<{self.type.value} "
            f"id={self.message_id[:8]} "
            f"trace={self.trace_id[:8]} "
            f"{self.source} → {self.target or '*'} @{self.topic}>"
        )


# ── 消息验证 ──

def validate_message(data: dict[str, Any]) -> list[str]:
    """
    验证消息信封是否符合 PROTOCOL.md §1 规范。

    与 core validate_message 行为完全一致。
    返回错误列表，空列表表示消息合法。
    """
    errors: list[str] = []

    # 必填字段存在性
    required = ["type", "source", "payload", "version"]
    for field in required:
        if field not in data:
            errors.append(f"缺少必填字段: {field}")

    # source 不能为空
    source = data.get("source", "")
    if not isinstance(source, str) or not source.strip():
        errors.append("source 不能为空")

    # type 必须是已知类型
    raw_type = data.get("type")
    if isinstance(raw_type, str):
        known_types = [t.value for t in MessageType]
        if raw_type not in known_types and raw_type.upper() not in known_types:
            errors.append(f"未知消息类型: {raw_type}")
    elif raw_type is not None:
        errors.append(f"type 必须是字符串, 收到 {type(raw_type).__name__}")

    # version 检查
    version = data.get("version", "")
    if version:
        major = version.split(".")[0] if "." in version else version
        if not major.isdigit() or int(major) != 1:
            errors.append(f"协议版本不匹配: 期望 1.x, 收到 {version}")

    # target 和 correlation_id 必须是字符串 (允许空)
    for field in ("target", "correlation_id"):
        val = data.get(field)
        if val is not None and not isinstance(val, str):
            errors.append(f"{field} 必须是字符串, 收到 {type(val).__name__}")

    # payload 必须是 dict
    payload = data.get("payload")
    if payload is not None and not isinstance(payload, dict):
        errors.append("payload 必须是 JSON Object")

    return errors
