"""
AQA 消息协议 — 信封、主题、消息类型

线格式严格遵循 PROTOCOL.md 定义。
两种 Message 序列化后必须产出相同的 JSON 结构。
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


logger = logging.getLogger("aqa.core.message")


# ── 消息类型 ──

class MessageType(str, Enum):
    """AQA 标准消息类型 — 严格匹配 PROTOCOL.md §2"""

    # 未知类型 (来自外部或不兼容版本)
    UNKNOWN = "UNKNOWN"

    # 生命周期
    HEARTBEAT = "HEARTBEAT"
    REGISTER = "REGISTER"
    SHUTDOWN = "SHUTDOWN"

    # 检测流程
    TASK_DISPATCH = "TASK_DISPATCH"       # 下发检测任务
    TASK_RESULT = "TASK_RESULT"           # 检测结果
    JUDGE_REQUEST = "JUDGE_REQUEST"       # 请求评判
    JUDGE_VERDICT = "JUDGE_VERDICT"       # 评判结果
    REPORT_REQUEST = "REPORT_REQUEST"     # 请求报告
    REPORT_DELIVER = "REPORT_DELIVER"     # 报告送达

    # 插件事件
    PLUGIN_EVENT = "PLUGIN_EVENT"

    # 系统
    ERROR = "ERROR"
    LOG = "LOG"

    def __str__(self) -> str:
        return self.value


# ── Topic 系统 ──

class Topic:
    """消息主题 — 命名规范见 PROTOCOL.md §3"""

    # Agent 专属通道
    AGENT_PROBE = "aqa:agent:probe"
    AGENT_JUDGE = "aqa:agent:judge"
    AGENT_REPORTER = "aqa:agent:reporter"

    # 广播
    BROADCAST = "aqa:broadcast"
    SYSTEM_EVENTS = "aqa:system:events"

    # 插件
    PLUGIN_EVENTS = "aqa:plugin:events"

    # 死信
    DLQ = "aqa:dlq"

    @staticmethod
    def agent_inbox(agent_id: str) -> str:
        return f"aqa:inbox:{agent_id}"


# ── 错误码体系 ──

class ErrorCode(str, Enum):
    """协议级错误码 — PROTOCOL.md §2.3"""
    UNKNOWN_TYPE = "UNKNOWN_TYPE"
    VERSION_MISMATCH = "VERSION_MISMATCH"
    MALFORMED = "MALFORMED"
    ROUTING_FAILURE = "ROUTING_FAILURE"
    PROCESSING_ERROR = "PROCESSING_ERROR"
    TIMEOUT = "TIMEOUT"

    def __str__(self) -> str:
        return self.value


# ── 主消息类 ──

@dataclass
class Message:
    """
    AQA 消息信封

    这是整个系统的**线协议实现**。
    所有 Agent 通信使用此格式。
    与 SDK AQAMessage 序列化后必须产出相同的 JSON。
    """

    type: MessageType                     # 消息语义类型
    source: str                           # 发送者 ID (必填)
    payload: dict[str, Any]               # 业务负载

    message_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    target: str = ""                      # 目标 Agent ID。""=广播
    topic: str = ""                       # 路由 topic
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    correlation_id: str = ""              # 回复关联。""=非回复
    version: str = "1.0"                  # 协议版本
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # ── 序列化 ──

    def to_dict(self) -> dict[str, Any]:
        """序列化为线格式 JSON dict"""
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
    def from_dict(cls, data: dict[str, Any]) -> "Message":
        """从 JSON dict 反序列化"""
        raw_type = data.get("type", "")

        # 类型解析: 未知类型 → UNKNOWN (不静默丢弃, 供 Agent 发 ERROR)
        msg_type = MessageType.UNKNOWN
        if isinstance(raw_type, str):
            try:
                msg_type = MessageType(raw_type)
            except ValueError:
                # 尝试大写化匹配
                try:
                    msg_type = MessageType(raw_type.upper())
                except ValueError:
                    logger.warning("未知消息类型 '%s', 标记为 UNKNOWN", raw_type)
                    msg_type = MessageType.UNKNOWN

        return cls(
            type=msg_type,
            message_id=data.get("message_id", uuid.uuid4().hex[:16]),
            source=data.get("source", ""),
            target=data.get("target", ""),
            topic=data.get("topic", ""),
            trace_id=data.get("trace_id", uuid.uuid4().hex[:16]),
            correlation_id=data.get("correlation_id", ""),
            version=data.get("version", "1.0"),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
        )

    @classmethod
    def from_json(cls, raw: str) -> "Message":
        return cls.from_dict(json.loads(raw))

    # ── 消息构建 ──

    def reply(self, msg_type: MessageType, payload: dict | None = None) -> "Message":
        """
        创建对此消息的回复。

        依据 PROTOCOL.md §4:
        - source/target 交换
        - trace_id 透传 (不准重新生成)
        - correlation_id = 原消息的 message_id
        """
        return Message(
            type=msg_type,
            source=self.target or "unknown",
            target=self.source,
            topic=self.topic,
            payload=payload or {},
            trace_id=self.trace_id,
            correlation_id=self.message_id,
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

    返回错误列表，空列表表示消息合法。
    这条函数在 core 和 SDK 中必须保持行为一致。
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


# ── 便捷构造函数 ──

def task_dispatch(source: str, payload: dict) -> Message:
    return Message(
        type=MessageType.TASK_DISPATCH,
        source=source,
        payload=payload,
        topic=Topic.AGENT_PROBE,
    )


def task_result(source: str, payload: dict) -> Message:
    return Message(
        type=MessageType.TASK_RESULT,
        source=source,
        payload=payload,
        topic=Topic.AGENT_JUDGE,
    )


def judge_request(source: str, target: str, evidence: dict) -> Message:
    return Message(
        type=MessageType.JUDGE_REQUEST,
        source=source,
        target=target,
        payload=evidence,
        topic=Topic.AGENT_JUDGE,
    )


def judge_verdict(source: str, payload: dict) -> Message:
    return Message(
        type=MessageType.JUDGE_VERDICT,
        source=source,
        payload=payload,
        topic=Topic.AGENT_REPORTER,
    )


def heartbeat(agent_id: str, status: dict | None = None) -> Message:
    return Message(
        type=MessageType.HEARTBEAT,
        source=agent_id,
        payload={"status": status or {"alive": True}},
    )


def error_message(
    source: str,
    code: str,
    message: str,
    trace_id: str = "",
    original_message_id: str = "",
) -> Message:
    """构造协议级 ERROR 消息 (PROTOCOL.md §2.3)"""
    return Message(
        type=MessageType.ERROR,
        source=source,
        payload={
            "code": code,
            "message": message,
            "trace_id": trace_id,
            "original_message_id": original_message_id,
        },
    )
