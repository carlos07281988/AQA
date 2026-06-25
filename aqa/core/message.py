"""AQA 消息协议 — 信封、主题、消息类型"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class MessageType(str, Enum):
    """AQA 标准消息类型"""

    # 生命周期
    HEARTBEAT = "heartbeat"
    REGISTER = "register"
    SHUTDOWN = "shutdown"

    # 检测流程
    TASK_DISPATCH = "task_dispatch"       # 下发检测任务
    TASK_RESULT = "task_result"           # 检测结果
    JUDGE_REQUEST = "judge_request"       # 请求评判
    JUDGE_VERDICT = "judge_verdict"       # 评判结果
    REPORT_REQUEST = "report_request"     # 请求报告
    REPORT_DELIVER = "report_deliver"     # 报告送达

    # 插件事件
    PLUGIN_EVENT = "plugin_event"         # 插件自定义事件
    PLUGIN_REGISTER = "plugin_register"   # 插件注册
    PLUGIN_UNREGISTER = "plugin_unregister"

    # 系统
    ERROR = "error"
    LOG = "log"


class Topic(str, Enum):
    """消息主题 — 对应 Redis Stream key / Kafka topic"""

    # Agent 专属通道
    AGENT_PROBE = "aqa:agent:probe"
    AGENT_JUDGE = "aqa:agent:judge"
    AGENT_REPORTER = "aqa:agent:reporter"

    # 广播通道
    BROADCAST = "aqa:broadcast"
    SYSTEM_EVENTS = "aqa:system:events"

    # 插件通道
    PLUGIN_EVENTS = "aqa:plugin:events"

    # Agent 独立收件箱 (每个 agent 一个)
    @staticmethod
    def agent_inbox(agent_id: str) -> str:
        return f"aqa:inbox:{agent_id}"


@dataclass
class Message:
    """
    消息信封 — 所有 agent 通信的统一格式

    Schema versioning + 全链路 trace_id + 路由字段
    """
    type: MessageType                    # 消息类型
    source: str                          # 发送者 agent_id
    target: Optional[str] = None         # 目标 agent_id (None=广播)
    payload: dict[str, Any] = field(default_factory=dict)
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None # 关联消息 ID (请求-响应配对)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    version: str = "1.0"
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "source": self.source,
            "target": self.target,
            "payload": self.payload,
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "headers": self.headers,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        msg_type = data.get("type", "")
        if isinstance(msg_type, str):
            try:
                msg_type = MessageType(msg_type)
            except ValueError:
                msg_type = MessageType.PLUGIN_EVENT
        else:
            msg_type = MessageType.PLUGIN_EVENT

        return cls(
            type=msg_type,
            source=data.get("source", "unknown"),
            target=data.get("target"),
            payload=data.get("payload", {}),
            trace_id=data.get("trace_id", str(uuid.uuid4())),
            correlation_id=data.get("correlation_id"),
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            version=data.get("version", "1.0"),
            headers=data.get("headers", {}),
        )

    @classmethod
    def from_json(cls, raw: str) -> "Message":
        return cls.from_dict(json.loads(raw))

    def reply(self, msg_type: MessageType, payload: dict | None = None) -> "Message":
        """快速创建回复消息"""
        return Message(
            type=msg_type,
            source=self.target or "unknown",
            target=self.source,
            payload=payload or {},
            trace_id=self.trace_id,
            correlation_id=self.trace_id,
        )


# ── 便捷构造函数 ──

def task_dispatch(source: str, task: dict) -> Message:
    return Message(type=MessageType.TASK_DISPATCH, source=source, payload=task)


def task_result(source: str, result: dict) -> Message:
    return Message(type=MessageType.TASK_RESULT, source=source, payload=result)


def judge_request(source: str, target: str, evidence: dict) -> Message:
    return Message(
        type=MessageType.JUDGE_REQUEST,
        source=source,
        target=target,
        payload=evidence,
    )


def judge_verdict(source: str, verdict: dict) -> Message:
    return Message(type=MessageType.JUDGE_VERDICT, source=source, payload=verdict)


def heartbeat(agent_id: str, status: dict | None = None) -> Message:
    return Message(
        type=MessageType.HEARTBEAT,
        source=agent_id,
        payload={"status": status or {"alive": True}},
    )
