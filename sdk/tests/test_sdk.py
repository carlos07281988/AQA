"""
SDK 测试 — 消息协议 + Consumer/Producer
"""
from __future__ import annotations

import json
import pytest

from aqa_sdk.message import (
    AQAMessage,
    MessageType,
    Topic,
    validate_message,
)


class TestMessageProtocol:
    """消息信封协议测试"""

    def test_create_dispatch(self):
        msg = AQAMessage.task_dispatch(
            source="test-cli",
            payload={"task_id": "t-001", "target": "svc-a"},
        )
        assert msg.type == MessageType.TASK_DISPATCH
        assert msg.topic == Topic.AGENT_PROBE
        assert msg.source == "test-cli"
        assert msg.payload["task_id"] == "t-001"
        assert msg.message_id
        assert msg.trace_id
        assert msg.version == "1.0"

    def test_json_roundtrip(self):
        original = AQAMessage.task_dispatch(
            source="tester", payload={"key": "value"}
        )
        raw = original.to_json()
        decoded = AQAMessage.from_json(raw)
        assert decoded.type == original.type
        assert decoded.source == original.source
        assert decoded.trace_id == original.trace_id
        assert decoded.payload == original.payload
        assert decoded.topic == original.topic

    def test_reply(self):
        # 广播消息 (target=""): reply 的 source 回退到 incoming.source
        incoming = AQAMessage.task_dispatch(
            source="req-agent", payload={"task_id": "42"}
        )
        assert incoming.target == ""  # 广播
        reply = incoming.reply(
            MessageType.TASK_RESULT,
            payload={"passed": True},
        )
        assert reply.type == MessageType.TASK_RESULT
        assert reply.source == "req-agent"  # target 为空, 回退到 source
        assert reply.target == "req-agent"
        assert reply.correlation_id == incoming.message_id
        assert reply.trace_id == incoming.trace_id

    def test_reply_targeted(self):
        # 定向消息 (target!==""): reply 的 source 用 original target
        incoming = AQAMessage(
            type=MessageType.TASK_DISPATCH,
            source="orch",
            target="probe-1",
            payload={},
            topic="aqa:inbox:probe-1",
        )
        reply = incoming.reply(
            MessageType.TASK_RESULT,
            payload={"done": True},
        )
        assert reply.source == "probe-1"
        assert reply.target == "orch"

    def test_heartbeat(self):
        msg = AQAMessage.heartbeat("worker-1", {"cpu": 0.3, "mem": 512})
        assert msg.type == MessageType.HEARTBEAT
        assert msg.payload["cpu"] == 0.3

    def test_topic_inbox(self):
        inbox = Topic.inbox("agent-007")
        assert inbox == "aqa:inbox:agent-007"

    def test_validate_valid(self):
        data = {
            "type": "TASK_DISPATCH",
            "message_id": "abc",
            "source": "tester",
            "target": "",
            "topic": "aqa:agent:probe",
            "trace_id": "trace-1",
            "correlation_id": "",
            "version": "1.0",
            "payload": {"ok": True},
            "timestamp": "2026-01-01T00:00:00",
        }
        errors = validate_message(data)
        assert errors == []

    def test_validate_missing_fields(self):
        errors = validate_message({"type": "TASK_DISPATCH"})
        assert len(errors) >= 2  # source, payload, version missing

    def test_validate_unknown_type(self):
        errors = validate_message({
            "type": "UNKNOWN_TYPE",
            "source": "x",
            "payload": {},
            "version": "1.0",
        })
        unknown_errors = [e for e in errors if "未知" in e]
        assert len(unknown_errors) == 1

    def test_validate_version_mismatch(self):
        errors = validate_message({
            "type": "TASK_DISPATCH",
            "source": "x",
            "payload": {},
            "version": "2.0",
        })
        version_errors = [e for e in errors if "版本" in e]
        assert len(version_errors) == 1

    def test_from_dict_topic_default(self):
        msg = AQAMessage.from_dict({
            "type": "HEARTBEAT",
            "source": "x",
            "payload": {},
            "version": "1.0",
        })
        assert msg.type == MessageType.HEARTBEAT
        assert msg.topic == ""  # 默认空

    def test_aqa_compatibility(self):
        """验证 SDK 消息格式与 aqa.core.message 兼容"""
        # SDK 格式
        sdk_msg = AQAMessage.task_dispatch("cli", {"id": "1"})
        sdk_dict = sdk_msg.to_dict()

        # 验证字段名一致
        assert "type" in sdk_dict
        assert "message_id" in sdk_dict
        assert "source" in sdk_dict
        assert "target" in sdk_dict
        assert "topic" in sdk_dict
        assert "trace_id" in sdk_dict
        assert "correlation_id" in sdk_dict
        assert "version" in sdk_dict
        assert "payload" in sdk_dict
        assert "timestamp" in sdk_dict


class TestTopic:
    """Topic 定义测试"""

    def test_all_topics(self):
        topics = Topic.all()
        assert Topic.AGENT_PROBE in topics
        assert Topic.AGENT_JUDGE in topics
        assert Topic.AGENT_REPORTER in topics
        assert Topic.BROADCAST in topics
