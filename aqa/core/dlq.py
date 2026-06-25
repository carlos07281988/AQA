"""
AQA 死信队列 — 失败消息处理

当消息超过最大重试次数后, 自动转发到 DLQ topic 并记录原因。
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("aqa.dlq")

# DLQ topic (全局死信通道)
DLQ_TOPIC = "aqa:dlq"


@dataclass
class DeadLetterRecord:
    """死信记录 — 包裹原始失败消息 + 失败原因"""

    original_message: dict  # 原始消息完整内容
    error: str  # 失败原因
    retry_count: int  # 已重试次数
    max_retries: int  # 最大允许次数
    failed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    failed_by: str = "unknown"  # 哪个 Agent 上报的

    def to_dict(self) -> dict:
        return {
            "type": "DLQ",
            "original_message": self.original_message,
            "error": self.error,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "failed_at": self.failed_at,
            "failed_by": self.failed_by,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


def create_dlq_message(
    original: dict,
    error: str,
    retry_count: int,
    max_retries: int,
    failed_by: str = "unknown",
) -> DeadLetterRecord:
    """构造死信记录"""
    return DeadLetterRecord(
        original_message=original,
        error=error,
        retry_count=retry_count,
        max_retries=max_retries,
        failed_by=failed_by,
    )
