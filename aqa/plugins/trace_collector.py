"""TraceCollector 插件 — 记录消息处理耗时与链路"""
from __future__ import annotations

import logging
import time
from typing import Any

from aqa.plugin.base import Plugin

logger = logging.getLogger("aqa.trace")


class TraceCollector(Plugin):
    """链路追踪收集器

    记录每条消息的处理耗时、来源、类型, 用于可观测性。
    数据写入日志, 可配置接外部 Tracing 系统 (Jaeger/Zipkin)。
    """

    @property
    def name(self) -> str:
        return "trace-collector"

    @property
    def version(self) -> str:
        return "1.0.0"

    async def initialize(self, config: dict) -> None:
        self._report_interval = config.get("report_interval", 60)
        self._buffer: list[dict] = []

    async def execute(self, context: dict) -> dict:
        context_start = context.pop("_aqa_start_time", None)
        if not context_start:
            return {}

        elapsed = time.time() - context_start
        trace_id = context.get("_aqa_trace_id", "unknown")
        msg_type = context.get("_aqa_message_type", "unknown")
        source = context.get("_aqa_source", "unknown")

        record = {
            "trace_id": trace_id,
            "type": msg_type,
            "source": source,
            "elapsed_ms": round(elapsed * 1000, 2),
            "timestamp": time.time(),
        }

        self._buffer.append(record)
        logger.info(
            "[trace] %s | %s | %s | %.2fms",
            trace_id[:12],
            msg_type,
            source,
            elapsed * 1000,
        )

        if len(self._buffer) >= self._report_interval:
            self._flush()

        return {"traced": True, "elapsed_ms": elapsed * 1000}

    def _flush(self):
        """批量上报 (预留接外部系统)"""
        if self._buffer:
            logger.debug("[trace] flush %d records", len(self._buffer))
            self._buffer.clear()

    async def cleanup(self) -> None:
        self._flush()
