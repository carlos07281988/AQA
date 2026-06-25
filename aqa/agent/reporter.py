"""Reporter Agent — 报告生成器"""
from __future__ import annotations

from datetime import datetime, timezone

from aqa.core.message import Message, MessageType, Topic
from aqa.agent.base import Agent


class ReporterAgent(Agent):
    """
    报告 Agent

    职责:
    1. 接收 REPORT_REQUEST
    2. 汇总检测 + 评判结果
    3. 生成报告并投递 (REPORT_DELIVER)
    """

    @property
    def agent_type(self) -> str:
        return "reporter"

    async def handle_message(self, message: Message) -> list[Message] | None:
        replies = []

        if message.type == MessageType.REPORT_REQUEST:
            report = await self._generate_report(message.payload)

            deliver_msg = Message(
                type=MessageType.REPORT_DELIVER,
                source=self.agent_id,
                target=message.source,  # 回传给请求者
                payload=report,
                trace_id=message.trace_id,
                correlation_id=message.correlation_id,
            )
            replies.append(deliver_msg)

            # 同时发布到广播频道
            broadcast = Message(
                type=MessageType.REPORT_DELIVER,
                source=self.agent_id,
                payload=report,
                trace_id=message.trace_id,
            )
            await self._transport.publish(Topic.BROADCAST, broadcast)

        return replies or None

    async def _generate_report(self, data: dict) -> dict:
        """生成质量报告"""
        plugin_results = await self.run_plugins("reporter", data)

        return {
            "task_id": data.get("task", {}).get("task_id", "unknown"),
            "title": f"AQA Report — {data.get('task', {}).get('name', 'unknown')}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "score": data.get("verdict", {}).get("score", 0.0),
            "passed": data.get("verdict", {}).get("passed", False),
            "summary": _build_summary(data),
            "plugin_results": plugin_results,
        }


def _build_summary(data: dict) -> str:
    verdict = data.get("verdict", {})
    score = verdict.get("score", 0)
    passed = verdict.get("passed", False)
    if not passed:
        return f"❌ 质量不达标 (得分: {score}) — 需人工复审"
    if score >= 0.9:
        return f"✅ 质量优秀 (得分: {score})"
    return f"⚠️ 质量达标但需关注 (得分: {score})"
