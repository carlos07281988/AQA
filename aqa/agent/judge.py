"""Judge Agent — 评判裁决器"""
from __future__ import annotations

from typing import Any

from aqa.core.message import Message, MessageType, judge_verdict
from aqa.agent.base import Agent


class JudgeAgent(Agent):
    """
    评判 Agent

    职责:
    1. 接收 JUDGE_REQUEST
    2. 调用评分/验证插件
    3. 给出裁决 (JUDGE_VERDICT)
    4. 触发报告生成 (REPORT_REQUEST)
    """

    @property
    def agent_type(self) -> str:
        return "judge"

    async def handle_message(self, message: Message) -> list[Message] | None:
        replies = []

        if message.type == MessageType.JUDGE_REQUEST:
            evidence = message.payload
            verdict = await self._judge(evidence)

            verdict_msg = judge_verdict(self.agent_id, verdict)
            verdict_msg.trace_id = message.trace_id
            verdict_msg.correlation_id = message.correlation_id

            # 发送裁决结果
            replies.append(verdict_msg)

            # 触发报告生成
            report_msg = message.reply(
                MessageType.REPORT_REQUEST,
                payload={
                    "task": evidence.get("task", {}),
                    "result": evidence.get("result", {}),
                    "verdict": verdict,
                },
            )
            report_msg.target = "reporter-1"
            replies.append(report_msg)

        return replies or None

    async def _judge(self, evidence: dict[str, Any]) -> dict[str, Any]:
        """评判逻辑 — 调用评分/验证插件"""
        plugin_results = await self.run_plugins("judge", evidence)

        scores = []
        passed = True
        for pr in plugin_results:
            if pr["error"] is None:
                r = pr["result"]
                score = r.get("score", 0.5)
                scores.append(score)
                if not r.get("passed", True):
                    passed = False
            else:
                scores.append(0.0)
                passed = False

        avg_score = sum(scores) / len(scores) if scores else 0.0

        return {
            "task_id": evidence.get("task", {}).get("task_id", "unknown"),
            "agent": self.agent_id,
            "score": round(avg_score, 4),
            "passed": passed,
            "details": plugin_results,
        }
