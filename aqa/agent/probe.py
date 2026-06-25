"""Probe Agent — 检测执行器"""
from __future__ import annotations

from typing import Any

from aqa.core.message import (
    Message,
    MessageType,
    Topic,
    task_result,
)
from aqa.agent.base import Agent


class ProbeAgent(Agent):
    """
    检测 Agent

    职责:
    1. 接收 TASK_DISPATCH 消息
    2. 调用绑定的插件执行检测
    3. 返回检测结果 (TASK_RESULT)
    4. 请求评判 (JUDGE_REQUEST)
    """

    @property
    def agent_type(self) -> str:
        return "probe"

    async def handle_message(self, message: Message) -> list[Message] | None:
        replies = []

        if message.type == MessageType.TASK_DISPATCH:
            # 执行检测
            result = await self._probe(message.payload)

            # 返回检测结果
            replies.append(task_result(self.agent_id, result))

            # 请求评判
            judge_msg = message.reply(
                MessageType.JUDGE_REQUEST,
                payload={"task": message.payload, "result": result},
            )
            judge_msg.target = "judge-1"
            replies.append(judge_msg)

        return replies or None

    async def _probe(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行检测逻辑 — 调用插件"""
        # 运行绑定到 "probe" topic 的插件
        plugin_results = await self.run_plugins("probe", task)

        return {
            "task_id": task.get("task_id", "unknown"),
            "agent": self.agent_id,
            "plugin_results": plugin_results,
            "passed": all(
                r.get("result", {}).get("passed", True)
                for r in plugin_results
                if r["error"] is None
            ),
        }
