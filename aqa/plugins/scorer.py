"""内置评分插件 — 根据多项指标计算质量得分"""
from __future__ import annotations

from typing import Any

from aqa.plugin.base import Plugin


class ScorerPlugin(Plugin):
    """评分插件 — 综合多项指标计算质量分数"""

    @property
    def name(self) -> str:
        return "scorer"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "综合评分计算: 通过率、字段完整性、插件健康度"

    async def initialize(self, config: dict[str, Any]) -> None:
        self.weights = config.get(
            "weights",
            {"pass_rate": 0.5, "field_completeness": 0.3, "plugin_health": 0.2},
        )

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """计算综合得分"""
        result = context.get("result", {})
        plugin_results = result.get("plugin_results", [])
        task = context.get("task", {})

        # 通过率
        total = len(plugin_results)
        passed_count = sum(
            1 for pr in plugin_results if pr.get("result", {}).get("passed", True)
        )
        pass_rate = passed_count / total if total > 0 else 1.0

        # 字段完整性
        required = task.get("required_fields", ["task_id", "result"])
        filled = sum(1 for f in required if f in result)
        completeness = filled / len(required) if required else 1.0

        # 插件健康度 (无错误的比例)
        healthy = sum(1 for pr in plugin_results if pr["error"] is None)
        health = healthy / total if total > 0 else 1.0

        # 加权总分
        score = (
            self.weights["pass_rate"] * pass_rate
            + self.weights["field_completeness"] * completeness
            + self.weights["plugin_health"] * health
        )

        passed = score >= 0.7

        return {
            "score": round(score, 4),
            "passed": passed,
            "details": {
                "pass_rate": round(pass_rate, 4),
                "completeness": round(completeness, 4),
                "plugin_health": round(health, 4),
                "weights_used": self.weights,
            },
        }

    async def cleanup(self) -> None:
        pass
