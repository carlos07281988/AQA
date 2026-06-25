"""内置验证插件 — 检查结果完整性"""
from __future__ import annotations

from typing import Any

from aqa.plugin.base import Plugin


class ValidatorPlugin(Plugin):
    """结果验证插件 — 检查检测结果是否完整、合规"""

    @property
    def name(self) -> str:
        return "validator"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "验证检测结果的完整性与规范性"

    async def initialize(self, config: dict[str, Any]) -> None:
        self.required_fields = config.get("required_fields", ["task_id", "passed"])
        self._ready = True

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """验证结果"""
        issues = []

        # 检查必填字段
        for field in self.required_fields:
            if field not in context:
                issues.append(f"缺少必填字段: {field}")

        # 检查类型
        if "passed" in context and not isinstance(context["passed"], bool):
            issues.append("'passed' 字段必须为布尔值")

        # 检查插件结果
        plugin_results = context.get("plugin_results", [])
        for pr in plugin_results:
            if pr.get("error"):
                issues.append(f"插件执行错误: {pr['plugin']} — {pr['error']}")

        passed = len(issues) == 0
        return {
            "passed": passed,
            "issues": issues,
            "field_count": len(context),
            "validated_at": self._now(),
        }

    async def cleanup(self) -> None:
        self._ready = False

    def _now(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
