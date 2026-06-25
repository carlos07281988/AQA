"""插件注册中心 — 管理插件的生命周期与路由"""
from __future__ import annotations

from typing import Any

from aqa.plugin.base import Plugin


class PluginRegistry:
    """
    插件注册中心

    职责:
    1. 管理插件注册/注销 (热插拔)
    2. 将消息路由到匹配的插件
    3. 提供插件发现 (按名称/类型/Topic 查询)
    """

    def __init__(self):
        self._plugins: dict[str, Plugin] = {}
        self._topic_mapping: dict[str, list[str]] = {}  # topic -> [plugin_names]

    # ── 注册/注销 ──

    def register(self, plugin: Plugin, topics: list[str] | None = None) -> None:
        """注册插件, 可选绑定 topic"""
        self._plugins[plugin.name] = plugin
        if topics:
            for topic in topics:
                self._topic_mapping.setdefault(topic, []).append(plugin.name)
        print(f"[plugin] 已注册: {plugin}")

    def unregister(self, name: str) -> bool:
        """注销插件 (热卸载)"""
        if name not in self._plugins:
            print(f"[plugin] 插件 {name} 未注册")
            return False

        plugin = self._plugins.pop(name)
        # 清理 topic 映射
        for topic, plugins in list(self._topic_mapping.items()):
            if name in plugins:
                plugins.remove(name)
            if not plugins:
                del self._topic_mapping[topic]

        print(f"[plugin] 已注销: {plugin}")
        return True

    # ── 查询 ──

    def get(self, name: str) -> Plugin | None:
        return self._plugins.get(name)

    def list(self) -> dict[str, str]:
        return {name: p.version for name, p in self._plugins.items()}

    def get_for_topic(self, topic: str) -> list[Plugin]:
        """获取绑定到某 topic 的所有插件"""
        names = self._topic_mapping.get(topic, [])
        return [self._plugins[n] for n in names if n in self._plugins]

    # ── 批量执行 ──

    async def execute_all(self, topic: str, context: dict) -> list[dict[str, Any]]:
        """执行绑定到 topic 的所有插件"""
        results = []
        for plugin in self.get_for_topic(topic):
            try:
                result = await plugin.execute(context)
                results.append({"plugin": plugin.name, "result": result, "error": None})
            except Exception as e:
                results.append({"plugin": plugin.name, "result": None, "error": str(e)})
        return results

    async def initialize_all(self, configs: dict[str, dict]) -> None:
        """批量初始化所有插件"""
        for name, plugin in self._plugins.items():
            cfg = configs.get(name, {})
            await plugin.initialize(cfg)

    async def cleanup_all(self) -> None:
        """批量清理所有插件"""
        for plugin in self._plugins.values():
            await plugin.cleanup()

    # ── 生命周期 ──

    @property
    def count(self) -> int:
        return len(self._plugins)

    @property
    def topics(self) -> list[str]:
        return list(self._topic_mapping.keys())


# 全局单例
registry = PluginRegistry()
