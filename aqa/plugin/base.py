"""Plugin 基类 — 所有 AQA 插件的统一接口"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Plugin(ABC):
    """
    插件基类

    所有第三方/内置插件必须实现此接口。
    插件通过 config.yaml 注册，支持运行时热插拔。
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """插件唯一标识名"""

    @property
    @abstractmethod
    def version(self) -> str:
        """插件版本 (语义化版本号)"""

    @property
    def description(self) -> str:
        """插件描述"""
        return ""

    @abstractmethod
    async def initialize(self, config: dict[str, Any]) -> None:
        """插件初始化 (启动时调用)"""

    @abstractmethod
    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """插件执行 — 核心逻辑"""

    async def cleanup(self) -> None:
        """插件清理 (关闭时调用)"""

    def __repr__(self) -> str:
        return f"<Plugin {self.name} v{self.version}>"
