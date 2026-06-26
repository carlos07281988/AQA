"""AQA 配置加载器 — YAML + 环境变量合并"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("aqa.core.config")


class AQAConfig:
    """AQA 系统配置 — 支持 YAML 文件 + 环境变量覆盖"""

    DEFAULTS = {
        "app": {
            "name": "Agent Quality Assurance",
            "version": "1.0.0",
            "debug": False,
        },
        "transport": {
            "backend": "redis-streams",
            "redis_url": "redis://127.0.0.1:6379/0",
            "kafka_servers": "127.0.0.1:9092",
        },
        "agents": {
            # 无默认 Agent — 由 config.yaml 中 agents 段定义
        },
        "plugins": {},  # {name: {enabled: true, config: {...}}}
    }

    def __init__(self, config_path: str | None = None):
        self._data = self._merge_defaults()
        if config_path:
            self._load_yaml(config_path)
        self._apply_env_overrides()

    def _merge_defaults(self) -> dict:
        import copy
        return copy.deepcopy(self.DEFAULTS)

    def _load_yaml(self, path: str):
        import yaml
        p = Path(path)
        if not p.exists():
            print(f"[config] WARN 配置文件不存在, 使用默认值: {path}")
            return
        with open(p) as f:
            user_config = yaml.safe_load(f) or {}
        self._deep_merge(self._data, user_config)
        logger.info("配置加载完成: %s", path)

    def _deep_merge(self, base: dict, override: dict):
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def _apply_env_overrides(self):
        """环境变量覆盖 (AQA_TRANSPORT_BACKEND=redis-streams 风格)"""
        prefix = "AQA_"
        for key, value in sorted(os.environ.items()):
            if key.startswith(prefix):
                parts = key[len(prefix):].lower().split("_")
                target = self._data
                for part in parts[:-1]:
                    if part not in target:
                        break
                    target = target[part]
                else:
                    if parts[-1] in target:
                        target[parts[-1]] = value

    # ── 访问器 ──

    @property
    def transport_backend(self) -> str:
        return self._data["transport"]["backend"]

    @property
    def redis_url(self) -> str:
        return os.getenv("REDIS_URL") or self._data["transport"]["redis_url"]

    @property
    def kafka_servers(self) -> str:
        return self._data["transport"]["kafka_servers"]

    @property
    def debug(self) -> bool:
        return self._data["app"]["debug"]

    @property
    def agent_configs(self) -> dict[str, dict]:
        return self._data.get("agents", {})

    @property
    def plugin_configs(self) -> dict[str, dict]:
        return self._data.get("plugins", {})

    def get(self, *keys: str, default: Any = None) -> Any:
        """深层获取配置值"""
        target = self._data
        for key in keys:
            if isinstance(target, dict) and key in target:
                target = target[key]
            else:
                return default
        return target

    def raw(self) -> dict:
        """获取完整配置"""
        return self._data
