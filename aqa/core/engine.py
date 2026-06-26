"""
AQA Engine — 配置驱动的运行时引擎

职责:
  1. 加载 config.yaml
  2. 根据配置创建 Transport (Redis / Kafka / InMemory)
  3. 注册并初始化插件
  4. 根据 agents 段创建 Agent 实例, 注册到 Supervisor
  5. 启动所有 Agent
  6. 安装信号处理器, 等待退出
"""
from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path
from typing import Any

from aqa.agent.base import Agent
from aqa.agent.judge import JudgeAgent
from aqa.agent.probe import ProbeAgent
from aqa.agent.reporter import ReporterAgent
from aqa.agent.supervisor import AgentSupervisor
from aqa.core.config import AQAConfig
from aqa.core.log_config import setup_logging
from aqa.core.security import PayloadCipher
from aqa.plugin.registry import registry
from aqa.transport.base import Transport

logger = logging.getLogger("aqa.engine")

# Transport 映射
TRANSPORT_MAP: dict[str, type[Transport]] = {}


def _discover_transports():
    """延迟导入支持的 Transport 实现"""
    global TRANSPORT_MAP

    from aqa.transport.redis_streams import RedisStreamsTransport

    TRANSPORT_MAP["redis-streams"] = RedisStreamsTransport

    from aqa.transport.inmemory import InMemoryTransport

    TRANSPORT_MAP["in-memory"] = InMemoryTransport

    try:
        from aqa.transport.kafka_transport import KafkaTransport

        TRANSPORT_MAP["kafka"] = KafkaTransport
    except ImportError:
        pass


# Agent 类型映射
AGENT_MAP: dict[str, type[Agent]] = {
    "probe": ProbeAgent,
    "judge": JudgeAgent,
    "reporter": ReporterAgent,
}


class AQAEngine:
    """AQA 系统引擎 — 配置驱动的入口"""

    def __init__(self, config_path: str | Path = "config.yaml"):
        self._config = AQAConfig(Path(config_path).resolve())
        self._transport: Transport | None = None
        self._supervisor = AgentSupervisor(
            heartbeat_timeout=self._config.get(
                "supervisor", "heartbeat_timeout", default=90
            )
        )
        self._started = False
        self._cipher: PayloadCipher | None = None

    @property
    def config(self) -> AQAConfig:
        return self._config

    @property
    def transport(self) -> Transport | None:
        return self._transport

    @property
    def supervisor(self) -> AgentSupervisor:
        return self._supervisor

    async def start(self):
        """启动引擎"""
        logger.info("=" * 50)
        logger.info("AQA Engine 启动中...")
        logger.info("=" * 50)

        # 初始化日志系统 (读取 config.yaml 的 logging 段)
        logging_cfg = self._config.get("logging")
        setup_logging(debug=self._config.debug, config=logging_cfg)
        logger.info("AQA Engine v%s starting", self._config.get("app", "version", default="?"))

        _discover_transports()

        # 1. 创建安全层
        self._init_security()

        # 2. 创建 Transport
        await self._init_transport()

        # 3. 注册插件
        self._init_plugins()

        # 4. 创建 Agent
        self._init_agents()

        # 5. 启动
        await self._supervisor.start_all()
        self._started = True

        logger.info("AQA Engine 已启动, %d 个 Agent 运行中", len(self._supervisor._agents))

    async def stop(self):
        """停止引擎"""
        if not self._started:
            return
        logger.info("AQA Engine 停止中...")
        await self._supervisor.stop_all()
        self._started = False
        logger.info("AQA Engine 已停止")

    async def wait_until_shutdown(self):
        """等待进程退出 (阻塞)"""
        self._supervisor.install_signal_handlers()
        try:
            while True:
                import asyncio

                await asyncio.sleep(3600)
        except (KeyboardInterrupt, asyncio.CancelledError):
            await self.stop()
            sys.exit(0)

    async def health(self) -> dict:
        """返回系统健康状态"""
        return {
            "engine": {"started": self._started},
            "transport": {"type": self._transport.name if self._transport else None},
            "security": {"encryption_enabled": self._cipher.enabled if self._cipher else False},
            **await self._supervisor.health_check(),
        }

    # ── 内部初始化 ──

    def _init_security(self):
        """初始化 payload 加密"""
        sec_cfg = self._config.get("security", default={})
        if sec_cfg.get("enabled"):
            secret = sec_cfg.get("secret")
            if not secret:
                logger.warning("[engine] security.enabled=true 但未设置 secret, 跳过加密")
            else:
                self._cipher = PayloadCipher(secret)
                logger.info("[engine] payload 加密已启用")
        else:
            self._cipher = PayloadCipher()

    async def _init_transport(self):
        """根据配置创建 Transport"""
        backend = self._config.transport_backend

        transport_cls = TRANSPORT_MAP.get(backend)
        if not transport_cls:
            available = ", ".join(TRANSPORT_MAP.keys())
            raise ValueError(
                f"不支持的 transport.backend: '{backend}'. 可用: {available}"
            )

        # 构造 Transport 实例
        if backend == "redis-streams":
            self._transport = transport_cls(
                stream_url=self._config.redis_url,
            )
        elif backend == "kafka":
            self._transport = transport_cls(
                servers=self._config.kafka_servers,
            )
        elif backend == "in-memory":
            self._transport = transport_cls()
        else:
            self._transport = transport_cls()

        await self._transport.connect()
        logger.info("[engine] Transport 已连接: %s", self._transport.name)

    def _init_plugins(self):
        """从配置注册插件"""
        plugin_configs = self._config.plugin_configs
        if not plugin_configs:
            logger.info("[engine] 未配置插件")
            return

        for name, cfg in plugin_configs.items():
            if not cfg.get("enabled", True):
                continue

            class_path = cfg.get("class")
            if not class_path:
                logger.warning("[engine] 插件 %s 未设置 class_path, 跳过", name)
                continue

            try:
                module_path, class_name = class_path.rsplit(".", 1)
                module = importlib.import_module(module_path)
                plugin_cls = getattr(module, class_name)
                plugin = plugin_cls()
                topic_bind = cfg.get("topic_bind", [])
                registry.register(plugin, topic_bind=topic_bind)
                logger.info(
                    "[engine] 插件已注册: %s (%s) → %s", name, class_path, topic_bind
                )
            except Exception as e:
                logger.error("[engine] 插件 %s 注册失败: %s", name, e)

    def _init_agents(self):
        """根据配置创建 Agent 并注册到 Supervisor"""
        agent_configs = self._config.agent_configs
        if not agent_configs:
            logger.warning("[engine] 未配置 Agent")
            return

        for agent_id, cfg in agent_configs.items():
            agent_type = cfg.get("type", "")
            agent_cls = AGENT_MAP.get(agent_type)
            if not agent_cls:
                logger.warning("[engine] 不支持的 agent type: %s (%s)", agent_type, agent_id)
                continue

            max_retries = cfg.get("max_retries", 3)
            heartbeat_interval = cfg.get("heartbeat_interval", 30)
            group = cfg.get("group", f"aqa-{agent_type}s")

            agent = agent_cls(
                agent_id=agent_id,
                transport=self._transport,
                group=group,
                max_retries=max_retries,
                heartbeat_interval=heartbeat_interval,
                cipher=self._cipher,
                **cfg.get("targets", {}),
            )

            # 订阅 topic
            for topic in cfg.get("topics", []):
                agent.subscribe_to(topic)

            self._supervisor.register(agent)
            logger.info("[engine] Agent 已创建: %s (%s)", agent_id, agent_type)


async def main():
    """CLI 入口：从 config.yaml 启动 AQA 引擎"""
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="AQA — Agent Quality Assurance Engine")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="配置文件路径 (默认: config.yaml)"
    )
    parser.add_argument(
        "--backend",
        choices=["redis-streams", "kafka", "in-memory"],
        help="覆盖 transport.backend"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="覆盖 app.debug 为 true"
    )
    args = parser.parse_args()

    engine = AQAEngine(config_path=args.config)

    if args.backend:
        engine._config._data["transport"]["backend"] = args.backend
    if args.debug:
        engine._config._data["app"]["debug"] = True

    try:
        await engine.start()
        logger.info("AQA Engine 运行中，按 Ctrl+C 停止")
        await engine.wait_until_shutdown()
    except KeyboardInterrupt:
        pass
    finally:
        await engine.stop()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
