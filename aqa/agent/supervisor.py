"""AgentSupervisor — Agent 生命周期总管

职责:
  - 管理一组 Agent 的启动/停止
  - 心跳超时检测 (stale → restart)
  - 故障自动重启
  - 优雅关闭 (SIGTERM handler)
  - 统一状态查询
"""
from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any

from aqa.agent.base import Agent

logger = logging.getLogger("aqa.supervisor")

HEARTBEAT_TIMEOUT = 90  # 超过 N 秒未收到心跳视为失联


class AgentSupervisor:
    """Agent 生命周期总管"""

    def __init__(self, heartbeat_timeout: int = HEARTBEAT_TIMEOUT):
        self._agents: dict[str, Agent] = {}
        self._heartbeat_timeout = heartbeat_timeout
        self._last_heartbeat: dict[str, float] = {}  # agent_id -> timestamp
        self._running = False
        self._monitor_task: asyncio.Task | None = None

    def register(self, agent: Agent):
        """注册 Agent"""
        self._agents[agent.agent_id] = agent
        logger.info("[supervisor] 注册 Agent: %s (%s)", agent.agent_id, agent.agent_type)

    def record_heartbeat(self, agent_id: str):
        """记录 Agent 心跳时间戳"""
        self._last_heartbeat[agent_id] = time.time()

    async def start_all(self):
        """启动所有 Agent"""
        logger.info("[supervisor] 启动 %d 个 Agent...", len(self._agents))
        self._running = True

        # 启动所有 Agent
        for agent_id, agent in self._agents.items():
            try:
                await agent.start()
            except Exception as e:
                logger.error("[supervisor] Agent %s 启动失败: %s", agent_id, e)

        # 启动心跳监控
        self._monitor_task = asyncio.create_task(self._monitor_loop())

        logger.info("[supervisor] 所有 Agent 已启动")

    async def stop_all(self):
        """优雅停止所有 Agent"""
        logger.info("[supervisor] 停止所有 Agent...")
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        for agent_id, agent in list(self._agents.items()):
            try:
                await agent.stop()
            except Exception as e:
                logger.error("[supervisor] Agent %s 停止异常: %s", agent_id, e)

        logger.info("[supervisor] 所有 Agent 已停止")

    async def restart_agent(self, agent_id: str):
        """重启指定 Agent"""
        agent = self._agents.get(agent_id)
        if not agent:
            logger.warning("[supervisor] 找不到 Agent: %s", agent_id)
            return
        logger.info("[supervisor] 重启 Agent: %s", agent_id)
        try:
            await agent.stop()
        except Exception:
            pass
        await agent.start()

    async def health_check(self) -> dict[str, Any]:
        """返回所有 Agent 的健康状态"""
        statuses: dict = {}
        now = time.time()
        all_ids = set(self._agents.keys()) | set(self._last_heartbeat.keys())
        for agent_id in all_ids:
            if agent_id not in self._last_heartbeat:
                statuses[agent_id] = {"status": "unknown", "reason": "未收到心跳"}
                continue
            elapsed = now - self._last_heartbeat[agent_id]
            if elapsed > self._heartbeat_timeout:
                statuses[agent_id] = {
                    "status": "stale",
                    "last_heartbeat": self._last_heartbeat[agent_id],
                    "elapsed_seconds": round(elapsed, 1),
                }
            else:
                statuses[agent_id] = {"status": "healthy", "elapsed_seconds": round(elapsed, 1)}
        return {"agents": statuses, "total": len(all_ids)}

    def install_signal_handlers(self):
        """安装 SIGTERM/SIGINT 处理 (用于优雅关闭)"""
        loop = asyncio.get_event_loop()

        async def shutdown(sig):
            logger.info("[supervisor] 收到 %s 信号, 开始优雅关闭...", sig)
            await self.stop_all()
            loop.stop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig, lambda s=sig: asyncio.create_task(shutdown(s))
                )
            except NotImplementedError:
                # Windows 不支持 add_signal_handler
                logger.debug("[supervisor] add_signal_handler 不可用 (非 Unix)")
                break

    async def _monitor_loop(self):
        """监控循环 — 检查心跳超时并重启失联 Agent"""
        while self._running:
            status = await self.health_check()
            for agent_id, s in status["agents"].items():
                if s.get("status") == "stale":
                    logger.warning(
                        "[supervisor] Agent %s 心跳超时 (%ds), 自动重启",
                        agent_id,
                        s["elapsed_seconds"],
                    )
                    await self.restart_agent(agent_id)
            await asyncio.sleep(self._heartbeat_timeout // 3)
