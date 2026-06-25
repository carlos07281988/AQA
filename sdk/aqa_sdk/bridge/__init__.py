"""
AQA SDK — Protocol Bridge

将外部协议 (HTTP / gRPC / WebSocket / 文件) 桥接到 AQA 队列。
Bridge 是薄转换层, 不包含业务逻辑。
"""
from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, Optional

from aqa_sdk.message import AQAMessage, MessageType, Topic, validate_message
from aqa_sdk.consumer import StreamProducer


class Bridge(ABC):
    """
    协议桥接器基类

    实现双向转换:
      外部协议 -> AQA Queue (收到外部请求, 转为 AQA 消息发布到 Queue)
      AQA Queue -> 外部协议 (从 Queue 消费, 转为外部请求发送)
    """

    def __init__(self, redis_url: str, name: str = "bridge"):
        self.redis_url = redis_url
        self.name = name
        self._producer = StreamProducer(redis_url)
        self._running = False

    async def start(self):
        self._running = True
        await self._producer.connect()
        await self._serve()

    async def stop(self):
        self._running = False
        await self._producer.disconnect()

    @abstractmethod
    async def _serve(self):
        """启动外部协议服务"""

    async def _publish(self, message: AQAMessage) -> str:
        return await self._producer.publish(message)

    @abstractmethod
    async def _to_queue(self, external_request: Any) -> Optional[AQAMessage]:
        """外部请求 -> AQA 消息"""


class HTTPBridge(Bridge):
    """
    HTTP -> AQA Queue 桥接器

    将外部 HTTP POST 请求转为 AQA 消息发布到指定 topic。
    内置简易 asyncio TCP 服务器, 生产环境建议用 uvicorn/fastapi 替换。

    示例: aqa_sdk/examples/http_bridge_demo.py
    """

    def __init__(
        self,
        redis_url: str,
        host: str = "127.0.0.1",
        port: int = 8080,
        publish_topic: str = Topic.AGENT_PROBE,
    ):
        super().__init__(redis_url, name=f"http-bridge-{port}")
        self.host = host
        self.port = port
        self.publish_topic = publish_topic

    async def _serve(self):
        """启动简易 HTTP 服务器, 接收 POST 请求并转发到 AQA Queue"""

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            try:
                request_data = b""
                delim = b"\r\n\r\n"
                while delim not in request_data:
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    request_data += chunk

                if not request_data:
                    writer.close()
                    return

                header_end = request_data.find(delim)
                headers_raw = request_data[:header_end].decode(errors="replace")
                body = request_data[header_end + 4:]

                # 按 Content-Length 读完整 body
                clen = 0
                for line in headers_raw.split("\r\n"):
                    if line.lower().startswith("content-length:"):
                        clen = int(line.split(":")[1].strip())
                while len(body) < clen:
                    body += await reader.read(4096)

                first_line = headers_raw.split("\r\n")[0]
                parts = first_line.split(" ")
                method = parts[0] if len(parts) > 0 else "GET"
                path = parts[1] if len(parts) > 1 else "/"
                source = f"http-{method.lower()}-{self.port}"

                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    payload = {"raw_body": body.decode(errors="replace")}

                msg = AQAMessage(
                    type=MessageType.TASK_DISPATCH,
                    source=source,
                    target="",
                    topic=self.publish_topic,
                    payload={
                        "http_method": method,
                        "http_path": path,
                        "external_request": payload,
                    },
                )

                await self._publish(msg)
                resp = json.dumps(
                    {
                        "status": "accepted",
                        "message_id": msg.message_id,
                        "trace_id": msg.trace_id,
                    }
                ).encode()

                writer.write(
                    b"HTTP/1.1 202 Accepted\r\n"
                    + b"Content-Type: application/json\r\n"
                    + f"Content-Length: {len(resp)}\r\n".encode()
                    + b"\r\n"
                    + resp
                )
            except Exception as e:
                err = json.dumps({"error": str(e)}).encode()
                writer.write(
                    b"HTTP/1.1 500\r\n"
                    + b"Content-Type: application/json\r\n"
                    + f"Content-Length: {len(err)}\r\n".encode()
                    + b"\r\n"
                    + err
                )
            finally:
                writer.close()

        self._server = await asyncio.start_server(handle, self.host, self.port)
        async with self._server:
            await self._server.serve_forever()

    async def _to_queue(self, external_request: Any) -> Optional[AQAMessage]:
        raise NotImplementedError("HTTPBridge 使用内置 HTTP 服务器")
