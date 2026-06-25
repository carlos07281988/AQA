"""
AQA 安全层 — payload 端到端加密

可选特性: 当 config.yaml 中 security.enabled=true 时,
Agent 在 publish 前加密 payload, subscribe 后解密 payload。
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any

CRYPTO_AVAILABLE = False
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    CRYPTO_AVAILABLE = True
except ImportError:
    pass


def _derive_key(password: str, salt: bytes) -> bytes:
    """从密码派生 Fernet 密钥"""
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100_000)
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))


class PayloadCipher:
    """payload 加解密器

    使用 Fernet (AES-128-CBC + HMAC) 对称加密。
    密钥从配置中的 security.secret 派生。
    """

    def __init__(self, secret: str | None = None):
        self._enabled = False
        self._fernet = None
        if secret and CRYPTO_AVAILABLE:
            salt = secret.encode()[:16].ljust(16, b"\0")
            key = _derive_key(secret, salt)
            self._fernet = Fernet(key)
            self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    def encrypt_payload(self, payload: dict) -> dict:
        """加密 payload

        返回: {"_encrypted": True, "_ciphertext": "base64..."}
        """
        if not self._enabled:
            return payload
        plaintext = json.dumps(payload, ensure_ascii=False).encode()
        ciphertext = self._fernet.encrypt(plaintext)
        return {
            "_encrypted": True,
            "_ciphertext": base64.b64encode(ciphertext).decode(),
        }

    def decrypt_payload(self, payload: dict) -> dict:
        """解密 payload"""
        if not self._enabled:
            return payload
        if not payload.get("_encrypted"):
            return payload
        ciphertext = base64.b64decode(payload["_ciphertext"])
        plaintext = self._fernet.decrypt(ciphertext)
        return json.loads(plaintext.decode())


def generate_secret() -> str:
    """生成随机密钥 (用于初始化配置)"""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()
