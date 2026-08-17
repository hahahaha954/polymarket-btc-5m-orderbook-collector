from __future__ import annotations

from pathlib import Path

import requests

from .config import CollectorConfig


class TelegramSender:
    def __init__(self, config: CollectorConfig):
        self.config = config

    def send_document(self, path: Path, caption: str = "") -> bool:
        if self.config.telegram_dry_run:
            print(f"[TG DRY-RUN] 将发送: {path} caption={caption}")
            return True
        if not self.config.telegram_bot_token:
            raise ValueError("缺少 TELEGRAM_BOT_TOKEN")
        if not self.config.telegram_channel_id:
            raise ValueError("缺少 TELEGRAM_CHANNEL_ID")
        url = (
            f"{self.config.telegram_bot_api_base}/bot"
            f"{self.config.telegram_bot_token}/sendDocument"
        )
        with path.open("rb") as fh:
            resp = requests.post(
                url,
                data={"chat_id": self.config.telegram_channel_id, "caption": caption},
                files={"document": (path.name, fh)},
                timeout=600,
            )
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code >= 400:
            description = data.get("description") or resp.text[:200]
            raise RuntimeError(f"Telegram HTTP {resp.status_code}: {description}")
        return bool(data.get("ok"))
