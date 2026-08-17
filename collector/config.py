from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


@dataclass(frozen=True)
class CollectorConfig:
    target_symbol: str
    data_dir: Path
    retention_days: int
    daily_send_time: str
    timezone: ZoneInfo
    gamma_api_base: str
    clob_ws_url: str
    telegram_bot_api_base: str
    telegram_bot_token: str
    telegram_channel_id: str
    telegram_dry_run: bool
    discovery_interval_sec: float
    telegram_retry_interval_sec: float
    test_mode: bool
    depth_enabled: bool
    depth_levels: int
    telegram_max_file_size_mb: float = 45.0

    @property
    def symbol_prefix(self) -> str:
        return f"{self.target_symbol.lower()}-updown-5m"


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> CollectorConfig:
    env_file = os.getenv("COLLECTOR_ENV_FILE", ".env.collector")
    load_dotenv(dotenv_path=env_file, override=False)
    target_symbol = os.getenv("TARGET_SYMBOL", "BTC").strip().upper()
    if target_symbol != "BTC":
        raise ValueError("当前采集器只支持 TARGET_SYMBOL=BTC")

    tz_name = os.getenv("TIMEZONE", "UTC").strip()
    send_time = os.getenv("DAILY_SEND_TIME", "00:10").strip()
    if len(send_time.split(":")) != 2:
        raise ValueError("DAILY_SEND_TIME 必须是 HH:MM 格式")

    return CollectorConfig(
        target_symbol=target_symbol,
        data_dir=Path(os.getenv("DATA_DIR", "data")).expanduser(),
        retention_days=int(os.getenv("RETENTION_DAYS", "3")),
        daily_send_time=send_time,
        timezone=ZoneInfo(tz_name),
        gamma_api_base=os.getenv("GAMMA_API_BASE", "https://gamma-api.polymarket.com").rstrip("/"),
        clob_ws_url=os.getenv(
            "CLOB_WS_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        ),
        telegram_bot_api_base=os.getenv("TELEGRAM_BOT_API_BASE", "https://api.telegram.org").rstrip("/"),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_channel_id=os.getenv("TELEGRAM_CHANNEL_ID", "").strip(),
        telegram_dry_run=_bool_env("TELEGRAM_DRY_RUN", False),
        discovery_interval_sec=float(os.getenv("DISCOVERY_INTERVAL_SEC", "15")),
        telegram_retry_interval_sec=float(os.getenv("TELEGRAM_RETRY_INTERVAL_SEC", "60")),
        test_mode=_bool_env("TEST_MODE", False),
        depth_enabled=_bool_env("DEPTH_ENABLED", True),
        depth_levels=int(os.getenv("DEPTH_LEVELS", "20")),
        telegram_max_file_size_mb=float(os.getenv("TELEGRAM_MAX_FILE_SIZE_MB", "45")),
    )
