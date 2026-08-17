from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import requests
from dateutil import parser

from .config import CollectorConfig


@dataclass(frozen=True)
class MarketContext:
    event_slug: str
    market_id: str
    condition_id: str
    up_asset_id: str
    down_asset_id: str
    asset_ids: tuple[str, str]
    window_start_ts: int
    window_end_ts: int
    expires: str


class MarketDiscovery:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "po5m-data-collector/1.0"}
        )

    @staticmethod
    def slot_open_ts(ts_sec: int | None = None) -> int:
        now_ts = int(time.time()) if ts_sec is None else int(ts_sec)
        return now_ts - now_ts % 300

    def forward_slugs(self, now_ts: int | None = None, count: int = 2) -> list[str]:
        base = self.slot_open_ts(now_ts)
        return [f"{self.config.symbol_prefix}-{base + i * 300}" for i in range(count)]

    def discover_forward_markets(self, count: int = 2) -> list[MarketContext]:
        contexts: list[MarketContext] = []
        for slug in self.forward_slugs(count=count):
            event = self.fetch_event_by_slug(slug)
            if not event:
                continue
            ctx = self.context_from_event(slug, event)
            if ctx:
                contexts.append(ctx)
        return contexts

    def fetch_event_by_slug(self, slug: str) -> dict | None:
        url = f"{self.config.gamma_api_base}/events/slug/{slug}"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return None
            data = resp.json()
        except requests.RequestException:
            return None
        if isinstance(data, list):
            return data[0] if data else None
        return data if isinstance(data, dict) else None

    def context_from_event(self, slug: str, event: dict) -> MarketContext | None:
        markets = event.get("markets") or []
        if not markets:
            return None
        market = markets[0]
        token_ids = self._jsonish_list(market.get("clobTokenIds"))
        outcomes = self._jsonish_list(market.get("outcomes"))
        if len(token_ids) < 2 or len(outcomes) < 2:
            return None

        up_asset_id = ""
        down_asset_id = ""
        for outcome, token_id in zip(outcomes, token_ids):
            name = str(outcome).strip().lower()
            if name in {"up", "yes"}:
                up_asset_id = str(token_id)
            elif name in {"down", "no"}:
                down_asset_id = str(token_id)
        if not up_asset_id:
            up_asset_id = str(token_ids[0])
        if not down_asset_id:
            down_asset_id = str(token_ids[1])

        end_iso = event.get("endDate")
        if not end_iso:
            return None
        end_ts = int(parser.isoparse(end_iso).timestamp())
        start_ts = end_ts - 300
        return MarketContext(
            event_slug=slug,
            market_id=str(event.get("id") or ""),
            condition_id=str(event.get("conditionId") or event.get("id") or ""),
            up_asset_id=up_asset_id,
            down_asset_id=down_asset_id,
            asset_ids=(up_asset_id, down_asset_id),
            window_start_ts=start_ts,
            window_end_ts=end_ts,
            expires=datetime.fromtimestamp(end_ts, timezone.utc).isoformat().replace("+00:00", "Z"),
        )

    @staticmethod
    def _jsonish_list(value) -> list:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return json.loads(value)
        return list(value)


def unique_asset_ids(contexts: Iterable[MarketContext]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for ctx in contexts:
        for asset_id in ctx.asset_ids:
            if asset_id not in seen:
                seen.add(asset_id)
                result.append(asset_id)
    return result
