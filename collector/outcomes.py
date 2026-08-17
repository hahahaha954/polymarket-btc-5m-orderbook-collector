from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import pyarrow.parquet as pq

from .market_discovery import MarketContext, MarketDiscovery
from .writer import JsonlParquetWriter


class MarketOutcomeResolver:
    def __init__(
        self,
        data_dir: Path,
        discovery: MarketDiscovery,
        writer: JsonlParquetWriter,
        settle_grace_sec: int = 300,
    ):
        self.discovery = discovery
        self.writer = writer
        self.settle_grace_sec = settle_grace_sec
        self.state_path = data_dir / "live" / "seen_markets.jsonl"
        self.outcome_jsonl_path = data_dir / "live" / "market_outcomes.jsonl"
        self.outcome_parquet_path = data_dir / "hourly" / "btc_5m_market_outcomes.parquet"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.contexts: dict[str, MarketContext] = {}
        self.resolved: set[str] = set()
        self._load_state()
        self._load_resolved()

    def remember_contexts(self, contexts: list[MarketContext]) -> None:
        for ctx in contexts:
            if ctx.event_slug in self.contexts:
                continue
            self.contexts[ctx.event_slug] = ctx
            with self.state_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(ctx.__dict__, ensure_ascii=False, separators=(",", ":")) + "\n")

    def poll_due(self, now_ts: int | None = None, max_fetches: int = 2) -> int:
        now_ts = int(time.time()) if now_ts is None else int(now_ts)
        count = 0
        fetched = 0
        for ctx in list(self.contexts.values()):
            if ctx.event_slug in self.resolved:
                continue
            if now_ts < ctx.window_end_ts + self.settle_grace_sec:
                continue
            if fetched >= max_fetches:
                break
            fetched += 1
            event = self.discovery.fetch_event_by_slug(ctx.event_slug)
            winner = self.resolve_winner_side(event) if event else None
            if not winner:
                continue
            self.writer.append_outcome(
                {
                    "event_slug": ctx.event_slug,
                    "window_start_ts": ctx.window_start_ts,
                    "window_end_ts": ctx.window_end_ts,
                    "winner_side": winner,
                }
            )
            self.resolved.add(ctx.event_slug)
            count += 1
        if count:
            self.writer.finalize_outcomes()
        return count

    @staticmethod
    def resolve_winner_side(event: dict | None) -> str | None:
        if not event:
            return None
        markets = event.get("markets") or []
        if not markets:
            return None
        market = markets[0]
        outcomes = _jsonish_list(market.get("outcomes"))
        prices = _jsonish_list(market.get("outcomePrices") or market.get("outcome_prices"))
        if len(outcomes) < 2 or len(prices) < 2:
            return None
        try:
            numeric_prices = [float(x) for x in prices]
        except (TypeError, ValueError):
            return None
        winner_idx = max(range(len(numeric_prices)), key=numeric_prices.__getitem__)
        if numeric_prices[winner_idx] < 0.99:
            logging.info("市场尚未结算到明确价格: %s", event.get("slug"))
            return None
        winner = str(outcomes[winner_idx]).strip().upper()
        if winner in {"YES", "UP"}:
            return "UP"
        if winner in {"NO", "DOWN"}:
            return "DOWN"
        return None

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return
        for line in self.state_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                ctx = MarketContext(
                    event_slug=data["event_slug"],
                    market_id=data["market_id"],
                    condition_id=data["condition_id"],
                    up_asset_id=data["up_asset_id"],
                    down_asset_id=data["down_asset_id"],
                    asset_ids=tuple(data["asset_ids"]),
                    window_start_ts=int(data["window_start_ts"]),
                    window_end_ts=int(data["window_end_ts"]),
                    expires=data["expires"],
                )
                self.contexts[ctx.event_slug] = ctx
            except Exception:
                logging.exception("读取 seen market 失败")

    def _load_resolved(self) -> None:
        if self.outcome_parquet_path.exists():
            try:
                table = pq.read_table(self.outcome_parquet_path, columns=["event_slug"])
                self.resolved.update(str(slug) for slug in table["event_slug"].to_pylist() if slug)
            except Exception:
                logging.exception("读取已结算 parquet 失败")
        if not self.outcome_jsonl_path.exists():
            return
        for line in self.outcome_jsonl_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                slug = data.get("event_slug")
                if slug:
                    self.resolved.add(str(slug))
            except Exception:
                logging.exception("读取已结算 jsonl 失败")


def _jsonish_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return list(value)
