from __future__ import annotations

import bisect
import threading
import time
from dataclasses import dataclass

from .market_discovery import MarketContext


@dataclass(frozen=True)
class BestQuote:
    bid: float | None
    ask: float | None

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid


class AssetBook:
    def __init__(self) -> None:
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.sorted_bids: list[float] = []
        self.sorted_asks: list[float] = []
        self.best_bid: float | None = None
        self.best_ask: float | None = None

    def reset(self, bids: list[dict], asks: list[dict]) -> None:
        self.bids = {str(item["price"]): str(item["size"]) for item in bids}
        self.asks = {str(item["price"]): str(item["size"]) for item in asks}
        self.sorted_bids = sorted(float(price) for price in self.bids)
        self.sorted_asks = sorted(float(price) for price in self.asks)
        self.best_bid = None
        self.best_ask = None

    def update(
        self,
        side: str,
        price: str,
        size: str,
        best_bid: str | None = None,
        best_ask: str | None = None,
    ) -> None:
        book = self.bids if side == "BUY" else self.asks
        sorted_prices = self.sorted_bids if side == "BUY" else self.sorted_asks
        price_key = str(price)
        price_value = float(price_key)
        if best_bid is not None:
            self.best_bid = float(best_bid)
        if best_ask is not None:
            self.best_ask = float(best_ask)
        if float(size) == 0:
            if price_key in book:
                book.pop(price_key, None)
                idx = bisect.bisect_left(sorted_prices, price_value)
                if idx < len(sorted_prices) and sorted_prices[idx] == price_value:
                    sorted_prices.pop(idx)
            return
        if price_key not in book:
            bisect.insort(sorted_prices, price_value)
        book[price_key] = str(size)

    def best_quote(self) -> BestQuote:
        bid = self.best_bid if self.best_bid is not None else (self.sorted_bids[-1] if self.sorted_bids else None)
        ask = self.best_ask if self.best_ask is not None else (self.sorted_asks[0] if self.sorted_asks else None)
        return BestQuote(bid=bid, ask=ask)

    def depth_rows(self, side: str, levels: int) -> list[dict]:
        book = self.bids if side == "BID" else self.asks
        prices = self.sorted_bids if side == "BID" else self.sorted_asks
        ordered_prices = reversed(prices) if side == "BID" else iter(prices)
        rows: list[dict] = []
        for level, price in enumerate(ordered_prices, start=1):
            if level > levels:
                break
            size = book.get(str(price))
            if size is None:
                size = next((value for key, value in book.items() if float(key) == price), None)
            if size is None:
                continue
            rows.append(
                {
                    "side": side,
                    "level": level,
                    "price": price,
                    "size": float(size),
                }
            )
        return rows


class OrderBookState:
    def __init__(self) -> None:
        self._books: dict[str, AssetBook] = {}
        self._contexts_by_asset: dict[str, MarketContext] = {}
        self._lock = threading.Lock()

    def set_contexts(self, contexts: list[MarketContext]) -> None:
        with self._lock:
            self._contexts_by_asset = {
                asset_id: ctx
                for ctx in contexts
                for asset_id in ctx.asset_ids
            }
            for asset_id in self._contexts_by_asset:
                self._books.setdefault(asset_id, AssetBook())

    def apply_book(self, asset_id: str, bids: list[dict], asks: list[dict]) -> dict | None:
        with self._lock:
            book = self._books.setdefault(asset_id, AssetBook())
            book.reset(bids, asks)
            return self._snapshot_locked(asset_id)

    def apply_price_change(
        self,
        asset_id: str,
        side: str,
        price: str,
        size: str,
        best_bid: str | None = None,
        best_ask: str | None = None,
    ) -> dict | None:
        with self._lock:
            book = self._books.setdefault(asset_id, AssetBook())
            book.update(side, price, size, best_bid=best_bid, best_ask=best_ask)
            return self._snapshot_locked(asset_id)

    def _snapshot_locked(self, changed_asset_id: str) -> dict | None:
        ctx = self._contexts_by_asset.get(changed_asset_id)
        if not ctx:
            return None
        up_quote = self._books.setdefault(ctx.up_asset_id, AssetBook()).best_quote()
        down_quote = self._books.setdefault(ctx.down_asset_id, AssetBook()).best_quote()
        now = time.time()
        ts_sec = int(now)
        return {
            "ts": now,
            "ts_sec": ts_sec,
            "datetime": int(now * 1000),
            "event_slug": ctx.event_slug,
            "expires": ctx.expires,
            "poly_up_bid": up_quote.bid,
            "poly_up_ask": up_quote.ask,
            "poly_down_bid": down_quote.bid,
            "poly_down_ask": down_quote.ask,
            "interval_min": 5,
            "window_start_ts": ctx.window_start_ts,
            "window_end_ts": ctx.window_end_ts,
            "offset_s": now - ctx.window_start_ts,
            "up_mid": up_quote.mid,
            "down_mid": down_quote.mid,
            "up_spread": up_quote.spread,
            "down_spread": down_quote.spread,
        }

    def depth_rows_for_snapshot(self, snapshot: dict, levels: int) -> list[dict]:
        if levels <= 0:
            return []
        with self._lock:
            ctx = next(
                (
                    item
                    for item in self._contexts_by_asset.values()
                    if item.event_slug == snapshot["event_slug"]
                ),
                None,
            )
            if not ctx:
                return []
            rows: list[dict] = []
            for outcome, asset_id in (("UP", ctx.up_asset_id), ("DOWN", ctx.down_asset_id)):
                book = self._books.setdefault(asset_id, AssetBook())
                for side in ("BID", "ASK"):
                    for depth in book.depth_rows(side, levels):
                        row = {
                            "ts": snapshot["ts"],
                            "ts_sec": snapshot["ts_sec"],
                            "datetime": snapshot["datetime"],
                            "event_slug": snapshot["event_slug"],
                            "expires": snapshot["expires"],
                            "interval_min": snapshot["interval_min"],
                            "window_start_ts": snapshot["window_start_ts"],
                            "window_end_ts": snapshot["window_end_ts"],
                            "offset_s": snapshot["offset_s"],
                            "outcome": outcome,
                        }
                        row.update(depth)
                        rows.append(row)
            return rows
