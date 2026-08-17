from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from typing import Callable

import certifi
from websocket import WebSocketApp

from .config import CollectorConfig
from .market_discovery import MarketContext, unique_asset_ids
from .orderbook import OrderBookState


SnapshotCallback = Callable[[dict], None]


class ClobMarketWebSocket:
    def __init__(
        self,
        config: CollectorConfig,
        orderbook: OrderBookState,
        on_snapshot: SnapshotCallback,
    ):
        self.config = config
        self.orderbook = orderbook
        self.on_snapshot = on_snapshot
        self._contexts: list[MarketContext] = []
        self._subscribed_assets: set[str] = set()
        self._ws: WebSocketApp | None = None
        self._lock = threading.Lock()
        self.running = True

    def set_contexts(self, contexts: list[MarketContext]) -> None:
        assets = unique_asset_ids(contexts)
        with self._lock:
            self._contexts = contexts
            self.orderbook.set_contexts(contexts)
            ws = self._ws
            desired = set(assets)
            to_add = desired - self._subscribed_assets
            to_remove = self._subscribed_assets - desired
            if ws and (to_add or to_remove):
                sent = True
                if to_add:
                    sent = self._send_json(
                        ws,
                        {"type": "market", "assets_ids": sorted(to_add), "operation": "subscribe"},
                    )
                if to_remove:
                    sent = self._send_json(
                        ws,
                        {"type": "market", "assets_ids": sorted(to_remove), "operation": "unsubscribe"},
                    ) and sent
                if sent:
                    self._subscribed_assets = desired

    def run_forever(self) -> None:
        while self.running:
            try:
                self._ws = WebSocketApp(
                    self.config.clob_ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(
                    sslopt={"ca_certs": certifi.where(), "cert_reqs": ssl.CERT_REQUIRED},
                    ping_interval=30,
                    ping_timeout=10,
                )
            except Exception:
                logging.exception("CLOB WebSocket 循环异常")
            if self.running:
                time.sleep(5)

    def _on_open(self, ws) -> None:
        time.sleep(2)
        with self._lock:
            assets = unique_asset_ids(self._contexts)
            if assets:
                if not self._send_json(
                    ws,
                    {"type": "market", "assets_ids": assets, "operation": "subscribe"},
                ):
                    return
            self._subscribed_assets = set(assets)
        logging.info("CLOB 已连接并订阅 %s 个 asset", len(self._subscribed_assets))

    def _on_message(self, ws, message: str) -> None:
        if message == "PING":
            self._send_text(ws, "PONG")
            return
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            logging.warning("收到非 JSON CLOB 消息: %s", message[:120])
            if "INVALID OPERATION" in message:
                logging.warning("CLOB 拒绝了订阅操作，准备重连")
                ws.close()
            return
        items = payload if isinstance(payload, list) else [payload]
        for item in items:
            self._handle_event(item)

    def _handle_event(self, data: dict) -> None:
        event_type = data.get("event_type")
        asset_id = str(data.get("asset_id") or "")
        if event_type == "book" and asset_id:
            snapshot = self.orderbook.apply_book(asset_id, data.get("bids", []), data.get("asks", []))
            if snapshot:
                self.on_snapshot(snapshot)
            return
        if event_type == "price_change":
            for change in data.get("price_changes", []):
                cid = str(change.get("asset_id") or "")
                if not cid:
                    continue
                snapshot = self.orderbook.apply_price_change(
                    cid,
                    str(change.get("side")),
                    str(change.get("price")),
                    str(change.get("size")),
                    best_bid=change.get("best_bid"),
                    best_ask=change.get("best_ask"),
                )
                if snapshot:
                    self.on_snapshot(snapshot)

    def _send_json(self, ws, payload: dict) -> bool:
        return self._send_text(ws, json.dumps(payload))

    @staticmethod
    def _send_text(ws, message: str) -> bool:
        try:
            ws.send(message)
        except Exception as exc:
            logging.warning("CLOB WebSocket 发送失败，准备重连: %s", exc)
            try:
                ws.close()
            except Exception:
                logging.exception("CLOB WebSocket 发送失败后关闭连接也失败")
            return False
        return True

    @staticmethod
    def _on_error(ws, error) -> None:
        logging.error("CLOB WebSocket 错误: %s", error)

    @staticmethod
    def _on_close(ws, close_status_code, close_msg) -> None:
        logging.warning("CLOB WebSocket 关闭: %s %s", close_status_code, close_msg)
