from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import zipfile

import pyarrow.parquet as pq

from collector.archiver import DailyArchiver
from collector.config import CollectorConfig
from collector.main import CollectorApp
from collector.market_discovery import MarketContext, MarketDiscovery
from collector.orderbook import OrderBookState
from collector.outcomes import MarketOutcomeResolver
from collector.writer import JsonlParquetWriter
from collector.ws_client import ClobMarketWebSocket
from zoneinfo import ZoneInfo


def _tick_row(event_slug: str = "btc-updown-5m-1779541800", ts: float = 1779541800.123) -> dict:
    ts_sec = int(ts)
    return {
        "ts": ts,
        "ts_sec": ts_sec,
        "datetime": ts_sec * 1000,
        "event_slug": event_slug,
        "expires": "2026-05-23T13:15:00Z",
        "poly_up_bid": 0.48,
        "poly_up_ask": 0.52,
        "poly_down_bid": 0.47,
        "poly_down_ask": 0.53,
        "interval_min": 5,
        "window_start_ts": ts_sec,
        "window_end_ts": ts_sec + 300,
        "offset_s": 0.123,
        "up_mid": 0.5,
        "down_mid": 0.5,
        "up_spread": 0.04,
        "down_spread": 0.06,
    }


def _ctx() -> MarketContext:
    return MarketContext(
        event_slug="btc-updown-5m-1779541800",
        market_id="m1",
        condition_id="c1",
        up_asset_id="up",
        down_asset_id="down",
        asset_ids=("up", "down"),
        window_start_ts=1779541800,
        window_end_ts=1779542100,
        expires="2026-05-23T13:15:00Z",
    )


def _cfg(tmp_path: Path) -> CollectorConfig:
    return CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:10",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="http://127.0.0.1:8081",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=False,
        depth_enabled=True,
        depth_levels=20,
    )


class _FailingWs:
    def __init__(self):
        self.closed = False

    def send(self, _message: str) -> None:
        raise RuntimeError("send failed")

    def close(self) -> None:
        self.closed = True


class _RecordingWs:
    def __init__(self):
        self.messages: list[str] = []

    def send(self, message: str) -> None:
        self.messages.append(message)


class _FakeDiscovery:
    def __init__(self):
        self.fetched_slugs: list[str] = []

    def fetch_event_by_slug(self, slug: str) -> dict | None:
        self.fetched_slugs.append(slug)
        return None


def test_ws_set_contexts_send_failure_does_not_crash_main_loop(tmp_path: Path):
    ws_client = ClobMarketWebSocket(_cfg(tmp_path), OrderBookState(), lambda _row: None)
    failing_ws = _FailingWs()
    ws_client._ws = failing_ws

    ws_client.set_contexts([_ctx()])

    assert failing_ws.closed
    assert ws_client._subscribed_assets == set()


def test_ws_pong_send_failure_closes_connection(tmp_path: Path):
    ws_client = ClobMarketWebSocket(_cfg(tmp_path), OrderBookState(), lambda _row: None)
    failing_ws = _FailingWs()

    ws_client._on_message(failing_ws, "PING")

    assert failing_ws.closed


def test_ws_open_subscribe_includes_operation(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("collector.ws_client.time.sleep", lambda _seconds: None)
    ws_client = ClobMarketWebSocket(_cfg(tmp_path), OrderBookState(), lambda _row: None)
    ws_client.set_contexts([_ctx()])
    recording_ws = _RecordingWs()

    ws_client._on_open(recording_ws)

    message = json.loads(recording_ws.messages[0])
    assert message["operation"] == "subscribe"
    assert message["assets_ids"] == ["up", "down"]


def test_orderbook_best_quote_and_snapshot():
    state = OrderBookState()
    state.set_contexts([_ctx()])
    assert state.apply_book("up", [{"price": "0.48", "size": "10"}], [{"price": "0.52", "size": "5"}])
    snap = state.apply_book("down", [{"price": "0.47", "size": "9"}], [{"price": "0.53", "size": "4"}])
    assert snap["poly_up_bid"] == 0.48
    assert snap["poly_up_ask"] == 0.52
    assert snap["poly_down_bid"] == 0.47
    assert snap["poly_down_ask"] == 0.53
    assert round(snap["up_mid"], 6) == 0.5
    assert round(snap["up_spread"], 6) == 0.04


def test_price_change_removes_level():
    state = OrderBookState()
    state.set_contexts([_ctx()])
    state.apply_book("up", [{"price": "0.48", "size": "10"}], [{"price": "0.52", "size": "5"}])
    snap = state.apply_price_change("up", "BUY", "0.48", "0")
    assert snap["poly_up_bid"] is None


def test_price_change_prefers_server_best_quote():
    state = OrderBookState()
    state.set_contexts([_ctx()])
    state.apply_book("up", [{"price": "0.53", "size": "10"}], [{"price": "0.52", "size": "5"}])
    snap = state.apply_price_change("up", "BUY", "0.53", "10", best_bid="0.51", best_ask="0.52")
    assert snap["poly_up_bid"] == 0.51
    assert snap["poly_up_ask"] == 0.52
    assert round(snap["up_spread"], 6) == 0.01


def test_orderbook_exports_depth_rows_with_price_size_and_levels():
    state = OrderBookState()
    state.set_contexts([_ctx()])
    state.apply_book(
        "up",
        [
            {"price": "0.48", "size": "10"},
            {"price": "0.49", "size": "11"},
            {"price": "0.47", "size": "12"},
        ],
        [
            {"price": "0.52", "size": "5"},
            {"price": "0.51", "size": "6"},
            {"price": "0.53", "size": "7"},
        ],
    )
    snap = state.apply_book(
        "down",
        [{"price": "0.46", "size": "8"}],
        [{"price": "0.54", "size": "9"}],
    )

    rows = state.depth_rows_for_snapshot(snap, 2)

    up_bid_rows = [row for row in rows if row["outcome"] == "UP" and row["side"] == "BID"]
    up_ask_rows = [row for row in rows if row["outcome"] == "UP" and row["side"] == "ASK"]
    assert [(row["level"], row["price"], row["size"]) for row in up_bid_rows] == [
        (1, 0.49, 11.0),
        (2, 0.48, 10.0),
    ]
    assert [(row["level"], row["price"], row["size"]) for row in up_ask_rows] == [
        (1, 0.51, 6.0),
        (2, 0.52, 5.0),
    ]


def test_writer_jsonl_to_parquet(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    writer.append_tick(_tick_row())
    outputs = writer.finalize_completed_hours(datetime(2026, 5, 24, tzinfo=timezone.utc))
    assert len(outputs) == 1
    table = pq.read_table(outputs[0])
    assert table.column_names == [
        "ts",
        "ts_sec",
        "datetime",
        "event_slug",
        "expires",
        "poly_up_bid",
        "poly_up_ask",
        "poly_down_bid",
        "poly_down_ask",
        "interval_min",
        "window_start_ts",
        "window_end_ts",
        "offset_s",
        "up_mid",
        "down_mid",
        "up_spread",
        "down_spread",
    ]


def test_writer_skips_corrupt_jsonl_lines(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    path = writer.tick_jsonl_path(datetime(2026, 5, 23, 13, tzinfo=timezone.utc))
    path.write_text(
        json.dumps(_tick_row(), ensure_ascii=False, separators=(",", ":"))
        + "\n"
        + '{"ts":1779541800.123,"event_slug":"broken}\n',
        encoding="utf-8",
    )

    outputs = writer.finalize_completed_hours(datetime(2026, 5, 24, tzinfo=timezone.utc))

    assert len(outputs) == 1
    table = pq.read_table(outputs[0])
    assert table.num_rows == 1
    assert not path.exists()


def test_writer_depth_rows_to_parquet(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    row = {
        "ts": 1779541800.123,
        "ts_sec": 1779541800,
        "datetime": 1779541800123,
        "event_slug": "btc-updown-5m-1779541800",
        "expires": "2026-05-23T13:15:00Z",
        "interval_min": 5,
        "window_start_ts": 1779541800,
        "window_end_ts": 1779542100,
        "offset_s": 0.123,
        "outcome": "UP",
        "side": "BID",
        "level": 1,
        "price": 0.49,
        "size": 11.0,
    }
    writer.append_depth_rows([row])

    outputs = writer.finalize_completed_hours(datetime(2026, 5, 24, tzinfo=timezone.utc))

    assert [path.name for path in outputs] == ["depth_20260523_13.parquet"]
    table = pq.read_table(outputs[0])
    assert table.column_names == [
        "ts",
        "ts_sec",
        "datetime",
        "event_slug",
        "expires",
        "interval_min",
        "window_start_ts",
        "window_end_ts",
        "offset_s",
        "outcome",
        "side",
        "level",
        "price",
        "size",
    ]
    assert table["size"].to_pylist() == [11.0]


def test_writer_finalize_outcomes_dedupes_event_slug(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    writer.append_outcome(
        {
            "event_slug": "btc-updown-5m-1779541800",
            "window_start_ts": 1779541800,
            "window_end_ts": 1779542100,
            "winner_side": "UP",
        }
    )
    writer.append_outcome(
        {
            "event_slug": "btc-updown-5m-1779541800",
            "window_start_ts": 1779541800,
            "window_end_ts": 1779542100,
            "winner_side": "DOWN",
        }
    )
    output = writer.finalize_outcomes()
    table = pq.read_table(output)
    assert table.num_rows == 1
    assert table["winner_side"].to_pylist() == ["DOWN"]
    assert table["target"].to_pylist() == [0]


def test_archiver_retention(tmp_path: Path):
    daily = tmp_path / "daily"
    daily.mkdir(parents=True)
    for day in ["20260520", "20260521", "20260522"]:
        (daily / f"btc_5m_{day}.zip").write_text("x", encoding="utf-8")
    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=2)
    deleted = archiver.cleanup_old_archives(datetime(2026, 5, 22, tzinfo=timezone.utc))
    assert [p.name for p in deleted] == ["btc_5m_20260520.zip"]


def test_archiver_cleanup_hourly_for_day(tmp_path: Path):
    hourly = tmp_path / "hourly"
    hourly.mkdir(parents=True)
    keep = hourly / "ticks_20260522_00.parquet"
    delete_a = hourly / "ticks_20260521_00.parquet"
    delete_b = hourly / "ticks_20260521_23.parquet"
    delete_depth = hourly / "depth_20260521_00.parquet"
    for path in [keep, delete_a, delete_b, delete_depth]:
        path.write_text("x", encoding="utf-8")
    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=2)
    deleted = archiver.cleanup_hourly_for_day("20260521")
    assert [p.name for p in deleted] == [
        "depth_20260521_00.parquet",
        "ticks_20260521_00.parquet",
        "ticks_20260521_23.parquet",
    ]
    assert keep.exists()
    assert not delete_a.exists()
    assert not delete_b.exists()
    assert not delete_depth.exists()


def test_archiver_reports_missing_outcomes(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    writer.append_tick(_tick_row("btc-updown-5m-1779541800", 1779541800.123))
    writer.append_tick(_tick_row("btc-updown-5m-1779542100", 1779542100.123))
    writer.finalize_completed_hours(datetime(2026, 5, 24, tzinfo=timezone.utc))
    writer.append_outcome(
        {
            "event_slug": "btc-updown-5m-1779541800",
            "window_start_ts": 1779541800,
            "window_end_ts": 1779542100,
            "winner_side": "UP",
        }
    )
    writer.finalize_outcomes()
    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=3)
    assert archiver.missing_outcomes_for_day("20260523") == {"btc-updown-5m-1779542100"}


def test_archiver_includes_depth_files_in_daily_archive(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    writer.append_tick(_tick_row("btc-updown-5m-1779541800", 1779541800.123))
    writer.append_depth_rows(
        [
            {
                "ts": 1779541800.123,
                "ts_sec": 1779541800,
                "datetime": 1779541800123,
                "event_slug": "btc-updown-5m-1779541800",
                "expires": "2026-05-23T13:15:00Z",
                "interval_min": 5,
                "window_start_ts": 1779541800,
                "window_end_ts": 1779542100,
                "offset_s": 0.123,
                "outcome": "UP",
                "side": "BID",
                "level": 1,
                "price": 0.49,
                "size": 11.0,
            }
        ]
    )
    writer.finalize_completed_hours(datetime(2026, 5, 24, tzinfo=timezone.utc))
    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=3)

    zip_path = archiver.archive_day("20260523")

    with zipfile.ZipFile(zip_path) as zf:
        assert "ticks_20260523_13.parquet" in zf.namelist()
        assert "depth_20260523_13.parquet" in zf.namelist()


def test_archiver_splits_oversized_daily_archive_into_two_parts(tmp_path: Path):
    daily = tmp_path / "daily"
    daily.mkdir(parents=True)
    zip_path = daily / "btc_5m_20260523.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("ticks_20260523_13.parquet", os.urandom(2500))
        zf.writestr("depth_20260523_13.parquet", os.urandom(2500))

    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=3)
    parts = archiver.split_daily_archive(zip_path, max_bytes=3500)

    assert [path.name for path in parts] == [
        "btc_5m_20260523_part1of2.zip",
        "btc_5m_20260523_part2of2.zip",
    ]
    assert all(path.stat().st_size <= 3500 for path in parts)
    with zipfile.ZipFile(parts[0]) as first, zipfile.ZipFile(parts[1]) as second:
        assert set(first.namelist()) | set(second.namelist()) == {
            "ticks_20260523_13.parquet",
            "depth_20260523_13.parquet",
        }


def test_archiver_refuses_empty_daily_archive(tmp_path: Path):
    archiver = DailyArchiver(tmp_path, timezone.utc, retention_days=3)
    zip_path = tmp_path / "daily" / "btc_5m_20260523.zip"
    zip_path.write_text("old package", encoding="utf-8")
    try:
        archiver.archive_day("20260523")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("没有小时 parquet 时不应该生成日包")
    assert zip_path.read_text(encoding="utf-8") == "old package"


def test_outcome_resolver_encodes_winner():
    event = {
        "markets": [
            {
                "outcomes": '["Up","Down"]',
                "outcomePrices": '["1","0"]',
            }
        ]
    }
    assert MarketOutcomeResolver.resolve_winner_side(event) == "UP"
    event["markets"][0]["outcomePrices"] = '["0","1"]'
    assert MarketOutcomeResolver.resolve_winner_side(event) == "DOWN"


def test_outcome_resolver_restores_resolved_and_limits_fetches(tmp_path: Path):
    writer = JsonlParquetWriter(tmp_path)
    resolved_ctx = _ctx()
    unresolved_ctx = MarketContext(
        event_slug="btc-updown-5m-1779542100",
        market_id="m2",
        condition_id="c2",
        up_asset_id="up2",
        down_asset_id="down2",
        asset_ids=("up2", "down2"),
        window_start_ts=1779542100,
        window_end_ts=1779542400,
        expires="2026-05-23T13:20:00Z",
    )
    writer.append_outcome(
        {
            "event_slug": resolved_ctx.event_slug,
            "window_start_ts": resolved_ctx.window_start_ts,
            "window_end_ts": resolved_ctx.window_end_ts,
            "winner_side": "UP",
        }
    )
    writer.finalize_outcomes()
    state_path = tmp_path / "live" / "seen_markets.jsonl"
    state_path.write_text(
        "\n".join(
            json.dumps(ctx.__dict__, ensure_ascii=False)
            for ctx in [resolved_ctx, unresolved_ctx]
        )
        + "\n",
        encoding="utf-8",
    )
    discovery = _FakeDiscovery()
    resolver = MarketOutcomeResolver(tmp_path, discovery, writer)

    resolver.poll_due(now_ts=1779543000, max_fetches=1)

    assert resolved_ctx.event_slug in resolver.resolved
    assert discovery.fetched_slugs == [unresolved_ctx.event_slug]


def test_app_writes_only_best_quote_changes(tmp_path: Path):
    cfg = CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:10",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="http://127.0.0.1:8081",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=False,
        depth_enabled=True,
        depth_levels=20,
    )
    app = CollectorApp(cfg)
    base = {
        "ts": 1782930001.0,
        "ts_sec": 1782930001,
        "datetime": 1782930001000,
        "event_slug": "btc-updown-5m-1782930000",
        "expires": "2026-07-01T18:25:00Z",
        "poly_up_bid": 0.5,
        "poly_up_ask": 0.51,
        "poly_down_bid": 0.49,
        "poly_down_ask": 0.5,
        "interval_min": 5,
        "window_start_ts": 1782930000,
        "window_end_ts": 1782930300,
        "offset_s": 1.0,
        "up_mid": 0.505,
        "down_mid": 0.495,
        "up_spread": 0.01,
        "down_spread": 0.01,
    }
    app.on_snapshot(dict(base))
    app.on_snapshot(dict(base))
    changed = dict(base)
    changed["poly_up_bid"] = 0.49
    app.on_snapshot(changed)
    early = dict(base)
    early["event_slug"] = "btc-updown-5m-1782930300"
    early["offset_s"] = -301
    app.on_snapshot(early)
    path = tmp_path / "live" / "ticks_20260701_18.jsonl"
    assert len(path.read_text(encoding="utf-8").splitlines()) == 2


def test_discovery_uses_current_and_next_slots(tmp_path: Path):
    cfg = CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:10",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="http://127.0.0.1:8081",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=False,
        depth_enabled=True,
        depth_levels=20,
    )
    discovery = MarketDiscovery(cfg)
    assert discovery.forward_slugs(now_ts=1782930408, count=2) == [
        "btc-updown-5m-1782930300",
        "btc-updown-5m-1782930600",
    ]


def test_test_mode_builds_market_package_and_pending_send(tmp_path: Path):
    cfg = CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:10",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="https://api.telegram.org",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=True,
        depth_enabled=True,
        depth_levels=20,
    )
    app = CollectorApp(cfg)
    ctx = _ctx()
    row = {
        "ts": 1779541800.123,
        "ts_sec": 1779541800,
        "datetime": 1779541800123,
        "event_slug": ctx.event_slug,
        "expires": ctx.expires,
        "poly_up_bid": 0.48,
        "poly_up_ask": 0.52,
        "poly_down_bid": 0.47,
        "poly_down_ask": 0.53,
        "interval_min": 5,
        "window_start_ts": ctx.window_start_ts,
        "window_end_ts": ctx.window_end_ts,
        "offset_s": 0.123,
        "up_mid": 0.5,
        "down_mid": 0.5,
        "up_spread": 0.04,
        "down_spread": 0.06,
    }
    app.writer.append_tick(row)
    app.writer.append_depth_rows(
        [
            {
                "ts": row["ts"],
                "ts_sec": row["ts_sec"],
                "datetime": row["datetime"],
                "event_slug": ctx.event_slug,
                "expires": ctx.expires,
                "interval_min": 5,
                "window_start_ts": ctx.window_start_ts,
                "window_end_ts": ctx.window_end_ts,
                "offset_s": row["offset_s"],
                "outcome": "UP",
                "side": "BID",
                "level": 1,
                "price": 0.48,
                "size": 10.0,
            }
        ]
    )
    zip_path = app.build_test_market_package(ctx)
    assert zip_path is not None
    assert zip_path.exists()
    with zipfile.ZipFile(zip_path) as zf:
        assert zf.namelist() == [
            f"{ctx.event_slug}_ticks.parquet",
            f"{ctx.event_slug}_depth.parquet",
        ]
    app.enqueue_send(zip_path, "测试", kind="test_market", event_slug=ctx.event_slug)
    pending = app.load_pending_sends()
    assert pending == {}


def test_daily_archive_waits_for_complete_outcomes(tmp_path: Path):
    cfg = CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:00",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="https://api.telegram.org",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=False,
        depth_enabled=True,
        depth_levels=20,
    )
    app = CollectorApp(cfg)
    target_day = datetime.now(timezone.utc) - timedelta(days=1)
    day_start = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    event_slug = f"btc-updown-5m-{int(day_start.timestamp())}"
    app.writer.append_tick(_tick_row(event_slug, day_start.timestamp() + 1))
    app.writer.finalize_completed_hours(datetime.now(timezone.utc))

    app.maybe_send_daily_archive()
    assert not list((tmp_path / "daily").glob("btc_5m_*.zip"))

    app.writer.append_outcome(
        {
            "event_slug": event_slug,
            "window_start_ts": int(day_start.timestamp()),
            "window_end_ts": int(day_start.timestamp()) + 300,
            "winner_side": "UP",
        }
    )
    app.writer.finalize_outcomes()
    app.maybe_send_daily_archive()
    assert (tmp_path / "daily" / f"btc_5m_{target_day.strftime('%Y%m%d')}.zip").exists()


def test_daily_archive_is_not_sent_again_after_restart(tmp_path: Path):
    cfg = CollectorConfig(
        target_symbol="BTC",
        data_dir=tmp_path,
        retention_days=3,
        daily_send_time="00:00",
        timezone=ZoneInfo("UTC"),
        gamma_api_base="https://gamma-api.polymarket.com",
        clob_ws_url="wss://example.invalid",
        telegram_bot_api_base="https://api.telegram.org",
        telegram_bot_token="",
        telegram_channel_id="",
        telegram_dry_run=True,
        discovery_interval_sec=15,
        telegram_retry_interval_sec=60,
        test_mode=False,
        depth_enabled=True,
        depth_levels=20,
    )
    app = CollectorApp(cfg)
    target_day = datetime.now(timezone.utc) - timedelta(days=1)
    day_start = datetime(target_day.year, target_day.month, target_day.day, tzinfo=timezone.utc)
    archive_day = target_day.strftime("%Y%m%d")
    event_slug = f"btc-updown-5m-{int(day_start.timestamp())}"
    app.writer.append_tick(_tick_row(event_slug, day_start.timestamp() + 1))
    app.writer.finalize_completed_hours(datetime.now(timezone.utc))
    app.writer.append_outcome(
        {
            "event_slug": event_slug,
            "window_start_ts": int(day_start.timestamp()),
            "window_end_ts": int(day_start.timestamp()) + 300,
            "winner_side": "UP",
        }
    )
    app.writer.finalize_outcomes()
    app.maybe_send_daily_archive()
    zip_path = tmp_path / "daily" / f"btc_5m_{archive_day}.zip"
    first_size = zip_path.stat().st_size
    assert not list((tmp_path / "hourly").glob(f"ticks_{archive_day}_*.parquet"))

    restarted = CollectorApp(cfg)
    restarted.maybe_send_daily_archive()
    assert zip_path.stat().st_size == first_size
    assert restarted.load_pending_sends() == {}
    assert archive_day in restarted.load_sent_daily_archives()
