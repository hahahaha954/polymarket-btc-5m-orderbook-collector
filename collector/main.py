from __future__ import annotations

import logging
import signal
import threading
import time
import json
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

from .archiver import DailyArchiver
from .config import CollectorConfig, load_config
from .market_discovery import MarketContext
from .market_discovery import MarketDiscovery
from .orderbook import OrderBookState
from .outcomes import MarketOutcomeResolver
from .telegram_sender import TelegramSender
from .writer import JsonlParquetWriter
from .ws_client import ClobMarketWebSocket


class CollectorApp:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.running = True
        self.discovery = MarketDiscovery(config)
        self.orderbook = OrderBookState()
        self.writer = JsonlParquetWriter(config.data_dir)
        self.archiver = DailyArchiver(config.data_dir, config.timezone, config.retention_days)
        self.telegram = TelegramSender(config)
        self._last_best_quotes: dict[str, tuple[float, float, float, float]] = {}
        self.ws = ClobMarketWebSocket(config, self.orderbook, self.on_snapshot)
        self.outcomes = MarketOutcomeResolver(config.data_dir, self.discovery, self.writer)
        self.last_send_marker = ""
        self.pending_sends_path = config.data_dir / "live" / "pending_sends.json"
        self.pending_sends: dict[str, dict] = self.load_pending_sends()
        self.sent_daily_archives_path = config.data_dir / "live" / "sent_daily_archives.json"
        self.sent_daily_archives: set[str] = self.load_sent_daily_archives()
        self.last_send_attempt_ts = 0.0
        self.test_sent_slugs: set[str] = {
            item["event_slug"]
            for item in self.pending_sends.values()
            if item.get("kind") == "test_market" and item.get("event_slug")
        }
        self.test_mode_package_count = 0

    def run(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        threading.Thread(target=self.ws.run_forever, daemon=True).start()
        while self.running:
            self.sync_markets()
            self.outcomes.poll_due()
            self.writer.finalize_completed_hours()
            self.maybe_send_daily_archive()
            if self.config.test_mode:
                self.maybe_send_test_market_package()
            self.flush_pending_sends()
            time.sleep(self.config.discovery_interval_sec)

    def stop(self, *_args) -> None:
        self.running = False
        self.ws.running = False

    def sync_markets(self) -> None:
        contexts = self.discovery.discover_forward_markets(count=2)
        if not contexts:
            logging.warning("未发现未来两场 BTC 5m 市场")
            return
        self.outcomes.remember_contexts(contexts)
        self.ws.set_contexts(contexts)
        logging.info("已同步市场: %s", ", ".join(ctx.event_slug for ctx in contexts))

    def on_snapshot(self, row: dict) -> None:
        if row["offset_s"] < -300 or row["offset_s"] > 300:
            return
        quote = (
            row.get("poly_up_bid"),
            row.get("poly_up_ask"),
            row.get("poly_down_bid"),
            row.get("poly_down_ask"),
        )
        if any(value is None for value in quote):
            return
        if self._last_best_quotes.get(row["event_slug"]) == quote:
            return
        self._last_best_quotes[row["event_slug"]] = quote
        self.writer.append_tick(row)
        if self.config.depth_enabled:
            rows = self.orderbook.depth_rows_for_snapshot(row, self.config.depth_levels)
            self.writer.append_depth_rows(rows)

    def maybe_send_daily_archive(self) -> None:
        now = datetime.now(self.config.timezone)
        if now.strftime("%H:%M") < self.config.daily_send_time:
            return
        target_day = now - timedelta(days=1)
        archive_day = target_day.strftime("%Y%m%d")
        if self.last_send_marker == archive_day:
            return
        if archive_day in self.sent_daily_archives:
            self.last_send_marker = archive_day
            return
        if self.has_pending_daily_archive(archive_day):
            self.last_send_marker = archive_day
            return
        if not self.archiver.hourly_paths_for_day(target_day):
            if self.archiver.daily_zip_path(archive_day).exists():
                logging.info("日包 %s 已存在且小时文件已清理，按已发送处理", archive_day)
                self.mark_daily_archive_sent(archive_day)
                self.last_send_marker = archive_day
            else:
                logging.warning("日包 %s 无小时 ticks parquet，暂不打包发送", archive_day)
            return
        missing_outcomes = self.archiver.missing_outcomes_for_day(target_day)
        if missing_outcomes:
            logging.info(
                "日包 %s 等待 outcomes 补齐: missing=%d sample=%s",
                archive_day,
                len(missing_outcomes),
                ",".join(sorted(missing_outcomes)[:5]),
            )
            return
        zip_path = self.archiver.archive_day(target_day)
        caption = f"BTC 5m 盘口数据 {target_day.strftime('%Y-%m-%d')} UTC"
        try:
            archive_paths = self.archiver.split_daily_archive(
                zip_path,
                int(self.config.telegram_max_file_size_mb * 1024 * 1024),
            )
        except (OSError, ValueError) as exc:
            logging.warning("日包拆分失败，保留小时文件稍后重试: %s | %s", zip_path, exc)
            return
        self.last_send_marker = archive_day
        for index, archive_path in enumerate(archive_paths, start=1):
            part_caption = caption
            if len(archive_paths) > 1:
                part_caption = f"{caption}（第 {index}/{len(archive_paths)} 部分）"
            self.enqueue_send(
                archive_path,
                part_caption,
                kind="daily",
                archive_day=archive_day,
                part_index=index,
                part_total=len(archive_paths),
                flush=False,
            )
        self.flush_pending_sends(force=True)

    def has_pending_daily_archive(self, archive_day: str) -> bool:
        return any(
            item.get("kind") == "daily" and item.get("archive_day") == archive_day
            for item in self.pending_sends.values()
        )

    def maybe_send_test_market_package(self) -> None:
        if self.test_mode_package_count >= 1:
            return
        now_ts = int(time.time())
        for ctx in sorted(self.outcomes.contexts.values(), key=lambda item: item.window_start_ts):
            if ctx.event_slug in self.test_sent_slugs:
                continue
            if now_ts < ctx.window_end_ts + 3:
                continue
            package = self.build_test_market_package(ctx)
            if not package:
                continue
            self.test_sent_slugs.add(ctx.event_slug)
            self.test_mode_package_count += 1
            self.enqueue_send(
                package,
                f"TEST BTC 5m 盘口数据 {ctx.event_slug}",
                kind="test_market",
                event_slug=ctx.event_slug,
            )
            break

    def build_test_market_package(self, ctx: MarketContext) -> Path | None:
        test_dir = self.config.data_dir / "test_packages"
        parquet_path = test_dir / f"{ctx.event_slug}_ticks.parquet"
        written = self.writer.write_market_ticks_parquet(ctx.event_slug, parquet_path)
        if not written:
            logging.info("测试模式：%s 暂无 ticks 可打包", ctx.event_slug)
            return None
        depth_path = test_dir / f"{ctx.event_slug}_depth.parquet"
        depth_written = None
        if self.config.depth_enabled:
            depth_written = self.writer.write_market_depth_parquet(ctx.event_slug, depth_path)
        zip_path = test_dir / f"{ctx.event_slug}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            zf.write(parquet_path, arcname=parquet_path.name)
            if depth_written:
                zf.write(depth_path, arcname=depth_path.name)
        return zip_path

    def enqueue_send(
        self,
        path: Path,
        caption: str,
        kind: str,
        event_slug: str | None = None,
        archive_day: str | None = None,
        part_index: int | None = None,
        part_total: int | None = None,
        flush: bool = True,
    ) -> None:
        key = str(path)
        if key not in self.pending_sends:
            self.pending_sends[key] = {
                "path": key,
                "caption": caption,
                "kind": kind,
                "event_slug": event_slug,
                "archive_day": archive_day,
                "part_index": part_index,
                "part_total": part_total,
                "created_ts": time.time(),
            }
            self.save_pending_sends()
        if flush:
            self.flush_pending_sends(force=True)

    def _expand_oversized_daily_item(self, key: str, item: dict) -> bool:
        path = Path(item["path"])
        if item.get("kind") != "daily" or item.get("part_total") or not path.exists():
            return False
        max_bytes = int(self.config.telegram_max_file_size_mb * 1024 * 1024)
        if path.stat().st_size <= max_bytes:
            return False
        parts = self.archiver.split_daily_archive(path, max_bytes)
        self.pending_sends.pop(key, None)
        for index, part in enumerate(parts, start=1):
            self.pending_sends[str(part)] = {
                **item,
                "path": str(part),
                "caption": f"{item.get('caption', '')}（第 {index}/{len(parts)} 部分）",
                "part_index": index,
                "part_total": len(parts),
            }
        logging.info("日包超过 Telegram 限制，已拆分发送: %s -> %d parts", path, len(parts))
        return True

    def flush_pending_sends(self, force: bool = False) -> None:
        now_ts = time.time()
        if not force and now_ts - self.last_send_attempt_ts < self.config.telegram_retry_interval_sec:
            return
        if not self.pending_sends:
            return
        self.last_send_attempt_ts = now_ts
        changed = False
        for key, item in list(self.pending_sends.items()):
            try:
                expanded = self._expand_oversized_daily_item(key, item)
            except (OSError, ValueError) as exc:
                logging.warning("待发送日包拆分失败，稍后重试: %s | %s", item.get("path"), exc)
                continue
            if expanded:
                changed = True
                continue
            path = Path(item["path"])
            if not path.exists():
                logging.warning("待发送文件不存在，移除任务: %s", path)
                self.pending_sends.pop(key, None)
                changed = True
                continue
            try:
                ok = self.telegram.send_document(path, caption=item.get("caption", ""))
            except Exception as exc:
                logging.warning("Telegram 发送失败，稍后重试: %s | %s", path, exc)
                continue
            if not ok:
                logging.warning("Telegram 返回失败，稍后重试: %s", path)
                continue
            logging.info("Telegram 发送成功: %s", path)
            self.pending_sends.pop(key, None)
            changed = True
            if item.get("kind") == "daily":
                archive_day = item.get("archive_day")
                if archive_day and not self.has_pending_daily_archive(archive_day):
                    self.mark_daily_archive_sent(archive_day)
                    self.archiver.cleanup_hourly_for_day(archive_day)
                    self.archiver.cleanup_old_archives(datetime.now(self.config.timezone))
        if changed:
            self.save_pending_sends()

    def load_pending_sends(self) -> dict[str, dict]:
        if not self.pending_sends_path.exists():
            return {}
        try:
            data = json.loads(self.pending_sends_path.read_text(encoding="utf-8"))
        except Exception:
            logging.exception("读取待发送任务失败，使用空队列")
            return {}
        return data if isinstance(data, dict) else {}

    def save_pending_sends(self) -> None:
        self.pending_sends_path.parent.mkdir(parents=True, exist_ok=True)
        self.pending_sends_path.write_text(
            json.dumps(self.pending_sends, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def load_sent_daily_archives(self) -> set[str]:
        if not self.sent_daily_archives_path.exists():
            return set()
        try:
            data = json.loads(self.sent_daily_archives_path.read_text(encoding="utf-8"))
        except Exception:
            logging.exception("读取已发送日包记录失败，使用空记录")
            return set()
        if not isinstance(data, list):
            return set()
        return {str(item) for item in data}

    def mark_daily_archive_sent(self, archive_day: str) -> None:
        if archive_day in self.sent_daily_archives:
            return
        self.sent_daily_archives.add(archive_day)
        self.sent_daily_archives_path.parent.mkdir(parents=True, exist_ok=True)
        self.sent_daily_archives_path.write_text(
            json.dumps(sorted(self.sent_daily_archives), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    setup_logging()
    app = CollectorApp(load_config())
    signal.signal(signal.SIGINT, app.stop)
    signal.signal(signal.SIGTERM, app.stop)
    app.run()


if __name__ == "__main__":
    main()
