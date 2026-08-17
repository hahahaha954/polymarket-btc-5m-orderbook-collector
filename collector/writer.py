from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.json as pajson
import pyarrow.parquet as pq


TICK_SCHEMA = pa.schema(
    [
        ("ts", pa.float64()),
        ("ts_sec", pa.int64()),
        ("datetime", pa.timestamp("ms")),
        ("event_slug", pa.string()),
        ("expires", pa.string()),
        ("poly_up_bid", pa.float64()),
        ("poly_up_ask", pa.float64()),
        ("poly_down_bid", pa.float64()),
        ("poly_down_ask", pa.float64()),
        ("interval_min", pa.int32()),
        ("window_start_ts", pa.int64()),
        ("window_end_ts", pa.int64()),
        ("offset_s", pa.float64()),
        ("up_mid", pa.float64()),
        ("down_mid", pa.float64()),
        ("up_spread", pa.float64()),
        ("down_spread", pa.float64()),
    ]
)

OUTCOME_SCHEMA = pa.schema(
    [
        ("event_slug", pa.string()),
        ("window_start_ts", pa.int64()),
        ("window_end_ts", pa.int64()),
        ("winner_side", pa.string()),
        ("target", pa.int8()),
    ]
)

DEPTH_SCHEMA = pa.schema(
    [
        ("ts", pa.float64()),
        ("ts_sec", pa.int64()),
        ("datetime", pa.timestamp("ms")),
        ("event_slug", pa.string()),
        ("expires", pa.string()),
        ("interval_min", pa.int32()),
        ("window_start_ts", pa.int64()),
        ("window_end_ts", pa.int64()),
        ("offset_s", pa.float64()),
        ("outcome", pa.string()),
        ("side", pa.string()),
        ("level", pa.int32()),
        ("price", pa.float64()),
        ("size", pa.float64()),
    ]
)


class JsonlParquetWriter:
    def __init__(self, data_dir: Path):
        self.live_dir = data_dir / "live"
        self.hourly_dir = data_dir / "hourly"
        self.live_dir.mkdir(parents=True, exist_ok=True)
        self.hourly_dir.mkdir(parents=True, exist_ok=True)

    def append_tick(self, row: dict) -> Path:
        path = self.tick_jsonl_path(datetime.fromtimestamp(row["ts"], timezone.utc))
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        return path

    def append_depth_rows(self, rows: list[dict]) -> Path | None:
        if not rows:
            return None
        path = self.depth_jsonl_path(datetime.fromtimestamp(rows[0]["ts"], timezone.utc))
        with path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        return path

    def tick_jsonl_path(self, dt: datetime) -> Path:
        return self.live_dir / f"ticks_{dt.strftime('%Y%m%d_%H')}.jsonl"

    def depth_jsonl_path(self, dt: datetime) -> Path:
        return self.live_dir / f"depth_{dt.strftime('%Y%m%d_%H')}.jsonl"

    def finalize_completed_hours(self, now: datetime | None = None) -> list[Path]:
        now = now or datetime.now(timezone.utc)
        current_prefix = f"ticks_{now.strftime('%Y%m%d_%H')}"
        outputs: list[Path] = []
        for path in sorted(self.live_dir.glob("ticks_*.jsonl")):
            if path.stem >= current_prefix:
                continue
            output = self.hourly_dir / f"{path.stem}.parquet"
            if output.exists():
                path.unlink()
                continue
            self.jsonl_to_parquet(path, output, TICK_SCHEMA)
            path.unlink()
            outputs.append(output)
        depth_current_prefix = f"depth_{now.strftime('%Y%m%d_%H')}"
        for path in sorted(self.live_dir.glob("depth_*.jsonl")):
            if path.stem >= depth_current_prefix:
                continue
            output = self.hourly_dir / f"{path.stem}.parquet"
            if output.exists():
                path.unlink()
                continue
            self.jsonl_to_parquet(path, output, DEPTH_SCHEMA)
            path.unlink()
            outputs.append(output)
        return outputs

    @staticmethod
    def jsonl_to_parquet(input_path: Path, output_path: Path, schema: pa.Schema) -> None:
        if input_path.stat().st_size == 0:
            return
        clean_path = input_path.with_suffix(input_path.suffix + ".clean")
        valid_rows = 0
        bad_rows = 0
        try:
            with input_path.open("r", encoding="utf-8") as src, clean_path.open("w", encoding="utf-8") as dst:
                for line_no, line in enumerate(src, start=1):
                    if not line.strip():
                        continue
                    try:
                        json.loads(line)
                    except json.JSONDecodeError:
                        bad_rows += 1
                        logging.warning("跳过损坏 JSONL 行: file=%s line=%d", input_path, line_no)
                        continue
                    dst.write(line if line.endswith("\n") else line + "\n")
                    valid_rows += 1
            if bad_rows:
                logging.warning("JSONL 清洗完成: file=%s valid=%d bad=%d", input_path, valid_rows, bad_rows)
            if valid_rows == 0:
                logging.warning("JSONL 没有可封存的有效行: %s", input_path)
                return
            table = pajson.read_json(clean_path)
            table = table.cast(schema)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, output_path, compression="zstd")
        finally:
            clean_path.unlink(missing_ok=True)

    def append_outcome(self, row: dict) -> Path:
        path = self.live_dir / "market_outcomes.jsonl"
        row = dict(row)
        row["target"] = 1 if row["winner_side"] == "UP" else 0
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        return path

    def finalize_outcomes(self) -> Path | None:
        path = self.live_dir / "market_outcomes.jsonl"
        if not path.exists() or path.stat().st_size == 0:
            return None
        output = self.hourly_dir / "btc_5m_market_outcomes.parquet"
        try:
            rows: dict[str, dict] = {}
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    rows[row["event_slug"]] = row
            table = pa.Table.from_pylist(list(rows.values()), schema=OUTCOME_SCHEMA)
            output.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, output, compression="zstd")
        except Exception:
            logging.exception("结果表封存失败")
            raise
        return output

    def write_market_ticks_parquet(self, event_slug: str, output_path: Path) -> Path | None:
        return self._write_market_rows_parquet(event_slug, output_path, "ticks_*.jsonl", TICK_SCHEMA)

    def write_market_depth_parquet(self, event_slug: str, output_path: Path) -> Path | None:
        return self._write_market_rows_parquet(event_slug, output_path, "depth_*.jsonl", DEPTH_SCHEMA)

    def _write_market_rows_parquet(
        self,
        event_slug: str,
        output_path: Path,
        glob_pattern: str,
        schema: pa.Schema,
    ) -> Path | None:
        temp_jsonl = output_path.with_suffix(".jsonl")
        matched = 0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with temp_jsonl.open("w", encoding="utf-8") as out:
            for path in sorted(self.live_dir.glob(glob_pattern)):
                with path.open("r", encoding="utf-8") as src:
                    for line in src:
                        if not line.strip():
                            continue
                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if row.get("event_slug") == event_slug:
                            out.write(line)
                            matched += 1
        if matched == 0:
            temp_jsonl.unlink(missing_ok=True)
            return None
        self.jsonl_to_parquet(temp_jsonl, output_path, schema)
        temp_jsonl.unlink(missing_ok=True)
        return output_path
