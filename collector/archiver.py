from __future__ import annotations

import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq


class DailyArchiver:
    def __init__(self, data_dir: Path, timezone: ZoneInfo, retention_days: int):
        self.data_dir = data_dir
        self.hourly_dir = data_dir / "hourly"
        self.daily_dir = data_dir / "daily"
        self.timezone = timezone
        self.retention_days = retention_days
        self.daily_dir.mkdir(parents=True, exist_ok=True)

    def archive_day(self, day: datetime | str | None = None) -> Path:
        if isinstance(day, str):
            day_str = day
        else:
            local_day = day.astimezone(self.timezone) if day else datetime.now(self.timezone)
            day_str = local_day.strftime("%Y%m%d")
        zip_path = self.daily_dir / f"btc_5m_{day_str}.zip"
        hourly_paths = self.hourly_paths_for_day(day_str)
        if not hourly_paths:
            raise FileNotFoundError(f"没有可打包的小时 ticks parquet: {day_str}")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for path in hourly_paths:
                zf.write(path, arcname=path.name)
            for path in self.depth_paths_for_day(day_str):
                zf.write(path, arcname=path.name)
            outcome = self.hourly_dir / "btc_5m_market_outcomes.parquet"
            if outcome.exists():
                zf.write(outcome, arcname=outcome.name)
        return zip_path

    def split_daily_archive(self, zip_path: Path, max_bytes: int) -> list[Path]:
        """将超大日包按文件拆成两份，返回实际待发送的文件列表。"""
        if zip_path.stat().st_size <= max_bytes:
            return [zip_path]

        with zipfile.ZipFile(zip_path) as source:
            infos = source.infolist()
            if len(infos) < 2:
                raise ValueError(f"日包只有一个文件，无法拆分: {zip_path}")
            # 按压缩后大小从大到小分配，尽量让两部分大小接近。
            infos.sort(key=lambda info: info.compress_size, reverse=True)
            groups: list[list[zipfile.ZipInfo]] = [[], []]
            sizes = [0, 0]
            for info in infos:
                index = 0 if sizes[0] <= sizes[1] else 1
                groups[index].append(info)
                sizes[index] += info.compress_size
            for index, group in enumerate(groups, start=1):
                if not group:
                    raise ValueError(f"日包拆分失败: {zip_path}")

        part_paths = [
            zip_path.with_name(f"{zip_path.stem}_part{index}of2.zip")
            for index in (1, 2)
        ]
        with zipfile.ZipFile(zip_path) as source:
            for part_path, group in zip(part_paths, groups):
                with zipfile.ZipFile(
                    part_path,
                    "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=6,
                ) as target:
                    for info in group:
                        target.writestr(info, source.read(info.filename))

        oversized = [path for path in part_paths if path.stat().st_size > max_bytes]
        if oversized:
            raise ValueError(
                "日包拆分后仍有文件超过 Telegram 限制: "
                + ", ".join(str(path) for path in oversized)
            )
        return part_paths

    def daily_zip_path(self, day: datetime | str) -> Path:
        day_str = day if isinstance(day, str) else day.astimezone(self.timezone).strftime("%Y%m%d")
        return self.daily_dir / f"btc_5m_{day_str}.zip"

    def hourly_paths_for_day(self, day: datetime | str) -> list[Path]:
        day_str = day if isinstance(day, str) else day.astimezone(self.timezone).strftime("%Y%m%d")
        return sorted(self.hourly_dir.glob(f"ticks_{day_str}_*.parquet"))

    def depth_paths_for_day(self, day: datetime | str) -> list[Path]:
        day_str = day if isinstance(day, str) else day.astimezone(self.timezone).strftime("%Y%m%d")
        return sorted(self.hourly_dir.glob(f"depth_{day_str}_*.parquet"))

    def missing_outcomes_for_day(self, day: datetime | str) -> set[str]:
        day_str = day if isinstance(day, str) else day.astimezone(self.timezone).strftime("%Y%m%d")
        tick_slugs: set[str] = set()
        for path in self.hourly_paths_for_day(day_str):
            tick_slugs.update(self._read_unique_values(path, "event_slug"))
        if not tick_slugs:
            return set()
        outcome_path = self.hourly_dir / "btc_5m_market_outcomes.parquet"
        outcome_slugs = self._read_unique_values(outcome_path, "event_slug")
        return tick_slugs - outcome_slugs

    def cleanup_old_archives(self, now: datetime | None = None) -> list[Path]:
        now = now or datetime.now(self.timezone)
        cutoff = (now - timedelta(days=self.retention_days - 1)).strftime("%Y%m%d")
        deleted: list[Path] = []
        for path in sorted(self.daily_dir.glob("btc_5m_*.zip")):
            day = path.stem.replace("btc_5m_", "")
            if day < cutoff:
                path.unlink()
                deleted.append(path)
        return deleted

    def cleanup_hourly_for_day(self, day: datetime | str) -> list[Path]:
        day_str = day if isinstance(day, str) else day.astimezone(self.timezone).strftime("%Y%m%d")
        deleted: list[Path] = []
        paths = list(self.hourly_dir.glob(f"ticks_{day_str}_*.parquet"))
        paths.extend(self.hourly_dir.glob(f"depth_{day_str}_*.parquet"))
        for path in sorted(paths):
            path.unlink()
            deleted.append(path)
        return deleted

    @staticmethod
    def _read_unique_values(path: Path, column: str) -> set[str]:
        if not path.exists():
            return set()
        values: set[str] = set()
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(columns=[column], batch_size=8192):
            values.update(value for value in batch.column(0).to_pylist() if value is not None)
        return values
