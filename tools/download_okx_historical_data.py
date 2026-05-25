#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Download OKX official historical data files to local storage.

OKX historical data download pages can change their concrete file URL layout.
This tool therefore supports two stable workflows:

1) URL template mode
   Copy one official download URL from OKX, replace the date with {date}, and run:

   python tools/download_okx_historical_data.py \
     --kind trades \
     --symbol ETH-USDT-SWAP \
     --start-date 2025-05-01 \
     --end-date 2025-05-31 \
     --url-template 'https://.../ETH-USDT-SWAP/.../{date}....zip'

2) Manifest mode
   Prepare a text file with one official URL per line:

   python tools/download_okx_historical_data.py \
     --kind books \
     --symbol ETH-USDT-SWAP \
     --manifest data/okx/manifests/eth_books_urls.txt

Downloaded files are saved under:
    data/okx/raw/<kind>/<symbol>/
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.log import get_logger

logger = get_logger("OKXHistoricalDownloader")

DEFAULT_OUT_ROOT = ROOT / "data" / "okx" / "raw"
VALID_KINDS = {"trades", "books", "books_l2"}


@dataclass
class DownloadTask:
    url: str
    output_path: Path
    date_tag: str = ""


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download OKX official historical data files for local backtests.")
    p.add_argument("--kind", required=True, choices=sorted(VALID_KINDS), help="Data kind: trades/books/books_l2.")
    p.add_argument("--symbol", default="ETH-USDT-SWAP", help="OKX instrument id.")
    p.add_argument("--start-date", help="Inclusive date, YYYY-MM-DD. Required for --url-template.")
    p.add_argument("--end-date", help="Inclusive date, YYYY-MM-DD. Required for --url-template.")
    p.add_argument("--url-template", help="Official OKX URL template. Supports {date}, {yyyymmdd}, {symbol}, {kind}.")
    p.add_argument("--manifest", type=Path, help="Text/JSONL manifest containing official OKX download URLs.")
    p.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT, help="Default: data/okx/raw")
    p.add_argument("--overwrite", action="store_true", help="Re-download existing files.")
    p.add_argument("--dry-run", action="store_true", help="Print tasks without downloading.")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--sleep-sec", type=float, default=0.5, help="Sleep between downloads to avoid hammering the official endpoint.")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tasks = list(build_tasks(args))
    if not tasks:
        raise SystemExit("No download tasks generated. Provide --url-template with date range or --manifest.")

    manifest_out = args.out_root / args.kind / args.symbol / "download_manifest.jsonl"
    manifest_out.parent.mkdir(parents=True, exist_ok=True)

    logger.info("[OKX-DOWNLOAD-START] kind=%s symbol=%s tasks=%d out=%s", args.kind, args.symbol, len(tasks), manifest_out.parent)
    ok = skipped = failed = 0
    with manifest_out.open("a", encoding="utf-8") as mf:
        for task in tasks:
            if args.dry_run:
                print(f"DRY-RUN {task.url} -> {task.output_path}")
                continue
            result = download_one(task, overwrite=bool(args.overwrite), timeout=int(args.timeout), retries=int(args.retries))
            if result["status"] == "downloaded":
                ok += 1
            elif result["status"] == "skipped":
                skipped += 1
            else:
                failed += 1
            mf.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            mf.flush()
            if args.sleep_sec > 0:
                time.sleep(float(args.sleep_sec))
    logger.info("[OKX-DOWNLOAD-DONE] downloaded=%d skipped=%d failed=%d manifest=%s", ok, skipped, failed, manifest_out)
    return 1 if failed else 0


def build_tasks(args: argparse.Namespace) -> Iterable[DownloadTask]:
    out_dir = args.out_root / args.kind / args.symbol
    if args.url_template:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date are required with --url-template")
        for d in date_range(parse_date(args.start_date), parse_date(args.end_date)):
            url = args.url_template.format(
                date=d.isoformat(),
                yyyymmdd=d.strftime("%Y%m%d"),
                symbol=args.symbol,
                kind=args.kind,
            )
            yield DownloadTask(url=url, output_path=out_dir / filename_from_url(url, fallback=f"{args.symbol}_{args.kind}_{d.isoformat()}.dat"), date_tag=d.isoformat())
        return
    if args.manifest:
        for idx, url in enumerate(read_manifest_urls(args.manifest), start=1):
            yield DownloadTask(url=url, output_path=out_dir / filename_from_url(url, fallback=f"{args.symbol}_{args.kind}_{idx:06d}.dat"))


def download_one(task: DownloadTask, overwrite: bool, timeout: int, retries: int) -> dict[str, object]:
    task.output_path.parent.mkdir(parents=True, exist_ok=True)
    base = {
        "url": task.url,
        "output_path": str(task.output_path),
        "date_tag": task.date_tag,
        "downloaded_at_utc": utc_now(),
    }
    if task.output_path.exists() and task.output_path.stat().st_size > 0 and not overwrite:
        return {**base, "status": "skipped", "size_bytes": task.output_path.stat().st_size, "sha256": sha256_file(task.output_path)}

    tmp = task.output_path.with_suffix(task.output_path.suffix + ".part")
    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(task.url, headers={"User-Agent": "GlacierPulse/okx-historical-downloader"})
            with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as f:
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            tmp.replace(task.output_path)
            return {**base, "status": "downloaded", "size_bytes": task.output_path.stat().st_size, "sha256": sha256_file(task.output_path), "attempts": attempt}
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = repr(exc)
            logger.warning("[OKX-DOWNLOAD-RETRY] attempt=%d/%d url=%s error=%s", attempt, retries, task.url, last_error)
            time.sleep(min(2 ** attempt, 30))
    if tmp.exists():
        tmp.unlink(missing_ok=True)
    return {**base, "status": "failed", "error": last_error, "attempts": retries}


def read_manifest_urls(path: Path) -> list[str]:
    urls: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            if text.startswith("{"):
                obj = json.loads(text)
                url = str(obj.get("url") or obj.get("download_url") or "").strip()
            else:
                url = text
            if url:
                urls.append(url)
    return urls


def filename_from_url(url: str, fallback: str) -> str:
    name = Path(urlparse(url).path).name
    return name or fallback


def parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> Iterable[date]:
    if end < start:
        raise SystemExit("--end-date must be >= --start-date")
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


if __name__ == "__main__":
    raise SystemExit(main())
