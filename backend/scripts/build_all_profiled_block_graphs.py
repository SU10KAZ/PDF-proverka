#!/usr/bin/env python3
"""Построить профильные графы всех активных версий projects_v2.

Обрабатываются только реальные объекты, чей `legacy_path` находится внутри
рабочего `projects/`. Pytest-объекты, корзина, backup и исторические run-output
не считаются самостоятельными проектами. Индекс кропов можно читать из последнего
run, но канонический граф всегда записывается в `03_analysis/latest`, откуда его
читает TXT API.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


BLOCK_DIRS = ("blocks_stage02_100", "blocks_gemma_100", "blocks")


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except (OSError, ValueError):
        return False


def _blocks_index(version: Path) -> Path | None:
    analysis = version / "03_analysis"
    places = [analysis / "latest"]
    runs = analysis / "runs"
    if runs.is_dir():
        places.extend(sorted((p for p in runs.iterdir() if p.is_dir()), reverse=True))
    legacy = version / "99_service" / "legacy_output"
    if legacy.is_dir():
        places.extend(sorted(legacy.glob("*/_output")))
    for place in places:
        for dirname in BLOCK_DIRS:
            candidate = place / dirname / "index.json"
            if candidate.is_file():
                return candidate
    return None


def discover() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    objects = ROOT / "projects_v2" / "objects"
    projects = ROOT / "projects"
    ready: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for obj in sorted(objects.iterdir() if objects.is_dir() else []):
        try:
            meta = json.loads((obj / "object.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        legacy_path = Path(str(meta.get("legacy_path") or ""))
        if not _inside(legacy_path, projects):
            continue
        for version in sorted(obj.glob("disciplines/*/documents/*/versions/v*")):
            output = version / "03_analysis" / "latest"
            graph = output / "document_graph.json"
            index = _blocks_index(version)
            pdf = version / "02_work" / "document.pdf"
            if not pdf.is_file() and (version / "02_work").is_dir():
                pdfs = sorted((version / "02_work").glob("*.pdf"))
                if pdfs:
                    pdf = pdfs[0]
            parts = version.parts
            discipline = parts[parts.index("disciplines") + 1]
            document = parts[parts.index("documents") + 1]
            base = {
                "object": meta.get("display_name") or obj.name,
                "object_folder": obj.name,
                "discipline": discipline,
                "document": document,
                "version": version.name,
                "version_dir": str(version),
                "output_dir": str(output),
            }
            missing = []
            if not graph.is_file():
                missing.append("document_graph.json")
            if index is None:
                missing.append("blocks index")
            if not pdf.is_file():
                missing.append("PDF")
            if missing:
                skipped.append({**base, "reason": ", ".join(missing)})
                continue
            try:
                payload = json.loads(index.read_text(encoding="utf-8"))
                blocks = sum(
                    1 for item in payload.get("blocks") or []
                    if str(item.get("block_type") or "").lower() == "image"
                )
            except (OSError, json.JSONDecodeError) as exc:
                skipped.append({**base, "reason": f"не читается index: {exc}"})
                continue
            ready.append({
                **base,
                "project_dir": str(version),
                "blocks_index": str(index),
                "blocks_expected": blocks,
            })
    return ready, skipped


def _worker(item: dict[str, Any]) -> dict[str, Any]:
    from backend.app.pipeline.stages.block_context.builder import build_block_context

    started = time.monotonic()
    try:
        summary = asyncio.run(build_block_context(
            Path(item["project_dir"]),
            output_dir=Path(item["output_dir"]),
            blocks_index_path=Path(item["blocks_index"]),
        ))
        return {
            **{key: item[key] for key in (
                "object", "object_folder", "discipline", "document", "version",
                "version_dir", "output_dir", "blocks_expected",
            )},
            "ok": summary.get("status") in {"ok", "partial"},
            "status": summary.get("status"),
            "blocks_total": summary.get("blocks_total", 0),
            "blocks_ready": summary.get("blocks_ready", 0),
            "blocks_failed": summary.get("blocks_failed", 0),
            "source_counts": summary.get("source_counts") or {},
            "profile_counts": summary.get("profile_counts") or {},
            "elapsed_s": round(time.monotonic() - started, 2),
        }
    except Exception as exc:
        return {
            **{key: item[key] for key in (
                "object", "object_folder", "discipline", "document", "version",
                "version_dir", "output_dir", "blocks_expected",
            )},
            "ok": False,
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_s": round(time.monotonic() - started, 2),
        }


def _report(results: list[dict[str, Any]], skipped: list[dict[str, Any]], started: float) -> dict[str, Any]:
    sources: Counter[str] = Counter()
    profiles: Counter[str] = Counter()
    disciplines: Counter[str] = Counter()
    for item in results:
        sources.update(item.get("source_counts") or {})
        profiles.update(item.get("profile_counts") or {})
        disciplines[item.get("discipline") or "?"] += int(item.get("blocks_total") or 0)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.monotonic() - started, 2),
        "versions_discovered": len(results) + len(skipped),
        "versions_processed": len(results),
        "versions_ok": sum(bool(item.get("ok")) for item in results),
        "versions_failed": sum(not item.get("ok") for item in results),
        "versions_skipped_missing_inputs": len(skipped),
        "blocks_total": sum(int(item.get("blocks_total") or 0) for item in results),
        "blocks_ready": sum(int(item.get("blocks_ready") or 0) for item in results),
        "blocks_failed": sum(int(item.get("blocks_failed") or 0) for item in results),
        "source_counts": dict(sources.most_common()),
        "profile_counts": dict(profiles.most_common()),
        "discipline_block_counts": dict(sorted(disciplines.items())),
        "disk_free_gb": round(shutil.disk_usage(ROOT).free / 1024**3, 2),
        "results": sorted(results, key=lambda x: (
            x.get("object", ""), x.get("discipline", ""),
            x.get("document", ""), x.get("version", ""),
        )),
        "skipped": skipped,
    }


def _write_report(payload: dict[str, Any]) -> Path:
    directory = ROOT / "comparison" / "profiled_block_graphs_bulk"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "latest.json"
    temp = target.with_suffix(".json.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, target)
    return target


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--inventory-only", action="store_true")
    args = parser.parse_args()
    ready, skipped = discover()
    if args.limit > 0:
        ready = ready[:args.limit]
    print(
        f"Найдено: {len(ready)} готовых версий / "
        f"{sum(x['blocks_expected'] for x in ready)} блоков; пропущено {len(skipped)}",
        flush=True,
    )
    started = time.monotonic()
    if args.inventory_only:
        path = _write_report(_report([], skipped, started) | {"inventory": ready})
        print(path)
        return 0
    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(_worker, item): item for item in ready}
        for number, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)
            print(
                f"[{number:>3}/{len(ready)}] {'✓' if result.get('ok') else '✗'} "
                f"{result.get('discipline')} {result.get('document')} {result.get('version')}: "
                f"{result.get('blocks_ready', 0)}/{result.get('blocks_total', 0)} "
                f"({result.get('elapsed_s', 0):.1f}s)",
                flush=True,
            )
            if number % 5 == 0 or not result.get("ok"):
                _write_report(_report(results, skipped, started))
    payload = _report(results, skipped, started)
    path = _write_report(payload)
    print(
        f"ГОТОВО: версии {payload['versions_ok']}/{payload['versions_processed']}; "
        f"блоки {payload['blocks_ready']}/{payload['blocks_total']}; отчёт {path}",
        flush=True,
    )
    return 0 if payload["versions_failed"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
