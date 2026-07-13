#!/usr/bin/env python3
"""Mass backfill text-layer shadow highlights for current projects_v2 versions.

The command is read-only by default. With ``--apply`` it creates or refreshes
only ``textlayer_highlights_shadow.json`` inside every current version's
``03_analysis/latest`` directory. It never modifies ``03_findings.json`` and
never calls an LLM.

Examples::

    python scripts/backfill_textlayer_highlights.py
    python scripts/backfill_textlayer_highlights.py --apply
    python scripts/backfill_textlayer_highlights.py --apply --project 133_23-ГК-ЭО1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.findings_merge.ground_highlights_textlayer import (  # noqa: E402
    SHADOW_FILENAME,
    backfill_textlayer_highlights,
)
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter  # noqa: E402


@dataclass(frozen=True)
class Target:
    object_folder: str
    discipline: str
    document_code: str
    version_id: str
    version_dir: str
    output_dir: str

    @property
    def key(self) -> str:
        return f"{self.object_folder}/{self.discipline}/{self.document_code}@{self.version_id}"


def _matches_project(doc: dict, requested: set[str]) -> bool:
    if not requested:
        return True
    code = str(doc.get("document_code") or "")
    scoped = f"{doc.get('object_folder')}/{doc.get('discipline')}/{code}"
    return code in requested or scoped in requested


def collect_targets(
    adapter: ProjectsV2Adapter,
    requested: Optional[set[str]] = None,
) -> tuple[list[Target], Counter]:
    """Collect current versions that have canonical final findings."""
    requested = requested or set()
    targets: list[Target] = []
    stats: Counter = Counter()
    for doc in adapter.list_documents():
        stats["documents_total"] += 1
        if not _matches_project(doc, requested):
            stats["filtered_out"] += 1
            continue
        stats["documents_selected"] += 1
        version_id = adapter.current_version_id(Path(doc["doc_dir"]))
        if not version_id:
            stats["skipped_no_current_version"] += 1
            continue
        version_dir = adapter.version_dir(Path(doc["doc_dir"]), version_id)
        output_dir = adapter.latest_dir(Path(doc["doc_dir"]), version_id)
        if not (output_dir / "03_findings.json").is_file():
            stats["skipped_no_findings"] += 1
            continue
        targets.append(Target(
            object_folder=str(doc.get("object_folder") or ""),
            discipline=str(doc.get("discipline") or ""),
            document_code=str(doc.get("document_code") or Path(doc["doc_dir"]).name),
            version_id=version_id,
            version_dir=str(version_dir),
            output_dir=str(output_dir),
        ))
    stats["targets"] = len(targets)
    return targets, stats


def process_target(target: Target) -> dict:
    started = time.perf_counter()
    try:
        result = backfill_textlayer_highlights(
            target.version_dir,
            target.output_dir,
            enabled=True,
            shadow=True,
            override_existing=False,
        )
        artifact = Path(target.output_dir) / SHADOW_FILENAME
        return {
            **asdict(target),
            "key": target.key,
            "status": "completed" if artifact.is_file() else "no_artifact",
            "checked": int(result.get("checked") or 0),
            "grounded": int(result.get("grounded") or 0),
            "coverage": float(result.get("coverage") or 0.0),
            "agreement_iou_mean": result.get("agreement_iou_mean"),
            "artifact": str(artifact) if artifact.is_file() else None,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
        }
    except Exception as exc:  # fail-soft per document; the batch continues
        return {
            **asdict(target),
            "key": target.key,
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "checked": 0,
            "grounded": 0,
            "coverage": 0.0,
            "artifact": None,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
        }


def build_report(rows: list[dict], inventory: Counter, elapsed: float) -> dict:
    statuses = Counter(row["status"] for row in rows)
    checked = sum(int(row.get("checked") or 0) for row in rows)
    grounded = sum(int(row.get("grounded") or 0) for row in rows)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "shadow",
        "llm_tokens": 0,
        "inventory": dict(inventory),
        "summary": {
            "processed": len(rows),
            "completed": statuses["completed"],
            "failed": statuses["failed"],
            "no_artifact": statuses["no_artifact"],
            "projects_with_highlights": sum(1 for row in rows if int(row.get("grounded") or 0) > 0),
            "findings_checked": checked,
            "findings_grounded": grounded,
            "coverage": round(grounded / checked, 5) if checked else 0.0,
            "elapsed_seconds": round(elapsed, 2),
        },
        "projects": sorted(rows, key=lambda row: row["key"]),
    }


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="create/refresh shadow artifacts")
    parser.add_argument(
        "--project",
        action="append",
        default=[],
        help="limit to document_code or object/discipline/document_code (repeatable)",
    )
    parser.add_argument("--workers", type=int, default=4, help="parallel workers (default: 4)")
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "projects_v2" / "_system" / "textlayer_highlights_backfill.json",
        help="aggregate report path",
    )
    args = parser.parse_args()

    adapter = ProjectsV2Adapter()
    if not adapter.is_available():
        print("ERROR: projects_v2 is unavailable", file=sys.stderr)
        return 2

    targets, inventory = collect_targets(adapter, set(args.project))
    print(
        f"documents={inventory['documents_total']} selected={inventory['documents_selected']} "
        f"targets={len(targets)} skipped_no_findings={inventory['skipped_no_findings']}"
    )
    if not args.apply:
        for target in targets:
            print(f"  [DRY] {target.key}")
        print("Dry-run: no files written. Use --apply to run the backfill.")
        return 0

    workers = max(1, min(int(args.workers or 1), 16))
    started = time.perf_counter()
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_target, target): target for target in targets}
        for index, future in enumerate(as_completed(futures), start=1):
            row = future.result()
            rows.append(row)
            print(
                f"[{index}/{len(targets)}] {row['status']:>11} "
                f"grounded={row.get('grounded', 0):>3}/{row.get('checked', 0):<3} {row['key']}",
                flush=True,
            )

    report = build_report(rows, inventory, time.perf_counter() - started)
    _atomic_json(args.report, report)
    summary = report["summary"]
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"Report: {args.report}")
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
