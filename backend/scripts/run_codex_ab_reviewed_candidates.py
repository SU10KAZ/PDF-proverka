#!/usr/bin/env python3
"""Run Codex A/B on reviewed classic-audit candidates.

The script is intentionally conservative:
  * uses projects with accepted expert-review findings;
  * backs up Claude ``03_analysis/latest`` before each Codex retry;
  * copies the Codex output into ``comparison/classic_codex_ab``;
  * restores Claude ``latest`` after every candidate, even on failure;
  * compares Codex findings against the expert-accepted Claude subset.

⚠️  ВНИМАНИЕ — НЕ ЗАПУСКАТЬ НА ЖИВЫХ ПРОД-ДАННЫХ БЕЗ ДОРАБОТКИ.
  В отличие от остальных A/B-скриптов (они пишут только в sandbox), этот
  РЕАЛЬНО гоняет боевой пайплайн на живом проекте и пишет в его
  ``03_analysis/latest`` (``start_from_stage``), затем восстанавливает latest
  из бэкапа. Три незакрытых риска (ревизия 2026-07-11):
    1) restore возвращает ТОЛЬКО ``03_analysis/latest``. Если этап каскадит в
       decision_carryover / verdict-preservation, живой
       ``04_review/expert_review.json`` может быть перезаписан и НЕ восстановлен.
    2) ``copytree_clean`` делает ``rmtree(dst)`` + ``copytree``: kill между ними
       оставляет живой ``latest`` уничтоженным (окно потери данных).
    3) бэкап и restore не атомарны.
  Перед прогоном на боевых проектах: гонять на КОПИИ проекта, либо расширить
  restore на ``04_review`` и сделать его атомарным (temp-swap).
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.services.common.project_service import pinned_object
from backend.scripts.compare_classic_findings_outputs import compare


REPO_ROOT = Path(__file__).resolve().parents[2]
V2_OBJECTS_ROOT = REPO_ROOT / "projects_v2" / "objects"
OUT_ROOT = REPO_ROOT / "comparison" / "classic_codex_ab" / "reviewed_candidates"


@dataclass(frozen=True)
class Candidate:
    object_id: str
    object_slug: str
    discipline: str
    document: str
    version: str = "v001"

    @property
    def project_id(self) -> str:
        return self.document

    @property
    def version_dir(self) -> Path:
        return (
            V2_OBJECTS_ROOT
            / self.object_slug
            / "disciplines"
            / self.discipline
            / "documents"
            / self.document
            / "versions"
            / self.version
        )

    @property
    def latest_dir(self) -> Path:
        return self.version_dir / "03_analysis" / "latest"

    @property
    def review_path(self) -> Path:
        return self.version_dir / "04_review" / "expert_review.json"


DEFAULT_CANDIDATES = [
    Candidate(
        object_id="0b540226",
        object_slug="213_Mosfilmovskaya_31A_KingSons",
        discipline="EOM",
        document="133_23-ГК-ЭО1",
    ),
    Candidate(
        object_id="0b540226",
        object_slug="213_Mosfilmovskaya_31A_KingSons",
        discipline="EOM",
        document="133_23-ГК-ЭО2",
    ),
    Candidate(
        object_id="0b540226",
        object_slug="213_Mosfilmovskaya_31A_KingSons",
        discipline="EOM",
        document="087-РД-ГП5",
    ),
]


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def safe_name(candidate: Candidate, index: int) -> str:
    raw = f"{candidate.object_slug}_{candidate.discipline}_{candidate.document}_{candidate.version}"
    ascii_part = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_")[:80]
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{index:02d}_{ascii_part}_{digest}"


def copytree_clean(src: Path, dst: Path) -> None:
    if not src.is_dir():
        raise FileNotFoundError(src)
    if dst.exists():
        shutil.rmtree(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dst)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_findings(path: Path) -> list[dict[str, Any]]:
    data = load_json(path)
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = data.get("findings") or data.get("items") or data.get("remarks") or []
    else:
        items = []
    return [item for item in items if isinstance(item, dict)]


def finding_id(item: dict[str, Any]) -> str:
    value = item.get("id") or item.get("finding_id") or item.get("item_id") or ""
    return str(value)


def accepted_review_ids(review_path: Path) -> list[str]:
    review = load_json(review_path)
    decisions = review.get("decisions", []) if isinstance(review, dict) else []
    accepted: list[str] = []
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        if decision.get("decision") != "accepted":
            continue
        item_type = decision.get("item_type")
        if item_type not in (None, "", "finding"):
            continue
        item_id = str(decision.get("item_id") or "").strip()
        if item_id:
            accepted.append(item_id)
    return accepted


def has_image_evidence(item: dict[str, Any]) -> bool:
    evidence = item.get("evidence")
    if isinstance(evidence, list):
        for ev in evidence:
            if isinstance(ev, dict) and str(ev.get("type", "")).lower() in {
                "image",
                "graphic",
                "graphics",
                "vision",
                "table_image",
            }:
                return True
    source_text = " ".join(
        str(item.get(key, ""))
        for key in ("source", "origin", "stage", "detected_by", "provider")
    ).lower()
    return any(marker in source_text for marker in ("image", "vision", "block", "openrouter"))


def is_image_only(item: dict[str, Any]) -> bool:
    evidence = item.get("evidence")
    if not isinstance(evidence, list) or not evidence:
        return False
    types = {
        str(ev.get("type", "")).lower()
        for ev in evidence
        if isinstance(ev, dict) and ev.get("type")
    }
    return bool(types) and types <= {"image", "graphic", "graphics", "vision", "table_image"}


def accepted_subsets(candidate: Candidate, out_dir: Path) -> dict[str, Any]:
    baseline_path = candidate.latest_dir / "03_findings.json"
    findings = load_findings(baseline_path)
    by_id = {finding_id(item): item for item in findings if finding_id(item)}
    accepted_ids = accepted_review_ids(candidate.review_path)
    accepted_items = [by_id[item_id] for item_id in accepted_ids if item_id in by_id]
    accepted_no_image = [item for item in accepted_items if not has_image_evidence(item)]
    missing_ids = [item_id for item_id in accepted_ids if item_id not in by_id]

    all_path = out_dir / "accepted_baseline_all.json"
    no_image_path = out_dir / "accepted_baseline_no_image_evidence.json"
    write_json(all_path, {"findings": accepted_items})
    write_json(no_image_path, {"findings": accepted_no_image})

    return {
        "accepted_review_ids": len(accepted_ids),
        "accepted_present_in_baseline": len(accepted_items),
        "accepted_missing_in_baseline": missing_ids,
        "accepted_with_image_evidence": sum(1 for item in accepted_items if has_image_evidence(item)),
        "accepted_image_only": sum(1 for item in accepted_items if is_image_only(item)),
        "accepted_no_image_evidence": len(accepted_no_image),
        "accepted_all_path": str(all_path),
        "accepted_no_image_path": str(no_image_path),
    }


def compare_against_accepted(
    accepted_path: Path,
    codex_findings_path: Path,
    out_path: Path,
    *,
    threshold: float,
) -> dict[str, Any]:
    report = compare(accepted_path, codex_findings_path, threshold=threshold)
    write_json(out_path, report)
    return report


async def wait_for_item(job_id: str, *, timeout_sec: int, poll_sec: int) -> dict[str, Any]:
    from backend.app.pipeline.manager import pipeline_manager

    deadline = time.monotonic() + timeout_sec
    last_status = None
    while time.monotonic() < deadline:
        queue = pipeline_manager.get_batch_queue()
        if queue:
            for item in queue.items:
                if item.job_id == job_id:
                    status = item.status
                    if status != last_status:
                        print(f"[wait] {item.project_id}/{item.version_id}: {status}", flush=True)
                        last_status = status
                    if status in {"completed", "failed", "cancelled", "skipped"}:
                        return {
                            "status": status,
                            "error": item.error,
                            "project_id": item.project_id,
                            "version_id": item.version_id,
                            "job_id": item.job_id,
                        }
        await asyncio.sleep(poll_sec)

    try:
        await pipeline_manager.cancel_batch()
    except Exception:
        pass
    return {"status": "timeout", "error": f"Timed out after {timeout_sec}s", "job_id": job_id}


async def run_candidate(
    candidate: Candidate,
    out_dir: Path,
    *,
    threshold: float,
    timeout_sec: int,
    poll_sec: int,
    stage: str,
    dry_run: bool,
) -> dict[str, Any]:
    from backend.app.pipeline.manager import pipeline_manager

    latest = candidate.latest_dir
    baseline_findings = latest / "03_findings.json"
    if not baseline_findings.is_file():
        raise FileNotFoundError(baseline_findings)
    if not candidate.review_path.is_file():
        raise FileNotFoundError(candidate.review_path)

    baseline_dir = out_dir / "baseline_latest"
    codex_dir = out_dir / "codex_latest"
    codex_run_dir = out_dir / "codex_run"
    review_copy = out_dir / "expert_review.json"

    copytree_clean(latest, baseline_dir)
    shutil.copy2(candidate.review_path, review_copy)
    subsets = accepted_subsets(candidate, out_dir)

    if dry_run:
        return {
            "status": "dry_run",
            "candidate": candidate.__dict__,
            "out_dir": str(out_dir),
            "baseline_findings": len(load_findings(baseline_findings)),
            **subsets,
        }

    job_info: dict[str, Any] | None = None
    try:
        with pinned_object(candidate.object_id):
            job = await pipeline_manager.start_from_stage(
                candidate.project_id,
                stage,
                version_id=candidate.version,
            )
        print(
            f"[run] {candidate.project_id}/{candidate.version}: job={job.job_id}, stage={stage}",
            flush=True,
        )
        job_info = await wait_for_item(
            job.job_id,
            timeout_sec=timeout_sec,
            poll_sec=poll_sec,
        )

        if latest.exists():
            copytree_clean(latest, codex_dir)

        run_src = candidate.version_dir / "03_analysis" / "runs" / job.job_id
        if run_src.exists():
            copytree_clean(run_src, codex_run_dir)

        codex_findings = codex_dir / "03_findings.json"
        if not codex_findings.is_file():
            raise FileNotFoundError(codex_findings)

        all_report = compare_against_accepted(
            Path(subsets["accepted_all_path"]),
            codex_findings,
            out_dir / "comparison_accepted_all.json",
            threshold=threshold,
        )
        no_image_report = compare_against_accepted(
            Path(subsets["accepted_no_image_path"]),
            codex_findings,
            out_dir / "comparison_accepted_no_image_evidence.json",
            threshold=threshold,
        )

        result = {
            "status": job_info["status"],
            "error": job_info.get("error"),
            "candidate": candidate.__dict__,
            "out_dir": str(out_dir),
            "job_id": job.job_id,
            "baseline_findings": len(load_findings(baseline_findings)),
            "codex_findings": len(load_findings(codex_findings)),
            **subsets,
            "accepted_all_matched": all_report["matched"],
            "accepted_all_recall": all_report["candidate_recall_vs_baseline"],
            "accepted_all_precision": all_report["candidate_precision_vs_baseline"],
            "accepted_no_image_matched": no_image_report["matched"],
            "accepted_no_image_recall": no_image_report["candidate_recall_vs_baseline"],
            "accepted_no_image_precision": no_image_report["candidate_precision_vs_baseline"],
        }
        write_json(out_dir / "candidate_summary.json", result)
        return result
    finally:
        # Restore Claude latest regardless of Codex success/failure.
        if baseline_dir.is_dir():
            copytree_clean(baseline_dir, latest)


async def amain() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-candidates", type=int, default=len(DEFAULT_CANDIDATES))
    parser.add_argument("--stage", default="text_analysis", choices=["text_analysis", "findings_merge"])
    parser.add_argument("--timeout-sec", type=int, default=3600)
    parser.add_argument("--poll-sec", type=int, default=10)
    parser.add_argument("--threshold", type=float, default=0.38)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Keep this run focused on findings/norms and avoid paying for optimization.
    os.environ.setdefault("PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", "true")
    os.environ["AUDIT_RESUME_SKIP_OPTIMIZATION"] = "true"

    run_dir = OUT_ROOT / f"run_{utc_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)

    selected = DEFAULT_CANDIDATES[: max(0, args.max_candidates)]
    summaries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        item_dir = run_dir / safe_name(candidate, index)
        item_dir.mkdir(parents=True, exist_ok=True)
        print(f"[candidate] {index}/{len(selected)} {candidate.project_id}/{candidate.version}", flush=True)
        try:
            summary = await run_candidate(
                candidate,
                item_dir,
                threshold=args.threshold,
                timeout_sec=args.timeout_sec,
                poll_sec=args.poll_sec,
                stage=args.stage,
                dry_run=args.dry_run,
            )
        except Exception as exc:
            summary = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "candidate": candidate.__dict__,
                "out_dir": str(item_dir),
            }
            write_json(item_dir / "candidate_summary.json", summary)
            print(f"[error] {candidate.project_id}: {summary['error']}", flush=True)
        summaries.append(summary)
        write_json(run_dir / "summary.json", {"run_dir": str(run_dir), "items": summaries})

    compact = [
        {
            "project_id": item.get("candidate", {}).get("document"),
            "status": item.get("status"),
            "accepted": item.get("accepted_present_in_baseline"),
            "codex_findings": item.get("codex_findings"),
            "matched": item.get("accepted_all_matched"),
            "recall": item.get("accepted_all_recall"),
            "matched_no_image": item.get("accepted_no_image_matched"),
            "recall_no_image": item.get("accepted_no_image_recall"),
            "error": item.get("error"),
        }
        for item in summaries
    ]
    print(json.dumps({"run_dir": str(run_dir), "items": compact}, ensure_ascii=False, indent=2))
    return 0 if all(item.get("status") in {"completed", "dry_run"} for item in summaries) else 1


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())
