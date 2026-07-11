#!/usr/bin/env python3
"""Run Codex optimization A/B against existing Claude/latest baselines by discipline.

The runner does not mutate project ``03_analysis/latest``. For every selected
document it builds an isolated v2-like work layout under
``comparison/classic_codex_ab/optimization_by_discipline`` and asks
``codex exec`` to write only ``optimization.json`` there.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from backend.app.pipeline.stages.optimization.visual_context import (
    collect_optimization_visual_context,
)
from backend.app.pipeline.stages.optimization.prescan import scan_optimization_opportunities
from backend.app.pipeline.stages.prepare.task_builder import prepare_optimization_task
from backend.app.services.llm.codex_runner import find_codex_cli, run_codex_exec


REPO_ROOT = Path(__file__).resolve().parents[2]
V2_OBJECTS_ROOT = REPO_ROOT / "projects_v2" / "objects"
OUT_ROOT = REPO_ROOT / "comparison" / "classic_codex_ab" / "optimization_by_discipline"
DISCIPLINES = ("AI", "AR", "EOM", "GP", "KJ", "KM", "OV", "PT", "SS", "TX", "VK")


@dataclass(frozen=True)
class Candidate:
    object_slug: str
    discipline: str
    document: str
    version: str
    version_dir: Path
    latest_dir: Path
    baseline_items: int
    prescan_items: int
    optimization_model: str

    @property
    def project_id(self) -> str:
        return self.document


def utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def safe_name(candidate: Candidate, index: int) -> str:
    raw = f"{candidate.object_slug}_{candidate.discipline}_{candidate.document}_{candidate.version}"
    ascii_part = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_")[:90]
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return f"{index:02d}_{ascii_part}_{digest}"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def copy_file(src: Path, dst: Path) -> None:
    if not src.is_file():
        raise FileNotFoundError(src)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def find_project_info(version_dir: Path) -> Path:
    for candidate in (
        version_dir / "01_input" / "project_info.json",
        version_dir / "02_work" / "project_info.json",
        version_dir / "project_info.json",
    ):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"project_info.json not found under {version_dir}")


def find_document_md(version_dir: Path) -> Path:
    for candidate in (
        version_dir / "02_work" / "document.md",
        *sorted((version_dir / "01_input").glob("*_document.md")),
        *sorted((version_dir / "01_input").glob("*.md")),
    ):
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(f"document.md not found under {version_dir}")


def optimization_items_count(path: Path) -> int:
    try:
        data = load_json(path)
    except Exception:
        return 0
    items = data.get("items") if isinstance(data, dict) else None
    return len(items) if isinstance(items, list) else 0


def optimization_model(latest_dir: Path) -> str:
    path = latest_dir / "pipeline_log.json"
    if not path.is_file():
        return ""
    try:
        data = load_json(path)
        return str((((data.get("stages") or {}).get("optimization") or {}).get("model") or ""))
    except Exception:
        return ""


def required_baseline_files(latest_dir: Path) -> list[Path]:
    return [
        latest_dir / "01_text_analysis.json",
        latest_dir / "02_blocks_analysis.json",
        latest_dir / "03_findings.json",
        latest_dir / "optimization.json",
    ]


def candidate_from_version_dir(version_dir: Path, *, require_claude_baseline: bool = False) -> Candidate | None:
    if "test_" in str(version_dir):
        return None
    parts = version_dir.parts
    try:
        marker = parts.index("projects_v2")
        object_slug = parts[marker + 2]
        discipline = parts[marker + 4]
        document = parts[marker + 6]
        version = parts[marker + 8]
    except (ValueError, IndexError):
        return None
    if discipline not in DISCIPLINES:
        return None

    latest_dir = version_dir / "03_analysis" / "latest"
    if not latest_dir.is_dir():
        return None
    if not all(path.is_file() for path in required_baseline_files(latest_dir)):
        return None

    baseline_items = optimization_items_count(latest_dir / "optimization.json")
    if baseline_items <= 0:
        return None
    model = optimization_model(latest_dir)
    if require_claude_baseline and "claude" not in model.lower():
        return None

    try:
        md_text = find_document_md(version_dir).read_text(encoding="utf-8", errors="replace")
        prescan_items = len(scan_optimization_opportunities(md_text, section=discipline, max_candidates=16))
    except Exception:
        prescan_items = 0

    return Candidate(
        object_slug=object_slug,
        discipline=discipline,
        document=document,
        version=version,
        version_dir=version_dir,
        latest_dir=latest_dir,
        baseline_items=baseline_items,
        prescan_items=prescan_items,
        optimization_model=model,
    )


def collect_candidates(*, require_claude_baseline: bool = False) -> list[Candidate]:
    candidates: list[Candidate] = []
    for version_dir in V2_OBJECTS_ROOT.glob("*/disciplines/*/documents/*/versions/*"):
        if not version_dir.is_dir():
            continue
        candidate = candidate_from_version_dir(
            version_dir,
            require_claude_baseline=require_claude_baseline,
        )
        if candidate:
            candidates.append(candidate)
    return candidates


def select_candidates(candidates: list[Candidate], *, per_discipline: int) -> list[Candidate]:
    by_discipline: dict[str, list[Candidate]] = defaultdict(list)
    for candidate in candidates:
        by_discipline[candidate.discipline].append(candidate)

    selected: list[Candidate] = []
    for discipline in DISCIPLINES:
        bucket = by_discipline.get(discipline, [])
        ranked = sorted(
            bucket,
            key=lambda c: (
                c.prescan_items > 0,
                c.baseline_items,
                c.prescan_items,
                c.document,
            ),
            reverse=True,
        )
        selected.extend(ranked[:per_discipline])
    return selected


def filter_candidates_with_images(
    candidates: list[Candidate],
    *,
    max_images: int,
) -> list[Candidate]:
    filtered: list[Candidate] = []
    for candidate in candidates:
        visual_context = collect_optimization_visual_context(
            candidate.latest_dir,
            max_images=max_images,
        )
        if visual_context.image_paths:
            filtered.append(candidate)
    return filtered


def normalize_text(value: Any) -> str:
    if isinstance(value, list):
        value = " ".join(str(v) for v in value)
    text = str(value or "").lower().replace("ё", "е")
    text = text.replace("×", "x").replace("х", "x")
    text = re.sub(r"[^0-9a-zа-я]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def item_text(item: dict[str, Any]) -> str:
    return normalize_text(
        " ".join(
            str(item.get(key) or "")
            for key in ("spec_items", "current", "proposed", "type", "section", "risks", "norm")
        )
    )


_MATCH_STOPWORDS = {
    "без", "более", "будет", "быть", "все", "для", "его", "или", "как", "над", "при", "что",
    "это", "этой", "этот", "этого", "после", "перед", "только", "нужно", "требуется",
    "проверить", "применить", "заменить", "поз", "номер", "номера", "лист", "разд", "пункт",
    "система", "системы", "проект", "проектных", "проектной", "решение", "решения",
    "труба", "трубы", "труб", "узел", "узлы", "сп", "гост",
}


def _match_tokens(text: str) -> set[str]:
    tokens = set()
    for token in text.split():
        if token in _MATCH_STOPWORDS:
            continue
        if len(token) < 4 and not any(char.isdigit() for char in token):
            continue
        tokens.add(token)
    return tokens


def token_overlap_similarity(left_text: str, right_text: str) -> float:
    left_tokens = _match_tokens(left_text)
    right_tokens = _match_tokens(right_text)
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens & right_tokens
    if not intersection:
        return 0.0
    containment = len(intersection) / min(len(left_tokens), len(right_tokens))
    jaccard = len(intersection) / len(left_tokens | right_tokens)
    return (containment * 0.75) + (jaccard * 0.25)


def similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    left_text = item_text(left)
    right_text = item_text(right)
    return max(
        SequenceMatcher(None, left_text, right_text).ratio(),
        token_overlap_similarity(left_text, right_text),
    )


def load_optimization_items(path: Path) -> list[dict[str, Any]]:
    try:
        data = load_json(path)
    except Exception:
        return []
    items = data.get("items") if isinstance(data, dict) else None
    return [item for item in (items or []) if isinstance(item, dict)]


def compare_optimization(
    baseline_path: Path,
    codex_path: Path,
    out_dir: Path,
    *,
    threshold: float,
) -> dict[str, Any]:
    baseline = load_optimization_items(baseline_path)
    codex = load_optimization_items(codex_path)
    pairs: list[tuple[float, int, int]] = []
    for b_idx, b_item in enumerate(baseline):
        for c_idx, c_item in enumerate(codex):
            score = similarity(b_item, c_item)
            if score >= threshold:
                pairs.append((score, b_idx, c_idx))
    pairs.sort(reverse=True)

    used_baseline: set[int] = set()
    used_codex: set[int] = set()
    matches: list[dict[str, Any]] = []
    for score, b_idx, c_idx in pairs:
        if b_idx in used_baseline or c_idx in used_codex:
            continue
        used_baseline.add(b_idx)
        used_codex.add(c_idx)
        b_item = baseline[b_idx]
        c_item = codex[c_idx]
        matches.append({
            "score": round(score, 3),
            "baseline_id": b_item.get("id"),
            "codex_id": c_item.get("id"),
            "baseline_type": b_item.get("type"),
            "codex_type": c_item.get("type"),
            "type_match": b_item.get("type") == c_item.get("type"),
            "baseline_current": b_item.get("current"),
            "codex_current": c_item.get("current"),
            "baseline_spec_items": b_item.get("spec_items") or [],
            "codex_spec_items": c_item.get("spec_items") or [],
        })

    baseline_count = len(baseline)
    codex_count = len(codex)
    matched = len(matches)
    report = {
        "baseline_items": baseline_count,
        "codex_items": codex_count,
        "matched": matched,
        "codex_recall_vs_baseline": round(matched / baseline_count, 3) if baseline_count else 0,
        "codex_precision_vs_baseline": round(matched / codex_count, 3) if codex_count else 0,
        "baseline_by_type": dict(Counter(str(item.get("type") or "?") for item in baseline)),
        "codex_by_type": dict(Counter(str(item.get("type") or "?") for item in codex)),
        "type_match_rate_on_matched": round(
            sum(1 for item in matches if item["type_match"]) / matched, 3
        ) if matched else 0,
        "avg_match_score": round(sum(item["score"] for item in matches) / matched, 3) if matched else 0,
        "unmatched_baseline": [baseline[idx] for idx in range(len(baseline)) if idx not in used_baseline],
        "unmatched_codex": [codex[idx] for idx in range(len(codex)) if idx not in used_codex],
        "matches": matches,
    }
    write_json(out_dir / "optimization_comparison.json", report)
    write_json(out_dir / "matches.json", matches)
    write_json(out_dir / "unmatched_baseline.json", report["unmatched_baseline"])
    write_json(out_dir / "unmatched_codex.json", report["unmatched_codex"])
    return report


@contextmanager
def temporary_env(values: dict[str, str]):
    previous: dict[str, str | None] = {}
    for key, value in values.items():
        previous[key] = os.environ.get(key)
        os.environ[key] = value
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def prepare_isolated_layout(
    candidate: Candidate,
    item_dir: Path,
    *,
    copy_images: bool = False,
) -> tuple[Path, Path, dict[str, Any]]:
    input_dir = item_dir / "input"
    version_dir = item_dir / "work_version"
    output_dir = item_dir / "codex_output"
    for path in (input_dir, version_dir, output_dir):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    project_info_src = find_project_info(candidate.version_dir)
    document_md_src = find_document_md(candidate.version_dir)

    copy_file(project_info_src, input_dir / "project_info.json")
    copy_file(document_md_src, input_dir / "document.md")
    copy_file(candidate.latest_dir / "optimization.json", input_dir / "baseline_optimization.json")

    copy_file(project_info_src, version_dir / "01_input" / "project_info.json")
    copy_file(document_md_src, version_dir / "02_work" / "document.md")

    for name in ("01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json"):
        copy_file(candidate.latest_dir / name, output_dir / name)
        copy_file(candidate.latest_dir / name, input_dir / name)
    if (candidate.latest_dir / "document_graph.json").is_file():
        copy_file(candidate.latest_dir / "document_graph.json", output_dir / "document_graph.json")
        copy_file(candidate.latest_dir / "document_graph.json", input_dir / "document_graph.json")
    if copy_images:
        for dirname in ("blocks", "blocks_gemma_100", "blocks_gemma_300"):
            src_dir = candidate.latest_dir / dirname
            if src_dir.is_dir():
                shutil.copytree(src_dir, output_dir / dirname, dirs_exist_ok=True)

    project_info = load_json(project_info_src)
    return version_dir, output_dir, project_info


async def run_candidate(
    candidate: Candidate,
    item_dir: Path,
    *,
    model: str | None,
    timeout_sec: int,
    threshold: float,
    dry_run: bool,
    with_images: bool,
    max_images: int,
) -> dict[str, Any]:
    version_dir, output_dir, project_info = prepare_isolated_layout(
        candidate,
        item_dir,
        copy_images=with_images,
    )
    summary: dict[str, Any] = {
        "status": "dry_run" if dry_run else "running",
        "candidate": {
            **asdict(candidate),
            "version_dir": str(candidate.version_dir),
            "latest_dir": str(candidate.latest_dir),
        },
        "item_dir": str(item_dir),
        "baseline_optimization": str(candidate.latest_dir / "optimization.json"),
        "codex_optimization": str(output_dir / "optimization.json"),
        "with_images": with_images,
    }
    write_json(item_dir / "candidate_summary.json", summary)
    if dry_run:
        return summary

    with temporary_env({
        "AUDIT_VERSION_DIR": str(version_dir),
        "AUDIT_OUTPUT_DIR": str(output_dir),
        "AUDIT_CODEX_SANDBOX": os.environ.get("AUDIT_CODEX_SANDBOX", "danger-full-access"),
    }):
        task = prepare_optimization_task(project_info, candidate.project_id)
        image_paths: list[Path] = []
        if with_images:
            visual_context = collect_optimization_visual_context(
                output_dir,
                max_images=max_images,
            )
            image_paths = visual_context.image_paths
            write_json(item_dir / "visual_context.json", visual_context.to_dict())
            if visual_context.prompt_section:
                task = task.rstrip() + "\n\n" + visual_context.prompt_section
        (item_dir / "optimization_task.md").write_text(task, encoding="utf-8")
        exit_code, output, cli_result = await run_codex_exec(
            task,
            timeout=timeout_sec,
            stage="optimization",
            project_id=candidate.project_id,
            model=model,
            image_paths=image_paths,
        )

    (item_dir / "codex_exec_output.txt").write_text(output or "", encoding="utf-8")
    summary.update({
        "exit_code": exit_code,
        "duration_ms": cli_result.duration_ms,
        "codex_exec_error": cli_result.is_error,
        "attached_images": len(image_paths) if with_images else 0,
    })

    codex_path = output_dir / "optimization.json"
    if exit_code != 0 or not codex_path.is_file():
        summary.update({
            "status": "error",
            "error": "codex_failed_or_optimization_missing",
            "output_tail": (output or "")[-2000:],
        })
        write_json(item_dir / "candidate_summary.json", summary)
        return summary

    comparison = compare_optimization(
        candidate.latest_dir / "optimization.json",
        codex_path,
        item_dir,
        threshold=threshold,
    )
    summary.update({
        "status": "done",
        "baseline_items": comparison["baseline_items"],
        "codex_items": comparison["codex_items"],
        "matched": comparison["matched"],
        "recall": comparison["codex_recall_vs_baseline"],
        "precision": comparison["codex_precision_vs_baseline"],
        "type_match_rate": comparison["type_match_rate_on_matched"],
        "avg_match_score": comparison["avg_match_score"],
    })
    write_json(item_dir / "candidate_summary.json", summary)
    return summary


def compact_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for item in items:
        candidate = item.get("candidate") or {}
        result.append({
            "discipline": candidate.get("discipline"),
            "document": candidate.get("document"),
            "version": candidate.get("version"),
            "status": item.get("status"),
            "baseline": item.get("baseline_items") or candidate.get("baseline_items"),
            "codex": item.get("codex_items"),
            "matched": item.get("matched"),
            "recall": item.get("recall"),
            "precision": item.get("precision"),
            "attached_images": item.get("attached_images"),
            "error": item.get("error"),
        })
    return result


async def amain() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--per-discipline", type=int, default=2)
    parser.add_argument("--timeout-sec", type=int, default=1200)
    parser.add_argument("--threshold", type=float, default=0.30)
    parser.add_argument("--model", default=os.environ.get("AUDIT_CODEX_MODEL", "gpt-5.4"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-total", type=int, default=0, help="optional cap for smoke runs")
    parser.add_argument("--require-claude-baseline", action="store_true")
    parser.add_argument("--with-images", action="store_true")
    parser.add_argument("--max-images", type=int, default=12)
    parser.add_argument(
        "--require-images",
        action="store_true",
        help="select only candidates that have attachable drawing block PNGs",
    )
    parser.add_argument(
        "--document-contains",
        default="",
        help="case-insensitive substring filter for document name",
    )
    parser.add_argument(
        "--disciplines",
        default="",
        help="comma-separated discipline filter, e.g. AR,GP,KJ",
    )
    args = parser.parse_args()

    candidates = collect_candidates(require_claude_baseline=args.require_claude_baseline)
    if args.require_images:
        candidates = filter_candidates_with_images(candidates, max_images=args.max_images)
    if args.disciplines.strip():
        wanted = {
            item.strip().upper()
            for item in args.disciplines.split(",")
            if item.strip()
        }
        unknown = sorted(wanted - set(DISCIPLINES))
        if unknown:
            parser.error(f"unknown disciplines: {', '.join(unknown)}")
        candidates = [item for item in candidates if item.discipline in wanted]
    if args.document_contains.strip():
        needle = args.document_contains.casefold().strip()
        candidates = [item for item in candidates if needle in item.document.casefold()]
    selected = select_candidates(candidates, per_discipline=max(1, args.per_discipline))
    if args.max_total > 0:
        selected = selected[: args.max_total]

    run_dir = OUT_ROOT / f"{'dry_run' if args.dry_run else 'run'}_{utc_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "selection.json", [asdict(item) | {
        "version_dir": str(item.version_dir),
        "latest_dir": str(item.latest_dir),
    } for item in selected])

    print(json.dumps({
        "run_dir": str(run_dir),
        "dry_run": args.dry_run,
        "with_images": args.with_images,
        "max_images": args.max_images,
        "codex_cli": find_codex_cli(),
        "selected": [
            {
                "discipline": item.discipline,
                "document": item.document,
                "version": item.version,
                "baseline_items": item.baseline_items,
                "prescan_items": item.prescan_items,
                "model": item.optimization_model,
                "version_dir": str(item.version_dir),
            }
            for item in selected
        ],
    }, ensure_ascii=False, indent=2), flush=True)

    summaries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        item_dir = run_dir / safe_name(candidate, index)
        print(f"[candidate] {index}/{len(selected)} {candidate.discipline}/{candidate.document}/{candidate.version}", flush=True)
        try:
            summary = await run_candidate(
                candidate,
                item_dir,
                model=args.model,
                timeout_sec=args.timeout_sec,
                threshold=args.threshold,
                dry_run=args.dry_run,
                with_images=args.with_images,
                max_images=args.max_images,
            )
        except Exception as exc:
            summary = {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
                "candidate": {
                    **asdict(candidate),
                    "version_dir": str(candidate.version_dir),
                    "latest_dir": str(candidate.latest_dir),
                },
                "item_dir": str(item_dir),
            }
            write_json(item_dir / "candidate_summary.json", summary)
            print(f"[error] {candidate.discipline}/{candidate.document}: {summary['error']}", flush=True)
        summaries.append(summary)
        write_json(run_dir / "summary.json", {
            "run_dir": str(run_dir),
            "dry_run": args.dry_run,
            "items": summaries,
            "compact": compact_summary(summaries),
        })

        if summary.get("status") == "error" and "usage limit" in str(summary.get("output_tail") or summary.get("error") or "").lower():
            print("[stop] usage limit detected", flush=True)
            break

    print(json.dumps({
        "run_dir": str(run_dir),
        "compact": compact_summary(summaries),
    }, ensure_ascii=False, indent=2), flush=True)
    return 0 if all(item.get("status") in {"done", "dry_run"} for item in summaries) else 1


def main() -> int:
    return asyncio.run(amain())


if __name__ == "__main__":
    raise SystemExit(main())
