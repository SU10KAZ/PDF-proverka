# -*- coding: utf-8 -*-
"""Pipeline V2 — Dry Run / Orchestrator (offline-связка этапов 1–4).

Backend-only слой, который объединяет четыре изолированных этапа Pipeline V2 в
ОДИН локальный offline dry-run и пишет на диск их артефакты:

    left_package / right_package (подготовленные пакеты)
      → [1] build_normalized_document_model      → *_normalized_document_model.json
      → [2] match_normalized_documents           → block_matching_report.json
      → [3] extract_entities_for_matched_documents → entity_extraction_report.json
      → [4] diff_entity_extraction_report        → entity_diff_report.json
      → pipeline_v2_summary.json / .md + pipeline_v2_manifest.json

Это НЕ UI, НЕ Opus/critic, НЕ замена старой логики. Только оркестрация уже
готовых чистых функций этапов 1–4 + сводка/манифест. Никаких сетевых вызовов,
Qwen/Opus/OCR/PDF-render и скачивания `crop_url`.

Fail-soft: writer'ы атомарны (tmp + os.replace), поэтому частично записанного
broken JSON не остаётся. Если этап падает — `status=failed`, в summary короткая
ошибка, уже записанные артефакты остаются валидными, последующие не пишутся.

См. docs/stage_comparison_pipeline_v2_dry_run.md.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.pipeline_v2_prepared_ingest import (
    build_normalized_document_model,
    write_normalized_document_model,
)
from backend.app.services.stage_comparison.pipeline_v2_block_matching import (
    match_normalized_documents,
    write_block_matching_report,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_extraction import (
    extract_entities_for_matched_documents,
    write_entity_extraction_report,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_diff import (
    diff_entity_extraction_report,
    write_entity_diff_report,
)

SUMMARY_VERSION = 1
SUMMARY_KIND = "stage_comparison_pipeline_v2_dry_run_summary"
MANIFEST_KIND = "stage_comparison_pipeline_v2_manifest"
NEXT_RECOMMENDED_STAGE = "delta_explanation_or_graphic_descriptor"

# artifact-ключ → имя файла в out_dir
_ARTIFACT_FILENAMES = {
    "left_model": "left_normalized_document_model.json",
    "right_model": "right_normalized_document_model.json",
    "block_matching": "block_matching_report.json",
    "entity_extraction": "entity_extraction_report.json",
    "entity_diff": "entity_diff_report.json",
    "summary_json": "pipeline_v2_summary.json",
    "summary_md": "pipeline_v2_summary.md",
    "manifest": "pipeline_v2_manifest.json",
}
# Артефакты, перечисляемые в манифесте (manifest сам себя не хеширует).
_MANIFEST_ARTIFACT_KEYS = [
    "left_model", "right_model", "block_matching", "entity_extraction",
    "entity_diff", "summary_json", "summary_md",
]


# ─── низкоуровневые помощники ────────────────────────────────────────────────


def _clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _atomic_write_text(out_path: str | Path, text: str) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_kind(path: Path) -> Optional[str]:
    if path.suffix.lower() != ".json":
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("kind") if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None


# ─── package paths / artifact paths ──────────────────────────────────────────


def normalize_package_paths(package: Any) -> dict:
    """Привести входной пакет к ``{pdf_path, result_json_path, document_md_path,
    ocr_html_path}`` (str | None). Bare-строка трактуется как result_json_path."""
    if isinstance(package, str):
        return {"pdf_path": None, "result_json_path": _clean(package),
                "document_md_path": None, "ocr_html_path": None}
    package = package or {}
    return {
        "pdf_path": _clean(package.get("pdf_path")),
        "result_json_path": _clean(package.get("result_json_path")),
        "document_md_path": _clean(package.get("document_md_path")),
        "ocr_html_path": _clean(package.get("ocr_html_path")),
    }


def build_pipeline_v2_artifact_paths(out_dir: str | Path) -> dict:
    """Карта artifact-ключ → ``Path`` в ``out_dir``."""
    out_dir = Path(out_dir)
    return {k: out_dir / name for k, name in _ARTIFACT_FILENAMES.items()}


# ─── inputs info + optional-file warnings ────────────────────────────────────

_OPTIONAL_KEYS = ("pdf_path", "document_md_path", "ocr_html_path")


def _input_info(paths: dict, warnings: list[str], side: str) -> dict:
    """Описание входа стороны + warnings про отсутствующие optional-файлы."""
    provided = {}
    exists = {}
    for key in ("pdf_path", "result_json_path", "document_md_path", "ocr_html_path"):
        p = paths.get(key)
        provided[key] = bool(p)
        exists[key] = bool(p and Path(p).exists())
        if key in _OPTIONAL_KEYS and p and not Path(p).exists():
            warnings.append(f"{side}: optional file missing on disk ({key}): {p}")
    return {**paths, "provided": provided, "exists": exists}


# ─── summary builders ────────────────────────────────────────────────────────


def _summary_of(report: Any) -> dict:
    if isinstance(report, dict) and isinstance(report.get("summary"), dict):
        return report["summary"]
    return {}


def _warnings_count(report: Any) -> int:
    if isinstance(report, dict) and isinstance(report.get("warnings"), list):
        return len(report["warnings"])
    return 0


def build_pipeline_v2_summary(*, left_model: Any, right_model: Any, block_report: Any,
                              entity_report: Any, diff_report: Any, inputs: dict,
                              artifact_paths: dict, warnings: list[str], status: str,
                              error: Optional[str] = None) -> dict:
    """Собрать ``pipeline_v2_summary`` из артефактов этапов 1–4."""
    lm, rm = _summary_of(left_model), _summary_of(right_model)
    bm, em, dm = _summary_of(block_report), _summary_of(entity_report), _summary_of(diff_report)

    stages = {
        "prepared_ingest": {
            "left_pages_total": lm.get("pages_total", 0),
            "right_pages_total": rm.get("pages_total", 0),
            "left_blocks_total": lm.get("blocks_total", 0),
            "right_blocks_total": rm.get("blocks_total", 0),
            "left_by_page_type": lm.get("by_page_type", {}),
            "right_by_page_type": rm.get("by_page_type", {}),
            "warnings_count": _warnings_count(left_model) + _warnings_count(right_model),
        },
        "block_matching": {
            "page_matches_total": bm.get("page_matches_total", 0),
            "block_matches_total": bm.get("block_matches_total", 0),
            "unmatched_left_blocks": bm.get("unmatched_left_blocks", 0),
            "unmatched_right_blocks": bm.get("unmatched_right_blocks", 0),
            "strong_block_matches": bm.get("strong_block_matches", 0),
            "weak_block_matches": bm.get("weak_block_matches", 0),
            "warnings_count": _warnings_count(block_report),
        },
        "entity_extraction": {
            "left_entities_total": em.get("left_entities_total", 0),
            "right_entities_total": em.get("right_entities_total", 0),
            "by_entity_type": em.get("by_entity_type", {}),
            "by_semantic_group": em.get("by_semantic_group", {}),
            "warnings_count": _warnings_count(entity_report),
        },
        "entity_diff": {
            "deltas_total": dm.get("deltas_total", 0),
            "added_total": dm.get("added_total", 0),
            "removed_total": dm.get("removed_total", 0),
            "changed_total": dm.get("changed_total", 0),
            "uncertain_total": dm.get("uncertain_total", 0),
            "by_entity_type": dm.get("by_entity_type", {}),
            "by_delta_type": dm.get("by_delta_type", {}),
            "by_confidence": dm.get("by_confidence", {}),
            "warnings_count": _warnings_count(diff_report),
        },
    }
    summary = {
        "version": SUMMARY_VERSION,
        "kind": SUMMARY_KIND,
        "status": status,
        "artifacts": {k: p.name for k, p in artifact_paths.items()},
        "inputs": inputs,
        "stages": stages,
        "warnings": warnings,
        "next_recommended_stage": NEXT_RECOMMENDED_STAGE,
    }
    if error:
        summary["error"] = error
    return summary


def write_pipeline_v2_summary_json(out_path: str | Path, summary: dict) -> Path:
    """Атомарно записать summary JSON."""
    return _atomic_write_text(out_path, json.dumps(summary, ensure_ascii=False, indent=2))


def _md_counts_table(title: str, counts: dict) -> list[str]:
    if not counts:
        return [f"- {title}: —"]
    parts = ", ".join(f"{k}={v}" for k, v in counts.items())
    return [f"- {title}: {parts}"]


def write_pipeline_v2_summary_md(out_path: str | Path, summary: dict,
                                 artifact_paths: dict) -> Path:
    """Атомарно записать человекочитаемый Markdown-summary."""
    st = summary.get("stages", {})
    ing = st.get("prepared_ingest", {})
    blk = st.get("block_matching", {})
    ent = st.get("entity_extraction", {})
    dif = st.get("entity_diff", {})
    inp = summary.get("inputs", {})
    status = summary.get("status", "unknown")
    warnings = summary.get("warnings", []) or []

    lines: list[str] = []
    lines.append("# Pipeline V2 — Dry Run Summary")
    lines.append("")
    lines.append(f"**Статус:** `{status}`")
    if summary.get("error"):
        lines.append("")
        lines.append(f"**Ошибка:** {summary['error']}")
    lines.append("")
    lines.append(f"**Следующий этап:** `{summary.get('next_recommended_stage', '')}`")
    lines.append("")

    lines.append("## Входные файлы")
    for side in ("left", "right"):
        s = inp.get(side, {})
        lines.append(f"- **{side}**: result_json=`{s.get('result_json_path')}`; "
                     f"md=`{s.get('document_md_path')}`; html=`{s.get('ocr_html_path')}`; "
                     f"pdf=`{s.get('pdf_path')}`")
    lines.append("")

    lines.append("## [1] Prepared Ingest")
    lines.append(f"- Страниц: left={ing.get('left_pages_total', 0)}, "
                 f"right={ing.get('right_pages_total', 0)}")
    lines.append(f"- Блоков: left={ing.get('left_blocks_total', 0)}, "
                 f"right={ing.get('right_blocks_total', 0)}")
    lines += _md_counts_table("Типы страниц (left)", ing.get("left_by_page_type", {}))
    lines += _md_counts_table("Типы страниц (right)", ing.get("right_by_page_type", {}))
    lines.append("")

    lines.append("## [2] Block Matching")
    lines.append(f"- Сопоставлено страниц: {blk.get('page_matches_total', 0)}")
    lines.append(f"- Сопоставлено блоков: {blk.get('block_matches_total', 0)} "
                 f"(strong={blk.get('strong_block_matches', 0)}, "
                 f"weak={blk.get('weak_block_matches', 0)})")
    lines.append(f"- Непарных блоков: left={blk.get('unmatched_left_blocks', 0)}, "
                 f"right={blk.get('unmatched_right_blocks', 0)}")
    lines.append("")

    lines.append("## [3] Entity Extraction")
    lines.append(f"- Сущностей: left={ent.get('left_entities_total', 0)}, "
                 f"right={ent.get('right_entities_total', 0)}")
    lines += _md_counts_table("По типам", ent.get("by_entity_type", {}))
    lines += _md_counts_table("По группам", ent.get("by_semantic_group", {}))
    lines.append("")

    lines.append("## [4] Deterministic Entity Diff")
    lines.append(f"- Всего дельт: **{dif.get('deltas_total', 0)}**")
    lines.append(f"- added={dif.get('added_total', 0)}, removed={dif.get('removed_total', 0)}, "
                 f"changed={dif.get('changed_total', 0)}, uncertain={dif.get('uncertain_total', 0)}")
    lines += _md_counts_table("Уверенность", dif.get("by_confidence", {}))
    lines += _md_counts_table("По типам дельт", dif.get("by_entity_type", {}))
    lines.append("")

    lines.append("## Warnings")
    if warnings:
        for w in warnings[:10]:
            lines.append(f"- {w}")
        if len(warnings) > 10:
            lines.append(f"- … ещё {len(warnings) - 10}")
    else:
        lines.append("- нет")
    lines.append("")

    lines.append("## Артефакты")
    for key in _MANIFEST_ARTIFACT_KEYS + ["manifest"]:
        p = artifact_paths.get(key)
        if p is not None:
            mark = "✓" if Path(p).exists() else "—"
            lines.append(f"- `{p.name}` [{mark}]")
    lines.append("")

    lines.append("## Вывод")
    if status == "failed":
        lines.append(f"❌ Dry-run не завершён. Сначала устраните ошибку: "
                     f"{summary.get('error', 'см. warnings')}.")
    elif status == "completed_with_warnings":
        lines.append("⚠ Можно передавать в LLM explanation/critic, но проверьте warnings.")
    else:
        lines.append("✅ Готово к передаче в LLM explanation/critic.")
    lines.append("")

    return _atomic_write_text(out_path, "\n".join(lines))


def write_pipeline_v2_manifest(out_path: str | Path, artifact_paths: dict) -> Path:
    """Атомарно записать манифест: для каждого существующего артефакта —
    относительный путь, размер, sha256, kind."""
    out_path = Path(out_path)
    entries: list[dict] = []
    for key in _MANIFEST_ARTIFACT_KEYS:
        p = artifact_paths.get(key)
        if p is None or not Path(p).exists():
            entries.append({"key": key,
                            "filename": (Path(p).name if p is not None else None),
                            "exists": False, "size_bytes": None,
                            "sha256": None, "kind": None})
            continue
        p = Path(p)
        entries.append({
            "key": key,
            "filename": p.name,
            "relative_path": p.name,
            "exists": True,
            "size_bytes": p.stat().st_size,
            "sha256": _sha256(p),
            "kind": _read_kind(p),
        })
    manifest = {
        "version": SUMMARY_VERSION,
        "kind": MANIFEST_KIND,
        "artifacts": entries,
        "artifacts_total": sum(1 for e in entries if e["exists"]),
    }
    return _atomic_write_text(out_path, json.dumps(manifest, ensure_ascii=False, indent=2))


# ─── orchestrator ────────────────────────────────────────────────────────────


def run_pipeline_v2_dry_run(left_package: Any, right_package: Any, out_dir: str | Path,
                            options: Optional[dict] = None) -> dict:
    """Прогнать этапы 1–4 offline и записать артефакты в ``out_dir``.

    Возвращает summary-словарь (status ok/completed_with_warnings/failed).
    Чистый offline-конвейер: без сети/Qwen/Opus/crop-download.
    """
    options = options or {}
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = build_pipeline_v2_artifact_paths(out_dir)

    left = normalize_package_paths(left_package)
    right = normalize_package_paths(right_package)
    warnings: list[str] = []
    inputs = {
        "left": _input_info(left, warnings, "left"),
        "right": _input_info(right, warnings, "right"),
    }

    status = "ok"
    error: Optional[str] = None
    left_model = right_model = block_report = entity_report = diff_report = None

    try:
        for side, pkg in (("left", left), ("right", right)):
            rj = pkg.get("result_json_path")
            if not rj:
                raise ValueError(f"{side}: result_json_path is required but missing")
            if not Path(rj).exists():
                raise FileNotFoundError(f"{side}: result_json_path not found: {rj}")

        # [1] prepared ingest
        left_model = build_normalized_document_model(
            left["result_json_path"], document_md_path=left["document_md_path"],
            ocr_html_path=left["ocr_html_path"], pdf_path=left["pdf_path"])
        right_model = build_normalized_document_model(
            right["result_json_path"], document_md_path=right["document_md_path"],
            ocr_html_path=right["ocr_html_path"], pdf_path=right["pdf_path"])
        write_normalized_document_model(paths["left_model"], left_model)
        write_normalized_document_model(paths["right_model"], right_model)

        # [2] block matching
        block_report = match_normalized_documents(
            left_model, right_model, options.get("matching"))
        write_block_matching_report(paths["block_matching"], block_report)

        # [3] entity extraction
        entity_report = extract_entities_for_matched_documents(
            left_model, right_model, block_report, options.get("extraction"))
        write_entity_extraction_report(paths["entity_extraction"], entity_report)

        # [4] deterministic entity diff
        diff_report = diff_entity_extraction_report(entity_report, options.get("diff"))
        write_entity_diff_report(paths["entity_diff"], diff_report)

    except Exception as exc:  # fail-soft: фиксируем, не роняем процесс
        status = "failed"
        error = f"{type(exc).__name__}: {exc}"

    # собрать warnings из артефактов этапов
    for rpt, prefix in ((left_model, "ingest_left"), (right_model, "ingest_right"),
                        (block_report, "block_matching"), (entity_report, "entity_extraction"),
                        (diff_report, "entity_diff")):
        if isinstance(rpt, dict):
            for w in rpt.get("warnings", []) or []:
                warnings.append(f"{prefix}: {w}")

    if status != "failed" and warnings:
        status = "completed_with_warnings"

    summary = build_pipeline_v2_summary(
        left_model=left_model, right_model=right_model, block_report=block_report,
        entity_report=entity_report, diff_report=diff_report, inputs=inputs,
        artifact_paths=paths, warnings=warnings, status=status, error=error)

    # summary + manifest пишем всегда (best-effort, атомарно)
    write_pipeline_v2_summary_json(paths["summary_json"], summary)
    write_pipeline_v2_summary_md(paths["summary_md"], summary, paths)
    write_pipeline_v2_manifest(paths["manifest"], paths)
    return summary


__all__ = [
    "SUMMARY_VERSION",
    "SUMMARY_KIND",
    "MANIFEST_KIND",
    "NEXT_RECOMMENDED_STAGE",
    "run_pipeline_v2_dry_run",
    "normalize_package_paths",
    "build_pipeline_v2_artifact_paths",
    "build_pipeline_v2_summary",
    "write_pipeline_v2_summary_md",
    "write_pipeline_v2_summary_json",
    "write_pipeline_v2_manifest",
]
