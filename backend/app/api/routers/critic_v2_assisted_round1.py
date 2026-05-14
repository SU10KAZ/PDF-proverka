"""
Read-only API для assisted_round1 review-package.

Назначение: помогает инженеру найти и проверить именно те карточки, которые
указаны в assisted_round1_risky_accepted_22.{csv,md} и
assisted_round1_sample_60.{csv,md} — а не все findings проекта.

Контракт (см. CLAUDE.md и docs/critic_corrector.md):
- Production pipeline не затрагивается.
- Legacy critic остаётся основным.
- 03_findings_review.json НЕ изменяется.
- LLM не вызывается, ничего на диск не пишется.
- Источник данных — CSV-файлы review-package (MD-файлы — лишь презентационный слой).

Источник:
- env CRITIC_V2_FEEDBACK_DIR (по умолчанию <repo>/critic v2 test/)
- review-package: <feedback_dir>/assisted_round1_review/

Endpoints:
- GET /api/critic-v2/assisted-round1/files
    Список найденных CSV-файлов с метаданными.
- GET /api/critic-v2/assisted-round1/items
    Все карточки из обоих CSV, нормализованные.
- GET /api/critic-v2/assisted-round1/items?project_id=<name>
    Только карточки, относящиеся к данному проекту (exact / exact_no_pdf
    / normalized матчинг — как в feedback-files).
"""
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api", tags=["critic-v2-assisted-round1"])

_REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_FEEDBACK_DIR = _REPO_ROOT / "critic v2 test"

# Имена CSV-файлов внутри assisted_round1_review/. MD-файлы умышленно не парсим:
# их структура — Markdown-таблицы с свободным текстом, CSV даёт детерминированный
# источник тех же данных.
_CSV_RISKY = "assisted_round1_risky_accepted_22.csv"
_CSV_SAMPLE = "assisted_round1_sample_60.csv"

_GROUP_BY_CSV: dict[str, str] = {
    _CSV_RISKY: "risky_accepted_22",
    _CSV_SAMPLE: "sample_60",
}

# Маппинг round1 reason → русская группа причины (для UI-фильтра/диагностики).
# Для не-round1 (например suggested_reject_not_safe_to_hide) reason_group=None.
_REASON_GROUPS = {
    "round1_ocr_artifact_suggested_reject": "OCR / ошибка распознавания",
    "round1_rd_vs_pz_suggested_reject": "Расчётный параметр: ПЗ/расчёт, не чертёж РД",
    "round1_already_covered_suggested_reject": "Уже есть в смежном разделе / спецификации",
}


def _resolve_feedback_dir() -> Path:
    env = os.environ.get("CRITIC_V2_FEEDBACK_DIR", "").strip()
    return Path(env) if env else DEFAULT_FEEDBACK_DIR


def _review_dir() -> Path:
    return _resolve_feedback_dir() / "assisted_round1_review"


def _normalize(name: str) -> str:
    """Tools-grade нормализация: lowercase, trim, отрезать .pdf, схлопнуть пробелы.

    Идентична _normalize в critic_v2_ui.py — должны вести себя одинаково,
    чтобы matching feedback-files и assisted-round1 совпадал.
    """
    if not name:
        return ""
    s = str(name).strip().lower()
    if s.endswith(".pdf"):
        s = s[:-4].rstrip()
    return " ".join(s.split())


def _parse_csv_file(path: Path, group: str) -> list[dict[str, Any]]:
    """Прочитать один CSV review-package и вернуть нормализованные items.

    На обоих файлах одинаковая схема: bucket, section, project_name,
    finding_id, title, original_tab, current_tab, queue, reason,
    taxonomy_reason, evidence_quality, score, human_decision, human_reason,
    explanation, reviewer_instruction.
    """
    items: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_idx, row in enumerate(reader, start=1):
                fid = (row.get("finding_id") or "").strip()
                if not fid:
                    continue
                reason = (row.get("reason") or "").strip() or None
                items.append({
                    "source_file": path.name,
                    "group": group,
                    "row_index": row_idx,
                    "section": (row.get("section") or "").strip(),
                    "project_name": (row.get("project_name") or "").strip(),
                    "finding_id": fid,
                    "title": (row.get("title") or "").strip(),
                    "original_tab": (row.get("original_tab") or "").strip(),
                    "current_tab": (row.get("current_tab") or "").strip(),
                    "expected_queue": (row.get("queue") or "").strip() or "suggested_reject",
                    "reason": reason,
                    "reason_group": _REASON_GROUPS.get(reason or ""),
                    "evidence_quality": (row.get("evidence_quality") or "").strip(),
                    "score": (row.get("score") or "").strip(),
                    "human_decision": (row.get("human_decision") or "").strip(),
                    "human_reason": (row.get("human_reason") or "").strip(),
                    "reviewer_instruction": (row.get("reviewer_instruction") or "").strip(),
                })
    except (OSError, csv.Error):
        # Битый файл просто не даёт карточек; не валим listing.
        return []
    return items


def _load_all_items() -> list[dict[str, Any]]:
    """Прочитать оба CSV review-package. Порядок: risky_accepted_22, потом sample_60."""
    review = _review_dir()
    out: list[dict[str, Any]] = []
    if not review.exists() or not review.is_dir():
        return out
    for name, group in _GROUP_BY_CSV.items():
        p = review / name
        if p.exists() and p.is_file():
            out.extend(_parse_csv_file(p, group))
    return out


def _match_quality_for(
    target: str, target_no_pdf: str, target_norm: str, project_name: str,
) -> str | None:
    if not project_name:
        return None
    if project_name == target:
        return "exact"
    if project_name == target_no_pdf:
        return "exact_no_pdf"
    if _normalize(project_name) == target_norm:
        return "normalized"
    return None


@router.get("/critic-v2/assisted-round1/files")
async def assisted_round1_files() -> dict[str, Any]:
    """Список CSV-файлов review-package с базовой метаинформацией."""
    review = _review_dir()
    files: list[dict[str, Any]] = []
    if review.exists() and review.is_dir():
        for name, group in _GROUP_BY_CSV.items():
            p = review / name
            entry: dict[str, Any] = {
                "name": name,
                "group": group,
                "exists": p.exists() and p.is_file(),
            }
            if p.exists() and p.is_file():
                try:
                    stat = p.stat()
                    entry["size"] = stat.st_size
                    entry["mtime"] = int(stat.st_mtime)
                except OSError:
                    pass
                # Быстро посчитать количество строк.
                rows = _parse_csv_file(p, group)
                entry["items"] = len(rows)
            files.append(entry)
    return {
        "review_dir": str(review),
        "exists": review.exists(),
        "files": files,
    }


@router.get("/critic-v2/assisted-round1/items")
async def assisted_round1_items(
    project_id: str | None = Query(
        None, description="optional filter: только карточки этого проекта",
    ),
    group: str | None = Query(
        None, description="optional filter: risky_accepted_22 / sample_60",
    ),
) -> dict[str, Any]:
    """Все карточки из обоих CSV.

    При указании project_id возвращаются только карточки, у которых
    project_name матчится с target по exact / exact_no_pdf / normalized.
    Карточки, попавшие в match, обогащаются полем match_quality.
    """
    items = _load_all_items()

    # Sanity-фильтр по группе. По умолчанию обе.
    if group is not None and group.strip():
        g = group.strip()
        if g not in {"risky_accepted_22", "sample_60"}:
            raise HTTPException(
                status_code=400,
                detail={"error": "invalid_group", "value": g},
            )
        items = [it for it in items if it["group"] == g]

    response: dict[str, Any] = {
        "review_dir": str(_review_dir()),
        "items": items,
        "total": len(items),
    }

    if project_id is not None and project_id.strip():
        target = project_id.strip()
        target_no_pdf = (
            target[:-4].rstrip() if target.lower().endswith(".pdf") else target
        )
        target_norm = _normalize(target)
        matched: list[dict[str, Any]] = []
        for it in items:
            q = _match_quality_for(
                target, target_no_pdf, target_norm, it.get("project_name", ""),
            )
            if q:
                copy = dict(it)
                copy["match_quality"] = q
                matched.append(copy)
        response["project_id"] = target
        response["items"] = matched
        response["matched_count"] = len(matched)
        response["total"] = len(matched)
        response["all_items_total"] = sum(1 for _ in _load_all_items())

    return response
