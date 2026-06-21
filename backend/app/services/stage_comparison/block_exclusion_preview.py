# -*- coding: utf-8 -*-
"""Block exclusion preview (Stage Gate-1 — aggregating, mark-only).

Единый безопасный слой ПОВЕРХ двух уже существующих mark-only прекчеков:

  * :mod:`visual_block_equivalence` (image/graphic блоки → identical_visual / …);
  * :mod:`text_block_equivalence`   (text/table блоки → identical_text / …).

Назначение — ответить на ОДИН вопрос, ничего реально не исключая:

    «если бы мы включили enforce, какие блоки были бы исключены и почему?»

Поэтому preview:

  * читает ОБА артефакта (``visual_block_equivalence.json`` и
    ``text_block_equivalence.json``), если они есть;
  * объединяет кандидатов в единый список ``items[]`` с полем ``modality``;
  * пишет ОТДЕЛЬНЫЙ артефакт
    ``pairs/<pid>/block_exclusion_preview/block_exclusion_preview.json``;
  * остаётся ``mode=mark_only`` / ``enforced=false`` — реального skip НЕТ.

Что НЕ меняется (инвариант безопасности): Qwen / MD enrichment / Opus /
enriched.md / links.json / page_alignment.json / comparison_result.json /
findings. Модуль их даже не импортирует.

Правила решения (decision) — ровно из задачи Gate-1:

| источник статус | decision | exclude_from_qwen | exclude_from_opus_md |
|---|---|---|---|
| ``identical_visual``      | candidate_exclude | true  | true  |
| ``minor_render_noise``    | review_only       | false | false |
| ``changed_visual`` / ``uncertain`` / ``render_failed`` | keep | false | false |
| ``identical_text``        | candidate_exclude | false | true  |
| ``near_identical_text``   | review_only       | false | false |
| ``changed_text`` / ``uncertain_text`` | keep  | false | false |

``near_identical_text`` и ``minor_render_noise`` НЕ исключаются автоматически.
``enforced`` всегда ``False``. Skipped-статусы источников (``skipped_*``) в
items НЕ попадают (это не результаты сравнения, а вне-скоупные/служебные связи) —
они учитываются только счётчиками в summary.

Дедупликация по построению: каждая связь СРАВНИВАЕТСЯ ровно в одном слое
(текст/таблица → text, image/graphic → visual; в «чужом» артефакте она
``skipped_non_image`` / ``skipped_non_text`` и в items не идёт), поэтому один
left/right block_id pair даёт не более одного item.

Модуль fail-soft: отсутствие одного/обоих артефактов → пустой/частичный preview,
никаких исключений наружу.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from .text_block_equivalence import read_pair_text_block_equivalence
from .visual_block_equivalence import read_pair_visual_block_equivalence

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Decision rules (status → (decision, exclude_from_qwen, exclude_from_opus_md))
# ═══════════════════════════════════════════════════════════════════════════

DECISION_CANDIDATE = "candidate_exclude"
DECISION_REVIEW = "review_only"
DECISION_KEEP = "keep"

# Visual слой
_VISUAL_RULES: dict[str, tuple[str, bool, bool]] = {
    "identical_visual": (DECISION_CANDIDATE, True, True),
    "minor_render_noise": (DECISION_REVIEW, False, False),
    "changed_visual": (DECISION_KEEP, False, False),
    "uncertain": (DECISION_KEEP, False, False),
    "render_failed": (DECISION_KEEP, False, False),
}

# Text слой
_TEXT_RULES: dict[str, tuple[str, bool, bool]] = {
    "identical_text": (DECISION_CANDIDATE, False, True),
    "near_identical_text": (DECISION_REVIEW, False, False),
    "changed_text": (DECISION_KEEP, False, False),
    "uncertain_text": (DECISION_KEEP, False, False),
}

# Короткие человекочитаемые reason'ы решения (source reason кладётся отдельно).
_DECISION_REASON: dict[str, str] = {
    "identical_visual": "identical_visual: exclude candidate for Qwen and Opus/MD",
    "minor_render_noise": "minor_render_noise: review only, not excluded",
    "changed_visual": "changed_visual: keep",
    "uncertain": "uncertain visual: keep",
    "render_failed": "render_failed: keep",
    "identical_text": "identical_text after normalized text comparison: "
                      "exclude candidate for Opus/MD only",
    "near_identical_text": "near_identical_text: review only, not excluded",
    "changed_text": "changed_text: keep",
    "uncertain_text": "uncertain_text: keep",
}

# Источники relative-пути (для блока sources в артефакте).
_VISUAL_SOURCE_REL = "visual_block_equivalence/visual_block_equivalence.json"
_TEXT_SOURCE_REL = "text_block_equivalence/text_block_equivalence.json"


# ═══════════════════════════════════════════════════════════════════════════
# Item builder
# ═══════════════════════════════════════════════════════════════════════════


def _item_from_record(record: dict, *, modality: str, rules: dict[str, tuple[str, bool, bool]],
                      source_artifact: str) -> Optional[dict]:
    """Собрать preview-item из одной записи source-артефакта.

    Возвращает ``None``, если статус не входит в правила (skipped_* и пр.) —
    такие связи в items не попадают."""
    status = record.get("status")
    rule = rules.get(status)
    if rule is None:
        return None
    decision, ex_qwen, ex_opus = rule
    return {
        "left_block_id": record.get("left_block_id"),
        "right_block_id": record.get("right_block_id"),
        "left_page": record.get("left_page"),
        "right_page": record.get("right_page"),
        "modality": modality,
        "source_status": status,
        "decision": decision,
        "exclude_from_qwen": ex_qwen,
        "exclude_from_opus_md": ex_opus,
        "enforced": False,                       # Gate-1: реального skip нет
        "confidence": record.get("confidence"),
        "reason": _DECISION_REASON.get(status, status),
        "source_reason": record.get("reason"),
        "metrics": record.get("metrics") or {},
        "source_artifact": source_artifact,
    }


def _collect_items(report: Optional[dict], *, modality: str,
                   rules: dict[str, tuple[str, bool, bool]], source_artifact: str,
                   ) -> tuple[list[dict], int]:
    """Из одного source-отчёта собрать items + посчитать пропущенные skipped-связи.

    Возвращает ``(items, skipped_source_links)``.

    Fail-soft по форме входа (артефакт на диске мог быть повреждён частичной
    записью / ручной правкой / дрейфом формата): не-dict отчёт, не-list ``pairs``
    и не-dict элементы списка не валят preview — они трактуются как пусто/skip,
    не как исключение наружу."""
    if not isinstance(report, dict):
        return [], 0
    pairs = report.get("pairs")
    if not isinstance(pairs, list):
        if pairs is not None:
            logger.warning(
                "block_exclusion_preview: 'pairs' is not a list (%s); treating as empty",
                type(pairs).__name__)
        return [], 0
    items: list[dict] = []
    skipped = 0
    for rec in pairs:
        if not isinstance(rec, dict):       # повреждённый элемент списка — не падаем
            skipped += 1
            continue
        item = _item_from_record(rec, modality=modality, rules=rules,
                                 source_artifact=source_artifact)
        if item is None:
            skipped += 1
            continue
        items.append(item)
    return items, skipped


# ═══════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════


def build_summary(items: list[dict], *, skipped_visual: int = 0, skipped_text: int = 0) -> dict:
    """Посчитать summary preview из собранных items.

    Ключи задачи Gate-1 (visual_candidates / text_candidates /
    qwen_exclusion_candidates / opus_md_exclusion_candidates /
    near_text_review_candidates / blocked_by_uncertain / blocked_by_changed) +
    дополнительные информативные счётчики."""
    summary = {
        "visual_candidates": 0,
        "text_candidates": 0,
        "qwen_exclusion_candidates": 0,
        "opus_md_exclusion_candidates": 0,
        "near_text_review_candidates": 0,
        "blocked_by_uncertain": 0,
        "blocked_by_changed": 0,
        # доп. счётчики (информативно)
        "minor_render_noise_review": 0,
        "blocked_by_render_failed": 0,
        "candidate_exclude_total": 0,
        "review_only_total": 0,
        "keep_total": 0,
        "items_total": len(items),
        "visual_items": 0,
        "text_items": 0,
        "skipped_source_links_visual": skipped_visual,
        "skipped_source_links_text": skipped_text,
    }
    for it in items:
        modality = it.get("modality")
        status = it.get("source_status")
        decision = it.get("decision")

        if modality == "visual":
            summary["visual_items"] += 1
        elif modality == "text":
            summary["text_items"] += 1

        if decision == DECISION_CANDIDATE:
            summary["candidate_exclude_total"] += 1
            if modality == "visual":
                summary["visual_candidates"] += 1
            elif modality == "text":
                summary["text_candidates"] += 1
            if it.get("exclude_from_qwen"):
                summary["qwen_exclusion_candidates"] += 1
            if it.get("exclude_from_opus_md"):
                summary["opus_md_exclusion_candidates"] += 1
        elif decision == DECISION_REVIEW:
            summary["review_only_total"] += 1
        elif decision == DECISION_KEEP:
            summary["keep_total"] += 1

        if status == "near_identical_text":
            summary["near_text_review_candidates"] += 1
        elif status == "minor_render_noise":
            summary["minor_render_noise_review"] += 1
        elif status in ("uncertain", "uncertain_text"):
            summary["blocked_by_uncertain"] += 1
        elif status in ("changed_visual", "changed_text"):
            summary["blocked_by_changed"] += 1
        elif status == "render_failed":
            summary["blocked_by_render_failed"] += 1

    return summary


# ═══════════════════════════════════════════════════════════════════════════
# build / read
# ═══════════════════════════════════════════════════════════════════════════


def build_block_exclusion_preview(
    session_id: str,
    pair_id: str,
    *,
    visual_report: Optional[dict] = None,
    text_report: Optional[dict] = None,
    read_from_disk: bool = True,
    write_artifact: bool = True,
    generated_at: Optional[str] = None,
) -> dict:
    """Собрать единый mark-only preview исключений по паре.

    Источники:
      * ``visual_report`` / ``text_report`` — если переданы, используются как
        есть (инъекция для тестов, никаких чтений с диска);
      * иначе при ``read_from_disk=True`` читаются с диска через
        ``read_pair_visual_block_equivalence`` / ``read_pair_text_block_equivalence``
        (отсутствующий артефакт → ``None`` → пустой вклад этого слоя);
      * при ``read_from_disk=False`` отсутствующий инъецированный отчёт = пусто
        (используется в тестах, чтобы НИКОГДА не читать live comparison/sessions).

    mark-only: ничего не исключает, ``enforced=False``. Возвращает собранный
    отчёт (dict). Fail-soft.
    """
    generated_at = generated_at or _utc_now_iso()

    if visual_report is None and read_from_disk:
        try:
            visual_report = read_pair_visual_block_equivalence(session_id, pair_id)
        except Exception as exc:  # noqa: BLE001 — fail-soft
            logger.debug("block_exclusion_preview: read visual failed %s/%s: %s",
                         session_id, pair_id, exc)
            visual_report = None
    if text_report is None and read_from_disk:
        try:
            text_report = read_pair_text_block_equivalence(session_id, pair_id)
        except Exception as exc:  # noqa: BLE001 — fail-soft
            logger.debug("block_exclusion_preview: read text failed %s/%s: %s",
                         session_id, pair_id, exc)
            text_report = None

    # Повреждённый/неожиданной формы артефакт (напр. top-level JSON array) трактуем
    # как отсутствующий — чтобы sources_present/warnings оставались честными.
    if visual_report is not None and not isinstance(visual_report, dict):
        warn_visual_malformed = True
        visual_report = None
    else:
        warn_visual_malformed = False
    if text_report is not None and not isinstance(text_report, dict):
        warn_text_malformed = True
        text_report = None
    else:
        warn_text_malformed = False

    visual_items, skipped_visual = _collect_items(
        visual_report, modality="visual", rules=_VISUAL_RULES,
        source_artifact="visual_block_equivalence")
    text_items, skipped_text = _collect_items(
        text_report, modality="text", rules=_TEXT_RULES,
        source_artifact="text_block_equivalence")
    items = visual_items + text_items

    summary = build_summary(items, skipped_visual=skipped_visual, skipped_text=skipped_text)

    warnings: list[str] = []
    if visual_report is None:
        warnings.append("visual_block_equivalence_artifact_missing")
    if text_report is None:
        warnings.append("text_block_equivalence_artifact_missing")
    if warn_visual_malformed:
        warnings.append("visual_block_equivalence_artifact_malformed")
    if warn_text_malformed:
        warnings.append("text_block_equivalence_artifact_malformed")

    report = {
        "schema_version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "mode": "mark_only",
        "enforced": False,                  # Gate-1: реального skip нет
        "sources": {
            "visual": _VISUAL_SOURCE_REL,
            "text": _TEXT_SOURCE_REL,
        },
        "sources_present": {
            "visual": visual_report is not None,
            "text": text_report is not None,
        },
        "summary": summary,
        "items": items,
        "warnings": warnings,
    }

    if write_artifact:
        try:
            from . import paths as paths_mod
            out = paths_mod.block_exclusion_preview_report_path(session_id, pair_id)
            _atomic_write_json(out, report)
        except Exception as exc:  # noqa: BLE001 — artifact write non-fatal
            logger.warning("block_exclusion_preview: report write failed %s/%s: %s",
                           session_id, pair_id, exc)

    return report


def read_block_exclusion_preview(session_id: str, pair_id: str) -> Optional[dict]:
    """Прочитать сохранённый preview-артефакт пары (если есть)."""
    try:
        import json
        from . import paths as paths_mod
        p = paths_mod.block_exclusion_preview_report_path(session_id, pair_id)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════════════════════════════


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Атомарная запись JSON (tmp → os.replace) — паттерн проекта."""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


__all__ = [
    "build_block_exclusion_preview",
    "read_block_exclusion_preview",
    "build_summary",
    "DECISION_CANDIDATE",
    "DECISION_REVIEW",
    "DECISION_KEEP",
]
