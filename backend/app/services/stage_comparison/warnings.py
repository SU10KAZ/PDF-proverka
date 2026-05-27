"""Warnings раздела «Сравнение стадий».

Сводит признаки качества исходных данных и состояния сессии в единый
список warnings:
  • PDF-пары без MD;
  • PDF-пары без result.json;
  • disabled pairs;
  • stale_links > 0;
  • page_alignment saved_with_warnings;
  • LLM jobs failed/blocked;
  • reports без изображений;
  • unmatched PDFs.

Вычисляется по требованию — отдельного файла нет.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from . import jobs as jobs_mod
from . import paths as paths_mod
from . import store as store_mod
from . import text_llm as text_llm_mod

logger = logging.getLogger(__name__)


def _read_raw_alignment(session_id: str, pair_id: str) -> dict:
    try:
        p = paths_mod.page_alignment_path(session_id, pair_id)
    except (KeyError, ValueError):
        return {}
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _sev_rank(s: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(s, 3)


def compute_warnings(session_id: str) -> dict:
    items: list[dict] = []
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")

    pairs = session.get("pairs") or []
    matched_pairs = [p for p in pairs if p.get("status") != "disabled"]
    for p in matched_pairs:
        pid = p.get("id")
        left = p.get("left") or {}
        right = p.get("right") or {}
        if not left.get("md_path"):
            items.append({
                "type": "missing_md",
                "severity": "medium",
                "pair_id": pid,
                "title": "Нет Markdown для стадии A",
                "details": f"PDF: {left.get('filename') or '—'}",
            })
        if not right.get("md_path"):
            items.append({
                "type": "missing_md",
                "severity": "medium",
                "pair_id": pid,
                "title": "Нет Markdown для стадии B",
                "details": f"PDF: {right.get('filename') or '—'}",
            })
        if left.get("pdf_path") and not left.get("result_json_path"):
            items.append({
                "type": "missing_result_json",
                "severity": "low",
                "pair_id": pid,
                "title": "Нет result.json для стадии A",
                "details": f"PDF: {left.get('filename') or '—'}",
            })
        if right.get("pdf_path") and not right.get("result_json_path"):
            items.append({
                "type": "missing_result_json",
                "severity": "low",
                "pair_id": pid,
                "title": "Нет result.json для стадии B",
                "details": f"PDF: {right.get('filename') or '—'}",
            })

    # disabled pairs
    for p in pairs:
        if p.get("status") == "disabled":
            items.append({
                "type": "disabled_pair",
                "severity": "low",
                "pair_id": p.get("id"),
                "title": "Пара отключена",
                "details": ((p.get("left") or {}).get("filename") or "—") + " ↔ "
                            + ((p.get("right") or {}).get("filename") or "—"),
            })

    # ─── Text LLM analysis state ─────────────────────────────────────────
    # Для каждой не-disabled пары с MD проверяем text_llm_diff.json и
    # выпускаем соответствующее предупреждение.
    for p in matched_pairs:
        pid = p.get("id")
        left = p.get("left") or {}
        right = p.get("right") or {}
        has_left_md = bool(left.get("md_path"))
        has_right_md = bool(right.get("md_path"))
        if not (has_left_md or has_right_md):
            continue
        if not (has_left_md and has_right_md):
            items.append({
                "type": "text_llm_missing_md",
                "severity": "medium",
                "pair_id": pid,
                "title": "Семантический анализ текста невозможен: нет MD с одной стороны",
                "details": ("Стадия A: " + (left.get("filename") or "—")
                            + " ↔ Стадия B: " + (right.get("filename") or "—")),
            })
            continue
        text_llm = text_llm_mod.get_text_llm_diff(session_id, pid)
        if text_llm is None:
            items.append({
                "type": "text_llm_not_run",
                "severity": "medium",
                "pair_id": pid,
                "title": "LLM-анализ текста ещё не выполнен",
                "details": "Нажмите «Сравнить текст через Claude Sonnet» на вкладке «Расхождения → Текст».",
            })
            continue
        st = text_llm.get("status")
        if st == "provider_not_available":
            items.append({
                "type": "text_llm_provider_unavailable",
                "severity": "high",
                "pair_id": pid,
                "title": "Claude Code provider недоступен",
                "details": text_llm.get("error") or "claude_cli_not_found",
            })
        elif st == "disabled":
            items.append({
                "type": "text_llm_disabled",
                "severity": "low",
                "pair_id": pid,
                "title": "LLM-анализ текста выключен в окружении",
                "details": "STAGE_COMPARISON_TEXT_LLM_ENABLED=true для включения.",
            })
        elif st == "too_large":
            stats = text_llm.get("input_stats") or {}
            items.append({
                "type": "text_llm_too_large",
                "severity": "medium",
                "pair_id": pid,
                "title": "MD-файлы превышают лимит для полного анализа",
                "details": (f"total={stats.get('total_chars')} > limit={stats.get('limit_chars')}. "
                            "Увеличьте STAGE_COMPARISON_TEXT_LLM_MAX_CHARS."),
            })
        elif st == "error" or st == "timeout":
            items.append({
                "type": "text_llm_error",
                "severity": "high",
                "pair_id": pid,
                "title": "LLM вернул ошибку при анализе текста",
                "details": (text_llm.get("error") or st)[:300],
            })
        elif st == "missing_md":
            items.append({
                "type": "text_llm_missing_md",
                "severity": "medium",
                "pair_id": pid,
                "title": "Семантический анализ текста невозможен: нет MD",
                "details": "Проверьте, что для обеих стадий пары загружены Markdown-файлы.",
            })

    # stale links, alignment with warnings
    for p in matched_pairs:
        pid = p.get("id")
        try:
            summary = store_mod.compute_graphic_summary(session_id, pid) or {}
        except Exception:
            summary = {}
        stale = summary.get("stale_links") or []
        if stale:
            items.append({
                "type": "stale_links",
                "severity": "medium",
                "pair_id": pid,
                "title": f"Устаревшие связи блоков ({len(stale)})",
                "details": "Связи помечены '_stale' — карта листов изменилась после создания связей.",
            })
        # page_alignment saved_with_warnings — берём из сырого файла
        raw_al = _read_raw_alignment(session_id, pid)
        if raw_al.get("saved_with_warnings"):
            errs = raw_al.get("validation_errors") or []
            items.append({
                "type": "page_alignment_warnings",
                "severity": "medium",
                "pair_id": pid,
                "title": "Карта страниц сохранена с предупреждениями",
                "details": f"Ошибок валидации: {len(errs)}",
            })

    # unmatched PDFs
    try:
        unmatched = store_mod.list_unmatched(session_id) or {}
        left_un = unmatched.get("left_unmatched") or []
        right_un = unmatched.get("right_unmatched") or []
        if left_un:
            items.append({
                "type": "unmatched_left",
                "severity": "low",
                "pair_id": None,
                "title": f"PDF в стадии A без пары ({len(left_un)})",
                "details": ", ".join(e.get("filename") or "—" for e in left_un[:5]),
            })
        if right_un:
            items.append({
                "type": "unmatched_right",
                "severity": "low",
                "pair_id": None,
                "title": f"PDF в стадии B без пары ({len(right_un)})",
                "details": ", ".join(e.get("filename") or "—" for e in right_un[:5]),
            })
    except Exception:
        pass

    # LLM jobs failed/blocked
    try:
        all_jobs = jobs_mod.list_jobs(session_id)
        for j in all_jobs:
            st = j.get("status")
            if st in ("failed",):
                items.append({
                    "type": "job_failed",
                    "severity": "high",
                    "pair_id": None,
                    "title": f"LLM job '{j.get('id')}' завершился с ошибкой",
                    "details": f"progress: {j.get('progress')}",
                })
            elif st == "interrupted":
                items.append({
                    "type": "job_interrupted",
                    "severity": "medium",
                    "pair_id": None,
                    "title": f"LLM job '{j.get('id')}' прерван (рестарт сервера)",
                    "details": f"progress: {j.get('progress')}",
                })
    except Exception:
        pass

    # Reports without images
    try:
        from . import reports as reports_mod
        all_reports = reports_mod.list_reports(session_id)
        without_imgs = [r for r in all_reports if r.get("include_images") is False]
        if without_imgs:
            items.append({
                "type": "report_without_images",
                "severity": "low",
                "pair_id": None,
                "title": f"Отчёты без изображений: {len(without_imgs)}",
                "details": "В этих отчётах включить картинки нельзя — пересоздайте с include_images=true.",
            })
    except Exception:
        pass

    items.sort(key=lambda x: _sev_rank(x.get("severity", "low")))
    summary = {"high": 0, "medium": 0, "low": 0}
    for it in items:
        sev = it.get("severity", "low")
        if sev in summary:
            summary[sev] += 1
    return {"items": items, "summary": summary}


__all__ = ["compute_warnings"]
