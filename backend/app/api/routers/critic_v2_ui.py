"""
Critic v2 UI — read-only API для experimental project-scoped triage view.

ПРИНЦИПЫ (см. CLAUDE.md / docs/critic_corrector.md):
- Production pipeline не затрагивается.
- Legacy critic остаётся основным.
- 03_findings_review.json не изменяется.
- LLM не вызывается.
- На диск ничего не пишется.
- Только чтение готового UI export, сгенерированного скриптом
  backend/scripts/replay_critic_v2_triage_policy.py --ui-export.

Источник данных:
- env CRITIC_V2_UI_EXPORT_PATH (полный путь к critic_v2_triage_ui.json), или
- default: /tmp/critic_v2_ui_export_for_manual_review/llm_no_context/critic_v2_triage_ui.json
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/api", tags=["critic-v2-ui"])

DEFAULT_EXPORT_PATH = Path(
    "/tmp/critic_v2_ui_export_for_manual_review/llm_no_context/critic_v2_triage_ui.json"
)
REPLAY_HINT = (
    "python backend/scripts/replay_critic_v2_triage_policy.py "
    "--matrix-output-dir /tmp/critic_v2_matrix_real_expanded_full "
    "--experiment llm_no_context --profile conservative "
    "--output-dir /tmp/critic_v2_ui_export_for_manual_review --ui-export"
)


def _resolve_artifact_path() -> Path:
    env = os.environ.get("CRITIC_V2_UI_EXPORT_PATH", "").strip()
    return Path(env) if env else DEFAULT_EXPORT_PATH


def _load_artifact() -> dict[str, Any]:
    path = _resolve_artifact_path()
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "error": "critic_v2_artifact_missing",
                "message": (
                    "Critic v2 UI export не найден. Сначала сформируйте artifact."
                ),
                "expected_path": str(path),
                "hint_command": REPLAY_HINT,
            },
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "critic_v2_artifact_unreadable",
                "message": f"Не удалось прочитать artifact: {exc}",
                "path": str(path),
            },
        )


def _normalize(name: str) -> str:
    """Нормализация: lowercase, trim, отрезать .pdf, схлопнуть пробелы."""
    if not name:
        return ""
    s = str(name).strip().lower()
    if s.endswith(".pdf"):
        s = s[:-4].rstrip()
    return " ".join(s.split())


def _match_project(items: list[dict[str, Any]], project_id: str) -> tuple[
    list[dict[str, Any]], str | None
]:
    """
    Сопоставляет items текущему проекту.

    Стратегия:
    1. exact match: item.project_name == project_id
    2. exact match без .pdf
    3. normalized match
    """
    target = (project_id or "").strip()
    if not target:
        return [], None

    # 1. exact
    exact = [it for it in items if it.get("project_name") == target]
    if exact:
        return exact, "project_name"

    # 2. exact без .pdf
    target_no_pdf = target[:-4].rstrip() if target.lower().endswith(".pdf") else target
    if target_no_pdf != target:
        m = [it for it in items if it.get("project_name") == target_no_pdf]
        if m:
            return m, "project_name_no_pdf"

    # 3. normalized
    norm_target = _normalize(target)
    if norm_target:
        m = [it for it in items if _normalize(it.get("project_name", "")) == norm_target]
        if m:
            return m, "normalized"

    return [], None


def _recompute_summary(
    items: list[dict[str, Any]], src_summary: dict[str, Any]
) -> dict[str, Any]:
    """Пересчёт summary только по items текущего проекта."""
    total = len(items)
    primary = sum(1 for it in items if it.get("tab") == "primary")
    needs_context = sum(1 for it in items if it.get("tab") == "needs_context")
    suggested_reject = sum(1 for it in items if it.get("tab") == "suggested_reject")
    hidden = sum(1 for it in items if it.get("tab") == "hidden_by_critic")
    collapsed = needs_context + suggested_reject + hidden

    reduction_pct = round((collapsed / total) * 100, 1) if total else 0.0

    accepted_total = sum(1 for it in items if it.get("human_decision") == "accepted")
    accepted_not_hidden = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and it.get("tab") != "hidden_by_critic"
    )
    accepted_primary = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and it.get("tab") == "primary"
    )

    nh_recall = round(accepted_not_hidden / accepted_total, 4) if accepted_total else None
    pv_recall = round(accepted_primary / accepted_total, 4) if accepted_total else None

    return {
        "total": total,
        "primary_count": primary,
        "needs_context_count": needs_context,
        "suggested_reject_count": suggested_reject,
        "hidden_by_critic_count": hidden,
        "primary_queue_reduction_percent": reduction_pct,
        "accepted_not_hidden_recall": nh_recall,
        "accepted_primary_visible_recall": pv_recall,
        "profile": src_summary.get("profile"),
        "experimental": True,
    }


def _recompute_tabs(
    items: list[dict[str, Any]], src_tabs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Сохраняет порядок и метаданные исходных вкладок, пересчитывает count."""
    counts: dict[str, int] = {}
    for it in items:
        t = it.get("tab")
        if t:
            counts[t] = counts.get(t, 0) + 1
    out: list[dict[str, Any]] = []
    for tab in src_tabs:
        out.append({
            "key": tab.get("key"),
            "title": tab.get("title"),
            "default_open": tab.get("default_open"),
            "queues": tab.get("queues", []),
            "count": counts.get(tab.get("key"), 0),
        })
    return out


@router.get("/critic-v2/artifact-info")
async def critic_v2_artifact_info() -> dict[str, Any]:
    """Диагностический endpoint: путь к artifact и его статус."""
    path = _resolve_artifact_path()
    info: dict[str, Any] = {
        "expected_path": str(path),
        "exists": path.exists(),
        "experimental": True,
        "production_pipeline_modified": False,
    }
    if path.exists():
        try:
            stat = path.stat()
            info["size_bytes"] = stat.st_size
            info["mtime"] = int(stat.st_mtime)
        except OSError:
            pass
    else:
        info["hint_command"] = REPLAY_HINT
    return info


@router.get("/critic-v2/projects/{project_id:path}/triage-ui")
async def project_critic_v2_triage_ui(project_id: str) -> dict[str, Any]:
    """
    Возвращает project-scoped UI export для experimental Critic v2 view.

    Read-only: не пишет файлов, не вызывает LLM, не меняет 03_findings_review.json.
    """
    artifact = _load_artifact()
    src_items = artifact.get("items", []) or []
    src_tabs = artifact.get("tabs", []) or []
    src_summary = artifact.get("summary", {}) or {}

    matched_items, matched_by = _match_project(src_items, project_id)

    scope = {
        "mode": "project",
        "project_id": project_id,
        "project_name": matched_items[0].get("project_name") if matched_items else None,
        "matched_by": matched_by,
    }

    if not matched_items:
        # проект не найден в общем artifact
        return {
            "summary": _recompute_summary([], src_summary),
            "tabs": _recompute_tabs([], src_tabs),
            "items": [],
            "scope": scope,
            "warning": (
                "Этот проект отсутствует в Critic v2 UI export. "
                "Возможно, для него ещё не запускался matrix replay. "
                "Замечания не удалены, production pipeline не изменён."
            ),
            "hint_command": REPLAY_HINT,
            "experimental": True,
            "production_pipeline_modified": False,
        }

    return {
        "summary": _recompute_summary(matched_items, src_summary),
        "tabs": _recompute_tabs(matched_items, src_tabs),
        "items": matched_items,
        "scope": scope,
        "experimental": True,
        "production_pipeline_modified": False,
    }
