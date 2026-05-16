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

from fastapi import APIRouter, HTTPException, Query

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

# Default location for previously exported reviewer feedback files.
# Resolved relative to repo root (parent of backend/) so it works regardless of
# CWD. Can be overridden via env CRITIC_V2_FEEDBACK_DIR for tests / staging.
_REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_FEEDBACK_DIR = _REPO_ROOT / "critic v2 test"


def _resolve_artifact_path() -> Path:
    env = os.environ.get("CRITIC_V2_UI_EXPORT_PATH", "").strip()
    return Path(env) if env else DEFAULT_EXPORT_PATH


# ─── Read-only cache (key = abs path + mtime_ns + size) ─────────────────────
# critic_v2_triage_ui.json — единый файл со всеми проектами, может занимать
# мегабайты. Без кеша каждый GET /triage-ui перечитывает и парсит его, а ещё
# линейно сканирует items для project-фильтра. Кешируем:
#   - parsed JSON;
#   - предвычисленный индекс project_name → list[item] (exact + normalized).
# Инвалидация по (path, mtime_ns, size). Cache никогда ничего не пишет.

_CACHE: dict[str, Any] = {
    "key": None,
    "artifact": None,
    "items_by_exact": {},      # project_name -> list[item]
    "items_by_normalized": {}, # _normalize(project_name) -> list[item]
}


def _cache_key_for(path: Path) -> tuple[str, int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return (str(path), st.st_mtime_ns, st.st_size)


def _build_project_index(items: list[dict[str, Any]]) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, list[dict[str, Any]]],
]:
    by_exact: dict[str, list[dict[str, Any]]] = {}
    by_normalized: dict[str, list[dict[str, Any]]] = {}
    for it in items:
        name = it.get("project_name")
        if not isinstance(name, str) or not name:
            continue
        by_exact.setdefault(name, []).append(it)
        norm = _normalize(name)
        if norm:
            by_normalized.setdefault(norm, []).append(it)
    return by_exact, by_normalized


def _load_artifact() -> dict[str, Any]:
    """Read artifact with mtime-based cache. Returns parsed JSON.

    Side effect: keeps `_CACHE` warm with parsed artifact + project index so
    `_match_project()` can use O(1) lookups instead of O(N) scans.
    """
    path = _resolve_artifact_path()
    if not path.exists():
        # инвалидируем кеш — иначе следующий read с восстановленным файлом
        # мог бы попасть на старые данные из памяти процесса
        _CACHE["key"] = None
        _CACHE["artifact"] = None
        _CACHE["items_by_exact"] = {}
        _CACHE["items_by_normalized"] = {}
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
    key = _cache_key_for(path)
    if key is not None and _CACHE["key"] == key and _CACHE["artifact"] is not None:
        return _CACHE["artifact"]
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "critic_v2_artifact_unreadable",
                "message": f"Не удалось прочитать artifact: {exc}",
                "path": str(path),
            },
        )
    items = artifact.get("items", []) or []
    by_exact, by_normalized = _build_project_index(items)
    _CACHE["key"] = key
    _CACHE["artifact"] = artifact
    _CACHE["items_by_exact"] = by_exact
    _CACHE["items_by_normalized"] = by_normalized
    return artifact


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

    Если кеш содержит готовый индекс — используем O(1) lookup; иначе fallback
    на линейный скан переданных items (чтобы функцию можно было вызывать и
    напрямую с произвольным списком, в т.ч. в тестах).
    """
    target = (project_id or "").strip()
    if not target:
        return [], None

    use_index = (
        _CACHE.get("artifact") is not None
        and (_CACHE["artifact"].get("items") or []) is items
    )

    if use_index:
        by_exact = _CACHE["items_by_exact"]
        by_normalized = _CACHE["items_by_normalized"]
        # 1. exact
        if target in by_exact:
            return by_exact[target], "project_name"
        # 2. exact без .pdf — только если target оканчивается на .pdf
        # (точно зеркалит fallback-ветку, чтобы поведение совпадало 1-в-1).
        if target.lower().endswith(".pdf"):
            stripped = target[:-4].rstrip()
            if stripped and stripped in by_exact:
                return by_exact[stripped], "project_name_no_pdf"
        # 3. normalized
        norm_target = _normalize(target)
        if norm_target and norm_target in by_normalized:
            return by_normalized[norm_target], "normalized"
        return [], None

    # Fallback: linear scan (без кеша)
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


# ─── Feedback files (read-only listing + read) ────────────────────────────
# Reviewer feedback is persisted as JSON files (output of cv2ExportFeedback in
# the UI). Listing them and serving the file content lets the UI re-hydrate
# preferred_tab state after a browser reload, so findings the expert moved to
# "suggested_reject" actually appear in that queue.
#
# Strict rules (mirror the rest of this module):
# - read-only: no writes, no LLM, no production pipeline mutation
# - directory restricted to CRITIC_V2_FEEDBACK_DIR (default <repo>/critic v2 test/)
# - filename must end with _feedback.json (or .json with feedback in the name)
#   to avoid serving unrelated artifacts
# - reject path traversal (no slashes, no ..)


def _resolve_feedback_dir() -> Path:
    env = os.environ.get("CRITIC_V2_FEEDBACK_DIR", "").strip()
    return Path(env) if env else DEFAULT_FEEDBACK_DIR


def _is_safe_feedback_name(name: str) -> bool:
    if not name or "/" in name or "\\" in name or ".." in name:
        return False
    if not name.lower().endswith(".json"):
        return False
    return True


def _scan_feedback_file(path: Path) -> dict[str, Any] | None:
    """Read top-level metadata from a feedback file: scope, totals, sr-count.

    Returns None if the file is unreadable. Logic is read-only and tolerant —
    a broken file just won't be auto-matched, not crash the listing.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    scope = data.get("scope") if isinstance(data, dict) else None
    if not isinstance(scope, dict):
        scope = {}
    feedback = data.get("feedback") if isinstance(data, dict) else None
    if not isinstance(feedback, list):
        feedback = []
    sr = sum(
        1 for e in feedback
        if isinstance(e, dict) and e.get("preferred_tab") == "suggested_reject"
    )
    return {
        "scope_project_name": scope.get("project_name") or scope.get("project_id") or "",
        "scope_mode": scope.get("mode") or "",
        "entries": len(feedback),
        "suggested_reject_count": sr,
    }


@router.get("/critic-v2/feedback-files")
async def list_feedback_files(
    project_id: str | None = Query(None, description="optional project filter"),
) -> dict[str, Any]:
    """List *.json files (with 'feedback' in name) in the feedback directory.

    Each entry carries scope_project_name + counts so the UI can pick the
    right file without reading every one client-side. If `project_id` is
    passed, the response also returns `matches[]` sorted by match-quality
    (best first) for that project.
    """
    feedback_dir = _resolve_feedback_dir()
    files: list[dict[str, Any]] = []
    if feedback_dir.exists() and feedback_dir.is_dir():
        for p in sorted(feedback_dir.iterdir()):
            if not p.is_file():
                continue
            name = p.name
            if not name.lower().endswith(".json"):
                continue
            if "feedback" not in name.lower():
                continue
            try:
                stat = p.stat()
            except OSError:
                continue
            entry: dict[str, Any] = {
                "name": name,
                "size": stat.st_size,
                "mtime": int(stat.st_mtime),
            }
            meta = _scan_feedback_file(p)
            if meta is not None:
                entry.update(meta)
            files.append(entry)

    out: dict[str, Any] = {
        "feedback_dir": str(feedback_dir),
        "exists": feedback_dir.exists(),
        "files": files,
    }

    if project_id is not None and project_id.strip():
        target = project_id.strip()
        target_no_pdf = (
            target[:-4].rstrip() if target.lower().endswith(".pdf") else target
        )
        target_norm = _normalize(target)
        matches: list[dict[str, Any]] = []
        for f in files:
            scope_name = f.get("scope_project_name") or ""
            if not scope_name:
                continue
            quality: str | None = None
            if scope_name == target:
                quality = "exact"
            elif scope_name == target_no_pdf:
                quality = "exact_no_pdf"
            elif _normalize(scope_name) == target_norm:
                quality = "normalized"
            elif target_norm and target_norm in _normalize(scope_name):
                quality = "substring"
            if quality is not None:
                matches.append({
                    "name": f["name"],
                    "scope_project_name": scope_name,
                    "match_quality": quality,
                    "entries": f.get("entries", 0),
                    "suggested_reject_count": f.get("suggested_reject_count", 0),
                    "mtime": f.get("mtime", 0),
                })
        # Sort by quality (exact > exact_no_pdf > normalized > substring), then
        # by recency. Best match first.
        _quality_rank = {
            "exact": 0, "exact_no_pdf": 1, "normalized": 2, "substring": 3,
        }
        matches.sort(key=lambda m: (
            _quality_rank.get(m["match_quality"], 99),
            -m.get("mtime", 0),
        ))
        out["project_id"] = target
        out["matches"] = matches

    return out


@router.get("/critic-v2/feedback-files/{name}")
async def read_feedback_file(name: str) -> dict[str, Any]:
    """Return the parsed contents of a feedback file from the feedback dir."""
    if not _is_safe_feedback_name(name):
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_feedback_filename", "name": name},
        )
    feedback_dir = _resolve_feedback_dir()
    path = feedback_dir / name
    if not path.exists() or not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={"error": "feedback_file_not_found", "name": name, "dir": str(feedback_dir)},
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=500,
            detail={"error": "feedback_file_unreadable", "name": name, "message": str(exc)},
        )
