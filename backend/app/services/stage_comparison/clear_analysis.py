"""Per-pair «очистить анализ» — сброс найденных расхождений и ручных отметок
проверки по выбранным парам сессии «Сравнение стадий».

Назначение: вернуть пару в состояние «не проверена» (нет comparison_result,
нет review-статусов, нет экспертных решений), НЕ удаляя дорогие исходники и
обогащение. После очистки повторный прогон даёт чистое сравнение.

ГАРАНТИИ (hard floor — НИКОГДА не удаляется этим модулем):
  * исходные PDF;
  * OCR `result.json`;
  * `page_enriched.json` и tile-результаты (large-sheet Qwen артефакты);
  * `left_enriched.md` / `right_enriched.md`;
  * `text_enrichment/cache` (Qwen image-cache);
  * `pair.json`, `page_alignment.json`, `links.json`, рендеры страниц/кропов.

Перед любым удалением создаётся backup в
`comparison/sessions/<sid>/pairs/<pid>/_backup_before_clear_analysis_<ts>/`.

Если по паре есть running/queued job — пара НЕ очищается (warning
«pair has running job, cancel first»).
"""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from . import paths as paths_mod
from . import store as store_mod
from . import expert_review as expert_review_mod
from . import unified_analysis_jobs as unified_jobs_mod

_ACTIVE_STATUSES = ("running", "queued")
_RUNNING_JOB_REASON = "pair has running job, cancel first"


def _utc_stamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


# ─── running-job detection ───────────────────────────────────────────────────

def _add_job_pairs(acc: set[str], job: Optional[dict]) -> None:
    if not isinstance(job, dict):
        return
    pid = job.get("pair_id")
    if pid:
        acc.add(str(pid))
    for p in job.get("pair_ids") or []:
        if p:
            acc.add(str(p))
    for it in job.get("items") or []:
        if isinstance(it, dict) and it.get("pair_id"):
            acc.add(str(it["pair_id"]))


def active_pair_ids(session_id: str) -> set[str]:
    """Множество pair_id, по которым прямо сейчас есть running/queued job
    (unified Opus — единственный оставшийся тип после удаления локальных
    LLM-мощностей). Детектор обёрнут в try/except (fail-soft): его сбой не
    блокирует очистку."""
    active: set[str] = set()

    try:
        job = unified_jobs_mod.find_active_session_job(session_id)
        if job and (job.get("status") or "") in _ACTIVE_STATUSES:
            _add_job_pairs(active, job)
    except Exception:  # noqa: BLE001 — детектор не должен ронять очистку
        pass

    return active


def pair_active_job_reason(session_id: str, pair_id: str) -> Optional[str]:
    """Вернуть причину-блокировку, если по паре идёт работа, иначе None."""
    if str(pair_id) in active_pair_ids(session_id):
        return _RUNNING_JOB_REASON
    return None


# ─── per-pair clear ──────────────────────────────────────────────────────────

def _findings_paths(session_id: str, pair_id: str) -> list[Path]:
    """Найденные расхождения (выход Opus-сравнения)."""
    return [
        paths_mod.enriched_comparison_result_path(session_id, pair_id),
        paths_mod.enriched_comparison_raw_path(session_id, pair_id),
        paths_mod.enriched_comparison_job_path(session_id, pair_id),
        paths_mod.enriched_comparison_prompt_path(session_id, pair_id),
        paths_mod.enriched_comparison_fallback_progress_path(session_id, pair_id),
    ]


def _review_paths(session_id: str, pair_id: str) -> list[Path]:
    """Ручные отметки проверки (per-pair файлы). expert_review.json —
    session-level, чистится отдельно по ключам пары."""
    return [
        paths_mod.v2_review_status_path(session_id, pair_id),
        paths_mod.v2_excluded_changes_path(session_id, pair_id),
    ]


def _enrichment_extra_paths(session_id: str, pair_id: str) -> list[Path]:
    """Производные diff/precheck-кеши. НЕ Qwen-обогащение: page_enriched/
    tiles/enriched MD/large-sheet НЕ входят. Регенерируются на лету."""
    return [
        paths_mod.graphic_diffs_path(session_id, pair_id),
        paths_mod.text_llm_diff_path(session_id, pair_id),
        paths_mod.text_diff_cache_path(session_id, pair_id),
    ]


def _enrichment_extra_dirs(session_id: str, pair_id: str) -> list[Path]:
    return [paths_mod.block_equivalence_dir(session_id, pair_id)]


def _backup_then_remove(
    targets: list[Path], dirs: list[Path], pair_root: Path, backup_root: Path,
) -> tuple[list[str], list[str]]:
    """Скопировать существующие файлы/папки в backup (с сохранением
    относительной структуры внутри pair_root), затем удалить. Возвращает
    (deleted_relpaths, skipped_relpaths)."""
    deleted: list[str] = []
    skipped: list[str] = []

    for p in targets:
        try:
            rel = p.relative_to(pair_root)
        except ValueError:
            rel = Path(p.name)
        if not p.exists():
            continue
        dst = backup_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(p, dst)
            p.unlink()
            deleted.append(str(rel))
        except OSError:
            skipped.append(str(rel))

    for d in dirs:
        try:
            rel = d.relative_to(pair_root)
        except ValueError:
            rel = Path(d.name)
        if not d.exists() or not d.is_dir():
            continue
        dst = backup_root / rel
        try:
            shutil.copytree(d, dst, dirs_exist_ok=True)
            shutil.rmtree(d, ignore_errors=True)
            deleted.append(str(rel) + "/")
        except OSError:
            skipped.append(str(rel) + "/")

    return deleted, skipped


def clear_pair_analysis(
    session_id: str,
    pair_id: str,
    *,
    clear_findings: bool = True,
    clear_review: bool = True,
    clear_enrichment: bool = False,
) -> dict:
    """Очистить анализ/проверку одной пары. Сначала backup, потом удаление.

    Возвращает dict: pair_id, backup_path (str|None), deleted_files[],
    skipped[], expert_review_removed_keys (int)."""
    pair_root = paths_mod.pair_dir(session_id, pair_id)
    backup_root = pair_root / f"_backup_before_clear_analysis_{_utc_stamp()}"

    file_targets: list[Path] = []
    dir_targets: list[Path] = []
    if clear_findings:
        file_targets += _findings_paths(session_id, pair_id)
    if clear_review:
        file_targets += _review_paths(session_id, pair_id)
    if clear_enrichment:
        file_targets += _enrichment_extra_paths(session_id, pair_id)
        dir_targets += _enrichment_extra_dirs(session_id, pair_id)

    # Backup session-level expert_review.json ДО удаления ключей пары.
    er_removed = 0
    er_backed = False
    if clear_review:
        er_path = paths_mod.expert_review_path(session_id)
        if er_path.exists():
            backup_root.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(er_path, backup_root / "expert_review.json")
                er_backed = True
            except OSError:
                er_backed = False

    deleted, skipped = _backup_then_remove(
        file_targets, dir_targets, pair_root, backup_root)

    if clear_review and (er_backed or backup_root.exists()):
        try:
            res = expert_review_mod.clear_pairs(session_id, [pair_id])
            er_removed = len(res.get("removed_keys") or [])
            if er_removed:
                deleted.append(f"expert_review.json[{er_removed} keys]")
        except Exception:  # noqa: BLE001 — clear не должен падать целиком
            skipped.append("expert_review.json")

    backup_path = str(backup_root) if backup_root.exists() else None
    return {
        "pair_id": str(pair_id),
        "backup_path": backup_path,
        "deleted_files": deleted,
        "skipped": skipped,
        "expert_review_removed_keys": er_removed,
    }


def clear_pairs_analysis(
    session_id: str,
    pair_ids: list[str],
    *,
    clear_findings: bool = True,
    clear_review: bool = True,
    clear_enrichment: bool = False,
) -> dict:
    """Batch-очистка по выбранным pair_ids. Пары с running/queued job
    пропускаются (warning). Только пары, существующие в сессии."""
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")

    known = {str(p.get("id")) for p in (session.get("pairs") or [])
             if isinstance(p, dict) and p.get("id")}
    active = active_pair_ids(session_id)

    cleared: list[dict] = []
    backup_paths: list[str] = []
    deleted_files: list[str] = []
    skipped: list[dict] = []

    seen: set[str] = set()
    for raw in pair_ids or []:
        pid = str(raw).strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        if pid not in known:
            skipped.append({"pair_id": pid, "reason": "pair not in session"})
            continue
        if pid in active:
            skipped.append({"pair_id": pid, "reason": _RUNNING_JOB_REASON})
            continue
        res = clear_pair_analysis(
            session_id, pid,
            clear_findings=clear_findings,
            clear_review=clear_review,
            clear_enrichment=clear_enrichment,
        )
        cleared.append(res)
        if res.get("backup_path"):
            backup_paths.append(res["backup_path"])
        for f in res.get("deleted_files") or []:
            deleted_files.append(f"{pid}/{f}")

    return {
        "ok": True,
        "cleared_pairs": len(cleared),
        "backup_paths": backup_paths,
        "deleted_files": deleted_files,
        "skipped": skipped,
        "results": cleared,
    }


__all__ = [
    "active_pair_ids",
    "pair_active_job_reason",
    "clear_pair_analysis",
    "clear_pairs_analysis",
]
