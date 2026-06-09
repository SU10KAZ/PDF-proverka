"""Opus-only (unified-analysis) запуск по выбранным парам.

Режим «Только Opus»: Opus читает уже готовые `left_enriched.md` /
`right_enriched.md` и (пере)создаёт `enriched_comparison/comparison_result.json`.
Qwen / large-sheet enrichment / md-enrichment НЕ запускаются, enriched MD не
пересобирается, page_enriched.json / OCR / исходные PDF не трогаются.

Этот модуль делает только per-pair eligibility-проверку и (опционально)
backup/clear текущего `comparison_result.json`. Сам Opus запускает router через
`unified_analysis_jobs.create_unified_job(force_enrichment=False,
force_compare=True)`.

Пара пропускается с причиной:
* `unknown_pair` — нет в сессии;
* `running_job` — по паре идёт running/queued job;
* `missing_enriched_md` — нет left/right enriched MD;
* `too_large` — суммарный объём enriched MD превышает лимит Opus (для таких пар
  используйте per-pair fallback-бейдж «⚠ файл большой ▸ fallback»).

`expert_review.json` / `v2_review_status.json` НЕ трогаются (ручные отметки
сохраняются; их удаляет только полная clear-analysis).
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import paths as paths_mod
from . import enriched_comparison as ec_mod
from . import clear_analysis as clr_mod


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _enriched_md_ready(session_id: str, pair_id: str) -> tuple[bool, int]:
    """(обе стороны enriched MD существуют, суммарный объём символов)."""
    md = ec_mod.enriched_md_status(session_id, pair_id)
    left_ok = bool((md.get("left") or {}).get("exists"))
    right_ok = bool((md.get("right") or {}).get("exists"))
    total = int(md.get("total_chars") or 0)
    return (left_ok and right_ok), total


def _backup_comparison_result(
    session_id: str, pair_id: str, *, remove: bool,
) -> Optional[str]:
    """Бэкап текущего `comparison_result.json` (+ raw/job/prompt, если есть) в
    `_backup_before_opus_only_<ts>/enriched_comparison/`. При `remove=True`
    после бэкапа удаляет сам `comparison_result.json`.

    `expert_review.json` / `v2_review_status.json` НЕ трогаются.
    Возвращает путь backup-папки или None, если бэкапить нечего."""
    pair_root = paths_mod.pair_dir(session_id, pair_id)
    res = paths_mod.enriched_comparison_result_path(session_id, pair_id)
    if not res.exists():
        return None
    backup_root = pair_root / f"_backup_before_opus_only_{_utc_stamp()}"
    ec_dir = res.parent  # enriched_comparison/
    targets = [res]
    for name in ("raw_response.txt", "job.json", "prompt.md"):
        p = ec_dir / name
        if p.exists():
            targets.append(p)
    for p in targets:
        try:
            rel = p.relative_to(pair_root)
        except ValueError:
            rel = Path(p.name)
        dst = backup_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(p, dst)
        except OSError:
            pass
    if remove:
        try:
            res.unlink()
        except OSError:
            pass
    return str(backup_root)


def prepare_opus_only(
    session_id: str,
    pair_ids: list[str],
    *,
    backup_existing: bool = True,
    clear_comparison_result: bool = False,
) -> dict:
    """Per-pair eligibility + (опц.) backup/clear `comparison_result.json`.

    НЕ запускает Opus (это делает router через `create_unified_job`). Только
    чтение/файловые операции. Возвращает
    `{eligible: [...], skipped: [{pair_id, reason, ...}], backups: {pid: path}}`.
    """
    from . import store as store_mod
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    known = {str(p.get("id")) for p in (session.get("pairs") or [])
             if isinstance(p, dict) and p.get("id")}
    active = clr_mod.active_pair_ids(session_id)
    cfg = ec_mod.load_config()
    limit = cfg.max_chars if (cfg.max_chars and cfg.max_chars > 0) else 0

    eligible: list[str] = []
    skipped: list[dict] = []
    backups: dict[str, str] = {}
    seen: set[str] = set()
    for raw in pair_ids or []:
        pid = str(raw)
        if pid in seen:
            continue
        seen.add(pid)
        if pid not in known:
            skipped.append({"pair_id": pid, "reason": "unknown_pair"})
            continue
        if pid in active:
            skipped.append({"pair_id": pid, "reason": "running_job"})
            continue
        ready, total = _enriched_md_ready(session_id, pid)
        if not ready:
            skipped.append({"pair_id": pid, "reason": "missing_enriched_md"})
            continue
        if limit and total > limit:
            skipped.append({"pair_id": pid, "reason": "too_large",
                            "total_chars": total, "limit_chars": limit})
            continue
        if backup_existing or clear_comparison_result:
            bp = _backup_comparison_result(
                session_id, pid, remove=bool(clear_comparison_result))
            if bp:
                backups[pid] = bp
        eligible.append(pid)
    return {"eligible": eligible, "skipped": skipped, "backups": backups}


__all__ = ["prepare_opus_only"]
