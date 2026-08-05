"""
findings_merge/runner.py
------------------------
Stage runner для этапа findings_merge (свод замечаний → 03_findings.json).

Содержит:
  - Pure helper functions (backfill_text_evidence_in_findings, refresh_finding_quality,
    merge_similar_findings) — перенесены из manager.py в предыдущих pass-ах.
  - run_findings_merge(ctx) — полный merge stage через claude_runner.

Публичный API (helpers):
  backfill_text_evidence_in_findings(project_id)
  refresh_finding_quality(project_id, filename)
  merge_similar_findings(project_id) -> dict | None
  renumber_findings_sequentially(project_id) -> dict | None

Публичный API (runner):
  run_findings_merge(ctx) -> FindingsMergeResult
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    resolve_existing,
)
import backend.app.services.llm.claude_runner as claude_runner
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.block_analysis.runner import validate_and_repair_json
from backend.app.pipeline.stages.prepare.graph_builder import (
    build_block_page_index,
    build_md_line_page_index,
    build_page_sheet_map,
    looks_like_sheet_ref,
    resolve_document_markdown,
    resolve_finding_sheet_label,
)
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited
from backend.app.services.common.project_service import resolve_project_dir


def _error_detail(exit_code: int, output: str, max_len: int = 200) -> str:
    """Extract useful error from CLI output."""
    if not output:
        return f"Exit code {exit_code}"
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    useful = [
        ln for ln in lines
        if any(kw in ln.lower() for kw in (
            "error", "ошибка", "failed", "timeout", "rate limit",
            "exception", "traceback", "invalid", "json", "cannot",
        ))
    ]
    msg = " | ".join((useful or lines)[-3:])
    return msg[:max_len] if len(msg) > max_len else msg


# ─── Pure helper functions (re-exported from previous pass) ──────────────────

def _version_output_dir(project_id: str):
    """Папка _output активной версии. AUDIT_OUTPUT_DIR override → иначе единый
    резолвер version_service.resolve_active_output_dir (reserc.md #97, v2-aware)."""
    env_output_dir = os.environ.get("AUDIT_OUTPUT_DIR")
    if env_output_dir:
        return Path(env_output_dir)
    from backend.app.services.common import version_service
    return version_service.resolve_active_output_dir(project_id)


def backfill_text_evidence_in_findings(project_id: str, output_dir: Path | None = None):
    """Backfill text-evidence + sheet в 03_findings.json.

    1. selected_text_block_ids/evidence_text_refs — из 01_blocks_analysis.json
    2. sheet — детерминированно из document_graph.json page_sheet_map

    output_dir: явная папка артефактов (run dir стадии). Без неё резолв по
    project_id даёт v2 latest — НЕ ту копию, в которую пишет стадия, и вся
    работа затиралась последующим promote run→latest.
    """
    output_dir = output_dir or _version_output_dir(project_id)
    findings_path = output_dir / "03_findings.json"
    blocks_path = resolve_existing(output_dir, BLOCKS_ANALYSIS_FILENAME)
    graph_path = output_dir / "document_graph.json"

    if not findings_path.exists():
        return

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    # Индекс block_id → block_analysis data из 02
    bc_index = {}
    if blocks_path.exists():
        try:
            data02 = json.loads(blocks_path.read_text(encoding="utf-8"))
            for ba in data02.get("block_analyses", []):
                bid = ba.get("block_id", "")
                if bid:
                    bc_index[bid] = ba
        except (json.JSONDecodeError, OSError):
            pass

    # page_sheet_map из document_graph.json + block_id → page для замечаний
    # текстового этапа (у них page=None, но есть ссылки на блоки).
    psm: dict = {}
    bpi: dict = {}
    if graph_path.exists():
        try:
            graph = json.loads(graph_path.read_text(encoding="utf-8"))
            psm = build_page_sheet_map(graph)
            bpi = build_block_page_index(graph)
        except (json.JSONDecodeError, OSError):
            pass

    # Строки MD → страница: последний источник листа для замечаний текстового
    # этапа, у которых evidence несёт только md_lines.
    mdi: list = []
    md_path = resolve_document_markdown(output_dir)
    if md_path is not None:
        try:
            mdi = build_md_line_page_index(md_path.read_text(encoding="utf-8"))
        except OSError:
            pass

    modified = 0
    for finding in fd.get("findings", []):
        # ── Text-evidence backfill ──
        if not finding.get("selected_text_block_ids"):
            source_blocks = finding.get("source_block_ids", [])
            related_blocks = finding.get("related_block_ids", [])
            lookup_blocks = source_blocks or related_blocks

            all_stbi = []
            all_etr = []
            for bid in lookup_blocks:
                bc = bc_index.get(bid)
                if not bc:
                    continue
                for stbi in bc.get("selected_text_block_ids", []):
                    if stbi not in all_stbi:
                        all_stbi.append(stbi)
                for etr in bc.get("evidence_text_refs", []):
                    if etr not in all_etr:
                        all_etr.append(etr)

            if all_stbi:
                finding["selected_text_block_ids"] = all_stbi
                modified += 1
            if all_etr and not finding.get("evidence_text_refs"):
                finding["evidence_text_refs"] = all_etr

        # ── Sheet backfill (deterministic) ──
        sheet = finding.get("sheet")
        page = finding.get("page")
        sheet_empty = sheet is None or (isinstance(sheet, str) and not sheet.strip())
        # Занятое поле больше не считается корректным: merge копировал из
        # block-контекста НАЗВАНИЕ листа («Корпус 14.6. Маркировочные планы
        # 1 этажа»), и бэкфилл молчал → в UI/Excel вместо «Лист 2» название.
        sheet_is_name = not sheet_empty and not looks_like_sheet_ref(sheet)
        needs_sheet = sheet_empty or sheet_is_name

        if needs_sheet and psm:
            # Замечания текстового этапа приходят без page — для них лист
            # выводим по блокам, на которые они ссылаются (block_page_index).
            label = resolve_finding_sheet_label(finding, psm, bpi, mdi)
            if label:
                if sheet_is_name and not finding.get("sheet_title"):
                    # Название листа из штампа не теряем — переносим в отдельное
                    # поле, а sheet отдаём под номер.
                    finding["sheet_title"] = str(sheet).strip()
                finding["sheet"] = label
                finding.pop("sheet_unavailable", None)
                finding.pop("sheet_unavailable_reason", None)
                modified += 1
            elif sheet_empty:
                finding["sheet_unavailable"] = True
                finding["sheet_unavailable_reason"] = (
                    "page_not_in_map" if page is not None else "no_page"
                )
                modified += 1

    if modified > 0:
        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def apply_phase0_dedup(project_id: str, output_dir: Path | None = None) -> dict | None:
    """Phase 0 post-merge dedup: class-key + fuzzy. Gated by STAGE01_DEDUP_ENABLED.

    Reads 03_findings.json, runs class_dedup.collapse_to_canonical then
    fuzzy_dedup, writes back the deduped findings plus meta.dedup_report.

    Safety contract (see backend/app/services/findings/dedup/README.md):
      - КРИТИЧЕСКОЕ findings are never silently collapsed.
      - Output count never exceeds input count.
      - Fail-open: on any exception, original findings are preserved
        on disk and a {"error": ...} dict is returned. Pipeline never fails
        because of dedup.

    Returns:
        None if feature flag is off or the findings file is missing.
        Otherwise a telemetry dict:
            {
              "enabled": True,
              "before": int, "after": int,
              "class_dedup": {...DedupReport.to_dict()...},
              "fuzzy_dedup": {...DedupReport.to_dict()...},
              "critical_collapsed_count": int,  # combined, MUST be 0 in production
              "duration_ms": int,
              "error": str | None,
            }
    """
    from backend.app.core.config import (
        STAGE01_DEDUP_ENABLED,
        STAGE01_DEDUP_FUZZY_THRESHOLD,
    )
    if not STAGE01_DEDUP_ENABLED:
        return None

    output_dir = output_dir or _version_output_dir(project_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return None

    import time
    started = time.monotonic()
    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return {
            "enabled": True,
            "before": 0,
            "after": 0,
            "error": f"read_failed: {exc}",
            "duration_ms": int((time.monotonic() - started) * 1000),
        }

    items = fd.get("findings") or fd.get("items") or []
    if not items:
        return {
            "enabled": True,
            "before": 0,
            "after": 0,
            "duration_ms": int((time.monotonic() - started) * 1000),
        }

    before = len(items)
    try:
        from backend.app.services.findings.dedup import (
            collapse_to_canonical, fuzzy_dedup,
        )
        kept, class_report = collapse_to_canonical(items)
        kept, fuzzy_report = fuzzy_dedup(
            kept, sim_threshold=STAGE01_DEDUP_FUZZY_THRESHOLD,
        )
        crit_count = int(
            class_report.critical_collapsed_count
            + fuzzy_report.critical_collapsed_count
        )
        # Defensive: output count must not exceed input count.
        if len(kept) > before:
            raise AssertionError(
                f"phase0_dedup count invariant violated: in={before} out={len(kept)}"
            )

        # reserc.md #28: merge_similar_findings перенумеровывает F-ID, а dedup
        # дропает записи ПОСЛЕ него → в id появлялись дыры (F-001, F-003, …).
        # Перенумеровываем оставшиеся сквозь F-001..F-NNN: это ФИНАЛЬНЫЙ шаг
        # findings_merge, downstream (critic/norms/opt) ещё не читал эти id и
        # ссылок на старые id внутри прогона нет → безопасно. Идемпотентно
        # (на уже-сплошной нумерации даёт тот же результат).
        for _new_idx, _it in enumerate(kept, start=1):
            _it["id"] = f"F-{_new_idx:03d}"

        # ── Write back ────────────────────────────────────────────────────
        fd["findings"] = kept
        meta = fd.get("meta") or {}
        meta_dedup = {
            "class_dedup": class_report.to_dict(),
            "fuzzy_dedup": fuzzy_report.to_dict(),
            "before": before,
            "after": len(kept),
            "critical_collapsed_count": crit_count,
            "fuzzy_threshold": STAGE01_DEDUP_FUZZY_THRESHOLD,
        }
        meta["dedup_report"] = meta_dedup
        # If existing total_findings was set by merge_similar_findings, refresh.
        if "total_findings" in meta:
            meta["total_findings"] = len(kept)
        if "by_severity" in meta:
            by_sev: dict[str, int] = {}
            for it in kept:
                sev = it.get("severity", "НЕИЗВЕСТНО")
                by_sev[sev] = by_sev.get(sev, 0) + 1
            meta["by_severity"] = by_sev
        fd["meta"] = meta

        # Бэкап исходного 03_findings перед write-back (как pre_merge в
        # merge_similar_findings) — дедуп не должен быть безвозвратным; идемпотентно
        # сохраняем только первый pre-dedup снимок (reserc.md #29).
        import shutil
        backup_path = output_dir / "03_findings_pre_dedup.json"
        if not backup_path.exists():
            try:
                shutil.copy2(findings_path, backup_path)
            except OSError:
                pass

        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8",
        )

        return {
            "enabled": True,
            "before": before,
            "after": len(kept),
            "class_dedup": class_report.to_dict(),
            "fuzzy_dedup": fuzzy_report.to_dict(),
            "critical_collapsed_count": crit_count,
            "duration_ms": int((time.monotonic() - started) * 1000),
            "error": None,
        }
    except Exception as exc:
        return {
            "enabled": True,
            "before": before,
            "after": before,
            "error": f"{type(exc).__name__}: {exc}",
            "duration_ms": int((time.monotonic() - started) * 1000),
        }


def renumber_findings_sequentially(
    project_id: str,
    output_dir: Path | None = None,
) -> dict | None:
    """Сплошная перенумерация F-ID по порядку следования в 03_findings.json.

    Почему безусловно (а не только внутри merge/dedup): F-ID выдаются в двух
    местах ХВОСТОМ, а элементы вставляются В СЕРЕДИНУ списка —
    `enforce_stage01_atomicity` расщепляет замечание и кладёт части сразу за
    родителем с номерами max_idx+1, а targeted-аудиты дописываются в конец.
    Сплошная нумерация раньше делалась только если merge_similar_findings слил
    хотя бы одну группу (merge_count > 0) либо был включён phase0-дедуп
    (STAGE01_DEDUP_ENABLED, по умолчанию OFF). Когда ни то, ни другое не
    сработало, в отчёте получалось «F-016, F-035, F-036, F-017».

    Это ФИНАЛЬНЫЙ шаг findings_merge: downstream (critic/norms/optimization,
    expert_review, decisions) читает id уже после него, ссылок на старые
    номера внутри прогона нет. Идемпотентна: на сплошной нумерации no-op.
    """
    output_dir = output_dir or _version_output_dir(project_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return None

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return {"renumbered": 0, "error": f"read_failed: {exc}"}

    items = fd.get("findings")
    if not isinstance(items, list) or not items:
        return {"renumbered": 0}

    id_map: dict[str, str] = {}
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        new_id = f"F-{index:03d}"
        old_id = str(item.get("id") or "")
        if old_id and old_id != new_id:
            id_map[old_id] = new_id

    if not id_map:
        return {"renumbered": 0}

    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        item["id"] = f"F-{index:03d}"
        guard = item.get("atomicity_guard")
        if isinstance(guard, dict):
            split_from = str(guard.get("split_from") or "")
            if split_from in id_map:
                guard["split_from"] = id_map[split_from]
        dup_of = str(item.get("duplicate_of") or "")
        if dup_of in id_map:
            item["duplicate_of"] = id_map[dup_of]

    meta = fd.get("meta")
    if isinstance(meta, dict):
        meta["findings_renumbered"] = len(id_map)
    findings_path.write_text(
        json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return {"renumbered": len(id_map), "total": len(items)}


def refresh_finding_quality(
    project_id: str,
    filename: str = "03_findings.json",
    output_dir: Path | None = None,
) -> dict | None:
    """Refresh deterministic practicality metadata for findings."""
    target_path = (output_dir or _version_output_dir(project_id)) / filename
    if not target_path.exists():
        return None

    try:
        from backend.app.services.findings.finding_quality import enrich_findings_file
        return enrich_findings_file(target_path)
    except Exception:
        return None


def merge_similar_findings(project_id: str, output_dir: Path | None = None) -> dict | None:
    """Объединить похожие замечания в 03_findings.json."""
    from backend.app.services.findings.findings_service import (
        _normalize_problem_pattern,
        aggregate_merged_fields,
    )
    from backend.app.pipeline.stages.block_analysis.provenance import (
        is_disputed_comparison,
    )
    from collections import OrderedDict
    import re as _re
    import shutil

    output_dir = output_dir or _version_output_dir(project_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return None

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    items = fd.get("findings", fd.get("items", []))
    if len(items) < 2:
        return None

    groups: OrderedDict[str, list[dict]] = OrderedDict()
    for index, f in enumerate(items):
        problem = f.get("problem") or f.get("description") or f.get("finding") or ""
        severity = f.get("severity", "")
        category = f.get("category", "")
        pattern = _normalize_problem_pattern(problem)
        if is_disputed_comparison(f.get("detector_comparison")):
            # A detector conflict is evidence to verify, not an ordinary
            # duplicate. Keep each disputed candidate until verification.
            key = f"__disputed__||{f.get('id') or index}"
        else:
            key = f"{severity}||{category}||{pattern}"
        if key not in groups:
            groups[key] = []
        groups[key].append(f)

    merged_items = []
    merge_count = 0
    new_id = 1
    for key, group_items in groups.items():
        if len(group_items) == 1:
            item = group_items[0]
            item["id"] = f"F-{new_id:03d}"
            merged_items.append(item)
            new_id += 1
        else:
            merge_count += 1
            leader = dict(group_items[0])
            leader["id"] = f"F-{new_id:03d}"
            new_id += 1

            details_lines = [
                f"{i}) " + (it.get("problem") or it.get("description")
                           or it.get("finding") or "")
                for i, it in enumerate(group_items, 1)
            ]
            leader_problem = leader.get("problem") or leader.get("description") or ""
            # Единая агрегация source-of-truth полей (reserc.md #92/#6/#27): sheet/
            # page/related_block_ids/source_block_ids/evidence_text_refs/evidence +
            # norm_quote/highlight_regions. Иначе critic ложно ставит no_evidence/
            # phantom и теряются норм-цитаты.
            leader.update(aggregate_merged_fields(group_items, leader))
            leader["problem"] = f"[Объединено {len(group_items)} замечаний] {leader_problem}"
            leader["description"] = "\n".join(details_lines)
            leader["sub_findings"] = [
                {
                    "original_id": it.get("id", ""),
                    "problem": it.get("problem") or it.get("description") or "",
                    "sheet": it.get("sheet", ""),
                    "page": it.get("page"),
                    "source_finding_ids": it.get("source_finding_ids") or [],
                }
                for it in group_items
            ]

            merged_items.append(leader)

    if merge_count == 0:
        return {"merged_groups": 0}

    meta = fd.get("meta", {})
    meta["total_findings"] = len(merged_items)
    meta["pre_merge_total"] = len(items)
    meta["merged_groups"] = merge_count

    by_severity = {}
    for it in merged_items:
        sev = it.get("severity", "НЕИЗВЕСТНО")
        by_severity[sev] = by_severity.get(sev, 0) + 1
    meta["by_severity"] = by_severity

    fd["meta"] = meta
    fd["findings"] = merged_items

    backup_path = output_dir / "03_findings_pre_merge.json"
    if not backup_path.exists():
        shutil.copy2(findings_path, backup_path)

    findings_path.write_text(
        json.dumps(fd, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "merged_groups": merge_count,
        "before": len(items),
        "after": len(merged_items),
    }


# ─── FindingsMergeResult ─────────────────────────────────────────────────────

@dataclass
class FindingsMergeResult:
    """Результат run_findings_merge."""
    success: bool
    cancelled: bool = False
    error: Optional[str] = None
    excluded_count: int = 0      # кол-во блоков вне полного анализа (для лога)
    findings_count: int = 0      # кол-во замечаний (для лога)


# ─── run_findings_merge ──────────────────────────────────────────────────────

async def run_findings_merge(ctx: PipelineStageContext) -> FindingsMergeResult:
    """Запуск свода замечаний через claude_runner.run_findings_merge.

    Управляет:
    - rate limit pre-check и retry после паузы;
    - валидацией и авторемонтом 03_findings.json;
    - post-merge операциями:
        backfill_text_evidence, merge_similar_findings,
        renumber_findings_sequentially, refresh_finding_quality,
        backfill_highlight_regions, attach_stage02_coverage;
    - update_pipeline_log("findings_merge", ...).

    Не управляет:
    - job.stage / job.status (выставляет оркестратор);
    - _stream_findings_events (WS — вызывает оркестратор после возврата);
    - heartbeat / cleanup (оркестратор);
    """
    pid = ctx.project_id
    project_info = ctx.project_info or {}
    output_dir = ctx.output_dir

    from backend.app.core.config import get_stage_model
    findings_model = get_stage_model("findings_merge")

    ctx.update_pipeline_log("findings_merge", "running")
    await ctx.log(f"═══ Свод замечаний ({findings_model}) ═══")

    can_go = await ctx.check_before_launch()
    if not can_go:
        ctx.update_pipeline_log("findings_merge", "error",
                                error="Rate limit: ожидание превышено")
        return FindingsMergeResult(success=False,
                                   error="Rate limit: ожидание превышено или отменено")

    exit_code, output, cli_result = await claude_runner.run_findings_merge(
        project_info, pid,
        on_output=ctx.log,
        output_dir=output_dir,
        version_dir=ctx.project_dir,
        version_id=ctx.version_id,
    )
    ctx.record_cli_usage(cli_result, "findings_merge")

    if is_cancelled(exit_code):
        return FindingsMergeResult(success=False, cancelled=True)

    if is_rate_limited(exit_code, output or "", ""):
        await ctx.log("Rate limit при своде замечаний, ожидание...", "warn")
        can_continue = await ctx.wait_for_rate_limit(
            "rate limit при своде замечаний", output or ""
        )
        if can_continue:
            exit_code, output, cli_result = await claude_runner.run_findings_merge(
                project_info, pid,
                on_output=ctx.log,
                output_dir=output_dir,
                version_dir=ctx.project_dir,
                version_id=ctx.version_id,
            )
            ctx.record_cli_usage(cli_result, "findings_merge_retry")
        else:
            ctx.update_pipeline_log("findings_merge", "error",
                                    error="Rate limit: ожидание превышено")
            return FindingsMergeResult(success=False,
                                       error="Rate limit: ожидание превышено или отменено")

    if exit_code != 0:
        error_detail = _error_detail(exit_code, output or "")
        ctx.update_pipeline_log("findings_merge", "error", error=error_detail)
        await ctx.log(f"Свод замечаний: код {exit_code}", "error")
        return FindingsMergeResult(success=False, error=f"Свод замечаний: код {exit_code}")

    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        error = "03_findings.json не создан"
        ctx.update_pipeline_log("findings_merge", "error", error=error)
        await ctx.log(f"{error} — свод замечаний считается ошибкой", "error")
        return FindingsMergeResult(success=False, error=error)

    # to_thread: repair-ветка функции квадратичная — на большом битом JSON
    # блокировала event loop (вотчдог убивал бэкенд, инциденты 03.07).
    import asyncio as _asyncio
    is_valid, repair_msg = await _asyncio.to_thread(
        validate_and_repair_json, findings_path
    )
    if not is_valid:
        error = f"03_findings.json невалиден: {repair_msg}"
        ctx.update_pipeline_log("findings_merge", "error", error=error)
        return FindingsMergeResult(success=False, error=error)
    if "Repaired" in repair_msg:
        await ctx.log(f"03_findings.json починен: {repair_msg}", "warn")

    ctx.update_pipeline_log("findings_merge", "done", message="OK")

    # ── Post-merge operations ─────────────────────────────────────────────────
    # ВСЕМ хелперам передаётся ctx.output_dir (run dir стадии). Раньше они
    # резолвили папку сами по pid → v2 latest: обогащали ЧУЖУЮ копию
    # 03_findings.json, которую последующий promote run→latest затирал.
    # Подтверждено эмпирически: у v2-прогонов 0 замечаний с finding_quality/
    # text_evidence/highlight_regions против 100% в legacy.
    from backend.app.pipeline.stages.block_analysis.provenance import (
        backfill_final_findings_provenance,
    )

    # Attach explicit Stage 01 comparison labels before any deterministic
    # collapse. This lets dedup protect unresolved GPT/Codex conflicts.
    backfill_final_findings_provenance(ctx.output_dir)
    backfill_text_evidence_in_findings(pid, output_dir=ctx.output_dir)

    merge_result = merge_similar_findings(pid, output_dir=ctx.output_dir)
    if merge_result and merge_result.get("merged_groups", 0) > 0:
        await ctx.log(
            f"Объединено похожих замечаний: {merge_result['before']} → {merge_result['after']} "
            f"({merge_result['merged_groups']} групп)",
        )

    # ── Phase 0 post-merge dedup (class-key + fuzzy) ─────────────────────────
    # OFF by default (STAGE01_DEDUP_ENABLED=false). On A0 baseline this is a
    # provable no-op. Fail-open: errors are logged, original findings preserved.
    dedup_telemetry = apply_phase0_dedup(pid, output_dir=ctx.output_dir)
    if dedup_telemetry and dedup_telemetry.get("enabled"):
        crit_collapsed = int(dedup_telemetry.get("critical_collapsed_count") or 0)
        err = dedup_telemetry.get("error")
        if err:
            await ctx.log(f"Phase 0 dedup: ошибка (findings оставлены без изменений) — {err}", "warn")
        elif crit_collapsed > 0:
            await ctx.log(
                f"Phase 0 dedup: ALARM critical_collapsed_count={crit_collapsed}"
                f" (must be 0 in production)",
                "error",
            )
        else:
            before = dedup_telemetry.get("before", 0)
            after = dedup_telemetry.get("after", 0)
            if before != after:
                await ctx.log(
                    f"Phase 0 dedup: {before} → {after} замечаний "
                    f"(class+fuzzy, threshold={dedup_telemetry.get('fuzzy_dedup', {}).get('sim_threshold')})",
                )
            else:
                await ctx.log("Phase 0 dedup: no-op (0 duplicates)")

    # ── Сплошная нумерация F-001…F-NNN ───────────────────────────────────────
    # Безусловно, а не побочным эффектом merge/dedup: atomicity_guard вставляет
    # расщеплённые замечания в середину списка с хвостовыми номерами, из-за чего
    # в отчёте шло «F-016, F-035, F-036, F-017».
    renumber_report = renumber_findings_sequentially(pid, output_dir=ctx.output_dir)
    if renumber_report and renumber_report.get("renumbered"):
        await ctx.log(
            f"Нумерация замечаний выровнена: изменено {renumber_report['renumbered']} "
            f"из {renumber_report.get('total', 0)} ID",
        )

    # Recalculate counts and aggregate labels after all merge/dedup passes.
    provenance_report = backfill_final_findings_provenance(ctx.output_dir)
    if provenance_report.get("updated"):
        counts = provenance_report.get("counts") or {}
        await ctx.log(
            "Источники замечаний: "
            f"GPT={counts.get('gpt', 0)}, Codex={counts.get('codex', 0)}, "
            f"GPT+Codex={counts.get('gpt_codex', 0)}, "
            f"без трассировки={counts.get('unattributed', 0)}"
        )

    refresh_finding_quality(pid, output_dir=ctx.output_dir)

    from backend.app.pipeline.stages.findings_merge.backfill_highlights import backfill_project
    backfill_project(ctx.project_dir, output_dir=ctx.output_dir)

    # После восстановления LLM-регионов: text-layer grounding не трогает уже
    # заполненные highlights, а в default shadow-режиме только пишет coverage/IoU.
    from backend.app.pipeline.stages.findings_merge.ground_highlights_textlayer import (
        backfill_textlayer_highlights,
    )
    textlayer = backfill_textlayer_highlights(ctx.project_dir, output_dir=ctx.output_dir)
    if textlayer.get("enabled"):
        mode = "shadow" if textlayer.get("shadow") else "live"
        await ctx.log(
            f"Text-layer highlights ({mode}): grounded "
            f"{textlayer.get('grounded', 0)}/{textlayer.get('checked', 0)}, "
            f"coverage={textlayer.get('coverage', 0):.1%}, "
            f"written={textlayer.get('fixed', 0)}"
        )

    from backend.app.pipeline.stages.block_analysis.runner import attach_stage02_coverage_to_findings
    coverage = attach_stage02_coverage_to_findings(pid, output_dir=ctx.output_dir)
    excluded_count = (coverage.get("summary") or {}).get("excluded_from_full_analysis_count", 0)
    if excluded_count:
        await ctx.log(
            f"В финальный отчёт добавлены блоки вне полноценного анализа: {excluded_count}",
            "warn",
        )

    # Гуманизация ссылок на блоки: внутренние block_id в текстах → подписи
    # «Название» (лист N, стр. PDF M). СТРОГО ПОСЛЕДНЕЙ из post-merge операций:
    # merge_similar_findings/дедуп группируют по _normalize_problem_pattern
    # сырых текстов (подписи схлопнули бы разные блоки в один паттерн), а
    # extract_anchors текст-слоя ищет якоря тоже по сырым формулировкам.
    # Fail-soft: ошибка не роняет merge.
    from backend.app.core.config import FINDINGS_BLOCK_CAPTIONS_ENABLED
    if FINDINGS_BLOCK_CAPTIONS_ENABLED:
        try:
            from backend.app.services.findings.block_captions import (
                humanize_findings_file,
            )
            caption_stats = await _asyncio.to_thread(
                humanize_findings_file, ctx.output_dir
            )
            if caption_stats and caption_stats.get("ids_replaced"):
                await ctx.log(
                    "Ссылки на блоки в текстах заменены подписями: "
                    f"{caption_stats['ids_replaced']} ID "
                    f"в {caption_stats['findings_changed']} замечаниях",
                )
        except Exception as exc:  # noqa: BLE001 — гуманизация не должна ронять merge
            await ctx.log(f"Гуманизация ссылок на блоки: пропущена ({exc})", "warn")

    try:
        findings_count = len(
            json.loads(findings_path.read_text(encoding="utf-8")).get("findings", [])
        )
    except Exception:
        findings_count = 0

    return FindingsMergeResult(
        success=True,
        excluded_count=excluded_count,
        findings_count=findings_count,
    )
