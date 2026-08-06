"""
norms/runner.py
---------------
Stage runner и pure helper functions для этапа norm_verify.

Содержит:
  - Pure helper functions (enrich_norm_quotes_from_checks, fix_paragraph_refs,
    count_manual_check_flags) — перенесены из manager.py в предыдущих pass-ах.
  - run_norm_verification(ctx, *, force, wait_before_fix) — полный norm stage.

Публичный API (helpers):
  enrich_norm_quotes_from_checks(output_dir) -> int
  fix_paragraph_refs(output_dir) -> int
  count_manual_check_flags(output_dir) -> int

Публичный API (runner):
  run_norm_verification(ctx, *, force, wait_before_fix) -> StageResult
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import backend.app.services.llm.claude_runner as claude_runner
from backend.app.core.config import RATE_LIMIT_MAX_RETRIES
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited, is_timeout


def enrich_optimization_norm_status(output_dir: Path) -> int:
    """Проставить предложениям статус нормы по norm_checks.json (чистый Python).

    Без этого шага «✓ норма проверена» не появлялась бы НИКОГДА: поля писал
    только этап пересмотра (3c), а он запускается лишь когда норма плохая. На
    живом прогоне 13АВ-РД-ВК1-К1 V1 все 18 норм оказались действующими →
    пересмотр не потребовался → 24 предложения остались вообще без признака,
    хотя нормы у них проверены и лежат в norm_checks.json.

    Не трогает то, что уже выставил пересмотр (norm_status revised/warning):
    его вердикт содержательнее — он видел текст нормы через MCP.

    Returns: количество обогащённых предложений.
    """
    optimization_path = output_dir / "optimization.json"
    norm_checks_path = output_dir / "norm_checks.json"
    if not optimization_path.exists() or not norm_checks_path.exists():
        return 0

    try:
        od = json.loads(optimization_path.read_text(encoding="utf-8"))
        nc = json.loads(norm_checks_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0

    items = od.get("items") or []
    if not items:
        return 0

    # OPT-ID → его проверки норм
    by_opt: dict[str, list[dict]] = {}
    for check in nc.get("checks") or []:
        for oid in check.get("affected_optimizations") or []:
            by_opt.setdefault(str(oid), []).append(check)

    enriched = 0
    for item in items:
        oid = str(item.get("id") or "")
        checks = by_opt.get(oid) or []
        if not checks:
            # У предложения нет распознанных норм — проверять нечего. Молчим:
            # «не проверено» и «проверено и всё хорошо» — разные вещи.
            continue
        if item.get("norm_status") in ("revised", "warning"):
            continue  # вердикт пересмотра сильнее — не перебиваем

        bad = [c for c in checks if c.get("status") != "active"]
        item["norm_verified"] = True
        if bad:
            reasons = "; ".join(
                f"{c.get('norm_as_cited', '?')}: {c.get('status', '?')}" for c in bad[:3]
            )
            item["norm_status"] = "warning"
            item.setdefault("norm_revision", {})["revision_reason"] = (
                f"Норма не подтверждена в Norms-main — {reasons}"
            )
        else:
            item["norm_status"] = "ok"
        enriched += 1

    if enriched:
        optimization_path.write_text(
            json.dumps(od, ensure_ascii=False, indent=2), encoding="utf-8",
        )
    return enriched


def enrich_norm_quotes_from_checks(output_dir: Path) -> int:
    """Обогатить findings из norm_checks.json (полный norm contract).

    Обогащает:
    - norm_verification: {status, edition_status, verified_via, ...}
    - norm_status / norm_quote_status: classification
    - norm_quote: actual_quote если найдена и лучше текущей

    Returns: количество обогащённых findings.
    """
    findings_path = output_dir / "03_findings.json"
    norm_checks_path = output_dir / "norm_checks.json"
    if not findings_path.exists() or not norm_checks_path.exists():
        return 0

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
        nc = json.loads(norm_checks_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0

    findings = fd.get("findings", [])
    if not findings:
        return 0

    try:
        from norms import enrich_findings_from_norm_checks
        stats = enrich_findings_from_norm_checks(findings, nc)
        enriched = stats.get("enriched_verification", 0) + stats.get("enriched_quote", 0)
    except ImportError:
        # Fallback: старая логика для backward compat
        paragraph_checks = nc.get("paragraph_checks", [])
        verified_quotes = {}
        for pc in paragraph_checks:
            if pc.get("paragraph_verified") and pc.get("actual_quote"):
                fid = pc.get("finding_id", "")
                if fid:
                    verified_quotes[fid] = pc["actual_quote"]

        enriched = 0
        for finding in findings:
            fid = finding.get("id", "")
            if fid in verified_quotes and not finding.get("norm_quote"):
                finding["norm_quote"] = verified_quotes[fid]
                enriched += 1

    if enriched > 0:
        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return enriched


def fix_paragraph_refs(output_dir: Path) -> int:
    """Исправить неверные номера пунктов норм по данным paragraph_checks.

    Для каждого finding с paragraph_verified=False: извлекаем правильный
    пункт из mismatch_details (regex) и обновляем поле norm. Если правильный
    пункт не определить однозначно — добавляем пометку [ручная сверка].

    Returns: количество исправленных findings.
    """
    import re as _re
    import shutil as _shutil

    findings_path = output_dir / "03_findings.json"
    norm_checks_path = output_dir / "norm_checks.json"
    if not findings_path.exists() or not norm_checks_path.exists():
        return 0

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
        nc = json.loads(norm_checks_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0

    para_checks = nc.get("paragraph_checks", [])
    if not para_checks:
        return 0

    _p_re = _re.compile(r"п\.\s*([\d]+(?:\.[\d]+)+)")

    # Группируем по finding_id — только unverified
    by_fid: dict[str, list[dict]] = {}
    for pc in para_checks:
        if not pc.get("paragraph_verified", True):
            by_fid.setdefault(pc.get("finding_id", ""), []).append(pc)
    if not by_fid:
        return 0

    findings = fd.get("findings", [])
    fmap = {f.get("id", ""): f for f in findings}
    fixed = 0

    for fid, checks in by_fid.items():
        finding = fmap.get(fid)
        if not finding:
            continue

        norm_field = finding.get("norm", "") or ""
        desc = finding.get("description", "") or ""
        made_change = False

        for pc in checks:
            norm_str = pc.get("norm") or ""
            mismatch = pc.get("mismatch_details") or ""
            old_paras = _p_re.findall(norm_str)
            if not old_paras:
                continue
            old_p = old_paras[0]

            # Ищем правильный пункт в mismatch_details (исключая старый)
            all_in_mismatch = _p_re.findall(mismatch)
            new_candidates = [p for p in all_in_mismatch if p != old_p]

            if new_candidates:
                new_p = new_candidates[0]
                new_norm = _re.sub(r"п\.\s*" + _re.escape(old_p), f"п. {new_p}", norm_field)
                new_desc = _re.sub(r"п\.\s*" + _re.escape(old_p), f"п. {new_p}", desc)
                if new_norm != norm_field or new_desc != desc:
                    norm_field = new_norm
                    desc = new_desc
                    made_change = True
            else:
                # Не определить пункт → ставим пометку если её нет
                flag = f"[Пункт нормы {norm_str} требует ручной сверки] "
                if flag not in desc:
                    desc = flag + desc
                    made_change = True

        if made_change:
            finding["norm"] = norm_field
            finding["description"] = desc
            fixed += 1

    if fixed > 0:
        if "meta" in fd and isinstance(fd["meta"], dict):
            fd["meta"]["paragraph_fix_applied"] = True
            fd["meta"]["paragraph_fix_stats"] = {"fixed_paragraph": fixed}
        findings_path.write_text(
            json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return fixed


def _optimization_intact(optimization_path: Path, backup_path: Path) -> bool:
    """True, если пересмотр не потерял предложения (инвариант «ничего не удаляем»).

    Агентный корректор оптимизаций уже ловили на тихой потере данных (замер 07-07:
    ЭО1 14 → 7, удалено 41 предложение). Здесь та же защита на входе: файл обязан
    остаться валидным JSON, а КАЖДЫЙ исходный id — на месте. Иначе откат к бэкапу.
    """
    try:
        new = json.loads(optimization_path.read_text(encoding="utf-8"))
        old = json.loads(backup_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False

    old_ids = {i.get("id") for i in old.get("items") or [] if i.get("id")}
    new_ids = {i.get("id") for i in new.get("items") or [] if i.get("id")}
    return old_ids.issubset(new_ids)


def _norm_fix_left_findings_untouched(findings_path: Path, backup_path: Path) -> bool:
    """True, если norm_fix завершился, НЕ изменив 03_findings.json (байт-в-байт = бэкап).

    Детерминированная замена текстовой эвристики по словам-маркерам («невозможно»,
    «недоступны»…): те же слова встречаются в резюме УСПЕШНОГО прогона («часть цитат
    недоступны») и приводили к откату реально применённых правок из бэкапа.
    Файл не изменился → агент ничего не применил → фолбэк на deterministic enrichment
    (откат при этом не нужен и не выполняется — содержимое и так равно бэкапу).
    """
    try:
        if not findings_path.exists() or not backup_path.exists():
            return False
        return findings_path.read_bytes() == backup_path.read_bytes()
    except OSError:
        return False


def count_manual_check_flags(output_dir: Path) -> int:
    """Подсчитать количество findings с флагом [Пункт нормы ... ручной сверки]."""
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return 0
    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
        return sum(
            1 for f in fd.get("findings", [])
            if "[Пункт нормы" in (f.get("description") or "")
        )
    except (json.JSONDecodeError, OSError):
        return 0


# ─── run_norm_verification ───────────────────────────────────────────────────

def other_projects_on_norm_stage(exclude_project_id: str) -> int:
    """Сколько ДРУГИХ проектов сейчас находятся на норм-этапе.

    Считаем по живому состоянию очереди, а не отдельным счётчиком: у
    run_norm_verification много ранних return'ов, и счётчик, увеличенный на
    входе, при первом же early-return протёк бы навсегда — модели перестали
    бы выгружаться совсем, а это ровно профиль OOM-инцидента 01.07.
    """
    try:
        from backend.app.models.audit import AuditStage, JobStatus
        from backend.app.pipeline.manager import pipeline_manager
    except Exception:
        return 0
    norm_stages = {AuditStage.NORM_VERIFY, AuditStage.NORM_FIX}
    count = 0
    for pid, job in list(pipeline_manager.active_jobs.items()):
        if pid in ("__BATCH__", exclude_project_id):
            continue
        if job.status == JobStatus.RUNNING and job.stage in norm_stages:
            count += 1
    return count


async def run_norm_verification(
    ctx: PipelineStageContext,
    *,
    force: bool = False,
    wait_before_fix: Optional[asyncio.Event] = None,
) -> StageResult:
    """Верификация нормативных ссылок (authoritative режим через Norms-main).

    Шаги:
    1. Извлечь нормы из 03_findings.json (Python)
    2. Резолв статусов через Norms-main status_index.json (Python)
    3. Записать missing_norms_queue
    4. LLM через MCP ТОЛЬКО для верификации цитат пунктов
    5. Если есть устаревшие — пересмотреть замечания через Claude CLI
       (ждёт wait_before_fix, т.к. corrector тоже пишет в 03_findings.json)
    6. Обогатить norm_quote из paragraph_checks
    7. Исправить неверные номера пунктов
    8. Уточнить оставшиеся цитаты (native semantic search)

    Не управляет:
    - job.stage / job.status (выставляет оркестратор);
    - heartbeat / cleanup (оркестратор);
    - параллельным запуском с findings_review (_run_post_findings_parallel).
    """
    pid = ctx.project_id
    output_dir = ctx.output_dir
    project_info = ctx.project_info or {}

    findings_path = output_dir / "03_findings.json"
    norm_checks_path = output_dir / "norm_checks.json"
    norm_checks_llm_path = output_dir / "norm_checks_llm.json"
    verified_path = output_dir / "03a_norms_verified.json"

    from norms import (
        extract_norms_from_findings,
        extract_norms_from_optimization,
        merge_norms_maps,
        generate_deterministic_checks,
        format_optimizations_to_fix,
        format_llm_work_for_template,
        merge_llm_norm_results,
        merge_chunked_llm_results,
        format_findings_to_fix,
        validate_norm_checks,
        write_missing_norms_queue,
        verify_paragraphs_native,
        requote_norms_native,
        backfill_missing_quotes_native,
    )

    ctx.update_pipeline_log("norm_verify", "running")

    if not findings_path.exists():
        error = "Файл 03_findings.json не найден. Сначала выполните основной аудит."
        ctx.update_pipeline_log("norm_verify", "error", error=error)
        return StageResult.fail(error)

    # ── Шаг 1: Извлечение норм ──
    await ctx.log("Шаг 1: Извлечение нормативных ссылок из замечаний...")
    norms_data = extract_norms_from_findings(findings_path)

    # Нормы ПРЕДЛОЖЕНИЙ по оптимизации — в тот же контур. Промпт оптимизации
    # требует поле `norm` и соответствие ДЕЙСТВУЮЩИМ нормам, но справочника не
    # даёт, а этап 04 читал только findings → ссылки предложений не проверял
    # никто (отсюда и самодеятельный web-поиск внутри стадии оптимизации).
    # optimization.json появляется здесь только при PIPELINE_NORMS_AFTER_MERGE_ENABLED
    # (нормы после параллельного блока). В легаси-порядке файла ещё нет — тогда
    # ведём себя ровно как раньше, без ошибки.
    optimization_path = output_dir / "optimization.json"
    if optimization_path.exists():
        try:
            opt_norms = extract_norms_from_optimization(optimization_path)
            if opt_norms["total_unique_norms"]:
                before = norms_data["total_unique_norms"]
                norms_data = merge_norms_maps(norms_data, opt_norms)
                added = norms_data["total_unique_norms"] - before
                await ctx.log(
                    f"Нормы предложений по оптимизации: {opt_norms['total_unique_norms']} "
                    f"из {opt_norms['total_optimizations']} предложений, "
                    f"новых для проверки: {added}",
                )
        except (OSError, json.JSONDecodeError, KeyError) as e:
            # fail-soft: битый optimization.json не должен ронять верификацию норм
            await ctx.log(f"Нормы предложений пропущены: {e}", "warn")

    total_norms = norms_data["total_unique_norms"]

    if total_norms == 0:
        await ctx.log("Нормативных ссылок не найдено. Верификация не требуется.", "warn")
        ctx.update_pipeline_log("norm_verify", "done", message="no norms found")
        return StageResult.ok(checks_count=0, manual_check_count=0)

    await ctx.log(f"Найдено {total_norms} уникальных нормативных ссылок")

    # ── Шаг 2: Детерминированный резолв через Norms-main ──
    await ctx.log(
        "Шаг 2: Authoritative резолв статусов через Norms-main (status_index.json)...",
    )
    det_result = generate_deterministic_checks(norms_data, project_id=pid)

    det_meta = det_result["meta"]
    paragraphs_to_verify = det_result["paragraphs_to_verify"]

    await ctx.log(
        f"Norms-main: {det_meta['authoritative']} authoritative, "
        f"{det_meta['missing']} missing, {det_meta['unsupported']} unsupported; "
        f"{len(paragraphs_to_verify)} цитат для проверки через MCP",
    )
    trusted_skipped = det_meta.get("paragraphs_trusted_skipped", 0)
    legacy_ignored = det_meta.get("paragraphs_legacy_ignored", 0)
    if trusted_skipped or legacy_ignored:
        await ctx.log(
            f"Paragraph cache: {trusted_skipped} trusted (skip LLM), "
            f"{legacy_ignored} legacy (не доверяем, пере-проверка через MCP)",
            "info",
        )

    # Записать missing_norms_queue
    try:
        report = write_missing_norms_queue(output_dir, det_result, project_id=pid)
        if report.get("queue_size", 0) > 0:
            await ctx.log(
                f"Missing norms queue: {report['queue_size']} позиций "
                f"(missing={report['missing']}, unsupported={report['unsupported']}). "
                f"См. {output_dir}/missing_norms_queue.json",
                "warn",
            )
    except Exception as e:
        await ctx.log(f"Не удалось записать missing_norms_queue: {e}", "warn")

    # Накопить missing norms в глобальный vault-список
    try:
        from backend.app.services.knowledge_base.missing_norms_service import accumulate_from_queue
        queue_path = output_dir / "missing_norms_queue.json"
        new_norms = accumulate_from_queue(pid, queue_path)
        if new_norms > 0:
            await ctx.log(f"Добавлено {new_norms} новых норм в список 'Нормы для добавления'")
    except Exception as e:
        await ctx.log(f"Не удалось обновить missing_norms_vault: {e}", "warn")

    # Записать предварительный norm_checks.json (детерминированный)
    preliminary_data = {
        "meta": det_meta,
        "checks": det_result["checks"],
        "paragraph_checks": [],
    }
    with open(norm_checks_path, "w", encoding="utf-8") as f:
        json.dump(preliminary_data, f, ensure_ascii=False, indent=2)

    # ── Шаг 3: Верификация цитат — сначала Python, fallback на Claude ──
    llm_needed = bool(paragraphs_to_verify)

    if llm_needed:
        llm_task_count = len(paragraphs_to_verify)
        await ctx.log(
            f"Шаг 3: Верификация цитат через MCP norms для "
            f"{llm_task_count} позиций. WebSearch запрещён.",
        )

        # ── Native Python (fast path) ──
        _native_ok = False
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                verify_paragraphs_native,
                paragraphs_to_verify,
                findings_path,
                output_dir,
            )
            await ctx.log(f"Native verification: {llm_task_count} цитат проверено (Python)")
            _native_ok = True
        except Exception as _native_exc:
            await ctx.log(
                f"Native verification failed ({_native_exc}), fallback → Claude chunks",
                "warn",
            )

        if not _native_ok:
            can_go = await ctx.check_before_launch()
            if not can_go:
                ctx.update_pipeline_log("norm_verify", "error",
                                        error="Rate limit: ожидание превышено")
                return StageResult.fail("Rate limit: ожидание превышено или отменено")

            PARA_CHUNK_SIZE = 15
            use_chunked = len(paragraphs_to_verify) > PARA_CHUNK_SIZE
        else:
            use_chunked = False

        if use_chunked:
            para_chunks = [
                paragraphs_to_verify[i:i + PARA_CHUNK_SIZE]
                for i in range(0, len(paragraphs_to_verify), PARA_CHUNK_SIZE)
            ]
            await ctx.log(
                f"Chunked mode: {len(para_chunks)} чанков "
                f"({len(paragraphs_to_verify)} цитат)",
            )

            sem = asyncio.Semaphore(1)

            async def _run_chunk(idx: int, chunk_paragraphs: list):
                async with sem:
                    fname = f"norm_checks_llm_{idx + 1}.json"
                    chunk_text = format_llm_work_for_template(chunk_paragraphs, findings_path)
                    expected = output_dir / fname
                    if expected.exists():
                        expected.unlink()
                    for attempt in (1, 2):
                        exit_code, output, cli_result = await claude_runner.run_norm_verify(
                            chunk_text, pid,
                            on_output=ctx.log,
                            project_info=project_info,
                            llm_out_filename=fname,
                            output_dir=output_dir,
                            version_dir=ctx.project_dir,
                            version_id=ctx.version_id,
                        )
                        ctx.record_cli_usage(
                            cli_result,
                            f"norm_verify_chunk_{idx + 1}"
                            + ("" if attempt == 1 else f"_retry_{attempt}"),
                        )
                        if exit_code != 0:
                            raise RuntimeError(
                                f"Claude CLI norm_verify chunk {idx + 1}: exit {exit_code}"
                            )
                        if expected.exists():
                            return expected
                        await ctx.log(
                            f"chunk {idx + 1}: exit=0 но {fname} не создан — "
                            f"{'retry' if attempt == 1 else 'fail'}",
                            "warn",
                        )
                    raise RuntimeError(
                        f"Claude CLI norm_verify chunk {idx + 1}: "
                        f"exit=0 дважды, но {expected} не создан"
                    )

            tasks = [_run_chunk(ci, chunk) for ci, chunk in enumerate(para_chunks)]
            chunk_paths = await asyncio.gather(*tasks, return_exceptions=True)
            valid_paths = [p for p in chunk_paths if isinstance(p, Path)]
            errors = [e for e in chunk_paths if isinstance(e, Exception)]
            if errors:
                await ctx.log(f"Chunked mode: {len(errors)} чанков с ошибками", "warn")
            if not valid_paths:
                error = (
                    "Chunked norm_verify: ни один чанк не дал valid файл "
                    "— paragraph verification не выполнена"
                )
                ctx.update_pipeline_log("norm_verify", "error", error=error)
                return StageResult.fail(error)
            merge_chunked_llm_results(valid_paths, norm_checks_llm_path)
            await ctx.log(f"Chunked merge: {len(valid_paths)} чанков объединены")

        elif not _native_ok:
            llm_work_text = format_llm_work_for_template(paragraphs_to_verify, findings_path)
            max_retries = RATE_LIMIT_MAX_RETRIES
            if norm_checks_llm_path.exists():
                norm_checks_llm_path.unlink()

            for attempt in range(1, max_retries + 1):
                exit_code, output, cli_result = await claude_runner.run_norm_verify(
                    llm_work_text, pid,
                    on_output=ctx.log,
                    project_info=project_info,
                    output_dir=output_dir,
                    version_dir=ctx.project_dir,
                    version_id=ctx.version_id,
                )
                stage_label = "norm_verify" if attempt == 1 else f"norm_verify_retry_{attempt}"
                ctx.record_cli_usage(cli_result, stage_label)

                if is_cancelled(exit_code):
                    ctx.update_pipeline_log("norm_verify", "error", error="Отменено")
                    return StageResult.cancel()

                if exit_code == 0:
                    break

                if is_rate_limited(exit_code, output or "", "") or is_timeout(exit_code):
                    reason = "таймаут" if is_timeout(exit_code) else "rate limit"
                    await ctx.log(
                        f"{reason} при верификации норм (попытка {attempt}/{max_retries}), "
                        f"ожидание...", "warn",
                    )
                    if attempt < max_retries:
                        can_continue = await ctx.wait_for_rate_limit(
                            f"{reason} при верификации норм", output or ""
                        )
                        if not can_continue:
                            error = f"Верификация норм: ожидание {reason} превышено или отменено"
                            ctx.update_pipeline_log("norm_verify", "error", error=error)
                            return StageResult.fail(error)
                        continue
                    else:
                        error = f"Верификация норм: {max_retries} попыток исчерпано ({reason})"
                        ctx.update_pipeline_log("norm_verify", "error", error=error)
                        return StageResult.fail(error)

                await ctx.log(f"Ошибка верификации (код {exit_code})", "error")
                error = f"Claude CLI norm_verify: exit code {exit_code}"
                ctx.update_pipeline_log("norm_verify", "error", error=error)
                return StageResult.fail(error)

            # Post-check: exit=0 НЕ считается успехом, если файла нет.
            if not norm_checks_llm_path.exists():
                await ctx.log(
                    f"norm_verify: exit=0, но {norm_checks_llm_path.name} "
                    f"не создан. Запускаю контролируемый retry...",
                    "warn",
                )
                exit_code, output, cli_result = await claude_runner.run_norm_verify(
                    llm_work_text, pid,
                    on_output=ctx.log,
                    project_info=project_info,
                    output_dir=output_dir,
                    version_dir=ctx.project_dir,
                    version_id=ctx.version_id,
                )
                ctx.record_cli_usage(cli_result, "norm_verify_missing_file_retry")
                if is_cancelled(exit_code):
                    ctx.update_pipeline_log("norm_verify", "error", error="Отменено")
                    return StageResult.cancel()
                if exit_code != 0 or not norm_checks_llm_path.exists():
                    error = (
                        f"norm_verify: paragraph verification не выполнена — "
                        f"{norm_checks_llm_path.name} не создан (retry exit={exit_code})"
                    )
                    ctx.update_pipeline_log("norm_verify", "error", error=error)
                    return StageResult.fail(error)
                await ctx.log("norm_verify retry: файл успешно создан", "info")

        # ── Шаг 3b: Слияние paragraph_checks ──
        if not norm_checks_llm_path.exists():
            error = (
                f"norm_verify invariant: {norm_checks_llm_path} "
                f"должен был существовать на этом шаге"
            )
            ctx.update_pipeline_log("norm_verify", "error", error=error)
            return StageResult.fail(error)

        await ctx.log(
            "Слияние paragraph_checks (статусы norm_checks остаются authoritative)...",
        )
        merge_stats = merge_llm_norm_results(norm_checks_path, norm_checks_llm_path)
        # #37: явно логируем долю подтверждённых цитат.
        _pv_true = merge_stats.get("paragraph_verified_true", 0)
        _pv_total = merge_stats.get("paragraph_verified_total", merge_stats["paragraph_checks"])
        await ctx.log(
            f"Слияние: {merge_stats['paragraph_checks']} цитат получено, "
            f"подтверждено {_pv_true}/{_pv_total}, "
            f"{merge_stats.get('ignored_llm_status_attempts', 0)} попыток "
            f"изменить статус отброшено. Paragraph cache: "
            f"+{merge_stats.get('paragraph_cache_added', 0)} новых, "
            f"{merge_stats.get('paragraph_cache_updated', 0)} обновлено.",
        )
    else:
        await ctx.log(
            "Нет цитат для верификации через MCP — ограничиваемся authoritative статусами",
            "info",
        )

    # Проверяем что файл существует
    if not norm_checks_path.exists():
        await ctx.log("norm_checks.json не создан", "warn")
        ctx.update_pipeline_log("norm_verify", "done", message="no norm_checks file")
        return StageResult.ok(checks_count=0, manual_check_count=0)

    # Читаем результаты
    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    # ── Пост-валидация ──
    validation = validate_norm_checks(norm_checks_path)
    if validation.get("fixes_applied"):
        await ctx.log(
            f"Пост-валидация: {len(validation['fixes_applied'])} исправлений: "
            + "; ".join(validation["fixes_applied"][:3]),
            "warn",
        )
        with open(norm_checks_path, "r", encoding="utf-8") as f:
            checks_data = json.load(f)
    if validation.get("violations"):
        await ctx.log(
            f"Пост-валидация: {len(validation['violations'])} нарушений: "
            + "; ".join(validation["violations"][:3]),
            "warn",
        )

    checks = checks_data.get("checks", [])
    needs_fix = [c for c in checks if c.get("needs_revision", False)]

    results = checks_data.get("meta", {}).get("results", {})
    await ctx.log(
        f"Результат: {results.get('active', 0)} актуальных, "
        f"{results.get('outdated_edition', 0)} устаревших, "
        f"{results.get('replaced', 0)} заменённых, "
        f"{results.get('cancelled', 0)} отменённых",
        "info",
    )

    # ── Шаг 3: Пересмотр замечаний (если нужен) ──
    norm_fix_failed = False
    if needs_fix:
        if wait_before_fix is not None and not wait_before_fix.is_set():
            await ctx.log("Ожидание завершения Corrector перед пересмотром норм...")
            await wait_before_fix.wait()

        await ctx.log(
            f"Шаг 3: Пересмотр {len(needs_fix)} замечаний с устаревшими нормами..."
        )

        findings_to_fix_text = format_findings_to_fix(norm_checks_path, findings_path)

        import shutil
        pre_norm_path = output_dir / "03_findings_pre_norm.json"
        if findings_path.exists():
            shutil.copy2(findings_path, pre_norm_path)

        can_go = await ctx.check_before_launch()
        if not can_go:
            error = "Rate limit: ожидание превышено или отменено"
            ctx.update_pipeline_log("norm_verify", "error", error=error)
            return StageResult.fail(error)

        try:
            exit_code, output, cli_result = await claude_runner.run_norm_fix(
                findings_to_fix_text, pid,
                on_output=ctx.log,
                project_info=project_info,
                output_dir=output_dir,
                version_dir=ctx.project_dir,
                version_id=ctx.version_id,
            )
            ctx.record_cli_usage(cli_result, "norm_fix")
        except Exception as exc:
            exit_code = 1
            output = str(exc)
            norm_fix_failed = True
            await ctx.log(f"Norm fix: исключение ({exc}); продолжаю с исходными findings", "warn")

        if exit_code == 0 and _norm_fix_left_findings_untouched(findings_path, pre_norm_path):
            # Файл не изменился → правки не применены. НЕ форсируем exit_code=1:
            # откат из бэкапа не нужен (содержимое идентично), а реальный успех
            # с изменённым файлом сюда не попадает по построению.
            norm_fix_failed = True
            await ctx.log(
                "Norm fix: агент завершился без изменений 03_findings.json; "
                "продолжаю с deterministic norm enrichment",
                "warn",
            )

        if is_cancelled(exit_code):
            ctx.update_pipeline_log("norm_verify", "error", error="Отменено при norm_fix")
            return StageResult.cancel()

        if exit_code != 0:
            norm_fix_failed = True
            await ctx.log(f"Norm fix: код {exit_code}; {output[:300] if output else ''}", "warn")
            if pre_norm_path.exists():
                shutil.copy2(pre_norm_path, findings_path)
                await ctx.log("Восстановлен 03_findings.json из бэкапа", "warn")
    else:
        await ctx.log("Все нормы актуальны — пересмотр не требуется", "info")

    # ── Шаг 3c: Пересмотр ОПТИМИЗАЦИЙ с изменившимися нормами ──
    # Предложение возвращается автору вместе с вердиктом по норме: замена могла
    # потерять смысл (obsolete) или измениться по сути (revised), а не только
    # сослаться не туда. Удалять запрещено — решает эксперт.
    if optimization_path.exists():
        opts_to_fix_text = format_optimizations_to_fix(norm_checks_path, optimization_path)
        if "Пересмотр не требуется" in opts_to_fix_text:
            await ctx.log("Нормы в оптимизациях актуальны — пересмотр не требуется", "info")
        else:
            n_opts = opts_to_fix_text.count("\n### ") + opts_to_fix_text.startswith("### ")
            await ctx.log(
                f"Шаг 3c: Пересмотр {n_opts} предложений с изменившимися нормами...",
            )
            import shutil as _shutil
            pre_opt_norm_path = output_dir / "optimization_pre_norm.json"
            _shutil.copy2(optimization_path, pre_opt_norm_path)
            try:
                exit_code, output, cli_result = await claude_runner.run_optimization_norm_fix(
                    opts_to_fix_text, pid,
                    on_output=ctx.log,
                    project_info=project_info,
                    output_dir=output_dir,
                    version_dir=ctx.project_dir,
                    version_id=ctx.version_id,
                )
                ctx.record_cli_usage(cli_result, "optimization_norm_fix")
            except Exception as exc:
                exit_code, output = 1, str(exc)
                await ctx.log(f"Пересмотр оптимизаций: исключение ({exc})", "warn")

            # fail-soft: этап 04 существует ради findings. Провал пересмотра
            # предложений не должен ронять верификацию норм — откатываем файл
            # и идём дальше, оставив след в логе.
            if exit_code != 0 or not _optimization_intact(optimization_path, pre_opt_norm_path):
                _shutil.copy2(pre_opt_norm_path, optimization_path)
                await ctx.log(
                    f"Пересмотр оптимизаций не применён (код {exit_code}) — "
                    f"optimization.json восстановлен из бэкапа",
                    "warn",
                )
            else:
                await ctx.log("Оптимизации пересмотрены с учётом актуальных норм", "info")

    # ── Шаг 4: No-op (norms_db.json больше не authoritative) ──
    await ctx.log(
        "norms_db.json: пропуск обновления — authoritative источник Norms-main",
        "info",
    )

    # ── Шаг 5: Обогащение norm_quote из paragraph_checks ──
    enriched = enrich_norm_quotes_from_checks(output_dir)
    if enriched > 0:
        await ctx.log(f"norm_quote обогащён из paragraph_checks: {enriched} замечаний")

    # ── Шаг 5b: Статус нормы предложениям (детерминированно, без LLM) ──
    # Иначе «✓ норма проверена» не появится никогда: поля пишет только шаг 3c,
    # а он идёт лишь при плохой норме. Штатный случай (все нормы действуют)
    # оставался бы в UI немым, хотя проверка прошла.
    opt_enriched = enrich_optimization_norm_status(output_dir)
    if opt_enriched > 0:
        await ctx.log(f"Статус нормы проставлен предложениям: {opt_enriched}")

    # ── Шаг 6: Авто-исправление неверных номеров пунктов ──
    fixed_paras = fix_paragraph_refs(output_dir)
    if fixed_paras > 0:
        await ctx.log(f"Номера пунктов норм исправлены: {fixed_paras} замечаний")

    if findings_path.exists() and (needs_fix or enriched > 0 or fixed_paras > 0):
        import shutil
        shutil.copy2(findings_path, verified_path)
        size_kb = round(verified_path.stat().st_size / 1024, 1)
        if norm_fix_failed:
            await ctx.log(
                f"03a_norms_verified.json создан из текущих findings ({size_kb} KB); "
                "LLM-пересмотр норм не применён",
                "warn",
            )
        elif needs_fix:
            await ctx.log(f"03a_norms_verified.json обновлён ({size_kb} KB)")

    # ── Шаг 6.5: Дозаливка ОТСУТСТВУЮЩИХ цитат по номеру пункта ──
    # Шаг 7 ниже строит поисковый запрос из уже имеющейся цитаты и при пустой
    # norm_quote молча пропускает замечание. А пусто оно чаще всего: этапы 01/02
    # дают цитату лишь в 2-22% находок. Здесь текст пункта берётся точным
    # обращением к индексу норм — 0 токенов, без сети. Fail-soft.
    try:
        quotes_report = await asyncio.get_event_loop().run_in_executor(
            None, backfill_missing_quotes_native, output_dir
        )
        if quotes_report.get("filled"):
            await ctx.log(
                f"Цитаты норм восстановлены из индекса: {quotes_report['filled']} "
                f"из {quotes_report['candidates']} замечаний без цитаты"
            )
        if quotes_report.get("no_paragraph"):
            await ctx.log(
                f"Ссылок на норму без номера пункта: {quotes_report['no_paragraph']} — "
                "процитировать нечего, требуется уточнение пункта",
                "warn",
            )
        states = quotes_report.get("states") or {}
        if states:
            await ctx.log(
                "Ссылки на нормы по индексу: "
                + ", ".join(f"{k}={v}" for k, v in sorted(states.items()))
            )
        # Пункт назван, но в самом документе не найден — вероятнее всего номер
        # придуман. Раньше это тонуло: проверка сверяла цитату с текстом пункта,
        # а цитаты почти всегда не было (2 подтверждения на 4645 проверок).
        if states.get("paragraph_not_found"):
            await ctx.log(
                f"Пунктов, не найденных в своём документе: {states['paragraph_not_found']} — "
                "номер пункта под сомнением, нужна ручная сверка",
                "warn",
            )
    except Exception as _bq_exc:  # noqa: BLE001 — дозаливка не должна ронять нормы
        await ctx.log(f"Дозаливка цитат пропущена ({_bq_exc})", "warn")

    # ── Шаг 7: Уточнение оставшихся цитат (Python semantic search) ──
    remaining_flags = count_manual_check_flags(output_dir)
    if remaining_flags > 0:
        await ctx.log(
            f"Шаг 7: уточнение {remaining_flags} цитат норм (Python semantic search)"
        )
        try:
            loop = asyncio.get_event_loop()
            rq_result = await loop.run_in_executor(None, requote_norms_native, output_dir)
            resolved = rq_result.get("resolved", 0)
            remaining_after = rq_result.get("remaining", remaining_flags)
            await ctx.log(
                f"norm_requote завершён: исправлено {resolved}/{remaining_flags}, "
                f"осталось {remaining_after} [ручная сверка]"
            )
        except Exception as _rq_exc:
            await ctx.log(
                f"Native requote failed ({_rq_exc}), fallback → Claude CLI", "warn"
            )
            exit_code, _, cli_result = await claude_runner.run_norm_requote(
                pid, on_output=ctx.log, project_info=project_info,
                output_dir=output_dir, version_dir=ctx.project_dir,
                version_id=ctx.version_id,
            )
            ctx.record_cli_usage(cli_result, "norm_requote")
            if exit_code != 0:
                await ctx.log(f"norm_requote: код {exit_code} (не критично)", "warn")
            remaining_after = count_manual_check_flags(output_dir)
            resolved = remaining_flags - remaining_after
            await ctx.log(
                f"norm_requote завершён: исправлено {resolved}/{remaining_flags}, "
                f"осталось {remaining_after} [ручная сверка]"
            )

    # ── Шаг 7b: Синхронизация 03a_norms_verified.json с итоговыми findings ──
    # 03a создаётся на Шаге 6, то есть ДО дозаливки цитат (6.5) и уточнения
    # пунктов (7). А UI и Excel читают именно 03a — выше 03_findings.json. Без
    # этой синхронизации восстановленные цитаты и состояния ссылок доезжали до
    # файла, но не до глаз инженера.
    if findings_path.exists():
        import shutil as _shutil_sync
        try:
            _shutil_sync.copy2(findings_path, verified_path)
            await ctx.log(
                "03a_norms_verified.json синхронизирован с итоговыми findings", "info"
            )
        except OSError as _sync_exc:  # noqa: BLE001 — синхронизация не критична
            await ctx.log(
                f"03a_norms_verified.json не синхронизирован ({_sync_exc})", "warn"
            )

    # ── Финальные операции ──
    # Выгрузить семантические модели норм из процесса: native verify/requote
    # затянули в backend e5-large+reranker (~4.3 ГБ RSS) через module-cache
    # search.py — без release они жили в uvicorn навсегда (профиль OOM 01.07).
    # Замер: 4063 МБ → 477 МБ после release+malloc_trim.
    #
    # Выгружает ТОЛЬКО последний уходящий: кэш моделей в search.py глобальный
    # на процесс, и при параллельных проектах release одного проекта обнулял
    # бы модели, которыми прямо сейчас пользуются остальные — те получали бы
    # повторную загрузку 4,3 ГБ и десятки секунд простоя на каждом чужом
    # завершении. Счётчик активных норм-этапов ведёт norm_models_scope().
    _others_on_norms = other_projects_on_norm_stage(pid)
    if _others_on_norms == 0:
        try:
            from search import release_models as _release_norm_models  # norms/tools в sys.path
            _rss_after = await asyncio.to_thread(_release_norm_models)
            if _rss_after:
                await ctx.log(
                    f"Семантические модели норм выгружены (RSS процесса ≈{_rss_after} МБ)",
                    "info",
                )
        except Exception:
            pass  # release — оптимизация памяти, не должна ронять стадию
    else:
        await ctx.log(
            f"Норм-этап идёт ещё у {_others_on_norms} проект(ов) — "
            f"семантические модели оставлены загруженными",
            "info",
        )

    await ctx.log("Верификация нормативных ссылок завершена", "info")

    if ctx.refresh_finding_quality:
        ctx.refresh_finding_quality()

    if verified_path.exists():
        from backend.app.pipeline.stages.block_analysis.provenance import (
            backfill_final_findings_provenance,
        )
        backfill_final_findings_provenance(
            output_dir,
            findings_filename="03a_norms_verified.json",
        )
        from backend.app.pipeline.stages.findings_merge.runner import (
            refresh_finding_quality as _rfq,
        )
        # output_dir обязателен (класс бага B2): без него резолв по pid уходит
        # в v2 latest — обогащалась чужая копия, затираемая promote'ом.
        _rfq(pid, "03a_norms_verified.json", output_dir=output_dir)

    manual_check_count = count_manual_check_flags(output_dir)
    ctx.update_pipeline_log("norm_verify", "done", message="OK")

    return StageResult.ok(
        checks_count=len(checks),
        manual_check_count=manual_check_count,
        fixed_refs=fixed_paras,
        enriched_quotes=enriched,
    )
