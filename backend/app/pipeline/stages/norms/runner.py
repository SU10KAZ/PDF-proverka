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
        generate_deterministic_checks,
        format_llm_work_for_template,
        merge_llm_norm_results,
        merge_chunked_llm_results,
        format_findings_to_fix,
        validate_norm_checks,
        write_missing_norms_queue,
        verify_paragraphs_native,
        requote_norms_native,
    )

    ctx.update_pipeline_log("norm_verify", "running")

    if not findings_path.exists():
        error = "Файл 03_findings.json не найден. Сначала выполните основной аудит."
        ctx.update_pipeline_log("norm_verify", "error", error=error)
        return StageResult.fail(error)

    # ── Шаг 1: Извлечение норм ──
    await ctx.log("Шаг 1: Извлечение нормативных ссылок из замечаний...")
    norms_data = extract_norms_from_findings(findings_path)
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
        await ctx.log(
            f"Слияние: {merge_stats['paragraph_checks']} цитат получено, "
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

        exit_code, output, cli_result = await claude_runner.run_norm_fix(
            findings_to_fix_text, pid,
            on_output=ctx.log,
            project_info=project_info,
        )
        ctx.record_cli_usage(cli_result, "norm_fix")

        if is_cancelled(exit_code):
            ctx.update_pipeline_log("norm_verify", "error", error="Отменено при norm_fix")
            return StageResult.cancel()

        if exit_code == 0 and findings_path.exists():
            shutil.copy2(findings_path, verified_path)
            size_kb = round(verified_path.stat().st_size / 1024, 1)
            await ctx.log(f"03a_norms_verified.json создан ({size_kb} KB)")
        elif exit_code != 0:
            await ctx.log(f"Norm fix: код {exit_code}", "warn")
            if pre_norm_path.exists():
                shutil.copy2(pre_norm_path, findings_path)
                await ctx.log("Восстановлен 03_findings.json из бэкапа", "warn")
    else:
        await ctx.log("Все нормы актуальны — пересмотр не требуется", "info")

    # ── Шаг 4: No-op (norms_db.json больше не authoritative) ──
    await ctx.log(
        "norms_db.json: пропуск обновления — authoritative источник Norms-main",
        "info",
    )

    # ── Шаг 5: Обогащение norm_quote из paragraph_checks ──
    enriched = enrich_norm_quotes_from_checks(output_dir)
    if enriched > 0:
        await ctx.log(f"norm_quote обогащён из paragraph_checks: {enriched} замечаний")

    # ── Шаг 6: Авто-исправление неверных номеров пунктов ──
    fixed_paras = fix_paragraph_refs(output_dir)
    if fixed_paras > 0:
        await ctx.log(f"Номера пунктов норм исправлены: {fixed_paras} замечаний")

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

    # ── Финальные операции ──
    await ctx.log("Верификация нормативных ссылок завершена", "info")

    if ctx.refresh_finding_quality:
        ctx.refresh_finding_quality()

    if verified_path.exists():
        from backend.app.pipeline.stages.findings_merge.runner import (
            refresh_finding_quality as _rfq,
        )
        _rfq(pid, "03a_norms_verified.json")

    manual_check_count = count_manual_check_flags(output_dir)
    ctx.update_pipeline_log("norm_verify", "done", message="OK")

    return StageResult.ok(
        checks_count=len(checks),
        manual_check_count=manual_check_count,
        fixed_refs=fixed_paras,
        enriched_quotes=enriched,
    )
