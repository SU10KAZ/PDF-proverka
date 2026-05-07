# PipelineStageContext — план поэтапного выноса _run_* методов

**Дата:** 2026-05-06
**Статус:** черновик после safe-refactor pass 2

## 1. Оставшиеся _run_* методы в manager.py

| Метод | Строки | Этап |
|-------|--------|------|
| `_run_script` | 847 | инфраструктура (subprocess) |
| `_run_gemma_enrichment_stage` | 942 | gemma_enrichment |
| `_run_block_analysis_findings_only` | 1235 | block_analysis |
| `_run_resumed_pipeline` | 1994 | оркестратор |
| `_run_prepare` | 2652 | prepare |
| `_run_tile_audit` | 2697 | legacy tile audit |
| `_run_main_audit` | 2992 | legacy main audit |
| `_run_findings_review` | 3145 | findings_review |
| `_run_post_findings_parallel` | 3652 | norms + optimization (parallel) |
| `_run_norm_verification` | 3790 | norms |
| `_run_smart_pipeline` | 4335 | оркестратор (smart) |
| `_run_block_retry` | 4674 | block_analysis retry |
| `_run_ocr_pipeline` | 4817 | оркестратор (OCR full pipeline) |
| `_run_precrop_loop` | 5484 | batch precrop |
| `_run_batch_queue` | 5513 | batch оркестратор |
| `_run_optimization` | 6191 | optimization |
| `_run_optimization_review` | 6297 | optimization critic/corrector |
| `_run_optimization_review_standalone` | 6168 | optimization (standalone) |
| `_run_optimization_with_review` | 6185 | optimization + review |

## 2. Зависимости self, используемые в _run_* методах

| Зависимость | Описание | Нужно в StageContext |
|-------------|----------|----------------------|
| `self._log(job, msg, level)` | Async WS + stdout лог | `log: Callable[[str, str], Awaitable]` |
| `self._check_pause(job)` | Ожидание снятия паузы | `check_pause: Callable[[], Awaitable[bool]]` |
| `self._check_before_launch(job)` | Rate limit check + pause | `check_before_launch: Callable[[], Awaitable[bool]]` |
| `self._wait_for_rate_limit(...)` | Ожидание сброса rate limit | `wait_for_rate_limit: Callable[..., Awaitable[bool]]` |
| `self._run_script(pid, ...)` | Запуск subprocess | `run_subprocess: Callable[..., Awaitable]` |
| `self._record_cli_usage(job, ...)` | Usage tracking | `record_cli_usage: Callable[..., None]` |
| `self._update_pipeline_log(pid, stage, status)` | pipeline_log.json | `update_pipeline_log: Callable[..., None]` |
| `self._start_heartbeat(job)` / `_stop_heartbeat` | Job heartbeat | не переносить, оставить в manager |
| `self._progress(job, current, total)` | WS прогресс | `progress: Callable[[int, int], Awaitable]` |
| `self._emit_cli_summary(job, payload)` | WS summary | `emit_summary: Callable[[dict], Awaitable]` |
| `self._stream_findings_events(job, stage)` | WS findings stream | `stream_findings: Callable[[str], Awaitable]` |
| `self._enrich_pipeline_log(...)` | Обогащение pipeline_log | отдельно через колбэк |
| `self._cleanup(pid)` | Очистка active_jobs | не переносить, оставить в manager |
| `self.active_jobs` | Реестр запущенных job | не переносить |
| `ws_manager` | WebSocket broadcaster | передавать как зависимость |

## 3. Поля/методы для добавления в PipelineStageContext

```python
@dataclass
class PipelineStageContext:
    # Уже есть (pass 2):
    project_dir: Path
    project_id: str
    output_dir: Path
    log: Callable[..., Awaitable[None]]
    check_pause: Callable[[], Awaitable[bool]]
    run_subprocess: Callable[..., Awaitable[tuple[int, str, str]]]
    record_cli_usage: Callable[..., None]
    project_info: Optional[dict]
    progress: Optional[Callable[[int, int], Awaitable[None]]]
    object_id: Optional[str]

    # Добавить в pass 3:
    check_before_launch: Callable[[], Awaitable[bool]]
    wait_for_rate_limit: Callable[[str, str], Awaitable[bool]]
    update_pipeline_log: Callable[[str, str, str], None]
    emit_summary: Optional[Callable[[dict], Awaitable[None]]]
    stream_findings: Optional[Callable[[str], Awaitable[None]]]
```

## 4. Порядок безопасного выноса этапов

### Приоритет выноса (от простого к сложному)

#### Фаза 1 — низкий риск (нет LLM, нет WS-потоков)
1. **prepare** (`_run_prepare`) — запускает `process_project.py` как subprocess, нет LLM
2. **crop_blocks** (внутри `_run_gemma_enrichment_stage`, первый блок) — запускает `blocks.py crop`

#### Фаза 2 — средний риск (простой LLM вызов, нет chunked retry)
3. **optimization** (`_run_optimization`) — один claude_runner вызов, нет chunked retry
4. **optimization_review** (`_run_optimization_review`) — один claude_runner вызов

#### Фаза 3 — средний риск (есть LLM + events stream)
5. **text_analysis** (внутри `_run_tile_audit`) — claude_runner + `_stream_findings_events`
6. **findings_review** (`_run_findings_review`) — critic/corrector, chunked, WS events

#### Фаза 4 — высокий риск (сложная оркестрация)
7. **block_analysis** (`_run_block_analysis_findings_only`) — async parallelism, runtime plan
8. **norms** (`_run_norm_verification`) — chunked, native verify, многоэтапный

#### Не выносить (оркестраторы)
- `_run_resumed_pipeline` — главный оркестратор resume
- `_run_smart_pipeline` — оркестратор smart audit
- `_run_ocr_pipeline` — оркестратор OCR pipeline
- `_run_batch_queue` — оркестратор batch
- `_run_block_retry` — retry механизм с fallback логикой

### Шаблон переноса (на примере optimization)

```python
# В backend/app/pipeline/stages/optimization/runner.py:
async def run_optimization_stage(ctx: PipelineStageContext) -> None:
    ctx.update_pipeline_log("optimization", "running")
    project_info = ctx.project_info or {}
    await ctx.log("Запуск анализа оптимизации...")
    if not await ctx.check_before_launch():
        ctx.update_pipeline_log("optimization", "error", error="Rate limit")
        raise RuntimeError("Rate limit: ожидание превышено или отменено")
    exit_code, output, cli_result = await ctx.run_subprocess(...)
    ctx.record_cli_usage(cli_result, "optimization")
    ...
```

```python
# В manager.py:
async def _run_optimization(self, job: AuditJob, standalone: bool = True):
    from backend.app.pipeline.stages.optimization.runner import run_optimization_stage
    ctx = self._make_context(job)
    await run_optimization_stage(ctx)
    if standalone:
        self._cleanup(job.project_id)
```

## 5. Текущий статус (после pass 13 — gemma_enrichment runner) — **Stage Refactor ЗАВЕРШЁН**

| Этап | Runner создан | Функции перенесены | _run_* в manager thin |
|------|--------------|-------------------|------------------------|
| prepare | **runner.py** | run_prepare | **✓ (pass 5)** |
| crop_blocks | **runner.py** | run_crop_blocks, run_policy_recrop | **✓ (pass 7)** |
| gemma_enrichment | **runner.py** | run_gemma_enrichment_stage (crop policy, enrich_project, partial, no_blocks) | **✓ (pass 13)** |
| block_analysis | **runner.py** | run_block_analysis_findings_only + all helpers | **✓ (pass 11)** |
| text_analysis | **runner.py** | run_text_analysis (×3 call sites, triage+standard, rate-limit retry) | **✓ (pass 12)** |
| findings_merge | **runner.py** | run_findings_merge (×2 call sites) + helpers | **✓ (pass 8)** |
| findings_review | **runner.py** | run_findings_review (critic+corrector, chunked, WS) | **✓ (pass 10)** |
| norms | **runner.py** | run_norm_verification + enrich/fix/count helpers | **✓ (pass 9)** |
| optimization | **runner.py** | run_optimization, run_optimization_review | **✓ (pass 4)** |
| report (excel) | **runner.py** | run_excel_report (×3 call sites) | **✓ (pass 6)** |

manager.py: **4747 строк** (было 4934 до pass 13, 7277 изначально; сокращение −2530 строк)

### PipelineStageContext — финальный состав дополнительных полей

- `stream_findings_events` — WS стрим findings-событий
- `reset_job_progress` — сброс прогресса при переходе этапов
- `refresh_finding_quality` — deterministic quality после corrector
- `progress_sync` — синхронный progress для block_analysis executor thread
- `record_block_analysis_usage` — учёт usage findings_only mode
- `is_cancelled` — синхронная проверка отмены для gemma_enrichment thread callback

### Оркестраторы — не выносить

- `_run_smart_pipeline` — smart parallel audit (triage + tile + findings)
- `_run_ocr_pipeline` — основной OCR pipeline
- `_run_resumed_pipeline` — resume с произвольного stage
- `_run_batch_queue` — batch queue оркестратор
- `_run_precrop_loop` — batch precrop loop
- `_run_block_retry` — retry механизм с fallback логикой
- `_run_post_findings_parallel` — параллельный critic + norms + optimization

## 6. Открытые долги (после завершения stage-refactor)

- [ ] `norms/__init__.py` — bare relative import; ROOT_DIR в sys.path работает, но лучше перейти на явный пакет
- [ ] `_run_tile_audit` — legacy метод в batch; определить будет ли удалён или сохранён
- [ ] `_run_main_audit` — legacy метод; то же
- [ ] `_backfill_highlight_regions` — убедиться что highlight_regions корректно индексируются из `02_blocks_analysis.json`
- [ ] `_run_post_findings_parallel` — сложная asyncio.gather схема; рефакторить отдельно после стабилизации
- [ ] **critic/corrector improvement** — отдельный pass; менять только после smoke-прогона с реальным проектом
- [ ] **Smoke-прогон** — запустить полный аудит реального проекта и убедиться что все stage runners работают корректно
