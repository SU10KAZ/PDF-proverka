# Phase 0 — Final Activation Report

**Дата:** 2026-05-20
**Автор:** автоматическая финальная проверка (Claude Code, /goal)
**Объём:** state-check + offline regression на 130 реальных проектах + rollback verify

---

## TL;DR

| Вопрос | Ответ |
|---|---|
| Phase 0 стабильный? | **Да** |
| Где включён? | Только на staging (port 8083, PID 3120881) |
| Production тронут? | **Нет** |
| Сколько проектов проверено | **130** (все активные `_output/03_findings.json`) |
| Findings до → после | **4134 → 4019** (Δ=−115, −2.78%) |
| КРИТ до → после | **654 → 654** (полностью сохранены) |
| Ошибки | **0** |
| Rollback verified? | **Да** (subprocess с env=false → `apply_phase0_dedup` returns `None`, file SHA unchanged) |
| Можно идти к Phase 1? | **Да**, после короткой staging-soak с реальным аудит-пайплайном |

---

## 1. Текущее состояние main

| Параметр | Значение |
|---|---|
| HEAD repo working tree | `f50c3ae78a6588c7d0a91c6bf706d7329721a5a7` |
| origin/main | `f50c3ae78a6588c7d0a91c6bf706d7329721a5a7` |
| local `main` branch | `c8d765246c6ec1e7aacd134efe7055329efb9ee2` (stale, на 2 коммита позади origin — это локальный артефакт, не влияет на код) |
| Phase 0 merge commit | `f50c3ae` (Merge PR #1, 2026-05-20 14:09 MSK) |
| Phase 0 work commit | `ade03f7` (`fix(findings): add phase0 dedup post-process`) |
| `ade03f7 is ancestor of HEAD` | **yes** |
| `f50c3ae is ancestor of HEAD` | **yes** |
| Phase 0 артефакты на месте | `backend/app/services/findings/dedup/{__init__.py,class_dedup.py,fuzzy_dedup.py,_normalise.py,README.md}` + `tests/findings/dedup/` (4 тест-файла) + `apply_phase0_dedup` вызывается из `findings_merge/runner.py:553` |
| Config flags в `backend/app/core/config.py` | `STAGE01_DEDUP_ENABLED` (default **False**) на строке 700; `STAGE01_DEDUP_FUZZY_THRESHOLD` (default **0.7**) на строке 706 |
| Рабочее дерево | Содержит pre-existing «dirty» правки (data files, конфиги для external_registers и др.) — НЕ внесены этим прогоном. SHA `paid_cost.json` и `usage_data.json` зафиксированы до/после прогона как идентичные. |

**Вывод:** main содержит Phase 0 ровно там, где должен; рабочее дерево локально грязное, но мы ничего туда не добавляли.

---

## 2. Текущее состояние production

| Параметр | Значение |
|---|---|
| PID | 2993491 |
| Port | 127.0.0.1:8082 |
| cwd | `/home/coder/projects/PDF-proverka` |
| `/docs` | 200 OK |
| `/api/projects` | 200 OK (dict с ключами `projects`, `object_name`) |
| Started | 2026-05-20 10:58:25 (≈3ч 11мин **до** merge `f50c3ae`) |
| Загруженный код Phase 0 | **отсутствует в процессе** (процесс запущен до мерджа, dedup-модуль ещё не импортирован) |
| `.env` файл | mtime 2026-05-18 15:55 — **до Phase 0**, не содержит `STAGE01_DEDUP_*` |
| ENV `STAGE01_DEDUP_ENABLED` в proc | не задано → дефолт `False` |
| ENV `STAGE01_DEDUP_FUZZY_THRESHOLD` в proc | не задано → дефолт `0.7` |
| Файлы проектов с `meta.dedup_report` | **0 из 130** (production никогда не выполнял Phase 0) |
| `paid_cost.json` SHA | `b9d1c6ed428c4e3944a9f07663b3c6f9d37f8d6ae432b6125ca5f363b1ed7480` (не менялся за прогон) |
| `usage_data.json` SHA | `cad8e665b647c4d381129f08b9e9ae4eeb31c847aa97c6c499f4c1ac8acf403b` (не менялся за прогон) |

**Вывод:** production полностью изолирован от Phase 0. Процесс работает на pre-merge коде, env флага нет, ни одного проекта с `meta.dedup_report` нет. Никаких изменений production-state этой проверкой не сделано.

---

## 3. Текущее состояние staging

| Параметр | Значение |
|---|---|
| PID | 3120881 |
| Port | 127.0.0.1:8083 |
| cwd | `/home/coder/projects/PDF-proverka` |
| `/docs` | 200 OK |
| `/api/projects` | 200 OK |
| ENV в proc | `STAGE01_DEDUP_ENABLED=true`, `STAGE01_DEDUP_FUZZY_THRESHOLD=0.7` |
| Загруженный код Phase 0 | присутствует (процесс стартовал после мерджа) |
| Реальные аудиты через staging | **не запускались** (ни один `03_findings.json` в `projects/` не имеет `meta.dedup_report`) |

**Вывод:** staging здоровый, флаги выставлены правильно. Но за всё время существования (≈36+ минут на момент проверки) на нём не прогонялся `findings_merge` через реальный пайплайн, поэтому «production-like нагрузки» с включённым Phase 0 в боевом потоке у нас ещё не было.

---

## 4. Результаты offline telemetry check

### Метод

Скрипт `/tmp/phase0_telemetry/run_offline_telemetry.py`:

1. Авто-discover всех 130 активных `projects/*/*/*/_output*/03_findings.json` (бэкап-снимки `_pre_enrichment_*`, `_backup_*`, `_versions/v2/*` — 9 шт. — намеренно исключены: это не production state).
2. Для каждого файла:
   - SHA256-snapshot оригинала;
   - копия в `/tmp/phase0_telemetry/copies/<disc>__<name>.json`;
   - **in-memory** вызов `collapse_to_canonical` → `fuzzy_dedup(threshold=0.7)` ровно как в `apply_phase0_dedup` (импорт из `backend.app.services.findings.dedup`);
   - запись deduped результата в `/tmp/phase0_telemetry/outputs/`;
   - re-snapshot SHA оригинала и подсчёт инвариантов.
3. `paid_cost.json` и `usage_data.json` SHA снимались **до** и **после** всего прогона.

LLM не вызывался. Пайплайн не запускался. Production-файлы открывались только на чтение (verified by post-snapshot SHA).

### Сводка по дисциплинам

| discipline | projects | before | after | drops | %drop | КРИТ_b | КРИТ_a | КРИТ_collapsed | max_ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| AI | 3 | 259 | 247 | 12 | 4.6% | 9 | 9 | 0 | 3766 |
| AR | 37 | 1165 | 1101 | 64 | 5.5% | 190 | 190 | 23 | 313 |
| EOM | 20 | 661 | 650 | 11 | 1.7% | 98 | 98 | 9 | 326 |
| GP | 7 | 123 | 122 | 1 | 0.8% | 5 | 5 | 0 | 25 |
| KJ | 20 | 627 | 609 | 18 | 2.9% | 137 | 137 | 4 | 430 |
| KM | 7 | 237 | 237 | 0 | 0.0% | 69 | 69 | 0 | 235 |
| OV | 4 | 89 | 87 | 2 | 2.2% | 11 | 11 | 3 | 95 |
| PT | 2 | 39 | 39 | 0 | 0.0% | 8 | 8 | 2 | 40 |
| SS | 21 | 776 | 771 | 5 | 0.6% | 113 | 113 | 5 | 395 |
| TX | 4 | 63 | 63 | 0 | 0.0% | 4 | 4 | 0 | 25 |
| VK | 5 | 95 | 93 | 2 | 2.1% | 10 | 10 | 0 | 39 |

### Aggregate

| Метрика | Значение |
|---|---|
| total_projects | **130** |
| disciplines covered | **11 / 11** |
| ok_projects | 130 |
| error_count | **0** |
| total_findings_before | 4134 |
| total_findings_after | 4019 |
| total_drops | 115 (−2.78%) |
| total_critical_before | 654 |
| total_critical_after | **654** |
| total_critical_collapsed_count | 46 (см. примечание ниже) |
| duration_ms p50 / p90 / p95 / p99 / max | 61 / 265 / 321 / 996 / **3766** |
| original_file_sha_unchanged (all 130) | **true** |
| `paid_cost.json` SHA unchanged | **true** |
| `usage_data.json` SHA unchanged | **true** |
| invariant_failures | **0** |

### Замечательные строки на уровне проектов

Все 130 — без ошибок. Ниже — выборка из 42 проектов, где либо были drops, либо срабатывала защита КРИТ:

| disc | project | before | after | drops | КРИТ_b | КРИТ_a | КРИТ_coll | ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| AI | 133-23-ГК-АИ2 | 172 | 160 | 12 | 4 | 4 | 0 | 3766 |
| AR | 13АВ-РД-АР1.2-К3 (2) | 49 | 26 | 23 | 7 | 7 | 8 | 251 |
| AR | 13АВ-РД-АР0.4-ПА | 32 | 19 | 13 | 5 | 5 | 4 | 143 |
| AR | 13АВ-РД-АР0.3-ПА (Изм.1) | 30 | 17 | 13 | 2 | 2 | 3 | 81 |
| EOM | 133_23-ГК-ГРЩ | 14 | 12 | 2 | 11 | 11 | 9 | 16 |
| EOM | 133_23-ГК-ЭО2 | 22 | 19 | 3 | 3 | 3 | 0 | 28 |
| KJ | 13АВ-РД-КЖ5.17-23.2-К2 (Изм.1) | 90 | 88 | 2 | 14 | 14 | 2 | 430 |
| OV | 133-23-ГК-ОВ2.2 | 24 | 22 | 2 | 5 | 5 | 2 | 95 |
| PT | 133-23-ГК-АГПТ | 18 | 18 | 0 | 4 | 4 | 2 | 40 |
| SS | 13АВ-РД-АПЗ.АПС-К5 (согл от 02.04.2026) | 59 | 56 | 3 | 12 | 12 | 2 | 395 |
| … | (полный JSON — `/tmp/phase0_telemetry/telemetry_summary.json`) | | | | | | | |

**Heaviest project (AI/133-23-ГК-АИ2):** 172 findings → 160 findings, 12 drops, 0 КРИТ-collapse, 3766ms. Самый «тяжёлый» по времени, но это — единственный outlier; p95 = 321 ms. Прогон Phase 0 для всех 130 проектов закончился за ≈21 секунду walltime суммарно.

---

## 5. Результаты rollback check

Скрипт `/tmp/phase0_telemetry/verify_rollback.py`, отдельный subprocess, env hard-set `STAGE01_DEDUP_ENABLED=false`:

```
[OK]  config.STAGE01_DEDUP_ENABLED is False
[OK]  target findings sha (before): 0a5824d6f0a57769…
[OK]  apply_phase0_dedup returned: None
[OK]  target findings sha (after) : 0a5824d6f0a57769…

ROLLBACK VERIFIED
  flag read from config          : False
  apply_phase0_dedup returned    : None
  03_findings.json sha unchanged : True
  paid_cost.json sha unchanged   : True
  usage_data.json sha unchanged  : True
  meta.dedup_report state held   : True
```

Что подтверждено:

1. Env-флаг `false` реально прочитан через **import** `backend.app.core.config` (не словами, а `cfg.STAGE01_DEDUP_ENABLED is False` assert).
2. `apply_phase0_dedup(project_id)` отрабатывает **early-return None** (строка `runner.py:204`).
3. SHA256 целевого `03_findings.json` (PT/133_23-ГК-ПТ) до и после вызова **байт-в-байт идентичны**.
4. SHA256 `paid_cost.json` и `usage_data.json` не изменились.
5. Состояние `meta.dedup_report` (отсутствовало) не появилось.

---

## 6. Risks / Watch items

| # | Риск | Тяжесть | Статус |
|---|---|---|---|
| R-1 | КРИТ-collapse в защитном механизме сработал 46 раз на 130 проектов (18 проектов из 130, 14%) | medium-info | По коду: «zero is the expected value when problem_class semantics are correct». Сетка спасает корректно — `crit_after == crit_before` во всех 18 случаях. То есть **продакшен-импакт = 0**, но есть signal, что v2 problem_class иногда группирует КРИТ с не-КРИТ. Это **не блокер** для Phase 0 (защита работает), но это вход в работу Phase 1 (нормализация классов) — стоит включить в Phase 1 success metric. |
| R-2 | Performance: один проект (AI/133-23-ГК-АИ2, 172 findings) дал 3766 ms | low | p95 = 321 ms. Один outlier при 130 проектах — не показание. В пайплайне Phase 0 запускается один раз после `findings_merge` — добавочные ~1 сек на крупный аудит незаметны. |
| R-3 | Staging реально не прогонял `findings_merge` через пайплайн (нагрузки в боевом потоке нет) | low | Все 130 файлов всё ещё **без `meta.dedup_report`**. Offline проверка показывает только корректность самой функции; нужна одна реальная staging-soak с любым уже-готовым проектом → retry стадии `findings_merge` (без paid LLM, если стадия ниже paid — но в нашем пайплайне `findings_merge` идёт **после** Stage 02 и сама **не платит**). Это «следующее естественное действие», но не блокер для verdict «stable». |
| R-4 | Local `main` branch ниже origin/main | very low | Чисто локальный артефакт checkout-а. На production и staging это не влияет (оба запущены из `/home/coder/projects/PDF-proverka`, working tree HEAD = `f50c3ae`). Можно фоном сделать `git fetch && git pull` на main — но это операция со стейтом, мы её не делаем без явного запроса. |
| R-5 | Production процесс был запущен ДО мерджа | none | Это плюс: production гарантированно изолирован от Phase 0. Когда (и если) production будут включать в Phase 0, потребуется рестарт процесса + установка env-флага. Сейчас включение НЕ требуется. |

---

## 7. Final verdict по Phase 0

Все критерии PASS-критерия из задачи `/goal`:

| Критерий | Значение | Статус |
|---|---|---|
| `error_count == 0` | 0 / 130 | ✅ |
| `critical_after >= critical_before` для каждого проекта | 130 / 130 | ✅ |
| Original files unchanged | 130 / 130 (SHA256 match) | ✅ |
| `paid_cost.json` unchanged | SHA match | ✅ |
| `usage_data.json` unchanged | SHA match | ✅ |
| Production untouched | env пуст, файлы без `meta.dedup_report`, процесс на pre-merge коде | ✅ |
| Rollback verified | flag=false → None, SHA match | ✅ |
| `max_duration_ms` некритичен | 3766 ms на heaviest project, p95=321 ms | ✅ |
| No pipeline crash | telemetry script завершился чисто; staging health 200 | ✅ |

**Phase 0 stable: YES.**

---

## 8. Можно ли идти к Phase 1?

| Вопрос | Ответ |
|---|---|
| Phase 0 stable | **yes** |
| staging soak acceptable | **conditional yes** — функция доказана на 130 файлах; реального audit-run через staging пока не было, но это не блокер для Phase 1 (см. ниже) |
| production canary acceptable | **yes** (когда понадобится — рестарт PID 2993491 с env-флагом true; rollback = убрать env + рестарт) |
| можно ли начинать Phase 1 implementation | **yes**, но НЕ в этом /goal-проходе (явный запрет в задаче) |

Phase 0 включать в production **сейчас не требуется** для перехода к Phase 1. Phase 1 — prompt-upgrade Stage 01 — реализуется и тестируется отдельно. Phase 0 уже сидит как post-process в `findings_merge`, и его включение в production может быть отделено от Phase 1 implementation.

---

## 9. Что именно делать дальше

### Если оставить Phase 0 на staging как сейчас

Ничего не делать. Staging уже включён (env `STAGE01_DEDUP_ENABLED=true`), production выключен по умолчанию. Это safe steady state.

### Если включать Phase 0 на production (НЕ делать в этом /goal)

Когда придёт время:

1. Перед включением: подтвердить, что вы согласны.
2. Изменить `/home/coder/projects/PDF-proverka/.env` — добавить:
   ```
   STAGE01_DEDUP_ENABLED=true
   STAGE01_DEDUP_FUZZY_THRESHOLD=0.7
   ```
3. Рестарт production: `kill -TERM 2993491` и поднять заново на `:8082`.
4. Rollback (моментально): убрать строки из `.env` + рестарт. На уже-обработанных файлах `meta.dedup_report` останется (это безопасное метаполе, его наличие не влияет на дальнейший аудит).
5. Watch metrics после включения:
   - `crit_after >= crit_before` на каждом новом `findings_merge`;
   - `pipeline_log.json → phase0_dedup → critical_collapsed_count` — растущая величина = signal для Phase 1 review;
   - latency: добавка <1 с на heaviest, обычно <100 мс.

### Phase 1 implementation — короткий plan (НЕ начинать сейчас)

Из `docs` в `production_preparation/`:

| Что | Где |
|---|---|
| Что внедрять | Stage 01 prompt upgrade — обогащение полей `problem_class`, `affected_system`, `interface_type`, `discipline_pair`, `evidence_quote`. Это input для Phase 0 dedup, который сейчас гонит fallback-class через `category + signature(problem)`. |
| Что НЕ внедрять | completeness lens, manager.py changes, новые LLM calls, новые stages. |
| Файлы из `production_preparation/` для опоры | `prompts/`, `schemas/`, `integration_plan/`, `rollout/phase1_rollout.md`, `rollout/ab_testing_strategy.md` |
| Feature flag | `STAGE01_PROMPT_V2_ENABLED` (отдельный от Phase 0 флага) |
| Guardrails | строгая JSON-schema validation на ответе LLM; fail-open на исходный prompt; никаких изменений в `findings_merge` логике; A0/B0 baseline кэш (см. `algorithm_research/`) |
| С чего начать | (а) добавить feature-flag в `config.py`; (б) добавить v2 prompt template в `pipeline/stages/01_text_analysis/`; (в) включить branching в runner по флагу; (г) повторный offline regression на тех же 130 files. |
| Когда мерджить | после A0/B0 regression на проде-эквивалентном dataset. |

---

## Приложения

- Полные per-project метрики: `/tmp/phase0_telemetry/telemetry_summary.json` (130 записей)
- Deduped копии (для inspect, в production не уходят): `/tmp/phase0_telemetry/outputs/`
- Скрипты этой проверки: `/tmp/phase0_telemetry/run_offline_telemetry.py`, `/tmp/phase0_telemetry/verify_rollback.py`
