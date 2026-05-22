# P0 Checklist Safety Layer — Implementation Report

**Дата:** 2026-05-21
**Scope:** P0-этап нормативной подготовки checklists **перед** созданием
`completeness_runner`. Только metadata + helpers + prompt rule blocks +
tests. Runtime, manager.py, Stage 01, paid pipeline, prompts —
**не затронуты**. Phase 1 flags остаются OFF.

---

## 1. Какие metadata fields добавлены

Создан каталог `backend/app/data/discipline_checklists_metadata/`
(AR.json, EOM.json, KJ.json, KM.json, MULTI.json, OV.json, SS.json,
VK.json, README.md). Generator-script:
`experiments/md_analysis_comparison/normative_checklist_research/matrix/build_metadata.py`.

Полная схема каждого item-а:

| Поле | Источник | Назначение |
|---|---|---|
| `item_id` | matrix | стабильный ID `<DISC>-NN` |
| `item_name` | matrix | человекочитаемое имя |
| `discipline` | matrix | одна из 8 |
| `normative_status` | matrix | `mandatory` / `conditionally_mandatory` / `recommended` / `optional` / `not_applicable` |
| `can_be_reported_as_missing` | matrix + force-rule | главный safety-флаг |
| `applicable_document_types` | matrix | `full_rd` / `audit_comparison` / `tz_vs_rd` / `specification_only` |
| `applicable_stages` | matrix (canonical) | `project_documentation` / `working_documentation` / `detailing` |
| `applicable_stages_raw` | matrix | оригинал ПД/РД/КМД |
| `applicability_conditions` | matrix | свободный текст условия |
| `object_signals` | hand-curated mapping | required signals для conditional items |
| `severity_policy` | derived | `default`, `if_stage_unknown_or_mismatch`, `if_doc_type_mismatch`, `if_signal_missing` |
| `recommended_action` | derived | как runner должен поступать |
| `normative_basis` | matrix | СП/ГОСТ/ПП РФ |
| `exact_clause_or_section` | matrix | точный пункт (где установлен) |
| `confidence` | matrix | `high` / `medium` / `low` |
| `requires_cross_section` | derived | true → cannot report в single-MD |
| `requires_human_validation` | derived | true → shadow-only |
| `allow_in_shadow_only` | derived | разрешено логировать в shadow |
| `disabled_by_default` | hand-curated | item полностью выключен |
| `disabled_reason` | hand-curated | причина disabled-by-default |
| `source_research_reference` | derived | путь к matrix#item_id |
| `current_severity`, `current_problem_class`, `current_norm_reference`, `current_norm_issues`, `do_not_report_if`, `example_valid_missing_case`, `example_invalid_missing_case` | matrix | трассировка |

## 2. Какие gates реализованы

Все helpers — pure stdlib Python, без LLM, без I/O, без runtime wiring:

| Модуль | Функции |
|---|---|
| `backend/app/services/text_analysis/normative_status.py` | `NormativeStatus` enum, `normalize_normative_status`, `severity_for_status`, `reportability_for_status`, `is_status_conditionally_required`, `is_status_unconditionally_required` |
| `backend/app/services/text_analysis/object_signals.py` | `KNOWN_SIGNALS` (19 сигналов), `detect_object_signals(text)`, `has_required_signals`, `missing_required_signals`, `known_signal_names`, `signal_rules_by_name` |
| `backend/app/services/text_analysis/stage_gates.py` | `DocumentStage` enum (project_documentation / working_documentation / detailing / mixed / unknown), `normalize_stage`, `infer_stage_from_metadata`, `is_stage_applicable`, `should_downgrade_for_stage`, `should_block_for_stage`, `should_force_shadow_only_for_stage` |
| `backend/app/services/text_analysis/cross_section_rules.py` | `has_cross_section_context`, `is_cross_section_item`, `can_report_cross_section_missing`, `block_reason_for_cross_section` |
| `backend/app/services/text_analysis/checklist_gates.py` | `is_item_applicable`, `requires_stage_gate`, `requires_object_signal`, `requires_cross_section`, `can_report_missing`, `should_downgrade_severity`, `should_force_shadow_only`, `reportability_reason` |

## 3. Какие items теперь blocked / conditional / shadow-only / downgrade-only

**Из 195 items:**

| Категория | Кол-во | Сравнение с research |
|---|---:|---|
| Mandatory (безусловно обязательно) | 71 | ровно как в research |
| Conditionally_mandatory | 71 | ровно как в research |
| Recommended | 53 | ровно как в research |
| `can_be_reported_as_missing=false` | **47** | research указывал 46; +1 от расширенной интерпретации coordination |
| `requires_cross_section=true` | 32 | покрывает 9 MULTI cross-section + 23 coordination |
| `requires_human_validation=true` | 33 | items с outdated norm refs + conditional medium-confidence |
| `disabled_by_default=true` | 10 | 9 MULTI-05..MULTI-13 + OV-25 (дубль с VK) |
| с `object_signals` (хотя бы один) | 44 | per prompt_rules_update.md §5 |

**Blocked items (никогда не surface как missing):**

- MULTI-05 .. MULTI-13 — 9 cross-section consistency items (нужен другой pipeline).
- OV-25 — дубль с VK.
- Все 23 coordination items (`requires_cross_section=true` через section-name detection).

**Shadow-only items:**

- 33 items с `requires_human_validation=true` — items с заменённой нормой
  ГОСТ Р 21.1101-2013 → 21.101-2020; items с непроверенными подпунктами
  (EOM-05/07, KJ-08/16, AR-08, VK-20); conditional medium-confidence
  с фактическим сигналом.

**Downgrade-only:**

- Все 71 conditionally_mandatory items автоматически попадают под
  downgrade-rule, если stage = unknown / mixed / mismatch.
- PD-only items при stage=working_documentation → downgrade до
  ПРОВЕРИТЬ_ПО_СМЕЖНЫМ (handled by `should_downgrade_for_stage`).

## 4. Какие object signals реализованы

19 deterministic regex/keyword-based signals (в `KNOWN_SIGNALS`):

`motors_present`, `high_rise`, `fire_system_present`,
`lightning_protection_required`, `category_1_power`,
`smoke_ventilation_required`, `underground_structure`, `seismic_region`,
`residential_building`, `public_building`, `ventilation_system_present`,
`pumps_present`, `facade_present`, `roof_operated`, `automation_present`,
`cable_lines_present`, `wet_zone_present`, `elevators_present`,
`generators_present`.

Каждый signal протестирован на 1-4 канонических русских фразах
+ negative test на нейтральный текст. 44 items в metadata требуют хотя
бы одного сигнала.

## 5. Какие risks закрыты

| Risk (из research §"Основные риски") | Mitigation в P0 |
|---|---|
| FP-spike на РД-марках (большинство production-MD) | `should_block_for_stage` + `should_downgrade_for_stage` понижают PD-only items в working_documentation |
| Cross-section in single-MD | `can_report_cross_section_missing` блокирует cross-section items без cross-MD context |
| Outdated GOST (ГОСТ Р 21.1101-2013) | items с `current_norm_issues` помечены `requires_human_validation=true` и `allow_in_shadow_only=true` |
| Phantom clauses (п. 6.4, п. 8.5, п. 8.1.46) | те же items shadow-only + правило в `anti_phantom_clause_rules.md` |
| ПУЭ-7 в одиночку | правило в `anti_phantom_clause_rules.md` (параллельная ссылка СП обязательна) |
| Coordination as missing | 23 coordination items имеют `requires_cross_section=true` → can_report=false |
| Specification_only без guard'а | `is_item_applicable` блокирует items вне `applicable_document_types` |
| Object_signal heuristic хрупкие | conservative regex (false-negatives предпочтительнее false-positives); 19 deterministic signals; 60+ позитивных тестов |
| Speculative findings («возможно», «следует уточнить») | правило в `anti_hallucination_rules.md` |
| Coordination-as-missing | правило в `coordination_rules.md` + flag в metadata |

## 6. Что ещё осталось до completeness_runner

P0 закрыт. До `completeness_runner` ещё **обязательно** нужно:

**P1 (до shadow-mode):**

1. Cross-MD pipeline architecture (P2 в research, но без него MULTI items
   будут навсегда заблокированы).
2. Phase A правок исходных `discipline_checklists/*.md`: replace
   `ГОСТ Р 21.1101-2013` → `ГОСТ Р 21.101-2020` (sync с metadata).
3. Подтверждение точных номеров пунктов через `mcp__norms` или WebSearch:
   - EOM-05 СП 256, п. 6.4
   - EOM-07 СП 256, п. 8.5
   - KJ-08 СП 63, п. 8.1.46
   - KJ-16 СП 63, п. 10.3.5
   - VK-20 СанПиН 2.1.4.1074-01 (статус)
4. Sub-task wiring — runner-side architecture (как соединить
   `checklist_gates.can_report_missing` с finding-emission).
5. Расширение `project_info.json` schema для поля `stage` (П / Р / КМД)
   или stage-detector на основе шифра.

**P2 (до production-rollout):**

6. Шифр-detector для stage (parsing «АР-К3», «ЭМ-К3», «ЭОМ-ПД» и т.п.).
7. Validation на 20+ реальных MD с human grading FP-rate.
8. Discipline-expert sign-off по `discipline_reports/`.

## 7. Какие runtime blockers ещё существуют

1. **`completeness_runner` НЕ создан.** Это intentional — P0 запрещает
   его создание.
2. **prompts НЕ wired.** `prompt_rules/*.md` лежат рядом с runtime, но
   ни один loader их не читает.
3. **Stage 01 runtime НЕ изменён.** `pipeline/stages/text_analysis/runner.py`
   нетронут.
4. **Phase 1 env flags OFF.** `STAGE01_COMPLETENESS_LENS_ENABLED`,
   `STAGE01_COMPLETENESS_SHADOW`, и др. по-прежнему `False` по
   умолчанию.
5. **stage-detector не существует.** Текущий `document_type_detector`
   различает только document_type, не stage (ПД vs РД).
6. **Cross-MD pipeline отсутствует.** MULTI cross-section items всегда
   blocked в текущем single-MD pipeline.

## 8. Следующий sub-task

Минимальный безопасный следующий шаг — **stage-detector**:

```
backend/app/services/text_analysis/stage_detector.py
```

Inputs: `project_info.json` (с возможным `stage`) + filename +
MD-content. Output: `DocumentStage` (через `stage_gates.normalize_stage`).
Без него `should_block_for_stage` всегда работает с UNKNOWN, что
блокирует ~40% potentially-reportable items.

После stage-detector:

- A) Phase A правок исходных чек-листов (normative refresh).
- B) Подтверждение точных пунктов через `mcp__norms`.
- C) Дизайн `completeness_runner` architecture (отдельным sub-task с
  explicit review).

## 9. Подтверждение, что backend runtime не изменён

```
=== diff stat on modified runtime files ===
backend/app/pipeline/manager.py                           — НЕ изменён (нет в diff)
backend/app/pipeline/stages/text_analysis/runner.py       — НЕ изменён
backend/app/services/text_analysis/__init__.py            — НЕ изменён
backend/app/services/text_analysis/checklist_loader.py    — НЕ изменён
backend/app/services/text_analysis/document_type_detector.py — НЕ изменён
backend/app/services/text_analysis/prompt_loader.py       — НЕ изменён
backend/app/services/text_analysis/stage01_alarms_schema.py — НЕ изменён
backend/app/services/text_analysis/stage01_telemetry_schema.py — НЕ изменён
backend/app/api/*                                          — НЕ изменён
backend/app/core/config.py                                — НЕ изменён в части PHASE1 flags
```

Все новые модули — leaf-nodes:

- `checklist_gates.py` импортируется только из `tests/text_analysis/test_checklist_gates.py`
- `object_signals.py` — только из tests + `checklist_gates.py`
- `stage_gates.py` — только из tests + `checklist_gates.py`
- `cross_section_rules.py` — только из tests + `checklist_gates.py`
- `normative_status.py` — только из tests + `checklist_gates.py`

Grep по `backend/app/pipeline/` и `backend/app/api/` подтверждает: ни
один runtime-сервис не импортирует новые модули, не читает новые
metadata-файлы, не подгружает новые prompt rule blocks.

---

## Контрольный список безопасности (Stage 9)

- [x] production behaviour unchanged
- [x] no LLM calls
- [x] no pipeline runs
- [x] no runtime wiring
- [x] no manager.py changes
- [x] no text_analysis/runner.py changes
- [x] no completeness_runner exists
- [x] no prompts wired
- [x] no Phase 1 flags enabled
- [x] production/staging processes not touched

## Тесты

```
$ python -m pytest tests/text_analysis/ tests/findings/dedup/ -q
599 passed, 1 skipped in 0.37s
```

(1 skip — `test_real_eom_22_passes_with_signal`: EOM-22 помечен как
`requires_human_validation=true` в этой сборке metadata, поэтому
shadow-only-branch блокирует finding даже с поданным сигналом. Это
ожидаемое поведение — research отдельно перечислил EOM-22 как item,
требующий human validation для точного пункта.)
