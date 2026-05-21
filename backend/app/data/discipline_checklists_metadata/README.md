# discipline_checklists_metadata

**Дата сборки:** автоматически (см. `_generated_at`).
**Источник:** `experiments/md_analysis_comparison/normative_checklist_research/`.

Этот каталог — **metadata layer** для проверки чек-листов
`backend/app/data/discipline_checklists/`. Каждый JSON содержит per-item
нормативные атрибуты, gates и safety-флаги, выведенные из
`normative_checklist_research/final_report.md` + `matrix/...`.

## Назначение

Metadata НЕ исполняется runtime сейчас. Это **prepared safety layer** для
будущего `completeness_runner`. Backend читает эти файлы как plain data,
никакие LLM/pipeline/Stage-01 не затронуты этим каталогом.

## Структура

```
discipline_checklists_metadata/
├── AR.json    — 23 items
├── EOM.json   — 25 items
├── KJ.json    — 25 items
├── KM.json    — 25 items
├── MULTI.json — 22 items
├── OV.json    — 25 items
├── SS.json    — 25 items
├── VK.json    — 25 items
└── README.md
```

Всего: **195 items** (соответствует matrix).

## Поля item-а

| Поле | Тип | Описание |
|---|---|---|
| `item_id` | str | `<DISC>-NN` — ключ matrix |
| `item_name` | str | человеко-читаемое имя |
| `discipline` | str | одна из 8 |
| `normative_status` | enum | `mandatory` / `conditionally_mandatory` / `recommended` / `optional` / `not_applicable` |
| `can_be_reported_as_missing` | bool | **главный safety-флаг** для completeness_runner |
| `applicable_document_types` | list[str] | подмножество {`full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`} |
| `applicable_stages` | list[str] | подмножество {`project_documentation`, `working_documentation`, `detailing`} |
| `applicable_stages_raw` | list[str] | оригинал из matrix (ПД/РД/КМД) |
| `applicability_conditions` | str | свободный текст условия |
| `object_signals` | list[str] | required signals для условного item; пустой = no gate |
| `severity_policy` | dict | `default`, `if_stage_unknown_or_mismatch`, `if_doc_type_mismatch`, `if_signal_missing` |
| `recommended_action` | enum | как runner должен поступать |
| `normative_basis` | str | СП/ГОСТ/ПП РФ ссылка |
| `exact_clause_or_section` | str | точный пункт (если установлен) |
| `confidence` | enum | `high` / `medium` / `low` |
| `requires_cross_section` | bool | true → cannot report в single-MD pipeline |
| `requires_human_validation` | bool | true → shadow-only до подтверждения пункта |
| `allow_in_shadow_only` | bool | разрешено логировать в shadow-mode |
| `disabled_by_default` | bool | item полностью выключен (см. `disabled_reason`) |
| `disabled_reason` | str\|null | причина disabled-by-default |
| `source_research_reference` | str | путь к исходной записи matrix |
| `current_severity` | str | severity в текущем checklist.md (для трассировки) |
| `current_problem_class` | str | problem_class в текущем checklist.md |
| `current_norm_reference` | str | нормативная ссылка в текущем checklist.md |
| `current_norm_issues` | list[str] | известные проблемы текущей ссылки |
| `do_not_report_if` | str | свободный текст условий отказа от finding |
| `example_valid_missing_case` | str | пример допустимого missing |
| `example_invalid_missing_case` | str | пример недопустимого missing |

## Контракт значений

- `normative_status` ∈ {`mandatory`, `conditionally_mandatory`, `recommended`,
  `optional`, `not_applicable`}.
- `applicable_document_types` ⊆ {`full_rd`, `audit_comparison`,
  `tz_vs_rd`, `specification_only`}.
- `applicable_stages` ⊆ {`project_documentation`, `working_documentation`,
  `detailing`}.
- `object_signals` ⊆ allow-list из `object_signals.py` (см. там).
- Если `requires_cross_section=true` → `can_be_reported_as_missing=false`
  (force-enforced генератором).

## Как генерируется

Из `experiments/md_analysis_comparison/normative_checklist_research/matrix/completeness_requirements_matrix.json`
скриптом `experiments/md_analysis_comparison/normative_checklist_research/matrix/build_metadata.py`.
Скрипт идемпотентный — повторный запуск перезаписывает JSON-ы.

## Как НЕ использовать

- Не импортировать в runtime до того, как `completeness_runner` будет создан.
- Не модифицировать руками — генерировать через build_metadata.py.
- Не считать `disabled_by_default=true` items валидными missing-findings.
- Не считать items с `requires_cross_section=true` валидными missing-findings
  в single-MD pipeline.
