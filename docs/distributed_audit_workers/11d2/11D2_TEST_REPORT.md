# 11D.2 — отчёт по тестам (до единственного реального вызова)

Все прогоны выполнены на центре в worktree `9d6e06c2`, **до** обращения к
модели. Ни один тест модель не вызывает.

## 1. Наборы, обязательные по §9

| набор | что закрывает | результат |
|---|---|---|
| `tests/test_11d1_text_analysis_semantic_equivalence.py` | 28 тестов 11D.1: сохранение SEVERITY_SEMANTICS, отсутствие зависимости от `CLAUDE.md`, транспорт снят, инженерное сохранено | **28 passed** |
| `tests/test_distributed_workers_text_analysis_provider.py` | набор 11D: ProviderAdapter на этапе, сборка промпта, валидатор, exactly-once | **86 passed** |
| вместе | | **114 passed за 3,0 с** |

## 2. Полная поверхность распределённых воркеров

19 файлов `tests/test_distributed_workers_*.py` + набор 11D.1:

```
1043 passed, 1 skipped, 0 failed — 93,5 с
```

Сюда входят наборы, названные в §9 поимённо:

| требование §9 | где закрыто |
|---|---|
| relevant ProviderAdapter tests | `test_distributed_workers_provider_gate.py`, `..._pipeline_provider.py` |
| relevant text_analysis tests | `test_distributed_workers_text_analysis_provider.py`, `test_11d1_...` |
| model policy tests | `..._provider_gate.py` (политика воркера, `accepted_reported_models`) |
| exactly-once tests | `..._inference_gate.py`, `..._executor.py` (grant → consume → ledger) |
| prompt leak tests | `..._text_analysis_provider.py` (`build_map` и soft-contract без содержимого) |
| tool isolation tests | `test_l_no_tool_requirement`, `test_h_tool_instructions_present_in_legacy_absent_in_provider` |

## 3. Широкая выборка — сверка с базой 11D.1

Та же команда, что в 11D.1:

```
pytest tests backend/tests -k "provider or text_analysis or claude_runner or
distributed or prompt or prescan or absence"
```

| | 11D.1 (база) | 11D.2 |
|---|---|---|
| passed | 1741 | **1741** |
| failed | 1 | **1** |
| skipped | 7 | 7 |

Единственное падение — то же самое, что и в 11D.1:
`backend/tests/test_benchmark_critic_v2_against_human.py::TestProviderUnavailableSafeguard::test_cli_with_max_candidates`.
Это известный долг платформы, к 11D.1/11D.2 отношения не имеет. **Новых падений
нет.**

Из прогона исключены 9 файлов `test_*_geometry.py`: они падают **на сборе** из-за
отсутствия корпусных данных в worktree — ровно то же зафиксировано в
`11D_TEST_REPORT.md` и `11D1_TEST_REPORT.md`.

## 4. Минимальные подтверждения §9

| требование | подтверждено чем | итог |
|---|---|---|
| SEVERITY_SEMANTICS preservation | `test_c_severity_semantics_carried_into_provider_prompt`, `test_c_severity_semantics_is_symmetric` + проверка 1–3 предпрогонного гейта на реальном тексте промпта | **PASS** |
| CLAUDE.md dependency = absent | `test_q_personal_context_stays_excluded` + проверка 4 гейта (`"CLAUDE.md" not in prompt`) + argv `--setting-sources=` | **absent** |
| tools = 0 | `test_l_no_tool_requirement` + argv живого процесса (`--tools=`, поимённый `--disallowed-tools`) | **0** |
| legacy fallback = impossible | `test_b2`, `test_b3`, `test_a2_stage_outside_whitelist_fails_without_fallback` | **impossible** |
| prompt logging leak = impossible | `test_r_build_map_carries_no_content`, `test_r_soft_contract_report_carries_no_content` | **impossible** |
| exact model = claude-opus-5 | политика воркера → `binding.model`; проверка валидатора `model_matches_policy` | **claude-opus-5** |

Ни один из relevant-наборов не красный → §9 не заблокировал прогон.

## 5. Репетиции без модели

Помимо pytest, боевой путь этапа пройден целиком дважды в режиме `fake`
(подставной CLI, **0 обращений к модели**):

| task_id | результат | зачем |
|---|---|---|
| `11d2-fake-1` | PASS, инструкции 19 432 симв. | снять ТОЧНЫЙ текст, уходящий в stdin, и прогнать по нему 48 проверок §8 |
| `11d2-fake-2` | PASS | второй каталог попытки — доказать, что промпт не зависит от пути |

Отпечаток промпта в обеих репетициях совпал побайтово
(`a376d111…`), то есть промпт детерминирован и известен **до** оплаченного
вызова.

## 6. Чего тесты НЕ доказывают

1. **Что правка улучшает результат.** Тесты проверяют вход. Влияние на выход
   измеряется одним прогоном — этого мало для вывода о причинности (§21).
2. **Поведение на большом документе.** Все замеры — на одном документе в
   11,4 К символов; ветки нарезки промпта в provider-режиме по-прежнему нет.
3. **Что три исторические темы обязаны появиться.** Ни одна из них не является
   критерием (§16, §20); наблюдения вынесены в `11D2_SEVERITY_COMPARISON.json`.
