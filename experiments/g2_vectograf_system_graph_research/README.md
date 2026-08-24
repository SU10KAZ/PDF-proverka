# G2 research: расширение Вектографа до SYSTEM_GRAPH

Research-only. Production-код не изменён. Итоговый отчёт —
`G2_VECTOGRAF_SYSTEM_GRAPH_RESEARCH.md` в корне репозитория.

## Скрипты

| Файл | Что делает |
|---|---|
| `probe_blockers.py` | точечная проба: где именно production-вектограф отказывает на паре ГРЩ |
| `corpus_audit.py` | прогон production-вектографа по блокам старого формата (`result.json`) |
| `corpus_audit_blocksjson.py` | то же для нового формата (`blocks.json`), где живёт пара ГРЩ |
| `dialects.py` | сигнатуры и классификация диалектов по всему корпусу |
| `poc/g2_engine.py` | PoC-движок: улики → профиль → общий каркас → SYSTEM_GRAPH |
| `poc/g2_comparator.py` | PoC-компаратор двух SYSTEM_GRAPH (уровни A/B/C + проход «детализация ≠ изменение») |
| `poc/g2_ledger.py` | укладка результата Mode 2 в общий `graphic-change-ledger.v1` и проверка валидатором G1 |
| `poc/run_grsh.py` | построение обоих графов боевой пары |
| `poc/run_compare.py` | сравнение построенных графов |
| `poc/run_coverage.py` | покрытие корпуса маршрутизацией «classic-first» |
| `poc/run_regression.py` | замер риска обратного порядка (профиль до структурера) |

## Артефакты (`artifacts/`)

`current_gate_failures.json` — 12 блокеров с измерениями ·
`corpus_audit*.json`, `dialect_vs_outcome.json`, `dialects.json` — аудит корпуса ·
`coverage_results.json`, `regression_results.json` — покрытие и регресс ·
`grsh_left_graph.json`, `grsh_right_graph.json`, `grsh_comparison.json`,
`grsh_mode2_ledger.json` — боевая пара ·
`proposed_system_graph.schema.json`, `proposed_comparator.schema.json`,
`architecture.md` — предлагаемые контракты и архитектура ·
`corpus_texts/` — вектор-текст 175 блоков корпуса (сырьё для классификации).

## Главное

* Вектограф не разбирает ГРЩ по 12 причинам; первая — построчный расчётный
  якорь как единственный вход.
* Найден дефект production: игнорируется поворот страницы (`/Rotate 270` →
  348 слов вместо 500 в блоке).
* Боевая пара П↔РД разобрана автоматически, без ручных CASES: остов совпал
  полностью, отходящие 30 и 27 (15+15 / 13+14).
* Общий каркас переносится на корпус (остов у 70 из 89), слой функциональной
  идентичности — пока нет.
* Рекомендация: **B** — эволюционная переработка, начиная с фикса поворота.
