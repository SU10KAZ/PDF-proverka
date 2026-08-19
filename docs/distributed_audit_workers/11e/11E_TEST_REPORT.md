# 11E — тесты

## 1. Новый набор

`tests/test_distributed_workers_findings_merge_provider.py` — **78 тестов**,
ноль обращений к настоящей модели (везде подставной исполняемый файл).

| группа | тестов | что защищает |
|---|---|---|
| `TestStageRouting` | 4 | A/B/C/AM: мост уводит боевой свод в ProviderAdapter; без привязки прежний путь не меняется; отказ моста не ведёт к legacy CLI; ветка codex в provider-режиме недостижима |
| `TestInputContract` | 6 | D/E/F: оба входа читает конвейер; отсутствующий/битый/не-объектный вход — отказ ЭТАПА до модели; тихая подстановка «(файл не найден)» перехвачена |
| `TestInputTransport` | 11 | G/H/I/J/K: каждый T-/G-идентификатор в промпте; блочные метаданные и координаты доехали; ни кропа, ни PDF; детерминированная сериализация; Unicode цел; усечения нет; потолок промпта — отказ до вызова |
| `TestSemanticPreservation` | 22 | L..S: роль дисциплины, дедуп, объединение, evidence, трассировка источников, повышение/понижение severity, схема, sheet/page, norm_quote; смысл severity явно и в одном экземпляре с этапом 01; справка о входе не лжёт; ложная строка шаблона опровергнута |
| `TestTransportShell` | 9 | T/U/V/W/X/Y/Z: файловые инструкции сняты; путь выхода не сообщается; инструменты выключены набором И поимённо; один ход; cwd — пустой каталог внутри попытки |
| `TestModelPolicy` | 3 | AA/AB: точная модель в argv из локальной политики; чужая фактическая модель — отказ; политика резолвит способность локально |
| `TestExactlyOnce` | 5 | AD/AE/AF: один вызов и одна запись журнала; повтор берёт результат из журнала; `indeterminate` не повторяется; разрешение списывается ровно один раз; без `grant_id` вызова нет |
| `TestOutputWriting` | 4 | AG/AH/AI/AJ: файл пишет конвейер; запись вне попытки отклонена; ни промпта, ни ответа, ни клиентских формулировок в отчёте |
| `TestSafety` | 8 | AK/AL/AM/AN/AO: санитайзер учётных данных; маркер канарейки — отказ; codex отказывает на явной модели; norm_verify недостижим; downstream-артефактов нет; белый список этапов; текст ошибки CLI доезжает и обрезается |
| `TestProductionStageRunner` | 2 | боевой раннер `stages/findings_merge/runner.py` проходит поверх provider-режима целиком, включая post-merge; отказ маршрута доезжает как отказ этапа |

## 2. Прогон

```
tests/test_distributed_workers_findings_merge_provider.py    78 passed
tests/test_distributed_workers_text_analysis_provider.py     (11D)  passed
tests/test_11d1_text_analysis_semantic_equivalence.py        (11D.1) passed
                                                             итого 192 passed
```

Широкая выборка по поверхности распределённых воркеров и LLM-транспорта:

```
python -m pytest tests backend/tests -k "distributed or provider or findings_merge
                                         or claude_runner or adapter"
→ 1197 passed, 2 skipped, 1 failed, 9 errors (collection)
```

## 3. Единственное падение — не новое

`backend/tests/test_benchmark_critic_v2_against_human.py::TestProviderUnavailableSafeguard::test_cli_with_max_candidates`

```
CLI failed: ERROR: No projects found with both 03_findings.json and expert_review.json
```

Проверено прямым прогоном **на базовом коммите** `3b5a9bb2` в его собственном
worktree — падает там же и с тем же сообщением. Это дрейф окружения (в корпусе
нет проекта раздела AR с `expert_review.json`), а не регресс 11E.

Девять ошибок сборки (`tests/test_*_geometry.py`) — того же класса: тестам нужны
данные проектов, которых в этом окружении нет. На базе воспроизводятся так же.

## 4. Проверки в рантайме, а не только в тестах

Тест доказывает, что код УМЕЕТ. Гейт §23 доказывает, что в этот раз СДЕЛАЛ:
`scripts/verify_11e_pre_inference_gate.py` проверяет **93 именованных условия**
по дампу реально отправленного stdin и по отчёту, который написал сам конвейер.

```
воркер .31, релиз 43d2437f:  total 93, passed 93, failed []
```

Первый прогон гейта дал 92/93: проверка `canary_literal_armed` смотрела в
публичный вид привязки, который литералы не показывает намеренно. Исправлена не
привязка, а гейт — свидетельство берётся из файла привязки и берётся ТОЛЬКО
количество, без значений.

## 5. Офлайн-сверка смысла

`scripts/verify_11e_prompt_semantics.py`, ноль обращений к модели:

```
PRESERVED 171, TRANSPORT_ONLY 6, CONTENT_REMOVED 0, UNKNOWN 0
engineering_lost_vs_api []   transport_leaked []
input_missing_before_inference []
```
