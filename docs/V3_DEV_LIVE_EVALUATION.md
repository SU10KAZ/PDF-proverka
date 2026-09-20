# V3 DEV Live Evaluation

Дата отчёта (UTC): 2026-09-20T21:27:37.603380+00:00  
Dataset split: **DEV only**  
Validation: **НЕ открыт**  
Holdout: **НЕ открыт**

## 1. Run identity

| Поле | Значение |
| --- | --- |
| run_id | `73757e8b0c404316a8630c28e05444a6` |
| pair_id | `p290a06df79` |
| session_id | `e6fc8a2725eb4a67` |
| section | ИОС2.1 «Водоснабжение» |
| status | **FAILED** |
| reason_code | `v3_cancelled` |
| cancel_reason | `CANCELLED_MODEL_QUOTA_SWITCH` |
| cancelled_by | operator (Fable live verification) по решению пользователя |
| diagnostic_only | `True` |
| engine | `projectchange_v3` `3.4.0` |
| model | `gpt-6-astra` |
| reasoning | `xhigh` |
| release_id (прогон) | `ui-real-fd47c573-v3inferenceready` |
| release_commit (прогон) | `fd47c573c24affb6fa67856f557e3f216466724c` |
| freeze | `/home/coder/auditmanager/corpus-audits/20260921_v3_dev_live_ios21_freeze_73757e8b` |
| freeze_manifest_sha256 | `54309017f73d9c4bbb1237b116ba5c15734eb4b4ffc7794425571785347d55e2` |

**PROVENANCE (критично):** этот результат получен на **старом** production release `ui-real-fd47c573-v3inferenceready`. После выкладки operational fixes **нельзя** переатрибутировать этот run новому release.

## 2. Experiment integrity

| Правило | Статус |
| --- | --- |
| Дождаться terminal state | PASS (`FAILED`) |
| Не перезапускать анализ автоматически | PASS |
| FREEZE до Human Mapping | **PASS** |
| Human Mapping открыт до freeze | NO |
| Validation/holdout | NO / NO |
| Результат опубликован | **NO** (`diagnostic_only=true`, «не используется») |
| Final V3 cards | **отсутствуют** |

## 3. Source preparation

Источник упакован в `project_change_v3/source` (freeze checksum inventory: 273 файла, ~64.4 МБ).  
`source_packaging_version`: `projectchange_v3_source_pack/2`.  
Structure sha256: `26462a3506065674c1069dcf80776851d402d47c12551d0402d69024a793d86e`.

Страницы/блоки (по заявке эксперимента): 115 pages / 182 blocks — подтверждаются подготовкой source pack; точные PDF sha A/B см. `artifacts/SOURCE_MANIFEST.json` в freeze.

## 4. Mapper results

Mapper **завершён успешно** (1 model call, provider_ok=true).

- regions: **14** (A-R001…A-R014)
- mapper tokens (input+output): **291519**
- mapping complexity по page-cardinality регионов (эвристика, не human truth):

| Тип | Count |
| --- | ---: |
| 1→1 | 3 |
| 1→N | 1 |
| N→1 | 0 |
| N↔N | 10 |

Артефакт: `project_change_v3_semantic_map.json` (заморожен).

## 5. Miner results

| Region | Status | Tokens (in+out) | Notes |
| --- | --- | ---: | --- |
| A-R001 | ACCEPTED | 50617 | attempt 1 |
| A-R002 | ACCEPTED | 46365 | attempt 1 |
| A-R003 | ACCEPTED | 105644 | attempt 1 |
| A-R004 | PROVIDER_FAILED / interrupted | 0 | Code Mode host disabled; затем cancel |
| A-R005…A-R014 | NOT STARTED | — | после cancel |

Последний успешный checkpoint: **Mapper + Miner A-R001/002/003**.  
Dedup / final cards: **NOT REACHED**.

Побочный provider error на A-R004: `Code Mode is unavailable because code-mode host is disabled`.  
Операторский cancel: `CANCELLED_MODEL_QUOTA_SWITCH` (квота gpt-6-astra → перевод на Claude Opus).

## 6. Human Mapping comparison

**BLOCKED для card-level comparison.**

Причина: V3 final cards не опубликованы (`diagnostic_only=true`). Сравнивать Human Mapping с несуществующим/запрещённым к использованию выводом — загрязнение протокола.

После FREEZE=PASS Human Mapping **разрешён к открытию** для инвентаризации истины, но без TP/FP матчинга к V3 cards этого run.

HUMAN_TOTAL / per-change FOUND/MISSED: **не считались против V3 cards** (нет cards).

## 7. Precision / Recall / misses

| Метрика | Значение |
| --- | --- |
| HUMAN_TOTAL | N/A (card compare blocked) |
| V3_TOTAL | **0 published cards** |
| TRUE_POSITIVE | N/A |
| FALSE_POSITIVE | N/A |
| MISSED | N/A |
| PARTIAL_MATCH | N/A |
| DUPLICATES | N/A |
| PRECISION | N/A |
| RECALL | N/A |

**Limitation:** evaluation contract требует опубликованные final cards. Частичные ACCEPTED miner attempts без published cards **не** считаются V3_TOTAL.

## 8. Mapping complexity: 1→1 / 1→N / N→1 / N↔N

См. эвристику Mapper (§4). Качество Miner на many-to-many **не измерено** (прогон оборван на A-R004; сложные регионы A-R005/A-R006/A-R014 не майнились).

## 9. Evidence quality

Для published cards: **N/A** (нет cards).  
Для freeze: source rasters/block crops присутствуют в source pack; evidence chain final cards не собрана.

## 10. Token/cost profile

Факт (не проекция):

| Метрика | Значение |
| --- | ---: |
| TOTAL_TOKENS | **494145** |
| MAPPER_TOKENS | **291519** |
| MINER_TOKENS | **202626** |
| OTHER_TOKENS | **0** |
| MAX_REGION_TOKENS | **105644** |
| AVG_REGION_TOKENS | **50656.5** |
| TOTAL_MODEL_CALLS | **5** (usage у 4; без usage: 1) |
| Hard limit | 8_000_000 |
| Wall-clock | 0:32:17.587756 (2026-09-20T20:46:45.942042+00:00 → 2026-09-20T21:19:03.529798+00:00) |

Повторные вызовы: **нет** (все miner attempts = 1; A-R004 fail без retry usage).

## 11. Error taxonomy

| Событие | Категория | Пояснение |
| --- | --- | --- |
| Cancel quota switch | OPERATIONAL / EXTERNAL_QUOTA | Оператор остановил run для смены модели |
| A-R004 provider fail | UNKNOWN / PROVIDER_TRANSPORT | Code Mode host disabled |
| Missing final cards | SOURCE_PREPARATION? нет — PIPELINE_ABORT | Dedup/cards не достигнуты |
| Quality FP/FN | N/A | Нет published output |

Новые quality-дефекты Mapper/Miner **не классифицировались** — нет честной card-level картины.

## 12. Operational defects

Известные 4 UI/API дефекта (исправления уже в ветке, ещё не в production release на момент run):

1. running/failed V3 не на вкладке «Изменения проекта»
2. «Остановить анализ» для V3
3. UI/API stage + model call count
4. ложный «источники изменились»

Они **не менялись** во время этого live run (рестарт/выкладка до завершения запрещены протоколом). Backend был перезапущен позже (00:20 MSK 21.09) уже после terminal FAILED.

## 13. Conclusion for DEV only

1. Live DEV experiment **не доказал** качество V3 на ИОС2.1: terminal state = FAILED, cards unpublished.  
2. V3 **доказал операционно** на этой паре: source pack + Mapper 14 regions + частичный Miner (3/14) + token accounting + fail-closed без legacy.  
3. Честный next gate: **ещё DEV** — новый live run на выбранной модели (после quota switch), **после** выкладки operational fixes и smoke.  
4. Validation/holdout **не открывать**.

FREEZE_BEFORE_TRUTH: PASS  
HUMAN_MAPPING_OPENED_AFTER_FREEZE: YES (разрешено; card-compare не выполнялся)  
DEV_EVALUATION: **BLOCKED** (для precision/recall) / **PARTIAL** (для run forensics + tokens + mapper)


---

## 14. Post-evaluation operational release (после freeze)

Дата (UTC): 2026-09-20T21:34:23.075283+00:00

| Поле | Значение |
| --- | --- |
| RELEASE | `ui-real-a475a6e8-v3opfixes` |
| overlay commit | `a475a6e8be4614834c2a4efa10ea9809d3890a28` |
| base release | `ui-real-fd47c573-v3inferenceready` |
| commits (не squashed) | `21bae3e3`, `634fad35`, `a475a6e8` |
| prompts/model/mapper/miner/dedup | **не менялись** |
| inference after deploy | `ALLOW_INFERENCE=0` (kill_switch; квота/смена модели) |

### Smoke

| Check | Result | Notes |
| --- | --- | --- |
| Unit `test_run_control.py` | **10 passed** | cancel + stage/model_calls без live model |
| FALSE_STALE на FAILED DEV run | **PASS** | `published_run_is_stale(e6fc…, p290…)=False` |
| FAILED visibility (state) | **PASS** | status=FAILED, diagnostic_only, model_calls=4 |
| Live cancel/progress на новом run | **PARTIAL / NOT RUN** | inference kill_switch после quota cancel; новый live inference не запускался |
| АР1 sealed UI_DATA | **PASS** | sha match, 25 regions |
| ИОС4.2 sealed UI_DATA | **PASS** | sha match, 13 regions |
| Semantic map files in release fixtures dir | **PREEXISTING GAP** | `PAIR_*_SEMANTIC_MAP.json` отсутствуют уже в base `fd47c573` (не регрессия opfixes) |

### Provenance reminder

Live DEV run `73757e8b…` **не** получен на `ui-real-a475a6e8-v3opfixes`.
