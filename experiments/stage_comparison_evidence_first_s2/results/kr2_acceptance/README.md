# Acceptance — evidence_first_s2_fallback на КР2

**Дата:** 2026-05-29
**Коммиты:** `d4e0c34` (фича) → `8d4c295` (acceptance fixes)
**Статус:** ✅ принято; controlled rollout — см. [../../ROLLOUT.md](../../ROLLOUT.md)

Это зафиксированные доказательства приёмки fallback-стратегии для больших
enriched MD пар. Раньше лежали в `/tmp/kr2-research/` (ненадёжно) — перенесены
сюда.

## Проект

| | |
|---|---|
| Проект | `АА_БЭ-03-ДС3-КР2` (Балчуг Эстейт, КР) |
| session | `ba413a93c5754f6c` |
| pair | `p2ef68719` |
| left (ДС3) | 586 081 симв. |
| right (4.2) | 279 072 симв. |
| **сумма** | **865 153** > лимита **600 000** → `too_large` |

## Главная метрика — confirmed_unique_changes (НЕ raw)

Research-прогон 3 стратегий с ручной адъюдикацией по 7 критериям:

| Стратегия | raw | **confirmed_unique** |
|---|---:|---:|
| S1 naive full (поднять лимит, 865K одним куском) | 7 | 7 |
| S3 compact single-pass | 9 | 8 |
| **S2 scope-aware section split + evidence verification** | 38 | **13** |

Выходы стратегий: [strategy_comparison/](strategy_comparison/)
(`s1_naive_full.json`, `s3_compact.json`, `s2_chunk_01..05.json`).

## Real Opus shadow run

Живой прогон production-кода (`claude -p --model opus`) на реальной паре.

| Метрика | Значение |
|---|---|
| status | `done` |
| strategy | `evidence_first_s2_fallback` |
| duration | **831 s** |
| chunks | 5 / 5 done |
| raw changes | **63** (9 deterministic + 54 LLM) |
| dropped ungrounded | 0 (все evidence подтверждены против raw MD) |
| после acceptance-фиксов | **55** (5 det + 54 llm − 4 dups) |
| stamp_changed | 5 → **1** (global-singleton collapse) |
| cross-section факт | **W8/F150 → W6/F200** найден через shared global header |

Артефакты:
- [shadow_result.json](shadow_result.json) — сырой результат живого прогона (63, под старым merge).
- [shadow_result_postdedup.json](shadow_result_postdedup.json) — те же LLM-изменения, передедуплено фиксированным кодом `8d4c295` (55).
- [shadow_run.log](shadow_run.log) — лог прогона (831s, статусы, diagnostics).
- [comparison_result.too_large.backup.json](comparison_result.too_large.backup.json) — исходное `too_large`-состояние пары (доказательство non-destructive: после прогона восстановлено).
- [metrics.json](metrics.json) — машиночитаемая сводка.

## Что доказано

1. **Контракт.** Fallback отдаёт валидный `comparison_result.json` со
   `status=done`, `strategy`, `changes`, `diagnostics`, `input_stats` —
   формат совместим с downstream (unified_findings/UI).
2. **Grounding.** Все 63 LLM/детерминированных изменения подтверждены цитатой в
   исходном enriched MD (exact / token-overlap). 0 ungrounded.
3. **Recovery.** Восстановлен per-sheet структурный diff (отметки Корпуса 4,
   толщины плит 200→220, добавленные/удалённые листы), который naive/compact
   теряют, + cross-section материальное изменение бетона через shared header.
4. **Batch preflight.** При включённом флаге `too_large` идёт в run с
   `analysis_strategy=evidence_first_s2_fallback`, а не `skip_too_large`.
5. **Non-destructive.** Production `comparison_result.json` пары восстановлен в
   `too_large`; флаг по умолчанию OFF.

## Воспроизведение

Скрипты: [../../scripts/](../../scripts/).

```bash
# 1. shadow run (требует claude CLI + enriched MD пары на диске; ~14 мин, Opus)
python experiments/stage_comparison_evidence_first_s2/scripts/shadow_run.py

# 2. верификация контракта + grounding по текущему comparison_result.json
python experiments/stage_comparison_evidence_first_s2/scripts/verify_shadow.py
```

`system_prompt_snapshot.txt` — снимок `enriched_comparison.SYSTEM_PROMPT` на
момент приёмки (для диффа при будущих изменениях промпта).

Большие промежуточные MD (выровненные чанки s2_*, compact s3_*) не коммитятся —
регенерируются из enriched MD пары детерминированным split'ом
(`evidence_first_fallback.scope_aware_section_split`).
