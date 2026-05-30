# Final winner recommendation — Claude stage 02 block_batch

**Winner:** `aggressive_p3`

**Причина:** Gate 3 (quality subset) инвалидирован из-за rate-limit. По full-run данным aggressive нашёл на 10.7% больше замечаний (186 vs 168) при той же надёжности и на 189s быстрее.

**Fallback:** baseline_p3 (более консервативный, если нужна дополнительная верификация через subset)

---

## Gate 1 — Full-run (coverage, missing, failed)

| метрика | baseline_p3 | aggressive_p3 |
|---------|-------------|---------------|
| coverage | 100% | 100% |
| missing blocks | 0 | 0 |
| failed batches | 0 | 0 |
| extra blocks | 0 | 0 |

✅ Оба прошли Gate 1.

## Gate 2 — Stability

| metric | baseline | aggressive |
|---|---|---|
| unreadable_pct | 0.0 | 0.0 |
| parse_errors | 0 | 0 |
| p95_batch_sec | 244.94 | 279.97 |
| failed_batches | 0 | 0 |

✅ Stability concerns: нет. (p95 у aggressive на 35s выше — в пределах нормы)

## Gate 3 — Quality on fixed subset

**❌ ИНВАЛИДИРОВАН — rate limit**

Оба subset-рана были прерваны после исчерпания лимита Claude API:
- `baseline_p3_subset`: 3/10 батчей запустились (coverage 30%), остальные 7 — rate limit (2s each)
- `aggressive_p3_subset`: все 9 батчей — rate limit (2.4s each, total 7.63s)

Audit trail содержит явное подтверждение: `"result": "You've hit your limit · resets 8pm (Asia/Almaty)"`

**Применено fallback сравнение на full-run данных:**

| критерий | baseline_p3 | aggressive_p3 | результат |
|----------|-------------|---------------|-----------|
| total_findings | 168 | 186 (+10.7%) | ✅ aggressive лучше |
| blocks_with_findings | 132 (61.4%) | 143 (66.5%) | ✅ aggressive лучше |
| findings/100 blocks | 78.14 | 86.51 (+10.7%) | ✅ aggressive лучше |
| unreadable | 0% | 0% | ✅ равны |
| parse_errors | 0 | 0 | ✅ равны |

## Gate 4 — Speed

| метрика | baseline_p3 | aggressive_p3 |
|---------|-------------|---------------|
| total_elapsed | 1979.25s (33.0 min) | 1790.01s (29.8 min) |
| delta | — | **-189s быстрее** |
| avg_batch | 176.64s | 172.91s |
| batches_count | 33 | 31 |

✅ aggressive_p3 быстрее на 189 секунд.

---

## Final decision

**Production recommendation: `aggressive_p3`**

Профиль aggressive: heavy→target 6/max 6, normal→target 10/max 10, light→target 12/max 12, parallelism=3.

Доказательства (full-run, 215 блоков, 20.04.2026):
- +10.7% замечаний (186 vs 168)
- +8.4% блоков с замечаниями (143 vs 132)
- -189s времени (29.8 vs 33.0 мин)
- 100% coverage, 0 failures — обоими профилями

**Pending:** Перепроверить subset-gate после 20:00 (Asia/Almaty) когда rate-limit сбросится.
Команда: `python scripts/run_claude_block_batch_matrix.py --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" --final-comparison --subset-file <path>/fixed_subset_block_ids.json`

**Примечание для раннера:** Runner должен детектировать rate-limit в audit_trail и помечать ран как `rate_limited`, не `failed`. Тогда Gate 3 будет автоматически инвалидирован вместо применения нулевых данных.
