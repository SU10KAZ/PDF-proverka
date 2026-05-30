# Final winner recommendation — Claude stage 02 block_batch

**Winner:** `baseline_p3`

**Причина:** quality gate subset не пройден: ['blocks_with_findings_aggr_>=_95%_baseline']

**Fallback:** baseline_p2 (более консервативный параллелизм)

## Gate 1 — Full-run (coverage, missing, failed)

- baseline_full pass=True, reasons=[]
- aggressive_full pass=True, reasons=[]

## Gate 2 — Stability

| metric | baseline | aggressive |
|---|---|---|
| unreadable_pct | 0.0 | 0.0 |
| parse_errors | 0 | 0 |
| p95_batch_sec | 244.94 | 279.97 |
| failed_batches | 0 | 0 |

✅ Stability concerns: нет

## Gate 3 — Quality on fixed subset

Applied: yes. Passed: **False**

| criterion | baseline | aggressive | pass |
|---|---|---|---|
| unreadable_aggr_<=_baseline | 0 | 0 | ✅ |
| total_findings_aggr_>=_95%_baseline | 61 | 58 | ✅ |
| blocks_with_findings_aggr_>=_95%_baseline | 45 | 41 | ❌ |
| median_kv_aggr_>=_90%_baseline | 7.5 | 9.0 | ✅ |

## Gate 4 — Speed

- baseline_elapsed=1979.25s, aggressive_elapsed=1790.01s, aggressive_faster=True

## Final decision

- **Production recommendation**: `baseline_p3`
- **Fallback**: baseline_p2 (более консервативный параллелизм)
- **Fastest**: aggressive_p3
- **Best quality (full)**: aggressive_p3
