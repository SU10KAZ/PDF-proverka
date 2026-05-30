# Resolution recommendation

- Baseline: **r800**
- Final production recommendation: **r800**
- Safe fallback: **r800**

## 1. Code audit crop semantics

См. `crop_semantics_report.md`. Итог:
- Production crop идёт через `blocks.py:download_and_convert()` с `TARGET_DPI=100` и `MIN_LONG_SIDE_PX=800` как floor для длинной стороны.
- Верхнего clamp на длинную сторону PNG в non-compact режиме нет; `TARGET_LONG_SIDE_PX=1500` — legacy.
- Ось эксперимента — `MIN_LONG_SIDE_PX`. Всё остальное зафиксировано.

## 2. Crop stats

| profile | blocks | long avg/p95/max | size_kb avg/p95/max | risk h/n/l |
|---|---|---|---|---|
| r800 | 215 | 842.7/988.3/2629 | 44.5/69.4/605.6 | 11/151/53 |
| r1000 | 215 | 1022.6/1000.0/2629 | 56.1/91.6/605.6 | 11/151/53 |
| r1200 | 215 | 1216.5/1200.0/2629 | 68.4/110.6/605.6 | 11/151/53 |

## 3. Subset phase gate decision

- **r1000**: hard_passed=False, quality_passed=False, batch_cost_ok=True, overall=False
  - hard reasons: ['coverage=76.67% < 100', 'missing=14']
  - failed quality criteria: ['findings_>=_105%_base', 'median_kv_>=_110%_base', 'empty_kv_<=_80%_base', 'empty_summary_<=_80%_base']
- **r1200**: hard_passed=False, quality_passed=True, batch_cost_ok=False, overall=False
  - hard reasons: ['coverage=81.67% < 100', 'missing=11']
  - failed quality criteria: ['median_kv_>=_110%_base', 'empty_kv_<=_80%_base', 'empty_summary_<=_80%_base']

**Candidate selection:** Ни один candidate не прошёл subset quality gate

## 4. Full validation

Full validation не запускалась (либо candidate не прошёл subset gate, либо dry-run).

## 5. Final decision

- **Recommended block resolution (MIN_LONG_SIDE_PX):** r800
- **Safe fallback:** r800

### Notes / caveats

- Subset gate никто не прошёл — baseline сохранён.
