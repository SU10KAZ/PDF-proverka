# Vector comparison `blk_b211a2e9337f4919a1778e0cff077036` ↔ `blk_73badd6d4cb245f0a6938b14d5fd3f8b`

## Вердикт: **IDENTICAL**

- Geometry similarity: 1.000 при tolerance 0.10%
- Text similarity: 1.000 (reliable=True)
- Topology similarity: 1.000
- Exact signature equal: True
- Normalized signature equal: True
- Structural signature equal: True

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 1.000 | 1.000 | 45/45 | 1.000 |
| 0.25% | 1.000 | 1.000 | 45/45 | 1.000 |
| 0.50% | 1.000 | 1.000 | 45/45 | 1.000 |
| 1.00% | 1.000 | 1.000 | 45/45 | 1.000 |

## Изменения

- Детерминированных изменений не найдено.

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
