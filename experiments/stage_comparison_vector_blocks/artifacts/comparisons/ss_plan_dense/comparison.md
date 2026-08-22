# Vector comparison `blk_d90a5202d19446f98ac56faa29fa1866` ↔ `blk_1aa5b443eecf469baf0b5eb1ad7f1d90`

## Вердикт: **NEAR_IDENTICAL**

- Geometry similarity: 1.000 при tolerance 1.00%
- Text similarity: 1.000 (reliable=True)
- Topology similarity: 0.996
- Exact signature equal: False
- Normalized signature equal: False
- Structural signature equal: False

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 0.262 | 0.262 | 12000/12000 | 0.262 |
| 0.25% | 0.592 | 0.589 | 12000/12000 | 0.590 |
| 0.50% | 0.930 | 0.918 | 12000/12000 | 0.924 |
| 1.00% | 1.000 | 1.000 | 12000/12000 | 1.000 |

## Изменения

- Изменены повторяющиеся motifs: 81

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
