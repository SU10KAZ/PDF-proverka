# Vector comparison `blk_36924229bfbd4815a97d05574630c024` ↔ `blk_82b6b709d49f4212a51406e6718d4cc4`

## Вердикт: **NEAR_IDENTICAL**

- Geometry similarity: 1.000 при tolerance 0.25%
- Text similarity: 1.000 (reliable=True)
- Topology similarity: 0.999
- Exact signature equal: False
- Normalized signature equal: False
- Structural signature equal: False

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 0.915 | 0.913 | 12000/12000 | 0.914 |
| 0.25% | 1.000 | 1.000 | 12000/12000 | 1.000 |
| 0.50% | 1.000 | 1.000 | 12000/12000 | 1.000 |
| 1.00% | 1.000 | 1.000 | 12000/12000 | 1.000 |

## Изменения

- Число примитивов: 14800 → 14799
- Изменены повторяющиеся motifs: 40

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
