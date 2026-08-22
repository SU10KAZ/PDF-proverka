# Vector comparison `blk_3f8c724069da4c16ab2ad2b4937b30ea` ↔ `blk_b848efce2b7945d99d79a0e62f91b3d1`

## Вердикт: **NEAR_IDENTICAL**

- Geometry similarity: 1.000 при tolerance 0.50%
- Text similarity: 1.000 (reliable=True)
- Topology similarity: 0.967
- Exact signature equal: False
- Normalized signature equal: False
- Structural signature equal: False

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 0.813 | 0.805 | 12000/12000 | 0.809 |
| 0.25% | 0.969 | 0.964 | 12000/12000 | 0.967 |
| 0.50% | 1.000 | 1.000 | 12000/12000 | 1.000 |
| 1.00% | 1.000 | 1.000 | 12000/12000 | 1.000 |

## Изменения

- Изменены повторяющиеся motifs: 5
- Топология изменилась (similarity=0.967, ветвления 6672 → 6652)

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
