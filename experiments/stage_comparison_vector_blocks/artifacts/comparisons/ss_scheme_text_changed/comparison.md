# Vector comparison `blk_13624dcbe2024b148a2027e1b68e2a0d` ↔ `blk_11f1168f77f84b9283dec8a7bc14e9bb`

## Вердикт: **STRUCTURE_SAME_VALUES_CHANGED**

- Geometry similarity: 0.866 при tolerance 1.00%
- Text similarity: 0.914 (reliable=True)
- Topology similarity: 0.733
- Exact signature equal: False
- Normalized signature equal: False
- Structural signature equal: False

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 0.246 | 0.147 | 710/1190 | 0.197 |
| 0.25% | 0.820 | 0.498 | 710/1190 | 0.659 |
| 0.50% | 1.000 | 0.624 | 710/1190 | 0.812 |
| 1.00% | 1.000 | 0.733 | 710/1190 | 0.866 |

## Изменения

- Текст/значение 2.1 → 1.1
- Текст/значение 2.1.1.1 → 1.1.1.1
- Текст/значение 2.1.1.2 → 1.1.1.2
- Текст/значение 2.1.1.3 → 1.1.1.3
- Текст/значение (2. → (1.
- Текст/значение .6) → .4)
- Текст/значение Контроль → Контроль въезда в
- Добавлено text items: 1.1, 1.1.1.1, 1.1.1.2, 1.1.1.3, (1., .4), Контроль въезда в
- Удалено text items: 2.1, 2.1.1.1, 2.1.1.2, 2.1.1.3, (2., .6), Контроль, въезда, /, выезда в
- Число примитивов: 39 → 159
- Изменены повторяющиеся motifs: 10
- Топология изменилась (similarity=0.733, ветвления 218 → 388)

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
