# Vector comparison `blk_17e54c2231764e31a50c9d0562062595` ↔ `blk_d3ec638f07c74c30a7c1e377d31c3cb2`

## Вердикт: **NEAR_IDENTICAL**

- Geometry similarity: 0.997 при tolerance 1.00%
- Text similarity: 0.929 (reliable=True)
- Topology similarity: 0.861
- Exact signature equal: False
- Normalized signature equal: False
- Structural signature equal: False

## Эксперимент tolerances

| Tolerance | Left coverage | Right coverage | Used L/R | Similarity |
|---:|---:|---:|---:|---:|
| 0.10% | 0.014 | 0.015 | 1593/1583 | 0.014 |
| 0.25% | 0.249 | 0.266 | 1593/1583 | 0.258 |
| 0.50% | 0.816 | 0.847 | 1593/1583 | 0.832 |
| 1.00% | 0.993 | 1.000 | 1593/1583 | 0.997 |

## Изменения

- Текст/значение видеокамера → RVi
- Текст/значение под → видеокаме
- Текст/значение витую → вит
- Текст/значение "Sto → "
- Текст/значение протяжко → протя
- Текст/значение под → по
- Добавлено text items: 1, Монтажная, коробка, RVi, 2BM, видеокаме, вит, ", протя, по
- Удалено text items: видеокамера, под, витую, "Sto, протяжко, н
- Число примитивов: 11 → 7
- Топология изменилась (similarity=0.861, ветвления 797 → 792)

## Ограничения

- Segment coverage is order- and PDF-path-packaging-independent and uses block-normalized geometry; it does not use affine warping.
- Dense blocks are compared on the longest deterministic segment sample when the explicit cap is reached.
- A geometric X-crossing is not promoted to a connection without junction evidence.
- Undecodable embedded-font text is reported and excluded from status selection; OCR is not used.
- Status thresholds are research thresholds evaluated on this benchmark, not production policy.
