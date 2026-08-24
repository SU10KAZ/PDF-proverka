# RIGHT / RD — GRSh structural description

## Backbone

- Sources represented: **2**, with explicit transformer symbols `Т1` and `Т2`.
- Input devices: **2**, `QF1` and `QF2`.
- Bus sections: **2**, `РП1` and `РП2`.
- Inter-section tie: **present**, `QS1`, 1600 A, with `SA / Секц.` control representation.
- Metering: three CT groups, PW, Wh, and multimeter functions are explicitly laid out on each input path.
- Compensation: `АУКРМ-1` and `АУКРМ-2` are drawn below the buses and connected back to their sections.
- Grounding: N/PE rails, ГЗШ/equipotential connections, and transformer grounding are explicit.

```text
Т1 → busduct → QF1 → metering → BUS SECTION РП1 → 13 outgoing devices
                         ╲
                          QS1 (section tie)
                         ╱
Т2 → busduct → QF2 → metering → BUS SECTION РП2 → 14 outgoing devices
```

## Section 1 / РП1

Thirteen active outgoing devices are visible:

1. ДР1-ХМ1
2. ХМ1
3. АУКРМ-1
4. ВРУ4
5. ВРУ3
6. ВРУ1
7. ВРУ2
8. ВРУа
9. ВРУ-ИТП
10. ШУ-ХЦ
11. ШУ-АПТ, reserve input
12. ШУ-ХП, working input
13. ЩНО

## Section 2 / РП2

Fourteen outgoing devices are visible:

1. ВРУ4
2. ВРУ3
3. likely ВРУ1 — the feeder anchor repeats ВРУ3, so identity is uncertain
4. ВРУ2
5. ВРУа
6. ВРУ-ИТП
7. ШУ-ХЦ
8. ШУ-АПТ, working input
9. ШУ-ХП, reserve input
10. АУКРМ-2
11. ХМ2
12. ДР2-ХМ2
13. reserve DHW tanks / ЭБ-ГВС
14. `ЯСН ТП` by terminal anchor, but `ЭБ-ГВС` by feeder anchor — identity uncertain

## Evidence and uncertainties

- The `13+14` device positions come from QF vector anchors inside the existing upstream polygon.
- `Т1/Т2`, `QF1/QF2`, `QS1`, the two long busbars, and the branch stems were verified on the rendered raster.
- The exact automatic-control semantics of `QS1` are not inferred from `SA` alone.
- Two outgoing identities are explicitly capped as uncertain because their local vector anchors conflict. They are not forced into the comparison.

The lossless node/edge graph and page-normalized evidence bboxes are in `right_structural_description.json`.
