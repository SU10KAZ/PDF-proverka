# LEFT / P — GRSh structural description

## Backbone

- Sources represented: **2** external connections — `Ввод 1 к ТП1` and `Ввод 2 к ТП2`.
- Explicit transformer symbols: **0**. This is an abstraction boundary, not evidence that transformers are absent.
- Input devices: **2**, `QF1` and `QF2`.
- Bus sections: **2**, `ГРЩ1 РП1` and `ГРЩ1 РП2`.
- Inter-section tie: **present**, motorized `QF3`, with `АВР` control shown.
- Metering: present on both input paths; TA/TT, quality-analysis, PW/Wh anchors are visible.
- Grounding: N/PE/PEN and equipotential/grounding paths are present.

```text
ТП1 connection → QF1 → metering → BUS SECTION РП1 → 15 outgoing devices
                         ╲
                          QF3 / АВР (section tie)
                         ╱
ТП2 connection → QF2 → metering → BUS SECTION РП2 → 15 outgoing devices
```

## Section 1 / РП1

Fifteen outgoing positions are visible. Fourteen are functional and one is a free reserve.

1. ВРУ1, input 1 — корпус 1,2
2. ВРУ2, input 1 — встроенные помещения корпуса 1,2
3. ВРУ3, input 1
4. ВРУ4, input 1
5. ВРУа — underground parking
6. ВРУ-ИТП
7. ВРУ-ХЦ — refrigeration-control group
8. ВРУ-АПТ — fire-pump group
9. ВРУ-НСТ — water-supply/pump group
10. ДР1-ХМ1
11. free reserve
12. ХМ1
13. ЩНО — outdoor lighting
14. reserve DHW tanks
15. АУКРМ №1

## Section 2 / РП2

Fifteen outgoing positions are visible. Fourteen are functional and one is a free reserve.

1. ВРУ1, input 2
2. ВРУ2, input 2
3. ВРУ3, input 2
4. ВРУ4, input 2
5. ВРУа
6. ВРУ-ИТП
7. ВРУ-ХЦ
8. ВРУ-АПТ
9. ВРУ-НСТ
10. ДР2-ХМ2
11. free reserve
12. ХМ2
13. ЩНО / auxiliary needs of the transformer substation
14. reserve DHW tanks
15. АУКРМ №2

## Evidence and uncertainties

- The 30 outgoing positions are deterministic vector anchors inside the existing upstream polygon, not newly detected block bounds.
- Branch identity comes from the functional label in the same branch column and from manual raster verification.
- Two labels in the second section are inconsistent with their positional sequence. The graph therefore uses section/slot plus functional identity, not the raw repeated QF text as global identity.
- The source boundary stops at the connections to `ТП1/ТП2`; transformer internals are outside this representation.

The lossless node/edge graph and page-normalized evidence bboxes are in `left_structural_description.json`.
