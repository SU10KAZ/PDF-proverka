# mine — набор трудных пар подготовленных графических блоков (VECTOR 0.3)

Пар: **33** (из них помечены `uncertain` и НЕ являются эталоном: 2).  Ось: кросс-ревизионная (один документ, соседние версии). Разметку делал ОДИН размечающий.

Каждая пара подтверждена глазами по `artifacts/mine_crops/<pair_id>.png` (A | B после регистрации | наложение: красное — только A, синее — только B).

**Проверка «файл сам с собой»**: для каждой пары сверены sha256 обоих PDF, совпадений нет (см. поля `side_a.sha256` / `side_b.sha256` в `mine_pairs.json`).


## Сводка по классам

| класс | пар |
|---|---|
| `with_labels` | 22 |
| `dense_block` | 9 |
| `object_added` | 9 |
| `rotated_page` | 5 |
| `block_moved` | 4 |
| `object_removed` | 4 |
| `no_labels` | 3 |
| `table_like` | 3 |
| `bbox_boundary_artifact` | 3 |
| `text_only_change` | 3 |
| `table_only_change` | 3 |
| `raster_graphics` | 3 |
| `object_moved` | 3 |
| `unchanged_control` | 2 |
| `different_packaging` | 2 |
| `few_labels` | 2 |
| `small_local_change` | 2 |
| `block_scope_change` | 2 |
| `uncertain` | 2 |
| `garbled_text_layer` | 1 |
| `object_moved_label` | 1 |
| `sparse_block` | 1 |
| `occluding_fill` | 1 |
| `hatch_noise` | 1 |
| `block_match_failure` | 1 |

## Пары

| # | pair_id | классы | вердикт | объектов | дисц. | документ | версии | стр. A/B | /Rotate | сегментов A/B | diff (равный масштаб) | уверенность |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `AR-b38a7dbc` | unchanged_control, dense_block, with_labels | **NO_GRAPHIC_CHANGE** | 0 | AR | 13АВ-РД-АР1.2-К5 | v003→v004 | 11/11 | 0/0 | 32312/32312 | 0.0 | high |
| 2 | `TX-95031dbd` | unchanged_control, no_labels, rotated_page | **NO_GRAPHIC_CHANGE** | 0 | TX | 13АВ-РД-ТХ2-К1 V1 | v001→v002 | 13/13 | 270/270 | 814/814 | 0.0 | high |
| 3 | `VK-b71afe81` | different_packaging, rotated_page, dense_block, garbled_text_layer | **NO_GRAPHIC_CHANGE** | 0 | VK | 13АВ-РД-ВК.КВ-К4_V1 | v001→v002 | 14/14 | 90/90 | 16205/16205 | 0.0 | high |
| 4 | `SS-6fc75e05` | different_packaging, with_labels, table_like | **NO_GRAPHIC_CHANGE** | 0 | SS | 13АВ-РД-АК-К6 (Книга 1) | v001→v002 | 13/13 | 0/0 | 5845/12921 | 0.000844 | high |
| 5 | `KJ-b5c82f7c` | bbox_boundary_artifact, with_labels | **NO_GRAPHIC_CHANGE** | 0 | KJ | 13АВ-РД-КЖ5.22-28.1-К1 | v001→v002 | 32/33 | 0/0 | 7253/7254 | 0.000926 | high |
| 6 | `AR-46f62636` | bbox_boundary_artifact, dense_block, with_labels | **NO_GRAPHIC_CHANGE** | 0 | AR | 13АВ-РД-АР1.2-К5 | v003→v004 | 7/7 | 0/0 | 30876/30877 | 0.002542 | high |
| 7 | `SS-392b7bd3` | bbox_boundary_artifact, with_labels | **NO_GRAPHIC_CHANGE** | 0 | SS | 13АВ-РД-АК-К3 (Книга 1) | v001→v002 | 12/12 | 0/0 | 860/862 | 0.004579 | high |
| 8 | `EOM-36de2ce2` | block_moved, rotated_page, few_labels | **NO_GRAPHIC_CHANGE** | 0 | EOM | 13АВ-РД-ЭО-К3 | v001→v002 | 28/29 | 90/0 | 2207/2207 | 0.086698 | high |
| 9 | `KJ-548814a7` | block_moved, few_labels | **NO_GRAPHIC_CHANGE** | 0 | KJ | 13АВ-РД-КЖ5.30-31.1-К2 | v001→v002 | 9/10 | 0/0 | ?/? | 0.030982 | high |
| 10 | `EOM-3306e907` | block_moved, rotated_page | **NO_GRAPHIC_CHANGE** | 0 | EOM | 13АВ-РД-ЭО-К3 | v001→v002 | 34/35 | 90/0 | ?/? | 0.063601 | medium |
| 11 | `SS-982f7f30` | block_moved, with_labels, table_like | **NO_GRAPHIC_CHANGE** | 0 | SS | 13АВ-РД-АК-К5 (Книга 2) | v001→v002 | 6/6 | 0/0 | ?/? | 0.009744 | medium |
| 12 | `KJ-25717577` | text_only_change, table_only_change, with_labels | **NO_GRAPHIC_CHANGE** | 0 | KJ | 13АВ-РД-КЖ5.22-28.1-К1 | v001→v002 | 14/15 | 0/0 | ?/? | 0.009788 | high |
| 13 | `SS-76640e11` | text_only_change, with_labels | **NO_GRAPHIC_CHANGE** | 0 | SS | 13АВ-РД-АПЗ.АПС-К3 V1 | v001→v002 | 20/20 | 0/0 | ?/? | 0.007274 | high |
| 14 | `EOM-0c86dfde` | text_only_change, object_moved_label, dense_block | **NO_GRAPHIC_CHANGE** | 0 | EOM | 13АВ-РД-ЭМ-К1 | v001→v002 | 44/44 | 0/0 | ?/? | 0.003246 | high |
| 15 | `EOM-c50e2170` | object_removed, dense_block, with_labels | **GRAPHIC_CHANGE** | 30 | EOM | 13АВ-РД-ЭМ-К6 | v001→v002 | 21/22 | 0/0 | 8027/4502 | 0.024668 | high |
| 16 | `AR-dbfd82b8` | object_removed, with_labels, table_like | **GRAPHIC_CHANGE** | 1 | AR | 13АВ-РД-АР4.2-К6 | v001→v002 | 7/7 | 0/0 | 8895/7829 | 0.028062 | high |
| 17 | `VK-56115717` | object_removed, with_labels | **GRAPHIC_CHANGE** | 1 | VK | 13АВ-РД-ВК2-К3 V1 | v001→v002 | 16/17 | 0/0 | ?/? | 0.027877 | high |
| 18 | `AR-441907a2` | object_removed, with_labels | **GRAPHIC_CHANGE** | 2 | AR | 13АВ-РД-АР1.1-К4 | v002→v003 | 20/20 | 0/0 | ?/? | 0.041525 | high |
| 19 | `SS-c7aa8d26` | object_added, with_labels, sparse_block | **GRAPHIC_CHANGE** | 2 | SS | 13АВ-РД-АК-К5 (Книга 2) | v001→v002 | 10/11 | 0/0 | ?/? | 0.045209 | high |
| 20 | `AR-a32b30a6` | object_added, raster_graphics, no_labels | **GRAPHIC_CHANGE** | 1 | AR | 13АВ-РД-АР3-К3 | v001→v002 | 10/11 | 0/0 | 1/0 | 0.069078 | high |
| 21 | `EOM-7fef43a3` | object_added, with_labels | **GRAPHIC_CHANGE** | 2 | EOM | 13АВ-РД-ЭМ-К4 | v001→v002 | 14/16 | 0/0 | ?/? | 0.012027 | high |
| 22 | `EOM-46355862` | object_added, dense_block, with_labels | **GRAPHIC_CHANGE** | 15 | EOM | 13АВ-РД-ЭМ2-ПА V1 | v002→v003 | 9/9 | 0/0 | ?/? | 0.02593 | medium |
| 23 | `SS-a369f492` | object_added, object_moved, raster_graphics, with_labels | **GRAPHIC_CHANGE** | 2 | SS | 13АВ-РД-АК-К5 (Книга 1) | v001→v002 | 24/24 | 0/0 | 4876/6678 | 0.23704 | high |
| 24 | `VK-148ffe6c` | object_added, object_moved, rotated_page, dense_block | **GRAPHIC_CHANGE** | 3 | VK | 13АВ-РД-ВК.КВ-К4_V1 | v001→v002 | 9/9 | 90/90 | 106552/115081 | 0.081341 | medium |
| 25 | `OV-93cc012f` | object_added, object_moved, with_labels | **GRAPHIC_CHANGE** | 4 | OV | 13АВ-РД-ОВ1.1-К2 V1 | v001→v002 | 28/29 | 0/0 | 9880/9620 | 0.014521 | medium |
| 26 | `AR-55eda7fb` | small_local_change, table_only_change, with_labels | **GRAPHIC_CHANGE** | 1 | AR | 13АВ-РД-АР1.1-К2 | v002→v003 | 21/21 | 0/0 | 4315/4412 | 0.005366 | high |
| 27 | `AR-577a293f` | small_local_change, table_only_change, with_labels | **GRAPHIC_CHANGE** | 3 | AR | 13АВ-РД-АР4.2-К4 | v001→v002 | 22/22 | 0/0 | ?/? | 0.036151 | medium |
| 28 | `EOM-14558cda` | object_added, occluding_fill, with_labels | **GRAPHIC_CHANGE** | 1 | EOM | 13АВ-РД-ЭМ-К5 | v001→v002 | 10/11 | 0/0 | 7932/7767 | 0.139662 | medium |
| 29 | `AR-5acaab0e` | object_added, dense_block, hatch_noise, with_labels | **GRAPHIC_CHANGE** | 3 | AR | 13АВ-РД-АР3.2-ПА | v001→v002 | 7/8 | 0/0 | 111161/111000 | 0.037897 | medium |
| 30 | `AR-490254e9` | block_scope_change, with_labels | **NOT_COMPARABLE** | — | AR | 13АВ-РД-АР1.2-К4 | v002→v003 | 9/9 | 0/0 | 466/4398 | 0.661861 | high |
| 31 | `OV-2cc2a382` | block_scope_change, raster_graphics, no_labels | **NOT_COMPARABLE** | — | OV | 13АВ-РД-ОВ2-К1 V1 | v001→v002 | 145/112 | 0/0 | 0/128 | 2.344339 | high |
| 32 | `EOM-1db297d2` | uncertain, block_match_failure | **UNCERTAIN** | — | EOM | 13АВ-РД-ЭО-К3 | v001→v002 | 13/13 | 0/0 | ?/? | 0.049518 | high |
| 33 | `GP-6bc1c029` | uncertain, dense_block | **UNCERTAIN** | — | GP | 13АВ-РД-ГП2 | v001→v002 | 6/6 | 0/0 | 378441/314651 | 0.015172 | medium |

## Что именно изменилось (глазами)

### `AR-b38a7dbc` — unchanged_control, dense_block, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Плотный план (32 312 сегментов, 487 текстовых строк) из двух РАЗНЫХ файлов PDF; любой шум компаратора здесь — ложное срабатывание.
* **Ожидаемое (человек):** Изменений нет: наложение даёт 0 пикселей несовпадения (diff=0.0), число сегментов и текстовых строк совпадает точно.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К5/versions/v003/02_work/document.pdf` стр. 11 (page_index 10), блок `7THC-DUYN-A69`, coords_px [311.0, 109.0, 4377.0, 6767.0], page_px [9932, 7015], /Rotate 0, sha256 `3230b0fe3aa3dbf1…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К5/versions/v004/02_work/document.pdf` стр. 11 (page_index 10), блок `blk_4ae65e0a6a274b17ac265af9f82d949f`, coords_px [306.0, 98.0, 4307.0, 6802.0], page_px [9933, 7016], /Rotate 0, sha256 `33b518ab02fc0fa5…`
* **Картинка:** `artifacts/mine_crops/AR-b38a7dbc.png`

### `TX-95031dbd` — unchanged_control, no_labels, rotated_page → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Страница повёрнута (/Rotate 270), текст выведен кривыми: в текстовом слое 0 строк, значит текстового якоря нет вообще.
* **Ожидаемое (человек):** Изменений нет: две схемы («Цепь освещения 220 В» и «Силовая цепь 380 В») совпадают попиксельно (diff=0.0).
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/TX/documents/13АВ-РД-ТХ2-К1 V1/versions/v001/02_work/document.pdf` стр. 13 (page_index 12), блок `JAMC-66UX-F6T`, coords_px [2126.0, 1681.0, 2474.0, 2321.0], page_px [3508, 2479], /Rotate 270, sha256 `8d2b370f26257166…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/TX/documents/13АВ-РД-ТХ2-К1 V1/versions/v002/02_work/document.pdf` стр. 13 (page_index 12), блок `blk_d33b098888bb4fa1aa4faad2e467191d`, coords_px [2130.0, 1662.0, 2518.0, 2352.0], page_px [3509, 2480], /Rotate 270, sha256 `4f2617acefbcf006…`
* **Картинка:** `artifacts/mine_crops/TX-95031dbd.png`

### `VK-b71afe81` — different_packaging, rotated_page, dense_block, garbled_text_layer → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Растр совпадает побитно, но текстовый слой извлекается по-разному (битая кодировка шрифта): text_jaccard=0.123 при diff=0.0. Компаратор, опирающийся на текстовый якорь, увидит здесь «изменение».
* **Ожидаемое (человек):** Графика идентична: узлы «Узел гиг. душа» и «Узел кранов с эл. приводом» совпадают попиксельно (diff=0.0, 16 205 сегментов с обеих сторон). Отличается только извлекаемый текст: «Ø161/2"» против «Ø16*1/2"» — дефект кодировки шрифта, не чертежа.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf` стр. 14 (page_index 13), блок `blk_f0f8fd174c204013a6a1f1aa76b719cf`, coords_px [10028.0, 208.0, 11539.0, 3182.0], page_px [13031, 6140], /Rotate 90, sha256 `360626f01c1a0ccc…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v002/02_work/document.pdf` стр. 14 (page_index 13), блок `blk_8d21b6cd7a2e43968fafbed420c3d731`, coords_px [10057.0, 154.0, 11516.0, 3238.0], page_px [13031, 6140], /Rotate 90, sha256 `61d95a84b064f4a3…`
* **Картинка:** `artifacts/mine_crops/VK-b71afe81.png`

### `SS-6fc75e05` — different_packaging, with_labels, table_like → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Тот же штамп записан 5 845 сегментами в одной версии и 12 921 в другой (×2.21) — прямая проверка устойчивости к декомпозиции путей.
* **Ожидаемое (человек):** Штамп тот же самый; видимое отличие — только полоска рамки на границе кропа (0.08 % площади). Число сегментов различается вдвое из-за другой упаковки путей.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К6 (Книга 1)/versions/v001/02_work/document.pdf` стр. 13 (page_index 12), блок `646E-DDXY-XMH`, coords_px [5186.0, 2794.0, 7386.0, 3448.0], page_px [7441, 3508], /Rotate 0, sha256 `b26b4fe5029aabf7…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К6 (Книга 1)/versions/v002/02_work/document.pdf` стр. 13 (page_index 12), блок `9NNN-H4TT-3MF`, coords_px [5197.0, 2792.0, 7389.0, 3450.0], page_px [7441, 3508], /Rotate 0, sha256 `1a6d4800cec64277…`
* **Картинка:** `artifacts/mine_crops/SS-6fc75e05.png`

### `KJ-b5c82f7c` — bbox_boundary_artifact, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Единственная разница — вертикальная линия рамки листа, попавшая в bbox одной версии и не попавшая в другой.
* **Ожидаемое (человек):** Узлы армирования совпадают полностью; в версии B в кроп дополнительно попала линия рамки листа слева.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.22-28.1-К1/versions/v001/02_work/document.pdf` стр. 32 (page_index 31), блок `H3PE-YWLN-GAH`, coords_px [532.0, 2558.0, 8926.0, 4954.0], page_px [9932, 7015], /Rotate 0, sha256 `635096da7e823800…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.22-28.1-К1/versions/v002/02_work/document.pdf` стр. 33 (page_index 32), блок `6GMW-6L4K-699`, coords_px [223.0, 2563.0, 9556.0, 4957.0], page_px [9932, 7015], /Rotate 0, sha256 `0f7f8d33274be9e5…`
* **Картинка:** `artifacts/mine_crops/KJ-b5c82f7c.png`

### `AR-46f62636` — bbox_boundary_artifact, dense_block, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Плотный план 30 876 сегментов; вся «разница» — линии рамки на краю кропа, содержимое совпадает.
* **Ожидаемое (человек):** План этажа не изменился; в A в кроп попала горизонтальная линия рамки сверху, в B — вертикальная слева.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К5/versions/v003/02_work/document.pdf` стр. 7 (page_index 6), блок `9UN7-PPMM-9CG`, coords_px [261.0, 61.0, 3701.0, 6777.0], page_px [9932, 7015], /Rotate 0, sha256 `3230b0fe3aa3dbf1…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К5/versions/v004/02_work/document.pdf` стр. 7 (page_index 6), блок `blk_54ef396e4a0e44e2b5d28a921b4555d5`, coords_px [207.0, 77.0, 3715.0, 6934.0], page_px [9933, 7016], /Rotate 0, sha256 `33b518ab02fc0fa5…`
* **Картинка:** `artifacts/mine_crops/AR-46f62636.png`

### `SS-392b7bd3` — bbox_boundary_artifact, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Схема управления дверьми; наивный детектор объектов увидит «добавленную линию» длиной во всю ширину блока.
* **Ожидаемое (человек):** Схема (EZ1, У1.1, «Входные двери помещения», ПУ-У1) идентична; в версии B сверху добавилась линия рамки листа.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К3 (Книга 1)/versions/v001/02_work/document.pdf` стр. 12 (page_index 11), блок `7H3N-LEAD-JGN`, coords_px [249.0, 79.0, 2467.0, 1405.0], page_px [4962, 3508], /Rotate 0, sha256 `e741091243307743…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К3 (Книга 1)/versions/v002/02_work/document.pdf` стр. 12 (page_index 11), блок `79EK-WRJG-LLX`, coords_px [249.0, 55.0, 2572.0, 1455.0], page_px [4962, 3508], /Rotate 0, sha256 `fd30a236cd49e1d6…`
* **Картинка:** `artifacts/mine_crops/SS-392b7bd3.png`

### `EOM-36de2ce2` — block_moved, rotated_page, few_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Одна и та же схема вычерчена в другом масштабе, попиксельная разница 8.7 % — классический ложноположительный.
* **Ожидаемое (человек):** «Схема размещения фрагмента» та же самая, в версии B крупнее (масштаб блока другой). Сегментов 2 207 и 2 207, текст совпадает.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v001/02_work/document.pdf` стр. 28 (page_index 27), блок `6LCV-AKQM-PYH`, coords_px [3042.0, 1366.0, 4194.0, 2210.0], page_px [4962, 3508], /Rotate 90, sha256 `5778ec55c0a2181f…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v002/02_work/document.pdf` стр. 29 (page_index 28), блок `6KFH-KYVQ-FC6`, coords_px [2860.0, 1379.0, 4248.0, 2228.0], page_px [4962, 3508], /Rotate 0, sha256 `596b41dd408e54f8…`
* **Картинка:** `artifacts/mine_crops/EOM-36de2ce2.png`

### `KJ-548814a7` — block_moved, few_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Тот же чертёж в другом масштабе: сдвигом не совмещается, разница 3.1 %.
* **Ожидаемое (человек):** «Схема отгиба стержня» одна и та же, в версии B вычерчена примерно в 1.25 раза крупнее.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.30-31.1-К2/versions/v001/02_work/document.pdf` стр. 9 (page_index 8), блок `9T4G-4U9G-R39`, coords_px [10915.0, 281.0, 11557.0, 1109.0], page_px [14043, 9932], /Rotate 0, sha256 `e9616839308c3400…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.30-31.1-К2/versions/v002/02_work/document.pdf` стр. 10 (page_index 9), блок `69YQ-6FQ9-9W4`, coords_px [10445.0, 149.0, 11031.0, 969.0], page_px [14043, 9932], /Rotate 0, sha256 `2ca10a3ec82a6f13…`
* **Картинка:** `artifacts/mine_crops/KJ-548814a7.png`

### `EOM-3306e907` — block_moved, rotated_page → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Версии хранят страницу с РАЗНЫМ /Rotate (90 против 0) И в разном масштабе — двойная ловушка систем координат.
* **Ожидаемое (человек):** «Схема размещения фрагмента»: то же изображение, в версии B крупнее; после корректной дерotation обе стороны стоят вертикально.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v001/02_work/document.pdf` стр. 34 (page_index 33), блок `4H4N-QL9J-YVC`, coords_px [3748.0, 1004.0, 4898.0, 2216.0], page_px [4962, 3508], /Rotate 90, sha256 `5778ec55c0a2181f…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v002/02_work/document.pdf` стр. 35 (page_index 34), блок `9UYQ-9EKL-766`, coords_px [3770.0, 1157.0, 4890.0, 2154.0], page_px [4962, 3508], /Rotate 0, sha256 `596b41dd408e54f8…`
* **Картинка:** `artifacts/mine_crops/EOM-3306e907.png`

### `SS-982f7f30` — block_moved, with_labels, table_like → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Штамп совпадает по содержанию, но нарисован в чуть другом масштабе — 1 % площади уходит в невязку.
* **Ожидаемое (человек):** Штамп («Книга 2. Автоматизация и диспетчеризация… Корпус 5») тот же; отличается только масштаб/положение блока.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 2)/versions/v001/02_work/document.pdf` стр. 6 (page_index 5), блок `7J4T-LCQ3-VVC`, coords_px [2714.0, 2772.0, 4890.0, 3446.0], page_px [4962, 3508], /Rotate 0, sha256 `830bd0cedc1a5b76…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 2)/versions/v002/02_work/document.pdf` стр. 6 (page_index 5), блок `blk_6f12e2c1a82e43cf9502a3da88077e25`, coords_px [2710.0, 2792.0, 4910.0, 3453.0], page_px [4963, 3509], /Rotate 0, sha256 `ccee7dbb1446b5e8…`
* **Картинка:** `artifacts/mine_crops/SS-982f7f30.png`

### `KJ-25717577` — text_only_change, table_only_change, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** В таблице штампа изменились только значения ячеек (даты); сетка не тронута. Это работа текстового конвейера, а не графического.
* **Ожидаемое (человек):** Изменились только даты во всех строках штампа: 25.03.26 → 22.05.26. Линии таблицы, подписи и логотип те же.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.22-28.1-К1/versions/v001/02_work/document.pdf` стр. 14 (page_index 13), блок `47TT-A6HM-YMW`, coords_px [7686.0, 6284.0, 9882.0, 6964.0], page_px [9932, 7015], /Rotate 0, sha256 `635096da7e823800…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/KJ/documents/13АВ-РД-КЖ5.22-28.1-К1/versions/v002/02_work/document.pdf` стр. 15 (page_index 14), блок `9KNC-KCCY-HYR`, coords_px [7696.0, 6309.0, 9870.0, 6959.0], page_px [9932, 7015], /Rotate 0, sha256 `0f7f8d33274be9e5…`
* **Картинка:** `artifacts/mine_crops/KJ-25717577.png`

### `SS-76640e11` — text_only_change, with_labels → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Удалён крупный текстовый блок (8 398 пикселей краски) — соблазн объявить «удалён объект», хотя графика плана не менялась.
* **Ожидаемое (человек):** С плана удалён текстовый блок «Примечание:» из трёх пунктов внизу листа. Сам план (стены, оси, оборудование) не изменился.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АПЗ.АПС-К3 V1/versions/v001/02_work/document.pdf` стр. 20 (page_index 19), блок `97WM-79HH-VWA`, coords_px [210.0, 58.0, 4372.0, 4858.0], page_px [7016, 4962], /Rotate 0, sha256 `943f0f5e6e0977d0…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АПЗ.АПС-К3 V1/versions/v002/02_work/document.pdf` стр. 20 (page_index 19), блок `blk_b5462bfeac214c32a7cdbbccc127c811`, coords_px [229.0, 57.0, 4498.0, 4096.0], page_px [7017, 4963], /Rotate 0, sha256 `eb1c679dbc1b5c8e…`
* **Картинка:** `artifacts/mine_crops/SS-76640e11.png`

### `EOM-0c86dfde` — text_only_change, object_moved_label, dense_block → **NO_GRAPHIC_CHANGE**, объектов: 0

* **Чем трудна:** Сдвинута только колонка текстовых марок кабелей; геометрия плана не тронута — прямая проверка правила «подпись не объект».
* **Ожидаемое (человек):** Столбец марок кабелей (K1.2.3n-12, K1.2.15, K1.2.21, K1.2.3n-6, K1.2.24, K1.2.20, …) сдвинут влево относительно плана; сам план тот же.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К1/versions/v001/02_work/document.pdf` стр. 44 (page_index 43), блок `4HHW-CNTA-MNG`, coords_px [599.0, 0.0, 6487.0, 5474.0], page_px [14891, 7016], /Rotate 0, sha256 `6db65011480f1863…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К1/versions/v002/02_work/document.pdf` стр. 44 (page_index 43), блок `4WGC-MYEY-F6Q`, coords_px [561.0, 429.0, 6131.0, 5998.0], page_px [14891, 7016], /Rotate 0, sha256 `511528837547108d…`
* **Картинка:** `artifacts/mine_crops/EOM-0c86dfde.png`

### `EOM-c50e2170` — object_removed, dense_block, with_labels → **GRAPHIC_CHANGE**, объектов: 30

* **Чем трудна:** Удалён целый слой (координационные оси) — сотни примитивов, но семантически это одна правка; счётчик объектов должен не взорваться.
* **Ожидаемое (человек):** В версии B исчезли координационные оси: штрихпунктирные линии и кружки с марками осей по всему периметру плана. Сегментов 8 027 → 4 502.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К6/versions/v001/02_work/document.pdf` стр. 21 (page_index 20), блок `7VCE-3GL3-HPM`, coords_px [211.0, 51.0, 4619.0, 4071.0], page_px [7015, 4961], /Rotate 0, sha256 `a1b87850072be29e…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К6/versions/v002/02_work/document.pdf` стр. 22 (page_index 21), блок `blk_53a84e168a404b6b8b8aac2176a9fe31`, coords_px [237.0, 41.0, 4522.0, 4073.0], page_px [7016, 4961], /Rotate 0, sha256 `5a79afd66dc2feeb…`
* **Картинка:** `artifacts/mine_crops/EOM-c50e2170.png`

### `AR-dbfd82b8` — object_removed, with_labels, table_like → **GRAPHIC_CHANGE**, объектов: 1

* **Чем трудна:** Удалён составной объект (легенда из ~25 строк с образцами штриховок) при полностью неизменной таблице выше.
* **Ожидаемое (человек):** В версии B удалён блок «Условные обозначения» целиком (около 25 строк с образцами штриховок и подписями). Таблица «Ведомость полов 2-го этажа» не изменилась.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР4.2-К6/versions/v001/02_work/document.pdf` стр. 7 (page_index 6), блок `7R7U-73U3-QTT`, coords_px [5649.0, 55.0, 8739.0, 3906.0], page_px [9933, 7016], /Rotate 0, sha256 `cc15fc321500a1b7…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР4.2-К6/versions/v002/02_work/document.pdf` стр. 7 (page_index 6), блок `4TUU-V3XV-4T3`, coords_px [5667.0, 75.0, 8723.0, 2543.0], page_px [9933, 7016], /Rotate 0, sha256 `65d17b09e220083e…`
* **Картинка:** `artifacts/mine_crops/AR-dbfd82b8.png`

### `VK-56115717` — object_removed, with_labels → **GRAPHIC_CHANGE**, объектов: 1

* **Чем трудна:** Удалена целая аксонометрическая схема (~40 поэтажных отводов) — крупнейший вид на листе.
* **Ожидаемое (человек):** В версии B удалена правая схема «Схема системы В2.3 (2 зона)» целиком; левая схема В2.2 осталась. Заодно исчез текст в штампе.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК2-К3 V1/versions/v001/02_work/document.pdf` стр. 16 (page_index 15), блок `3KXN-ADNV-YUD`, coords_px [4815.0, 0.0, 9368.0, 6529.0], page_px [9932, 7015], /Rotate 0, sha256 `299b2783962fa323…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК2-К3 V1/versions/v002/02_work/document.pdf` стр. 17 (page_index 16), блок `blk_b8e90a2981df42259013a305c75d27de`, coords_px [4812.0, 81.0, 8004.0, 6382.0], page_px [9933, 7016], /Rotate 0, sha256 `b95246b3d93bfb91…`
* **Картинка:** `artifacts/mine_crops/VK-56115717.png`

### `AR-441907a2` — object_removed, with_labels → **GRAPHIC_CHANGE**, объектов: 2

* **Чем трудна:** Из листа исчезли и чертёж-разрез, и содержимое штампа — два разных по природе объекта в одной паре.
* **Ожидаемое (человек):** В версии B удалены разрез «Сеч. а–а» в правом верхнем углу и всё содержимое штампа (остались только линии таблицы).
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.1-К4/versions/v002/02_work/document.pdf` стр. 20 (page_index 19), блок `GKWQ-RPPD-TPA`, coords_px [229.0, 47.0, 4911.0, 3435.0], page_px [4961, 3507], /Rotate 0, sha256 `52acb86a91041431…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.1-К4/versions/v003/02_work/document.pdf` стр. 20 (page_index 19), блок `blk_edf62d677a244bb1a2d9e645e3344764`, coords_px [206.0, 42.0, 3041.0, 3441.0], page_px [4961, 3508], /Rotate 0, sha256 `32068e934da15496…`
* **Картинка:** `artifacts/mine_crops/AR-441907a2.png`

### `SS-c7aa8d26` — object_added, with_labels, sparse_block → **GRAPHIC_CHANGE**, объектов: 2

* **Чем трудна:** Разреженный блок: два новых узла добавлены рядом с единственным существующим.
* **Ожидаемое (человек):** В версии B добавлены два узла: «Крепление горизонтальных кабельных трасс…» и «Крепление кабельных трасс в месте изгиба…». Первый узел («вертикальных трасс») не изменился.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 2)/versions/v001/02_work/document.pdf` стр. 10 (page_index 9), блок `JJU3-WQ4G-RAK`, coords_px [6901.0, 4332.0, 8059.0, 6184.0], page_px [9933, 7016], /Rotate 0, sha256 `830bd0cedc1a5b76…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 2)/versions/v002/02_work/document.pdf` стр. 11 (page_index 10), блок `blk_f41b7be6ed314b82928a7027091a1765`, coords_px [7104.0, 4446.0, 9420.0, 6124.0], page_px [9934, 7017], /Rotate 0, sha256 `ccee7dbb1446b5e8…`
* **Картинка:** `artifacts/mine_crops/SS-c7aa8d26.png`

### `AR-a32b30a6` — object_added, raster_graphics, no_labels → **GRAPHIC_CHANGE**, объектов: 1

* **Чем трудна:** Оба блока — растровые вставки (0 векторных сегментов, 0 строк текста): векторный компаратор здесь слеп по построению.
* **Ожидаемое (человек):** В версии B рядом с «Фундамент под оборудование Фпк-3, Фпк-4. Опалубка» добавлен второй чертёж «Фундамент под оборудование Фпк-2. Армирование».
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР3-К3/versions/v001/02_work/document.pdf` стр. 10 (page_index 9), блок `9P9T-4WXT-FUH`, coords_px [212.0, 2002.0, 2206.0, 3296.0], page_px [7015, 4961], /Rotate 0, sha256 `5581e20b93a36960…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР3-К3/versions/v002/02_work/document.pdf` стр. 11 (page_index 10), блок `9VH4-FMFL-DRY`, coords_px [285.0, 1977.0, 4549.0, 3237.0], page_px [7015, 4961], /Rotate 0, sha256 `180efa24cbef5115…`
* **Картинка:** `artifacts/mine_crops/AR-a32b30a6.png`

### `EOM-7fef43a3` — object_added, with_labels → **GRAPHIC_CHANGE**, объектов: 2

* **Чем трудна:** Добавлены выноска с полочкой и сноска: графика (линия-выноска) и текст меняются вместе — граница между текстовой и графической правкой.
* **Ожидаемое (человек):** В версии B добавлены выноска с подписью «Закладная гильза для кабеля обогрева» и сноска «* Зазор между трубой и гильзой необходимо заделать мягким водонепроницаемым материалом».
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/v001/02_work/document.pdf` стр. 14 (page_index 13), блок `DNPX-ALMQ-JVQ`, coords_px [8478.0, 228.0, 10442.0, 2412.0], page_px [10524, 4961], /Rotate 0, sha256 `ee122c122e3c33fb…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/v002/02_work/document.pdf` стр. 16 (page_index 15), блок `blk_646dbe17f8244b23bf68ae6f57bced27`, coords_px [8471.0, 86.0, 10398.0, 2507.0], page_px [10524, 4961], /Rotate 0, sha256 `77460c9d1af44d81…`
* **Картинка:** `artifacts/mine_crops/EOM-7fef43a3.png`

### `EOM-46355862` — object_added, dense_block, with_labels → **GRAPHIC_CHANGE**, объектов: 15

* **Чем трудна:** Добавлена штриховка примерно в 15 помещениях: тысячи новых сегментов, но объектов — десятки.
* **Ожидаемое (человек):** В версии B появилась штриховка (заливка) примерно в 15 помещениях плюс новые выноски с марками; контуры стен и осей те же.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ2-ПА V1/versions/v002/02_work/document.pdf` стр. 9 (page_index 8), блок `9XGY-QVUJ-CFW`, coords_px [947.0, 821.0, 11155.0, 13757.0], page_px [19865, 14043], /Rotate 0, sha256 `e19acb1db3651ee1…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ2-ПА V1/versions/v003/02_work/document.pdf` стр. 9 (page_index 8), блок `blk_79d88ac3659b41dab5fb04360c7bc5c4`, coords_px [515.0, 291.0, 5968.0, 7312.0], page_px [10628, 7528], /Rotate 0, sha256 `614f23b15ee32355…`
* **Картинка:** `artifacts/mine_crops/EOM-46355862.png`

### `SS-a369f492` — object_added, object_moved, raster_graphics, with_labels → **GRAPHIC_CHANGE**, объектов: 2

* **Чем трудна:** Один узел добавлен, второй сдвинут вниз — смешанный случай; в блоке ещё и растровые вставки (6 → 24).
* **Ожидаемое (человек):** В версии B добавлен «Узел А прохода группы кабелей в лотке через строительные конструкции», а существующий «Узел В прохода кабеля в гильзе…» сдвинут вниз.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 1)/versions/v001/02_work/document.pdf` стр. 24 (page_index 23), блок `7GRH-LCRJ-NW4`, coords_px [4983.0, 1471.0, 7273.0, 3281.0], page_px [9933, 7016], /Rotate 0, sha256 `938796423724adf2…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АК-К5 (Книга 1)/versions/v002/02_work/document.pdf` стр. 24 (page_index 23), блок `blk_daabfa6e61a84217929a984d5370fc64`, coords_px [4863.0, 35.0, 7222.0, 3284.0], page_px [9934, 7017], /Rotate 0, sha256 `05ba8b778e3873e4…`
* **Картинка:** `artifacts/mine_crops/SS-a369f492.png`

### `VK-148ffe6c` — object_added, object_moved, rotated_page, dense_block → **GRAPHIC_CHANGE**, объектов: 3

* **Чем трудна:** Повёрнутая страница (/Rotate 90) + 106 552 сегмента + три разнородные правки одновременно.
* **Ожидаемое (человек):** В версии B добавлены спецификация оборудования справа и узел «Узел кранов с эл. приводом»; узел «Узел гиг. душа» сдвинут вправо. Сегментов 106 552 → 115 081.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf` стр. 9 (page_index 8), блок `blk_627feed96d9e4685ad1f7206a9c68d34`, coords_px [4500.0, 35.0, 8434.0, 6968.0], page_px [9934, 7017], /Rotate 90, sha256 `360626f01c1a0ccc…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v002/02_work/document.pdf` стр. 9 (page_index 8), блок `blk_313e9de24a6c40c6941187f6cf5625ca`, coords_px [4284.0, 24.0, 9875.0, 6601.0], page_px [9934, 7017], /Rotate 90, sha256 `61d95a84b064f4a3…`
* **Картинка:** `artifacts/mine_crops/VK-148ffe6c.png`

### `OV-93cc012f` — object_added, object_moved, with_labels → **GRAPHIC_CHANGE**, объектов: 4

* **Чем трудна:** Часть чертежа сдвинута относительно остальной (нежёсткое смещение) — глобальная регистрация одним сдвигом здесь принципиально не работает.
* **Ожидаемое (человек):** В версии B добавлены выноски-«шарики» 5 (33) и 1 (33) (три штуки), а верхняя часть трассы теплоснабжения сдвинута относительно нижней.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ1.1-К2 V1/versions/v001/02_work/document.pdf` стр. 28 (page_index 27), блок `6PY4-TFDE-HLQ`, coords_px [6328.0, 1737.0, 9430.0, 4527.0], page_px [9932, 7015], /Rotate 0, sha256 `e3c1d9406108e4b0…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ1.1-К2 V1/versions/v002/02_work/document.pdf` стр. 29 (page_index 28), блок `69ET-WJ49-VTQ`, coords_px [6116.0, 1587.0, 9394.0, 4539.0], page_px [9932, 7015], /Rotate 0, sha256 `e82e47a92c8a79b4…`
* **Картинка:** `artifacts/mine_crops/OV-93cc012f.png`

### `AR-55eda7fb` — small_local_change, table_only_change, with_labels → **GRAPHIC_CHANGE**, объектов: 1

* **Чем трудна:** Изменение занимает ≈0.5 % площади блока и спрятано в одной строке таблицы; число и эскиз изменились согласованно (D6/D7 из брифа контрфактов, но настоящий).
* **Ожидаемое (человек):** В таблице «Перемычки 39 этаж» изменён эскиз строки ПР-14: было L=850 (поз. 10) с размерами 150/550/150, стало L=1000 (поз. 8) со штриховкой стены справа. Остальные строки не изменились.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.1-К2/versions/v002/02_work/document.pdf` стр. 21 (page_index 20), блок `6UJM-PVAX-LLT`, coords_px [697.0, 4399.0, 2759.0, 6689.0], page_px [9932, 7015], /Rotate 0, sha256 `dbf1e384f3526089…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.1-К2/versions/v003/02_work/document.pdf` стр. 21 (page_index 20), блок `4D97-6HTT-NAA`, coords_px [571.0, 4493.0, 2775.0, 6723.0], page_px [9932, 7015], /Rotate 0, sha256 `c7246abb6d5b37de…`
* **Картинка:** `artifacts/mine_crops/AR-55eda7fb.png`

### `AR-577a293f` — small_local_change, table_only_change, with_labels → **GRAPHIC_CHANGE**, объектов: 3

* **Чем трудна:** Три строки таблицы изменены локально при неизменной сетке — проверка локализации, а не факта изменения.
* **Ожидаемое (человек):** В «Ведомости полов 17-го этажа» изменены эскизы и составы строк 2.1, 2.4 и 2.7 (число слоёв и толщины). Сетка таблицы и остальные строки не изменились.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР4.2-К4/versions/v001/02_work/document.pdf` стр. 22 (page_index 21), блок `9URF-UTRX-D4D`, coords_px [222.0, 3800.0, 3800.0, 6990.0], page_px [9933, 7016], /Rotate 0, sha256 `9f6b0b7fc8624bb1…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР4.2-К4/versions/v002/02_work/document.pdf` стр. 22 (page_index 21), блок `9GTD-U4PC-ENE`, coords_px [239.0, 3780.0, 3781.0, 6990.0], page_px [9933, 7016], /Rotate 0, sha256 `e54f6467e529f4d9…`
* **Картинка:** `artifacts/mine_crops/AR-577a293f.png`

### `EOM-14558cda` — object_added, occluding_fill, with_labels → **GRAPHIC_CHANGE**, объектов: 1

* **Чем трудна:** В одной версии весь вид закрыт сплошной чёрной заливкой при почти том же числе сегментов (7 932 / 7 767): различается ровно один перекрывающий объект, а попиксельная разница 14 %.
* **Ожидаемое (человек):** В версии A вид «Внешний вид УЭРМ» полностью закрыт сплошной чёрной заливкой; в версии B тот же вид виден нормально. Подписи и отметки вокруг совпадают.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К5/versions/v001/02_work/document.pdf` стр. 10 (page_index 9), блок `WYRG-AQ9K-KR7`, coords_px [4902.0, 88.0, 7858.0, 3134.0], page_px [10524, 4961], /Rotate 0, sha256 `ba2cc8bde6c72536…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К5/versions/v002/02_work/document.pdf` стр. 11 (page_index 10), блок `3RM7-TX6N-U7M`, coords_px [4896.0, 79.0, 7834.0, 3201.0], page_px [10524, 4961], /Rotate 0, sha256 `3c9dc4330e53294d…`
* **Картинка:** `artifacts/mine_crops/EOM-14558cda.png`

### `AR-5acaab0e` — object_added, dense_block, hatch_noise, with_labels → **GRAPHIC_CHANGE**, объектов: 3

* **Чем трудна:** 111 161 сегмент, штриховка сдвинута по фазе и даёт красно-синий шум по всей площади, поверх которого лежит настоящая правка.
* **Ожидаемое (человек):** В версии B достроен объём стен в правой верхней части (штриховка расширена вправо) и добавлен заголовок сверху; остальная штриховка совпадает по смыслу, но сдвинута по фазе.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР3.2-ПА/versions/v001/02_work/document.pdf` стр. 7 (page_index 6), блок `7ELN-4AF3-9WF`, coords_px [239.0, 39.0, 9908.0, 12237.0], page_px [9932, 14043], /Rotate 0, sha256 `6d2e93b0b5fe8644…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР3.2-ПА/versions/v002/02_work/document.pdf` стр. 8 (page_index 7), блок `79LK-YN6E-D6J`, coords_px [239.0, 119.0, 9928.0, 11535.0], page_px [9932, 14043], /Rotate 0, sha256 `31deaf8d6f4712f2…`
* **Картинка:** `artifacts/mine_crops/AR-5acaab0e.png`

### `AR-490254e9` — block_scope_change, with_labels → **NOT_COMPARABLE**

* **Чем трудна:** Границы подготовленного блока разъехались: в v002 блок покрывает только «Вид 1», в v003 — «Вид 1»…«Вид 4». Сравнивать как «один и тот же регион» нельзя, наивный компаратор объявит три добавленных чертежа.
* **Ожидаемое (человек):** Содержимое стороны A целиком присутствует в стороне B («Вид 1»), но блок B охватывает ещё три вида. Это разница нарезки блоков, а не доказанная правка листа.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v002/02_work/document.pdf` стр. 9 (page_index 8), блок `4CPH-ENT4-6C6`, coords_px [3839.0, 4677.0, 4333.0, 6493.0], page_px [9932, 7015], /Rotate 0, sha256 `52b9b7bf552277ef…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v003/02_work/document.pdf` стр. 9 (page_index 8), блок `9QKX-7CUT-AWA`, coords_px [3793.0, 4815.0, 7698.0, 6481.0], page_px [9932, 7015], /Rotate 0, sha256 `b12caa81ebb6b1d1…`
* **Картинка:** `artifacts/mine_crops/AR-490254e9.png`

### `OV-2cc2a382` — block_scope_change, raster_graphics, no_labels → **NOT_COMPARABLE**

* **Чем трудна:** Крайний случай той же болезни: A — узкая полоска с эскизом установки (0 текстовых строк, 1 растровая вставка), B — целый опросный лист поставщика с тем же эскизом внутри (65 строк, 7 вставок).
* **Ожидаемое (человек):** Эскиз установки из A присутствует в B; сторона B дополнительно содержит весь бланк «ТЕХНИЧЕСКИЕ ДАННЫЕ № РА26-005271-05» с таблицами. Разница нарезки блоков, а не правка.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ2-К1 V1/versions/v001/02_work/document.pdf` стр. 145 (page_index 144), блок `7W3K-HLP3-74K`, coords_px [413.0, 1671.0, 2032.0, 1947.0], page_px [2480, 3507], /Rotate 0, sha256 `37600164602b2f29…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ2-К1 V1/versions/v002/02_work/document.pdf` стр. 112 (page_index 111), блок `blk_22190bc12d204174a27b8ac00f64f4a4`, coords_px [218.0, 96.0, 2479.0, 3320.0], page_px [2481, 3508], /Rotate 0, sha256 `c4e799a52802fb65…`
* **Картинка:** `artifacts/mine_crops/OV-2cc2a382.png`

### `EOM-1db297d2` — uncertain, block_match_failure → **UNCERTAIN**

* **Чем трудна:** Сопоставитель блоков связал РАЗНЫЕ блоки: слева «Условные обозначения», справа «Проход кабелей через перегородки и перекрытия». Пара показывает отказ сопоставления, а не изменение чертежа.
* **Ожидаемое (человек):** Содержимое сторон не соответствует друг другу — сравнивать нечего; как эталон не использовать.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v001/02_work/document.pdf` стр. 13 (page_index 12), блок `6XCP-3CVN-MNC`, coords_px [4300.0, 4433.0, 6944.0, 6959.0], page_px [14894, 7015], /Rotate 0, sha256 `5778ec55c0a2181f…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭО-К3/versions/v002/02_work/document.pdf` стр. 13 (page_index 12), блок `9R7K-NRXF-7W7`, coords_px [7831.0, 5201.0, 9553.0, 6923.0], page_px [19865, 7015], /Rotate 0, sha256 `596b41dd408e54f8…`
* **Картинка:** `artifacts/mine_crops/EOM-1db297d2.png`

### `GP-6bc1c029` — uncertain, dense_block → **UNCERTAIN**

* **Чем трудна:** Генплан на 378 441 сегмент: разница 1.5 % рассыпана мелкими пятнами, на экране блока не читается.
* **Ожидаемое (человек):** Что именно изменилось — по картинке блока определить не удалось; нужна поэлементная сверка. Как эталон не использовать.
* **A:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/GP/documents/13АВ-РД-ГП2/versions/v001/02_work/document.pdf` стр. 6 (page_index 5), блок `474Q-GKNU-ACJ`, coords_px [1567.0, 27.0, 9911.0, 3534.0], page_px [9933, 7016], /Rotate 0, sha256 `13da850c9f146c04…`
* **B:** `projects_v2/objects/214_Alia_ASTERUS/disciplines/GP/documents/13АВ-РД-ГП2/versions/v002/02_work/document.pdf` стр. 6 (page_index 5), блок `blk_0671117538fa44249e579eabde2cbd9e`, coords_px [1694.0, 35.0, 9917.0, 3550.0], page_px [9934, 7017], /Rotate 0, sha256 `5ee01acef97bb648…`
* **Картинка:** `artifacts/mine_crops/GP-6bc1c029.png`
