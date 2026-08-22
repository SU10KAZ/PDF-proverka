# Vector Research Track A — техническая проверка Vector + Vision

## Короткий вывод

Page cache решает повторное чтение тяжёлой PDF-страницы, но не устраняет стоимость block-level topology/comparison. На плотной странице с 15 352 drawings пять обращений к raw page payload ускорились в 3,66 раза уже при cold shared cache, в 7,97 раза с disk cache и в 2819 раз при RAM hit. На обычной странице полный цикл пяти блоков ускорился скромнее: в 1,37 раза cold и в 1,62 раза warm, потому что после чтения страницы всё ещё выполняется clip, построение topology и comparator.

Vector без Vision оказался безопасным только при одновременном прохождении шести gates: `geometry_ok`, `text_ok`, `topology_ok`, `crop_ok`, `caps_ok`, `style_ok`. Таких пар было 5 из 39. Все пять решений совпали с человеческим route, опасных `False VECTOR_OK` не обнаружено. Это хороший fail-safe результат, но пять наблюдений ещё не дают production confidence.

Vision обязателен при сломанном/частичном text layer, caps, ненадёжном style matching, разном semantic crop и недостаточном vector layer. В выборке 32 пары ушли в `VECTOR_WITH_VISION`, ещё 2 — в `VISION_ONLY`; measured Vision usage rate составил 87,18%.

Vector ошибается прежде всего на плотной capped geometry, пограничных crop, outlined/неизвлекаемом тексте и мелких annotations на фоне тысяч primitives. Из 39 comparator verdicts 33 были полностью корректны, 6 частично корректны, 0 полностью ошибочны. Fail-safe routing при этом дал 0 routing errors и 0 `False VECTOR_OK`.

Hybrid на десяти трудных парах оказался существенно надёжнее каждого отдельного AI-arm: 9/10 правильных классов против 5/10 у Vector-only и 4/10 у Vision-only; false structural calls — 0 против 3 и 2. Цена — 114 008 input tokens против 65 547 и 62 511. Единственный miss Hybrid — тонкое изменение notes/value в `vk_nodes`.

Отдельный предварительный verifier не улучшил Hybrid, а ухудшил его: сохранённый Pipeline B дал 5/10 правильных классов, completeness 0,70, два false structural и три пропущенных важных изменения. Нормализованная цена выросла до ~210 089 input tokens и ~337,76 s против 114 008 и 234,47 s у одного fused Hybrid call. Verifier полезен как диагностика extractor, но не как обязательная стадия comparison.

`L3_CHANGE_ONLY` имеет медиану 8 518 bytes / 1 979 estimated tokens на десяти парах, на 68,82% меньше старого L3 payload. Реальный одинаковый model probe на трёх парах сократил input с 56 087 до 29 459 tokens, то есть на 47,48% (примерно 9 820 input tokens на пару вместе с prompt/schema overhead).

Production Vector Stage уже можно проектировать на уровне контракта, cache, evidence ledger и fail-safe routing, но подключать его к production comparison рано. Нужен ещё один небольшой, заранее размеченный research-stage: исправить crop text leakage, проверить больше реальных changed/style-only/raster cases и повторить fused Hybrid на более широкой выборке и нескольких model runs.

## 1. Границы исследования

Эксперимент полностью изолирован в `experiments/stage_comparison_vector_blocks_v02_codex/`. Production Stage Comparison, Stage 3/4/5, sheet/block matching, text comparison, AI reviewer, `project_change_summary`, UI, API и `sheet_links` не менялись. Старые `PreparedDocument`, ORB, affine, change regions и semantic diff не возвращались.

Track A продолжает v0.1 и переиспользует только его исследовательские extractor/comparator primitives. Параллельный отчёт Claude Opus не читался и не использовался. Baseline зафиксирован на commit `1619fc3f`.

Benchmark содержит 78 реальных блоков / 39 вручную заданных пар из пяти discipline/document pairs: СС — 12, ВК — 11, АР — 10, ОВ — 5, ЭОМ — 1. Это 10 отдельных version PDF. Для КЖ/КМ/ГП в active corpus не найдено пригодных пар версий с block manifests, поэтому искусственными парами они не заменялись. Три одиночных реальных блока КЖ/КМ/ГП дополнительно использованы в verification experiment.

Ground truth задавался по side-by-side raster inspection, а не выводом модели. После повторной визуальной проверки были честно уточнены два verdict: `vk_axono_page17` содержит добавленный notes block, `ov_plan_floor07` — удалённые внутренние equipment contours в нескольких зонах.

## 2. Что сохранено от текущего вектографа и v0.1

Текущий вектограф остаётся профильным каскадом: discipline-specific факты, строгий clip, provenance, validation/readiness и fail-soft/fail-closed политика. Универсальный v0.1 слой уже доказал полезность raw + block-normalized geometry, endpoint/T topology, unconnected X-crossings, contours, repeated motifs, positioned vector text, anchors, signatures и caps.

В v0.2 переиспользованы именно общие идеи:

- `page.get_drawings()` и `page.get_text("dict")` как единственные data sources extractor;
- исходные и block-relative coordinates без affine warp;
- geometry, text, topology, patterns и style как отдельные evidence families;
- caps и quality status вместо скрытого truncation;
- deterministic diff до AI;
- компактная change-only проекция вместо отправки raw primitives модели.

Не переносились discipline regex/entities, production router, sheet matcher, reference catalog и специальные физические правила ЭОМ/ВК.

Размеры полного `VectorBlockDescription` не пересчитывались заново: v0.1 на 20 блоках дал median 2 880 581 bytes для Level 0 raw, 58 818 bytes для grouped Level 2 и 8 133 bytes для compact Level 3; median raw/compact reduction — 204×. Track A измеряет следующий шаг: парный `L3_CHANGE_ONLY` с медианой 8 518 bytes вместо 29 124,5 bytes старого pair payload.

## 3. Page-level cache

Cache key: `SHA-256 PDF + page_index + extractor_version`. Payload содержит raw drawings, full page text dict, page metadata и provenance. На один page miss `get_drawings()` и `get_text()` вызываются ровно по одному разу; последующие блоки только фильтруют cached payload. Disk payload — локальный trusted gzip/pickle и не предназначен для untrusted input.

| Измерение | Результат |
|---|---:|
| Обычная страница, 5 блоков, baseline без shared page | 0,168538 s |
| Shared cold page + 5 block clips | 0,122992 s, 1,37× |
| Warm disk page + 5 block clips | 0,104346 s, 1,62× |
| Только page payload: cold → disk | 0,023886 → 0,006893 s, 3,47× |
| Только page payload: cold → RAM | 0,023886 → 0,000228 s, 104,66× |
| Dense page, 15 352 drawings, 5 raw reads | 0,761321 s |
| Dense shared cold / disk / RAM | 0,208074 / 0,095473 / 0,000270 s |
| Dense speedup | 3,66× / 7,97× / 2818,56× |

В полном cold benchmark было 78 block requests, 68 уникальных page reads и 10 RAM hits; cache занял 199 364 433 bytes. Полное время — 841,32 s: extraction 560,74 s, comparison 279,12 s. Это показывает следующий bottleneck: cache убрал повторный PDF parse, но topology и tolerance matching плотных блоков всё ещё дороги.

## 4. Style diff

Style сравнивается отдельно от geometry: stroke, fill, width, dash, stroke/fill opacity, line cap и line join. Material style change разрешает `STYLE_ONLY_CHANGED` только при ≥99,5% bidirectional geometry coverage, равном числе segments, стабильной topology, надёжном style matching и отсутствии caps.

Controlled suite: 11/11 passed — unchanged, solid→dashed, fill added/removed, width, color, opacity, cap, join и два sub-threshold noise cases. Исследовательские tolerances: 0,015 на color channel, `max(0,05 pt, 5%)` для width, 0,02 для opacity, normalized/exact для dash/cap/join.

Первоначальное свободное правило дало шесть ложных real `STYLE_ONLY_CHANGED`. Строгая перекалибровка снизила этот показатель с 6 до 0. Реальных подтверждённых style-only пар в корпусе не было, поэтому controlled успех нельзя считать полной real-world validation.

## 5. Quality, text, caps и crop gates

Routing использует независимые booleans:

- `geometry_ok`: обе стороны имеют ≥3 segments и comparator не capped;
- `text_ok`: обе стороны имеют `TEXT_GOOD`;
- `topology_ok`: vector достаточен и topology не capped;
- `crop_ok`: aspect/content extent/coverage/border diagnostics не дали mismatch;
- `caps_ok`: ни extractor, ни comparison не truncation-capped;
- `style_ok`: геометрическое сопоставление styles достаточно надёжно.

Любой failed gate запрещает `VECTOR_OK`. Недостаточный vector layer даёт `VISION_ONLY`; сильный crop mismatch с geometry coverage <0,5 также даёт `VISION_ONLY`; остальные риски — `VECTOR_WITH_VISION`.

Text quality основан на реальных сигналах: printable/control/replacement/private-use ratios, подозрительное font mapping и span consistency. `TEXT_BROKEN` не участвует в semantic verdict. Caps разделены на `segments_capped`, `topology_capped`, `patterns_capped`, `text_capped`.

Crop mismatch не использует affine. Он объединяет aspect delta, content-extent edge deltas, asymmetric directional geometry coverage и border anchor imbalance. Это полезно как fail-safe, но два partial verdict показывают, что правило пока бывает слишком чувствительным к padding и может спутать crop с настоящим локальным удалением.

## 6. Результаты 39 реальных пар

| Pair | Discipline | Human route | Actual route | Vector verdict | Human verdict | Result |
|---|---|---|---|---|---|---|
| ss_scheme_text_changed | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_SAME_VALUES_CHANGED | CORRECT |
| ss_plan_dense | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ss_simple_node | SS | VECTOR_OK | VECTOR_OK | IDENTICAL | IDENTICAL | CORRECT |
| ss_table_graphic | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_wall_sections | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_plan | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_nodes | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | STRUCTURE_SAME_VALUES_CHANGED | PARTIAL |
| vk_node_plan | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| eom_singleline_changed | EOM | VISION_ONLY | VISION_ONLY | CROP_MISMATCH | CROP_MISMATCH | CORRECT |
| ss_crop_mismatch_page07 | SS | VISION_ONLY | VISION_ONLY | CROP_MISMATCH | CROP_MISMATCH | CORRECT |
| ss_plan_page09 | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | IDENTICAL | IDENTICAL | CORRECT |
| ss_plan_page11 | SS | VECTOR_OK | VECTOR_OK | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ss_plan_page12 | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | IDENTICAL | IDENTICAL | CORRECT |
| ss_plan_page13 | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ss_plan_page14 | SS | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ss_detail_page17 | SS | VECTOR_OK | VECTOR_OK | IDENTICAL | IDENTICAL | CORRECT |
| ss_table_page19 | SS | VECTOR_OK | VECTOR_OK | IDENTICAL | IDENTICAL | CORRECT |
| vk_plan_page07 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_plan_page08 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_plan_page10 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_nodes_page11 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_diagrams_page16 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_axono_page17 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | STRUCTURE_CHANGED | STRUCTURE_SAME_VALUES_CHANGED | PARTIAL |
| vk_diagrams_page18 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| vk_axono_page20 | VK | VECTOR_WITH_VISION | VECTOR_WITH_VISION | STRUCTURE_CHANGED | NEAR_IDENTICAL | PARTIAL |
| ar_plan_page05 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan_page07 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan_page08 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | STRUCTURE_CHANGED | NEAR_IDENTICAL | PARTIAL |
| ar_plan_page10 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | STRUCTURE_CHANGED | NEAR_IDENTICAL | PARTIAL |
| ar_plan_page11 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan_page12 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan_page13 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ar_plan_page16 | AR | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ov_plan_floor04 | OV | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ov_plan_floor05 | OV | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ov_plan_floor06 | OV | VECTOR_WITH_VISION | VECTOR_WITH_VISION | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |
| ov_plan_floor07 | OV | VECTOR_WITH_VISION | VECTOR_WITH_VISION | CROP_MISMATCH | STRUCTURE_CHANGED | PARTIAL |
| ov_equipment_table | OV | VECTOR_OK | VECTOR_OK | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT |

Сводная статистика:

| Метрика | Значение |
|---|---:|
| Pairs / blocks | 39 / 78 |
| VECTOR_OK | 5 |
| VECTOR_WITH_VISION | 32 |
| VISION_ONLY | 2 |
| Routing errors | 0 |
| False VECTOR_OK | 0 |
| Correct / partial / wrong | 33 / 6 / 0 |
| IDENTICAL / NEAR_IDENTICAL | 5 / 26 |
| VALUES_CHANGED / STRUCTURE_CHANGED | 1 / 4 |
| CROP_MISMATCH / INSUFFICIENT_VECTOR_DATA | 3 / 0 |
| Vision usage rate | 87,18% |
| Cache speedup | 1,37–1,62× full five-block; 3,66–2819× page payload on dense page |
| Median L3 AI payload | 1 979 estimated tokens; ~9 820 actual input/pair in 3-pair probe |

Шесть partial cases объяснимы и трассируемы: `vk_nodes` пропустил notes/value; `vk_axono_page17` завысил добавление notes до structural change; `vk_axono_page20` и два AR plan дали false structural на dense/crop-sensitive geometry; `ov_plan_floor07` принял реальное удаление equipment contours за crop mismatch.

## 7. Comparator evidence и L3_CHANGE_ONLY

Каждый verdict сохраняет числа, а не только текст: directional geometry coverage и segment delta; topology similarity, branch/component deltas; changed/added/removed text; repeated-pattern deltas; style field changes; crop diagnostics; quality/cap provenance.

`L3_CHANGE_ONLY` исключает unchanged primitives, signatures и hashes. Он передаёт route/gates, только geometry/topology deltas, changed values/text, pattern/style deltas, crop diagnostics при mismatch и uncertainties.

| Payload | Median bytes | Median estimated tokens |
|---|---:|---:|
| v0.1 full L3 + filtered diff | 29 124,5 | 6 806,5 |
| v0.2 L3_CHANGE_ONLY | 8 518 | 1 979 |
| Reduction | 68,82% | 70,92% по median token estimate |

Real `gpt-5.6-sol` probe на одинаковых трёх парах: old 129 594 bytes / 56 087 input tokens; change-only 46 263 bytes / 29 459 input tokens. Latency 6,60 s против 6,95 s находится в обычном шуме короткого model call; выигрыш доказан по контексту, не по latency.

## 8. Vector-only, Vision-only и Hybrid

Одна и та же сильная модель `gpt-5.6-sol` проверена на десяти намеренно трудных парах. Ground truth и оценка каждого claim выполнены вручную.

| Arm | Correct class | Completeness | False structural | Missed change | Input tokens | Latency |
|---|---:|---:|---:|---:|---:|---:|
| Vector-only | 5/10 | 0,65 | 3 | 4 | 65 547 | 121,84 s |
| Vision-only | 4/10 | 0,90 | 2 | 1 | 62 511 | 243,55 s |
| Hybrid | 9/10 | 0,90 | 0 | 1 | 114 008 | 234,47 s |

Vector-only лучше трассирует counts, но наследует false geometry/crop calls и не видит текст вне пригодного vector layer. Vision полнее, но переоценивает визуальные различия и путает class/crop. Hybrid использовал source tags для 100% claims, снял все false structural calls и правильно разобрал EOM/OV реальные изменения. Он дороже в 1,74 раза по input tokens относительно Vector и в 1,82 раза относительно Vision. Latency одного прогона нельзя считать стабильным преимуществом Hybrid над Vision.

AI не нужен для low-level segment/style/text/topology diff. Он полезен для объединения противоречивых evidence, чтения raster-only facts и осторожного итогового инженерного объяснения.

## 9. Предварительный Vision verifier VectorBlockDescription

Дополнительный experiment реализует требуемую схему:

```text
VectorBlockDescription + raster crop
                ↓
         Vision verifier
                ↓
VERIFIED / PARTIAL / FAILED
verified_facts / missing_facts / suspicious_facts
```

Verifier не получает права переписывать coordinates или точные geometry facts. Проверено 15 реальных описаний (12 blocks из benchmark и новые GP/KJ/KM) и 8 controlled cases: correct, removed element, wrong count, missing label, wrong topology, broken text, capped geometry, wrong crop.

На реальных блоках сам verifier выдал 11 `VERIFIED`, 4 `PARTIAL`, 0 `FAILED`; human ground truth после отдельной проверки raster/description — 7 `VERIFIED`, 8 `PARTIAL`, 0 `FAILED`.

| Verification metric | Результат |
|---|---:|
| Real exact status accuracy | 11/15 = 73,33% |
| Controlled exact status | 7/8 = 87,5% |
| Controlled corrupted sample detected as non-VERIFIED | 7/7 = 100% |
| Wrong crop correctly FAILED | Да |

Verifier нашёл два настоящих дефекта, подтверждённых отдельной ручной проверкой raster/description:

- EOM crop: description содержит невидимые в crop подписи title block из-за span intersection на границе;
- GP: видимые title/upper layer labels отсутствуют в PDF text extraction, поскольку часть текста представлена не обычным text layer.

Но он четыре раза вернул `VERIFIED` для заведомо `PARTIAL` capped descriptions AR и SS, хотя в `limitations` сам упомянул caps. Следовательно, status verifier нельзя использовать для повышения доверия или отмены deterministic failed gate. Он годится только для downgrade, missing/suspicious facts и локализации причины.

### Сравнение конвейеров

Экстраполяция использует 78 benchmark blocks, из них 62 risky и 16 high-confidence; 34 pair comparisons уже требуют Vision по route.

| Pipeline | Verify calls | Diff Vision calls | Total Vision calls | Extra verifier input | Extra verifier latency |
|---|---:|---:|---:|---:|---:|
| A. Только Vision/Hybrid по diff route | 0 | 34 | 34 | 0 | 0 |
| B. Verify каждый block, затем diff | 78 | 34 | 112 | ~250 899 tokens | ~1053 s |
| C. Verify только risky blocks, затем diff | 62 | 34 | 96 | ~199 432 tokens | ~837 s |

Таблица выше — экстраполяция стоимости на все 39 пар. Кроме неё Pipeline B реально прогнан end-to-end на тех же десяти hard pairs: все 20 blocks прошли verifier, затем тот же Hybrid comparator получил raster, L3 diff и verification evidence.

| Hard-10 pipeline | Correct class | Completeness | False structural | Missed important change | Input tokens | Latency | Conceptual Vision calls |
|---|---:|---:|---:|---:|---:|---:|---:|
| A. Один fused Hybrid diff | 9/10 | 0,90 | 0 | 1 | 114 008 | 234,47 s | 10 |
| B. 20 block verifies + Hybrid diff | 5/10 | 0,70 | 2 | 3 | ~210 089 | ~337,76 s | 30 |

Для B восемь уже выполненных block verifications нормализованы как 8/12 их batch, остальные 12 и comparator взяты по фактическому usage; реально сохранены три batched model invocations и все 20 verification projections. Pipeline B нашёл реальное изменение EOM, но оставил класс `CROP_MISMATCH`; удаление equipment в OV пропустил, а на AR p08 и VK axonometry заякорился на шумных topology/style counts и дал false structural. Exact-class accuracy регрессировала на 40 percentage points, input вырос в 1,84 раза, latency — в 1,44 раза.

Ответ на обязательный архитектурный вопрос: отдельный этап `Vector → Vision Verify → Compare` **не нужен**. Verify-all одновременно дороже и хуже по measured accuracy. Selective `risky → verify` можно оставить только как offline/downgrade-only диагностику extractor; он не должен становиться обязательной runtime стадией. Для comparison лучший измеренный вариант — один fused Hybrid call, который получает crop + vector uncertainties и одновременно проверяет evidence и diff.

## 10. Предлагаемая следующая архитектура

```text
PDF SHA + page + extractor version
                ↓
        page-level payload cache
                ↓
   block clip → VectorBlockDescription
                ↓
 geometry/text/topology/crop/caps/style gates
        ├─ all pass → deterministic Compare
        ├─ risky → one fused Hybrid verify+Compare
        └─ insufficient / severe crop → Vision-only
                ↓
      deterministic evidence ledger
                ↓
       L3_CHANGE_ONLY summary
```

Vision никогда не должен создавать или заменять coordinates. Его факты получают provenance `VISION`; exact vector facts — `VECTOR`; подтверждённые обоими — `BOTH`. Любой cap/failed deterministic gate остаётся failed независимо от verbal `VERIFIED` модели.

## 11. Что проверить в последнем маленьком research-stage

1. Заранее зафиксировать не менее 30 дополнительных changed-heavy пар, отдельно real style-only, small symbol add/remove, raster/Type3 text и adversarial crop; нынешняя выборка смещена к `NEAR_IDENTICAL` (26/39).
2. Исправить text clipping: принимать span по center/overlap policy и повторить EOM boundary cases.
3. Разделить semantic content extent от padding/background hatch, затем перепроверить `ov_plan_floor07`, AR p08/p10 и VK axonometry.
4. Повторить fused Hybrid против selective risky verifier на расширенной выборке и нескольких model runs, чтобы отделить устойчивый эффект от model variance; текущий verify-all B уже проиграл 5/10 против 9/10.
5. Набрать реальные style-only ground truth и проверить tolerances вне synthetic fixtures.
6. Снизить topology cost/caps на dense pages и измерить warm end-to-end, а не только page-payload cache.

## Финальная рекомендация Track A

**B. Нужен ещё один маленький research-stage.**

Базовая архитектурная рекомендация v0.1 сохраняется: универсальный Vector полезен как проверяемый слой только внутри Hybrid Vector + Vision, а не как замена Vision для произвольного PDF. Production ничего не внедрять до устранения перечисленных falsifiers.
