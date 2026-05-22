# Coordination rules — completeness lens prompt block

**Назначение:** запретить флагать coordination artifacts как `missing
findings`. Координация между разделами — это процесс-артефакт, часто
оформляемый отдельным письмом-заданием смежнику. Эти документы не
попадают внутрь марки дисциплины и не должны считаться обязательными
для completeness lens.

## Главное правило

Запрещено генерировать missing finding на:

- «**Отсутствует координация с разделом X**» — координация может быть в
  письме-задании, которое мы не видим.
- «**Отсутствует передача в ЭОМ перечня электроприёмников ОВ**» — это
  задание смежнику.
- «**Отсутствует координация АПС с ОВ для управления ПДВ**» — алгоритм
  обычно в разделе АПС/СС, не в OV.
- «**Отсутствуют закладные / отверстия для смежника**» — часто оформляется
  отдельным заданием, не внутри марки.
- Любой item с категорией «Mandatory coordination requirements
  (cross-discipline)» в исходном чек-листе и `can_be_reported_as_missing=false`.

## Когда coordination-finding допустим

Только при явном противоречии **внутри текущего MD**:

- ✅ «На плане АР показано отверстие 200×200 без указания размеров для
  задания смежнику» — параметрический пробел в одном чертеже.
- ✅ «Насос упомянут в ВК, но без P_уст для передачи в ЭОМ» —
  не отсутствие координации, а нехватка ключевого параметра.

## Coordination items в matrix

Из `completeness_requirements_matrix.json`:

- AR-15, AR-16, AR-17 — координация с ОВ/ВК/ЭОМ/КЖ/КМ
- EOM-16, EOM-17, EOM-18 — координация с ОВ/ВК/АПС
- KJ-17, KJ-18 — закладные / отверстия
- KM-17, KM-18, KM-19 — нагрузки / опирание / фасадные узлы
- OV-17, OV-18, OV-19, OV-20 — передача в ЭОМ / АПС / АР / газ
- VK-16, VK-17, VK-18, VK-19 — передача в ЭОМ / АПС / ИТП / АР
- SS-16, SS-17, SS-18, SS-19 — передача в ЭОМ / координация АПС
- MULTI-19 — журнал согласований

Все они в metadata имеют `can_be_reported_as_missing=false` и
`requires_cross_section=true`. Runner обязан их **drop** в single-MD lens.

## Не путать с

Coordination ≠ Cross-section.

- **Coordination** — задание смежнику, отдельный документ-артефакт.
- **Cross-section consistency** — сравнение параметров двух разделов.

Оба запрещены в single-MD pipeline, но по разным причинам.

## Source

`experiments/md_analysis_comparison/normative_checklist_research/final_report.md`,
вопрос 5 «Coordination artifacts» (15 items);
`recommendations/prompt_rules_update.md` §3.
