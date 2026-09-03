# Function Lineage v2.3 — group / partial candidate forensics

## Execution boundary

- Frozen experiment record: `94eb48b8` (read-only).
- Production reference: `4d489bf9033ad40c40099fe5e1436493bc56c0ed` / `ui-real-4d489bf9`.
- New model calls: `0`; vision: `0`; prompt/model input/model output changes: `0`.
- Deploy, shadow, materialization and candidate regeneration: not performed.
- In-memory double build is byte-identical before artifacts are written.

## LEFT20 Function Passport

| Field | Frozen value |
|---|---|
| physical_page | 20 |
| graphic_sheet_number | 5 |
| function_id | func_2767e2d48433038ab2c5 |
| fragment_id | frag_f1b4378224832f41a1b1 |
| function_class | DOMESTIC_PRESSURE_BOOST |
| serviced_object | UNKNOWN/null |
| corpus | UNKNOWN/null |
| zone | UNKNOWN/null |
| floors | UNKNOWN/null |
| consumers | трубопровод хозяйственно-питьевого водопровода (совмещенный с противопожарным) |
| upstream | Входные трубопроводы обозначены как «Ввод В1 2Ø150».; В правой части изображены две установки повышения давления: для пожаротушения (АЛЬФА Stream СПДс 2 СДМ 15-7 5,5 кВт К 65 мм) и для хозяйственно-питьевого водоснабжения (АЛЬФА Stream СПДс 3 СДМ 10-7 3 кВт КЧЗ 65 мм). |
| downstream | Принципиальная схема водопроводных насосных установок (АПТ, ХВС, ВПВ) и водомерного узла.; Включает блок условных обозначений и границы проектирования.; Фрагмент содержит принципиальную схему водопроводных систем и блок условных обозначений слева.; Схема разделена пунктирными линиями на зоны: «Насосная АПТ и водомерный узел» (слева) и «Насосная ХВС и ВПВ» (справа).; Входные трубопроводы обозначены как «Ввод В1 2Ø150».; В левой части показан «Общедомовой водомерный узел на базе турбинного счетчика ВСХнд-40 (или аналог)» и стрелка перехода «Далее к АПТ, см.; В правой части изображены две установки повышения давления: для пожаротушения (АЛЬФА Stream СПДс 2 СДМ 15-7 5,5 кВт К 65 мм) и для хозяйственно-питьевого водоснабжения (АЛЬФА Stream СПДс 3 СДМ 10-7 3 кВт КЧЗ 65 мм).; Также показан гидропневматический бак и трубопроводы Ø80. |
| systems | Внутренние; водоснабжения; водопроводных; АПТ; ВПВ; водомерного; Включает; блок; водомерный; Входные; трубопроводы; как |
| equipment_roles | Насосная АПТ и водомерный узел; Насосная ХВС и ВПВ; Общедомовой водомерный узел на базе турбинного счетчика ВСХнд-40 (или аналог); Установка повышения давления для нужд пожаротушения; Установка повышения давления для нужд хозяйственно-питьевого водоснабжения; Гидропневматический бак; Принципиальная схема водопроводных насосных установок (АПТ, ХВС, ВПВ) и водомерного узла.; Схема разделена пунктирными линиями на зоны: «Насосная АПТ и водомерный узел» (слева) и «Насосная ХВС и ВПВ» (справа).; В левой части показан «Общедомовой водомерный узел на базе турбинного счетчика ВСХнд-40 (или аналог)» и стрелка перехода «Далее к АПТ, см.; Также показан гидропневматический бак и трубопроводы Ø80.; Блок условных обозначений содержит графические символы для насосов, счетчиков, клапанов, манометров и других элементов. |
| document_role | GRAPHIC_SHEET |
| neighbors | func_2d769e892d3d3b4a6a75; func_6f2f59729b0c8f23582b; func_add99e2f9ac682ddd307; func_c70c920df990623ffde3; func_cd81e03fc56d01414ccd; func_ded02102e4a5e67fdbc7 |

## LEFT20 candidates and coverage

| Candidate | Relation | Rank (display only) | Atomic task scope | Declared composite scope | Covered composite roles | Missing composite roles |
|---|---|---:|---|---|---|---|
| R26 `lcand_1d1f175a30c34b88c6e0` | `CONTINUED_1_TO_1` | 5 | `FULL` | `PARTIAL` | DOMESTIC_PRESSURE_BOOST | INCOMING_METERING, FIRE_PRESSURE_BOOST |
| [26,28,29] `lcand_9c617494b14c2b922d3f` | `FUNCTION_DISTRIBUTED` | 1 | `FULL` | `FULL` | INCOMING_METERING, FIRE_PRESSURE_BOOST, DOMESTIC_PRESSURE_BOOST | UNKNOWN/null |

The exact mapping relation is `R26 STRICT_SUBSET OF distributed`; rank and score are not inputs to that result. It is simultaneously `ALTERNATIVE_GRANULARITY`: R26 is FULL for the declared one-fragment task, but PARTIAL against the three-component composite scope. Therefore the unqualified question “is R26 PARTIAL?” is `UNKNOWN`; it has no safe answer until a scope is named.

The distributed mapping is the exact union of three independently generated singleton candidates: domestic pressure boost → R26, fire pressure boost → R28, and incoming metering → R29. The group itself is atomic and supported; the defect is that this three-fragment candidate is projected into each one-fragment task while the task passport remains singular.

Page 20 also contains extracted roles outside the declared composite ontology: FIRE_WATER, METERING, PUMPING_PRESSURE, WATER_SUPPLY. Neither candidate maps those roles, so `FULL` above means full coverage of the generator's declared three-role composite, not proof of coverage of every extracted function on the physical page.

## Corpus-wide top-12 audit

| Corpus | Tasks | Exclusive OVERLAP tasks | Strict-containment tasks | Alternative-granularity tasks | Contradictory tasks | Group/singleton exact-overlap tasks |
|---|---:|---:|---:|---:|---:|---:|
| IOS1.1 | 61 | 35 | 55 | 48 | 0 | 54 |
| IOS2.1 | 58 | 52 | 55 | 35 | 0 | 54 |
| IOS3.1 | 26 | 17 | 23 | 16 | 0 | 23 |
| **Total** | **145** | **104** | **133** | **99** | **0** | **131** |

Inclusive exact-component overlap occurs in `133` tasks; the exclusive `OVERLAP` classifier bucket is `104` because strict subset/superset is reported separately. There are `8` strict-containment 1→1/FUNCTION_DISTRIBUTED pairs in `3` tasks.

No explicit contradiction candidates were found. Capacity-key defects: `0`; new capacity conflicts: `0`; candidate recall changes: `0`. There were no pre-existing FULL/PARTIAL labels to falsify. One unsafe would-be classification was identified: `structural subset ⇒ task-level PARTIAL` is false for R26 because the task scope contains only its domestic-pressure fragment.

## LEFT19 control

| Candidate | Rank (display only) | Function/component | Scope evidence | Explicit contradictions |
|---|---:|---|---|---|
| R30 `lcand_26bcd544f168ff9ccea5` | 1 | HOT_WATER / HOT_WATER_DISTRIBUTION | Корпус №4; stronger floor, corpus/zone, function evidence, consumers and topology overlap | none |
| R25 `lcand_c725393a11cb3b17ed2d` | 2 | HOT_WATER / HOT_WATER_DISTRIBUTION | Корпус №4; weaker on those fields, stronger on some systems/stable-entity tokens | none |

R30 is deterministically better supported, but R25 is not structurally invalid: both have exact ontology class/role, object/corpus, document role and no contradiction. Thus 6/6 for R30 demonstrates stable preference, not that the historical ambiguity has been resolved as truth.

## Safety and architecture options

- **Option A — deterministic eligibility:** unsafe on the frozen schema. Excluding R26 as PARTIAL would suppress a complete continuation of the actual atomic task.
- **Option B — explicit coverage fact:** useful only if accompanied by an explicit scope identifier and required component set. A bare FULL/PARTIAL flag is ambiguous; the forensic artifacts therefore expose both atomic-task and declared-composite scopes.
- **Option C — legitimate ambiguity:** the current PASS_DISAGREEMENT is correct fail-closed behavior for this frozen smoke. It should remain until granularity is represented consistently.

The statement “a STRICT PARTIAL candidate must not automatically beat a FULL candidate” cannot safely drive eligibility here: strict partiality is not established without choosing composite scope over the task's declared atomic scope.

## Verdict

**D — candidate task projection / fragmentation defect.** R26 is a valid atomic FUNCTIONAL_ANALOGUE, not an erroneous generation hypothesis. The architectural defect is competition between a one-fragment candidate and a three-fragment atomic group inside a task whose passport and identity name only one fragment. Option B is the safest diagnostic direction, but a later design must first make the selection scope explicit; no selector, prompt, eligibility, deployment, shadow, or materialization change is made in this forensic phase.

Model calls = `0`.
