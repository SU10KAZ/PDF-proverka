# Checklist update plan — proposed changes

**Дата:** 2026-05-21
**Цель:** конкретные предложения по правкам `backend/app/data/discipline_checklists/`
до старта completeness_runner. **НЕ применяется автоматически** — это
proposed-changes для human review.

## Структура изменений

Изменения разбиты на 4 категории:

1. **Normative refresh** — обновление номеров действующих норм
2. **Severity adjustments** — повышения/понижения severity
3. **Status reclassification** — mandatory → conditionally_mandatory или
   recommended; recommended → drop
4. **Add applicability gates** — добавить условия `только если ...`

Все правки нумеруются `<DISC>-<NN>` ссылками на matrix.

---

## 1. Normative refresh

**Найти и заменить во всех 8 файлах:**

| Old | New | Затронутые items |
|---|---|---|
| `ГОСТ Р 21.1101-2013` | `ГОСТ Р 21.101-2020` | AR-01, AR-07, AR-09; MULTI-01, MULTI-02, MULTI-03, MULTI-05 |

**Сверить через mcp__norms / WebSearch:**

| Item | Текущая ссылка | Проблема |
|---|---|---|
| AR-08 | СП 50.13330.2012 | Точный пункт по инсоляции — лучше СанПиН 1.2.3685-21 п. 79 |
| EOM-05 | СП 256.1325800.2016 п. 6.4 | Точный пункт нужно сверить |
| EOM-07 | СП 256.1325800.2016 п. 8.5 | То же |
| KJ-08 | СП 63.13330.2018 п. 8.1.46 | То же |
| KJ-16 | СП 63.13330.2018 п. 10.3.5 | То же |
| VK-20 | СанПиН 2.1.4.1074-01 | Возможно заменён на СанПиН 1.2.3685-21 / 2.1.3684-21 |

**Добавить параллельные ссылки СП к ПУЭ-7 ссылкам:**

| Item | Текущая | Добавить параллель |
|---|---|---|
| EOM-01 | ПУЭ-7 п. 1.2.18 | + СП 256.1325800.2016 разд. 5 |
| EOM-02 | ПУЭ-7 гл. 1.7 | + СП 437.1325800.2018 |
| EOM-19 | ПУЭ-7 гл. 1.8 | + ГОСТ Р 50571.16-2019 (уже есть) |
| EOM-24 | ПУЭ-7 п. 7.1.71-7.1.83 | + СП 256.1325800.2016 разд. 7 |
| SS-04 | ПУЭ-7 гл. 1.7 | + ГОСТ Р 50571.5.54 |

---

## 2. Severity adjustments

### Понизить severity (downgrades)

| Item | Текущая | Новая | Причина |
|---|---|---|---|
| AR-01 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | conditional: ПРОВЕРИТЬ_ПО_СМЕЖНЫМ только для ПД | ПЗ может быть в томе ПД |
| AR-05 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | conditional: ПРОВЕРИТЬ_ПО_СМЕЖНЫМ только для ПД | ПБ — отдельный раздел |
| AR-07 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | РЕКОМЕНДАТЕЛЬНОЕ | Штамп — формальный атрибут |
| AR-09 | ЭКСПЛУАТАЦИОННОЕ | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | ТЭП в ПЗ |
| EOM-04 | КРИТИЧЕСКОЕ | условно КРИТИЧЕСКОЕ для ПД, drop для РД с ссылкой | Может быть в ПД |
| EOM-16 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | Coordination |
| EOM-17 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | АПС-домен |
| EOM-18 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Coordination |
| EOM-19 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Программа испытаний — отдельный документ |
| KJ-04 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | ППР |
| KJ-17, 18 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | Coordination |
| KJ-20 | РЕКОМЕНДАТЕЛЬНОЕ | drop | ППР |
| KJ-22 | РЕКОМЕНДАТЕЛЬНОЕ | drop | ППР |
| KM-04 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | ППР |
| KM-17, 18, 19 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ / РЕКОМЕНДАТЕЛЬНОЕ | drop | Coordination |
| KM-20 | РЕКОМЕНДАТЕЛЬНОЕ | drop | ППР |
| KM-22 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Исполнительная |
| OV-01 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop для РД | Атрибут тома ПД |
| OV-04 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | conditional только для МКД ≥ 28 м | См. ниже |
| OV-17–20 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | Coordination |
| OV-21 | РЕКОМЕНДАТЕЛЬНОЕ | drop | ПНР — отдельный документ |
| OV-25 | ЭКСПЛУАТАЦИОННОЕ | **delete** | Дубль с VK |
| VK-01 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop для РД | |
| VK-16–19 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ / РЕКОМЕНДАТЕЛЬНОЕ | drop | Coordination |
| VK-20 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Отдельный документ |
| SS-16–19 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | Coordination |
| SS-20 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Отдельный документ |
| SS-21 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Атрибут спец. ТЗ |
| MULTI-05–13 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | drop | Cross-section, нужен другой pipeline |
| MULTI-17–19 | РЕКОМЕНДАТЕЛЬНОЕ | drop | Атрибут комплекта |

### Повысить severity (upgrades)

| Item | Текущая | Новая | Причина |
|---|---|---|---|
| AR-14 | ЭКСПЛУАТАЦИОННОЕ | **КРИТИЧЕСКОЕ** | Эвакуация в МКД/общ. — life safety |
| AR-06 | ПРОВЕРИТЬ_ПО_СМЕЖНЫМ | КРИТИЧЕСКОЕ при явном отсутствии | То же |

---

## 3. Status reclassification

Изменения нормативного статуса в matrix:

| Item | Текущее восприятие | Новый normative_status | Причина |
|---|---|---|---|
| AR-01, AR-05, AR-06 | mandatory | conditionally_mandatory (только ПД) | ПП РФ 87 |
| AR-08, AR-21 | mandatory | conditionally_mandatory (жилые МКД) | СанПиН/СП |
| AR-13, AR-22, AR-23 | mandatory | conditionally_mandatory (наличие элемента) | |
| AR-15, AR-16, AR-17 | mandatory | recommended + cannot-report | Coordination |
| EOM-04, EOM-06 | mandatory | conditionally_mandatory (ПД vs РД) | |
| EOM-16, EOM-17, EOM-18, EOM-19 | mandatory | recommended + cannot-report | Coordination |
| EOM-20 | mandatory | conditionally_mandatory (большие двигатели) | |
| EOM-21, EOM-23 | mandatory | conditionally_mandatory (I категория) | |
| KJ-04, KJ-17, KJ-18, KJ-20 | mandatory | recommended + cannot-report | ППР / Coordination |
| KJ-23, KJ-24, KJ-25 | mandatory | conditionally_mandatory (высота/сейсмика/подз.) | |
| KM-04, KM-17, KM-18, KM-19, KM-20 | mandatory | recommended + cannot-report | |
| KM-02, KM-03 | mandatory | conditionally_mandatory | |
| OV-04 | mandatory | conditionally_mandatory (МКД ≥ 28 м) | |
| OV-11, OV-13, OV-23, OV-24 | mandatory | conditionally_mandatory | |
| OV-17, OV-18, OV-19, OV-20, OV-21 | mandatory | recommended + cannot-report | |
| VK-01, VK-03 | mandatory | conditionally_mandatory | |
| VK-13, VK-14, VK-22, VK-23, VK-24, VK-25 | mandatory | conditionally_mandatory (наличие) | |
| VK-16–20 | mandatory | recommended + cannot-report | |
| SS-02 | mandatory | conditionally_mandatory (СП 486) | |
| SS-03, SS-05, SS-07, SS-13, SS-23, SS-24, SS-25 | mandatory | conditionally_mandatory | |
| SS-16–20 | mandatory | recommended + cannot-report | |
| MULTI-01, MULTI-05–19 | mandatory | recommended + cannot-report | Cross-section |

**Итого:** ~80 items требуют изменения статуса.

---

## 4. Add applicability gates

Добавить условные блоки `# Conditional items (by stage / object signal)` с
явными гейтами:

### Stage gate

Применяется к items, чей статус — атрибут ПД-тома (не РД-марки):

```
- [stage_gate=ПД]
  AR-01 Пояснительная записка АР
  AR-09 ТЭП
  EOM-01 Общие данные ЭОМ
  EOM-04 Электробаланс
  EOM-06 Расчёт КЗ
  KJ-05–08 Расчёты КЖ
  KM-05–08 Расчёты КМ
  OV-01, OV-05 Общие данные ОВ, расчёт воздухообмена
  VK-01, VK-05 Общие данные ВК, Q_расч
  SS-01, SS-05–07 Общие данные SS, расчёты
```

### Object_signal gate (эвристика по тексту MD)

```
- [object_signal=жилой_МКД]
  AR-08 Расчёт инсоляции
  AR-21 КЕО
  SS-23 Домофония
  OV-25 → удалить или перенести в VK

- [object_signal=высотный]   # MD упоминает "≥ 75 м" или "высотное"
  KJ-23 Расчёт ветровых пульсаций

- [object_signal=сейсмо]    # MD упоминает сейсмо-район
  KJ-24 Сейсмика

- [object_signal=подземная_часть]
  KJ-25 Гидроизоляция

- [object_signal=АПС]
  SS-02 АПС
  SS-06 Расчёт извещателей

- [object_signal=МКД_≥28м]
  OV-04 Противодымная вентиляция

- [object_signal=I_категория]
  EOM-21 АВР
  EOM-23 Резервное питание
```

---

## 5. Items to delete entirely

| Item | Причина |
|---|---|
| OV-25 Вентиляция канализационных стояков | Дубль с VK |

---

## 6. Recommended new section в каждом checklist file

Добавить в шапку каждого `<DISC>.md`:

```markdown
## Stage applicability

Items в этом чек-листе применимы на следующих стадиях:
- ПД-том целиком: items <list>
- РД-марка (отдельный том марки): items <list>
- Спецификация (выгрузка): items <list>

Phase 1 completeness_runner ОБЯЗАН использовать
`stage` из project_info.json (если введён) или вынести гипотезу
«стадия не определена» и понизить severity до ПРОВЕРИТЬ_ПО_СМЕЖНЫМ для
ПД-only items.
```

---

## 7. Application order (когда будем применять)

1. **Phase A — normative refresh.** Заменить устаревшие номера ГОСТ Р 21.1101-2013. Минимальный риск.
2. **Phase B — anti-pattern enrichment.** Добавить новые anti-patterns
   (stage-mismatch, doc-type-mismatch, coordination-as-missing).
3. **Phase C — cannot-report flags.** Маркировать ~46 items как
   `can_be_reported_as_missing=false`. Их prompt не должен генерировать
   findings, но runner должен учитывать их в classification.
4. **Phase D — severity adjustments.** Conditional gates по stage и
   object_signal.
5. **Phase E — delete duplicates.** OV-25, etc.

Каждая фаза — отдельный sub-task с human review перед applied.
