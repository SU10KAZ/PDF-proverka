# Checklist Examples — какой пункт когда сработает / когда нет

**Дата:** 2026-05-20

5 примеров: checklist-пункт → trigger MD-state → suppress (anti-pattern) MD-state.

Reference: [checklist_quality_report.md](../../algorithm_research/reports/checklist_quality_report.md),
`production_preparation/checklists/AR.md`, `production_preparation/checklists/KJ.md`.

---

## 1. AR — Расчёт инсоляции жилых помещений

**Checklist item:**
```
[problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
Расчёт инсоляции жилых помещений (для жилых МКД)
(СП 50.13330.2012, СанПиН 1.2.3685-21).
```

**Trigger MD-state (item срабатывает):**
```markdown
# Раздел АР — МКД, 17 этажей
## Технико-экономические показатели
- Этажность: 17
- Площадь квартир: 9450 м²
- Жилых квартир: 144
[...нет упоминания инсоляции в тексте всего раздела...]
```

→ Lens выдаст finding:
```json
{
  "problem_class": "missing_mandatory_parameter",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "problem": "Расчёт инсоляции жилых помещений не представлен",
  "norm": "СП 50.13330.2012; СанПиН 1.2.3685-21"
}
```

**Suppress MD-state (anti-pattern — item НЕ срабатывает):**
```markdown
# Раздел АР
## Расчёт инсоляции
Расчёт инсоляции выполнен в соответствии с СанПиН 1.2.3685-21.
Минимальная продолжительность инсоляции для жилых комнат — 2,5 часа.
Для квартир секции 1: 3,2 часа (соответствует).
[...]
```

→ MD упоминает инсоляцию + данные есть → lens НЕ выдаёт finding.

---

## 2. EOM — Кабельный журнал

**Checklist item:**
```
[problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
Кабельный журнал отходящих линий с указанием кабеля, длины, сечения, типа защиты
(ГОСТ 21.613-2014).
```

**Trigger (full_rd):**
```markdown
# Раздел ЭОМ
## Содержание
1. ПЗ
2. Расчёт нагрузок
3. Однолинейная схема
4. Спецификация
[нет упоминания "кабельный журнал" нигде]
```

→ Lens выдаст `missing_mandatory_schedule`.

**Suppress 1 — есть журнал:**
```markdown
## Кабельный журнал
| Кабель | Длина | Сечение | Защита |
|---|---|---|---|
| W1 | 35 м | 4×16 | АВ 32A |
| W2 | 22 м | 4×10 | АВ 25A |
```

→ Не triggered.

**Suppress 2 — anti-pattern для non-full_rd (`document_type=audit_comparison`):**
```markdown
# Сравнение нагрузок ЭОМ vs ОВ
| Система | По ЭОМ | По ОВ |
[табл. ...]
```

→ Lens **НЕ** выдаст `missing_mandatory_schedule`, потому что:
- HARD RULE: на audit_comparison запрещено flag'ать "отсутствует кабельный
  журнал" — он не в scope сравнения.
- KILL-LIST в [stage01_production_prompt.md](../prompts/stage01_production_prompt.md) §"СТРОГИЙ ЗАПРЕТ".

---

## 3. KJ — Защитный слой бетона арматуры

**Checklist item:**
```
[problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
Защитный слой бетона для рабочей арматуры по классу среды эксплуатации
(СП 28.13330.2017).
```

**Trigger:**
```markdown
# Раздел КЖ — Фундаментная плита
Класс среды: XC3 (умеренно агрессивная).
Класс бетона: B25.
[нет упоминания защитного слоя]
```

→ Lens flag'нет: `Защитный слой бетона не задан для класса среды XC3`.

**Suppress:**
```markdown
Защитный слой:
- Нижняя арматура: 50 мм (XC3).
- Верхняя арматура: 35 мм.
```

→ Не triggered.

**Edge case (был в FP-аудите, vk_03):**
```markdown
Защитный слой по проекту не указан — выполнить по нормативу.
```

→ Это **намеренная пометка для подрядчика**. Anti-pattern в checklist'е
говорит: если есть фраза "выполнить по нормативу" / "согласно СП" — не
flag'ать как critical, downgrade к РЕКОМ.

---

## 4. OV — Подпор воздуха в незадымляемую лестничную клетку

**Checklist item:**
```
[problem_class=missing_mandatory_section, severity=КРИТИЧЕСКОЕ, applies=full_rd]
Подпор воздуха в незадымляемую лестничную клетку и шахту лифта
(СП 7.13130.2013, ФЗ-123).
```

**Trigger (full_rd, дом > 28 м):**
```markdown
# Раздел ОВ — Жилой дом, 25 этажей, высота 76 м
## Системы вентиляции
- Приточная вентиляция: ...
- Вытяжная: ...
[нет упоминания подпора в ЛК или шахту лифта]
```

→ Lens flag'нет КРИТ — без подпора 25-этажный дом не сдаётся.

**Suppress:**
```markdown
## Подпор воздуха
- В лестничную клетку Н2: Q = 14400 м³/ч (расчёт по СП 7.13130).
- В шахту лифта: Q = 3600 м³/ч.
```

**Edge case:**
```markdown
Здание этажностью 6 (высота 18 м).
```
→ Дом < 28 м → подпор не обязателен → checklist item smart enough НЕ
trigger'ить. Это обработано через `applies=full_rd` + дополнительная
эвристика в prompt'е ("если высота < 28 м — пункт не применим").

Будущее улучшение (Phase 2 в [FINAL_SUMMARY §6](../../algorithm_research/reports/FINAL_SUMMARY.md)):
conditional pieces по object type.

---

## 5. KM — Огнезащита несущих стальных конструкций

**Checklist item:**
```
[problem_class=missing_mandatory_section, severity=КРИТИЧЕСКОЕ, applies=full_rd]
Огнезащита несущих стальных конструкций (REI 60+ для I степени огнестойкости)
(ФЗ-123, СП 2.13130.2020).
```

**Trigger (full_rd):**
```markdown
# Раздел КМ — Стропильные фермы покрытия
Сечения ферм: 2L 80×80×8 (раскосы), 2L 100×100×10 (пояса).
Связи: Ø6 A240.
[нет упоминания огнезащиты]
```

→ Lens flag'нет КРИТ.

**Suppress:**
```markdown
## Огнезащита
Огнезащитное покрытие "Огракс СК-1" толщиной 1,5 мм (REI 90 по протоколу испытаний).
```

**Anti-pattern (FP-trap):**
```markdown
Сварочные электроды: Э42, Э50А.
```

В checklist'е специально перечислен `false_positive_trap`: упоминание
сварочных электродов Э42/Э50А — **не** trigger для "огнезащита". Это про
сварочные швы, не про покрытия.

См. [checklist_quality_report.md](../../algorithm_research/reports/checklist_quality_report.md)
"Anti-pattern блоки".

---

## Структура checklist item (формальная)

```
[problem_class=<class>, severity=<sev>, applies=<csv_doc_types>]
<Описание пункта>
(<норма-источник>).
```

- `applies` — список через `|`: например `full_rd|specification_only`.
- `applies=full_rd` (default) — только для полных РД.
- Anti-pattern блоки — отдельная секция `## Anti-patterns` с явными
  trigger-strings.

---

## Какой checklist для какой дисциплины

| Discipline | Checklist file | LOC | Phase 1 ready? |
|---|---|---|---|
| AR | `discipline_checklists/AR.md` | ~120 | YES |
| KJ | `discipline_checklists/KJ.md` | ~120 | YES |
| KM | `discipline_checklists/KM.md` | ~120 | YES (новый, [FINAL_SUMMARY §6](../../algorithm_research/reports/FINAL_SUMMARY.md)) |
| EOM | `discipline_checklists/EOM.md` | ~120 | YES |
| OV | `discipline_checklists/OV.md` | ~120 | YES |
| VK | `discipline_checklists/VK.md` | ~120 | YES |
| SS | `discipline_checklists/SS.md` | ~120 | YES |
| MULTI / CROSS | `discipline_checklists/MULTI.md` | ~80 | YES (для cross-discipline аудитов) |

Каждый checklist:
- Имеет mandatory + recommended + anti-pattern блоки.
- Версионируется (commit history = revision history).
- Должен ревьювится discipline expert минимум раз в год.

---

## Когда checklist updated

| Сценарий | Action |
|---|---|
| Норма обновилась | Update reference + дата редакции |
| Новый FP-trap обнаружен (через canary feedback) | Добавить в anti-pattern block |
| Новый дефект-pattern обнаружен | Добавить mandatory или recommended item |
| Старый item становится out-of-scope | Decommission (commented-out с датой) |

Каждое изменение — PR с обоснованием. Не "автоматическое".
