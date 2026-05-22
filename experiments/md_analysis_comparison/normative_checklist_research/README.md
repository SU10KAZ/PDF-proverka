# Normative Checklist Research — Phase 1 prep

**Дата:** 2026-05-21
**Цель:** Превратить `backend/app/data/discipline_checklists/` из «экспертных
списков» в **нормативно обоснованную систему completeness validation**
до того, как будет создан `completeness_runner` и lens начнёт выдавать
missing-findings продакшен-пользователям.

## Scope

Только research, проверка существующих чек-листов на соответствие
актуальной нормативной базе РФ для проектной (ПД) и рабочей (РД)
документации жилых МКД и инфраструктуры.

**НЕ делается:**

- НЕ создаётся `completeness_runner`
- НЕ wire'ятся prompts в Stage 01
- НЕ меняется `backend/app/data/discipline_checklists/` (оригиналы остаются)
- НЕ запускается LLM
- НЕ запускается pipeline
- НЕ трогается production / staging

## Структура

```
normative_checklist_research/
├── README.md                                       # этот файл
├── sources/                                        # сводки нормативной базы
│   ├── pp_rf_87.md                                 # ПП РФ №87 — состав ПД
│   ├── gost_r_21_101_2020.md                       # СПДС — основные требования
│   ├── disciplinary_norms.md                       # СП и ГОСТы по дисциплинам
│   ├── stages_pd_rd.md                             # стадии ПД vs РД
│   └── document_type_normative_mapping.md          # как detector маппится в нормы
├── matrix/
│   ├── _data.py                                    # ЕДИНЫЙ источник правды
│   ├── build_matrix.py                             # генератор CSV+JSON
│   ├── completeness_requirements_matrix.json       # генерируется
│   └── completeness_requirements_matrix.csv        # генерируется
├── discipline_reports/
│   ├── AR.md
│   ├── KJ.md
│   ├── KM.md
│   ├── EOM.md
│   ├── OV.md
│   ├── VK.md
│   ├── SS.md
│   └── MULTI.md
├── recommendations/
│   ├── checklist_update_plan.md                    # что и как править
│   └── prompt_rules_update.md                      # что добавить в lens prompt
└── final_report.md                                 # ответы на 10 вопросов /goal
```

## Inventory исходных чек-листов

Всего `193` checklist item'а в 8 файлах backend (без учёта anti-pattern блоков,
которые не являются items, а инструкциями для runner'а):

| Discipline | Items | Anti-patterns | Mandatory | Recommended | Conditional |
|---|---:|---:|---:|---:|---:|
| AR    | 23 | 6 | 13 | 3 | 3 |
| EOM   | 25 | 8 | 15 | 2 | 4 |
| KJ    | 25 | 7 | 16 | 2 | 3 |
| KM    | 25 | 7 | 15 | 2 | 3 |
| OV    | 25 | 7 | 14 | 1 | 3 |
| VK    | 25 | 6 | 14 | 1 | 4 |
| SS    | 25 | 8 | 14 | 2 | 3 |
| MULTI | 22 | 8 | 13 | 2 | 2 |
| **Σ** | **195** | **57** | **114** | **15** | **25** |

(Точное число items может разойтись на ±2 — некоторые bullet'ы переразделены
по двум "## ..." заголовкам. См. matrix/ для точного перечня.)

## Confidence rating шкала

В matrix/ каждому item'у присвоен `confidence` для нашего normative-vердикта:

| Confidence | Что означает |
|---|---|
| `high` | Требование прямо зафиксировано в действующей норме с точным пунктом |
| `medium` | Требование выводится из действующей нормы, но точный пункт надо verifу |
| `low` | Требование — общепринятая практика, но прямой нормы нет / норма устарела |
| `unknown` | Нужна human-engineer verification |

## Когда читать что

| Задача | Файл |
|---|---|
| Понять, какие нормы взяты за основу | `sources/` |
| Per-item статус всех 193 пунктов | `matrix/completeness_requirements_matrix.csv` |
| Понять логику дисциплины | `discipline_reports/<КОД>.md` |
| План правок чек-листов | `recommendations/checklist_update_plan.md` |
| Что добавить в lens prompt до запуска | `recommendations/prompt_rules_update.md` |
| Финальный verdict + 10 вопросов | `final_report.md` |
