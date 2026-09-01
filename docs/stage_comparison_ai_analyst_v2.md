# Stage Comparison AI Analyst v2

AI Analyst v2 — экспериментальный слой над завершённым FAST-прогоном. Он не
меняет распознавание, сопоставление, нормативный контур, решения инженера или
production STANDARD. Входом служит замороженный набор JSON-артефактов FAST,
выход записывается в отдельный каталог эксперимента.

## Запуск

Функция по умолчанию выключена. Для явного запуска нужны feature flag и каталог
вне `production`:

```bash
STAGE_COMPARISON_AI_ANALYST_V2=true python3 \
  scripts/stage_comparison_ai_v2_experiment.py \
  comparison/sessions/<session>/pairs/<pair>/production \
  --pair-id <pair> \
  --output-dir comparison/ai_analyst_v2/<run>
```

Команда проверяет Session Gateway, строит один неизменяемый evidence set и
последовательно сравнивает `gpt-5.6-sol` с reasoning `low` и `medium`. На один
режим допускается не более четырёх сессий и двух контролируемых доборов.

## Архитектура

Поток данных:

```text
FAST artifacts
  -> complete unresolved inventory
  -> Sheet Context (Level 1) + Focused Evidence (Level 2)
  -> typed analyst batches
  -> deterministic verifier v2
  -> AI_RESOLVED_VERIFIED or HUMAN_REQUIRED
  -> deterministic Human Review Orchestrator
  -> experimental Preliminary Report
```

Sheet Context содержит обе стороны, секции, сущности, связи, строки таблиц,
режимы, FAST-находки, противоречия и качество распознавания. Focused Evidence
содержит кандидатов и локальные связи конкретной задачи. Все записи имеют
стабильные `evidence_ref`; модель не получает пути к исходным файлам.

Инвентаризация разделяет каждый нерешённый элемент на:

- `AI_ELIGIBLE`;
- `AI_INELIGIBLE_NO_EVIDENCE`;
- `AI_INELIGIBLE_POLICY`;
- `AI_INELIGIBLE_HUMAN_AUTHORITY`.

Для каждого элемента существует ровно один результат. Несовпадение множества
результатов с inventory считается ошибкой, а не скрыто пропускается.

## Проверка и безопасность

Модель отвечает по строгой JSON Schema и не может выставить Engineer APPROVED.
Verifier проверяет существование ссылок и кандидатов, стороны, типы и секции,
значения и единицы, графовые отношения и арифметику. Низкоуверенная FAST-находка
не может доказать собственное значение: интерпретация изменения требует
независимого положительного evidence обеих сторон.

Session Gateway сохраняет отключённые shell, repository/filesystem, browser,
plugins и vision по умолчанию, allowlist окружения, отсутствие секретов,
timeout, cancellation, cleanup, content-addressed cache и audit trail.
Дополнительное evidence выбирает только backend из закрытого справочника уже
замороженного контекста.

## Артефакты эксперимента

Каталог запуска содержит:

- `fast_baseline.json`;
- `sheet_context.json`, `focused_evidence.json`, `evidence_catalog.json`;
- `unresolved_inventory.json`;
- `<effort>/run.json`, `preliminary_report.json`, `manual_audit.json`;
- `<effort>/human_review_plan.json` — атомарные targets, корневые группы,
  самостоятельные вопросы и информационные ограничения;
- `pre_ai_human_review_plan.json` — классификация, применённая до AI routing;
- `ab_comparison.json`, `runtime_check.json`, `response_cache/`.

Публикация в production и подключение к STANDARD этим скриптом не выполняются.
