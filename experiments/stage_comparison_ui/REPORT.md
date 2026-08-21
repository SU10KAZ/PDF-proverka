# Stage 4: вывод готового результата в «Расхождениях»

Дата проверки: 2026-08-21.

## Реальный проект

- Объект: `272_Sadovnicheskaya_76_Balchug_Esteyt`.
- Пара: `13АВ-РД-АР0.1-ПА_V2.pdf` ↔ `13АВ-РД-АР0.1-ПА_V3.pdf`.
- Session / pair: `121d764109184c13` / `p570d156f57`.
- Основной UI-источник: `text_final_comparison.json`
  (`kind=stage_comparison_text_final_comparison`).

### Фактическая сводка

| Показатель | Значение |
|---|---:|
| Строк в таблице | 8 |
| Групп `sheet_links` без расхождений | 3 |
| SAME | 314 |
| MOVED | 16 |
| CHANGED | 22 |
| REMOVED | 62 |
| ADDED | 86 |
| UNCERTAIN | 91 |

В `sheet_links.json` находится 11 групп. Восемь групп с CHANGED, REMOVED,
ADDED или UNCERTAIN показаны отдельными строками; три группы только с
SAME/MOVED в таблицу не попали. Порядок строк по первой странице П:
`4, 6, 7, 8, 9, 10, 11, 14`.

## Браузерная проверка

В Chrome 151, viewport 1920×1200, тёмная тема:

- в шапке показаны фактические `314 / 16 / 22 / 62 / 86 / 91`;
- DOM содержит 8 строк по 6 ячеек; SAME/MOVED есть только в сводке;
- каждый длинный bucket показывает 5 пунктов и `+ ещё N`;
- UNCERTAIN выведен нейтрально в отдельной колонке;
- кнопка `Открыть лист П` из CHANGED перевела viewer на П стр. 6 /
  РД стр. 5;
- при загрузке готовой пары, открытии «Расхождений» и source navigation
  зафиксировано 0 запросов к `/text-ai-review`.

Скриншот: [stage4_discrepancies_balchug_v2_v3.png](artifacts/stage4_discrepancies_balchug_v2_v3.png).

## Неизменность артефактов

Контрольные суммы до и после UI-проверки совпали:

- `text_final_comparison.json`:
  `96d6f68152c5b26a5e5d90d8e8fb3c6ae82225665efc82fbf3aeeb2cc94a84b4`;
- `text_ai_review.json`:
  `6dfc0a205af52cdd6198968083565ef96ee71687faa987a54ec7544f73e5d567`.

Алгоритмы Stage 3/4, reviewer, validator, prompt, model, matching, masks и
классификация статусов не менялись.

## Проверки

- Целевые frontend: 22 passed.
- API/shell: 17 passed.
- Все Python-тесты Stage Comparison: 142 passed.
- Production frontend build: passed.
- Полный frontend-набор: 388 passed, 7 несвязанных baseline-падений
  (тесты требуют строки, которых уже нет в `HEAD`):
  `section_optimization_card`, `stage_algorithm_guide`, `opt_norm_badge`,
  `md_page_alignment`.
