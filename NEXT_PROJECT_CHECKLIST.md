# Чек-лист: запуск нового проекта

## Подготовка

- [ ] Создать папку `projects/<ДИСЦИПЛИНА>/<ИМЯ_ПРОЕКТА>/`
- [ ] Положить PDF в папку
- [ ] Создать `project_info.json`:
  ```json
  {
    "project_id": "<ДИСЦИПЛИНА>/<ИМЯ_ПРОЕКТА>",
    "name": "<ИМЯ_ПРОЕКТА>",
    "section": "<ДИСЦИПЛИНА>",
    "description": "Описание",
    "pdf_file": "имя_файла.pdf"
  }
  ```
- [ ] Запустить подготовку: `python process_project.py projects/<ДИСЦИПЛИНА>/<ИМЯ_ПРОЕКТА>`
- [ ] Запустить кропинг блоков: `python blocks.py crop projects/<ДИСЦИПЛИНА>/<ИМЯ_ПРОЕКТА>`

## Проверка готовности

- [ ] MD-файл есть (`*_document.md`) или текст извлечён (`_output/extracted_text.txt`)
- [ ] Блоки готовы (`_output/blocks/*.png` + `index.json`)
- [ ] Document graph создан (`_output/document_graph.json`)
- [ ] `project_info.json` содержит `text_source`

## Запуск pipeline

- [ ] Запустить webapp: `cd webapp && python main.py`
- [ ] На дашборде найти проект → **Полный аудит**
- [ ] Дождаться завершения (статус COMPLETED)

## Проверка результатов

- [ ] Посчитать метрики: `python tools/quality_metrics.py projects/<...>/_output`
- [ ] Проверить ключевые метрики:

| Метрика | Ожидание |
|---------|----------|
| pipeline.errors | 0 |
| pipeline.completed | 11/11 |
| findings.evidence_coverage | >90% |
| findings.related_block_ids_coverage | >80% |
| findings.norm_quote_coverage | >20% |
| norms.deterministic_count | >80% от total |

## Сборка артефактов

- [ ] Собрать output zip (см. RUNBOOK.md, п.5)
- [ ] Сохранить `metrics_summary.json`

## Сравнение (если есть baseline)

- [ ] Сравнить findings total (±20% от baseline — в пределах нормы)
- [ ] Сравнить evidence_coverage (не должна падать)
- [ ] Сравнить pipeline errors (должно быть 0)
- [ ] Отметить регрессии / улучшения

## Финализация

- [ ] Обновить базу норм: `python norms.py update --all`
- [ ] Сгенерировать Excel: `python generate_excel_report.py projects/<...>`
- [ ] Архив output отправлен / сохранён
