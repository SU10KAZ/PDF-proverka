# Runbook — Операционное руководство

## 1. Проверка кода перед прогоном

```bash
cd "D:\Отедел Системного Анализа\1. Calude code"

# Компиляция
python -m compileall . -q

# Тесты
python -m pytest tests/ -v
```

Ожидание: 0 ошибок компиляции, 122+ passed.

---

## 2. Запуск webapp

```bash
cd "D:\Отедел Системного Анализа\1. Calude code\webapp"
python main.py
```

Дождаться: `Uvicorn running on http://0.0.0.0:8080`

Дашборд: http://localhost:8080

---

## 3. Запуск pipeline для проекта

### Через дашборд
1. Открыть http://localhost:8080
2. Найти проект → кнопка **«Аудит»** → **«Полный аудит»**
3. Следить за WebSocket-логом в реальном времени

### Через API
```bash
# Запуск
curl -X POST "http://localhost:8080/api/audit/<ДИСЦИПЛИНА>/<ИМЯ_ПРОЕКТА>/start"

# Статус
curl "http://localhost:8080/api/audit/live-status"

# Пример для АР1
curl -X POST "http://localhost:8080/api/audit/АР/133-23-ГК-АР1/start"
```

### Очистка перед повторным прогоном (PowerShell)
```powershell
cd "projects\<ДИСЦИПЛИНА>\<ПРОЕКТ>\_output"
Get-ChildItem *.json | Where-Object { $_.Name -notmatch "block" } | Remove-Item
```

Блоки (`blocks/`, `block_batch*.json`) НЕ удалять — пересоздание стоит времени и токенов.

---

## 4. Сбор метрик после прогона

```bash
cd "D:\Отедел Системного Анализа\1. Calude code"
python tools/quality_metrics.py "projects/<ДИСЦИПЛИНА>/<ПРОЕКТ>/_output"
```

Файл `metrics_summary.json` будет создан в `_output/`.

### Все baselines разом
```bash
python tools/quality_metrics.py --all-baselines
```

---

## 5. Сборка output в zip

```bash
python -c "
import zipfile, os
from pathlib import Path

project = 'projects/<ДИСЦИПЛИНА>/<ПРОЕКТ>/_output'
out_name = '<ПРОЕКТ>_outputs.zip'

files = [
    '03_findings.json',
    '03_findings_review_input.json',
    '03_findings_review.json',
    '03_findings_pre_review.json',
    'norm_checks.json',
    'optimization.json',
    'optimization_review.json',
    'pipeline_log.json',
    'metrics_summary.json',
]

zf = zipfile.ZipFile(out_name, 'w', zipfile.ZIP_DEFLATED)
for f in files:
    fp = Path(project) / f
    if fp.exists():
        zf.write(str(fp), f)
zf.close()
print(f'Готово: {out_name} ({os.path.getsize(out_name) // 1024} KB)')
"
```

---

## 6. Ключевые метрики для проверки

| # | Метрика | Где в metrics_summary.json | Хорошо | Проблема |
|---|---------|---------------------------|--------|----------|
| 1 | findings total | findings.total | >0 | 0 = pipeline не сработал |
| 2 | evidence_coverage | findings.evidence_coverage | >0.9 | <0.7 = слабая привязка |
| 3 | related_block_ids_coverage | findings.related_block_ids_coverage | >0.8 | <0.5 = нет трассировки |
| 4 | norm_quote_coverage | findings.norm_quote_coverage | >0.25 | <0.1 = цитаты теряются |
| 5 | deterministic_count | norms.deterministic_count | >80% от total | низкий = стоит обновить norms_db |
| 6 | websearch_count | norms.websearch_count | <20% от total | высокий = много неизвестных норм |
| 7 | paragraph_cache_verified | norms.paragraph_cache_verified | растёт | 0 = cache не работает |
| 8 | pipeline errors | pipeline.errors | 0 | >0 = смотри pipeline_log.json |

---

## 7. Обновление базы норм

```bash
# Обновить из всех проектов
python norms.py update --all

# Статистика базы
python norms.py update --stats

# Устаревшие нормы
python norms.py update --stale
```

---

## 8. Excel-отчёт

```bash
# Все проекты
python generate_excel_report.py

# Один проект
python generate_excel_report.py "projects/<ДИСЦИПЛИНА>/<ПРОЕКТ>"

# Только оптимизации
python generate_excel_report.py --type optimization
```
