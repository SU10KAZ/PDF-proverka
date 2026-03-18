# Release Candidate — Система аудита проектной документации

**Версия:** RC-1
**Дата:** 2026-03-18
**Объект:** 213. Мосфильмовская 31А "King&Sons"
**Подтверждающий прогон:** АР 133-23-ГК-АР1 (Архитектурные решения)

---

## 1. Реализованные возможности

### Ядро конвейера
- **Structured context** — Document Knowledge Graph (`document_graph.json`): text_block_id + image_block_id для каждой страницы
- **Блочный метод** — кропинг чертежей по координатам OCR вместо тайл-сетки (5× экономия токенов)
- **Adaptive batching** — группировка блоков по размеру с контролем MAX_BLOCKS и MAX_SIZE_KB
- **Findings merge** — свод T-замечаний (текст) и G-замечаний (графика) в единые F-замечания с дедупликацией

### Проверка качества
- **Selective Critic** — проверка только risky findings (нет evidence, low confidence), пропуск well-grounded
- **Grounding layer** — автоматический поиск grounding_candidates для ungrounded findings
- **Evidence trассировка** — каждое замечание содержит `evidence[]` с типом и block_id

### Нормативная база
- **Deterministic norms layer** — статус документа (active/replaced/cancelled) определяется Python из `norms_db.json`, не LLM
- **Paragraph cache** (`norms_paragraphs.json`) — подтверждённые цитаты пунктов норм для повторного использования
- **norm_quote + norm_confidence** — каждое замечание содержит цитату нормы и уверенность
- **Восстановление norm_quote** — Python-fallback после corrector + обогащение из paragraph_checks
- **Normalize paragraph key** — стабильные ключи paragraph cache (убираются "(действует)", "(ред...)")

### Оптимизация
- **Optimization pipeline** — анализ спецификаций, замена аналогов, упрощение монтажа
- **Optimization Critic + Corrector** — проверка vendor-листа, savings, traceability
- **Pre-check + fallback** — валидация JSON до corrector, восстановление из pre_review при ошибке

### Веб-приложение
- **API pagination + cache** — пагинация findings, TTL-кеш проектов
- **WebSocket live-log** — прогресс аудита в реальном времени
- **Batch queue** — последовательный аудит нескольких проектов
- **Pause/Resume** — пауза конвейера с двумя режимами
- **Per-stage model selection** — Sonnet/Opus для разных этапов

### Модульная система дисциплин
- **ASCII-реестр** (`_registry.json`) — 13 дисциплин, folder_patterns, drag-and-drop порядок
- **Полные профили** — EM, OV, АР, АИ, ТХ, ВК, КМ (role.md, checklist.md, norms_reference.md)

### Тесты и CI
- **122 теста** (0 failures) — batching, grounding, norms, paragraph cache, integration AR1, selective critic, API
- **Offline AR1 integration** — структура findings, norm_checks, pipeline_log без вызова Claude CLI

### Excel-отчёт
- **Генерация Excel** — лист per project, сводка, оптимизации
- **Устойчивость** — try/except per finding/optimization, защита от PermissionError

---

## 2. Подтверждение реальным AR1 прогоном (2026-03-18)

| Метрика | Значение |
|---------|----------|
| Pipeline stages | **11/11 done** |
| Pipeline errors | **0** |
| Findings total | **60** |
| evidence_coverage | **100.0%** |
| related_block_ids_coverage | **90.0%** |
| norm_quote_coverage | **26.7%** |
| Deterministic norm checks | **30/34** (88%) |
| WebSearch norm checks | **4/34** |
| paragraph_cache_verified | **3/10** |
| Needs revision (norms) | **7** |
| Policy violations | **0** |
| By severity | КРИТИЧЕСКОЕ: 8, СУЩЕСТВЕННОЕ: 8, ЭКСПЛУАТАЦИОННОЕ: 3, РЕКОМЕНДАТЕЛЬНОЕ: 26, ПРОВЕРИТЬ: 15 |

---

## 3. Зоны роста

| Направление | Текущее | Цель | Как достичь |
|-------------|---------|------|-------------|
| norm_quote_coverage | 26.7% | >50% | Улучшить промпты для заполнения norm_quote, расширить paragraph cache |
| paragraph_cache | 3/10 verified | >70% hit rate | Накопление подтверждённых цитат через прогоны |
| findings_corrector | Работает | Стабильнее | Corrector уже сохраняет norm_quote (правило #7) |
| Покрытие дисциплин | 7 профилей | 13 | Создать профили для ПТ, ИТП, ГП, ПС, ПОС, СС |

---

## 4. Статус

**READY FOR PILOT USAGE**

Система прошла полный цикл на реальном проекте АР1 (60 замечаний, 11/11 этапов, 0 ошибок). Пригодна для пилотных прогонов на других проектах с контролем качества через metrics.
