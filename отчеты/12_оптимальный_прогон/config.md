# Конфигурация прогона: Оптимальная связка (по результатам 11 прогонов)

Проект: **133_23-ГК-ТХ.М**

| Этап | Модель | Через |
|------|--------|-------|
| text_analysis (01) | Opus | Claude CLI |
| block_batch (02) | GPT-5.4 | OpenRouter |
| findings_merge (03) | Opus | Claude CLI |
| findings_critic (03b) | Sonnet | Claude CLI |
| findings_corrector (03b) | Sonnet | Claude CLI |
| norm_verify (04) | Opus | Claude CLI |
| norm_fix (04b) | Sonnet | Claude CLI |
| optimization (05) | Opus | Claude CLI |
| opt_critic (05b) | Sonnet | Claude CLI |
| opt_corrector (05b) | Sonnet | Claude CLI |

Источник: прогон #07 (opus_gpt_sonnet)
Результат: 12 замечаний, 3 оптимизации, 18.6 мин

## Обоснование выбора

- **Opus CLI** для аналитических этапов (текст, свод, нормы, оптимизация) — максимальная глубина анализа
- **GPT-5.4** для блоков — быстрый и качественный визуальный анализ
- **Sonnet CLI** для critic/corrector — достаточная точность при меньшей стоимости
