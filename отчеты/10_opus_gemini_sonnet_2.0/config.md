# Конфигурация прогона: Opus+Gemini+Sonnet 2.0 (обновлённые промпты critic)

Изменения vs #08: обновлён findings_critic промпт (калибровка строгости), обновлён opt_critic промпт (чёткие правила конфликтов)

| Этап | Модель |
|------|--------|
| text_analysis | Opus |
| block_batch | Gemini |
| findings_merge | Opus |
| findings_critic | GPT-5.4 |
| findings_corrector | Sonnet |
| norm_verify | Opus |
| norm_fix | Sonnet |
| optimization | Gemini |
| opt_critic | Sonnet |
| opt_corrector | — (не потребовался) |

Результат: 16 замечаний, 4 оптимизации (все pass!), 19.4 мин — ЛУЧШИЙ РЕЗУЛЬТАТ
