# Конфигурация прогона: Opus (тяжёлые) + GPT block_batch + Sonnet (critics)

| Этап | Модель |
|------|--------|
| text_analysis | Opus |
| block_batch | GPT-5.4 |
| findings_merge | Opus |
| findings_critic | Sonnet |
| findings_corrector | Sonnet |
| norm_verify | Opus |
| norm_fix | Sonnet |
| optimization | Opus |
| opt_critic | Sonnet |
| opt_corrector | Sonnet |

Результат: 12 замечаний, 3 оптимизации, 18.6 мин
