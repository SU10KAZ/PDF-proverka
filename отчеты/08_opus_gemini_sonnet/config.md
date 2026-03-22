# Конфигурация прогона: Opus (тяжёлые) + Gemini (blocks) + Sonnet (critics)

| Этап | Модель |
|------|--------|
| text_analysis | Opus |
| block_batch | Gemini |
| findings_merge | Opus |
| findings_critic | Sonnet |
| findings_corrector | Sonnet |
| norm_verify | Opus |
| norm_fix | Sonnet |
| optimization | Opus |
| opt_critic | Sonnet |
| opt_corrector | Sonnet |

Результат: 15 замечаний, 3 оптимизации, 24.3 мин — ЛУЧШИЙ
