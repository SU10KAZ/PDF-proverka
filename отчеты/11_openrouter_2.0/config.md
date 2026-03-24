# Конфигурация прогона: OpenRouter 2.0 (Opus+Sonnet через OpenRouter, только norm_verify на CLI)

Всё через OpenRouter кроме norm_verify (WebSearch нужен CLI).

| Этап | Модель | Через |
|------|--------|-------|
| text_analysis | Opus | OpenRouter |
| block_batch | Gemini | OpenRouter |
| findings_merge | Opus | OpenRouter |
| findings_critic | GPT-5.4 | OpenRouter |
| findings_corrector | GPT-5.4 | OpenRouter |
| norm_verify | Opus | Claude CLI |
| norm_fix | — (не потребовался) | — |
| optimization | Gemini | OpenRouter |
| opt_critic | Sonnet | OpenRouter |
| opt_corrector | Sonnet | OpenRouter |

Результат: 17 замечаний, 4 оптимизации, 8.9 мин — АБСОЛЮТНЫЙ РЕКОРД
