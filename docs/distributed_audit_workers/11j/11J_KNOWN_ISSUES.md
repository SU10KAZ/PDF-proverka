# 11J.1 — известные ограничения

## Закрыто этой задачей

- KI-11J-1: живой `.128 ↔ .31` HTTPS multi-provider E2E — закрыт 111/111.
- KI-11J-3: result package не несёт routing hash/provenance — закрыт; mismatch
  отклоняется импортёром.
- Молчаливый fallback frozen plan — закрыт маркерами FOUND/NOT_FOUND/INVALID и
  fail-closed для новых contract-v1 jobs.

## Остаётся

| ID | Ограничение | Влияние на 11J.1 |
|---|---|---|
| KI-11J-2 | runtime ceiling остаётся одним числом на job, хотя отчёт даёт per-provider breakdown | Не нарушает predicted=executed; отдельная задача accounting |
| KI-11J-4 | способности обновляются при registration, не на каждом heartbeat | После provisioning нужен restart/re-registration |
| KI-11J-5 | OpenRouter `configured` не означает `verified` | Намеренно: zero-inference status не проверяет платный endpoint |
| KI-11J-6/7 | `norm_requote` не имеет model action; три списка center-only scope дублируются | Поймано contract tests; сетевой маршрут не меняет |
| KI-11J-8 | наследие 11I: per-provider grant не является per-action spending counter; concurrency ceilings консервативны | Ledger остаётся per-action и exactly-once |
| KI-11J-9/10 | отдельный ручной retry norm stage и flag-off clause binding не всегда получают frozen binding | Не путь проверенного automatic central tail |
| KI-11J-11 | `decision_carryover` не описан routing plan и теряет ContextVar в сыром pool | Не нормативный model tail и не влияет на сетевой PASS |
| KI-11J-12 | frozen plan влияет на чтение worker-stage моделей на центре | Намеренное выравнивание prompt topology |
| KI-11J-13/14 | замораживается provider/capability, не локальная строка модели | Строка модели — политика машины и не должна уезжать в plan |
| KI-11J-15 | неиспользуемая center table row `findings_corrector` расходится с фактическим Claude route | Regression mapping проверяет достижимые model roles |

## Operational readiness

На `.31` Claude и Codex ambient auth настроены, OpenRouter credential отсутствует.
Это не architecture defect: итог worker readiness —
`NEEDS_OPENROUTER_PROVISIONING`.

Три slow-process pytest timeout являются baseline-проблемой: те же тесты с
теми же точками отказа падают на `069987d5`.
