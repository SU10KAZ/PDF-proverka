# Winner Recommendation (single-block + selective escalation)

## TL;DR

**Practical recommendation: Flash single-block + selective Pro escalation on heavy/weak blocks.**

| # | Question | Answer |
|---|----------|--------|
| 1 | Flash single-block as practical mainline? | **YES** — 100% coverage, 46 findings, $0.46 for 215 blocks |
| 2 | Selective Pro escalation needed? | **YES** — on 20-block sample: 18/20 improved, +47 findings, 0 degraded |
| 3 | Best trigger rule | **`risk=heavy AND findings=0`** (10 blocks, highest ROI) |
| 4 | Projected hybrid full-doc cost | **~$1.56** (Flash $0.46 + narrow Pro escalation $1.10) |
| 5 | Final recommendation for Gemini/OpenRouter stage 02 | **Flash single-block mainline + narrow Pro escalation** |

## Flash single-block full-doc results (Phase S1)

- Model: `google/gemini-2.5-flash`
- Blocks: **215** (heavy 11 / normal 151 / light 53)
- **Coverage: 100.0%** (missing 0, dup 0, extra 0)
- Findings: **46** on 17 blocks (21.4/100)
- KV total: 4951 (median 18/block)
- Cost: **$0.4627** (cost/valid block $0.00215)
- Elapsed: **372 s** (p95 per-block 9.9 s)
- All 215 batches: actual `usage.cost` from OpenRouter.

**This is production-ready as stage 02 mainline** — dramatically better than Flash batch mode (which gave 94.4% coverage + 0 findings), same cost order of magnitude.

## Pro selective escalation sample (Phase S2)

- 20 weakest Flash blocks (top of weak-block ranking)
- Model: `google/gemini-3.1-pro-preview`, single-block, parallelism 2
- Coverage: **100%** (no completeness regressions)
- Improved: **18 / 20** | Unchanged: 2 | Degraded: **0**
- Additional findings: **+47** (Flash had 0 on these blocks; Pro found 47)
- Additional KV: +9 (Pro less verbose than Flash on these blocks)
- Pro cost: **$2.2670** ($0.11 per block, $0.04823 per added finding)
- Elapsed: 853 s

**Gate assessment**: PASS. 18 ≥ 3 threshold, 0 degraded, 47 ≥ 5 added findings, $0.13 per improved block ≤ $0.50 threshold.

## Weak-block distribution (for trigger-rule design)

Из 215 блоков после Flash single-block:
- 198 блоков с `findings=0` (92%!) — Flash находит замечания только на ~8% документа
- 11 блоков с score 6 — все **heavy AND findings=0** (это наши top-escalation candidates)
- 187 блоков с score 4 — normal blocks с findings=0 (много шума)
- 17 блоков с score 0 — Flash нашёл всё что нужно

## Trigger rule options (projected hybrid costs)

Initial trigger rule `findings=0 AND kv<=2` catches только 1 блок (Flash обычно даёт много KV), поэтому projection в `hybrid_policy_recommendation.md` занижен. Реальные варианты:

| Trigger rule | Blocks escalated | Projected Pro cost | Hybrid total | Commentary |
|---|---|---|---|---|
| `findings=0 AND kv<=2` (narrow, в hybrid_policy.md) | 1 | $0.11 | $0.58 | Почти не ловит |
| **`risk=heavy AND findings=0` (recommended)** | **10** | **~$1.10** | **~$1.56** | Все top-score блоки, лучший ROI |
| `risk∈{heavy,normal} AND findings=0 AND kv<=5` | ~14 | ~$1.55 | ~$2.00 | + "скудные" normal |
| `findings=0` (все подряд) | 198 | ~$21.80 | ~$22.25 | Слишком широко |
| `top-20 weakest (score-based)` — измеренный | 20 | $2.27 (measured) | $2.73 | 10 heavy + 9 normal с findings=0 |

**Рекомендую** `risk=heavy AND findings=0` как trigger rule:
- Простое, воспроизводимое (risk-class уже классифицирован в `blocks.py`).
- Захватывает все top-score блоки (10 heavy+weak).
- Стоит ~$1.10 при предполагаемом improvement rate ~90% (на sample было 18/20 = 90%).
- Итого $1.56/документ vs $0.46 только Flash — +$1.10 за ~18-20 дополнительных findings.

## Hybrid economics summary

| Режим | Cost | Findings | Time |
|---|---|---|---|
| Только Flash single-block | $0.46 | 46 | 6 мин |
| **Flash + narrow Pro escalation (10 heavy)** | **~$1.56** | **~64-66** (46 + ~18-20 from Pro) | ~12 мин последовательно |
| Flash + tested 20-block escalation (измеренный) | $2.73 | 93 (46 + 47) | ~21 мин |

(Pro escalation sequential с Flash — нужны его results для trigger.)

## Constraints honored

- Phase A / B-lite / C-lite / D NOT rerun.
- No full-document Pro run (tested only 20-block sample).
- Production `stage_models.json` **UNCHANGED**.
- Claude CLI path untouched.
- Actual `usage.cost` preferred over estimate — 235/235 batches across S1+S2.
- Strict schema + response_healing + provider.require_parameters=true always on.
- Direct Gemini API path (geo-blocked) not used.

## Budget note — overrun

- Cap: $2.50 | Spent: **$2.73** | Overrun: **+$0.23** (+9.2%)
- Root cause: Pro per-block cost estimate was $0.07 (based on Phase A Pro single-block на случайных 60 блоков) но actual на 20 WEAK блоков was **$0.11**. Weak blocks зачастую heavy и с большим контентом → Pro генерирует больше reasoning tokens.
- Preflight approved S2: est $1.40 + S1 actual $0.46 = $1.86 ≤ $2.50. Actual S2 ($2.27) overshot.
- Fix для future runs: bumped `per_block_est(pro, single_weak) = $0.12` (выше чем для случайных single-block).

## Practical next steps (не выполнялись в этом раунде)

1. Production adapter: hybrid orchestrator — Flash single-block → trigger rule `risk=heavy AND findings=0` → Pro escalation → merge.
2. Верификация правила на другом проекте (другая дисциплина) для универсальности.
3. Попробовать Sonnet через OpenRouter для escalation — может быть дешевле при похожем качестве.
4. **Do NOT switch production defaults automatically** — adoption требует independent project validation.
