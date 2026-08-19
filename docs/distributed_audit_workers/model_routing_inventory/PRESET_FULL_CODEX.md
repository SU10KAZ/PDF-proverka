# Пресет «Full Codex» — фактическая матрица

Ключ пресета: `codex_exec` (app.js:3100). Литерал `__codex_exec__` резолвится в
`codexModelId()` — id первой модели с `provider === "codex_cli"` из `/api/audit/model/stages`,
то есть **`codex/gpt-5.4`** (`CODEX_STAGE_MODEL_ID`).

Что пресет кладёт в `STAGE_MODEL_CONFIG`:

```
text_analysis          codex/gpt-5.4
block_batch            ensemble/gpt-codex          ← ТОТ ЖЕ, что в пресете A
findings_merge         codex/gpt-5.4
findings_critic        codex/gpt-5.4
findings_corrector     codex/gpt-5.4
norm_verify            codex/gpt-5.4
norm_fix               codex/gpt-5.4
norm_requote           codex/gpt-5.4               ← строки в UI НЕТ
optimization           ensemble/claude-codex-opt   ← ТОТ ЖЕ, что в пресете A
optimization_critic    codex/gpt-5.4
optimization_corrector codex/gpt-5.4
```

«Full Codex» **не означает** «всё на Codex»: этапы 01 и 05 остаются смешанными
по построению пресета, а Верификатор уходит на Claude по коду (см. ниже).

---

## Матрица исполнения (с учётом действующего `.env`)

| Строка UI | Ключ | Сохранено | PROVIDER | Фактические модели рантайма | Параллельность | Вызовов |
|---|---|---|---|---|---|---|
| 01 Блоки | `block_batch` | `ensemble/gpt-codex` | openrouter + codex_cli ×2 | `openai/gpt-5.4` ‖ `codex/gpt-5.4` ‖ `codex/gpt-5.6-sol` → судья `codex/gpt-5.6-sol` | идентично пресету A | **4 × B** |
| 02 Текст | `text_analysis` | `codex/gpt-5.4` | codex_cli | `codex exec --model gpt-5.4` в JSON-режиме; при промпте > `CODEX_TEXT_INPUT_BUDGET` — нарезка по листам на N чанков + Python-merge | чанки последовательно | 1 или N |
| 03 Свод | `findings_merge` | `codex/gpt-5.4` | codex_cli | базовый merge `codex exec` + **targeted-проходы** (`AUDIT_CODEX_TARGETED_FINDINGS` вкл. по умолчанию): дисциплинарный (AR/EOM/SS/KM) + `alia_docnorm_audit` + `alia_mark_system_audit` (за флагом observer) | последовательно | **2…4** |
| Верификатор | `findings_critic` | `codex/gpt-5.4` | **не используется** | Python `deterministic_critic` + `deterministic_corrector` | — | **0** |
| Верификатор (фикс) | `findings_corrector` | `codex/gpt-5.4` | **не используется → Claude** | «Страж отсутствия»: `claude -p --model claude-sonnet-5`. Codex здесь **не участвует** | чанки ≤4 параллельно | 0…K (условно) |
| 04 Нормы | `norm_verify` | `codex/gpt-5.4` | codex_cli | привязка пунктов — `codex exec --model gpt-5.4` (2 раунда по замеру в коде); проверка цитат — **native Python**; codex-ветка `run_norm_verify` несёт `NORM_VERIFY_TOOLS` с `mcp__norms__*` | — | ⌈T/25⌉ (+0 обычно) |
| 04b Пересмотр | `norm_fix` | `codex/gpt-5.4` | codex_cli | `run_norm_fix` + `run_optimization_norm_fix` через `codex exec` | — | 0…2 (условно) |
| — (нет строки) | `norm_requote` | `codex/gpt-5.4` | codex_cli | native Python; **codex-ветка отключена явно** — при падении native возвращает `exit 1` с текстом `norm_requote Codex fallback skipped` | — | 0 |
| 05 Оптимизация | `optimization` | `ensemble/claude-codex-opt` | claude_cli + codex_cli | `claude-opus-5` ‖ `codex/gpt-5.6-sol` (`xhigh`) → детерминированный merge. Одна нога — **Claude**, несмотря на «Full Codex» | 2 ноги параллельно | **2** |
| C OPT Critic | `optimization_critic` | `codex/gpt-5.4` | codex_cli | `_run_codex_json_stage` → `optimization_review.json`; затем Python-аугментация | — | 1 |
| F OPT Fix | `optimization_corrector` | `codex/gpt-5.4` | **не используется** | `OPTIMIZATION_CRITIC_DETERMINISTIC=true` → Python-корректор | — | **0** |

---

## Особенности codex-пути, которых нет у Claude-пути

1. **Targeted findings после свода** (`claude_runner._run_codex_targeted_findings_merge`,
   строка 551). Существует **только** в codex-ветке `run_findings_merge`. Базовый merge
   сохраняется как `03_findings_codex_base.json`, затем объединяется с targeted-результатами
   и проходит `enforce_stage01_atomicity`. В пресете «Claude+GPT+Codex» этих проходов нет вовсе.
2. **Ретрай разбора JSON**: `_run_codex_json_stage` делает до `AUDIT_CODEX_JSON_ATTEMPTS`
   (по умолчанию 3) попыток — но **только** на `codex_json_not_found`, не на исчерпание лимита.
3. **Нарезка text_analysis** по листам со скелетом — только codex-ветка
   (`_run_codex_text_analysis_chunked`). Падение любого чанка = hard fail этапа.
4. **Песочница**: `.env` `AUDIT_CODEX_SANDBOX=danger-full-access`; вызов идёт
   `codex exec --ephemeral --ignore-user-config --ignore-rules -C <ROOT_DIR>`.

---

## Что означают колонки «GPT-5.4» и «Codex»

| Колонка UI | Что это | provider | Как попадает в рантайм |
|---|---|---|---|
| **GPT-5.4** (радио) | `openai/gpt-5.4` | `openrouter` | HTTP на OpenRouter, `OPENROUTER_API_KEY`, платный, под `paid_api_guard` |
| **Codex** (чекбокс) | `codex/gpt-5.4` либо ensemble-id | `codex_cli` | локальный `codex exec` по подписке, `--model gpt-5.4` |

Один этап может иметь **и то и другое одновременно** — но только этап 01 и только
в виде составного id `ensemble/gpt-codex`. Для остальных этапов чекбокс Codex
перебивает радио (`toggleStageCodex` пишет один id).

**Модель Codex определяется центром явно**, а не локальной политикой:
`resolve_codex_model("codex/gpt-5.4") → "gpt-5.4"` уходит в argv `--model`.
Локальная политика (`capability → модель`) существует **только на удалённом воркере**
(`audit_worker/providers/model_policy.py`) и на центральном пути не задействована.
Между пресетами способ определения модели Codex **не различается**.
