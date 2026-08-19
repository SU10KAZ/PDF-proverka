# Пресет «Claude+GPT +Codex» — фактическая матрица

Ключ пресета: `claude_gpt_codex` (app.js:3090). Конфиг = `BASE_STAGE_MODEL_CONFIG`
с двумя заменами: `block_batch → ensemble/gpt-codex`, `optimization → ensemble/claude-codex-opt`.

Что пресет кладёт в `STAGE_MODEL_CONFIG`:

```
text_analysis          claude-opus-5
block_batch            ensemble/gpt-codex
findings_merge         claude-opus-5
findings_critic        claude-sonnet-5
findings_corrector     claude-sonnet-5
norm_verify            claude-opus-5
norm_fix               claude-opus-5
norm_requote           claude-opus-5     ← строки в UI НЕТ
optimization           ensemble/claude-codex-opt
optimization_critic    claude-sonnet-5
optimization_corrector claude-sonnet-5
batchModes.block_batch findings_only_block_context
```

---

## Матрица исполнения (с учётом действующего `.env`)

| Строка UI | Ключ | Сохранено | PROVIDER | Фактические модели рантайма | Параллельность | Вызовов |
|---|---|---|---|---|---|---|
| 01 Блоки | `block_batch` | `ensemble/gpt-codex` | openrouter + codex_cli ×2 | `openai/gpt-5.4` ‖ `codex/gpt-5.4` ‖ `codex/gpt-5.6-sol` → судья `codex/gpt-5.6-sol` | 3 ноги параллельно, судья после; 2 блока одновременно | **4 × B** |
| 02 Текст | `text_analysis` | `claude-opus-5` | claude_cli | `claude -p --model claude-opus-5`, tools `Read,Write,Grep,Glob,WebSearch,WebFetch` | — | 1 |
| 03 Свод | `findings_merge` | `claude-opus-5` | claude_cli | `claude -p --model claude-opus-5` | — | 1 |
| Верификатор | `findings_critic` | `claude-sonnet-5` | **не используется** | Python: `deterministic_critic` + `deterministic_corrector` | — | **0** |
| Верификатор (фикс) | `findings_corrector` | `claude-sonnet-5` | **не используется** | «Страж отсутствия»: `claude -p --model <get_claude_model()>` = **claude-sonnet-5** (глобальная модель, не stage-модель) | чанки ≤4 параллельно | 0…K (условно) |
| 04 Нормы | `norm_verify` | `claude-opus-5` | claude_cli | шаг 0 привязка пунктов — `claude-opus-5` батчами по 25; шаг 3 проверка цитат — **native Python**, LLM только при исключении | привязка последовательно; чанки норм sem=1 | ⌈T/25⌉ (+0 обычно) |
| 04b Пересмотр | `norm_fix` | `claude-opus-5` | claude_cli | `run_norm_fix` (findings) и `run_optimization_norm_fix` (предложения) — обе на `claude-opus-5` | последовательно | 0…2 (условно) |
| — (нет строки) | `norm_requote` | `claude-opus-5` | claude_cli | native Python semantic search; Claude только при исключении | — | 0…1 |
| 05 Оптимизация | `optimization` | `ensemble/claude-codex-opt` | claude_cli + codex_cli | `claude-opus-5` ‖ `codex/gpt-5.6-sol` (effort `xhigh`) → merge детерминированный Python | 2 ноги параллельно | **2** |
| C OPT Critic | `optimization_critic` | `claude-sonnet-5` | claude_cli | `claude -p --model claude-sonnet-5`, затем Python-аугментация вердиктов | — | 1 |
| F OPT Fix | `optimization_corrector` | `claude-sonnet-5` | **не используется** | `OPTIMIZATION_CRITIC_DETERMINISTIC=true` → `run_deterministic_corrector` (Python), агентная ветка не достигается | — | **0** |

`B` = число графических блоков, `T` = число замечаний без номера пункта нормы.

---

## Скрытые модельные шаги, которых в таблице UI нет

1. **Судья этапа 01** (`dual_review.review_dual_findings`) — отдельный вызов
   `codex/gpt-5.6-sol` на каждый блок. Управляется только `.env`, в UI не выбирается.
2. **Gap-search** — не отдельный вызов: он идёт **внутри того же вызова судьи**,
   к промпту прикладывается PNG блока (`image_paths=[image_path] if gap_search_enabled`).
3. **Нормативная привязка пунктов** (`clause_binding_runner`) — LLM-шаг перед всей
   верификацией норм, модель наследуется от `norm_verify`, батч 25, до 2 раундов
   с самопроверкой по базе норм.
4. **Пересмотр оптимизаций по нормам** (`run_optimization_norm_fix`) — ещё один
   вызов модели `norm_fix`, если у предложений изменились нормы.
5. **Страж отсутствия** — вызов Claude, не привязанный ни к какой строке таблицы.

---

## Порядок и параллельность стадий

`.env`: `PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED=true`, `PIPELINE_NORMS_AFTER_MERGE_ENABLED=true`.

```
CTX (0 моделей) → 01 Блоки → 02 Текст → 03 Свод
   → параллельный блок (manager.py:4529 _run_parallel_after_merge):
        ├ Верификатор (Python + страж отсутствия)
        └ 05 Оптимизация (2 ноги) → ждёт corrector_done → C OPT Critic → F OPT Fix
   → Контроль долгов → 04 Нормы (последовательно) → Перенос вердиктов → Excel
```

Нормы вынесены из параллельного блока флагом, чтобы работать по финальным F-ID.
