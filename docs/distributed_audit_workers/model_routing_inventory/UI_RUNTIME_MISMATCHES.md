# UI против рантайма: построчная сверка

Вердикты: **MATCH** / **MISMATCH** / **PARTIAL** / **DYNAMIC**.

| # | Строка UI | UI говорит (пресет A / пресет B) | Рантайм делает | Вердикт | Где расходится |
|---|---|---|---|---|---|
| 1 | 01 Блоки | «GPT-5.4 ✔ + Codex ✔» — две модели | Три ноги (`openai/gpt-5.4`, `codex/gpt-5.4`, `codex/gpt-5.6-sol`) + судья `codex/gpt-5.6-sol` | **PARTIAL** | Таблица моделей не знает про третью ногу и судью. Панель «Алгоритм этапа» их показывает — `audit.py:120-140` зеркалит `STAGE01_THIRD_LEG_*` и `STAGE01_DUAL_REVIEW_MODEL` |
| 2 | 02 Текст | Opus 5 / Codex | Ровно так | **MATCH** | — |
| 3 | 03 Свод | Opus 5 / Codex | A — 1 вызов Opus. B — 1 вызов Codex **+ 1…3 targeted-прохода** | **PARTIAL (только пресет B)** | `claude_runner.py:897 _run_codex_targeted_findings_merge`. В UI нет ни строки, ни счётчика |
| 4 | **Верификатор** | Sonnet 5 / Codex | **Ни одна модель не вызывается.** Чистый Python | **MISMATCH** | `findings_verify/runner.py:85-90` — `run_deterministic_critic(..., llm_call=None)`. Значение `findings_critic` конвейер не читает вообще (используется лишь в `services/external_register/matcher.py:256` и как подпись в `/api/audit/model/stages`) |
| 5 | **Верификатор (фикс)** | Sonnet 5 / **Codex** | `claude -p --model claude-sonnet-5` — всегда Claude, в обоих пресетах | **MISMATCH** | `text_analysis/absence_guard.py:207` — `get_claude_model()`, глобальная модель CLI, а не stage-модель. В «Full Codex» строка обещает Codex, а работает Claude |
| 6 | 04 Нормы | Opus 5 / Codex | Основная проверка цитат — **native Python** (`verify_paragraphs_native`), модель вызывается только на шаге привязки пунктов и только при `NORM_CLAUSE_BINDING_ENABLED=true` | **PARTIAL** | `norms/runner.py:505-521` — LLM-ветка идёт лишь при исключении native-пути |
| 7 | 04b Пересмотр | Opus 5 / Codex | Верно, но это **два** разных вызова: `run_norm_fix` (замечания) и `run_optimization_norm_fix` (предложения), оба условные | **PARTIAL** | `norms/runner.py:772` и `:830`. Второй вызов в UI не отражён |
| 8 | 05 Оптимизация | Opus 5 ✔ + Codex ✔ | `claude-opus-5` ‖ `codex/gpt-5.6-sol` @ xhigh | **PARTIAL** | Колонка «Codex» подразумевает `codex/gpt-5.4`, фактически идёт `gpt-5.6-sol` с другим effort. Панель алгоритма показывает это правильно (`ensemble_details.optimization.parallel_models`), таблица — нет. Плюс Codex-нога получает PNG чертежей, Claude-нога — нет; в UI этого нет нигде |
| 9 | C OPT Critic | Sonnet 5 / Codex | Верно + Python-аугментация вердиктов поверх | **PARTIAL** | `optimization/runner.py:378` |
| 10 | **F OPT Fix** | Sonnet 5 / Codex | **0 вызовов модели** — `run_deterministic_corrector`, `return` стоит выше агентной ветки | **MISMATCH** | `optimization/runner.py:445-474` при `OPTIMIZATION_CRITIC_DETERMINISTIC=true` (в `.env` стоит) |
| 11 | (строки нет) | — | `norm_requote` в конфиге пресета есть, строки в `stageLabels` нет | **PARTIAL** | `app.js:3057-3068` против `app.js:3084/3111`. Значение сохраняется, но пользователь его не видит |
| 12 | Схема 05: «C OPT Critic: X / F OPT Fix: Y» | Может показывать Codex GPT-5.4 при выбранном пресете A | Панель читает **сохранённое на сервере** состояние, таблица — **несохранённое** состояние формы | **DYNAMIC** | `app.js:4254-4259` ← `audit.py:148-149`. Расхождение исчезает после сохранения |

---

## Сводка

* **MISMATCH — 3 строки:** Верификатор, Верификатор (фикс), F OPT Fix.
  Все три предлагают выбрать модель, которая в аудите не применяется.
  Две из них — «мёртвая настройка», одна (Верификатор (фикс)) — **скрытый уход на Claude**.
* **PARTIAL — 6 строк.** Общий мотив: UI показывает по одной модели на строку,
  а этап делает больше одного вызова и/или использует модель из `.env`, а не из таблицы.
* **MATCH — 1 строка:** 02 Текст.
* **DYNAMIC — 1 наблюдение:** расхождение схемы и таблицы по C OPT Critic / F OPT Fix
  объясняется тем, что они читают разные источники (сервер vs форма).

## Почему это важно для распределённых воркеров

Если контракт задания захотят строить «по строкам UI», три из десяти строк
опишут несуществующие вызовы, а одна (Верификатор (фикс)) заставит воркер
запросить Codex там, где центр делает Claude. Источником для контракта должен
служить рантайм-путь, а не таблица.
