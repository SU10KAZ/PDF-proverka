# Журнал идей

Файл для фиксации идей, которые предлагаются по ходу работы над проектом.

## Как пользоваться

- Когда у пользователя появляется идея — он пишет тег `идея N` (например `идея 1`, `идея 2`).
- Всё, что он напишет в сообщении с этим тегом, Claude добавляет сюда отдельной записью.
- В конце сессии (или по запросу «саммари») Claude делает краткий итог:
  - какая была идея,
  - что попробовали реализовать,
  - что получилось,
  - что не получилось / отложено.

---

## Идеи

<!-- Сюда добавляются идеи в формате:

### Идея N — короткое название
**Дата:** YYYY-MM-DD
**Формулировка (как сказал пользователь):**
> цитата

**Контекст / зачем:**
- ...

**Что попробовали:**
- ...

**Результат:**
- ✅ что получилось
- ❌ что не получилось / почему
- ⏸ отложено

-->

### Идея 2 — Router + специализированные промпты для блоков (Flash → Pro)
**Дата:** 2026-04-25
**Формулировка (как сказал пользователь):**
> на вход подается большой бесполезный контекст вместе с блоком, который по сути ничего не добавляет... в начале flash смотрит блок определяет что в нем, после этого подбирает к нему специализированный промт и отправляет к джемени про сам блок со специализированным простом, чтоб только сделал глубокий анализ и дал замечания

**Контекст / зачем:**
- Текущий универсальный промпт stage 02 ([prompts/pipeline/ru/block_analysis_task.md](prompts/pipeline/ru/block_analysis_task.md), 284 строки) уходит на КАЖДЫЙ блок целиком, плюс полный `{DISCIPLINE_CHECKLIST}` (EOM = 39 строк, 6 разделов по типам).
- Гипотеза: для большинства блоков значимая часть промпта нерелевантна → токены/внимание тратятся впустую, растёт риск галлюцинаций.
- Идея: 2-этапная схема per-block — лёгкий классификатор → выбор специализированного промпта → глубокий анализ.

**Развилка по классификатору (обсудили):**
- Отдельный Flash-проход VS использование уже имеющихся OCR-метаданных (`ocr_label`, `is_full_page`, `quadrant`, `ocr_text_len`, `merged_block_ids`, `size_kb`).
- Если метаданных хватит для 4–6 кластеров — классификатор = Python-функция (без LLM-вызова, без латентности, без риска мисклассификации).

**Что попробовали:**
- Шаг (1) теоретическая разметка применимости проверок текущего промпта по типам блоков EOM (12 sheet_types из [drawing_types.md](prompts/disciplines/EOM/drawing_types.md)):

  | sheet_type | Применимо checklist | % |
  |---|---|---|
  | `single_line_diagram` | 13/29 | 45% |
  | `panel_schedule` | 11/29 | 38% |
  | `floor_plan` / `parking_plan` / `cable_routing` / `grounding` | 10/29 | 34% |
  | `entry_node` / `detail` | 4/29 | 14% |
  | `specification` | 3/29 | 10% |
  | `title_block` / `general_notes` | 0/29 | 0% |

- Спец-секции универсального промпта («полное распознавание текста на схемах» ~20 строк, «конструктивное решение узлов» ~15 строк) уходят всем блокам, но применимы только к 4 из 12 типов.
- Предложены 5 естественных кластеров: **A.** Схемы (single_line + panel_schedule), **B.** Планы (floor/parking/cable/grounding), **C.** Узлы/разрезы (entry_node + detail), **D.** Спецификации, **E.** Штампы/служебные (1-строчный промпт).

**Результат:**
- ✅ Гипотеза «контекст раздут» подтверждается на разметке: даже самый «богатый» тип блока использует только 45% checklist, штампы — 0%.
- ✅ Спецификация уже структурирована по типам внутри checklist — т.е. достаточно роутинга на этапе подстановки `{DISCIPLINE_CHECKLIST}`, не требуется переписывать сами проверки.
- ⏸ **Отложено:** шаг (2) — посчитать фактическое распределение блоков по 5 кластерам на реальном `02_blocks_analysis.json` (например, КЖ5.17 из последнего A/B) и связать с количеством `findings[]` по кластерам — это покажет, какие кластеры дают findings, а какие — мёртвый вес.
- ⏸ **Не реализовано:** сам router и 5 специализированных промптов.
- ❓ **Открытый вопрос:** хватит ли OCR-метаданных для классификации, или всё-таки нужен Flash-проход. Решается замером точности Python-классификатора на тех же блоках, где есть `sheet_type` от Pro — золотой стандарт уже есть в `02_blocks_analysis.json`.

---

### Идея 3 — Document Knowledge Graph вместо сырого MD
**Дата:** ~2026-03-19 (зафиксировано в журнале: 2026-04-25)
**Формулировка (ретроспективно):**
> `document_graph.json` как структурированный источник вместо сырого MD. Per-block контекст вместо «брось весь MD в промпт».

**Контекст / зачем:**
- До этого этапы конвейера сканировали MD-файл целиком (Chandra OCR, длинный текст с `[TEXT]` и `[IMAGE]` блоками).
- Промпт раздувался, контекст плохо адресовался к конкретному блоку, дублирование между этапами.
- Идея: один раз распарсить MD в структурированный JSON и дальше работать с ним.

**Что попробовали:**
- В [process_project.py](process_project.py) добавлена `build_document_graph()`: парсит MD → `document_graph.json` со структурой `pages[].sheet_no/sheet_name/text_blocks[]/image_blocks[]`.
- `blocks.py crop` обогащает `image_blocks` данными из `blocks/index.json` (file, size_kb).
- В [webapp/services/task_builder.py](webapp/services/task_builder.py) — per-block контекст подставляется из графа, а не из сырого MD.
- Fallback: если `document_graph.json` нет — парсим MD напрямую (старый путь).
- Используется также для `_enrich_sheet_page()` в [findings_service.py](webapp/services/findings_service.py): мапим `page → sheet_no` из графа (раньше путали page и sheet).

**Результат:**
- ✅ Внедрено в production, описано в [CLAUDE.md](CLAUDE.md) как «Document Knowledge Graph».
- ✅ Этапы 01/02/03 читают граф, а не сканируют MD заново.
- ✅ Sheet/page разделены корректно (лист штампа vs страница PDF).
- ⏸ Открытый вопрос: расширить граф связями между блоками (cross-references «см. лист X»), сейчас они извлекаются на стадии findings.

---

### Идея 4 — Детерминированная верификация норм (Python считает статус, LLM — только на unknown/stale)
**Дата:** ~2026-03-15…03-19 (commits 57f9204 «12-шаговый план: norms», 6938e7f «norm contract»)
**Формулировка (ретроспективно):**
> Статус документа считает Python из `norms_db.json` + TTL, LLM зовётся только на unknown/stale. Раньше LLM решал active/replaced — теперь нет.

**Контекст / зачем:**
- LLM регулярно ошибался со статусом норм (active/replaced/cancelled): то ссылался на отменённый СП 31-110-2003, то «галлюцинировал» актуальность.
- При этом статус — детерминированная вещь: достаточно сверить с базой и проверить TTL.
- Идея: вынести решение из LLM в Python, LLM использовать только там, где база реально не знает.

**Что попробовали:**
- `norms_db.json` (176+ записей) как источник истины статуса.
- `generate_deterministic_checks()` в [norms.py](norms.py): даёт предварительный `norm_checks.json`, помечает свежие записи `verified_via="deterministic"`, stale/unknown — на WebSearch.
- LLM (условно) пишет `norm_checks_llm.json` только для unknown/stale + цитаты пунктов.
- `merge_llm_norm_results()` сливает в финальный `norm_checks.json`.
- 3-уровневая система верификации цитат: `norm_quote` + `norm_confidence` → `paragraph_checks` (если confidence < 0.8) → накопительный кеш `norms_paragraphs.json`.

**Результат:**
- ✅ Если все нормы есть в базе и кеш свежий — LLM на этом этапе не вызывается (экономия токенов).
- ✅ Невозможно случайно «оживить» отменённую норму — Python жёстко возвращает статус из базы.
- ✅ `norms.py update --all` пополняет `norms_db.json` и `norms_paragraphs.json` из проектов.
- ⏸ TTL и периодичность ревалидации stale-записей — настраивается, но не было замера, насколько часто реально срабатывает stale-путь.

---

### Идея 5 — Локальный QWEN как LLM-бэкенд через ngrok
**Дата:** в работе (зафиксировано в журнале: 2026-04-25)
**Формулировка (ретроспективно):**
> Локальный QWEN как LLM-бэкенд (через ngrok+Basic Auth, qwen3.6-35b). Известная проблема: chunked findings_merge на больших проектах.

**Контекст / зачем:**
- Хочется снять зависимость от Claude API / OpenRouter на тяжёлых стадиях, где можно обойтись локальной моделью.
- Основной сервер 176.12.77.31 (1stVDS/Alhost KZ) получает FAILED_PRECONDITION от Gemini → нужен альтернативный путь.
- На локальной машине поднят qwen3.6-35b, проброшен через ngrok с Basic Auth.

**Что попробовали:**
- Два endpoint'а: `/v1/chat/completions` (OpenAI-совместимый) и `/api/v1/chat` (нативный).
- Подключение к webapp через настраиваемый base_url + auth.
- Тесты на стадиях, где LLM делает структурную работу.

**Результат:**
- ✅ Связка ngrok + Basic Auth + qwen работает как LLM-бэкенд для лёгких стадий.
- ❌ **Проблема:** chunked `findings_merge` на больших проектах — модель не справляется с длинным контекстом и/или ломается стриминг при разбиении на чанки.
- ⏸ Открытые вопросы: как чанковать findings_merge безопасно (по дисциплинам? по страницам?), стоит ли держать qwen только на стадиях 01/02 и оставлять merge на Opus.
- 📌 Полный контекст инфраструктуры — в memory: [infra_local_llm.md](/home/coder/.claude/projects/-home-coder-projects-PDF-proverka/memory/infra_local_llm.md).

---

### Идея 6 — Qwen-enrichment + GPT-5.4 low + extended prompt как дешёвая альтернатива stage 02
**Дата:** 2026-04-25
**Формулировка (как сказал пользователь):**
> я с помощью qwen обрабатываю каждый блок отдельно и создаю для каждого блока обогащённое описание... и далее отправлять этот обогащённый файл вместе с блоком gemeni? чтоб они имея обогащённый файл могли понимать что в нем и выдавать только короткий ответ какие проблемы увидел... и тем самым мы решим вопрос с джемени что он не должен выдавать дорогие ответы и большие

**Контекст / зачем:**
- Текущий stage 02 (Pro модель на каждый блок) выдаёт полный пакет: `summary` + `key_values_read` + `evidence_text_refs` + `findings`. Большая часть output — пересказ того, что и так видно.
- Гипотеза: разделить роли. Локальный QWEN бесплатно делает структурное извлечение (block_type, marks, dimensions, references, level_marks, rebar_specs), потом дорогая модель видит блок + готовое описание и пишет ТОЛЬКО findings[].
- Развитие [Идеи 2 (Router + спец. промпты)](#идея-2--router--специализированные-промпты-для-блоков-flash--pro) и [Идеи 5 (QWEN backend)](#идея-5--локальный-qwen-как-llm-бэкенд-через-ngrok), но в новом углу — не выбирать промпт, а вынести описательную работу из дорогой модели.

**Что попробовали:**
1. **Qwen enrichment** ([scripts/experiments/qwen_block_enrichment.py](scripts/experiments/qwen_block_enrichment.py)) на 25 блоков КЖ5.1. После починки 2 паттернов фейлов (resize PNG > 1.5 MB при «Invalid image» и retry на ngrok HTML page) — 25/25 OK за ~7 мин, $0. Качество: dimensions 25/25, references_on_block 25/25, level_marks 22/25, marks 19/25, rebar_specs 17/25, концентрат бетона ловит когда явно показан в блоке (3/25).
2. **Pro 3.1 + enrichment + findings-only** ([scripts/experiments/qwen_enrichment_pro_pilot.py](scripts/experiments/qwen_enrichment_pro_pilot.py)) — провал: 9 419 reasoning-токенов на блоке, идея «короткий output» не даёт экономии. Экономика та же, что у baseline.
3. **Gemini 2.5 Pro с `thinking_budget=0`** — OpenRouter возвращает HTTP 400 «Reasoning is mandatory for this endpoint and cannot be disabled». Через OpenRouter Pro Gemini без reasoning не получить. Gemini 1.5 Pro/Flash на OpenRouter уже недоступны.
4. **Gemini 2.5 Flash + enrichment** — output короткий, но: не знает норм РФ, генерит шум (5 «не указано X» вместо 2 нормативных), пропускает cross-reference замечания. Coverage критических категорий ~30%.
5. **GPT-5.4 minimal/low/medium** на 3 блоках для калибровки. Sweet spot — `effort=low`: на 9GNP нашёл расхождение 7650 vs 7850 мм со ссылкой на СП 63.13330.2018 п.10.3.15 (то, что Pro 3.1 пропустил), на 4MQJ — 0/2 даже на medium (системный пропуск, не лечится thinking budget).
6. **Полный прогон GPT-5.4 low + enrichment на 25 блоков КЖ5.1**: 41/50 findings (82%), 4 полных пропуска, $0.56 ($0.022/блок).
7. **Расширение промпта чек-листом КЖ** ([prompts/disciplines/KJ/finding_categories.md](prompts/disciplines/KJ/finding_categories.md), 24 категории) → ключевая находка. На тех же 25 блоках: 63/50 findings (126%), 2 пропуска (8%), reasoning **уменьшился** на 20% (модель ищет направленно, а не вслепую). Cost $0.65 (+15%).
8. **Repro на КЖ6 (78 блоков, другой корпус)** — 152/173 findings (88%), 7 пропусков (9%), $1.64 ($0.021/блок). Те же ~9% misses, та же цена.
9. **Escalation на 4 пропуска КЖ5.1**: medium восстановил 2/4 (9J9X, MANG), Pro 3.1 — ещё 1/2 (VGGJ). Один блок (4UTW) не подхватил даже Pro 3.1 + enrichment — это **системный прокол findings-only промпта**, потому что baseline ловил MEP coordination через специальный категориальный промпт.

10. **Сравнительный pilot 4 моделей на КЖ5.1, 25 блоков, единый findings-only + extended prompt**: чтобы проверить семантическое качество (а не только count) запустили четыре независимых прогона:
    - **A — Opus 4.6** через Claude CLI ([scripts/experiments/qwen_enrichment_opus_baseline.py](scripts/experiments/qwen_enrichment_opus_baseline.py)): 25/25 OK, **89 findings**, 51 мин wall-clock, 255K output tokens, ~$6.4 (subscription).
    - **B — Sonnet 4.6** через Claude CLI (тот же скрипт, --model claude-sonnet-4-6): 25/25 OK (1 retry на 6DRC из-за невалидного JSON), **94 findings**, 28 мин, 184K output tokens, ~$4.6 (subscription).
    - **C — GPT-5.4 `effort=low` + extended** через OpenRouter (прогон от 24.04): 25/25 OK, 63 findings, ~3 мин, $0.65.
    - **D — Gemini 3.1 Pro Preview + extended** через OpenRouter ([scripts/experiments/qwen_enrichment_pro_pilot.py](scripts/experiments/qwen_enrichment_pro_pilot.py) --model google/gemini-3.1-pro-preview): 25/25 OK после 2 retry (HJ7G, DTGF упали с провайдер-side glitch), **75 findings**, ~12 мин, ~$2.5.

11. **LLM-judge через Claude CLI claude-opus-4-7** ([scripts/experiments/findings_judge.py](scripts/experiments/findings_judge.py)) на тех же 25 блоках: для каждого блока judge получал 4 списка findings + Qwen enrichment, искал семантические matches и оценивал unique findings как `valuable | questionable | noise`. Выбирал `best_overall` per block. 25/25 judged за ~11 мин.

| Source | Total findings | Matched (consensus) | Valuable Unique | Questionable | Noise | **Wins** |
|---|---|---|---|---|---|---|
| **A — Opus 4.6** | 89 | 71 (80%) | 12 | 7 | 0 | 4 |
| **B — Sonnet 4.6** | 94 | 68 (72%) | **22** | 7 | 0 | **18** |
| **C — GPT-5.4 low** | 63 | 55 (87%) | 9 | 0 | 0 | 1 |
| **D — Gemini 3.1 Pro** | 75 | 43 (57%) | 21 | 11 | **2** | 1 |

(1 блок — ничья; «Wins» = в скольких блоках judge назвал источник `best_overall`.)

**Результат:**
- ✅ Qwen multimodal через ngrok устойчив: 25/25 + 78/78 без падений (после resize+retry).
- ✅ GPT-5.4 `effort=low` + extended prompt — стабильный sweet spot: ~91-92% покрытия, ~$0.02-0.03/блок, **экономия 3-10× против baseline Pro 3.1**.
- ✅ Воспроизводимость: КЖ5.1 (8% misses) и КЖ6 (9% misses) дали почти идентичные пропорции — паттерн системный, не случайный.
- ✅ Reasoning **снижается** при расширении промпта чек-листом — направленный поиск дешевле свободного.
- ✅ **Sonnet 4.6 + Qwen enrichment + extended prompt — выиграл семантическое сравнение**: 18 побед из 25 (72%) против Opus 4.6 (4), GPT-5.4 (1), Gemini 3.1 Pro (1). При этом нашёл **больше valuable_unique findings** (22 vs 12 у Opus), потратил **в 1.4× меньше output-токенов** (184K vs 255K), отработал **в 1.8× быстрее** (28 vs 51 мин). Гипотеза «Opus избыточен для этой задачи» подтвердилась.
- ✅ **GPT-5.4 = high-precision low-recall**: 0 noise, 87% findings имеют consensus с другими моделями. Из 63 findings — 55 совпадают. Подходит когда «уверенные находки» важнее «найти всё».
- ✅ **Семантическое сравнение возможно через LLM-judge**: 90 matched + 91 unique findings в 25 блоках — judge разрешает близкое к 100% от пар (ничьих почти нет). Можно использовать как стабильный инструмент калибровки моделей.
- ❌ **Pro Gemini findings-only через OpenRouter мертв** — reasoning обязателен, не отключается, идея «короткий output = экономия» ломается.
- ❌ **Flash без Pro для КЖ не годится** — нет нормативных ссылок, генерирует поверхностный шум.
- ❌ **Gemini 3.1 Pro — high-variance**: 21 valuable_unique, но 11 questionable + 2 noise (17% от его findings — спорные/шум). Match rate всего 57% (хуже всех). Самая «творческая», самая ненадёжная.
- ✅ **Stage 02 production profile (закреплён 2026-04-27):** `claude-sonnet-4-6` + Qwen enrichment + extended KJ prompt + `--minimal-prompt` (без page_text/метаданных) + **`--clean-cwd`** (запуск `claude -p` из чистой папки `/tmp/sonnet_clean/` + stripped env). Результат на КЖ5.1 (25 блоков): **123 findings** (vs 91 baseline), **$4.06 sub-parity** (vs $6.35), total input/блок 67K (vs 115K). См. артефакт [_experiments/qwen_enrichment_opus_baseline/20260426_213717__claude-sonnet-4-6_extended_minimal_clean-cwd/](projects/214.%20Alia%20%28ASTERUS%29/KJ/13%D0%90%D0%92-%D0%A0%D0%94-%D0%9A%D0%965.1-%D0%9A1%D0%9A2%20%282%29.pdf/_experiments/qwen_enrichment_opus_baseline/20260426_213717__claude-sonnet-4-6_extended_minimal_clean-cwd/). Скрипт: [scripts/experiments/qwen_enrichment_opus_baseline.py](scripts/experiments/qwen_enrichment_opus_baseline.py) с флагами `--extended-prompt --minimal-prompt --clean-cwd`.
- ✅ **3-way judge на 10 проблемных блоках** ([_experiments/findings_judge/20260427_062134/](projects/214.%20Alia%20%28ASTERUS%29/KJ/13%D0%90%D0%92-%D0%A0%D0%94-%D0%9A%D0%965.1-%D0%9A1%D0%9A2%20%282%29.pdf/_experiments/findings_judge/20260427_062134/)): clean_cwd выиграл 8/10 блоков, нашёл **14 valuable_unique** против 5 у baseline и 3 у minimal. Восстановил 7 из 10 потерянных в minimal valuable findings (через A∩C matches без B). Гипотеза «harness Claude CLI = distractor для Sonnet'а»: при удалении ~47K токенов harness'а (CLAUDE.md проекта, .claude/settings, hooks, project memory, skills manifest) Sonnet работает прицельнее — снижается attention dilution на нерелевантный контекст.
- ⏸ **Не реализовано:** production runner ([scripts/run_stage02_qwen_sonnet.py](scripts/run_stage02_qwen_sonnet.py) или `qwen_gpt54.py`); webapp pair-mode для нового профиля; escalation policy в production.
- ⏸ **Только КЖ:** extended prompt построен на `KJ/finding_categories.md`. Для EOM/OV/AR — нужен прогон с их собственными `finding_categories.md`.
- ❓ **Bias judge:** судья — `claude-opus-4-7`, той же семьи что Opus 4.6 / Sonnet 4.6. Возможны (1) стилистическая близость к ответам Anthropic-моделей и (2) предпочтение лаконичности (Sonnet короче и фокуснее). Это не отменяет лидерство Sonnet, но цифру «18 wins vs 4» стоит проверить через альтернативного судью (GPT-5.4 или Gemini Pro). Стоимость bias-check: ~$5-10.
- ❓ **Variance new/baseline ratio:** КЖ5.1=1.26×, КЖ6=0.88×. Двух точек мало — нужен прогон ещё на 3-5 проектах.
- ❓ **Поведение stage 03 (merge + critic + corrector)** с этим input не проверено — итоговый `03_findings.json` может отличаться сильнее, чем per-block stats показывают.

**Артефакты:**
- Qwen enrichment: `<project>/_experiments/qwen_enrichment/<ts>/`
- Pilot runs (OpenRouter): `<project>/_experiments/qwen_enrichment_pro_pilot/<ts>__<model><_extended>/`
- Opus/Sonnet baseline (Claude CLI): `<project>/_experiments/qwen_enrichment_opus_baseline/<ts>__<model>_extended/`
- Judge runs: `<project>/_experiments/findings_judge/<ts>/` — `summary.json`/`summary.md` + per-block `block_<id>.json` с детальными вердиктами matches/uniques/value_judgment.

---

### Идея 7 — Qwen enrichment как stage 1 augment-step в MD (вместо stage 02 pre-step)

**Дата:** 2026-04-25
**Формулировка (как сказал пользователь):**
> вообще нужно идти по уровню 2 но перед его выполнением я хочу обсудить другой момент. может обработку блоков графических на qwen сделать отдельным этапом на 1 этапе? и перепишем весь мд файл в котором описание блоков заменим на новые описания.

**Контекст / зачем:**
- Идея 6 показала, что Qwen-enrichment даёт +значительный качественный прирост (Sonnet 4.6 без enrichment проигрывает 22:2 wins, теряет 27% valuable findings).
- В Идее 6 enrichment делался как pre-step внутри stage 02 (отдельные `block_<id>.json` файлы рядом с PNG, дорогая модель читает их).
- Альтернатива: сделать enrichment частью **подготовки данных** — переписать MD-файл от Chandra, заменив `[IMAGE]` блоки на `[ENRICHED ...]` от Qwen. Дальше весь pipeline (текст stage 01, блоки stage 02, findings_merge stage 03) работает с одним обогащённым источником без необходимости знать про Qwen.

**Архитектурные решения (согласовано):**

| # | Решение | Детали |
|---|---|---|
| 1 | Куда вписывать enrichment | Augment в MD: переписать `[IMAGE]` блоки с `[ENRICHED ...]` от Qwen |
| 2 | Последовательность шагов | parse MD light (только список блоков) → crop (PNG с Chandra) → Qwen enrichment → merge в MD → parse MD финал → `document_graph.json` |
| 3 | UI | Переименовать «Скачать блоки» → «Подготовить данные»; зашить crop + Qwen в одну кнопку |
| 4 | Промежуточный граф | Нет. Один парсинг MD после enrichment, единственный финальный `document_graph.json` |
| 5 | `blocks batches` | Убираем во всех пресетах (Классический/Подписка/QWEN). Везде single-block в stage 02 |
| 6 | Повторный «Подготовить данные» | Skip по умолчанию + кнопка «Force re-enrich» в UI |
| 7 | Маркер enriched | MD-комментарий `<!-- ENRICHMENT: qwen3.6-35b @ <ts> blocks=N -->` + `graph.meta.enrichment` (source/timestamp/block_count/md_hash) |
| 8 | Backup MD | `*_document.md.pre_enrichment.bak` (один, самый первый — не перезаписывать) |
| 9 | При force re-enrich | Backup `_output/` (кроме `blocks/`) в `_output/_pre_enrichment_<ts>/` + UI баннер «Запустить аудит?» (не запускает автоматически) |
| 10 | Логирование | `pipeline_log.json.enrichment_source = "qwen3.6-35b@<ts>"` для каждого этапа аудита |

**Почему Augment, а не Replace:**
- Сохраняет оригинальный текст `[IMAGE] описание от Chandra OCR` — не теряем источник истины.
- Qwen-описание идёт как дополнительный блок `[ENRICHED ...]` рядом, можно сравнить «что видел Chandra vs что увидел Qwen».
- Если Qwen ошибся — оригинал `[IMAGE]` остаётся доступен.

**Почему один граф (без промежуточного):**
- Источник списка блоков для Qwen = `blocks/index.json` (создаётся `crop`), не граф.
- Двойной парсинг MD не даёт пользы кроме «снапшота до enrichment» — а его роль выполняет `*.md.pre_enrichment.bak`.
- Меньше артефактов, меньше путаницы «какой граф читать».

**Почему single-block везде:**
- Идея 6 показала: на single-block stage 02 качество выше (нет деградации обдумывания при батчах графики).
- Унифицированный режим = меньше веток в коде, проще логика task_builder.
- `blocks batches` оставался legacy от старой архитектуры — теперь не нужен.

**Что попробовали:**
- Только обсуждение архитектуры в этой сессии. Реализация ещё не начата.

**Ожидаемый результат (гипотезы):**
- Один источник истины для аудита — обогащённый MD. Любая модель stage 02 (Sonnet/Opus/GPT/Gemini) получает одинаковый контекст, можно честно сравнивать.
- Qwen-стоимость локализована в одну кнопку «Подготовить данные» — пользователь явно понимает, когда тратится время.
- Кеширование естественное: повторные запуски аудита не вызывают Qwen.

**Риски — обсуждены, решения:**
- ⚠️ **Отравление stage 01 плохим Qwen-описанием** (Qwen может галлюцинировать «Ø12» вместо «Ø10», ложное findings) — **игнорируем**. Принимаем риск, мониторим качество по факту.
- 💰 **Рост MD ×2-3** (~$25-40 доплата на проект input-токенами Opus 4.7) — **приемлемо**. Контекст 1M не лопнет. Деградация «lost in the middle» возможна на проектах >100 блоков, мониторим.
- 🕐 **Force re-enrich UX на 100+ блоков** (50-100 мин Qwen-времени) — **добавляем прогресс-бар** в UI «Подготовить данные»: «X из N блоков обработано, текущий: block_007_1 (стр. 4)», ETA. Реализация через тот же WebSocket-механизм что у live-лога аудита (`/ws/audit/{project_id}`).
- 🔄 **PDF поменялся → incremental enrichment** — **не нужен**. Workflow: каждый изменённый PDF = новая папка `projects/<имя> (Изм.N)/` = с нуля. Force re-enrich оставляем только для случая «поменяли настройки Qwen или промпт» на текущем PDF. Поле `md_hash` в `graph.meta.enrichment` НЕ требуется.
- ⏸ **Не реализовано:** изменения в `process_project.py` (двойной парсинг убирается, добавляется один parse после enrichment); новая функция augment-merge MD с Qwen-выходом; изменения в `blocks.py` (объединение crop + Qwen в одну команду или wrapper-скрипт); UI-кнопка «Подготовить данные» (переименование «скачать блоки»); versioning через `document_graph.meta.enrichment`; reset `_output/0X_*.json` с backup в `_output/_pre_enrichment_<ts>/`; удаление `blocks batches` из всех пресетов.

**Связь с другими идеями:**
- Идея 5 (QWEN backend) — реализация enrichment-вызовов идёт через тот же ngrok-тоннель.
- Идея 6 (Qwen-enrichment + GPT-5.4) — Идея 7 это её эволюция: вынос enrichment из stage 02 в общий prep-шаг.
- Идея 3 (Document Knowledge Graph) — графа становится единственным после enrichment, без промежуточных версий.

---

## Саммари по сессиям

<!-- В конце сессии добавляется краткий итог:

### Сессия YYYY-MM-DD
- Идея N (название) → результат: ...
- Идея N+1 (название) → результат: ...

-->
