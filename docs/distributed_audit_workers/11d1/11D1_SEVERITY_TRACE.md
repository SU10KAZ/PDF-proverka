# 11D.1 — трасса правил `severity`

Вопрос §11 задания: откуда production-этап `text_analysis` берёт правила
категорий, и одинаковы ли они в legacy-пути и в provider-пути.

## 1. Все места, где severity вообще фигурирует

| # | правило | где определено | legacy B | provider 11D | provider после 11D.1 |
|---|---|---|---|---|---|
| S1 | перечень 5 значений | `prompts/pipeline/en/text_analysis_task.md:108` (в JSON-схеме) | есть | есть | есть |
| S2 | «severity — ONLY one of the 5 values» | `…task.md:145` | есть | есть | есть |
| S3 | развёрнутые критерии «ПРОВЕРИТЬ ПО СМЕЖНЫМ» (4 когда + 2 когда нет) | `…task.md:150-165` | есть | есть | есть |
| S4 | «…self-confirm each other and **inflate severity**» | `…task.md:70-73` | есть | есть | есть |
| S5 | страж отсутствия: «не можешь назвать просмотренные места → ПРОВЕРИТЬ ПО СМЕЖНЫМ, а не критическое» | `task_builder.py:54-74`, плейсхолдер `{ABSENCE_GUARD}` | **нет** (флаг OFF) | **нет** (флаг OFF) | **нет** (флаг OFF) |
| S6 | severity-метки кандидатов pre-scan | `md_prescan.py:508-527` | есть (пусто для этого документа) | есть (пусто) | есть (пусто) |
| S7 | **смысл значений** («Критическое — нельзя строить») | **только** корневой `CLAUDE.md:211-216` | **есть косвенно** (проектная память CLI) | **НЕТ** | **есть явно** (`SEVERITY_SEMANTICS`) |
| S8 | калибровка severity с матрицей решений и few-shot | `prompts/pipeline/ru/phase1/stage01_severity_calibration.md`, `…few_shot_examples.md` | **нет** (не подключено) | **нет** | **нет** |
| S9 | понижение severity после аудита | `stages/text_analysis/absence_guard.py:107-129` | вне этапа (`findings_verify`) | вне этапа | вне этапа |

Дословно из шаблона:

```
"severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ"
```
```
3. severity — ONLY one of the 5 values
```

Это всё. Никаких определений.

## 2. Что НЕ является правилом severity

`{DISCIPLINE_FINDING_CATEGORIES}` → `prompts/disciplines/EOM/finding_categories.md`
задаёт **другую ось** — поле `category` (`protection`, `cable`, `grounding`,
`documentation`, `calculation`, …). `grep -rn "КРИТИЧЕСК\|severity" prompts/disciplines/`
даёт **ноль совпадений во всех 15 профилях**. Профиль подставляется одинаково в
обоих путях.

## 3. Ни один фильтр не режет severity-блок

Проверено фактическим прогоном регулярок по шаблону с подставленным профилем:

* `_clean_template_for_api` (`prompt_builder.py:92-127`) удаляет **ровно 6
  строк**, все транспортные; строки `:108`, `:145`, `:150-165`, `:70-73` целы.
  Побочный эффект — нумерация `## Rules` становится 1,2,3,6 (косметика).
* `strip_filesystem_references` (`provider_transport.py:_ABS_PATH_RE`) на
  инструкциях документа 11D срабатывает **2 раза**, оба — на
  `{BLOCKS_ANALYSIS_PATH}` (`…task.md:65` и `:128`). Норм-база и severity-блок не
  задеты (0 совпадений регэкспа по всем файлам профиля и по `norms_reference.md`).

## 4. Детерминированного post-processing severity нет

* `md_prescan.augment_text_analysis_file` (`md_prescan.py:614-674`) **только
  добавляет** находки и бэкфиллит `related_block_ids`; severity уже имеющихся не
  трогает. Для документа 11D добавила 0 (пре-скан пуст).
* Раннер этапа (`stages/text_analysis/runner.py:229-284`) severity не проверяет и
  не меняет.
* `provider_transport.FIELD_TYPES` / `EXPECTED_SEMANTICS` и 13 проверок
  `validate_inference` severity не касаются.
* В provider-ветке payload модели пишется как есть (`claude_runner.py:1083`).
* `absence_guard.enforce_absence_guard` (единственное место, где severity
  понижается кодом) вызывается из этапа `findings_verify` поверх
  `03_findings.json` — то есть ПОСЛЕ и вне `text_analysis`.

## 5. Единственная асимметрия — S7

Доказательство с обеих сторон:

**legacy:** `claude_runner.py:1175-1178` зовёт `_run_cli` без `clean_cwd` ⇒
дефолт `False` (`:326`) ⇒ `cwd_arg = None` (`:396`) ⇒
`process_runner.py:293` `work_dir = cwd or str(BASE_DIR)` = корень репозитория.
`_build_cmd` (`:209-232`) не передаёт ни `--setting-sources=`, ни `--safe-mode`.
Комментарий репозитория (`claude_runner.py:235-238`) прямо говорит, что чистая
cwd нужна, чтобы **не** подгружались project `CLAUDE.md` / hooks / memory /
skills — значит без неё они подгружаются. Чистая cwd включена ровно одному этапу
и это не text_analysis (`:1796`, блочный батч).

**provider:** `claude_adapter._inference_argv` — `--safe-mode
--strict-mcp-config --disable-slash-commands --no-session-persistence
--setting-sources=`, cwd = пустой `providers/<p>/runtime`, инструментов ноль.
Зафиксировано в argv боевого прогона (`11D_STAGE01_RUN.json`).

Итого: определения severity — единственная инженерная инструкция, которая
доходила до legacy-модели и не доходит до provider-модели. В самом промпте её
никогда не было.

## 6. Что сделано на 11D.1

Личный контекст **не возвращён** (§17 это запрещает). Вместо этого определения
перенесены в промпт явным текстом — константа `provider_transport.SEVERITY_SEMANTICS`,
вставляется перед `## Output JSON Schema`:

```
## Severity Semantics (what each value means)

`severity` is a fixed five-value scale with fixed meanings. Use these:

- **КРИТИЧЕСКОЕ** — it cannot be built as designed: a violation of ПУЭ / ГОСТ / СП.
- **ЭКОНОМИЧЕСКОЕ** — money, volumes, wrong grade or quantity.
- **ЭКСПЛУАТАЦИОННОЕ** — it will cause problems later, during operation.
- **РЕКОМЕНДАТЕЛЬНОЕ** — typos and minor inconsistencies of the paperwork.
- **ПРОВЕРИТЬ ПО СМЕЖНЫМ** — it needs data from an adjacent discipline
  (see the detailed criteria at the end of these instructions).

Pick the value whose definition the defect actually matches. Do not soften it and
do not inflate it.
```

Границы правки:

* **не** подгонка под три исторические темы: в тексте нет ни «ОСУП», ни «TN-S»,
  ни «7.35», ни слова «заземление» (закреплено тестом
  `test_c_severity_semantics_has_no_project_specific_content`);
* **симметрична**: ни «err on the side», ни «when in doubt», ни «escalate»
  (тест `test_c_severity_semantics_is_symmetric`);
* **не трогает шаблон** — значит поведение центра (ветки A/B/C) не меняется
  (тесты `test_p_*`);
* **не читает `CLAUDE.md`** — это литеральная константа модуля; в
  `provider_transport.py` вообще нет обращений к файловой системе (тест
  `test_q_personal_context_stays_excluded`).

**Влияние правки на выход модели НЕ ПРОВЕРЕНО.** 11D.1 запрещает реальные вызовы.
Проверка — отдельный шаг с одним оплаченным прогоном.

## 7. Оговорка, снимающая соблазн простого объяснения

S7 — доказанная асимметрия входа, но **не доказанная причина** обвала
«3 КРИТИЧЕСКИХ → 0». Прогон того же документа тем же legacy-путём (с тем же
ambient-контекстом) 2026-03-17 дал **0 КРИТИЧЕСКИХ** и ровно ту же калибровку,
что 11D: «ПУЭ 7.35» → Рекомендательное, «ОСУП» → Эксплуатационное. Подробности —
`11D1_HISTORICAL_BASELINE_PROVENANCE.json` и `11D1_ROOT_CAUSE_MATRIX.json`.
