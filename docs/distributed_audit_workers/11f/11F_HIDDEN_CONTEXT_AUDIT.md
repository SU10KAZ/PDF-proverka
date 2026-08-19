# 11F — аудит скрытых зависимостей от CLAUDE.md и личного контекста

Проверка шла по коду и по файлам промптов, а не по документации. Основание —
дефект 11D.1: определения severity («Критическое = нельзя строить») жили
**только** в корневом `CLAUDE.md`, и при переносе на воркер, где личный контекст
подавлен, этап молча получал другую шкалу важности.

## 1. Дефект оказался системным, а не частным

`grep` по всему `prompts/` не находит **ни одного** определения шкалы важности:
строк «нельзя строить» / «cannot be built» нет ни в `prompts/pipeline/{ru,en}/`,
ни в `prompts/disciplines/`. Слово «КРИТИЧЕСК» в дисциплинарных профилях не
встречается вовсе. `finding_categories.md` задаёт машинные значения поля
`category` (что за тип проблемы), а не шкалу важности (насколько это тяжело).

Закрыт дефект ровно в двух местах и только в provider-режиме:

* `backend/app/pipeline/stages/text_analysis/provider_transport.py:176` —
  константа `SEVERITY_SEMANTICS`, вставка в промпт на :290-291;
* `backend/app/pipeline/stages/findings_merge/provider_transport.py:48` —
  импорт той же константы, вставка на :367-368.

Больше константа нигде не встречается. Значит **определений severity не
получают**: `block_analysis`, `optimization`, `optimization_critic`,
`optimization_corrector`, `findings_verify` — ни на одной ветке, ни на центре,
ни на воркере.

## 2. Постадийный разбор оставшихся модельных стадий

| Стадия | Где промпт | Что легаси брал из CLAUDE.md | Нужно ли | Есть ли явно |
|---|---|---|---|---|
| `block_analysis` | инлайн `build_system_prompt` в `gemma_findings_only.py` + `finding_categories.md` | шкала severity; правило «PDF > MD»; роль эксперта РФ; русский язык ответа | шкала — да; остальное частично покрыто инлайн-промптом | **нет** шкалы |
| `optimization` | `prompts/pipeline/en/optimization_task.md` | шкала severity; формат ссылки на норму; роль | шкала и формат нормы — да | **нет** |
| `optimization_critic` | `prompts/pipeline/en/optimization_critic_task.md` | шкала; критерии качества | да | **нет** |
| `optimization_corrector` | `prompts/pipeline/en/optimization_corrector_task.md` | то же | да | **нет** |
| `findings_verify` (страж отсутствия) | инлайн `build_verification_prompt` в `absence_guard.py:143` | шкала не нужна: задача бинарная «есть/нет в MD» | нет | не требуется |
| `text_analysis` | `prompts/pipeline/en/text_analysis_task.md` + `SEVERITY_SEMANTICS` | закрыто на 11D.1 | — | **да** |
| `findings_merge` | `prompts/pipeline/en/findings_merge_task.md` + `SEVERITY_SEMANTICS` | закрыто на 11E | — | **да** |

**Решение 11F.** `SEVERITY_SEMANTICS` выносится в общий модуль и подставляется
в provider-промпты `block_analysis`, `optimization`, `optimization_critic`,
`optimization_corrector`. Это перенос УЖЕ УТВЕРЖДЁННОЙ на 11D.1 инженерной
семантики на оставшиеся стадии, а не новая формулировка: текст константы не
меняется ни на символ. Никакие другие фрагменты `CLAUDE.md` не переносятся —
ни структура каталогов, ни правила git, ни обращение к пользователю, ни
инструкции агенту.

## 3. Механизм подавления личного контекста

Задан **только** в провайдерском слое воркера,
`audit_worker/providers/claude_adapter.py:104-115`:

```
--safe-mode  --strict-mcp-config  --disable-slash-commands
--no-session-persistence  --setting-sources=
```

плюс пустой рабочий каталог `providers/<provider>/runtime` (`base.py:636`) и
окружение, собранное с нуля по белому списку.

На центре подавления нет: `claude_runner._build_cmd` (:209-232) собирает
`[claude, -p, --model, --allowedTools, --output-format json]` — ни `--safe-mode`,
ни `--setting-sources=`. При `clean_cwd=False` рабочим каталогом остаётся корень
репозитория, то есть `CLAUDE.md` подхватывается. Это не дефект центра (там это
осмысленно), но это ровно та разница, из-за которой перенос обязан
восстанавливать нужную семантику явно.

Дополнительно: `audit_runner.build_env` не пропускает в процесс конвейера ничего
кроме `PATH/LANG/LC_ALL/TZ` (+3 системные), а `harden_process_env` ставит
`AUDIT_DISABLE_DOTENV=1`. `CLAUDE.md` в релизный бандл не входит
(`deploy_audit_worker.BUNDLE_INCLUDE` — явный allowlist).

## 4. KI-1 — подтверждён и оказался шире формулировки

Известная проблема формулировалась как «`{DISCIPLINE_PROJECT_PARAMS}` не
вставляется в ru/en шаблон text_analysis». Факт хуже: плейсхолдера нет **ни в
одном** файле под `prompts/` (grep — ноль совпадений). Вместе с ним отсутствуют
`{DISCIPLINE_TRIAGE_TABLE}`, `{DISCIPLINE_TEXT_ANALYSIS}`,
`{DISCIPLINE_COMPACT_STRATEGY}`.

Код подстановки при этом исправен: `discipline_service.py:333`
`"{DISCIPLINE_PROJECT_PARAMS}": profile.project_params`. То есть
`project_params.md`, `triage_table.md`, `compact_strategy.md` всех 14 дисциплин
читаются, трижды хэшируются, доставляются в пакете — и не доходят до модели,
потому что потребителя нет.

По §44 задания KI-1 в 11F **не чинится**: он не блокирует исполнение. Зафиксирован
в `11F_KNOWN_ISSUES.md` с уточнённой формулировкой.

## 5. Найдено попутно (не чинится в 11F)

* **KI-3. EN-шаблоны рассинхронены с RU при `"synced": true` в `_sync.json`.**
  Все восемь пар. Из EN-шаблона `text_analysis` выпал целый инженерный блок
  «Паттерны текстовых ошибок» (RU строки 59-77). В модель уходит EN
  (`task_builder.load_template_for_llm:159`) — то есть боевой аудит идёт без
  этого блока. Это дефект качества, а не исполнения.
* **KI-4.** «Normative reference — provided in system context» в EN-шаблоне —
  дефект перевода: в RU на том же месте рабочий `{DISCIPLINE_NORMS_FILE}`.

## 6. Вывод

Скрытых зависимостей от `CLAUDE.md`, кроме шкалы severity, не обнаружено:
остальной инженерный контекст (роль, категории, приоритет PDF над MD, формат
нормативной ссылки) присутствует в файловых шаблонах и инлайновых промптах.
Единственный перенос, который делает 11F, — `SEVERITY_SEMANTICS` на четыре
оставшиеся модельные стадии worker-участка.
