# 11F — граница WORKER / CENTER

Главный вывод разведки: **границу не нужно проектировать, она уже объявлена в
коде** и совпадает с целевой схемой задания. Два независимых объявления:

* `backend/app/pipeline/remote_audit_runner.py:876` — `WORKER_STAGE_PLAN`:
  что удалённый профиль **обязан** уметь;
* `backend/app/pipeline/remote_audit_runner.py:29` — `FORBIDDEN_STAGES`:
  что ему **запрещено**.

Запрет проверяется дважды и с разных сторон. До прогона — `PipelineManager.
_central_stage_blocked()` (env `AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED`, который
ставит сам `remote_audit_runner`) возвращает этапу «пропущено, центральный».
После прогона — `audit_stage_history()` сверяет журнал и **валит сборку пакета**,
если центральный этап всё-таки отработал. Второй барьер существует потому, что
импортёр центра отверг бы такой пакет целиком, и многочасовой прогон
выбрасывался бы уже после транспорта.

Задача 11F поэтому не «определить границу», а **пройти её исполнением**.

## Классификация по фактическому коду

| Стадия | Класс | Модель | Почему |
|---|---|---|---|
| `crop_blocks` | DETERMINISTIC_PORTABLE | нет | PyMuPDF-рендер; сеть только как ПЕРВЫЙ выбор (`crop_url`), локальный PDF — гарантированный офлайн-фолбэк |
| `document_graph_v2` | DETERMINISTIC_PORTABLE | нет | чистый Python поверх `result.json` + MD |
| `block_context` | DETERMINISTIC_PORTABLE | нет | детерминированный `build_block_context` / Вектограф по вектор-слою. Локальных LLM на платформе больше нет |
| `block_grounding` | DETERMINISTIC_PORTABLE | нет | флаг OFF, fail-soft |
| `block_analysis` | **WORKER_TARGET** | 1 вызов на блок × число ног | мультимодальный, самый дорогой; в 11F — главный новый участок |
| `text_analysis` | **WORKER_TARGET** | 1 | доказан боевым прогоном 11D.2 |
| `findings_merge` | **WORKER_TARGET** | 1 | доказан боевым прогоном 11E.1 |
| `findings_review` | **WORKER_TARGET** | 0 + условные вызовы «стража отсутствия» | детерминированное ядро переносимо целиком; LLM-фаза условна |
| `critic_v2_triage` | DETERMINISTIC_PORTABLE | 0 (OFF) | post-processing за флагом |
| `optimization` | **WORKER_TARGET** | 1 | читает только артефакты версии |
| `optimization_review` | **WORKER_TARGET** | до 2 | критик + условный корректор |
| `norm_verify` | **CENTER_ONLY** | несколько | центральная норм-база + запись в глобальный реестр отсутствующих норм + norms-MCP |
| `debt_control` | **CENTER_ONLY** | есть | сквозной реестр замечаний между версиями |
| `decision_carryover` | **CENTER_ONLY** | есть | вердикты эксперта и `knowledge_base/decisions_log` |
| `excel` | **CENTER_ONLY** | нет | сводный отчёт по всем проектам центра |

NEEDS_RESEARCH — пусто: каждая стадия классифицирована по коду, а не по догадке.

## Что реально мешало переносу, и это не граница

Граница объявлена честно. Мешает другое — **три стадии из WORKER_TARGET сегодня
физически не могут дойти до модели через провайдерский слой**:

1. **`block_analysis`.** Ветка Claude (`call_claude_cli_for_block`,
   `gemma_findings_only.py:1435`) поднимает `claude -p` **прямым**
   `asyncio.create_subprocess_exec`, минуя `_run_cli`, а значит и мост. Модели
   выдаётся `--allowedTools Read,Write`: PNG она читает сама с диска, результат
   пишет во временный файл. На воркере это одновременно и обход провайдерского
   слоя, и свободный доступ к файловой системе. Параметр `image_paths` у
   `_run_cli` существует, но **ветка моста его игнорирует** — при активной
   привязке изображения молча теряются.

2. **`optimization` и `optimization_review`.** Идут через `_run_cli`, то есть
   мост их перехватит, — но обе ветки написаны в расчёте на то, что модель
   **сама запишет** `optimization.json` инструментом Write. Под мостом
   инструментов ноль, и записывать некому.

3. **`findings_review`, фаза «страж отсутствия».**
   `absence_guard.run_claude_verification` (`absence_guard.py:189`) — снова
   прямой `subprocess.run`, и вдобавок **fail-soft**: любая ошибка даёт `{}`, и
   этап выглядит выполненным. На воркере `HOME` изолирован каталогом попытки,
   поэтому CLI там неавторизован — то есть штатным исходом была бы **тихая
   деградация**, а не отказ. Это ровно тот «silent legacy fallback», который
   §23-F требует исключить.

Отдельно — **дефолтные модели**. `block_batch = ensemble/gpt-codex`
(OpenRouter GPT-5.4 + Codex CLI), `optimization = ensemble/claude-codex-opt`.
На воркере с единственной Claude-подпиской обе недостижимы: Codex запрещён
§35, OpenRouter требует платного ключа, которого у воркера нет и не должно
быть. При этом `claude-*` — **существующий production-транспорт** обеих стадий,
а не выдуманный: ветки `is_claude_cli_model` / `model.startswith("claude-")`
написаны и работают на центре. Поэтому 11F не переизобретает ML-архитектуру, а
выбирает уже существующий транспорт конфигурацией снимка `stage_models.json`,
и фиксирует это отклонение от центрального дефолта явно.

## Целевая схема, подтверждённая кодом

```
ЦЕНТР
  └─ пакет версии (projects_v2-раскладка + снимки промптов/моделей/флагов
                   + профиль дисциплины + runtime-конфиг)
       ↓
ВОРКЕР  (audit_runner → remote_audit_runner → _dispatch_action → _run_ocr_pipeline)
  ├─ crop_blocks            0 вызовов
  ├─ document_graph_v2      0
  ├─ block_context          0
  ├─ block_analysis         N вызовов (N = число блоков)
  ├─ text_analysis          1
  ├─ findings_merge         1
  ├─ findings_review        0 + условные
  ├─ optimization           1
  └─ optimization_review    1–2
       ↓
PRE-CENTRAL HANDOFF  (пакет результата + audit_manifest.json + usage_report.json)
       ↓
ЦЕНТР
  └─ norm_verify → debt_control → decision_carryover → excel
```

`resume_hint` в манифесте результата и `central_only_stages` прямо называют
центру следующий обязательный этап. Никакой «остановки по ошибке» на границе
нет: это штатный HANDOFF.
