# ProjectChange V3 — включение живого inference

Статус на 2026-09-19: **inference выключен.** В окружении прод-сервиса
(`/home/coder/.config/auditmanager/backend.phaseb.env`, `backend.secrets.env`)
нет ни одной переменной `PROJECT_COMPARISON_*`, поэтому действуют значения по
умолчанию: движок `v3`, живой вызов запрещён. Любой запуск сравнения пары
заканчивается состоянием `FAILED` с `reason_code = v3_inference_kill_switch`.
В этом режиме CLI не опрашивается, модель не вызывается, legacy не запускается.

Источник истины — код [provider_gate.py](../backend/app/services/project_change_v3/provider_gate.py)
и [engine.py](../backend/app/services/project_change_v3/engine.py). Других
переключателей нет.

## Условия допуска живого прогона

Прогон пары допускается, только если выполнены **все** условия:

| # | Условие | Значение по умолчанию |
|---|---------|-----------------------|
| 1 | `PROJECT_COMPARISON_ENGINE` ∈ {`v3`, `projectchange_v3`, `project_change_v3`} | `v3` |
| 2 | `PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE` ≠ `1` | `0` |
| 3 | `PROJECT_COMPARISON_V3_ALLOW_INFERENCE` = `1` (строго) | `0` → выключатель закрыт |
| 4 | `PROJECT_COMPARISON_V3_PROVIDER_READY` = `1` **или** офлайн-проверка `gateway.validate_runtime(require_vision=True, deep=False)` вернула `ok` | `0` + проверка |

Что проверяет пункт 4 (без единого запроса к провайдеру): у codex CLI есть
`--output-schema`, `--sandbox`, `--ignore-user-config`, `--disable`, `--config`,
`--image`; `features list` подтверждает, что shell/view_image/browser/computer
use/hooks/plugins выключены; в окружение сессии не попали секреты.
`PROJECT_COMPARISON_V3_PROVIDER_READY=1` пропускает эту проверку, поэтому
использовать его стоит только осознанно.

Бинарь codex находится по обычным правилам шлюза сравнения:
`STAGE_COMPARISON_AI_CODEX_BIN`, затем `codex` в `PATH`, затем расширение VS
Code. Это не переключатель V3, а общая настройка шлюза.

`PROJECT_COMPARISON_ENGINE=legacy` — только ручной откат на старый конвейер.
Автоматического перехода на legacy при ошибке, квоте или отказе проверки нет:
V3 закрывается в `FAILED`.

## Как включить (когда появится квота)

1. Проверить, что в очереди нет живых задач. Рестарт бэкенда прерывает их.
2. Дописать в `/home/coder/.config/auditmanager/backend.phaseb.env`:
   ```
   PROJECT_COMPARISON_V3_ALLOW_INFERENCE=1
   ```
3. `systemctl --user restart auditmanager-backend.service` и проверить
   `systemctl --user is-active auditmanager-backend.service`.
4. Выключение: удалить строку (или поставить `0`) и перезапустить; либо
   аварийно `PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE=1`.

## Модель и транспорт

- Модель `gpt-6-astra`, reasoning `xhigh`; промпты — замороженные
  `MAPPER_PROMPT.txt` / `MINER_PROMPT.txt` / `DEDUPE_PROMPT.txt`
  (sha256 `324696ce…`, `7837a504…`, `b31304ff…`), совпадают с исследованием V3.
- Транспорт: `gateway.call_codex` → `codex exec -m gpt-6-astra -s read-only
  --ephemeral --ignore-user-config --ignore-rules --output-schema … -i <картинки> -`,
  `retries=0`, таймаут 3600 с на вызов.
- Число вызовов на пару: 1 Mapper + по одному Miner на семантический регион +
  1 Dedupe (если изменений ноль, Dedupe не вызывается).
- Полезная нагрузка побайтово равна замороженной (`SOURCE_PACKAGING_PARITY.json`:
  A — 25/25, B — 13/13 точных промптов майнера; промпт Mapper совпадает).

## Известные расхождения транспорта с исследованием — решить ДО включения

Упаковку источников они не затрагивают (сверка полная), но от них зависит, дойдёт
ли живой прогон до результата. Во всех случаях прод закрывается в `FAILED`, а не
выдаёт ложные изменения.

1. **Промпты длиннее 1 048 576 символов.** В паре A это регионы A-R003, A-R011
   и A-R022 (у A-R003 ≈ 1,65 млн символов). Исследование доставляло их через
   `codex app-server` и `thread/inject_items`: куски шли в историю, последний кусок
   запускал ход, склейка байт-в-байт равна замороженному промпту
   (`experiments/project_change_272/ai_first_semantic_mapping_projectchange_v3_pair_a_recover.py`).
   У прода такого транспорта нет, поэтому такие регионы упадут с отказом провайдера.
2. **Повтор после отказа проверки доказательств.** Исследование повторяло
   вызов майнера с тем же входом (`_RETRY_n`, лимит по провенансу). Прод при
   первой же ошибке `Untraceable evidence` завершает весь прогон `FAILED`.
3. **Явная «стандартная» обработка.** Для A-R017 исследование запрашивало
   `cyberAccessProgram: standard`, у `codex exec` в проде аналога нет.
4. **Флаги CLI.** Исследование ставило `service_tier=priority`,
   `web_search="disabled"`, `mcp_servers={}`, `project_doc_max_bytes=0`,
   изоляцию bubblewrap и `--disable memories`. Прод выключает более широкий
   набор возможностей (включая `view_image` и `standalone_web_search`),
   использует `--ignore-user-config` и пустой временный `cwd`, но
   `service_tier` и `web_search` явно не задаёт.
5. Проверка гранулярности (`GRANULARITY_AUDIT`) в исследовании была
   диагностической, на карту не влияла; в проде её нет.

## Проверка после включения (сначала на одной паре)

- `GET /api/stage-comparison/sessions/{sid}/pairs/{pid}/production/state` →
  `engine=projectchange_v3`, `status` ∈ {COMPLETED, REVIEW}, `model_calls` > 0,
  `legacy_invoked=false`.
- `GET /api/stage-comparison/objects/{object}/project-changes` → карточки пары
  в `project-change-view/1`.
- `GET /human-mapping/?object={object}&comparison={pid}` → регионы именно этой
  пары.
