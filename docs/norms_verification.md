# Нормативная база — детальные правила

## Приоритет документов

1. Федеральные законы (ФЗ-384, ФЗ-123)
2. Технические регламенты
3. СП из перечня обязательных (ПП РФ №815)
4. СП из перечня добровольных
5. ГОСТ (национальные и межгосударственные)
6. ПУЭ (в части, не противоречащей СП)

## Проверка актуальности

Перед каждой ссылкой:
1. Сверься с `norms_reference.md` дисциплины
2. Если нет в справочнике → WebSearch
3. Укажи номер, название, статус, редакцию

**Типичные замены:**
- СП 31-110-2003 → СП 256.1325800.2016
- СП 5.13130.2009 → СП 484/485/486.1311500.2020
- ВСН 59-88 → СП 256.1325800.2016 (через цепочку)

## Формат ссылки

```
[СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-6), п. X.X.X]
```

## Работа с ПУЭ

ПУЭ-7 **не зарегистрирован Минюстом** → применяется добровольно.
При ссылке на ПУЭ давай параллельную ссылку на соответствующий СП.

## Верификация цитат в пайплайне (фактический поток)

> Обновлено по факту кода после унификации индекса норм (reserc.md #34/#35/#36/#37).
> WebSearch/WebFetch в **пайплайне** запрещены концептуально — источник истины
> offline. (Это не отменяет ручную сверку ассистентом через WebSearch в разделе
> «Проверка актуальности» выше.)

```
Статусы документов (active/replaced/cancelled) — authoritative, offline, Python:
  external_provider.resolve_norm_status() читает status_index.json
  (in-repo norms/tools/status_index.json, 565 норм — authoritative; #34).
  Нет в индексе → found=False → missing_norms_queue.json (НЕ «угадываем»).
  LLM статус НЕ решает.

Цитаты пунктов (paragraph_verified) — native Python, offline:
  _native_verify.py вызывает norms_api (get_paragraph / semantic_search) из того
  же norms/tools (#34). Совпадение цитаты с текстом пункта = word-Jaccard
  (SIMILARITY_THRESHOLD=0.30) + ЧИСЛОВАЯ сверка номиналов/сечений
  (_salient_numbers / numeric_recall, #35): если слова совпали, но числа цитаты
  в пункте отсутствуют — НЕ подтверждаем. Пишет norm_checks_llm.json.
  Fallback на Claude-чанки — только если native verify упал.

Накопительный кеш цитат:
  norms_paragraphs.json — подтверждённые цитаты пунктов; native-записи доверенные
  (verified_via="native_python" ∈ trusted, #36). norms.py update пополняет его.
```

## Поток детерминированной верификации

```
[Python] extract_norms_from_findings() → список норм
    ↓
[Python] generate_deterministic_checks() → norm_checks.json (статусы из status_index)
    ↓
[Python] verify_paragraphs_native() → norm_checks_llm.json (цитаты пунктов, offline)
    ↓  fallback: Claude-чанки только при сбое native
    ↓
[Python] merge_llm_norm_results() → финальный norm_checks.json
    ↓  meta.paragraph_verification: verified_true/false/total + by_source (#37)
```

Весь штатный путь — offline (без сети). Сеть не задействуется.

## Ключевые файлы

- `status_index.json` — **authoritative** статусы документов; в development
  лежит in-repo в `norms/tools`, а immutable releases используют общий
  `<auditmanager-root>/shared/norms/tools`;
- `NORMS_TOOLS_PATH` переопределяет весь runtime; точечные overrides —
  `NORMS_STATUS_INDEX_PATH`, `NORMS_VAULT_PATH`, `NORMS_MCP_PYTHON`;
- `norms/norms_paragraphs.json` — проверенные цитаты конкретных пунктов
- `_output/norm_checks.json` — финальный результат (статусы + цитаты)
- `_output/norm_checks_llm.json` — промежуточный результат native verify
- `norms_db.json` — **legacy/CLI-only, НЕ authoritative** (исторический кеш статусов;
  status_index из norms/tools заменил его как источник истины)

## Поля замечания

- `norm_quote` — цитата нормы или `null`
- `norm_confidence` — `0.0–1.0`

## Инвариант: нормативная стадия не запускается без сервера норм

`norm_verify` обязана заявлять инструменты `mcp__norms__*`. Проверка живёт в
`codex_runner.assert_norms_stage_wired()` и стоит на **обоих** входах Codex
(`run_codex_exec` и `run_codex_json_messages`).

**Почему это предохранитель против класса ошибки, а не против модели.** У Codex
два входа. Сервер норм исходно подключили только к одному (`run_codex_exec`,
через `_tool_config_args`), а `norm_verify` уходит во второй — и оставалась
вообще без MCP: сверяла статус норм по памяти модели, молча, без ошибки.
Запрещать конкретную модель тут бесполезно — забыть пробросить инструменты
можно на любой. Инвариант «стадия обязана заявить сервер норм» ловит это
независимо от выбора модели в интерфейсе.

**Fail-closed на отсутствие базы.** `assert_norms_mcp_available()` проверяет не
только путь к Python, но и импорт `mcp`/ML-зависимостей, непустой
`status_index.json`, vault, semantic index и release-owned MCP server. Стадия
верификации отдельно запрещает зелёный результат с отсутствующим/пустым
authoritative index. Без этих проверок Codex обрывал сессию невнятным
`No such file or directory`, Claude молча терял `mcp__norms__*`, а native-контур
мог завершиться `OK` с `authoritative=0`. Установка общего runtime —
`python scripts/setup_norms_runtime.py`.
