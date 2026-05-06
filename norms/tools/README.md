# norms_search

**Norms = единственный доверенный source of truth по нормативной базе.**

Никакого WebSearch. Никакого bootstrap/seed из сторонних проектов
(в т.ч. не используем `PDF-proverka/norms/norms_db.json` как truth
или seed). Если нормы нет ни в `vault/`, ни в `status_overrides.yaml` —
это честно фиксируется как «не покрыто базой» и уходит в
`missing_norms_queue` (см. ниже).

## Source of truth

- **`vault/*.md`** — тексты нормативных документов (кроме `MOC - *.md`).
- **`tools/status_overrides.yaml`** — *authoritative manual mapping layer*.
  Единственное место для ручных знаний: replaced / cancelled / outdated
  edition / aliases. Никаких внешних источников.

Итоговый машинный артефакт — **`tools/status_index.json`**. Он собирается
скриптом `tools/build_status_index.py` **только** из этих двух источников.

## Схема `status_overrides.yaml`

Ключ — код нормы в каноничной форме (как в `status_index.json → "code"`).
Матчинг устойчив к точкам / подчёркиваниям / регистру / лишним пробелам.

```yaml
overrides:
  "СП 30.13330.2016":
    doc_status: active              # active | replaced | cancelled | unknown
    edition_status: outdated        # current | outdated | unknown | null
    current_version: "СП 30.13330.2020"
    aliases:
      - "СП 30.13330.2016"
      - "сп 30_13330_2016"
    details: "В базе хранится актуальная редакция 2020"
    source_url: null
    last_verified: "2026-04-17"

  "СНиП 2.04.01-85":
    doc_status: replaced
    edition_status: null
    replacement_doc: "СП 30.13330.2020"
    aliases: ["СНиП 2.04.01-85*"]
    details: "Старый документ заменён актуальным СП"
    last_verified: "2026-04-17"

  "ВСН 123-90":
    doc_status: cancelled
    edition_status: null
    details: "Документ отменён без прямой замены"
```

Короткая форма для обратной совместимости:

```yaml
overrides:
  "ГОСТ 25150-82": cancelled
  "СНиП 41-01-2003":
    replaced_by: "СП 60.13330.2020"
```

После правки — пересобрать индекс:

```bash
python3 tools/build_status_index.py
```

## Схема `status_index.json`

```json
{
  "meta": {
    "indexed_at": "2026-04-17T...",
    "vault_path": "/.../vault",
    "total": 337,
    "totals_by_doc_status":        {"active": 337, "replaced": 0, "cancelled": 0, "unknown": 0},
    "totals_by_edition_status":    {"null": 337, "outdated": 0, "current": 0, "unknown": 0},
    "totals_by_effective_status":  {"active": 337, "outdated_edition": 0, "replaced": 0, "cancelled": 0, "unknown": 0},
    "coverage_by_type": {
      "ГОСТ":   {"total": 153, "has_text": 153, "override_only": 0},
      "ГОСТ Р": {"total": 57,  "has_text": 57,  "override_only": 0}
    },
    "override_only": [],
    "parse_failures": ["..._document.md"]
  },
  "norms": [
    {
      "code":             "СП 256.1325800.2016",
      "aliases":          ["СП 256.1325800.2016", "СП 256_1325800_2016"],
      "type":             "СП",
      "year":             2016,
      "title":            "...",
      "file":             "СП 256_1325800_2016_..._document.md",
      "doc_status":       "active",
      "edition_status":   null,
      "replacement_doc":  null,
      "current_version":  "СП 256.1325800.2016",
      "details":          null,
      "source_url":       null,
      "last_verified":    null,
      "parse_confidence": "high",
      "source":           "vault",
      "authoritative":    true,
      "has_text":         true
    }
  ]
}
```

`doc_status` ∈ {`active`, `replaced`, `cancelled`, `unknown`}
`edition_status` ∈ {`current`, `outdated`, `unknown`, `null`}
`source` ∈ {`vault`, `override_only`}
`authoritative=true` для всего, что пришло из vault или overrides.
`has_text=true` только для записей, у которых есть реальный MD-файл.

## Python-API (`tools/norms_api.py`)

```python
from norms_api import (
    load_status_index,
    detect_family,
    is_supported_family,
    get_norm_status,
    get_paragraph,
    semantic_search,
)

get_norm_status("  сп 256_1325800_2016  ")
#  → matched_code = "СП 256.1325800.2016", status = "active",
#    authoritative=True, resolution_reason="exact", source="vault"

get_norm_status("ГОСТ 99999-0000")
#  → found=False, resolution_reason="not_in_index",
#    supported_family=True, needs_manual_addition=True

get_norm_status("СО 153-34")
#  → found=False, resolution_reason="not_in_index",
#    supported_family=True, suggested_action будет add_manual_override

get_norm_status("какая-то левая строка")
#  → found=False, resolution_reason="unsupported_family",
#    supported_family=False, needs_manual_addition=False

get_paragraph("ГОСТ 10180-2012", "4", max_lines=50)
#  → found, text, file, line, status, has_text, resolution_reason

semantic_search("прочность бетона", top=5)
#  → list[dict]; пустой запрос или ошибка модели → []
```

### Схема `get_norm_status(code)` ответа

```
query                str              исходная строка
normalized_query     str              trimmed + collapsed whitespace
found                bool
matched_code         str | null       каноничный код из status_index.json
status               str              effective: active | outdated_edition |
                                       replaced | cancelled | unknown
doc_status           str | null       active | replaced | cancelled | unknown
edition_status       str | null       current | outdated | unknown | null
authoritative        bool             true = пришло из vault / override
resolution_reason    str              exact | alias | manual_override |
                                       not_in_index | unsupported_family | not_found
detected_family      str | null       ГОСТ, ГОСТ Р, СП, СНиП, ВСН, МДС, РД,
                                       ПУЭ, ФЗ, ПП РФ, СО
supported_family     bool             семейство распознано нашими правилами
needs_manual_addition bool            true когда supported_family и нет в index
replacement_doc      str | null
current_version      str | null
title                str | null
file                 str | null
type                 str | null
year                 int | null
details              str | null
source_url           str | null
last_verified        str | null
parse_confidence     str | null       high | low | null
source               str              vault | override_only | not_found
```

### Схема `get_paragraph(...)` ответа

```
query_code          str
matched_code        str | null
paragraph           str
found               bool
text                str | null
file                str | null
line                int | null
status              str
doc_status          str | null
edition_status      str | null
authoritative       bool
has_text            bool
resolution_reason   str               exact | alias | manual_override |
                                       no_document_text | paragraph_not_found |
                                       not_in_index | unsupported_family | not_found
replacement_doc     str | null
truncated           bool
```

## MCP-сервер

Text-tools (legacy, не трогали): `list_norms`, `find_paragraph`, `search`,
`norm_info` — возвращают текст.

JSON-tools (authoritative, новая логика):
- `get_norm_status(code)` → dict
- `get_paragraph_json(code, paragraph, max_lines=50)` → dict
- `semantic_search_json(query, top=5, code_filter="")` → list[dict]
  (пустой запрос / ошибка модели → `[]`, не исключение)

```bash
claude mcp add norms -- /home/coder/projects/Norms/tools/venv/bin/python3 \
    /home/coder/projects/Norms/tools/mcp_server.py
```

## Intake отсутствующих норм

Если во внешнем артефакте (`03_findings.json`, список кодов, и т.д.)
встречаются нормы, которых нет в authoritative index — их нужно честно
поставить в очередь на ручное добавление.

```bash
# Из findings:
python3 tools/intake_missing_norms.py --findings /path/to/03_findings.json

# Из JSON-массива или txt:
python3 tools/intake_missing_norms.py --list missing.json

# Inline:
python3 tools/intake_missing_norms.py --codes "ГОСТ 00000-9999,СО 153-34"
```

Что создаётся:

| Файл | Назначение |
|---|---|
| `tools/missing_norms_report.json` | Полный отчёт: resolved + unresolved + family_stats |
| `tools/missing_norms_queue.json`  | Машинная очередь unresolved записей |
| `tools/missing_norms_queue.md`    | Человекочитаемая версия очереди |

### Схема `missing_norms_queue.json`

```json
{
  "meta": {
    "generated_at": "2026-04-17T...",
    "source": "findings | list | codes",
    "input_path": "/path/...",
    "total": 3,
    "actions": {"add_document_to_vault": 2, "add_manual_override": 1}
  },
  "items": [
    {
      "raw_norm":          "ГОСТ 99999-0000",
      "normalized_norm":   "ГОСТ 99999-0000",
      "detected_family":   "ГОСТ",
      "supported_family":  true,
      "resolution_reason": "not_in_index",
      "suggested_action":  "add_document_to_vault",
      "affected_findings": ["F-002"],
      "contexts":          ["см. ГОСТ 99999-0000..."],
      "cited_as":          ["ГОСТ 99999-0000"]
    }
  ]
}
```

`suggested_action` ∈:
- `add_document_to_vault` — семейство поддерживается, есть практика хранить
  полный текст документа в vault (ГОСТ / ГОСТ Р / СП / СНиП / ВСН / МДС / РД /
  ПУЭ / ФЗ). Положи MD в `vault/`, перезапусти builder.
- `add_manual_override` — семейство поддерживается, но текст в vault мы обычно
  не держим (ПП РФ / СО). Добавь запись в `status_overrides.yaml`.
- `review_family_support` — семейство не распознано ни одним правилом.
  Нужно расширить `detect_family()` или отказаться от этого кода.

## Coverage report

Сравнивает `status_index.json` с произвольным входом — без всякой
зависимости от первого проекта или кеша норм.

```bash
# Только статистика индекса:
python3 tools/coverage_report.py

# Покрытие findings:
python3 tools/coverage_report.py --findings /path/to/03_findings.json

# Из queue:
python3 tools/coverage_report.py --queue tools/missing_norms_queue.json

# Машинный вывод:
python3 tools/coverage_report.py --queue tools/missing_norms_queue.json --json
```

Отчёт показывает:
- сколько норм authoritative-резолвятся (по семействам);
- сколько относятся к **поддерживаемым семействам, но отсутствуют** (→ в queue);
- сколько попало в **unsupported_family** (→ review);
- breakdown по `ГОСТ / ГОСТ Р / СП / СНиП / ВСН / МДС / РД / ПУЭ / ФЗ / ПП РФ / СО / other`.

## Setup после clone

```bash
cd tools
python3 -m venv venv
source venv/bin/activate
pip install numpy sentence-transformers pyyaml mcp

python3 build_status_index.py           # source of truth (vault + overrides)
python3 list_active.py --quiet          # legacy active_norms.json
python3 extract_refs.py                 # refs_graph.json
python3 embed_norms.py                  # embeddings.npz
python3 build_paragraph_index.py        # paragraphs.jsonl
python3 embed_paragraphs.py             # paragraphs_embeddings.npz
```

## Файлы

| Файл | Назначение |
|---|---|
| `build_status_index.py`   | Собирает **`status_index.json`** из vault + overrides. |
| `norms_api.py`            | **Python-API** для внешних клиентов (PDF-проверка и т.п.). |
| `intake_missing_norms.py` | Intake unresolved норм → queue + report. |
| `coverage_report.py`      | Отчёт о покрытии authoritative index. |
| `status_overrides.yaml`   | Authoritative manual mapping layer. |
| `status_index.json`       | Single source of truth по статусам. |
| `parse_filename.py`       | Парсинг имён MD (шумо-устойчивый). |
| `list_active.py`          | Legacy active_norms.json (для старых text-tools). |
| `find_paragraph.py`       | CLI поиска пункта по номеру. |
| `search.py`               | CLI семантического поиска. |
| `mcp_server.py`           | MCP-сервер: text-tools + JSON-tools. |
| `smoke_test.py`           | Smoke-тест authoritative API. |
| `embed_norms.py`, `embed_paragraphs.py`, `extract_refs.py`, `build_paragraph_index.py` | Индексные билдеры. |
| `active_norms.json`       | **Legacy** — не использовать как truth из внешних проектов. |
| `venv/`                   | Python-окружение. |

## Smoke-тест

```bash
python3 tools/smoke_test.py                  # полный прогон с эмбеддингами
python3 tools/smoke_test.py --skip-semantic  # без e5-base (~1 сек)
```

Проверяет: active из vault, outdated_edition/replaced/cancelled через overrides,
supported-family-not-in-index, unsupported_family, get_paragraph на override-only
и not_in_index, интейк очереди, форму семантического поиска.

## Как интерпретировать поля

| Поле                  | Что значит в машинном pipeline |
|---|---|
| `authoritative=true`  | Можно брать как факт, без WebSearch. |
| `authoritative=false` | Authoritative-ответа **нет**. Нельзя заполнять статус гипотезой. |
| `resolution_reason=exact` / `alias` / `manual_override` | Успешный authoritative resolve. |
| `resolution_reason=not_in_index` | Надо ставить в `missing_norms_queue`. |
| `resolution_reason=unsupported_family` | Надо расширять `detect_family()` или refuse. |
| `needs_manual_addition=true` | Сигнал для intake: этот код нужно внести вручную. |
| `has_text=true`       | MD-файл доступен — `get_paragraph` может вернуть текст. |
| `has_text=false`      | Override-only запись, полного текста нет. |
```
