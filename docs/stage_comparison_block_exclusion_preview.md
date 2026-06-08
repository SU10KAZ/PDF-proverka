# Stage Comparison — block exclusion preview (Gate-1, aggregating, mark-only)

**Дата:** 2026-06-08
**Статус:** mark-only preview, **НЕ подключён** к Qwen/MD/Opus. Без флагов влияния на работу нет.
**Модуль:** [backend/app/services/stage_comparison/block_exclusion_preview.py](../backend/app/services/stage_comparison/block_exclusion_preview.py)

## Зачем

Единый безопасный слой ПОВЕРХ двух уже существующих mark-only прекчеков —
[visual_block_equivalence](stage_comparison_visual_block_equivalence.md) (image-блоки)
и [text_block_equivalence](stage_comparison_text_block_equivalence.md) (text/table).
Отвечает на ОДИН вопрос, ничего реально не исключая:

```text
если бы мы включили enforce, какие блоки были бы исключены и почему?
```

```text
visual_block_equivalence.json  ┐
                               ├─→ build_block_exclusion_preview
text_block_equivalence.json    ┘        → объединить кандидатов (modality=visual|text)
                                        → block_exclusion_preview.json (ОТДЕЛЬНЫЙ артефакт)
```

## Что НЕ меняется (инвариант безопасности)

`mode=mark_only`, `enforced=false` всегда (на уровне отчёта И каждого item).
Реального skip нет. Не трогаются и даже не импортируются: Qwen / MD enrichment /
Opus / `enriched_comparison` / `enriched.md` / `links.json` /
`page_alignment.json` / `comparison_result.json` / findings. Артефакт — НОВЫЙ
отдельный файл `pairs/<pid>/block_exclusion_preview/block_exclusion_preview.json`.

## Правила решения (decision)

| источник статус | decision | exclude_from_qwen | exclude_from_opus_md |
|---|---|---|---|
| `identical_visual` | `candidate_exclude` | **true** | **true** |
| `minor_render_noise` | `review_only` | false | false |
| `changed_visual` / `uncertain` / `render_failed` | `keep` | false | false |
| `identical_text` | `candidate_exclude` | false | **true** |
| `near_identical_text` | `review_only` | false | false |
| `changed_text` / `uncertain_text` | `keep` | false | false |

`near_identical_text` и `minor_render_noise` НЕ исключаются автоматически
(`review_only`). `identical_text` исключается только из Opus/MD (текст не
описывается Qwen). Только `identical_visual` даёт кандидата на исключение из Qwen.

`skipped_*`-статусы источников в `items` НЕ попадают (это не результаты
сравнения) — считаются только в `summary.skipped_source_links_*`.

**Дедупликация по построению:** каждая связь сравнивается ровно в одном слое
(text/table → text, image → visual; в «чужом» артефакте она `skipped_non_text` /
`skipped_non_image`), поэтому один `(left_block_id, right_block_id)` даёт не более
одного item.

## Артефакт

```json
{
  "schema_version": 1, "session_id": "...", "pair_id": "...", "generated_at": "...",
  "mode": "mark_only", "enforced": false,
  "sources": {"visual": "visual_block_equivalence/visual_block_equivalence.json",
              "text": "text_block_equivalence/text_block_equivalence.json"},
  "sources_present": {"visual": true, "text": true},
  "summary": { "...": "см. ниже" },
  "items": [
    {"left_block_id": "...", "right_block_id": "...", "modality": "visual|text",
     "source_status": "identical_visual|identical_text|...",
     "decision": "candidate_exclude|review_only|keep",
     "exclude_from_qwen": false, "exclude_from_opus_md": true, "enforced": false,
     "confidence": 1.0, "reason": "...", "source_reason": "...", "metrics": {},
     "source_artifact": "visual_block_equivalence|text_block_equivalence"}
  ],
  "warnings": []
}
```

`summary` (ключи задачи Gate-1 + доп. информативные): `visual_candidates`,
`text_candidates`, `qwen_exclusion_candidates`, `opus_md_exclusion_candidates`,
`near_text_review_candidates`, `blocked_by_uncertain`, `blocked_by_changed`,
`minor_render_noise_review`, `blocked_by_render_failed`, `candidate_exclude_total`,
`review_only_total`, `keep_total`, `items_total`, `visual_items`, `text_items`,
`skipped_source_links_visual`, `skipped_source_links_text`.

## API

`build_block_exclusion_preview(session_id, pair_id, *, visual_report=None,
text_report=None, read_from_disk=True, write_artifact=True, generated_at=None)`.
Если `visual_report`/`text_report` не переданы и `read_from_disk=True` — читаются
с диска через `read_pair_*` (отсутствующий артефакт → `None` → пустой вклад
слоя + warning). `read_from_disk=False` (тесты) гарантирует, что live
`comparison/sessions` НИКОГДА не читается. `read_block_exclusion_preview(...)` —
чтение готового артефакта.

## Контролируемая проверка (ПОС, без Qwen/Opus)

Session `ba413a93c5754f6c`, прогон на уже готовых visual+text артефактах:

| Пара | items | qwen_excl | opus_md_excl | near_review | minor_review | blocked_changed |
|---|---:|---:|---:|---:|---:|---:|
| pac34250b (ПОС) | 53 (8 visual + 45 text) | **0** | **3** | 29 | 1 | 20 |
| p698fce07 (ПЗУ) | 4 (3 visual + 1 text) | 0 | 0 | 0 | 0 | 4 |

Вывод подтверждён: на ПОС потенциал исключения — **3 текстовых блока из Opus/MD**,
графика реально переработана (0 visual-кандидатов). Это и есть «что было бы
исключено при enforce» — но enforce НЕ подключён.

## Безопасность

- mark-only, `enforced=false`; ни Qwen, ни Opus, ни MD pipeline не задействованы
  и не изменяются; пишется только новый артефакт;
- модуль импортирует только `read_pair_*` из visual/text слоёв + `paths`
  (тест проверяет отсутствие qwen/opus/pipeline/md-импортов через AST);
- fail-soft: отсутствие одного/обоих источников → частичный/пустой preview без
  исключений; запись атомарна (tmp → os.replace).

## Тесты

[tests/test_stage_comparison_block_exclusion_preview.py](../tests/test_stage_comparison_block_exclusion_preview.py)
— все правила decision (visual/text), skipped→no items, отсутствие одного/обоих
артефактов, summary counts, `enforced=false` всегда, отсутствие Qwen/Opus-импортов,
атомарная запись только в tmp, `read_from_disk=False` не читает live.

## Связанные файлы

- [block_exclusion_preview.py](../backend/app/services/stage_comparison/block_exclusion_preview.py)
- [visual_block_equivalence.py](../backend/app/services/stage_comparison/visual_block_equivalence.py)
- [text_block_equivalence.py](../backend/app/services/stage_comparison/text_block_equivalence.py)
- [paths.py](../backend/app/services/stage_comparison/paths.py) — `block_exclusion_preview_dir/_report_path`
