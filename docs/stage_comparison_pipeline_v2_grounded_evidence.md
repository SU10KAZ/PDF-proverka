# Pipeline V2 — Grounded Vision Evidence Integration (mark-only)

**Дата:** 2026-06-11
**Статус:** offline / mark-only слой; включается в dry-run только при наличии
grounding-результата.
**Модуль:** [pipeline_v2_grounded_evidence.py](../backend/app/services/stage_comparison/pipeline_v2_grounded_evidence.py)

## Зачем

Pipeline V2 умеет по отдельности:

```text
deterministic diff → находит изменения (entity_diff_report.deltas)
Qwen vision        → описывает графику (graphic_vision_enrichment)
grounding          → проверяет vision по текстовому слою блока (graphic_vision_grounding)
UI                 → показывает grounded / weak / rejected
```

Но дельта и подтверждённое визуальное изменение жили в РАЗНЫХ артефактах. Этот
слой их **связывает** и передаёт связку как evidence в delta-explanation/critic:

```text
delta: 400А → 200А (на блок-паре OLD↔NEW)
  + grounded vision: QF5 400А (OLD) и QF5 200А (NEW) подтверждены по anchors
  + block-link context: OLD block ↔ NEW block, страницы 52 / 21
  = grounded evidence для дельты (use_in_critic=true)
```

## Что это НЕ делает

Это **mark-only evidence layer**:

* НЕ создаёт замечаний автоматически;
* НЕ enforce'ит и не меняет deterministic дельты;
* НЕ применяет связи блоков;
* `rejected_*` / `ungrounded` vision-выводы **НИКОГДА** не используются как факт
  для critic'а.

## Поток

```text
entity_diff_report.deltas
  + graphic_vision_grounding_report (grounded/weak/rejected сущности + изменения)
  + (optional) visual_equivalence_gate / block_link_preview (page-контекст)
  → build_grounding_index            (по блок-парам и одиночным блокам)
  → для каждой дельты в зоне grounding:
        match_delta_to_grounded_vision  (designator-pair → value-pair → single → noop → rejected)
        build_delta_evidence_card       (evidence_level + use_in_critic)
  → grounded_evidence_report.json
  → (downstream) delta_explanation подмешивает grounded/weak в prompt
```

## Входные артефакты

| Артефакт | Обязателен | Назначение |
|---|---|---|
| `entity_diff_report.json` | да | дельты (что менять) |
| `graphic_vision_grounding_report.json` | да | grounded/weak/rejected сущности и изменения |
| `block_link_preview_report.json` | нет | page-контекст (left/right page) |
| `visual_equivalence_gate_report.json` | нет | page-контекст (приоритетный источник) |
| `graphic_vision_enrichment_report.json` | нет | контекст (сейчас не обязателен для матчинга) |
| `left/right_normalized_document_model.json` | нет | контекст |

Если grounding-report отсутствует/пуст → `status=skipped_no_grounding`, отчёт
валиден и пуст, dry-run не падает.

## Зона действия (vision-relevant дельты)

Карточка строится ТОЛЬКО для дельт, чьи блоки покрыты grounding-итемом:

* `changed` — блок-пара `(left_block_id, right_block_id)` совпала с итемом;
* `added` — `right_block_id` совпал с правым блоком итема;
* `removed` — `left_block_id` совпал с левым блоком итема.

Дельты вне зоны grounding (штампы, текст, незаснятая графика) в отчёт не попадают
— у них вообще не может быть визуального evidence. `summary.deltas_total` — это
число дельт В ЗОНЕ; `deltas_total_all_diff` — общее число дельт в entity_diff
(для прозрачности).

## Уровни evidence (`evidence_level`)

| Уровень | Когда | `use_in_critic` |
|---|---|---|
| `grounded` | есть подтверждённое (confirmed) совпадение значений дельты | **true** |
| `weak` | только weakly_grounded / частичное совпадение одной стороны | **true** (как hint) |
| `none` | grounding не подтверждает значения дельты | false |
| `conflict` | grounding противоречит дельте (noop на changed; confirmed+rejected) | false |
| `rejected_only` | дельта совпала ТОЛЬКО с rejected vision (designator-range/series) | false |

### Сигналы матчинга (по убыванию силы)

1. **designator-pair** — один дизайнатор `D` есть в grounded OLD с номиналом =
   `delta.old` И в grounded NEW с номиналом = `delta.new` (QF5 400А→200А). Самый
   сильный сигнал, поднимает дизайнатор в evidence. Если confirmed — value-pair и
   одиночные частичные подтверждения той же дельты не добавляются (избыточны).
2. **value-pair** — `delta.old` ∈ grounded OLD entity И `delta.new` ∈ grounded
   NEW entity (без общего дизайнатора).
3. **grounded change** — `delta.old/new` совпали с `old_values/new_values`
   grounded-изменения.
4. **single-side** — added/removed: значение ∈ grounded/weak entity нужной
   стороны; для changed — частичное подтверждение одной стороны (weak).
5. **noop-conflict** — changed-дельта, но grounding пометил изменение как noop →
   conflict-сигнал (не факт).
6. **rejected** — значения дельты совпали с rejected_entities/rejected_changes
   (`rejected_designator_range`, `rejected_artificial_series`, `rejected_noop`) →
   никогда не факт.

## Нормализация (`normalize_evidence_token`)

Переиспользует `normalize_engineering_token` из grounding-модуля (единая
канонизация для основного пути и evidence-слоя):

* `А`(кир) ↔ `A`; `х`/`×` → `x`; дефисы `–—−` → `-`;
* `400 А` → `400a` (склейка число+единица); `QF 5` → `qf5` (compact);
* `4х185` → `4x185`; `ТА1–ТА9` → `ta1-ta9` (гомоглифы); `Pp/Рр/Pрасч` → `pp`,
  `Ip/Iр` → `ip`.

Канонизация консервативна: разные номиналы НЕ схлопываются (`400a` ≠ `200a`).

## Как evidence передаётся critic

`delta_explanation` (см.
[stage_comparison_pipeline_v2_delta_explanation.md](stage_comparison_pipeline_v2_delta_explanation.md))
при наличии evidence-карточки добавляет в prompt:

* секцию `GROUNDED VISION EVIDENCE` с grounded (как факт) и weak (как hint)
  записями (дизайнатор, old/new anchor, score);
* блок правил:

  ```text
  Используй grounded evidence как ПОДТВЕРЖДАЮЩИЙ слой.
  weak evidence трактуй как ПОДСКАЗКУ, требующую ручной проверки.
  НЕ считай ungrounded/rejected vision-выводы фактами.
  НЕ выдумывай изменений сверх переданной deterministic-дельты.
  ```

`conflict`/`rejected_only` уровень показывается ОДНОЙ строкой-предупреждением
(«НЕ использовать как evidence, это НЕ факт») — сами rejected-якоря в prompt как
факт НЕ всплывают. Без evidence-карточки prompt полностью идентичен прежнему
(backward-compat).

## Контракт отчёта `grounded_evidence_report.json`

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_grounded_evidence",
  "status": "ok|completed_with_warnings|skipped_no_grounding|failed",
  "summary": {
    "deltas_total": 0,
    "deltas_total_all_diff": 0,
    "deltas_with_grounded_evidence": 0,
    "deltas_with_weak_evidence": 0,
    "deltas_without_evidence": 0,
    "deltas_with_rejected_conflicts": 0,
    "evidence_links_total": 0,
    "grounded_links": 0, "weak_links": 0, "rejected_links": 0
  },
  "delta_evidence": [
    {
      "delta_id": "...", "entity_type": "power_supply", "delta_type": "changed",
      "old_value": "400А", "new_value": "200А",
      "left_block_id": "...", "right_block_id": "...",
      "left_page_number": 52, "right_page_number": 21,
      "evidence_level": "grounded",
      "use_in_critic": true,
      "evidence": [
        {"source": "graphic_vision_grounding", "fact_level": "confirmed",
         "status": "grounded", "kind": "designator_pair", "designator": "qf5",
         "old_anchor": "QF5 (400А)", "new_anchor": "QF5 (200А)",
         "left_block_id": "...", "right_block_id": "...",
         "left_page_number": 52, "right_page_number": 21,
         "match_score": 0.97, "reason": "..."}
      ],
      "warnings": []
    }
  ],
  "warnings": []
}
```

## Интеграция в dry-run

`run_pipeline_v2_dry_run` добавляет этап **[5b] grounded_evidence** после
entity-diff [5] и перед delta-explanation [6]:

```text
graphic_vision_grounding [3e] → entity_diff [5] → grounded_evidence [5b] → delta_explanation [6]
```

* включается, только если `gvg_report.items` непуст (нечего связывать иначе);
  отключается `options.grounded_evidence.enabled=false`;
* fail-soft: падение слоя не валит pipeline (`grounded_evidence.error` в summary);
* пишет `grounded_evidence_report.json`, добавляет в манифест;
* summary получает секцию `grounded_evidence` со счётчиками;
* delta-explanation получает `grounded_evidence_report` и подмешивает карточки.

## UI payload

`build_pipeline_v2_ui_payload` добавляет (только если слой включён):

```json
"grounded_evidence": {
  "available": true,
  "deltas_with_grounded_evidence": 0,
  "deltas_with_weak_evidence": 0,
  "deltas_without_evidence": 0,
  "deltas_with_rejected_conflicts": 0
}
```

Frontend в этой задаче НЕ менялся.

## Безопасность

* по умолчанию слой работает только в dry-run при наличии grounding; без него —
  `skipped_no_grounding`;
* fail-soft на уровне дельты (одна дельта не валит слой) и на уровне слоя (не
  валит pipeline);
* LM Studio / Qwen / Opus не задействованы (это пост-обработка готовых
  артефактов); реальный LLM-runner не вызывается — он инъектируется в
  delta-explanation отдельно;
* rejected/ungrounded никогда не факт — это ключевой инвариант.

## Smoke / диагностика

Read-only smoke на готовых runtime-артефактах (без Qwen/Opus/сети):

```bash
python - <<'PY'
import json
from backend.app.services.stage_comparison.pipeline_v2_grounded_evidence import (
    build_grounded_evidence_report)
base="comparison/sessions/<sid>/pairs/<pid>/pipeline_v2/"
rep=build_grounded_evidence_report(
    json.load(open(base+"entity_diff_report.json")),
    json.load(open(base+"graphic_vision_grounding_report.json")),
    visual_gate_report=json.load(open(base+"visual_equivalence_gate_report.json")),
    block_link_report=json.load(open(base+"block_link_preview_report.json")))
print(json.dumps(rep["summary"], ensure_ascii=False, indent=2))
PY
```

## Тесты

[tests/test_stage_comparison_pipeline_v2_grounded_evidence.py](../tests/test_stage_comparison_pipeline_v2_grounded_evidence.py)
— 18 spec-кейсов: exact grounded change, grounded entity pair, weak, ungrounded
не usable, rejected designator-range/noop не факт, QF5 400→200 grounded,
нормализация `400 А`/`ТА1-ТА9`/`4х185`, out-of-scope, skipped_no_grounding,
dry-run пишет отчёт + fail-soft, prompt включает/помечает/исключает evidence,
ui-payload summary, backward-compat.

## Связанные файлы

- [pipeline_v2_grounded_evidence.py](../backend/app/services/stage_comparison/pipeline_v2_grounded_evidence.py)
- [pipeline_v2_graphic_vision_grounding.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_grounding.py) — источник grounded/weak/rejected + `normalize_engineering_token`
- [pipeline_v2_delta_explanation.py](../backend/app/services/stage_comparison/pipeline_v2_delta_explanation.py) — потребитель evidence (prompt)
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — этап [5b]
- [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — summary
