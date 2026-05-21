"""Builder for backend/app/data/discipline_checklists_metadata/<DISC>.json.

Reads `completeness_requirements_matrix.json` (the source of truth from
normative_checklist_research) and re-shapes it into one metadata file per
discipline, with the field set the P0 checklist safety layer requires:

    - item_id, item_name, discipline
    - normative_status, can_be_reported_as_missing
    - applicable_document_types, applicable_stages
    - applicability_conditions, object_signals
    - severity_policy, recommended_action
    - normative_basis, confidence
    - requires_cross_section, requires_human_validation
    - allow_in_shadow_only, disabled_by_default
    - source_research_reference

Run from repo root:

    python experiments/md_analysis_comparison/normative_checklist_research/matrix/build_metadata.py

Idempotent: writes exactly 8 JSON files plus README.md. No backend code is
imported. No runtime is touched.

This script is a research artifact — it generates static metadata, then the
backend reads the JSONs at rest. The mapping `ITEM_OBJECT_SIGNALS` and the
`disabled_by_default` rules below were curated against
`recommendations/prompt_rules_update.md` §5 and `final_report.md` §5.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]  # repo root
MATRIX = Path(__file__).resolve().parent / "completeness_requirements_matrix.json"
OUT_DIR = ROOT / "backend" / "app" / "data" / "discipline_checklists_metadata"

DISCIPLINES = ("AR", "EOM", "KJ", "KM", "MULTI", "OV", "SS", "VK")

# Per-item object-signal mapping. Empty list = no object_signal gate needed.
# Built from prompt_rules_update.md §5 + per-discipline reports.
# Items not listed default to [] (i.e. no gate).
ITEM_OBJECT_SIGNALS: dict[str, list[str]] = {
    # --- AR ---
    "AR-08": ["residential_building"],          # инсоляция — жилые МКД
    "AR-13": ["roof_operated"],                  # узлы кровли
    "AR-19": ["public_building", "residential_building"],
    "AR-21": ["residential_building", "public_building"],
    "AR-22": ["facade_present"],
    "AR-23": ["roof_operated"],

    # --- EOM ---
    "EOM-17": ["fire_system_present", "smoke_ventilation_required"],
    "EOM-20": ["motors_present"],
    "EOM-21": ["category_1_power"],
    "EOM-22": ["lightning_protection_required"],
    "EOM-23": ["category_1_power"],
    "EOM-24": ["wet_zone_present"],
    "EOM-25": ["elevators_present"],

    # --- KJ ---
    "KJ-23": ["high_rise"],
    "KJ-24": ["seismic_region"],
    "KJ-25": ["underground_structure"],

    # --- KM ---
    "KM-03": ["fire_system_present"],
    "KM-24": ["high_rise"],

    # --- OV ---
    "OV-04": ["high_rise", "smoke_ventilation_required"],
    "OV-11": ["ventilation_system_present"],
    "OV-13": ["fire_system_present", "ventilation_system_present"],
    "OV-18": ["fire_system_present"],
    "OV-23": ["ventilation_system_present"],
    "OV-24": ["automation_present"],
    "OV-25": ["residential_building"],  # disabled below — dup with VK

    # --- VK ---
    "VK-12": ["fire_system_present"],
    "VK-13": ["pumps_present"],
    "VK-14": ["pumps_present"],
    "VK-17": ["fire_system_present"],
    "VK-22": ["pumps_present"],
    "VK-23": ["pumps_present"],
    "VK-24": ["fire_system_present"],

    # --- SS ---
    "SS-02": ["fire_system_present"],
    "SS-03": ["fire_system_present"],
    "SS-05": ["fire_system_present"],
    "SS-06": ["fire_system_present"],
    "SS-07": ["fire_system_present"],
    "SS-13": ["fire_system_present"],
    "SS-17": ["fire_system_present", "smoke_ventilation_required"],
    "SS-18": ["elevators_present"],
    "SS-19": ["fire_system_present"],
    "SS-23": ["residential_building", "automation_present"],
    "SS-24": ["public_building"],
    "SS-25": ["underground_structure"],
}

# Items disabled-by-default for the future completeness_runner.
# Reasoning encoded so the future runner / human reviewer can audit.
ITEM_DISABLED_BY_DEFAULT_REASON: dict[str, str] = {
    "OV-25": "Дубль с VK (см. final_report.md §3). Подлежит удалению из чек-листа.",
    # MULTI cross-section items 05..13 — нельзя проверить на одном MD.
    "MULTI-05": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-06": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-07": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-08": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-09": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-10": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-11": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-12": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
    "MULTI-13": "Cross-section consistency: требует двух разделов одновременно (single-MD pipeline).",
}


def _normalize_stages(raw: list[str] | None) -> list[str]:
    """Normalize stage tokens to {project_documentation, working_documentation,
    detailing}.

    The matrix uses 'ПД', 'РД', 'КМД' (Russian acronyms). We canonicalize to
    English so downstream code (Python helpers) does not need to deal with
    Cyrillic identifiers, but we keep the originals available via the source
    reference for traceability.
    """
    if not raw:
        return []
    out: list[str] = []
    for token in raw:
        t = (token or "").strip().upper()
        if not t:
            continue
        if t in ("ПД", "PD", "PROJECT", "PROJECT_DOCUMENTATION"):
            out.append("project_documentation")
        elif t in ("РД", "RD", "WORKING", "WORKING_DOCUMENTATION"):
            out.append("working_documentation")
        elif t in ("КМД", "KMD", "DETAILING"):
            out.append("detailing")
        else:
            out.append(t.lower())
    # Dedup but preserve order.
    seen: set[str] = set()
    deduped: list[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def _requires_cross_section(item: dict[str, Any]) -> bool:
    section = (item.get("current_section") or "").lower()
    if "cross-section consistency" in section:
        return True
    if "coordination" in section:
        # Coordination is also a cross-discipline artifact (per final_report.md §5):
        # treated as cannot-report-missing, gated alongside cross-section.
        return True
    return False


def _requires_human_validation(item: dict[str, Any]) -> bool:
    issues = item.get("current_norm_issues") or []
    if issues:
        return True
    # Conditionally_mandatory items at medium confidence with a threshold ask
    # for engineer validation per final_report.md §"Checklist items requiring
    # human engineer validation".
    if (
        item.get("normative_status") == "conditionally_mandatory"
        and item.get("confidence") == "medium"
        and _requires_object_signal(item)
    ):
        return True
    return False


def _requires_object_signal(item: dict[str, Any]) -> bool:
    sigs = ITEM_OBJECT_SIGNALS.get(item["id"], [])
    return bool(sigs)


def _allow_in_shadow_only(item: dict[str, Any], requires_human: bool) -> bool:
    if requires_human:
        return True
    # Items with no concrete normative_basis but still mandatory -> shadow-only
    # until clause verified.
    if (
        item.get("normative_status") == "mandatory"
        and not (item.get("normative_basis") or "").strip()
    ):
        return True
    return False


def _disabled_by_default(item_id: str) -> tuple[bool, str | None]:
    if item_id in ITEM_DISABLED_BY_DEFAULT_REASON:
        return True, ITEM_DISABLED_BY_DEFAULT_REASON[item_id]
    return False, None


def _recommended_action(
    *,
    can_report: bool,
    status: str,
    cross_section: bool,
    requires_human: bool,
    has_signal: bool,
    disabled: bool,
) -> str:
    if disabled:
        return "context_only"
    if cross_section:
        return "context_only_cross_section"
    if not can_report:
        return "context_only"
    if requires_human:
        return "shadow_only_until_clause_verified"
    if status == "mandatory" and not has_signal:
        return "report_missing_if_absent"
    if status == "conditionally_mandatory" and has_signal:
        return "report_missing_only_if_signals_match"
    if status == "conditionally_mandatory" and not has_signal:
        return "report_missing_only_if_stage_matches"
    if status == "recommended":
        return "report_low_severity_or_drop"
    return "context_only"


def _severity_policy(item: dict[str, Any]) -> dict[str, str]:
    default = (item.get("recommended_severity") or "").strip() or "РЕКОМЕНДАТЕЛЬНОЕ"
    # On stage-mismatch or doc_type-mismatch always downgrade to the soft
    # "verify against adjacent" category (final_report.md §7).
    return {
        "default": default,
        "if_stage_unknown_or_mismatch": "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ",
        "if_doc_type_mismatch": "drop",
        "if_signal_missing": "drop",
    }


def _source_research_reference(item_id: str) -> str:
    return (
        "experiments/md_analysis_comparison/normative_checklist_research/"
        f"matrix/completeness_requirements_matrix.json#{item_id}"
    )


def transform_item(item: dict[str, Any]) -> dict[str, Any]:
    item_id = item["id"]
    status = (item.get("normative_status") or "recommended").strip()
    can_report = bool(item.get("can_be_reported_as_missing"))
    cross_section = _requires_cross_section(item)
    requires_human = _requires_human_validation(item)
    has_signal = _requires_object_signal(item)
    disabled, disabled_reason = _disabled_by_default(item_id)

    # Cross-section items are by definition not single-MD-reportable, override.
    if cross_section and can_report:
        can_report = False

    rec_action = _recommended_action(
        can_report=can_report,
        status=status,
        cross_section=cross_section,
        requires_human=requires_human,
        has_signal=has_signal,
        disabled=disabled,
    )

    out: dict[str, Any] = {
        "item_id": item_id,
        "item_name": item["item_name"],
        "discipline": item["discipline"],
        "normative_status": status,
        "can_be_reported_as_missing": can_report,
        "applicable_document_types": list(item.get("applicable_document_types") or []),
        "applicable_stages_raw": list(item.get("applicable_stages") or []),
        "applicable_stages": _normalize_stages(item.get("applicable_stages")),
        "applicability_conditions": item.get("applicability_conditions") or "",
        "object_signals": list(ITEM_OBJECT_SIGNALS.get(item_id, [])),
        "severity_policy": _severity_policy(item),
        "recommended_action": rec_action,
        "normative_basis": item.get("normative_basis") or "",
        "exact_clause_or_section": item.get("exact_clause_or_section") or "",
        "confidence": item.get("confidence") or "medium",
        "requires_cross_section": cross_section,
        "requires_human_validation": requires_human,
        "allow_in_shadow_only": _allow_in_shadow_only(item, requires_human),
        "disabled_by_default": disabled,
        "disabled_reason": disabled_reason,
        "source_research_reference": _source_research_reference(item_id),
        "current_severity": item.get("current_severity") or "",
        "current_problem_class": item.get("current_problem_class") or "",
        "current_norm_reference": item.get("current_norm_reference") or "",
        "current_norm_issues": list(item.get("current_norm_issues") or []),
        "do_not_report_if": item.get("do_not_report_if") or "",
        "example_valid_missing_case": item.get("example_valid_missing_case") or "",
        "example_invalid_missing_case": item.get("example_invalid_missing_case") or "",
    }
    return out


def build() -> dict[str, dict[str, Any]]:
    raw_items = json.loads(MATRIX.read_text(encoding="utf-8"))
    per_discipline: dict[str, list[dict[str, Any]]] = {d: [] for d in DISCIPLINES}
    for item in raw_items:
        disc = item["discipline"]
        if disc not in per_discipline:
            raise SystemExit(f"unknown discipline in matrix: {disc!r}")
        per_discipline[disc].append(transform_item(item))

    bundles: dict[str, dict[str, Any]] = {}
    for disc, items in per_discipline.items():
        bundles[disc] = {
            "schema_version": 1,
            "discipline": disc,
            "source_research_dir": (
                "experiments/md_analysis_comparison/normative_checklist_research/"
            ),
            "source_matrix": (
                "experiments/md_analysis_comparison/normative_checklist_research/"
                "matrix/completeness_requirements_matrix.json"
            ),
            "counts": {
                "total": len(items),
                "mandatory": sum(
                    1 for it in items if it["normative_status"] == "mandatory"
                ),
                "conditionally_mandatory": sum(
                    1
                    for it in items
                    if it["normative_status"] == "conditionally_mandatory"
                ),
                "recommended": sum(
                    1 for it in items if it["normative_status"] == "recommended"
                ),
                "cannot_be_reported_as_missing": sum(
                    1 for it in items if not it["can_be_reported_as_missing"]
                ),
                "requires_cross_section": sum(
                    1 for it in items if it["requires_cross_section"]
                ),
                "requires_human_validation": sum(
                    1 for it in items if it["requires_human_validation"]
                ),
                "with_object_signals": sum(1 for it in items if it["object_signals"]),
                "disabled_by_default": sum(
                    1 for it in items if it["disabled_by_default"]
                ),
            },
            "items": items,
        }
    return bundles


README_TEMPLATE = """# discipline_checklists_metadata

**Дата сборки:** автоматически (см. `_generated_at`).
**Источник:** `experiments/md_analysis_comparison/normative_checklist_research/`.

Этот каталог — **metadata layer** для проверки чек-листов
`backend/app/data/discipline_checklists/`. Каждый JSON содержит per-item
нормативные атрибуты, gates и safety-флаги, выведенные из
`normative_checklist_research/final_report.md` + `matrix/...`.

## Назначение

Metadata НЕ исполняется runtime сейчас. Это **prepared safety layer** для
будущего `completeness_runner`. Backend читает эти файлы как plain data,
никакие LLM/pipeline/Stage-01 не затронуты этим каталогом.

## Структура

```
discipline_checklists_metadata/
├── AR.json    — 23 items
├── EOM.json   — 25 items
├── KJ.json    — 25 items
├── KM.json    — 25 items
├── MULTI.json — 22 items
├── OV.json    — 25 items
├── SS.json    — 25 items
├── VK.json    — 25 items
└── README.md
```

Всего: **195 items** (соответствует matrix).

## Поля item-а

| Поле | Тип | Описание |
|---|---|---|
| `item_id` | str | `<DISC>-NN` — ключ matrix |
| `item_name` | str | человеко-читаемое имя |
| `discipline` | str | одна из 8 |
| `normative_status` | enum | `mandatory` / `conditionally_mandatory` / `recommended` / `optional` / `not_applicable` |
| `can_be_reported_as_missing` | bool | **главный safety-флаг** для completeness_runner |
| `applicable_document_types` | list[str] | подмножество {`full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`} |
| `applicable_stages` | list[str] | подмножество {`project_documentation`, `working_documentation`, `detailing`} |
| `applicable_stages_raw` | list[str] | оригинал из matrix (ПД/РД/КМД) |
| `applicability_conditions` | str | свободный текст условия |
| `object_signals` | list[str] | required signals для условного item; пустой = no gate |
| `severity_policy` | dict | `default`, `if_stage_unknown_or_mismatch`, `if_doc_type_mismatch`, `if_signal_missing` |
| `recommended_action` | enum | как runner должен поступать |
| `normative_basis` | str | СП/ГОСТ/ПП РФ ссылка |
| `exact_clause_or_section` | str | точный пункт (если установлен) |
| `confidence` | enum | `high` / `medium` / `low` |
| `requires_cross_section` | bool | true → cannot report в single-MD pipeline |
| `requires_human_validation` | bool | true → shadow-only до подтверждения пункта |
| `allow_in_shadow_only` | bool | разрешено логировать в shadow-mode |
| `disabled_by_default` | bool | item полностью выключен (см. `disabled_reason`) |
| `disabled_reason` | str\\|null | причина disabled-by-default |
| `source_research_reference` | str | путь к исходной записи matrix |
| `current_severity` | str | severity в текущем checklist.md (для трассировки) |
| `current_problem_class` | str | problem_class в текущем checklist.md |
| `current_norm_reference` | str | нормативная ссылка в текущем checklist.md |
| `current_norm_issues` | list[str] | известные проблемы текущей ссылки |
| `do_not_report_if` | str | свободный текст условий отказа от finding |
| `example_valid_missing_case` | str | пример допустимого missing |
| `example_invalid_missing_case` | str | пример недопустимого missing |

## Контракт значений

- `normative_status` ∈ {`mandatory`, `conditionally_mandatory`, `recommended`,
  `optional`, `not_applicable`}.
- `applicable_document_types` ⊆ {`full_rd`, `audit_comparison`,
  `tz_vs_rd`, `specification_only`}.
- `applicable_stages` ⊆ {`project_documentation`, `working_documentation`,
  `detailing`}.
- `object_signals` ⊆ allow-list из `object_signals.py` (см. там).
- Если `requires_cross_section=true` → `can_be_reported_as_missing=false`
  (force-enforced генератором).

## Как генерируется

Из `experiments/md_analysis_comparison/normative_checklist_research/matrix/completeness_requirements_matrix.json`
скриптом `experiments/md_analysis_comparison/normative_checklist_research/matrix/build_metadata.py`.
Скрипт идемпотентный — повторный запуск перезаписывает JSON-ы.

## Как НЕ использовать

- Не импортировать в runtime до того, как `completeness_runner` будет создан.
- Не модифицировать руками — генерировать через build_metadata.py.
- Не считать `disabled_by_default=true` items валидными missing-findings.
- Не считать items с `requires_cross_section=true` валидными missing-findings
  в single-MD pipeline.
"""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    bundles = build()
    written: list[str] = []
    for disc in DISCIPLINES:
        path = OUT_DIR / f"{disc}.json"
        path.write_text(
            json.dumps(bundles[disc], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        written.append(path.name)
    (OUT_DIR / "README.md").write_text(README_TEMPLATE, encoding="utf-8")
    print(f"Wrote {len(written)} JSON files + README.md to {OUT_DIR}")
    # Top-line stats per discipline.
    for disc, bundle in bundles.items():
        c = bundle["counts"]
        print(
            f"  {disc}: total={c['total']:>3}  "
            f"mand={c['mandatory']:>3}  "
            f"cond={c['conditionally_mandatory']:>3}  "
            f"rec={c['recommended']:>3}  "
            f"cannot_report={c['cannot_be_reported_as_missing']:>3}  "
            f"cross_section={c['requires_cross_section']:>3}  "
            f"human_validation={c['requires_human_validation']:>3}  "
            f"with_signals={c['with_object_signals']:>3}  "
            f"disabled={c['disabled_by_default']:>3}"
        )


if __name__ == "__main__":
    main()
