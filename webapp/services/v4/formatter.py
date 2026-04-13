"""
formatter.py — Stage 4 v4 pipeline.

Конвертирует candidates_v2 → 03_findings.json в формате, который ожидают
downstream этапы (findings_critic, norm_verify, optimization) и UI.

Ключевые поля каждого finding:
- id, severity, category, sheet, page, problem, description
- norm, norm_quote, norm_confidence (nullable — заполняются norm_verify)
- solution, risk (nullable — заполняются critic/downstream)
- related_block_ids — ID блоков для UI подсветки
- evidence[] — трассировка к image/text блокам
- quality — метаданные v4 pipeline (v4_candidate_id, v4_class и т.д.)
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def _human_field_name(field: str) -> str:
    """Преобразует техническое имя поля в человекочитаемое (родительный падеж для solution)."""
    from webapp.services.v4.generators import _FIELD_LABELS_DEFAULT
    label = _FIELD_LABELS_DEFAULT.get(field)
    if label:
        return label
    return field.replace("_", " ")


SEVERITY_MAP = {
    "CRITICAL": "КРИТИЧЕСКОЕ",
    "ECONOMIC": "ЭКОНОМИЧЕСКОЕ",
    "EXPLOITATION": "ЭКСПЛУАТАЦИОННОЕ",
    "RECOMMENDED": "РЕКОМЕНДАТЕЛЬНОЕ",
    "CHECK_ADJACENT": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

# class_id → category (совместимо с фронтендом)
CATEGORY_MAP = {
    1: "documentation",       # out_of_scope_or_wrong_discipline
    2: "documentation",       # identity / addressing
    3: "documentation",       # cross-view consistency
    4: "protection",          # requirement/selected-part conflict
    5: "documentation",       # nomenclature
    6: "calculation",         # calculations
    7: "calculation",         # cable sizing
    8: "protection",          # breaker/device sizing
    9: "coordination",        # interdiscipline coordination
    10: "cost",               # economic/cost
    11: "safety",             # safety/fire
}


def _generate_norm_solution(
    candidate: dict, class_id: int, severity: str, config: dict | None = None
) -> tuple[str | None, str | None, str | None]:
    """Сгенерировать norm, solution, risk на основе данных кандидата.

    Использует config.norms для дисциплинарных ссылок.
    Fallback на hardcoded EOM значения.
    """
    field = candidate.get("field", "")
    entity_key = candidate.get("entity_key", "")
    values = candidate.get("values", [])
    entity_label = entity_key.split(":")[-1] if ":" in entity_key else entity_key

    norms_cfg = (config or {}).get("norms", {})
    norm = None
    solution = None
    risk = None

    if class_id == 2:
        c2 = norms_cfg.get("class2", {})
        norm = c2.get("default_norm", "ГОСТ 21.613-2014, п. 4.1 — обозначения элементов на схемах должны быть уникальными")
        solution = f"Присвоить уникальные обозначения элементам {entity_label} на одном листе"
        risk = c2.get("default_risk", "Неоднозначность обозначений → ошибки при строительстве")

    elif class_id == 3:
        c3 = norms_cfg.get("class3", {})
        # Ищем специфичный конфиг по полю
        field_norm_key = None
        if "section" in field or "diameter" in field or "size" in field:
            field_norm_key = "section_fields"
        elif "count" in field:
            field_norm_key = "pe_count_fields"
        elif "panel" in field or "source" in field or "destination" in field:
            field_norm_key = "topology_fields"

        if field_norm_key and field_norm_key in c3:
            norm = c3[field_norm_key].get("norm")
            risk = c3[field_norm_key].get("risk")
        else:
            default_c3 = c3.get("default", {})
            norm = default_c3.get("norm")
            risk = default_c3.get("risk", "Несоответствие между чертежами → ошибки при монтаже")

        field_label = _human_field_name(field)
        val_list = [str(v.get("value_norm", "?")) for v in values[:5]]
        solution = f"Устранить расхождение {field_label} для {entity_label}: привести к единому значению ({' / '.join(val_list)})"

    elif class_id == 4:
        c4 = norms_cfg.get("class4", {})
        # Ищем по полю
        if field in c4:
            norm = c4[field].get("norm")
            risk = c4[field].get("risk")
        else:
            default_c4 = c4.get("default", {})
            norm = default_c4.get("norm")
            risk = default_c4.get("risk", "Несоответствие нормативному требованию")

        solution = f"Привести {entity_label} в соответствие с требованием примечания"

    return norm, solution, risk


def _normalize_page(pages: list[int]) -> Any:
    """Нормализация page: [] → None, [N] → N, [N,M] → [N,M]."""
    if not pages:
        return None
    if len(pages) == 1:
        return pages[0]
    return pages


def _normalize_sheet(sheets: list[Any]) -> Any:
    """Нормализация sheet: [] → None, [X] → 'X', [X,Y] → 'X, Y'."""
    cleaned = [str(s).strip() for s in sheets if s is not None and str(s).strip()]
    if not cleaned:
        return None
    return ", ".join(cleaned)


def format_candidates_to_findings(
    candidates: dict,
    project_id: str,
    blocks_analyzed: int = 0,
    config: dict | None = None,
) -> dict:
    """Конвертировать candidates_v2 в формат 03_findings.json.

    Args:
        candidates: содержимое candidates_v2.json (с полем all_candidates).
        project_id: project_id для meta.
        blocks_analyzed: сколько блоков обработано (для meta.blocks_analyzed).

    Returns:
        dict совместимый с 03_findings.json (meta + findings[]).
    """
    all_candidates = candidates.get("all_candidates", [])

    findings = []
    for i, c in enumerate(all_candidates, start=1):
        class_id = c.get("issue_class_id", 0)
        severity_raw = c.get("candidate_claim", {}).get("proposed_severity", "RECOMMENDED")
        severity = SEVERITY_MAP.get(severity_raw, "РЕКОМЕНДАТЕЛЬНОЕ")
        category = CATEGORY_MAP.get(class_id, "documentation")

        evidence = c.get("evidence", [])
        pages = sorted({e.get("page") for e in evidence if e.get("page")})
        sheets = sorted({e.get("sheet") for e in evidence if e.get("sheet")})

        # Block IDs (дедупликация с сохранением порядка)
        seen = set()
        related_block_ids = []
        for e in evidence:
            bid = e.get("block_id")
            if bid and bid not in seen:
                seen.add(bid)
                related_block_ids.append(bid)

        # Evidence entries в каноническом формате
        # Тип: "image" по умолчанию (v4 работает с image-блоками);
        # если view_type == "general_notes" или source_type == "text" → "text"
        evidence_out = []
        for e in evidence:
            view = (e.get("view_type") or "").lower()
            ev_type = "text" if "note" in view or "text" in view else "image"
            evidence_out.append({
                "type": ev_type,
                "block_id": e.get("block_id"),
                "page": e.get("page"),
            })

        summary = c.get("candidate_claim", {}).get("summary", "") or ""
        norm, solution, risk = _generate_norm_solution(c, class_id, severity, config=config)

        finding = {
            "id": f"F-{i:03d}",
            "severity": severity,
            "category": category,
            "sheet": _normalize_sheet(sheets),
            "page": _normalize_page(pages),
            "problem": summary[:300],
            "description": summary,
            "norm": norm,
            "norm_quote": None,
            "norm_confidence": 0.6 if norm else None,
            "solution": solution,
            "risk": risk,
            "related_block_ids": related_block_ids,
            "evidence": evidence_out,
            "quality": {
                "v4_candidate_id": c.get("candidate_id"),
                "v4_class": class_id,
                "v4_subtype": c.get("subtype"),
                "v4_entity_key": c.get("entity_key"),
                "v4_field": c.get("field"),
                "v4_matching_policy": c.get("matching_policy"),
                "v4_flags": c.get("flags", {}),
            },
        }
        findings.append(finding)

    # Агрегация by_severity
    by_severity: dict[str, int] = {}
    for f in findings:
        sev = f["severity"]
        by_severity[sev] = by_severity.get(sev, 0) + 1

    return {
        "meta": {
            "project_id": project_id,
            "audit_completed": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "audit_mode": "v4",
            "total_findings": len(findings),
            "blocks_analyzed": blocks_analyzed,
            "cross_discipline_blocks": 0,
            "source": "v4_pipeline",
            "by_severity": by_severity,
        },
        "findings": findings,
    }


def save_findings(findings_data: dict, output_dir: Path) -> Path:
    """Сохранить findings в output_dir/03_findings.json."""
    path = output_dir / "03_findings.json"
    path.write_text(
        json.dumps(findings_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path
