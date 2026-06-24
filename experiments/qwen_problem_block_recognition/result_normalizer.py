from __future__ import annotations

import json
import re
from typing import Any

EMPTY_FACTS = (
    "labels", "materials", "numeric_parameters", "visible_text",
    "elevations", "dimensions", "equipment", "connections", "tables",
)


def _empty_facts() -> dict[str, list]:
    return {k: [] for k in EMPTY_FACTS}


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _parse(raw_text: str | None):
    if not raw_text:
        return {}, True, False
    try:
        obj = json.loads(raw_text)
        return obj if isinstance(obj, dict) else {}, True, False
    except Exception:
        facts = _empty_facts()
        for m in re.finditer(r'"raw_text"\s*:\s*"([^"]+)"', raw_text):
            facts["labels"].append({"raw_text": m.group(1)})
        for m in re.finditer(r'"value"\s*:\s*"([^"]+)"', raw_text):
            facts["numeric_parameters"].append({"value": m.group(1)})
        return {"_facts": facts}, False, True


def _facts_from_data(data: dict) -> dict[str, list]:
    if "_facts" in data:
        return data["_facts"]
    facts = _empty_facts()
    anchors = data.get("diff_anchors") if isinstance(data.get("diff_anchors"), dict) else None
    if anchors:
        facts["labels"] = _as_list(anchors.get("labels"))
        facts["numeric_parameters"] = _as_list(anchors.get("ratings")) + _as_list(anchors.get("numeric_parameters"))
        facts["connections"] = _as_list(anchors.get("connections"))
    for key in EMPTY_FACTS:
        if key in data:
            facts[key] = _as_list(data.get(key))
    return facts


def _evidence_coverage(facts: dict[str, list]) -> float:
    scored = []
    for key, items in facts.items():
        if key == "visible_text":
            continue
        for item in items:
            if isinstance(item, dict):
                scored.append(bool(item.get("evidence_snippet") or item.get("raw_text") or item.get("value") or item.get("text")))
    return 1.0 if not scored else sum(scored) / len(scored)


def normalize(*, block_id: str, method: str, prompt_variant: str, provider: str, model: str,
              parameters: dict, status: str, latency_sec: float, input_size_bytes: int,
              raw_text: str | None = None, finish_reason: str | None = None,
              usage: dict | None = None, error: str | None = None) -> dict:
    data, json_valid, salvaged = _parse(raw_text)
    facts = _facts_from_data(data) if status == "done" or salvaged else _empty_facts()
    fact_counts = {k: len(v) for k, v in facts.items()}
    total = sum(fact_counts.values())
    warnings = list(data.get("warnings") or []) if isinstance(data.get("warnings"), list) else []
    if finish_reason == "length" and not json_valid:
        warnings.append("truncated_output")
    return {
        "block_id": block_id,
        "method": method,
        "prompt_variant": prompt_variant,
        "provider": provider,
        "model": model,
        "parameters": parameters,
        "status": status,
        "latency_sec": latency_sec,
        "input_size_bytes": input_size_bytes,
        "json_valid": json_valid,
        "salvaged": salvaged,
        "finish_reason": finish_reason,
        "usage": usage or {},
        "facts": facts,
        "fact_counts": fact_counts,
        "total_facts": total,
        "evidence_coverage": _evidence_coverage(facts),
        "confidence": float(data.get("confidence", 0) or 0) if isinstance(data, dict) else 0.0,
        "usable_for_diff": bool(data.get("usable_for_diff", total > 0)) if status == "done" else False,
        "warnings": warnings,
        "error": error,
    }
