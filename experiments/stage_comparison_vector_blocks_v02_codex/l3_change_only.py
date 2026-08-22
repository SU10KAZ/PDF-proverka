"""Compact change-only payload for a narrowly prompted Vision check."""
from __future__ import annotations

import json
import math
from typing import Any


def build_l3_change_only(comparison: dict[str, Any], routing: dict[str, Any]) -> dict[str, Any]:
    evidence = comparison["evidence"]
    uncertainties = []
    if routing["failed_gates"]:
        uncertainties.extend(routing["failed_gates"])
    if comparison["text"].get("truncated"):
        uncertainties.append("text_diff_truncated")
    if comparison["style"].get("changes_truncated"):
        uncertainties.append("style_diff_truncated")
    if comparison["repeated_patterns"].get("truncated"):
        uncertainties.append("pattern_diff_truncated")
    return {
        "quality_route": routing,
        "vector_verdict": comparison["status"],
        "geometry_delta": evidence["geometry"],
        "topology_delta": evidence["topology"],
        "changed_values": evidence["text"]["changed_values"],
        "added_text": evidence["text"]["added"],
        "removed_text": evidence["text"]["removed"],
        "pattern_deltas": evidence["patterns"]["changes"],
        "style_deltas": {
            "changed_pairs": evidence["style"]["changed_pairs"],
            "field_change_counts": evidence["style"]["field_change_counts"],
            "changes": evidence["style"]["changes"],
        },
        "crop_diagnostics": evidence["crop"] if comparison["crop"]["mismatch"] else None,
        "uncertainties": sorted(set(uncertainties)),
    }


def payload_metrics(value: Any) -> dict[str, int]:
    compact = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "bytes": len(compact.encode("utf-8")),
        "estimated_tokens": math.ceil(len(compact) / 4),
    }


__all__ = ["build_l3_change_only", "payload_metrics"]
