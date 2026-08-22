"""Fail-safe deterministic routing gates for Vector / Hybrid / Vision."""
from __future__ import annotations

from typing import Any


ROUTES = {"VECTOR_OK", "VECTOR_WITH_VISION", "VISION_ONLY"}


def route_comparison(left: dict[str, Any], right: dict[str, Any], comparison: dict[str, Any]) -> dict[str, Any]:
    geometry = comparison["geometry"]
    primitive_runs = geometry.get("primitive_matching_experiment") or []
    coverage_runs = geometry.get("tolerance_experiment") or []
    compare_capped = any(run.get("capped") for run in primitive_runs + coverage_runs)
    left_caps, right_caps = left["cap_flags"], right["cap_flags"]
    caps_ok = not compare_capped and not any(left_caps.values()) and not any(right_caps.values())
    sufficient = (
        left["vector_quality"] != "VECTOR_DATA_INSUFFICIENT"
        and right["vector_quality"] != "VECTOR_DATA_INSUFFICIENT"
        and left["topology"]["segments_total"] >= 3
        and right["topology"]["segments_total"] >= 3
    )
    geometry_ok = sufficient and not compare_capped
    text_ok = left["text_quality"]["status"] == right["text_quality"]["status"] == "TEXT_GOOD"
    topology_ok = sufficient and not left_caps["topology_capped"] and not right_caps["topology_capped"]
    crop_ok = not comparison["crop"]["mismatch"]
    style_ok = bool(comparison["style"]["reliable"])
    gates = {
        "geometry_ok": geometry_ok,
        "text_ok": text_ok,
        "topology_ok": topology_ok,
        "crop_ok": crop_ok,
        "caps_ok": caps_ok,
        "style_ok": style_ok,
    }
    reasons = [name for name, passed in gates.items() if not passed]
    if not sufficient:
        route = "VISION_ONLY"
    elif comparison["crop"]["mismatch"] and max(geometry["left_coverage"], geometry["right_coverage"]) < 0.5:
        route = "VISION_ONLY"
    elif all(gates.values()):
        route = "VECTOR_OK"
    else:
        route = "VECTOR_WITH_VISION"
    assert route in ROUTES
    return {
        "schema_version": "vector-routing-v0.2-codex",
        "route": route,
        "gates": gates,
        "failed_gates": reasons,
        "fail_safe_policy": "Any uncertainty routes to Vision; insufficient vector evidence skips Vector.",
    }


__all__ = ["route_comparison", "ROUTES"]
