#!/usr/bin/env python3
"""G2 research: укладка результата Mode 2 в ОБЩИЙ GraphicChangeLedger G1 (§42).

Верхний контракт не меняется: тот же конверт, тот же способ адресации (block_id +
page_index + bbox_visual_pt), те же confidence/provenance. Отличается только
природа улики: у Mode 1 это локальная область растра/вектора, у Mode 2 —
соответствие узлов и рёбер графа.

Предлагаемое расширение production-контракта (сейчас НЕ применено):
    CHANGE_TYPES ∪ MODE2_CHANGE_TYPES
    mode ∈ {None, "MODE_1", "MODE_2"}
    change.structural  — необязательный блок с уровнем и предметом изменения
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from backend.app.services.stage_comparison.graphic_comparison.contract import (  # noqa: E402
    CHANGE_TYPES, SCHEMA_VERSION, stable_id, validate_ledger,
)
from g2_comparator import CHANGE_TYPES as MODE2_CHANGE_TYPES  # noqa: E402


def build_mode2_ledger(left_graph, right_graph, comparison, *, policy=None) -> dict:
    def scope(g):
        return [{
            "block_id": g["block"]["block_id"],
            "page_index": g["block"]["page_index"],
            "block_type": "image",
            "bbox_visual_pt": g["block"].get("bbox_visual_pt"),
            "source": {"artifact": "blocks.json",
                       "coordinate_space": "normalized_page_top_left_visual"},
        }]

    changes = []
    for c in comparison["changes"]:
        changes.append({
            "change_id": stable_id("m2_", c["type"], c["subject"], c["summary"]),
            "type": c["type"],
            "left_region": c.get("left_region"),
            "right_region": c.get("right_region"),
            "evidence": [{"kind": "graph", "detail": e} for e in c["evidence"]],
            "address_hints": [c["subject"], f"level_{c['level']}"],
            "confidence": c["confidence"],
            "provenance": c.get("provenance") or ["VECTOR"],
            "structural": {
                "level": c["level"],
                "subject": c["subject"],
                "summary": c["summary"],
                "note": c.get("note"),
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "comparison_scope": {"left_blocks": scope(left_graph),
                             "right_blocks": scope(right_graph)},
        "route": "MODE_2_REQUIRED",
        "mode": "MODE_2",
        "policy": policy or {"extractor_version": "g2-research-0",
                             "profile": left_graph["profile"]["id"]},
        "quality": comparison["quality"],
        "changes": changes,
        "diagnostics": {
            "backbone_verdict": comparison["backbone_verdict"],
            "levels": comparison["levels"],
            "left_warnings": left_graph.get("warnings"),
            "right_warnings": right_graph.get("warnings"),
        },
    }


def validate_with_extension(ledger) -> tuple[bool, str]:
    """Проверка тем же валидатором G1, но со словарём, расширенным типами Mode 2."""
    import backend.app.services.stage_comparison.graphic_comparison.contract as contract
    original_types = set(contract.CHANGE_TYPES)
    original_validate_modes = None
    try:
        contract.CHANGE_TYPES |= MODE2_CHANGE_TYPES
        payload = json.loads(json.dumps(ledger))
        payload["mode"] = None      # действующий валидатор знает только MODE_1/None
        payload["route"] = "MODE_2_REQUIRED"
        validate_ledger(payload)
        return True, "прошёл валидатор G1 при расширении CHANGE_TYPES и mode"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        contract.CHANGE_TYPES.clear()
        contract.CHANGE_TYPES |= original_types


if __name__ == "__main__":
    A = Path(__file__).resolve().parents[1] / "artifacts"
    lg = json.loads((A / "grsh_left_graph.json").read_text(encoding="utf-8"))
    rg = json.loads((A / "grsh_right_graph.json").read_text(encoding="utf-8"))
    cmp_ = json.loads((A / "grsh_comparison.json").read_text(encoding="utf-8"))
    led = build_mode2_ledger(lg, rg, cmp_)
    (A / "grsh_mode2_ledger.json").write_text(
        json.dumps(led, ensure_ascii=False, indent=2), encoding="utf-8")
    ok, msg = validate_with_extension(led)
    print("изменений в ledger:", len(led["changes"]))
    print("валидация:", ok, "—", msg)
    print("НЕИЗВЕСТНЫЕ действующему контракту типы:",
          sorted({c["type"] for c in led["changes"]} - CHANGE_TYPES))
