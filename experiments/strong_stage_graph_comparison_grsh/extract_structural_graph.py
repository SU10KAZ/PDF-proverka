#!/usr/bin/env python3
"""Research-only EOM structural graph extraction for the selected GRSh pair.

The script consumes the two existing upstream IMAGE blocks.  It does not find
blocks, edit Production, or compare text/table content.  Vector words are used
only as identifiers and topology anchors; the branch profiles below are the
human-adjudicated EOM identities for this one proof-of-concept pair.
"""
from __future__ import annotations

import json
import hashlib
import math
import re
from pathlib import Path
from typing import Any

import fitz


EXPERIMENT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = EXPERIMENT_DIR.parents[1]

CASES = {
    "left": {
        "stage_role": "P",
        "blocks_json": "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_1/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия/versions/v001/02_work/blocks.json",
        "block_id": "blk_039909ec039649a1b8209f059c95167b",
        "source_labels": ["Ввод 1 к ТП1", "Ввод 2 к ТП2"],
        "source_subclass": "UPSTREAM_TP_CONNECTION",
        "input_labels": ["QF1", "QF2"],
        "input_ratings": ["3200A", "3200A"],
        "section_labels": ["ГРЩ1 РП1", "ГРЩ1 РП2"],
        "section_device": {
            "label": "QF3",
            "subclass": "MOTORIZED_CIRCUIT_BREAKER",
            "rating": "2000A",
            "control": "АВР",
        },
        "metering": [
            "ТТ/ТА и анализатор качества на вводе 1",
            "ТТ/ТА и анализатор качества на вводе 2",
        ],
        "branches": {
            1: [
                ("vru1", "ВРУ1, ввод 1 — корпус 1,2", "ACTIVE"),
                ("vru2", "ВРУ2, ввод 1 — встроенные помещения корпуса 1,2", "ACTIVE"),
                ("vru3", "ВРУ3, ввод 1 — корпус 3", "ACTIVE"),
                ("vru4", "ВРУ4, ввод 1 — корпус 4", "ACTIVE"),
                ("vru_parking", "ВРУа, ввод 1 — подземная автостоянка", "ACTIVE"),
                ("vru_itp", "ВРУ-ИТП, ввод 1", "ACTIVE"),
                ("refrigeration_control", "ВРУ-ХЦ, ввод 1", "ACTIVE"),
                ("fire_pumps_control", "ВРУ-АПТ, ввод 1", "ACTIVE"),
                ("water_pumps_control", "ВРУ-НСТ, ввод 1", "ACTIVE"),
                ("cooler_1", "ДР1-ХМ1", "ACTIVE"),
                ("free_reserve_s1", "Резерв", "FREE_RESERVE"),
                ("chiller_1", "ХМ1", "ACTIVE"),
                ("outdoor_lighting", "ЩНО — наружное освещение", "ACTIVE"),
                ("dhw_tanks_feed_s1", "Резервные баки ГВС", "ACTIVE"),
                ("compensation_1", "АУКРМ №1", "ACTIVE"),
            ],
            2: [
                ("vru1", "ВРУ1, ввод 2 — корпус 1,2", "ACTIVE"),
                ("vru2", "ВРУ2, ввод 2 — встроенные помещения корпуса 1,2", "ACTIVE"),
                ("vru3", "ВРУ3, ввод 2 — корпус 3", "ACTIVE"),
                ("vru4", "ВРУ4, ввод 2 — корпус 4", "ACTIVE"),
                ("vru_parking", "ВРУа, ввод 2 — подземная автостоянка", "ACTIVE"),
                ("vru_itp", "ВРУ-ИТП, ввод 2", "ACTIVE"),
                ("refrigeration_control", "ВРУ-ХЦ, ввод 2", "ACTIVE"),
                ("fire_pumps_control", "ВРУ-АПТ, ввод 2", "ACTIVE"),
                ("water_pumps_control", "ВРУ-НСТ, ввод 2", "ACTIVE"),
                ("cooler_2", "ДР2-ХМ2", "ACTIVE"),
                ("free_reserve_s2", "Резерв", "FREE_RESERVE"),
                ("chiller_2", "ХМ2", "ACTIVE"),
                ("tp_aux_lighting", "ЩНО — собственные нужды ТП", "ACTIVE"),
                ("dhw_tanks_feed_s2", "Резервные баки ГВС", "ACTIVE"),
                ("compensation_2", "АУКРМ №2", "ACTIVE"),
            ],
        },
        "uncertainties": [
            "Две опечатки в обозначениях отходящих аппаратов второй секции не используются как identity; identity задаётся позицией ветви и функциональным якорем.",
            "ТП1/ТП2 показаны как внешние точки подключения, поэтому отсутствие символов трансформаторов не трактуется как отсутствие трансформаторов в системе.",
        ],
    },
    "right": {
        "stage_role": "RD",
        "blocks_json": "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_2/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1/versions/v001/02_work/blocks.json",
        "block_id": "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6",
        "source_labels": ["Т1", "Т2"],
        "source_subclass": "TRANSFORMER_EXPLICIT",
        "input_labels": ["QF1", "QF2"],
        "input_ratings": ["2500A", "2500A"],
        "section_labels": ["РП1", "РП2"],
        "section_device": {
            "label": "QS1",
            "subclass": "SECTION_SWITCH_DISCONNECTOR",
            "rating": "1600A",
            "control": "SA / секц.",
        },
        "metering": [
            "1ТТ1…1ТТ9, PW1, Wh1, мультиметр",
            "2ТТ1…2ТТ9, PW2, Wh2, мультиметр",
        ],
        "branches": {
            1: [
                ("cooler_1", "ДР1-ХМ1", "ACTIVE"),
                ("chiller_1", "ХМ1", "ACTIVE"),
                ("compensation_1", "АУКРМ-1", "ACTIVE"),
                ("vru4", "ВРУ4", "ACTIVE"),
                ("vru3", "ВРУ3", "ACTIVE"),
                ("vru1", "ВРУ1", "ACTIVE"),
                ("vru2", "ВРУ2", "ACTIVE"),
                ("vru_parking", "ВРУа", "ACTIVE"),
                ("vru_itp", "ВРУ-ИТП", "ACTIVE"),
                ("refrigeration_control", "ШУ-ХЦ", "ACTIVE"),
                ("fire_pumps_control", "ШУ-АПТ, резервный ввод", "ACTIVE"),
                ("water_pumps_control", "ШУ-ХП, рабочий ввод", "ACTIVE"),
                ("outdoor_lighting", "ЩНО", "ACTIVE"),
            ],
            2: [
                ("vru4", "ВРУ4", "ACTIVE"),
                ("vru3", "ВРУ3", "ACTIVE"),
                ("vru1", "ВРУ1; identity восстановлена по симметрии ветвей, подпись feeder повторяет ВРУ3", "ACTIVE_UNCERTAIN_LABEL"),
                ("vru2", "ВРУ2", "ACTIVE"),
                ("vru_parking", "ВРУа", "ACTIVE"),
                ("vru_itp", "ВРУ-ИТП", "ACTIVE"),
                ("refrigeration_control", "ШУ-ХЦ", "ACTIVE"),
                ("fire_pumps_control", "ШУ-АПТ, рабочий ввод", "ACTIVE"),
                ("water_pumps_control", "ШУ-ХП, резервный ввод", "ACTIVE"),
                ("compensation_2", "АУКРМ-2", "ACTIVE"),
                ("chiller_2", "ХМ2", "ACTIVE"),
                ("cooler_2", "ДР2-ХМ2", "ACTIVE"),
                ("dhw_tanks_feed_s2", "Резервные баки ГВС / ЭБ-ГВС", "ACTIVE"),
                ("tp_aux_or_dhw_secondary", "ЯСН ТП по terminal-якорю; ЭБ-ГВС по feeder-якорю", "ACTIVE_UNCERTAIN_IDENTITY"),
            ],
        },
        "uncertainties": [
            "У ветви 2QF3 feeder-якорь повторяет ВРУ3; сопоставление с ВРУ1 основано на симметрии двухсекционной схемы и помечено как uncertain.",
            "У 2QF14 terminal-якорь ЯСН ТП конфликтует с feeder-якорем ЭБ-ГВС; точная identity не утверждается.",
            "Обозначение QS1 надёжно, но одного листа недостаточно, чтобы доказать полный алгоритм его автоматического управления.",
        ],
    },
}


def _point_in_polygon(point: tuple[float, float], polygon: list[list[float]]) -> bool:
    x, y = point
    inside = False
    x1, y1 = polygon[-1]
    for x2, y2 in polygon:
        if (y1 > y) != (y2 > y):
            cross = (x2 - x1) * (y - y1) / (y2 - y1) + x1
            if x < cross:
                inside = not inside
        x1, y1 = x2, y2
    return inside


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _visual_words(page: fitz.Page, block: dict[str, Any]) -> list[dict[str, Any]]:
    matrix = page.rotation_matrix
    polygon = block.get("polygon_points") or []
    x0, y0, x1, y1 = block["coords_norm"]
    rows = []
    for word in page.get_text("words"):
        rect = fitz.Rect(word[:4]) * matrix
        bbox = [
            rect.x0 / page.rect.width,
            rect.y0 / page.rect.height,
            rect.x1 / page.rect.width,
            rect.y1 / page.rect.height,
        ]
        center = ((bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2)
        in_bbox = x0 <= center[0] <= x1 and y0 <= center[1] <= y1
        if in_bbox and (not polygon or _point_in_polygon(center, polygon)):
            rows.append({
                "text": str(word[4]),
                "bbox_page_norm": [round(value, 6) for value in bbox],
                "center_page_norm": [round(center[0], 6), round(center[1], 6)],
            })
    return rows


def _outgoing_tokens(words: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [
        row for row in words
        if re.fullmatch(r"[12]QF\d+", row["text"], re.IGNORECASE)
        and row["center_page_norm"][1] < 0.62
    ]
    return sorted(candidates, key=lambda row: row["center_page_norm"][0])


def _nearest_anchor(words: list[dict[str, Any]], pattern: str, *, y_min: float = 0.0) -> dict[str, Any] | None:
    regex = re.compile(pattern, re.IGNORECASE)
    matches = [row for row in words if row["center_page_norm"][1] >= y_min and regex.fullmatch(row["text"])]
    if not matches:
        return None
    return max(matches, key=lambda row: row["center_page_norm"][1])


def _append_relation(relations: list[dict[str, Any]], source: str, target: str, relation: str, evidence: str, confidence: float = 1.0) -> None:
    relations.append({
        "id": f"edge_{len(relations) + 1:04d}",
        "source": source,
        "target": target,
        "relation": relation,
        "confidence": confidence,
        "evidence": evidence,
    })


def build_description(side: str) -> dict[str, Any]:
    profile = CASES[side]
    blocks_path = REPOSITORY_ROOT / profile["blocks_json"]
    blocks = json.loads(blocks_path.read_text(encoding="utf-8"))
    matches = [row for row in blocks["blocks"] if row["block_id"] == profile["block_id"]]
    if len(matches) != 1:
        raise ValueError(f"{side}: block resolved {len(matches)} times")
    block = matches[0]
    pdf_path = blocks_path.with_name("document.pdf")
    document = fitz.open(pdf_path)
    try:
        page = document[int(block["page_index"])]
        words = _visual_words(page, block)
        outgoing = _outgoing_tokens(words)
        drawings_total = len(page.get_drawings())
        page_rect = list(page.rect)
        page_rotation = int(page.rotation)
    finally:
        document.close()

    branch_counts = [len(profile["branches"][1]), len(profile["branches"][2])]
    if len(outgoing) != sum(branch_counts):
        raise ValueError(f"{side}: expected {sum(branch_counts)} outgoing device positions, got {len(outgoing)}")

    split = branch_counts[0]
    device_rows = {1: outgoing[:split], 2: outgoing[split:]}
    nodes: list[dict[str, Any]] = []
    relations: list[dict[str, Any]] = []

    for section in (1, 2):
        nodes.append({
            "id": f"source_{section}",
            "class": "SOURCE",
            "subclass": profile["source_subclass"],
            "label": profile["source_labels"][section - 1],
            "roles": ["SOURCE"],
            "section": section,
            "representation_detail": "EXPLICIT_TRANSFORMER" if side == "right" else "EXTERNAL_TP_CONNECTION",
            "evidence": "vector label + incoming trunk confirmed on raster",
        })
        nodes.append({
            "id": f"input_{section}",
            "class": "INPUT_DEVICE",
            "subclass": "CIRCUIT_BREAKER",
            "label": profile["input_labels"][section - 1],
            "rating_anchor": profile["input_ratings"][section - 1],
            "section": section,
            "evidence": "device tag on incoming vertical path",
        })
        nodes.append({
            "id": f"bus_{section}",
            "class": "BUS_SECTION",
            "label": profile["section_labels"][section - 1],
            "section": section,
            "evidence": "long busbar with repeated outgoing branches and section label",
        })
        nodes.append({
            "id": f"metering_{section}",
            "class": "METERING_GROUP",
            "label": profile["metering"][section - 1],
            "section": section,
            "evidence": "TT/TA, PW and Wh anchors on the incoming path",
        })
        _append_relation(relations, f"source_{section}", f"input_{section}", "FEEDS", "continuous incoming trunk", 0.99)
        _append_relation(relations, f"input_{section}", f"bus_{section}", "FEEDS", "continuous incoming trunk to bus section", 0.99)
        _append_relation(relations, f"metering_{section}", f"input_{section}", "MEASURES", "metering symbols share the incoming path", 0.98)

    nodes.append({
        "id": "section_device",
        "class": "SECTION_DEVICE",
        **profile["section_device"],
        "evidence_anchor": _nearest_anchor(words, re.escape(profile["section_device"]["label"]), y_min=0.62),
        "evidence": "tag on the only cross-link between the two incoming/section paths",
    })
    _append_relation(relations, "section_device", "bus_1", "TIES_SECTIONS", "cross-link geometry", 0.99)
    _append_relation(relations, "section_device", "bus_2", "TIES_SECTIONS", "cross-link geometry", 0.99)

    nodes.append({
        "id": "grounding",
        "class": "GROUNDING",
        "label": "N/PE/PEN and equipotential bonding group",
        "evidence": "N/PE/PEN rails and grounding/equipotential symbols",
    })
    _append_relation(relations, "grounding", "bus_1", "CONNECTED_TO", "grounding rails", 0.98)
    _append_relation(relations, "grounding", "bus_2", "CONNECTED_TO", "grounding rails", 0.98)

    branch_rows = []
    for section in (1, 2):
        for slot, ((identity, label, status), token) in enumerate(zip(profile["branches"][section], device_rows[section]), 1):
            device_id = f"outdev_s{section}_{slot:02d}"
            feeder_id = f"feeder_s{section}_{slot:02d}"
            terminal_id = f"terminal_s{section}_{slot:02d}"
            terminal_class = "UNKNOWN_FUNCTIONAL_NODE" if "UNCERTAIN" in status else (
                "COMPENSATION_GROUP" if identity.startswith("compensation_") else "LOAD_OR_TERMINAL"
            )
            nodes.extend([
                {
                    "id": device_id,
                    "class": "OUTGOING_DEVICE",
                    "label": token["text"],
                    "section": section,
                    "slot": slot,
                    "bbox_page_norm": token["bbox_page_norm"],
                    "evidence": "outgoing QF tag on branch stem",
                },
                {
                    "id": feeder_id,
                    "class": "OUTGOING_FEEDER",
                    "label": f"section {section} / slot {slot}",
                    "section": section,
                    "slot": slot,
                    "functional_identity": identity,
                    "status": status,
                    "evidence": "same branch column as outgoing device and terminal anchor",
                },
                {
                    "id": terminal_id,
                    "class": terminal_class,
                    "label": label,
                    "section": section,
                    "functional_identity": identity,
                    "status": status,
                    "evidence": "functional text anchor used only for graph identity; human raster adjudication",
                },
            ])
            _append_relation(relations, "bus_%d" % section, device_id, "BELONGS_TO_SECTION", "branch stem meets busbar", 0.99)
            _append_relation(relations, device_id, feeder_id, "PROTECTS_OR_SWITCHES", "device lies on feeder stem", 0.99)
            _append_relation(relations, feeder_id, terminal_id, "TERMINATES_AT", "continuous branch column", 0.95 if "UNCERTAIN" in status else 0.99)
            branch_rows.append({
                "section": section,
                "slot": slot,
                "device_label_as_drawn": token["text"],
                "device_bbox_page_norm": token["bbox_page_norm"],
                "functional_identity": identity,
                "terminal_label": label,
                "status": status,
            })

    active = sum(row["status"] != "FREE_RESERVE" for row in branch_rows)
    reserves = sum(row["status"] == "FREE_RESERVE" for row in branch_rows)
    uncertain = sum("UNCERTAIN" in row["status"] for row in branch_rows)
    return {
        "schema_version": "system-graph-grsh-poc-v1",
        "research_only": True,
        "side": side.upper(),
        "stage_role": profile["stage_role"],
        "input": {
            "blocks_json": profile["blocks_json"],
            "block_id": profile["block_id"],
            "source_pdf": str(pdf_path.relative_to(REPOSITORY_ROOT)),
            "blocks_json_sha256": _sha256(blocks_path),
            "source_pdf_sha256": _sha256(pdf_path),
            "page_index": int(block["page_index"]),
            "coords_norm": block["coords_norm"],
            "polygon_points_norm": block.get("polygon_points"),
            "pairing_source": "explicit user-selected existing IMAGE block; no detector or sheet matcher",
        },
        "method": {
            "sequence": [
                "resolve existing upstream block",
                "extract vector words in upstream polygon",
                "find repeated outgoing QF positions",
                "apply research-only EOM single-line profile",
                "adjudicate central and ambiguous local regions on raster",
            ],
            "coordinate_use": "within-side connectivity only; never used for cross-version identity",
            "text_use": "identifier/anchor only; no text or table diff",
            "generic_graph_result": "recorded separately in overlay_comparison_diagnostic.json",
        },
        "extraction_evidence": {
            "page_rect": page_rect,
            "page_rotation": page_rotation,
            "page_drawings_total": drawings_total,
            "words_in_upstream_polygon": len(words),
            "outgoing_device_positions": len(outgoing),
            "outgoing_by_section": branch_counts,
            "outgoing_token_x_gap_median": round(
                sorted(
                    outgoing[index + 1]["center_page_norm"][0] - outgoing[index]["center_page_norm"][0]
                    for index in range(len(outgoing) - 1)
                )[max(0, (len(outgoing) - 2) // 2)],
                6,
            ),
        },
        "backbone": {
            "source_count": 2,
            "bus_section_count": 2,
            "input_device_count": 2,
            "section_tie_present": True,
            "section_device": profile["section_device"],
            "source_representation": profile["source_subclass"],
        },
        "outgoing_summary": {
            "device_count": len(branch_rows),
            "by_section": branch_counts,
            "active_or_functional": active,
            "free_reserve": reserves,
            "uncertain_identity": uncertain,
        },
        "branches": branch_rows,
        "nodes": nodes,
        "relations": relations,
        "uncertainties": profile["uncertainties"],
    }


def main() -> None:
    for side in ("left", "right"):
        payload = build_description(side)
        output = EXPERIMENT_DIR / f"{side}_structural_description.json"
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"{side}: {payload['outgoing_summary']['device_count']} outgoing devices -> {output.relative_to(REPOSITORY_ROOT)}")


if __name__ == "__main__":
    main()
