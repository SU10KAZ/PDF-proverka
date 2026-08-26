#!/usr/bin/env python3
"""Rebuild the G2.4.4.3 GRAPHIC artifacts on the proven chronological sides.

This runner is orchestration only.  It resolves the two immutable prepared
blocks from the comparison session and delegates extraction, graph building,
comparison, validation and ledger adaptation to production functions.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.dense_sectioned_board import (  # noqa: E402
    PROFILE_ID,
    PROFILE_VERSION,
    build_dense_sectioned_board_graph,
    detect_dense_sectioned_board,
    evaluate_dense_sectioned_board_gate,
)
from backend.app.pipeline.stages.block_grounding.graph_identity_matcher import (  # noqa: E402
    MATCHER_VERSION,
)
from backend.app.pipeline.stages.block_grounding.system_graph import (  # noqa: E402
    SCHEMA_VERSION as SYSTEM_GRAPH_SCHEMA_VERSION,
    validate_system_graph,
)
from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (  # noqa: E402
    COMPARATOR_VERSION,
    COMPARISON_SCHEMA_VERSION,
    DEFAULT_COMPARISON_POLICY,
    compare_system_graphs,
    validate_comparison_result,
)
from backend.app.pipeline.stages.block_grounding.vector_evidence import (  # noqa: E402
    EXTRACTION_VERSION,
    extract_vector_evidence,
)
from backend.app.services.common.blocks_json import load_blocks_json  # noqa: E402
from backend.app.services.stage_comparison.graphic_comparison.confidence_policy import (  # noqa: E402
    MODE2_CONFIDENCE_POLICY_V1,
)
from backend.app.services.stage_comparison.graphic_comparison.contract import (  # noqa: E402
    MODE2_SCHEMA_VERSION,
    validate_ledger,
)
from backend.app.services.stage_comparison.graphic_comparison.graphic_change_ledger_adapter import (  # noqa: E402
    ADAPTER_ID,
    ADAPTER_VERSION,
    adapt_system_graph_comparison_to_ledger,
)


RUNNER_VERSION = "g2.4.4.3-correct-sides-runner-v1"
SESSION_ID = "7cccec69bb0b4327"
SESSION_PATH = ROOT / "comparison" / "sessions" / SESSION_ID / "session.json"
OUTPUT_DIR = Path(__file__).resolve().parent
SIDES = {
    "LEFT": {
        "stage": "stage_1",
        "chronology": "earlier",
        "block_id": "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6",
    },
    "RIGHT": {
        "stage": "stage_2",
        "chronology": "later",
        "block_id": "blk_039909ec039649a1b8209f059c95167b",
    },
}
ARTIFACT_FILENAMES = {
    "left_graph": "left_system_graph.json",
    "right_graph": "right_system_graph.json",
    "comparison": "comparison_result.json",
    "ledger": "graphic_change_ledger.json",
}


class RunFailure(RuntimeError):
    """The requested rebuild cannot continue without violating fail-closed rules."""


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RunFailure(f"json_read_failed:{path}:{type(error).__name__}") from error


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode(
        "utf-8"
    )


def _canonical_digest(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise RunFailure(f"sha256_read_failed:{path}:{type(error).__name__}") from error
    return digest.hexdigest()


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _validate_coords(record: dict[str, Any], side: str) -> None:
    coords = record.get("coords_norm")
    if not isinstance(coords, list) or len(coords) != 4:
        raise RunFailure(f"{side}:coords_norm_invalid")
    if any(
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= 1.0
        for value in coords
    ):
        raise RunFailure(f"{side}:coords_norm_invalid")
    if not (float(coords[2]) > float(coords[0]) and float(coords[3]) > float(coords[1])):
        raise RunFailure(f"{side}:coords_norm_empty")

    polygon = record.get("polygon_points")
    if polygon is not None:
        if not isinstance(polygon, list) or len(polygon) < 3:
            raise RunFailure(f"{side}:polygon_points_invalid")
        for point in polygon:
            if (
                not isinstance(point, list)
                or len(point) < 2
                or any(
                    not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(float(value))
                    or not 0.0 <= float(value) <= 1.0
                    for value in point[:2]
                )
            ):
                raise RunFailure(f"{side}:polygon_points_invalid")


def _document_matches(session: dict[str, Any], stage: str, block_id: str) -> list[dict]:
    matches = []
    documents = (session.get("documents") or {}).get(stage)
    if not isinstance(documents, list):
        raise RunFailure(f"session_documents_invalid:{stage}")
    for document in documents:
        if not isinstance(document, dict):
            continue
        pdf_path = Path(str(document.get("pdf_path") or ""))
        blocks_path = pdf_path.parent / "blocks.json"
        if not blocks_path.is_file():
            continue
        blocks_payload = load_blocks_json(blocks_path)
        if blocks_payload is None:
            continue
        records = [
            record
            for record in blocks_payload["blocks"]
            if isinstance(record, dict)
            and str(record.get("block_id") or record.get("id") or "") == block_id
        ]
        for record in records:
            matches.append(
                {
                    "document": document,
                    "pdf_path": pdf_path,
                    "blocks_path": blocks_path,
                    "blocks_payload": blocks_payload,
                    "record": record,
                }
            )
    return matches


def _resolve_inputs() -> dict[str, Any]:
    if not SESSION_PATH.is_file():
        raise RunFailure(f"session_not_found:{SESSION_PATH}")
    session = _read_json(SESSION_PATH)
    if (
        not isinstance(session, dict)
        or session.get("id") != SESSION_ID
        or session.get("kind") != "stage_comparison_shell"
        or session.get("schema_version") != 1
    ):
        raise RunFailure("session_contract_invalid")

    resolved: dict[str, Any] = {}
    for side, specification in SIDES.items():
        stage = specification["stage"]
        opposite_stage = "stage_2" if stage == "stage_1" else "stage_1"
        block_id = specification["block_id"]
        expected = _document_matches(session, stage, block_id)
        wrong_side = _document_matches(session, opposite_stage, block_id)
        if len(expected) != 1:
            raise RunFailure(
                f"{side}:expected_exactly_one_block_in_{stage}:found_{len(expected)}"
            )
        if wrong_side:
            raise RunFailure(
                f"{side}:block_also_present_on_wrong_side_{opposite_stage}:found_{len(wrong_side)}"
            )
        item = expected[0]
        pdf_path = item["pdf_path"]
        blocks_path = item["blocks_path"]
        blocks_payload = item["blocks_payload"]
        record = item["record"]
        if not blocks_path.is_file():
            raise RunFailure(f"{side}:blocks_json_not_found:{blocks_path}")
        if not pdf_path.is_file():
            raise RunFailure(f"{side}:document_pdf_not_found:{pdf_path}")
        if blocks_payload.get("schema_version") != 1:
            raise RunFailure(f"{side}:blocks_schema_invalid")
        if blocks_payload.get("coordinate_space") != "normalized_page_top_left":
            raise RunFailure(f"{side}:blocks_coordinate_space_invalid")
        page_index = record.get("page_index")
        if (
            not isinstance(page_index, int)
            or isinstance(page_index, bool)
            or page_index < 0
        ):
            raise RunFailure(f"{side}:page_index_invalid")
        page_records = [
            page
            for page in blocks_payload.get("pages") or []
            if isinstance(page, dict) and page.get("page_index") == page_index
        ]
        if len(page_records) != 1:
            raise RunFailure(f"{side}:page_metadata_not_unique")
        _validate_coords(record, side)
        resolved[side] = {
            **item,
            "page_metadata": page_records[0],
            "pdf_sha256": _file_digest(pdf_path),
            "blocks_sha256": _file_digest(blocks_path),
        }

    confirmed = (session.get("document_pairing") or {}).get("confirmed_pairs")
    if not isinstance(confirmed, list):
        raise RunFailure("session_document_pairing_absent")
    left_pdf = str(resolved["LEFT"]["pdf_path"])
    right_pdf = str(resolved["RIGHT"]["pdf_path"])
    matching_pairs = [
        pair
        for pair in confirmed
        if isinstance(pair, dict)
        and pair.get("left_pdf") == left_pdf
        and pair.get("right_pdf") == right_pdf
    ]
    if len(matching_pairs) != 1:
        raise RunFailure(
            f"session_document_pairing_not_unique:found_{len(matching_pairs)}"
        )
    return {"session": session, "sides": resolved, "document_pairing": matching_pairs[0]}


def _evidence_summary(evidence: Any) -> dict[str, Any]:
    return {
        "extraction_ok": evidence.extraction_ok,
        "reasons": evidence.reasons,
        "page_index": evidence.page_index,
        "coordinate_system": evidence.coordinate_system,
        "page_size": evidence.page_size,
        "block_bbox_visual_pt": evidence.block_bbox,
        "block_polygon_points": len(evidence.block_polygon),
        "counts": {
            "visual_words": len(evidence.visual_words),
            "paths": len(evidence.paths),
            "lines": len(evidence.lines),
            "curves": len(evidence.curves),
            "polygons": len(evidence.polygons),
        },
        "extraction_gate": evidence.extraction_gate,
        "provenance": evidence.provenance,
    }


def _build_side(side: str, source: dict[str, Any]) -> dict[str, Any]:
    record = source["record"]
    evidence = extract_vector_evidence(
        source["pdf_path"],
        page_index=record["page_index"],
        block_id=SIDES[side]["block_id"],
        bbox_norm=record["coords_norm"],
        polygon_norm=record.get("polygon_points"),
    )
    evidence_summary = _evidence_summary(evidence)
    if not evidence.extraction_ok:
        raise RunFailure(f"{side}:vector_extraction_failed:{evidence.reasons}")

    detector = detect_dense_sectioned_board(evidence)
    if detector.get("detected") is not True:
        raise RunFailure(
            f"{side}:dense_detector_failed:{detector.get('reasons') or detector}"
        )

    graph = build_dense_sectioned_board_graph(evidence, detection=detector)
    dense_gate = evaluate_dense_sectioned_board_gate(graph)
    if dense_gate.get("use") is not True:
        raise RunFailure(f"{side}:dense_gate_failed:{dense_gate.get('reasons')}")

    validation = validate_system_graph(graph)
    if validation.get("valid") is not True:
        raise RunFailure(f"{side}:system_graph_invalid:{validation.get('errors')}")
    return {
        "graph": graph,
        "evidence_summary": evidence_summary,
        "detector": detector,
        "dense_gate": dense_gate,
        "validation": validation,
    }


def _execute_pipeline(inputs: dict[str, Any]) -> dict[str, Any]:
    left = _build_side("LEFT", inputs["sides"]["LEFT"])
    right = _build_side("RIGHT", inputs["sides"]["RIGHT"])
    comparison = compare_system_graphs(left["graph"], right["graph"])
    comparison_validation = validate_comparison_result(comparison)
    if comparison_validation.get("valid") is not True:
        raise RunFailure(
            f"comparison_invalid:{comparison_validation.get('errors')}"
        )
    ledger = adapt_system_graph_comparison_to_ledger(
        comparison, left["graph"], right["graph"]
    )
    try:
        validated_ledger = validate_ledger(ledger)
    except ValueError as error:
        raise RunFailure(f"ledger_invalid:{error}") from error
    return {
        "left": left,
        "right": right,
        "comparison": comparison,
        "comparison_validation": comparison_validation,
        "ledger": validated_ledger,
    }


def _reproducibility(first: dict[str, Any], second: dict[str, Any]) -> dict[str, Any]:
    values = {
        "left_system_graph": (first["left"]["graph"], second["left"]["graph"]),
        "right_system_graph": (first["right"]["graph"], second["right"]["graph"]),
        "comparison_result": (first["comparison"], second["comparison"]),
        "graphic_change_ledger": (first["ledger"], second["ledger"]),
    }
    checks = {}
    for name, (left, right) in values.items():
        first_digest = _canonical_digest(left)
        second_digest = _canonical_digest(right)
        checks[name] = {
            "first_canonical_sha256": first_digest,
            "second_canonical_sha256": second_digest,
            "identical": first_digest == second_digest and left == right,
        }
    reproducible = all(item["identical"] for item in checks.values())
    result = {"reproducible": reproducible, "checks": checks}
    if not reproducible:
        mismatched = [name for name, item in checks.items() if not item["identical"]]
        raise RunFailure(f"reproducibility_failed:{','.join(mismatched)}")
    return result


def _side_manifest(side: str, inputs: dict[str, Any], run: dict[str, Any]) -> dict:
    source = inputs["sides"][side]
    document = source["document"]
    blocks = source["blocks_payload"]
    record = source["record"]
    side_run = run[side.lower()]
    return {
        "chronology": SIDES[side]["chronology"],
        "expected_stage": SIDES[side]["stage"],
        "block_id": SIDES[side]["block_id"],
        "document_identity": {
            "document_code": document.get("document_code"),
            "filename": document.get("filename"),
            "relative": document.get("relative"),
            "discipline": document.get("discipline"),
            "version_id": document.get("version_id"),
            "blocks_document_id": blocks.get("document_id"),
            "blocks_document_name": blocks.get("document_name"),
        },
        "source_paths": {
            "pdf": str(source["pdf_path"].resolve()),
            "blocks_json": str(source["blocks_path"].resolve()),
        },
        "source_sha256": {
            "pdf": source["pdf_sha256"],
            "blocks_json": source["blocks_sha256"],
        },
        "page_index": record["page_index"],
        "rotation": {
            "blocks_metadata_degrees": source["page_metadata"].get("rotation"),
            "production_extractor_degrees": side_run["evidence_summary"]["provenance"].get(
                "rotation_degrees"
            ),
            "production_rotation_applied": side_run["evidence_summary"]["provenance"].get(
                "rotation_applied"
            ),
        },
        "prepared_geometry": {
            "coords_norm": record["coords_norm"],
            "polygon_points": record.get("polygon_points"),
        },
        "vector_evidence_summary": side_run["evidence_summary"],
        "detector_result": side_run["detector"],
        "dense_gate_result": side_run["dense_gate"],
        "system_graph_validation": side_run["validation"],
        "system_graph_counts": {
            "nodes": len(side_run["graph"]["nodes"]),
            "edges": len(side_run["graph"]["edges"]),
        },
        "system_graph_warnings": side_run["graph"].get("warnings") or [],
    }


def _manifest(
    inputs: dict[str, Any],
    run: dict[str, Any],
    reproducibility: dict[str, Any],
    artifact_bytes: dict[str, bytes],
) -> dict[str, Any]:
    comparison = run["comparison"]
    return {
        "runner_version": RUNNER_VERSION,
        "session_id": SESSION_ID,
        "session_path": str(SESSION_PATH.resolve()),
        "direction": "LEFT_TO_RIGHT",
        "chronology": "earlier_to_later",
        "pair_semantics": "PD_earlier_to_PD_correction_later",
        "pair_id_created": False,
        "document_pairing": inputs["document_pairing"],
        "sides": {
            side: _side_manifest(side, inputs, run) for side in ("LEFT", "RIGHT")
        },
        "versions": {
            "blocks_schema_version": 1,
            "vector_evidence": EXTRACTION_VERSION,
            "dense_profile_id": PROFILE_ID,
            "dense_profile_version": PROFILE_VERSION,
            "system_graph_schema": SYSTEM_GRAPH_SCHEMA_VERSION,
            "comparison_schema": COMPARISON_SCHEMA_VERSION,
            "comparator_version": COMPARATOR_VERSION,
            "matcher_version": MATCHER_VERSION,
            "comparison_policy_id": DEFAULT_COMPARISON_POLICY.policy_id,
            "ledger_schema": MODE2_SCHEMA_VERSION,
            "ledger_adapter_id": ADAPTER_ID,
            "ledger_adapter_version": ADAPTER_VERSION,
            "ledger_confidence_policy_id": MODE2_CONFIDENCE_POLICY_V1.policy_id,
        },
        "validation": {
            "left_system_graph": run["left"]["validation"],
            "right_system_graph": run["right"]["validation"],
            "comparison": run["comparison_validation"],
            "ledger": {"valid": True, "schema_version": run["ledger"]["schema_version"]},
        },
        "comparison": {
            "status": comparison.get("status"),
            "policy": DEFAULT_COMPARISON_POLICY.public_contract(),
            "provenance": comparison.get("provenance"),
            "quality": comparison.get("comparison_quality"),
            "summary": comparison.get("summary"),
        },
        "reproducibility": reproducibility,
        "artifact_sha256": {
            ARTIFACT_FILENAMES[key]: hashlib.sha256(payload).hexdigest()
            for key, payload in artifact_bytes.items()
        },
    }


def main() -> int:
    try:
        inputs = _resolve_inputs()
        first = _execute_pipeline(inputs)
        second = _execute_pipeline(inputs)
        reproducibility = _reproducibility(first, second)

        artifacts = {
            "left_graph": first["left"]["graph"],
            "right_graph": first["right"]["graph"],
            "comparison": first["comparison"],
            "ledger": first["ledger"],
        }
        artifact_bytes = {key: _json_bytes(value) for key, value in artifacts.items()}
        manifest = _manifest(inputs, first, reproducibility, artifact_bytes)

        for key, payload in artifact_bytes.items():
            _atomic_write_bytes(OUTPUT_DIR / ARTIFACT_FILENAMES[key], payload)
        _atomic_write_bytes(OUTPUT_DIR / "run_manifest.json", _json_bytes(manifest))

        print(
            json.dumps(
                {
                    "output_dir": str(OUTPUT_DIR),
                    "reproducible": reproducibility["reproducible"],
                    "left": manifest["sides"]["LEFT"]["system_graph_counts"],
                    "right": manifest["sides"]["RIGHT"]["system_graph_counts"],
                    "comparison": manifest["comparison"]["summary"],
                    "ledger_changes": len(first["ledger"]["changes"]),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except RunFailure as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
