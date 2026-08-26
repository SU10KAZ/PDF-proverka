"""G2.2 production profile and real-corpus regressions."""
from __future__ import annotations

import inspect
import json
import shutil
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.dense_sectioned_board import (
    PROFILE_ID,
    build_dense_sectioned_board_graph,
    detect_dense_sectioned_board,
    evaluate_dense_sectioned_board_gate,
    extract_device_candidates,
)
from backend.app.pipeline.stages.block_grounding.system_graph import (
    EDGE_TYPES,
    NODE_TYPES,
    validate_system_graph,
)
from backend.app.pipeline.stages.block_grounding.vector_evidence import (
    VectorEvidence,
    extract_vector_evidence,
)


ROOT = Path(__file__).resolve().parent.parent

# Evaluation manifest only.  These ids are never imported by production code;
# they are the four dense blocks selected by the preceding corpus research.
DENSE_CORPUS = (
    (
        "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/"
        "stage_1/documents/АА_БЭ-03-ДС3-ИОС1.1/versions/v001/02_work/blocks.json",
        "blk_7d0d6c0536a541889e8082b4b2de2000",
    ),
    (
        "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/"
        "stage_2/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия/"
        "versions/v001/02_work/blocks.json",
        "blk_039909ec039649a1b8209f059c95167b",
    ),
    (
        "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/"
        "stage_2/documents/АА-БЭ-03-ДС3-ИОС1.1/versions/v001/02_work/blocks.json",
        "blk_055dc095ad2343b29e469ea6d3f58790",
    ),
    (
        "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/"
        "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1/"
        "versions/v001/02_work/blocks.json",
        "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6",
    ),
)


def _word(x: float, y: float, text: str) -> tuple:
    return (x - 4, y - 2, x + 4, y + 2, text, 0, 0, 0)


def _evidence(words, *, lines=()) -> VectorEvidence:
    return VectorEvidence(
        page_index=0,
        visual_words=list(words),
        lines=[list(line) for line in lines],
        page_size=[900.0, 500.0],
        block_bbox=[0.0, 0.0, 900.0, 500.0],
        extraction_gate={
            "extraction_ok": True,
            "reason": None,
            "reasons": [],
            "metrics": {"coordinates_valid": True, "geometry_available": True},
        },
        provenance={"block_id": "synthetic-dense", "rotation_degrees": 0},
    )


def _synthetic_dense_evidence() -> VectorEvidence:
    words = []
    for section, start in ((1, 100), (2, 500)):
        for index in range(1, 7):
            x = start + (index - 1) * 50
            words.append(_word(x, 100, f"{section}QF{index}"))
            if index <= 5:
                words.append(_word(x, 65, f"ВРУ{section}-{index}"))
        words.append(_word(start + 125, 82, f"РП{section}"))
    words.extend(
        (
            _word(225, 170, "QF1"),
            _word(225, 182, "2500А"),
            _word(625, 170, "QF2"),
            _word(625, 182, "2500А"),
            _word(425, 140, "QS7"),
            _word(225, 240, "ТП1"),
            _word(625, 240, "ТП2"),
            _word(420, 80, "L1-L2-L3"),
            _word(425, 88, "PEN"),
        )
    )
    return _evidence(
        words,
        lines=((80, 115, 370, 115), (480, 115, 780, 115)),
    )


def _load_real_case(relative_path: str, block_id: str):
    blocks_path = ROOT / relative_path
    if not blocks_path.exists():
        pytest.skip("real dense corpus is not installed")
    data = json.loads(blocks_path.read_text(encoding="utf-8"))
    block = next(item for item in data["blocks"] if item["block_id"] == block_id)
    evidence = extract_vector_evidence(
        blocks_path.parent / "document.pdf",
        page_index=block["page_index"],
        block_id=block_id,
        bbox_norm=block["coords_norm"],
        polygon_norm=block.get("polygon_points"),
    )
    return evidence


def test_device_recognizer_supports_numbering_and_non_qf_variants():
    evidence = _evidence(
        [
            _word(index * 30 + 20, 50, label)
            for index, label in enumerate(
                ("QF1", "1QF1", "2QF14", "QS1", "FU", "РН", "УЗИП", "ОПН2")
            )
        ]
    )

    devices = extract_device_candidates(evidence)

    assert [item["label"] for item in devices] == [
        "QF1", "1QF1", "2QF14", "QS1", "FU", "РН", "УЗИП", "ОПН2"
    ]
    assert all(
        {"type_candidate", "label", "bbox", "confidence", "evidence"} <= item.keys()
        for item in devices
    )


def test_sparse_or_classic_like_scheme_fails_closed_to_unknown_profile():
    sparse = _evidence([_word(100 + index * 40, 100, f"QF{index}.1") for index in range(9)])

    detection = detect_dense_sectioned_board(sparse)
    graph = build_dense_sectioned_board_graph(sparse, detection=detection)

    assert detection["id"] == "UNKNOWN"
    assert detection["detected"] is False
    assert 0 <= detection["profile_confidence"] < detection["threshold"]
    assert graph["nodes"] == []
    assert evaluate_dense_sectioned_board_gate(graph)["use"] is False


def test_synthetic_dense_graph_is_grounded_and_preserves_unknown_terminals():
    evidence = _synthetic_dense_evidence()

    detection = detect_dense_sectioned_board(evidence)
    graph = build_dense_sectioned_board_graph(evidence, detection=detection)

    assert detection["id"] == PROFILE_ID
    assert detection["detected"] is True
    assert graph["quality"]["sections"] == 2
    assert graph["quality"]["inputs"] == 2
    assert graph["quality"]["section_devices"] == 1
    assert graph["quality"]["outgoing_devices"] == 12
    assert graph["quality"]["unknown_nodes"] > 0
    assert graph["provenance"]["manual_cases"] is False
    assert evaluate_dense_sectioned_board_gate(graph)["use"] is True
    assert validate_system_graph(graph)["valid"] is True
    for item in graph["nodes"] + graph["edges"]:
        assert {"confidence", "evidence", "bbox", "source_tokens"} <= item.keys()
        assert item["evidence"]
        assert len(item["bbox"]) == 4
    assert {node["type"] for node in graph["nodes"]} <= NODE_TYPES
    assert {edge["type"] for edge in graph["edges"]} <= EDGE_TYPES


@pytest.mark.parametrize("relative_path,block_id", DENSE_CORPUS)
def test_real_dense_corpus_recovers_backbone(relative_path, block_id):
    evidence = _load_real_case(relative_path, block_id)

    detection = detect_dense_sectioned_board(evidence)
    graph = build_dense_sectioned_board_graph(evidence, detection=detection)

    assert detection["detected"] is True
    assert graph["quality"]["sections"] == 2
    assert graph["quality"]["inputs"] == 2
    assert graph["quality"]["section_devices"] == 1
    assert graph["quality"]["outgoing_devices"] >= 20
    assert len([node for node in graph["nodes"] if node["type"] == "SOURCE"]) == 2
    assert graph["validation"]["valid"] is True
    assert evaluate_dense_sectioned_board_gate(graph)["use"] is True


def test_router_emits_system_graph_for_mandatory_right_block(tmp_path):
    relative_path, block_id = DENSE_CORPUS[-1]
    source_blocks = ROOT / relative_path
    if not source_blocks.exists():
        pytest.skip("real dense corpus is not installed")
    prepared = json.loads(source_blocks.read_text(encoding="utf-8"))
    block = next(item for item in prepared["blocks"] if item["block_id"] == block_id)
    version = (
        tmp_path
        / "objects/test/disciplines/EOM/documents/dense/versions/v001"
    )
    work = version / "02_work"
    output = version / "03_analysis/runs/g2"
    work.mkdir(parents=True)
    output.mkdir(parents=True)
    shutil.copy2(source_blocks.parent / "document.pdf", work / "document.pdf")
    (output / "document_graph.json").write_text(
        json.dumps(
            {
                "pages": [
                    {
                        "page": 1,
                        "page_index": block["page_index"],
                        "image_blocks": [
                            {
                                "id": block_id,
                                "coords_norm": block["coords_norm"],
                                "polygon_points_norm": block["polygon_points"],
                            }
                        ],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    from backend.app.pipeline.stages.block_grounding.block_source_router import (
        resolve_block_package,
    )

    package = resolve_block_package(output, block_id, 1, prefer_prepared=False)

    assert package["source_kind"] == "structured_system_graph"
    assert package["profile_id"] == PROFILE_ID
    assert package["graph"]["validation"]["valid"] is True
    assert package["gate"]["use"] is True


def test_production_profile_has_no_case_ids_or_pdf_parser():
    import backend.app.pipeline.stages.block_grounding.dense_sectioned_board as module

    source = inspect.getsource(module)
    assert "blk_" not in source
    assert "fitz" not in source
    assert "pdfplumber" not in source
    assert "VectorEvidence" in source
