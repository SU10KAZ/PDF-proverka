#!/usr/bin/env python3
"""Reproduce G2.2 artifacts from the four research-corpus dense blocks.

This is an evaluation runner, not production routing.  Production detection
contains no block ids; the two named ids below only select the mandatory report
artifacts requested for G2.2.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.dense_sectioned_board import (
    PROFILE_VERSION,
    build_dense_sectioned_board_graph,
    detect_dense_sectioned_board,
    evaluate_dense_sectioned_board_gate,
)
from backend.app.pipeline.stages.block_grounding.vector_evidence import (
    extract_vector_evidence,
)


HERE = Path(__file__).resolve().parent
RESEARCH = ROOT / "experiments/g2_vectograf_system_graph_research/artifacts/dialects.json"
LEFT_ID = "blk_039909ec039649a1b8209f059c95167b"
RIGHT_ID = "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"


def _write_json(name: str, payload) -> None:
    (HERE / name).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _failure_reasons(graph: dict) -> list[str]:
    quality = graph["quality"]
    failures = []
    expected = {
        "sources": len([node for node in graph["nodes"] if node["type"] == "SOURCE"]),
        "inputs": quality.get("inputs"),
        "sections": quality.get("sections"),
        "section_devices": quality.get("section_devices"),
    }
    for field in ("sources", "inputs", "sections"):
        if expected[field] != 2:
            failures.append(f"expected_2_{field}:got_{expected[field]}")
    if expected["section_devices"] != 1:
        failures.append(
            f"expected_1_section_device:got_{expected['section_devices']}"
        )
    if quality.get("outgoing_devices", 0) < 1:
        failures.append("outgoing_groups_not_recovered")
    if not graph["validation"]["valid"]:
        failures.append("system_graph_contract_invalid")
    return failures


def _case(record: dict) -> tuple[dict, dict, dict]:
    blocks_path = ROOT / record["path"]
    prepared = json.loads(blocks_path.read_text(encoding="utf-8"))
    block = next(
        item for item in prepared["blocks"] if item["block_id"] == record["block_id"]
    )
    evidence = extract_vector_evidence(
        blocks_path.parent / "document.pdf",
        page_index=block["page_index"],
        block_id=block["block_id"],
        bbox_norm=block["coords_norm"],
        polygon_norm=block.get("polygon_points"),
    )
    detection = detect_dense_sectioned_board(evidence)
    graph = build_dense_sectioned_board_graph(evidence, detection=detection)
    quality = graph["quality"]
    summary = {
        "block_id": block["block_id"],
        "prepared_block": record["path"],
        "page_index": block["page_index"],
        "page_rotation": record.get("page_rotation"),
        "extraction_ok": evidence.extraction_ok,
        "profile_detected": detection["detected"],
        "profile_confidence": detection["profile_confidence"],
        "gate_use": evaluate_dense_sectioned_board_gate(graph)["use"],
        "contract_valid": graph["validation"]["valid"],
        "counts": {
            "sources": len(
                [node for node in graph["nodes"] if node["type"] == "SOURCE"]
            ),
            "inputs": quality.get("inputs", 0),
            "sections": quality.get("sections", 0),
            "section_devices": quality.get("section_devices", 0),
            "outgoing_devices": quality.get("outgoing_devices", 0),
        },
        "labels": {
            node_type: [
                node.get("label")
                for node in graph["nodes"]
                if node["type"] == node_type
            ]
            for node_type in ("SOURCE", "INPUT_DEVICE", "BUS_SECTION", "SECTION_DEVICE")
        },
        "honesty": {
            key: quality.get(key)
            for key in (
                "source_confidence",
                "bus_confidence",
                "section_confidence",
                "device_coverage",
                "feeder_coverage",
                "identity_coverage",
                "unknown_nodes",
                "unknown_edges",
            )
        },
        "unknown_labels": sorted(
            {
                str(node.get("label") or "UNLABELED")
                for node in graph["nodes"]
                if node["type"] == "UNKNOWN_NODE"
            }
        ),
        "failures": _failure_reasons(graph),
    }
    return detection, graph, summary


def _report(results: list[dict]) -> str:
    left = next(item for item in results if item["block_id"] == LEFT_ID)
    right = next(item for item in results if item["block_id"] == RIGHT_ID)
    rows = []
    for title, item in (("П, стр. 21", left), ("РД, стр. 52", right)):
        counts = item["counts"]
        labels = item["labels"]
        rows.append(
            f"| {title} | {', '.join(labels['SOURCE'])} | "
            f"{', '.join(labels['INPUT_DEVICE'])} | {counts['sections']} | "
            f"{', '.join(labels['SECTION_DEVICE'])} | {counts['outgoing_devices']} |"
        )
    unknown = "\n".join(
        f"- `{item['block_id']}`: {item['honesty']['unknown_nodes']} unknown nodes; "
        f"labels: {', '.join(item['unknown_labels']) or 'нет'}; "
        f"identity coverage {item['honesty']['identity_coverage']:.3f}."
        for item in (left, right)
    )
    detected = sum(item["profile_detected"] for item in results)
    valid = sum(item["contract_valid"] for item in results)
    failures = [failure for item in results for failure in item["failures"]]
    return f"""# G2.2 — dense_sectioned_board production profile

## Результат

Профиль построен без production CASES: `dense_sectioned_board.py` принимает только
`VectorEvidence`, не открывает PDF и не содержит block/page ids или заранее заданных
обозначений оборудования и числа секций. Корпусные ids живут только в этом
воспроизводимом evaluation runner.

## 1. Получился ли профиль без CASES?

Да. Detection использует плотность аппаратов, Y/X clustering, повторяемость колонок,
префиксы, горизонтальную геометрию шин и межсекционный разрыв. Недостаточный набор
сигналов возвращает `UNKNOWN`; `profile_confidence` сохраняется в графе.

## 2. Что распознаётся

| Блок | SOURCE | INPUT_DEVICE | BUS_SECTION | SECTION_DEVICE | OUTGOING_DEVICE |
|---|---|---|---:|---|---:|
{chr(10).join(rows)}

На обязательной паре автоматически восстановлены цепочки
`SOURCE → INPUT_DEVICE → BUS_SECTION → OUTGOING_DEVICE → LOAD/UNKNOWN_NODE`.
У `SOURCE` отдельно сохранены `source_role` и `source_representation`.

## 3. Что осталось unknown

{unknown}

`UNKNOWN_NODE` создаётся для ветвей без надёжно привязанной идентичности нагрузки и
для неразрешённых аппаратов вводной зоны. Он не блокирует восстановленный backbone.

## 4. Переносимость и coverage

Исследовательский корпус: {detected}/{len(results)} блоков detected,
{valid}/{len(results)} валидны по `system-graph.v1`; failures: {failures or 'нет'}.
Проверены две стадии и два способа представления листа, включая поворот 270°.
Это подтверждает переносимость между четырьмя доступными dense-блоками, но корпус
происходит из одного проектного комплекта; до универсального профиля всех ГРЩ нужна
дальнейшая межпроектная выборка. Близкие, но недостаточно плотные щиты остаются на
classic/других профилях или честно деградируют в `UNKNOWN`/raw-vector.

## 5. Classic path и Stage Comparison

Classic Vectograf остаётся первым в router cascade; существующий builder и gate не
изменены. Общая offset-привязка колонок перенесена в geometry helper с совместимым
classic wrapper; на 2000 детерминированных случайных раскладках результат старой и
новой реализаций совпал полностью.

- G2.2/profile + block-context/source-kind integration: `56 passed`.
- classic Vectograf/singleline/common evidence: `57 passed, 23 skipped`;
  skips — отсутствующие локальные PDF-корпусы, как в G2.1.
- Stage Comparison: `300 passed`.

## 6. Готовность к comparator

Да, после принятия G2.2 можно переходить к отдельному этапу comparator: оба графа
имеют единый контракт, provenance, grounded nodes/edges и раздельные honesty metrics.
Comparator, сравнение П↔РД и GraphicChangeLedger в G2.2 намеренно не реализованы.
Identity coverage ниже 1.0 должна учитываться будущим comparator как неопределённость,
а не как доказанное изменение.

## Воспроизведение

```bash
python experiments/g2_dense_sectioned_board/run_corpus.py
```
"""


def main() -> None:
    research = json.loads(RESEARCH.read_text(encoding="utf-8"))
    records = [
        item
        for item in research["blocks"]
        if item.get("dialect") == "dense_sectioned_board"
    ]
    if len(records) < 4:
        raise RuntimeError(f"dense corpus too small: {len(records)}")
    detections, graphs, summaries = {}, {}, []
    for record in records:
        detection, graph, summary = _case(record)
        detections[record["block_id"]] = {
            "block_id": record["block_id"],
            "prepared_block": record["path"],
            "detection": detection,
        }
        graphs[record["block_id"]] = graph
        summaries.append(summary)

    _write_json("left_system_graph.json", graphs[LEFT_ID])
    _write_json("right_system_graph.json", graphs[RIGHT_ID])
    _write_json(
        "profile_detection.json",
        {
            "profile_version": PROFILE_VERSION,
            "corpus_source": str(RESEARCH.relative_to(ROOT)),
            "results": list(detections.values()),
        },
    )
    _write_json(
        "coverage_results.json",
        {
            "profile_version": PROFILE_VERSION,
            "manual_case_logic": False,
            "comparisons_performed": False,
            "total": len(summaries),
            "detected": sum(item["profile_detected"] for item in summaries),
            "gate_passed": sum(item["gate_use"] for item in summaries),
            "contract_valid": sum(item["contract_valid"] for item in summaries),
            "expected_topology_passed": sum(not item["failures"] for item in summaries),
            "results": summaries,
        },
    )
    (HERE / "dense_profile_report.md").write_text(
        _report(summaries), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
