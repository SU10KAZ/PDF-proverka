"""Отступ сопоставления считается по признакам тождества, а не по уверенности.

Потолок качества доказательств одинаков у всех кандидатов одного узла, потому
что обе стороны выпущены одним экстрактором. Пока отступ считался по
уверенности, ``min(score, evidence_confidence)`` срезал всех кандидатов к
одному числу, различие «совпала секция» / «не совпала» исчезало, и надёжных
сопоставлений не возникало там, где чертёж прочитан лучше всего.
"""
from __future__ import annotations

from backend.app.pipeline.stages.block_grounding.graph_identity_matcher import (
    MATCHER_VERSION,
    match_graph_nodes,
    score_node_pair,
)
from backend.app.pipeline.stages.block_grounding.system_graph import (
    SCHEMA_VERSION,
    make_edge,
    make_node,
)


def _evidence(token: str) -> list[dict]:
    return [
        {
            "kind": "token",
            "role": "test_grounding",
            "value": token,
            "bbox": [10.0, 10.0, 20.0, 20.0],
            "source_tokens": [token],
        }
    ]


def _node(
    node_id: str,
    node_type: str,
    *,
    canonical: str | None = None,
    label: str | None = None,
    confidence: float = 0.85,
    section: str | None = None,
    attrs: dict | None = None,
) -> dict:
    extra: dict = {
        "label": label or node_id,
        "canonical_identity": canonical,
        "attrs": attrs or {},
    }
    if section is not None:
        extra["section"] = section
    return make_node(
        node_id,
        node_type,
        confidence=confidence,
        evidence=_evidence(label or node_id),
        bbox=[10.0, 10.0, 20.0, 20.0],
        source_tokens=[label or node_id],
        **extra,
    )


def _edge(edge_type: str, source: str, target: str, nodes: dict[str, dict]) -> dict:
    return make_edge(
        f"{edge_type}:{source}->{target}",
        edge_type,
        source,
        target,
        confidence=0.9,
        evidence=_evidence(f"{source}->{target}"),
        source_bbox=nodes[source]["bbox"],
        target_bbox=nodes[target]["bbox"],
        source_tokens=[source, target],
    )


def _symmetric_board(*, consumer_confidence: float = 0.85) -> dict:
    """Щит из двух секций, где одинаковое ВРУ4 висит в каждой секции.

    Ровно та конфигурация, что делает симметричный ГРЩ трудным: имя нагрузки
    само по себе не различает секции, различает только принадлежность к шине.
    """
    nodes = {}
    for index in ("1", "2"):
        bus = f"BUS{index}"
        nodes[bus] = _node(
            bus, "BUS_SECTION", canonical=f"SECTION#{index}", label=f"РП{index}"
        )
        device = f"OUT:{index}QF4"
        nodes[device] = _node(
            device,
            "OUTGOING_DEVICE",
            canonical="ВРУ4",
            label=f"{index}QF4",
            section=bus,
            attrs={"rating_a": 500},
        )
        load = f"LOAD:{index}QF4"
        nodes[load] = _node(
            load,
            "LOAD",
            canonical="ВРУ4",
            label="ВРУ4",
            section=bus,
            confidence=consumer_confidence,
        )
    edges = [
        _edge("FEEDS", f"BUS{index}", f"OUT:{index}QF4", nodes) for index in ("1", "2")
    ] + [
        _edge("FEEDS", f"OUT:{index}QF4", f"LOAD:{index}QF4", nodes)
        for index in ("1", "2")
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def _matched_pairs(matching: dict) -> set[tuple[str, str]]:
    return {(item["left_id"], item["right_id"]) for item in matching["matches"]}


def test_section_context_survives_the_evidence_ceiling():
    """Нагрузка привязывается к своей секции, а не к одноимённой в соседней.

    Уверенность обоих кандидатов упирается в один потолок качества, поэтому
    различать их может только счёт признаков тождества — совпавшая секция.
    """
    left = _symmetric_board()
    right = _symmetric_board()
    matching = match_graph_nodes(left, right)
    pairs = _matched_pairs(matching)
    assert ("LOAD:1QF4", "LOAD:1QF4") in pairs
    assert ("LOAD:2QF4", "LOAD:2QF4") in pairs
    assert ("LOAD:1QF4", "LOAD:2QF4") not in pairs
    assert ("LOAD:2QF4", "LOAD:1QF4") not in pairs


def test_ceiling_would_have_erased_the_difference():
    """Счёт различает кандидатов, уверенность — нет. Это и есть причина правки."""
    left = _symmetric_board()
    right = _symmetric_board()
    index = {node["id"]: node for node in left["nodes"]}
    right_index = {node["id"]: node for node in right["nodes"]}
    same_section = score_node_pair(
        left, right, index["LOAD:1QF4"], right_index["LOAD:1QF4"]
    )
    other_section = score_node_pair(
        left, right, index["LOAD:1QF4"], right_index["LOAD:2QF4"]
    )
    assert same_section["score"] > other_section["score"]
    assert same_section["confidence"] == other_section["confidence"]


def test_margin_is_two_sided_and_recorded_in_the_policy():
    matching = match_graph_nodes(_symmetric_board(), _symmetric_board())
    assert matching["policy"]["margin_basis"] == "identity_score"
    assert matching["matcher_version"] == MATCHER_VERSION
    for match in matching["matches"]:
        assert match["left_margin"] > 0
        assert match["right_margin"] > 0


def test_genuinely_indistinguishable_candidates_stay_uncertain():
    """Когда секция не различает, надёжного сопоставления не возникает.

    Правка снимает потолок уверенности с отступа, но не отменяет сам отступ:
    два по-настоящему одинаковых кандидата обязаны остаться неоднозначностью.
    """
    def board() -> dict:
        nodes = {
            "BUS1": _node("BUS1", "BUS_SECTION", canonical="SECTION#1", label="РП1"),
            "LOAD:A": _node("LOAD:A", "LOAD", canonical="ВРУ4", label="ВРУ4", section="BUS1"),
            "LOAD:B": _node("LOAD:B", "LOAD", canonical="ВРУ4", label="ВРУ4", section="BUS1"),
        }
        edges = [
            _edge("FEEDS", "BUS1", "LOAD:A", nodes),
            _edge("FEEDS", "BUS1", "LOAD:B", nodes),
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "nodes": list(nodes.values()),
            "edges": edges,
        }

    matching = match_graph_nodes(board(), board())
    load_pairs = {
        pair for pair in _matched_pairs(matching) if pair[0].startswith("LOAD:")
    }
    assert not load_pairs
