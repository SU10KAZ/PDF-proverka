"""Versioned domain policy for SYSTEM_GRAPH matching and comparison.

The comparison engine owns generic orchestration.  Vocabulary, graph roles,
quality thresholds and representation-equivalence rules live here so another
domain does not have to modify comparator control flow.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


#: Свойства аппарата, о ЛЮБОМ изменении которых сравнение вправе заявить.
#: Тождество на них опираться не должно: иначе аппарат, у которого номинал
#: как раз и поменялся, хуже опознаётся как тот же самый — сигнал «атрибуты
#: совпали» падает ровно там, где мы собираемся объявить расхождение. Это
#: замкнутый круг: свойство участвует и в вопросе «тот ли это объект», и в
#: ответе «что у него стало другим».
COMPARED_ATTRIBUTE_KEYS = frozenset({"rating_a", "status", "type_candidate"})


@dataclass(frozen=True)
class SystemGraphComparisonPolicy:
    policy_id: str
    source_node_type: str
    input_node_type: str
    section_node_type: str
    section_device_node_type: str
    repeated_node_type: str
    terminal_node_type: str
    unknown_node_type: str
    functional_group_types: tuple[str, ...]
    always_anchored_group_types: tuple[str, ...]
    aggregate_group_type: str
    aggregate_count_attribute: str
    feed_edge_type: str
    tie_edge_type: str
    terminal_edge_type: str
    non_unique_role_types: tuple[str, ...]
    source_representation_ranks: Mapping[str, int]
    allowed_detail_transitions: tuple[tuple[str, str], ...]
    detail_node_type: str
    detail_allowed_subclasses: tuple[str, ...]
    minimum_identity_coverage: float = 0.5
    minimum_average_node_confidence: float = 0.5
    minimum_average_edge_confidence: float = 0.5
    high_match_threshold: float = 0.68
    medium_match_threshold: float = 0.38
    high_match_margin: float = 0.05
    certain_change_threshold: float = 0.6
    repeated_group_min_size: int = 3

    def representation_rank(self, value: Any) -> int:
        return int(self.source_representation_ranks.get(str(value or "UNKNOWN_SOURCE"), 0))

    def detail_transition_allowed(self, left: Any, right: Any) -> bool:
        pair = (str(left or "UNKNOWN_SOURCE"), str(right or "UNKNOWN_SOURCE"))
        return pair in self.allowed_detail_transitions

    def functional_anchor(self, node: dict) -> bool:
        node_type = str(node.get("type") or "")
        if node_type in self.always_anchored_group_types:
            return True
        return (
            node_type == self.aggregate_group_type
            and (node.get("attrs") or {}).get(self.aggregate_count_attribute) is not None
        )

    def representation_detail_node(self, node: dict) -> bool:
        if self.functional_anchor(node):
            return False
        if str(node.get("type") or "") != self.detail_node_type:
            return False
        subclass = str((node.get("attrs") or {}).get("subclass") or "")
        return subclass in self.detail_allowed_subclasses

    def public_contract(self) -> dict:
        return {
            "policy_id": self.policy_id,
            "quality_thresholds": {
                "minimum_identity_coverage": self.minimum_identity_coverage,
                "minimum_average_node_confidence": self.minimum_average_node_confidence,
                "minimum_average_edge_confidence": self.minimum_average_edge_confidence,
                "certain_change_threshold": self.certain_change_threshold,
            },
            "match_thresholds": {
                "high": self.high_match_threshold,
                "medium": self.medium_match_threshold,
                "high_margin": self.high_match_margin,
            },
            "detail_equivalence": {
                "allowed_transitions": [list(item) for item in self.allowed_detail_transitions],
                "allowed_node_type": self.detail_node_type,
                "allowed_subclasses": list(self.detail_allowed_subclasses),
            },
        }


DENSE_SECTIONED_BOARD_COMPARISON_POLICY = SystemGraphComparisonPolicy(
    policy_id="dense-sectioned-board-comparison-v1",
    source_node_type="SOURCE",
    input_node_type="INPUT_DEVICE",
    section_node_type="BUS_SECTION",
    section_device_node_type="SECTION_DEVICE",
    repeated_node_type="OUTGOING_DEVICE",
    terminal_node_type="LOAD",
    unknown_node_type="UNKNOWN_NODE",
    functional_group_types=("METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP"),
    always_anchored_group_types=("METERING_GROUP", "COMPENSATION_GROUP"),
    aggregate_group_type="SERVICE_GROUP",
    aggregate_count_attribute="member_count",
    feed_edge_type="FEEDS",
    tie_edge_type="TIES_SECTIONS",
    terminal_edge_type="TERMINATES_AT",
    non_unique_role_types=("OUTGOING_DEVICE", "LOAD", "UNKNOWN_NODE"),
    source_representation_ranks={
        "UNKNOWN_SOURCE": 0,
        "EXTERNAL_FEEDER": 1,
        "UPSTREAM_TP_CONNECTION": 1,
        "TRANSFORMER_EXPLICIT": 2,
    },
    allowed_detail_transitions=(("UPSTREAM_TP_CONNECTION", "TRANSFORMER_EXPLICIT"),),
    detail_node_type="SERVICE_GROUP",
    detail_allowed_subclasses=("BUSWAY",),
)

DEFAULT_COMPARISON_POLICY = DENSE_SECTIONED_BOARD_COMPARISON_POLICY


__all__ = [
    "DEFAULT_COMPARISON_POLICY",
    "DENSE_SECTIONED_BOARD_COMPARISON_POLICY",
    "SystemGraphComparisonPolicy",
]
