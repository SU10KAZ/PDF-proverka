"""Deterministic TEXT entity ↔ GRAPHIC entity bridge."""

from .entity_bridge import (
    BridgeValidationError,
    build_entity_links,
    build_entity_links_from_artifacts,
    entity_links_are_stale,
    graphic_entities_from_system_graph,
    validate_entity_links,
    validate_entity_links_artifact,
)
from .graph_entity_adapter import build_graph_entities, validate_graph_entities
from .comparison_scope import (
    build_scope_join,
    query_text_scope,
    validate_scope_join,
)
from .entity_normalizer import normalize_entity_name, normalize_functional_role
from .graphic_coverage import (
    build_graphic_coverage,
    coverage,
    validate_graphic_coverage,
)
from .side_entity_contract import (
    build_side_entity_links,
    build_side_graph_entities,
    query_text_entity_side,
    validate_side_entity_links,
    validate_side_graph_entities,
)
from .text_entity_producer import build_text_entities, validate_text_entities

__all__ = [
    "BridgeValidationError",
    "build_entity_links",
    "build_entity_links_from_artifacts",
    "build_graph_entities",
    "build_graphic_coverage",
    "build_scope_join",
    "build_side_entity_links",
    "build_side_graph_entities",
    "build_text_entities",
    "coverage",
    "entity_links_are_stale",
    "graphic_entities_from_system_graph",
    "normalize_entity_name",
    "normalize_functional_role",
    "query_text_entity_side",
    "query_text_scope",
    "validate_entity_links",
    "validate_entity_links_artifact",
    "validate_graph_entities",
    "validate_graphic_coverage",
    "validate_scope_join",
    "validate_side_entity_links",
    "validate_side_graph_entities",
    "validate_text_entities",
]
