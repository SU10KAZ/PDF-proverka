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
from .document_binding import (
    BINDING_MISMATCH,
    BINDING_PROVEN,
    BINDING_STATES,
    BINDING_UNPROVEN,
    DocumentBindingValidationError,
    document_descriptor_for_block,
    normalize_document_descriptor,
    normalize_pair_documents,
    pair_documents_from_pair_artifact,
    validate_document_binding,
    verify_document_binding,
)
from .graph_entity_adapter import build_graph_entities, validate_graph_entities
from .comparison_scope import (
    build_scope_join,
    normalize_graphic_scope_groups,
    produce_graphic_scope_groups,
    query_text_scope,
    validate_scope_join,
)
from .entity_normalizer import normalize_entity_name, normalize_functional_role
from .graphic_coverage import (
    build_graphic_coverage,
    coverage,
    saved_coverage_bundle_is_stale,
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
    "BINDING_MISMATCH",
    "BINDING_PROVEN",
    "BINDING_STATES",
    "BINDING_UNPROVEN",
    "BridgeValidationError",
    "DocumentBindingValidationError",
    "build_entity_links",
    "build_entity_links_from_artifacts",
    "build_graph_entities",
    "build_graphic_coverage",
    "build_scope_join",
    "build_side_entity_links",
    "build_side_graph_entities",
    "build_text_entities",
    "coverage",
    "document_descriptor_for_block",
    "entity_links_are_stale",
    "graphic_entities_from_system_graph",
    "normalize_document_descriptor",
    "normalize_entity_name",
    "normalize_graphic_scope_groups",
    "normalize_functional_role",
    "normalize_pair_documents",
    "pair_documents_from_pair_artifact",
    "produce_graphic_scope_groups",
    "query_text_entity_side",
    "query_text_scope",
    "saved_coverage_bundle_is_stale",
    "validate_document_binding",
    "validate_entity_links",
    "validate_entity_links_artifact",
    "validate_graph_entities",
    "validate_graphic_coverage",
    "validate_scope_join",
    "validate_side_entity_links",
    "validate_side_graph_entities",
    "validate_text_entities",
    "verify_document_binding",
]
