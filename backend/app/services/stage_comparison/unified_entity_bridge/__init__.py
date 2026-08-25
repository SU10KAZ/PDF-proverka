"""Deterministic TEXT entity ↔ GRAPHIC entity bridge."""

from .entity_bridge import (
    BridgeValidationError,
    build_entity_links,
    graphic_entities_from_system_graph,
    validate_entity_links,
)
from .entity_normalizer import normalize_entity_name, normalize_functional_role

__all__ = [
    "BridgeValidationError",
    "build_entity_links",
    "graphic_entities_from_system_graph",
    "normalize_entity_name",
    "normalize_functional_role",
    "validate_entity_links",
]
