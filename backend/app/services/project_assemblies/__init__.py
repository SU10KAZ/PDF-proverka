"""Immutable project assemblies used as Stage Comparison documents."""

from .service import (
    AssemblyError,
    build_version,
    create_assembly,
    enabled,
    get_assembly,
    list_assemblies,
    list_sources,
    preview,
)

__all__ = [
    "AssemblyError",
    "build_version",
    "create_assembly",
    "enabled",
    "get_assembly",
    "list_assemblies",
    "list_sources",
    "preview",
]
