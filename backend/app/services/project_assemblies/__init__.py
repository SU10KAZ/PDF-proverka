"""Immutable project assemblies used as Stage Comparison documents."""

from .service import (
    AssemblyError,
    build_version,
    create_assembly,
    dedup_enabled,
    duplicate_review,
    enabled,
    get_assembly,
    list_assemblies,
    list_sources,
    preview,
    run_version,
    submit_duplicate_review,
)

__all__ = [
    "AssemblyError",
    "build_version",
    "create_assembly",
    "dedup_enabled",
    "duplicate_review",
    "enabled",
    "get_assembly",
    "list_assemblies",
    "list_sources",
    "preview",
    "run_version",
    "submit_duplicate_review",
]
