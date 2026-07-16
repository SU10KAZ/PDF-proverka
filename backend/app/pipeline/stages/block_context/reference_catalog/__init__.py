"""Производственный каталог эталонов этапа «Векторные графы блоков»."""

from .loader import (
    catalog_runtime_info,
    load_catalog_manifest,
    load_reference_records,
    load_reference_rules,
)

__all__ = [
    "catalog_runtime_info",
    "load_catalog_manifest",
    "load_reference_records",
    "load_reference_rules",
]
