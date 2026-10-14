"""Раскладка хранилища сравнения версий.

Версии привязаны к объекту только ссылкой ``object_id`` и хранятся отдельно
от ``projects_v2`` (решение 14.10, план §3.2)::

    <VERSION_COMPARISON_ROOT>/objects/<object_id>/
        catalog.json, sources/<sha>.pdf, profiles/<sha>/<algo>/,
        thumbs/<sha>/, lineage/<project_id>.json, runs/<run_id>/, uploads/<uid>/
"""
from __future__ import annotations

import os
from pathlib import Path

from backend.app.core.config import ROOT_DIR

from .settings import ALGO_VERSION, PROFILE_VERSION


def safe_id(value: str) -> str:
    safe = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in "-_.")
    safe = safe.strip(".")
    if not safe or safe != str(value):
        raise ValueError(f"недопустимый идентификатор: {value!r}")
    return safe


def root() -> Path:
    raw = os.environ.get("VERSION_COMPARISON_ROOT", "").strip()
    return Path(raw).expanduser().resolve() if raw else ROOT_DIR / "version_comparison"


def object_dir(object_id: str) -> Path:
    return root() / "objects" / safe_id(object_id)


def catalog_path(object_id: str) -> Path:
    return object_dir(object_id) / "catalog.json"


def sources_dir(object_id: str) -> Path:
    return object_dir(object_id) / "sources"


def source_pdf(object_id: str, sha: str) -> Path:
    return sources_dir(object_id) / f"{safe_id(sha)}.pdf"


def uploads_dir(object_id: str) -> Path:
    return object_dir(object_id) / "uploads"


def upload_dir(object_id: str, upload_id: str) -> Path:
    return uploads_dir(object_id) / safe_id(upload_id)


def profile_dir(object_id: str, sha: str) -> Path:
    return object_dir(object_id) / "profiles" / safe_id(sha) / PROFILE_VERSION


def thumbs_dir(object_id: str, sha: str) -> Path:
    return object_dir(object_id) / "thumbs" / safe_id(sha)


def lineage_path(object_id: str, project_id: str) -> Path:
    return object_dir(object_id) / "lineage" / f"{safe_id(project_id)}.json"


def runs_dir(object_id: str) -> Path:
    return object_dir(object_id) / "runs"


def run_dir(object_id: str, run_id: str) -> Path:
    return runs_dir(object_id) / safe_id(run_id)


def sheet_dir(object_id: str, run_id: str, sheet_pair_id: str) -> Path:
    return run_dir(object_id, run_id) / "sheets" / safe_id(sheet_pair_id)


def pair_cache_dir(object_id: str) -> Path:
    return object_dir(object_id) / "cache" / "pairs" / ALGO_VERSION
