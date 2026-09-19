"""Comparison scope: registry object ↔ stage-comparison sessions.

A stage-comparison session stores only its two stage folders.  The registry
object that owns them is the one whose ``projects_v2/objects/<obj>/comparison``
directory contains exactly those ``stage_1``/``stage_2`` folders
(``stage_comparison.objects.list_objects``).  Both directions are read-only:
nothing here creates a session, a folder or an artifact.
"""
from __future__ import annotations

from pathlib import Path

from backend.app.services.human_mapping_production.storage import require_safe_id


class ScopeUnresolved(LookupError):
    """The session/object relation cannot be proven from the stage folders."""


def _resolved(value: object) -> str:
    return str(Path(str(value or "")).expanduser().resolve())


def _object_stage_paths() -> dict[str, tuple[str, str]]:
    from backend.app.services.stage_comparison import objects as objects_mod

    out: dict[str, tuple[str, str]] = {}
    for item in objects_mod.list_objects().get("items") or []:
        stages = {s.get("name"): s.get("path") for s in item.get("stages") or []}
        if stages.get("stage_1") and stages.get("stage_2"):
            out[str(item["id"])] = (_resolved(stages["stage_1"]), _resolved(stages["stage_2"]))
    return out


def object_id_for_session(session_id: str) -> str:
    """Return the registry object that owns a session's stage folders."""
    from backend.app.services.stage_comparison import store

    session = store.get_session(session_id)
    if not session:
        raise ScopeUnresolved(f"session {session_id!r} not found")
    stages = (_resolved(session.get("stage_a_path")), _resolved(session.get("stage_b_path")))
    owners = [oid for oid, paths in _object_stage_paths().items() if paths == stages]
    if len(owners) != 1:
        raise ScopeUnresolved(
            f"session {session_id!r} belongs to {len(owners)} registry objects"
        )
    return require_safe_id(owners[0], "object")


def sessions_for_object(object_id: str) -> list[str]:
    """Sessions whose stage folders are this object's ``stage_1``/``stage_2``."""
    from backend.app.services.stage_comparison import store

    require_safe_id(object_id, "object")
    paths = _object_stage_paths().get(object_id)
    if paths is None:
        return []
    return [
        str(item["id"])
        for item in store.list_sessions()
        if (_resolved(item.get("stage_a_path")), _resolved(item.get("stage_b_path"))) == paths
    ]
