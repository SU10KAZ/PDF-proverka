"""Comparison scope: registry object ↔ stage-comparison sessions.

A stage-comparison session stores only its two stage folders.  The registry
object that owns them is the one whose ``projects_v2/objects/<obj>/comparison``
directory contains exactly those ``stage_1``/``stage_2`` folders
(``stage_comparison.objects.list_objects``) — either Version 1 (the
``comparison`` folder itself) or another comparison version
(``comparison/versions/cvNNN``, see ``stage_comparison.comparison_versions``).
Both directions are read-only: nothing here creates a session, a folder or an
artifact.
"""
from __future__ import annotations

from pathlib import Path

from backend.app.services.human_mapping_production.storage import require_safe_id


class ScopeUnresolved(LookupError):
    """The session/object relation cannot be proven from the stage folders."""


def _resolved(value: object) -> str:
    return str(Path(str(value or "")).expanduser().resolve())


def _object_stage_paths() -> dict[str, tuple[str, str]]:
    """Version 1 stage folders of every registry object (tests replace this)."""
    from backend.app.services.common import object_service
    from backend.app.services.stage_comparison import stage_upload

    # Ownership needs paths, not PDF counts or assembled document manifests.
    out: dict[str, tuple[str, str]] = {}
    for item in object_service.list_objects():
        oid = str(item.get("id") or "")
        if not oid:
            continue
        try:
            _, root = stage_upload.resolve_object_dir(oid, create=False)
        except stage_upload.StageUploadError:
            continue
        out[oid] = (_resolved(root / "stage_1"), _resolved(root / "stage_2"))
    return out


def _object_version_stage_paths() -> dict[str, dict[str, tuple[str, str]]]:
    """Stage folders of every comparison version of every object.

    Versions are read from the registry next to the Version 1 folder, so a
    test that replaces :func:`_object_stage_paths` keeps working unchanged.
    """
    from backend.app.services.stage_comparison import comparison_versions

    out: dict[str, dict[str, tuple[str, str]]] = {}
    for oid, (stage_a, stage_b) in _object_stage_paths().items():
        legacy_dir = Path(stage_a).parent
        versions = {
            version: (_resolved(left), _resolved(right))
            for version, (left, right) in comparison_versions.stage_paths_for_legacy_dir(legacy_dir).items()
        }
        versions[comparison_versions.LEGACY] = (stage_a, stage_b)
        out[oid] = versions
    return out


def _session_paths(session_id: str) -> tuple[str, str]:
    from backend.app.services.stage_comparison import store

    session = store.get_session(session_id)
    if not session:
        raise ScopeUnresolved(f"session {session_id!r} not found")
    return _resolved(session.get("stage_a_path")), _resolved(session.get("stage_b_path"))


def _owners(stages: tuple[str, str]) -> list[tuple[str, str]]:
    return [
        (oid, version)
        for oid, versions in _object_version_stage_paths().items()
        for version, paths in versions.items()
        if paths == stages
    ]


def object_id_for_session(session_id: str) -> str:
    """Return the registry object that owns a session's stage folders."""
    owners = sorted({oid for oid, _version in _owners(_session_paths(session_id))})
    if len(owners) != 1:
        raise ScopeUnresolved(
            f"session {session_id!r} belongs to {len(owners)} registry objects"
        )
    return require_safe_id(owners[0], "object")


def sessions_for_object(object_id: str, comparison_version: str | None = None) -> list[str]:
    """Sessions of this object's comparison folders.

    ``comparison_version=None`` — every version (ownership checks); a version
    id — only the sessions of that version (object-wide views and lists).
    """
    from backend.app.services.stage_comparison import store

    require_safe_id(object_id, "object")
    versions = _object_version_stage_paths().get(object_id)
    if versions is None:
        return []
    if comparison_version is None:
        wanted = set(versions.values())
    else:
        paths = versions.get(comparison_version)
        wanted = {paths} if paths else set()
    return [
        str(item["id"])
        for item in store.list_sessions()
        if (_resolved(item.get("stage_a_path")), _resolved(item.get("stage_b_path"))) in wanted
    ]
