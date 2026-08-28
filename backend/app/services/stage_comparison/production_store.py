"""Durable artifacts for the additive Stage Comparison production flow.

The production pipeline deliberately does not reuse the legacy Stage 5/5.3
files.  Every logical boundary has its own JSON document under
``pairs/<pair_id>/production``.  Human-written artifacts use a cross-process
read/modify/write lock; producer artifacts use the shared atomic writer.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

try:  # pragma: no cover - Linux production path; local fallback stays safe.
    import fcntl as _fcntl
except Exception:  # pragma: no cover
    _fcntl = None

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from . import paths
from .engineer_review import build_engineer_decisions
from .graphic_comparison import validate_ledger
from .unified_change_synthesizer import (
    canonical_synthesis_digest,
    validate_synthesis,
)


ARTIFACT_PATHS: dict[str, Callable[[str, str], Path]] = {
    "state": paths.production_state_path,
    "sheet_relations": paths.production_sheet_relations_path,
    "text_preparation": paths.production_text_preparation_path,
    "text_differences": paths.production_text_differences_path,
    "text_fact_production": paths.production_text_fact_production_path,
    "text_semantic_validation": paths.production_text_semantic_validation_path,
    "text_atoms": paths.production_text_atoms_path,
    "graphic_ledger": paths.production_graphic_ledger_path,
    "source_snapshot": paths.production_source_snapshot_path,
    "entity_relations": paths.production_entity_relations_path,
    "bound_atoms": paths.production_bound_atoms_path,
    "effective_bound_atoms": paths.production_effective_bound_atoms_path,
    "review_questions": paths.production_review_questions_path,
    "review_answers": paths.production_review_answers_path,
    "review_application": paths.production_review_application_path,
    "automatic_unified_synthesis": paths.production_automatic_synthesis_path,
    "unified_synthesis": paths.production_unified_synthesis_path,
    "engineer_decisions": paths.production_engineer_decisions_path,
    "final_report": paths.production_final_report_path,
    "direct_page_mode2": paths.production_direct_page_mode2_path,
    "page_graphic_bundle": paths.production_page_graphic_bundle_path,
    "document_graphic_bundle": paths.production_document_graphic_bundle_path,
}


class ProductionConflictError(ValueError):
    """An optimistic signature/revision no longer describes stored state."""


_pair_locks_guard = threading.Lock()
_pair_locks: dict[str, threading.Lock] = {}


def _local_pair_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _pair_locks_guard:
        return _pair_locks.setdefault(key, threading.Lock())


@contextmanager
def production_pair_lock(session_id: str, pair_id: str) -> Iterator[None]:
    """Serialize a complete run or human mutation for one document pair.

    Per-artifact atomic writes prevent torn JSON, but only this pair-level lock
    prevents two runs from publishing a mixture of generations.  Acquisition
    is deliberately non-blocking so an HTTP worker returns a conflict instead
    of waiting behind a long comparison.
    """
    root = paths.production_dir(session_id, pair_id)
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".pair.lock"
    local = _local_pair_lock(lock_path)
    if not local.acquire(blocking=False):
        raise ProductionConflictError("production pair is being updated")
    stream = None
    try:
        stream = lock_path.open("a+", encoding="utf-8")
        if _fcntl is not None:
            try:
                _fcntl.flock(stream.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ProductionConflictError(
                    "production pair is being updated"
                ) from exc
        yield
    finally:
        if stream is not None:
            try:
                if _fcntl is not None:
                    _fcntl.flock(stream.fileno(), _fcntl.LOCK_UN)
            finally:
                stream.close()
        local.release()


def production_pair_runner_active(session_id: str, pair_id: str) -> bool:
    """Return whether the pair lock is held by a real local/remote runner.

    This is deliberately a lock probe, not a heartbeat heuristic.  The local
    lock covers threads in this process; ``flock`` covers other worker
    processes and is automatically released if their process exits.
    """
    root = paths.production_dir(session_id, pair_id)
    lock_path = root / ".pair.lock"
    local = _local_pair_lock(lock_path)
    if not local.acquire(blocking=False):
        return True
    stream = None
    acquired_file_lock = False
    try:
        if not root.exists() or not lock_path.exists():
            return False
        stream = lock_path.open("a+", encoding="utf-8")
        if _fcntl is None:
            return False
        try:
            _fcntl.flock(stream.fileno(), _fcntl.LOCK_EX | _fcntl.LOCK_NB)
            acquired_file_lock = True
            return False
        except BlockingIOError:
            return True
    finally:
        if stream is not None:
            try:
                if _fcntl is not None and acquired_file_lock:
                    _fcntl.flock(stream.fileno(), _fcntl.LOCK_UN)
            finally:
                stream.close()
        local.release()


def artifact_path(session_id: str, pair_id: str, name: str) -> Path:
    try:
        factory = ARTIFACT_PATHS[name]
    except KeyError as exc:
        raise ValueError(f"unsupported production artifact: {name}") from exc
    return factory(session_id, pair_id)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def load_artifact(session_id: str, pair_id: str, name: str) -> dict[str, Any] | None:
    """Read an already-produced artifact; this function never starts work."""
    return _read_json(artifact_path(session_id, pair_id, name))


def save_artifact(
    session_id: str,
    pair_id: str,
    name: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError("production artifact must be an object")
    value = dict(payload)
    atomic_write_json(artifact_path(session_id, pair_id, name), value)
    return value


def mutate_artifact(
    session_id: str,
    pair_id: str,
    name: str,
    mutate: Callable[[Any], Any],
    *,
    default: Any,
) -> Any:
    """Cross-process safe mutation used for answers, decisions and state."""
    return load_modify_save(
        artifact_path(session_id, pair_id, name),
        mutate,
        default=default,
    )


def save_unified_synthesis(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist the strict G2.4.6 payload with no orchestration envelope."""
    validated = validate_synthesis(dict(synthesis))
    atomic_write_json(paths.production_unified_synthesis_path(session_id, pair_id), validated)
    return validated


def save_graphic_ledger(
    session_id: str,
    pair_id: str,
    ledger: Mapping[str, Any],
) -> dict[str, Any]:
    """Persist a production-private copy of a validated graphic ledger."""
    validated = validate_ledger(dict(ledger))
    atomic_write_json(
        paths.production_graphic_ledger_path(session_id, pair_id), validated
    )
    return validated


def load_graphic_ledger(session_id: str, pair_id: str) -> dict[str, Any] | None:
    value = _read_json(paths.production_graphic_ledger_path(session_id, pair_id))
    if value is None:
        return None
    try:
        return validate_ledger(value)
    except (TypeError, ValueError):
        return None


def update_engineer_decisions(
    session_id: str,
    pair_id: str,
    *,
    synthesis: Mapping[str, Any],
    updates: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    """Upsert decisions under one file lock and override client authorship."""
    if not expected_input_signature:
        raise ProductionConflictError("expected_input_signature is required")
    if expected_revision is None:
        raise ProductionConflictError("expected_revision is required")
    validated = validate_synthesis(dict(synthesis))
    synthesis_signature = canonical_synthesis_digest(validated)
    if (
        expected_input_signature is not None
        and expected_input_signature != synthesis_signature
    ):
        raise ProductionConflictError("production input signature changed")
    normalized_updates = [
        {
            **dict(update),
            "author": author,
        }
        for update in updates
    ]

    def apply(existing: Any) -> dict[str, Any]:
        current = existing if isinstance(existing, Mapping) else None
        current_revision = int((current or {}).get("revision") or 0)
        if expected_revision is not None and expected_revision != current_revision:
            raise ProductionConflictError("engineer decisions revision changed")
        return build_engineer_decisions(
            validated,
            existing=current,
            updates=normalized_updates,
        )

    return mutate_artifact(
        session_id,
        pair_id,
        "engineer_decisions",
        apply,
        default={},
    )


__all__ = [
    "ARTIFACT_PATHS",
    "ProductionConflictError",
    "artifact_path",
    "load_artifact",
    "load_graphic_ledger",
    "mutate_artifact",
    "production_pair_lock",
    "production_pair_runner_active",
    "save_artifact",
    "save_graphic_ledger",
    "save_unified_synthesis",
    "update_engineer_decisions",
]
