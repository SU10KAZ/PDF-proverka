"""Persistent package-level decisions for section optimisation.

The portfolio is recalculated from the snapshot on every read.  Only the
engineer's package economics and one final verdict are persisted here; the
snapshot and individual dossiers remain the source of technical evidence.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from backend.app.services.section_optimization_pipeline_service import section_data_dir


def _path(section: str, object_id: str | None) -> Path:
    return section_data_dir(section, object_id=object_id) / "optimization_portfolio.json"


def _read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def load_portfolio_state(section: str, *, object_id: str | None = None) -> dict:
    return _read(_path(section, object_id))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_package_evaluation(
    section: str,
    package_id: str,
    evaluation: dict[str, Any],
    *,
    object_id: str | None = None,
    expected_fingerprint: str = "",
    package_fingerprint: str = "",
    reviewer: str = "",
) -> dict:
    if expected_fingerprint and expected_fingerprint != package_fingerprint:
        raise ValueError("Пакет изменился: обновите страницу и повторите расчёт")
    state = load_portfolio_state(section, object_id=object_id)
    evaluations = state.setdefault("evaluations", {})
    clean = {key: value for key, value in evaluation.items() if value is not None}
    clean.update({"package_fingerprint": package_fingerprint, "reviewer": reviewer, "updated_at": _now()})
    evaluations[package_id] = clean
    # A new calculation invalidates a previous verdict for the same package.
    state.setdefault("decisions", {}).pop(package_id, None)
    _write(_path(section, object_id), state)
    return deepcopy(clean)


def save_package_verdict(
    section: str,
    package_id: str,
    decision: str,
    *,
    object_id: str | None = None,
    expected_fingerprint: str = "",
    package_fingerprint: str = "",
    reviewer: str = "",
    note: str = "",
    reason_code: str = "",
    decision_basis: dict | None = None,
) -> dict:
    if expected_fingerprint and expected_fingerprint != package_fingerprint:
        raise ValueError("Пакет изменился: обновите страницу и повторите решение")
    if decision not in {"economic_yes", "economic_no", "needs_data"}:
        raise ValueError("Недопустимое решение пакета")
    if decision in {"economic_no", "needs_data"} and not str(note or "").strip():
        raise ValueError("Для этого решения укажите причину")
    state = load_portfolio_state(section, object_id=object_id)
    result = {
        "decision": decision,
        "reason_code": reason_code or ("insufficient_data" if decision == "needs_data" else ""),
        "note": " ".join(str(note or "").split())[:3000],
        "reviewer": " ".join(str(reviewer or "").split())[:300],
        "decided_at": _now(),
        "package_fingerprint": package_fingerprint,
        "decision_basis": deepcopy(decision_basis or {}),
    }
    state.setdefault("decisions", {})[package_id] = result
    _write(_path(section, object_id), state)
    return deepcopy(result)


__all__ = ["load_portfolio_state", "save_package_evaluation", "save_package_verdict"]
