"""Canonical cross-contour identities for employees with legacy aliases.

Most employees still use the historical per-contour identifiers.  Entries in
this registry are deliberately opt-in: they provide one stable employee ID to
usage statistics, the portal user projection, and the production schedule
without rewriting append-only decisions history.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(frozen=True)
class EmployeeIdentity:
    employee_id: str
    display_name: str
    surname: str
    initials: str
    portal_user_id: str
    portal_login: str
    source_markers: tuple[str, ...]
    reviewer_aliases: tuple[str, ...] = ()


_IDENTITIES = (
    EmployeeIdentity(
        employee_id="maksheev",
        display_name="Макшеев П.Ю.",
        surname="Макшеев",
        initials="П.Ю.",
        portal_user_id="maksheev",
        portal_login="pavel",
        source_markers=("OSA-Maksheev",),
        # Existing production decisions were attributed before the initials
        # correction.  Keep that spelling as an input alias, never as output.
        reviewer_aliases=("Макшеев П.",),
    ),
)


def _norm(value: object) -> str:
    return " ".join(str(value or "").split()).casefold()


def identity_for_portal_user(user: Mapping[str, object]) -> Optional[EmployeeIdentity]:
    """Resolve a canonical identity from an existing users.json record."""
    user_id = _norm(user.get("id"))
    login = _norm(user.get("login"))
    for identity in _IDENTITIES:
        if (
            user_id == _norm(identity.portal_user_id)
            or login == _norm(identity.portal_login)
        ):
            return identity
    return None


def identity_for_source_directory(dirname: str) -> Optional[EmployeeIdentity]:
    """Resolve an encoded Claude project directory to an employee."""
    folded = str(dirname or "").casefold()
    for identity in _IDENTITIES:
        if any(marker.casefold() in folded for marker in identity.source_markers):
            return identity
    return None


def identity_for_reviewer(reviewer: str) -> Optional[EmployeeIdentity]:
    """Resolve canonical and legacy reviewer spellings from decisions history."""
    needle = _norm(reviewer)
    if not needle:
        return None
    for identity in _IDENTITIES:
        candidates = (identity.display_name, *identity.reviewer_aliases)
        if needle in {_norm(candidate) for candidate in candidates}:
            return identity
    return None


def canonicalize_user_record(user: Mapping[str, object]) -> dict:
    """Return the public canonical projection without mutating users.json."""
    result = dict(user)
    identity = identity_for_portal_user(result)
    if identity is None:
        return result
    result.update({
        "id": identity.portal_user_id,
        "login": identity.portal_login,
        "surname": identity.surname,
        "initials": identity.initials,
        "name": identity.display_name,
        "employee_id": identity.employee_id,
    })
    return result


def reviewer_matches_user(reviewer: str, user: Mapping[str, object]) -> bool:
    """Match current and legacy decisions to the same portal employee."""
    needle = _norm(reviewer)
    if not needle:
        return False
    identity = identity_for_portal_user(user)
    candidates = {
        _norm(user.get("name")),
        _norm(user.get("id")),
        _norm(user.get("surname")),
    }
    if identity is not None:
        candidates.update({
            _norm(identity.employee_id),
            _norm(identity.display_name),
            *(_norm(alias) for alias in identity.reviewer_aliases),
        })
    candidates.discard("")
    return needle in candidates


__all__ = [
    "EmployeeIdentity",
    "canonicalize_user_record",
    "identity_for_portal_user",
    "identity_for_reviewer",
    "identity_for_source_directory",
    "reviewer_matches_user",
]
