"""Temporary Codex model substitution, switched by a data file at runtime.

20.10.2026: the provider answered ``429 Too Many Requests`` to every
``gpt-5.6-sol`` call while ``gpt-6-sol`` and ``gpt-6-astra`` on the same
account worked. Stage 01 aborts on a failed leg, so every audit stopped.
The service env files are root-only, and the Sol model id is wired as a
constant in several places (ensemble mode check, leg labels), so the switch
lives at the one point where the id reaches the CLI: ``codex exec --model``.

File ``APP_DATA_DIR/codex_model_substitution.json``::

    {"substitutions": {"gpt-5.6-sol": "gpt-6-sol"}, "reason": "..."}

No file, an unreadable file or an invalid entry means no substitution.
Pipeline labels keep the requested id, so leg contracts are unchanged; every
substituted call is logged as ``codex_model_substituted``. Delete the file to
switch back; the file is read on each call, no restart is needed.
"""
from __future__ import annotations

import json
import logging
import re

from backend.app.core.config import APP_DATA_DIR

logger = logging.getLogger(__name__)

SETTINGS_FILE = APP_DATA_DIR / "codex_model_substitution.json"
_MODEL_ID_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{0,63}$")


def get_substitutions() -> dict[str, str]:
    try:
        raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8")).get("substitutions")
    except (OSError, ValueError, AttributeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        src: dst
        for src, dst in raw.items()
        if isinstance(src, str) and isinstance(dst, str)
        and _MODEL_ID_RE.match(src) and _MODEL_ID_RE.match(dst) and src != dst
    }


def substitute_codex_model(resolved_model: str, *, label: str = "") -> str:
    """Return the CLI model id to run instead of ``resolved_model``."""
    actual = get_substitutions().get(resolved_model)
    if not actual:
        return resolved_model
    logger.warning(
        "codex_model_substituted: %s -> %s (%s); switch: %s",
        resolved_model, actual, label or "-", SETTINGS_FILE,
    )
    return actual
