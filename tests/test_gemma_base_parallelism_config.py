"""reserc.md #16 — GEMMA_BASE_PARALLELISM конфигурируемый (default 1 = backward-safe).

Раньше parallelism base-прохода Gemma был захардкожен =1 в runner. Теперь читается
из config (env GEMMA_BASE_PARALLELISM), default 1 — поведение не меняется.
High-detail 300 DPI остаётся 1.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]


def test_default_is_one_backward_safe():
    from backend.app.core.config import GEMMA_BASE_PARALLELISM
    assert GEMMA_BASE_PARALLELISM == 1
    assert isinstance(GEMMA_BASE_PARALLELISM, int)


def test_runner_uses_config_symbol():
    # Runner должен ссылаться на конфиг-символ, а не на хардкод.
    src = (_REPO / "backend/app/pipeline/stages/gemma_enrichment/runner.py").read_text(
        encoding="utf-8"
    )
    assert "GEMMA_BASE_PARALLELISM" in src
    assert "parallelism=1" not in src  # старый хардкод убран


def _value_under_env(env_val: str) -> str:
    """Реально импортнуть config в отдельном процессе с заданным env."""
    code = (
        "from backend.app.core.config import GEMMA_BASE_PARALLELISM as p; "
        "print('VAL=' + str(p))"
    )
    env = {**os.environ, "GEMMA_BASE_PARALLELISM": env_val}
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True,
        env=env, cwd=str(_REPO),
    )
    line = [l for l in out.stdout.splitlines() if l.startswith("VAL=")]
    assert line, f"no VAL output; stderr={out.stderr[-500:]}"
    return line[-1].split("=", 1)[1].strip()


def test_env_override():
    assert _value_under_env("4") == "4"


def test_env_clamp_to_min_one():
    assert _value_under_env("0") == "1"   # clamp
    assert _value_under_env("") == "1"    # пусто → default
