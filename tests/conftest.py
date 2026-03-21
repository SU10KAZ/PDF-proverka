"""Common pytest fixtures for this repository."""

import os
import re
import shutil
import uuid
from pathlib import Path

import pytest


ROOT_DIR = Path(__file__).resolve().parent.parent
os.environ.setdefault("AUDIT_BASE_DIR", str(ROOT_DIR))

_TEST_TMP_ROOT = ROOT_DIR / ".codex_test_tmp"


def _safe_node_id(node_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", node_id).strip("._-")
    return cleaned[:80] or "test"


def _cleanup_tmp_dir(path: Path):
    try:
        shutil.rmtree(path, ignore_errors=True)
    except OSError:
        pass


@pytest.fixture
def tmp_path(request):
    """Workspace-local tmp_path without pytest tmpdir internals.

    The standard pytest tempdir factory is flaky in this sandbox and can create
    directories that immediately become inaccessible. A plain mkdir-based
    fixture is much more stable for this repository.
    """
    _TEST_TMP_ROOT.mkdir(parents=True, exist_ok=True)
    tmp_dir = _TEST_TMP_ROOT / (
        f"{_safe_node_id(request.node.nodeid)}-{uuid.uuid4().hex[:8]}"
    )
    tmp_dir.mkdir(parents=True, exist_ok=False)
    try:
        yield tmp_dir
    finally:
        _cleanup_tmp_dir(tmp_dir)
