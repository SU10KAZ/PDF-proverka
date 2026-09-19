"""scripts/install_release_runtime_deps.py: lock-файл, отказ писать в готовый релиз."""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts import install_release_runtime_deps as deps

REPO = Path(__file__).resolve().parents[1]
LOCK = REPO / "requirements-projectchange-v3-runtime.lock.txt"
H = "0" * 64


def test_committed_lock_pins_the_research_runtime_closure():
    pins = deps.parse_lock(LOCK.read_text(encoding="utf-8"))
    assert {name: pin["version"] for name, pin in pins.items()} == {
        "attrs": "26.1.0",
        "jsonschema": "4.23.0",
        "jsonschema-specifications": "2025.9.1",
        "referencing": "0.37.0",
        "rpds-py": "2026.6.3",
    }
    assert all(len(pin["hashes"]) == 1 for pin in pins.values())


def test_requirements_pin_matches_the_lock():
    lines = (REPO / "requirements.txt").read_text(encoding="utf-8").splitlines()
    assert "jsonschema==4.23.0" in lines


@pytest.mark.parametrize("text, message", [
    ("jsonschema>=4.23.0 --hash=sha256:" + H, "точной версии"),
    ("jsonschema==4.23.0", "sha256"),
    (f"a==1 --hash=sha256:{H}\nA==1 --hash=sha256:{H}", "дважды"),
    ("# only a comment\n", "пуст"),
])
def test_lock_without_exact_hashed_pins_is_refused(text, message):
    with pytest.raises(deps.InstallRefused, match=message):
        deps.parse_lock(text)


def test_continuation_lines_carry_the_hash():
    pins = deps.parse_lock(f"Jsonschema_Specifications==2025.9.1 \\\n    --hash=sha256:{H}\n")
    assert pins == {"jsonschema-specifications": {"version": "2025.9.1", "hashes": [H]}}


def _venv(release: Path) -> Path:
    (release / "venv" / "bin").mkdir(parents=True)
    (release / "venv" / "bin" / "python").write_text("", encoding="utf-8")
    return release / "venv"


def test_finished_and_live_releases_are_never_written(tmp_path):
    root = tmp_path / "auditmanager"
    finished = _venv(root / "releases" / "ui-real-x")
    with pytest.raises(deps.InstallRefused, match="неизменяемы"):
        deps.refuse_live_target(finished, root)
    live = _venv(tmp_path / "elsewhere" / "live")
    os.symlink(live.parent, root / "current")
    with pytest.raises(deps.InstallRefused, match="current"):
        deps.refuse_live_target(live, root)
    staging = _venv(root / "release-staging" / "ui-real-y.partial")
    deps.refuse_live_target(staging, root)  # a release being built is the only target
    with pytest.raises(deps.InstallRefused, match="не venv"):
        deps.refuse_live_target(root / "release-staging" / "missing" / "venv", root)


def test_wheel_outside_the_lock_is_refused_before_any_write(tmp_path, monkeypatch):
    root = tmp_path / "auditmanager"
    monkeypatch.setattr(deps, "AUDITMANAGER_ROOT", root)
    venv = _venv(root / "release-staging" / "r.partial")
    wheelhouse = tmp_path / "wheels"
    wheelhouse.mkdir()
    (wheelhouse / "evil-1.0-py3-none-any.whl").write_bytes(b"not in the lock")
    before = deps.snapshot(venv)
    with pytest.raises(deps.InstallRefused, match="вне lock-файла"):
        deps.install(venv, LOCK, wheelhouse)
    assert deps.snapshot(venv) == before
