from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/projects_v2/cleanup_pytest_pollution.py"
SPEC = importlib.util.spec_from_file_location("cleanup_pytest_pollution", SCRIPT)
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_cleanup_backs_up_and_removes_only_synthetic_data(tmp_path):
    v2 = tmp_path / "projects_v2"
    real = v2 / "objects" / "real_object"
    fake = v2 / "objects" / "test_case"
    disguised = v2 / "objects" / "214_Obj"
    (real / "keep").mkdir(parents=True)
    (fake / "remove").mkdir(parents=True)
    disguised.mkdir(parents=True)
    (real / "keep" / "data.txt").write_text("real", encoding="utf-8")
    (fake / "remove" / "data.txt").write_text("fake", encoding="utf-8")
    _write_json(disguised / "object.json", {
        "legacy_path": "/tmp/pytest-of-user/pytest-2/214. Obj",
    })
    entries = [
        {
            "legacy_folder_path": "/srv/projects/REAL",
            "v2_document_dir": str(real / "disciplines/AR/documents/REAL"),
        },
        {
            "legacy_folder_path": "/tmp/pytest-of-user/pytest-1/FAKE",
            "v2_document_dir": str(fake / "disciplines/AR/documents/FAKE"),
        },
        {
            "legacy_folder_path": "/srv/projects/PSEUDO",
            "v2_document_dir": str(disguised / "disciplines/KM/documents/PSEUDO"),
        },
    ]
    _write_json(v2 / "_system" / "old_to_new_map.json", {"migrations": entries})

    backup = tmp_path / "backup"
    result = cleanup.apply_cleanup(v2, backup)

    assert result["removed_entries"] == 2
    assert result["remaining_entries"] == 1
    assert result["removed_object_dirs"] == ["214_Obj", "test_case"]
    assert real.is_dir()
    assert not fake.exists()
    assert not disguised.exists()
    assert (backup / "old_to_new_map.before.json").is_file()
    assert (backup / "pytest_objects.tar.gz").is_file()
    current = json.loads((v2 / "_system" / "old_to_new_map.json").read_text())
    assert current["migrations"] == entries[:1]
