"""reserc.md #7/#81/#87 — общий atomic_write_json для shared-стораджей."""
from __future__ import annotations

import json
import threading

from backend.app.services.common.atomic_json import atomic_write_json


def test_round_trip(tmp_path):
    p = tmp_path / "x.json"
    payload = {"a": 1, "кир": [1, 2, 3], "nested": {"k": "v"}}
    atomic_write_json(p, payload)
    assert json.loads(p.read_text(encoding="utf-8")) == payload


def test_no_tmp_leftover(tmp_path):
    p = tmp_path / "x.json"
    atomic_write_json(p, {"a": 1})
    assert not (tmp_path / "x.json.tmp").exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_overwrite_is_atomic(tmp_path):
    p = tmp_path / "x.json"
    atomic_write_json(p, {"v": 1})
    atomic_write_json(p, {"v": 2})
    assert json.loads(p.read_text(encoding="utf-8")) == {"v": 2}


def test_concurrent_writers_no_corruption(tmp_path):
    p = tmp_path / "x.json"

    def w(n: int) -> None:
        for _ in range(25):
            atomic_write_json(p, {"n": n, "data": list(range(200))})

    threads = [threading.Thread(target=w, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Под локом + os.replace файл всегда валиден (не полу-записан) и нет .tmp.
    d = json.loads(p.read_text(encoding="utf-8"))
    assert "n" in d and len(d["data"]) == 200
    assert list(tmp_path.glob("*.tmp")) == []
