"""
test_static_parity.py
---------------------
Защита от drift между frontend/static и webapp/static.

После Pass 14 единственный source of truth для UI — `frontend/`. webapp/static
держит копию для legacy webapp.main:app (production 8081). Любая фронтовая
правка должна синхронизироваться в обе папки до cutover на backend.app.main:app.

Эти тесты предотвращают регрессию: если кто-то правит только одну сторону,
тест падает и указывает где drift.

Run:
    python -m pytest tests/test_static_parity.py -v
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent

# Пары (frontend path, webapp path), которые ОБЯЗАНЫ совпадать байт-в-байт.
# Если frontend меняет что-то здесь — должен синхронизировать webapp.
_PAIRS = [
    (_ROOT / "frontend" / "static" / "js" / "app.js",
     _ROOT / "webapp" / "static" / "js" / "app.js"),
    (_ROOT / "frontend" / "static" / "js" / "version_api.js",
     _ROOT / "webapp" / "static" / "js" / "version_api.js"),
    (_ROOT / "frontend" / "static" / "css" / "styles.css",
     _ROOT / "webapp" / "static" / "css" / "styles.css"),
    (_ROOT / "frontend" / "index.html",
     _ROOT / "webapp" / "static" / "index.html"),
]


def _hash(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.mark.parametrize("frontend_path,webapp_path", _PAIRS, ids=lambda p: p.name)
def test_static_file_parity(frontend_path: Path, webapp_path: Path):
    """Каждая пара должна совпадать байт-в-байт.

    Failure означает drift: frontend и webapp UI разойдутся, и любой production
    cutover legacy → backend сломает что-то в UI (либо новые фичи не появятся,
    либо старые исчезнут). Решение — синхронизировать файл из frontend в webapp.
    """
    assert frontend_path.exists(), f"Missing frontend file: {frontend_path}"
    assert webapp_path.exists(), f"Missing webapp file: {webapp_path}"

    f_hash = _hash(frontend_path)
    w_hash = _hash(webapp_path)

    assert f_hash == w_hash, (
        f"Static drift: {frontend_path.name}\n"
        f"  frontend: {f_hash}\n"
        f"  webapp:   {w_hash}\n"
        f"  → синхронизируйте: cp {frontend_path} {webapp_path}"
    )
