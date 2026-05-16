"""
Tests for the read-only mtime-based cache in critic_v2_ui router.

Контекст:
  critic_v2_triage_ui.json — единый файл со всеми проектами; без кеша каждый
  GET /api/critic-v2/projects/{id}/triage-ui перечитывал, парсил и линейно
  сканировал items. Когда обычная вкладка "Замечания" догружает Critic v2
  параллельно, это тормозило первую загрузку. Добавили mtime-based cache.

Что проверяем:
  1) первый запрос реально читает файл с диска;
  2) повторный запрос НЕ читает файл (по той же mtime) — берёт из кеша;
  3) изменение содержимого файла (изменение mtime) инвалидирует кеш;
  4) project-scoped response не меняет shape после внедрения кеша;
  5) endpoint остаётся read-only (artifact mtime не меняется);
  6) endpoint не импортирует production pipeline или LLM SDK.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


ARTIFACT_FIXTURE = {
    "summary": {
        "total": 3,
        "primary_count": 2,
        "needs_context_count": 0,
        "suggested_reject_count": 1,
        "hidden_by_critic_count": 0,
        "primary_queue_reduction_percent": 33.3,
        "accepted_not_hidden_recall": 1.0,
        "accepted_primary_visible_recall": 1.0,
        "profile": "conservative",
        "experimental": True,
    },
    "tabs": [
        {"key": "primary", "title": "Основная", "default_open": True,
         "queues": ["strong_keep"], "count": 2},
        {"key": "needs_context", "title": "Контекст",
         "default_open": False, "queues": ["needs_context"], "count": 0},
        {"key": "suggested_reject", "title": "К отклонению",
         "default_open": False, "queues": ["suggested_reject"], "count": 1},
        {"key": "hidden_by_critic", "title": "Скрыто",
         "default_open": False, "queues": ["hidden"], "count": 0},
    ],
    "items": [
        {"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
         "title": "t1", "tab": "primary", "queue": "strong_keep"},
        {"finding_id": "P1:F-002", "project_name": "P1", "section": "AR",
         "title": "t2", "tab": "suggested_reject", "queue": "suggested_reject"},
        {"finding_id": "P2:F-001", "project_name": "P2", "section": "EOM",
         "title": "t3", "tab": "primary", "queue": "strong_keep"},
    ],
}


@pytest.fixture
def artifact_path(tmp_path: Path) -> Path:
    p = tmp_path / "critic_v2_triage_ui.json"
    p.write_text(json.dumps(ARTIFACT_FIXTURE, ensure_ascii=False), encoding="utf-8")
    return p


@pytest.fixture
def fresh_module(artifact_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Reset _CACHE before each test so tests are independent."""
    monkeypatch.setenv("CRITIC_V2_UI_EXPORT_PATH", str(artifact_path))
    from backend.app.api.routers import critic_v2_ui as mod
    # Clear cache: the module is reused across tests in the same pytest session.
    mod._CACHE["key"] = None
    mod._CACHE["artifact"] = None
    mod._CACHE["items_by_exact"] = {}
    mod._CACHE["items_by_normalized"] = {}
    return mod


@pytest.fixture
def client(fresh_module) -> TestClient:
    app = FastAPI()
    app.include_router(fresh_module.router)
    return TestClient(app)


# ─── Cache behavior ─────────────────────────────────────────────────────────


def test_first_request_reads_file_from_disk(
    client: TestClient, fresh_module, artifact_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """First request hits the filesystem: cache is empty, so we MUST read."""
    assert fresh_module._CACHE["artifact"] is None  # precondition: cache cold

    read_calls = {"n": 0}
    real_read_text = Path.read_text

    def counting_read(self, *args, **kwargs):
        if self == artifact_path:
            read_calls["n"] += 1
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read)

    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r.status_code == 200
    assert read_calls["n"] == 1
    # Cache primed
    assert fresh_module._CACHE["artifact"] is not None
    assert "P1" in fresh_module._CACHE["items_by_exact"]


def test_second_request_uses_cache(
    client: TestClient, fresh_module, artifact_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Same file (same mtime/size) → no second read."""
    # warm-up
    client.get("/api/critic-v2/projects/P1/triage-ui")

    read_calls = {"n": 0}
    real_read_text = Path.read_text

    def counting_read(self, *args, **kwargs):
        if self == artifact_path:
            read_calls["n"] += 1
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read)

    # Multiple subsequent calls — must NOT re-read.
    for _ in range(5):
        r = client.get("/api/critic-v2/projects/P1/triage-ui")
        assert r.status_code == 200

    assert read_calls["n"] == 0, (
        f"Expected 0 reads after cache warm-up, got {read_calls['n']}"
    )


def test_mtime_change_invalidates_cache(
    client: TestClient, fresh_module, artifact_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """When the file is rewritten with new content, cache must invalidate."""
    # warm-up
    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert len(r.json()["items"]) == 2

    # Rewrite artifact with NEW content — only one project P1 with 1 item
    new_fixture = json.loads(json.dumps(ARTIFACT_FIXTURE))
    new_fixture["items"] = [
        {"finding_id": "P1:F-NEW", "project_name": "P1", "section": "AR",
         "title": "after-invalidate", "tab": "primary", "queue": "strong_keep"},
    ]
    artifact_path.write_text(json.dumps(new_fixture), encoding="utf-8")
    # Force mtime forward (some filesystems have low-resolution mtime)
    st = artifact_path.stat()
    new_mtime = st.st_mtime + 5
    os.utime(artifact_path, (new_mtime, new_mtime))

    read_calls = {"n": 0}
    real_read_text = Path.read_text

    def counting_read(self, *args, **kwargs):
        if self == artifact_path:
            read_calls["n"] += 1
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read)

    r2 = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r2.status_code == 200
    body = r2.json()
    assert read_calls["n"] == 1, "mtime change must trigger re-read"
    assert len(body["items"]) == 1
    assert body["items"][0]["finding_id"] == "P1:F-NEW"


def test_artifact_disappearing_invalidates_cache(
    client: TestClient, fresh_module, artifact_path: Path,
):
    """If the file is deleted, cache must drop and 404 must be raised."""
    # warm-up
    r = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r.status_code == 200
    assert fresh_module._CACHE["artifact"] is not None

    artifact_path.unlink()

    r2 = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r2.status_code == 404
    assert fresh_module._CACHE["artifact"] is None
    assert fresh_module._CACHE["items_by_exact"] == {}


# ─── Response shape stability ───────────────────────────────────────────────


def test_response_shape_unchanged_after_cache(client: TestClient):
    """Cached path must yield exactly the same response as a cold path."""
    r1 = client.get("/api/critic-v2/projects/P1/triage-ui")
    r2 = client.get("/api/critic-v2/projects/P1/triage-ui")
    assert r1.status_code == 200 == r2.status_code
    assert r1.json() == r2.json()


def test_normalized_match_uses_index(client: TestClient):
    """The cached normalized index must yield the same matched_by behavior."""
    # P1 lookup with case/space variation (normalized match)
    r = client.get("/api/critic-v2/projects/  p1  /triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["scope"]["matched_by"] == "normalized"
    assert len(body["items"]) == 2


def test_pdf_suffix_match_uses_index(client: TestClient):
    """A request with .pdf suffix must still strip + match via the cache."""
    r = client.get("/api/critic-v2/projects/P1.pdf/triage-ui")
    assert r.status_code == 200
    body = r.json()
    assert body["scope"]["matched_by"] == "project_name_no_pdf"
    assert len(body["items"]) == 2


def test_unknown_project_does_not_pollute_cache(
    client: TestClient, fresh_module, artifact_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """Unknown project lookup is a cache HIT for the underlying artifact —
    file must not be re-read once it's already cached."""
    client.get("/api/critic-v2/projects/P1/triage-ui")  # warm
    read_calls = {"n": 0}
    real_read_text = Path.read_text

    def counting_read(self, *args, **kwargs):
        if self == artifact_path:
            read_calls["n"] += 1
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting_read)

    r = client.get("/api/critic-v2/projects/UNKNOWN_PROJECT/triage-ui")
    assert r.status_code == 200
    assert "warning" in r.json()
    assert read_calls["n"] == 0


# ─── Read-only contract preserved ───────────────────────────────────────────


def test_endpoint_does_not_write_artifact_under_cache_load(
    client: TestClient, artifact_path: Path,
):
    """Repeated calls must not touch the artifact file."""
    before = artifact_path.stat().st_mtime_ns
    for _ in range(20):
        client.get("/api/critic-v2/projects/P1/triage-ui")
        client.get("/api/critic-v2/projects/P2/triage-ui")
        client.get("/api/critic-v2/projects/UNKNOWN/triage-ui")
        client.get("/api/critic-v2/artifact-info")
    assert artifact_path.stat().st_mtime_ns == before


def test_router_does_not_import_pipeline_or_llm():
    """Sanity guard for the cached version — ensure no production wiring crept in."""
    src = (Path(_PROJECT_ROOT) / "backend" / "app" / "api" / "routers"
           / "critic_v2_ui.py").read_text(encoding="utf-8")
    forbidden = [
        "from backend.app.pipeline",
        "import backend.app.pipeline",
        "from backend.app.services.findings",
        "anthropic",
        "openai",
    ]
    for token in forbidden:
        assert token not in src, f"Forbidden import found in router: {token}"


def test_cache_index_built_from_items(fresh_module, client: TestClient):
    """After a cold request, the cache index must be populated."""
    client.get("/api/critic-v2/projects/P1/triage-ui")
    assert "P1" in fresh_module._CACHE["items_by_exact"]
    assert "P2" in fresh_module._CACHE["items_by_exact"]
    assert "p1" in fresh_module._CACHE["items_by_normalized"]
    assert len(fresh_module._CACHE["items_by_exact"]["P1"]) == 2
    assert len(fresh_module._CACHE["items_by_exact"]["P2"]) == 1
