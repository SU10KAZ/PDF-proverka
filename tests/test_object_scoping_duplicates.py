"""Одинаковые шифры в разных объектах не смешиваются (план доступа, этап 6;
поправка 0.4). Временное хранилище: объекты A и B, в каждом EOM/ЭМ-1 v001
с разным числом замечаний (2 и 5) и разным PDF."""
from __future__ import annotations

import io
import json
import zipfile
from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from tests.test_viewer_no_side_effects import CODE, OBJ_A, OBJ_B, world  # noqa: F401 — фикстура

PID = quote(f"EOM/{CODE}", safe="")


@pytest.fixture
def client(world):  # noqa: F811
    from backend.app.main import app
    c = TestClient(app, headers={"X-Requested-With": "AuditManager"}, raise_server_exceptions=False)
    assert c.post("/api/auth/login", json={"username": "reader", "password": "reader-password-1"}).status_code == 200
    return c


def _count(resp):
    assert resp.status_code == 200, resp.text[:300]
    body = resp.json()
    findings = body.get("findings") if isinstance(body, dict) else body
    return len(findings or [])


def test_findings_by_header_and_query(client):
    assert _count(client.get(f"/api/findings/{PID}", headers={"X-Object-Id": OBJ_A})) == 2
    assert _count(client.get(f"/api/findings/{PID}", headers={"X-Object-Id": OBJ_B})) == 5
    assert _count(client.get(f"/api/findings/{PID}?object_id={OBJ_B}")) == 5


def test_conflicting_object_ids_rejected(client):
    r = client.get(f"/api/findings/{PID}?object_id={OBJ_B}", headers={"X-Object-Id": OBJ_A})
    assert r.status_code == 400 and r.json()["detail"]["code"] == "object_conflict"
    r = client.get(f"/api/version-comparison/objects/{OBJ_A}/catalog", headers={"X-Object-Id": OBJ_B})
    assert r.status_code == 400 and r.json()["detail"]["code"] == "object_conflict"


def test_unknown_object_rejected(client):
    r = client.get(f"/api/findings/{PID}", headers={"X-Object-Id": "ffffffff"})
    assert r.status_code == 404 and r.json()["detail"]["code"] == "unknown_object"
    r = client.get("/api/version-comparison/objects/ffffffff/catalog")
    assert r.status_code == 404 and r.json()["detail"]["code"] == "unknown_object"


def test_no_object_and_ambiguous_code_is_409_not_first_match(client):
    r = client.get(f"/api/findings/{PID}")
    assert r.status_code == 409 and r.json()["detail"]["code"] == "ambiguous_document"


def test_document_missing_in_explicit_object_is_404(world, client):  # noqa: F811
    import shutil
    shutil.rmtree(world["tmp"] / "projects_v2" / "objects" / f"obj_{OBJ_B}" / "disciplines")
    r = client.get(f"/api/findings/{PID}", headers={"X-Object-Id": OBJ_B})
    assert r.status_code == 404                      # не «первое совпадение» из A


def test_pdf_and_summary_are_per_object(world, client):  # noqa: F811
    pdf_a = client.get(f"/api/document/{PID}/pdf?object_id={OBJ_A}")
    pdf_b = client.get(f"/api/document/{PID}/pdf?object_id={OBJ_B}")
    assert pdf_a.status_code == pdf_b.status_code == 200
    assert pdf_a.content != pdf_b.content
    expected_a = (world["tmp"] / "projects_v2" / "objects" / f"obj_{OBJ_A}" / "disciplines" / "EOM"
                  / "documents" / CODE / "versions" / "v001" / "02_work" / "document.pdf").read_bytes()
    assert pdf_a.content == expected_a
    summary = client.get("/api/findings/summary", headers={"X-Object-Id": OBJ_B}).json()["summaries"]
    assert [s["total"] for s in summary] == [5]


def test_audit_package_is_per_object(client):
    for oid, n in ((OBJ_A, 2), (OBJ_B, 5)):
        r = client.get(f"/api/export/audit-package/{PID}", headers={"X-Object-Id": oid})
        assert r.status_code == 200, r.text[:300]
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            names = zf.namelist()
            findings = next(n_ for n_ in names if n_.endswith("03_findings.json"))
            assert len(json.loads(zf.read(findings))["findings"]) == n


def test_stage_session_of_other_object_is_404(world, client):  # noqa: F811
    sid = world["session_id"]
    ok = client.get(f"/api/stage-comparison/sessions/{sid}", headers={"X-Object-Id": OBJ_A})
    assert ok.status_code == 200
    other = client.get(f"/api/stage-comparison/sessions/{sid}", headers={"X-Object-Id": OBJ_B})
    assert other.status_code == 404 and other.json()["detail"]["code"] == "session_not_in_object"
    found = client.get(f"/api/stage-comparison/objects/{OBJ_A}/session")
    assert found.status_code == 200 and found.json()["session_id"] == sid
    none = client.get(f"/api/stage-comparison/objects/{OBJ_B}/session")
    assert none.status_code == 404 and none.json()["detail"]["code"] == "not_prepared"


def test_legacy_storage_forcing_is_admin_only(client):
    r = client.get(f"/api/findings/{PID}?storage=legacy", headers={"X-Object-Id": OBJ_A})
    assert r.status_code == 403
