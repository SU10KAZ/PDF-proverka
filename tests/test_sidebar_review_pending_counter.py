"""Счётчик «Не проверено» считает ПРОЕКТЫ по последней версии с результатами.

Запрос Андрея Ивановича 2026-08-18: «число должно отображаться только тех
проектов, которые не проверены, а если загрузил версию 2, а версия 1 была не
проверена — указывать только последнюю, то есть 1».

До правки бейдж в сайдбаре считал любой проект без двух галочек, включая те,
где аудит вообще не запускался (79 из 114 на живом объекте), и расходился с
«Не проверено (N)» в шапке раздела, у которой была своя формула.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.main import app  # noqa: E402
import backend.app.services.common.object_service as object_service  # noqa: E402
import backend.app.services.common.project_service as project_service  # noqa: E402
from backend.app.services.storage import read_canary as RC  # noqa: E402

client = TestClient(app, raise_server_exceptions=False)

OBJID = "revobj001"
OBJF = "998_ReviewCounter"


def _wj(p: Path, data) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _version(doc_dir: Path, vid: str, vno: int, *, findings: int, reviewed: int) -> None:
    """Версия с `findings` замечаниями, из которых `reviewed` получили вердикт."""
    vd = doc_dir / "versions" / vid
    (vd / "01_input").mkdir(parents=True, exist_ok=True)
    (vd / "01_input" / "doc.pdf").write_text("%PDF-1.4\n", encoding="utf-8")
    _wj(vd / "version.json", {"version_id": vid, "version_no": vno, "label": f"V{vno}"})
    if findings:
        _wj(vd / "03_analysis" / "latest" / "03_findings.json",
            {"findings": [{"id": f"F-{i:03d}"} for i in range(1, findings + 1)]})
    if reviewed:
        _wj(vd / "04_review" / "expert_review.json", {"decisions": [
            {"item_type": "finding", "finding_id": f"F-{i:03d}", "decision": "accepted"}
            for i in range(1, reviewed + 1)
        ]})


def _doc(v2: Path, code: str, versions: list[tuple[int, int]]) -> Path:
    """versions: [(findings, reviewed), ...] — по порядку v001, v002, …"""
    d = v2 / "objects" / OBJF / "disciplines" / "SS" / "documents" / code
    ids = [f"v{i:03d}" for i in range(1, len(versions) + 1)]
    _wj(d / "document.json", {
        "document_code": code, "object_id": OBJID, "discipline": "SS", "kind": "plain",
        "versions": [{"version_id": vid, "version_no": i + 1, "label": f"V{i + 1}"}
                     for i, vid in enumerate(ids)],
        "version_ids": ids,
        "current_version": ids[-1],
    })
    (d / "current_version.txt").write_text(ids[-1] + "\n", encoding="utf-8")
    for i, (findings, reviewed) in enumerate(versions):
        _version(d, ids[i], i + 1, findings=findings, reviewed=reviewed)
    return d


@pytest.fixture
def v2env(tmp_path, monkeypatch):
    data = tmp_path
    v2 = data / "projects_v2"
    (data / "projects").mkdir(parents=True, exist_ok=True)
    _wj(v2 / "objects" / OBJF / "object.json",
        {"object_id": OBJID, "display_name": "998 Review", "folder_name": OBJF})

    # V1 не проверена, поверх загружена V2 без аудита → ЖДЁТ проверки (счёт 1)
    _doc(v2, "PENDING-FROM-V1", [(5, 0), (0, 0)])
    # V1 проверена целиком, V2 без аудита → проверять нечего
    _doc(v2, "DONE-THEN-NEW", [(5, 5), (0, 0)])
    # текущая версия с результатами и без вердиктов → ждёт проверки
    _doc(v2, "PENDING-CURRENT", [(3, 0)])
    # текущая версия проверена целиком → не ждёт
    _doc(v2, "DONE-CURRENT", [(3, 3)])
    # аудита не было ни разу → в счётчик не попадает
    _doc(v2, "NEVER-AUDITED", [(0, 0)])
    # частично проверенная → ждёт
    _doc(v2, "PARTIAL", [(4, 2)])

    monkeypatch.setenv("AUDIT_DATA_DIR", str(data))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2))
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_CANARY_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    monkeypatch.setattr(object_service, "get_current_object",
                        lambda: {"id": OBJID, "name": "998 Review",
                                 "projects_dir": str(data / "projects")})
    monkeypatch.setattr(project_service, "_load_hidden_projects", lambda: set())
    return v2


def _pending(v2env) -> dict[str, bool]:
    rows = client.get("/api/projects").json()["projects"]
    return {r["project_id"]: r["review_pending"] for r in rows}


def test_new_version_without_audit_keeps_unreviewed_predecessor(v2env):
    """Ключевой случай: V1 не проверена + V2 без аудита = ОДИН непроверенный."""
    pending = _pending(v2env)
    assert pending["PENDING-FROM-V1"] is True


def test_new_version_after_completed_review_is_not_pending(v2env):
    pending = _pending(v2env)
    assert pending["DONE-THEN-NEW"] is False


def test_current_version_statuses_win_when_it_has_results(v2env):
    pending = _pending(v2env)
    assert pending["PENDING-CURRENT"] is True
    assert pending["DONE-CURRENT"] is False


def test_project_without_any_audit_is_not_counted(v2env):
    pending = _pending(v2env)
    assert pending["NEVER-AUDITED"] is False


def test_partially_reviewed_is_pending(v2env):
    pending = _pending(v2env)
    assert pending["PARTIAL"] is True


def test_counter_total(v2env):
    """Итог по разделу: 3 из 6 (PENDING-FROM-V1, PENDING-CURRENT, PARTIAL)."""
    pending = _pending(v2env)
    assert sum(1 for v in pending.values() if v) == 3


def test_review_status_version_id_points_at_counted_version(v2env):
    rows = {r["project_id"]: r for r in client.get("/api/projects").json()["projects"]}
    # счёт взят с предыдущей версии — она и указана
    assert rows["PENDING-FROM-V1"]["review_status_version_id"] == "v1"
    assert rows["PENDING-FROM-V1"]["version_id"] == "v2"
    # текущая версия с результатами — указана она
    assert rows["PENDING-CURRENT"]["review_status_version_id"] == "v1"


# ── ключ логического проекта: счётчики считают уникальные проекты ──────────

def test_base_project_key_strips_version_suffix():
    """Карточки «X V1» / «X_V1» / «X» — один логический проект."""
    from backend.app.services.common.project_service import base_project_key

    assert (base_project_key("13АВ-РД-ВК2-К6 V1")
            == base_project_key("13АВ-РД-ВК2-К6_V1")
            == base_project_key("13АВ-РД-ВК2-К6"))
    assert base_project_key("13АВ-РД-ВК1-К2 V2") == base_project_key("13АВ-РД-ВК1-К2")
    assert base_project_key("13АВ-РД-АК-К5_(Книга_1)_V2") == "13ав-рд-ак-к5_(книга_1)"


def test_base_project_key_keeps_cyrillic_shifr():
    """Кириллическая «В» с цифрой — часть шифра раздела, а не версия.

    «133-23-ГК-ОВ3» не должен схлопнуться с «133-23-ГК-О»: снятие кириллического
    суффикса склеило бы разные проекты (на корпусе 522 карточек такой случай
    ровно один, и он реальный).
    """
    from backend.app.services.common.project_service import base_project_key

    assert base_project_key("133-23-ГК-ОВ3") == "133-23-гк-ов3"
    # разделитель обязателен: «…ХV1» без пробела/подчёркивания не трогаем
    assert base_project_key("ABCV1") == "abcv1"


def test_api_returns_base_project_key(v2env):
    rows = {r["project_id"]: r for r in client.get("/api/projects").json()["projects"]}
    assert rows["PENDING-CURRENT"]["base_project_key"] == "pending-current"


def test_version_cards_collapse_into_one_project(v2env, tmp_path):
    """Две карточки одного проекта («X» и «X V2») дают один уникальный проект."""
    from backend.app.services.common.project_service import base_project_key

    _doc(v2env, "DUP-CARD", [(2, 0)])
    _doc(v2env, "DUP-CARD V2", [(2, 0)])
    rows = client.get("/api/projects").json()["projects"]
    keys = {r["base_project_key"] for r in rows}
    ids = {r["project_id"] for r in rows}
    assert {"DUP-CARD", "DUP-CARD V2"} <= ids          # карточки видны обе
    assert len(keys) == len(ids) - 1                    # а проект считается один
    assert base_project_key("DUP-CARD V2") in keys
