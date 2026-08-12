"""Smoke-тесты для finalization-ревизии раздела «Сравнение стадий».

Покрывает:
  • Task 1 — non-blocking batch LLM jobs (rejected_no_confirm + защита от
             двойного запуска через _is_task_alive);
  • Task 3 — отчёты с timestamp_ms+uuid не перезаписываются;
  • Task 4 — manifest reports.json;
  • Task 5 — single graphic-diff endpoint унифицированно обрабатывает
             paid_api_blocked / is_error / success;
  • Task 7 — bulk_patch_findings и list_findings(include_children);
  • Task 9 — compute_warnings() возвращает items+summary.

Тесты используют моки store / llm_runner — без реальных PDF и LLM.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, AsyncMock

import pytest


# ─── env: изолируем COMPARISON_ROOT во временный каталог ─────────────────

@pytest.fixture(autouse=True)
def _tmp_comparison_root(tmp_path, monkeypatch):
    root = tmp_path / "comparison_test"
    root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(root))
    # Inject минимальную сессию
    yield root


# ─── env: изолируем graphic-LLM provider от production .env ──────────────
#
# `backend/app/core/config.py` вызывает `load_dotenv()` при импорте, что
# заливает все `STAGE_COMPARISON_GRAPHIC_LLM_*` из `.env` в окружение.
# Если в `.env` указан `STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER=local_openai_compatible`,
# `graphic_diff_endpoint` уходит в `_graphic_diff_via_local`, который
# делает реальные httpx-запросы в LM Studio (и игнорирует моки
# `llm_runner.run_llm`, на которые опирается test_single_graphic_diff_status_unification).
#
# Чтобы тесты не зависели от настройки локального оператора и не
# обращались к LM Studio, явно сбрасываем всю `STAGE_COMPARISON_GRAPHIC_LLM_*`
# группу. Тесты, которым нужен local-провайдер (см.
# tests/test_stage_comparison_graphic_local_llm.py), выставляют переменные
# заново через свою фикстуру `_local_env`.
@pytest.fixture(autouse=True)
def _isolate_graphic_llm_env(monkeypatch):
    for key in (
        "STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER",
        "STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL",
        "STAGE_COMPARISON_GRAPHIC_LLM_MODEL",
        "STAGE_COMPARISON_GRAPHIC_LLM_FALLBACK_MODEL",
        "STAGE_COMPARISON_GRAPHIC_LLM_TEMPERATURE",
        "STAGE_COMPARISON_GRAPHIC_LLM_MAX_TOKENS",
        "STAGE_COMPARISON_GRAPHIC_LLM_TIMEOUT_SEC",
        "STAGE_COMPARISON_GRAPHIC_LLM_IMAGE_LONG_SIDE",
        "STAGE_COMPARISON_GRAPHIC_LLM_AUTH",
        "STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD",
        "STAGE_COMPARISON_GRAPHIC_LLM_LOAD_CONTEXT_LENGTH",
        "STAGE_COMPARISON_GRAPHIC_LLM_PROTECT_MODELS",
        "STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_REQUEST",
        "STAGE_COMPARISON_GRAPHIC_LLM_UNLOAD_AFTER_BATCH",
    ):
        monkeypatch.delenv(key, raising=False)
    # Default — existing провайдер (старое OpenRouter/Gemini поведение).
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_LLM_PROVIDER", "existing")
    yield


def _make_session(session_id: str = "sess_test") -> Path:
    """Создаёт минимальный session.json + pair.json для двух пар."""
    from backend.app.services.stage_comparison import paths as paths_mod
    sd = paths_mod.session_dir(session_id)

    session_meta = {
        "id": session_id,
        "stage_a_path": "/tmp/a",
        "stage_b_path": "/tmp/b",
        "pair_order": ["p1", "p2"],
        "warnings": [],
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    paths_mod.session_json_path(session_id).write_text(
        json.dumps(session_meta, ensure_ascii=False), encoding="utf-8",
    )
    pair1 = {
        "id": "p1",
        "status": "matched",
        "left":  {"filename": "a.pdf", "pdf_path": "/dev/null/a.pdf", "md_path": "/tmp/a.md"},
        "right": {"filename": "b.pdf", "pdf_path": "/dev/null/b.pdf", "md_path": None},
    }
    pair2 = {
        "id": "p2",
        "status": "disabled",
        "left":  {"filename": "c.pdf", "pdf_path": "/dev/null/c.pdf"},
        "right": {"filename": "d.pdf", "pdf_path": "/dev/null/d.pdf"},
    }
    for pair in (pair1, pair2):
        paths_mod.pair_json_path(session_id, pair["id"]).write_text(
            json.dumps(pair, ensure_ascii=False), encoding="utf-8",
        )
    # Empty findings.json
    paths_mod.findings_path(session_id).write_text(
        json.dumps({"version": 1, "items": []}), encoding="utf-8",
    )
    return sd


# ─── Task 1: jobs ────────────────────────────────────────────────────────

def test_create_job_rejected_no_confirm():
    from backend.app.services.stage_comparison import jobs

    _make_session()
    # scope=session без подтверждения
    with patch.object(jobs.store_mod, "get_session", return_value={"id": "sess_test", "pairs": []}):
        job = jobs.create_graphic_llm_job(
            "sess_test", scope="session",
            run_paid=False, confirm_paid=False,
        )
    assert job["status"] == "rejected_no_confirm"
    assert job["run_paid"] is False
    assert job["confirm_paid"] is False


def test_get_job_marks_interrupted_when_no_task():
    """Job со status=running, но без живой asyncio.Task → 'interrupted'."""
    from backend.app.services.stage_comparison import jobs

    _make_session()
    job = {
        "id": "job_orphan_1",
        "session_id": "sess_test",
        "type": "graphic_llm_batch",
        "status": "running",
        "items": [{"status": "running"}, {"status": "queued"}],
        "progress": {"total": 2, "done": 0, "failed": 0, "skipped": 0},
        "created_at": "x", "updated_at": "y",
    }
    jobs._write_job("sess_test", job)
    got = jobs.get_job("sess_test", "job_orphan_1")
    assert got["status"] == "interrupted"
    assert all(it["status"] == "interrupted" for it in got["items"])


def test_cancel_job_changes_status_and_items():
    from backend.app.services.stage_comparison import jobs

    _make_session()
    job = {
        "id": "job_cancel_1",
        "session_id": "sess_test",
        "type": "graphic_llm_batch",
        "status": "queued",
        "items": [
            {"status": "queued"}, {"status": "done"},
            {"status": "running"}, {"status": "skipped"},
        ],
        "progress": {"total": 4, "done": 1, "failed": 0, "skipped": 1},
        "created_at": "x", "updated_at": "y",
    }
    jobs._write_job("sess_test", job)
    cancelled = jobs.cancel_job("sess_test", "job_cancel_1")
    assert cancelled["status"] == "cancelled"
    statuses = [it["status"] for it in cancelled["items"]]
    # queued/running → cancelled, done/skipped — без изменений
    assert statuses == ["cancelled", "done", "cancelled", "skipped"]


def test_concurrency_env_parsed():
    from backend.app.services.stage_comparison import jobs

    os.environ["STAGE_COMPARISON_LLM_CONCURRENCY"] = "3"
    try:
        assert jobs._concurrency_limit() == 3
    finally:
        del os.environ["STAGE_COMPARISON_LLM_CONCURRENCY"]
    os.environ["STAGE_COMPARISON_LLM_CONCURRENCY"] = "abc"
    try:
        assert jobs._concurrency_limit() == 1
    finally:
        del os.environ["STAGE_COMPARISON_LLM_CONCURRENCY"]


# ─── Task 3 & 4: reports filenames + manifest ────────────────────────────

def test_report_filenames_unique_in_same_second(monkeypatch):
    from backend.app.services.stage_comparison import reports

    _make_session()
    # Зафиксируем системное время — _new_report_id всё равно использует uuid suffix
    seen = set()
    for _ in range(10):
        rid, fname, _ = reports._allocate_filename("sess_test", "md")
        assert rid not in seen
        assert fname not in seen
        seen.add(rid)
        seen.add(fname)


def test_manifest_append_and_lookup():
    from backend.app.services.stage_comparison import reports

    _make_session()
    entry = {
        "report_id": "abc",
        "filename": "comparison_report_abc.md",
        "format": "md",
        "created_at": "2026-05-22T10:00:00Z",
        "filters": {},
        "include_images": True,
        "findings_count": 5,
        "size_bytes": 100,
        "url": "/x",
    }
    reports._manifest_append("sess_test", entry)
    found = reports._manifest_lookup("sess_test", "abc")
    assert found is not None
    assert found["filename"] == entry["filename"]
    # повторный append с тем же id → не дублирует
    entry2 = {**entry, "size_bytes": 200}
    reports._manifest_append("sess_test", entry2)
    m = reports._read_manifest("sess_test")
    items = [it for it in m["items"] if it["report_id"] == "abc"]
    assert len(items) == 1
    assert items[0]["size_bytes"] == 200


def test_list_reports_uses_manifest_then_fallback():
    from backend.app.services.stage_comparison import paths as paths_mod
    from backend.app.services.stage_comparison import reports

    _make_session()
    # 1) Запись через manifest
    manifest_entry = {
        "report_id": "manif1",
        "filename": "comparison_report_manif1.md",
        "format": "md",
        "created_at": "2026-05-22T12:00:00Z",
        "include_images": True,
        "findings_count": 3,
        "size_bytes": 10,
        "url": "/x",
    }
    reports._manifest_append("sess_test", manifest_entry)
    # И настоящий файл (на диск тоже положим)
    p = paths_mod.report_path("sess_test", "comparison_report_manif1.md")
    p.write_text("hi", encoding="utf-8")

    # 2) Старый файл «оригинального» формата на диске (БЕЗ записи в manifest)
    legacy_p = paths_mod.report_path("sess_test", "comparison_report_20240101_120000.md")
    legacy_p.write_text("legacy", encoding="utf-8")

    listed = reports.list_reports("sess_test")
    ids = [r["report_id"] for r in listed]
    assert "manif1" in ids
    # Старый формат → в fallback должно появиться
    assert any("20240101_120000" in (r.get("report_id") or "") for r in listed)


def test_create_report_writes_manifest_and_returns_id():
    from backend.app.services.stage_comparison import reports

    _make_session()
    # Создадим md-отчёт — самый простой путь
    out = reports.create_report("sess_test", "md", filters={}, include_images=False)
    assert out["ok"]
    rid = out["report_id"]
    assert rid
    listed = reports.list_reports("sess_test")
    assert any(r["report_id"] == rid for r in listed)
    # Скачать тот же файл
    p = reports.resolve_report_file("sess_test", rid)
    assert p.exists()
    assert "Отчёт по сравнению стадий" in p.read_text(encoding="utf-8")


def test_create_two_reports_in_same_second_unique_files():
    from backend.app.services.stage_comparison import reports

    _make_session()
    out1 = reports.create_report("sess_test", "md", filters={}, include_images=False)
    out2 = reports.create_report("sess_test", "md", filters={}, include_images=False)
    assert out1["report_id"] != out2["report_id"]
    # Files exist отдельно
    p1 = reports.resolve_report_file("sess_test", out1["report_id"])
    p2 = reports.resolve_report_file("sess_test", out2["report_id"])
    assert p1.exists() and p2.exists()
    assert p1 != p2


def test_resolve_report_file_rejects_traversal():
    from backend.app.services.stage_comparison import reports

    _make_session()
    with pytest.raises(FileNotFoundError):
        reports.resolve_report_file("sess_test", "../../../etc/passwd")


# ─── Task 7: bulk patch и children ───────────────────────────────────────

def _seed_findings(session_id: str = "sess_test"):
    """Создаёт findings.json с тремя элементами, включая parent/child."""
    from backend.app.services.stage_comparison import paths as paths_mod
    payload = {
        "version": 1,
        "updated_at": "x",
        "items": [
            {
                "id": "f_parent",
                "stable_key": "k_parent",
                "pair_id": "p1",
                "type": "page_added",
                "category": "page",
                "status": "new",
                "severity": "high",
                "children_count": 2,
                "right": {"page": 37},
                "deleted": False,
                "user_note": "",
            },
            {
                "id": "f_child1",
                "stable_key": "k_child1",
                "pair_id": "p1",
                "type": "graphic_added",
                "category": "graphic",
                "status": "new",
                "severity": "low",
                "parent_finding_id": "f_parent",
                "right": {"page": 37},
                "deleted": False,
                "user_note": "",
            },
            {
                "id": "f_other",
                "stable_key": "k_other",
                "pair_id": "p1",
                "type": "text_changed",
                "category": "text",
                "status": "new",
                "severity": "medium",
                "deleted": False,
                "user_note": "",
            },
        ],
    }
    paths_mod.findings_path(session_id).write_text(json.dumps(payload), encoding="utf-8")


def test_list_findings_hides_children_by_default():
    from backend.app.services.stage_comparison import findings

    _make_session()
    _seed_findings()
    out = findings.list_findings("sess_test", filters={})
    ids = [i["id"] for i in out["items"]]
    assert "f_parent" in ids
    assert "f_other" in ids
    assert "f_child1" not in ids
    assert out["summary"]["total_all"] == 3
    assert out["summary"]["children_total"] == 1
    assert out["summary"]["total_visible"] == 2


def test_list_findings_include_children():
    from backend.app.services.stage_comparison import findings

    _make_session()
    _seed_findings()
    out = findings.list_findings("sess_test", filters={"include_children": True})
    ids = [i["id"] for i in out["items"]]
    assert {"f_parent", "f_child1", "f_other"} <= set(ids)


def test_list_child_findings():
    from backend.app.services.stage_comparison import findings

    _make_session()
    _seed_findings()
    children = findings.list_child_findings("sess_test", "f_parent")
    assert len(children) == 1
    assert children[0]["id"] == "f_child1"


# ─── Task 8: bulk_patch ──────────────────────────────────────────────────

def test_bulk_patch_findings_basic():
    from backend.app.services.stage_comparison import findings

    _make_session()
    _seed_findings()
    res = findings.bulk_patch_findings(
        "sess_test", ["f_parent", "f_other"], {"status": "accepted", "severity": "low"},
    )
    assert res["updated_count"] == 2
    out = findings.list_findings("sess_test", filters={})
    for it in out["items"]:
        if it["id"] in ("f_parent", "f_other"):
            assert it["status"] == "accepted"
            assert it["severity"] == "low"


def test_bulk_patch_append_user_note():
    from backend.app.services.stage_comparison import findings

    _make_session()
    _seed_findings()
    findings.bulk_patch_findings(
        "sess_test", ["f_parent"], {"append_user_note": "Note A"},
    )
    findings.bulk_patch_findings(
        "sess_test", ["f_parent"], {"append_user_note": "Note B"},
    )
    out = findings.list_findings("sess_test", filters={})
    parent = next(i for i in out["items"] if i["id"] == "f_parent")
    assert "Note A" in parent["user_note"]
    assert "Note B" in parent["user_note"]
    assert "\n" in parent["user_note"]


def test_bulk_patch_respects_deleted_flag():
    from backend.app.services.stage_comparison import findings
    from backend.app.services.stage_comparison import paths as paths_mod

    _make_session()
    _seed_findings()
    # Помечаем f_other как deleted
    data = json.loads(paths_mod.findings_path("sess_test").read_text())
    for it in data["items"]:
        if it["id"] == "f_other":
            it["deleted"] = True
    paths_mod.findings_path("sess_test").write_text(json.dumps(data))

    res = findings.bulk_patch_findings(
        "sess_test", ["f_other"], {"status": "accepted"},
    )
    assert res["updated_count"] == 0  # deleted не трогаем
    res2 = findings.bulk_patch_findings(
        "sess_test", ["f_other"], {"status": "accepted"}, include_deleted=True,
    )
    assert res2["updated_count"] == 1


# ─── Task 9: warnings ────────────────────────────────────────────────────

def test_compute_warnings_missing_md_and_disabled():
    from backend.app.services.stage_comparison import warnings

    _make_session()  # p1: left.md_path есть, right.md_path=None; p2 disabled
    out = warnings.compute_warnings("sess_test")
    types = {it["type"] for it in out["items"]}
    assert "missing_md" in types  # right.md_path=None
    assert "disabled_pair" in types
    assert out["summary"]["medium"] >= 1
    assert out["summary"]["low"] >= 1


# ─── Auto-objects (выбор объекта вместо ручного ввода путей) ─────────────

def test_list_objects_finds_object_with_two_stages(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import objects
    from backend.app.services.stage_comparison import stage_upload

    root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(root))
    platform_obj = {"id": "obj-42", "name": "42. Тест"}
    monkeypatch.setattr(stage_upload.object_service, "list_objects", lambda: [platform_obj])
    monkeypatch.setattr(stage_upload.object_service, "get_object_by_id", lambda _oid: platform_obj)

    out = objects.list_objects()
    assert out["count"] == 1
    item = out["items"][0]
    assert item["id"] == "obj-42"
    assert len(item["stages"]) == 2
    assert item["default_stage_a"]["name"] == "stage_1"
    assert item["default_stage_b"]["name"] == "stage_2"
    expected = root / "objects" / "42_Test" / "comparison"
    assert item["default_stage_a"]["path"] == str(expected / "stage_1")
    assert item["default_stage_b"]["path"] == str(expected / "stage_2")
    assert not expected.exists()  # GET списка не создаёт каталогов


def test_list_objects_keeps_platform_object_before_first_upload(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import objects
    from backend.app.services.stage_comparison import stage_upload

    root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(root))
    platform_obj = {"id": "new", "name": "Новый объект"}
    monkeypatch.setattr(stage_upload.object_service, "list_objects", lambda: [platform_obj])
    monkeypatch.setattr(stage_upload.object_service, "get_object_by_id", lambda _oid: platform_obj)
    item = objects.list_objects()["items"][0]
    assert [stage["pdf_count"] for stage in item["stages"]] == [0, 0]


def test_list_objects_uses_only_two_comparison_stages(tmp_path, monkeypatch):
    from backend.app.services.stage_comparison import objects
    from backend.app.services.stage_comparison import stage_upload

    root = tmp_path / "projects_v2"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(root))
    platform_obj = {"id": "obj", "name": "OBJ"}
    monkeypatch.setattr(stage_upload.object_service, "list_objects", lambda: [platform_obj])
    monkeypatch.setattr(stage_upload.object_service, "get_object_by_id", lambda _oid: platform_obj)
    extra = root / "objects" / "OBJ" / "comparison" / "stage_10"
    extra.mkdir(parents=True)
    item = objects.list_objects()["items"][0]
    names = [s["name"] for s in item["stages"]]
    assert names == ["stage_1", "stage_2"]
    assert item["default_stage_a"]["name"] == "stage_1"
    assert item["default_stage_b"]["name"] == "stage_2"


# ─── Scanner: regression — папка с расширением .pdf не должна сканироваться

def test_scanner_skips_directories_named_dot_pdf(tmp_path):
    """`Path.rglob('*.pdf')` матчит и каталоги — раньше папка-обёртка
    `Foo.pdf/` появлялась в UI как лишняя строка без MD/result.json.
    После фикса в _safe_iter каталоги игнорируются."""
    from backend.app.services.stage_comparison import scanner

    # Раскладка как у пользователя:
    #   stage_1/X.pdf/   (папка-обёртка)
    #   stage_1/X.pdf/X.pdf          (настоящий PDF)
    #   stage_1/X.pdf/X.md
    #   stage_1/X.pdf/X_result.json
    stage = tmp_path / "stage_1" / "X.pdf"
    stage.mkdir(parents=True)
    (stage / "X.pdf").write_bytes(b"%PDF-1.4\n%fake\n")
    (stage / "X.md").write_text("# md", encoding="utf-8")
    (stage / "X_result.json").write_text("{}", encoding="utf-8")

    entries, warns = scanner.scan_stage_folder(tmp_path / "stage_1")
    assert len(entries) == 1
    e = entries[0]
    assert e.pdf_path.is_file()
    assert e.pdf_path.name == "X.pdf"
    assert e.md_path is not None and e.md_path.name == "X.md"
    assert e.result_json_path is not None and e.result_json_path.name == "X_result.json"


# ─── Task 1+5: single graphic-diff endpoint (мокируем LLM) ───────────────

class _FakeLLMOk:
    text = "ok summary"
    is_error = False
    cost_usd = 0.001
    model = "fake"


class _FakeLLMBlocked:
    text = ""
    is_error = True
    error_message = "paid_api_blocked: kill_switch_off"
    model = "fake"


class _FakeLLMError:
    text = ""
    is_error = True
    error_message = "Local model HTTP 500"
    model = "fake"


@pytest.mark.parametrize("fake_result, expected_status", [
    (_FakeLLMOk(), "done"),
    (_FakeLLMBlocked(), "blocked"),
    (_FakeLLMError(), "error"),
])
def test_single_graphic_diff_status_unification(fake_result, expected_status, tmp_path):
    """POST graphic-diff унифицированно отдаёт status=blocked/error/done."""
    from backend.app.api.routers import stage_comparison as router_mod
    from backend.app.services.stage_comparison import store
    from backend.app.services.llm import llm_runner as llm_runner_mod

    _make_session()

    fake_png = tmp_path / "fake.png"
    fake_png.write_bytes(bytes.fromhex(
        "89504E470D0A1A0A0000000D49484452000000010000000108060000001F15C4"
        "890000000A49444154789C636000000002000150E5276E0000000049454E44AE426082"
    ))

    async def _run():
        req = router_mod.GraphicDiffRequest(
            left_block_id="lid", right_block_id="rid",
            run_paid=True, model="fake",
        )
        return await router_mod.graphic_diff_endpoint("sess_test", "p1", req)

    fake_run_llm = AsyncMock(return_value=fake_result)
    with patch.object(store, "render_block_crop", lambda *a, **kw: fake_png), \
         patch.object(llm_runner_mod, "run_llm", fake_run_llm):
        resp = asyncio.run(_run())

    assert resp["status"] == expected_status
    if expected_status == "blocked":
        assert resp["is_paid_blocked"] is True
        assert "paid_api_blocked" in (resp.get("error") or "")
    elif expected_status == "error":
        assert resp["is_paid_blocked"] is False
    elif expected_status == "done":
        assert resp["summary"] == "ok summary"
