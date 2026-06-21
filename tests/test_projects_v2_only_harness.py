"""Шаг 7/10 — broad v2-only тесты на reusable harness (legacy недоступен)."""
from __future__ import annotations

import json

import pytest

from helpers.projects_v2_only import build_v2_only_store, add_v2_document
from backend.app.services.storage import v2_primary_wiring as wiring


def test_v2_only_no_legacy_required(tmp_path):
    store = build_v2_only_store(tmp_path)
    add_v2_document(store)
    assert not store.legacy_root.exists()  # legacy отсутствует
    assert store.adapter().is_available() is True


def test_v2_only_project_list(tmp_path):
    store = build_v2_only_store(tmp_path)
    add_v2_document(store, document_code="DOC-A")
    add_v2_document(store, document_code="DOC-B", discipline="AR")
    docs = store.adapter().list_documents()
    codes = {d["document_code"] for d in docs}
    assert {"DOC-A", "DOC-B"} <= codes


def test_v2_only_project_details(tmp_path):
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store, document_code="DOC-D", extra_versions=("v002",))
    d = store.adapter().get_document(doc.object_folder, doc.discipline, doc.document_code)
    assert d is not None
    assert d["version_count"] == 2
    assert d["current_version"] == "v001"


def test_v2_only_findings(tmp_path):
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store, findings_n=7)
    assert store.adapter().findings_count(doc.doc_dir, doc.version_id) == 7


def test_v2_only_finding_by_id(tmp_path):
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store, findings=[{"id": "X-1"}, {"id": "X-2"}, {"id": "X-3"}])
    flist = store.adapter().findings_list(doc.doc_dir, doc.version_id)
    assert {f["id"] for f in flist} == {"X-1", "X-2", "X-3"}


def test_v2_only_block_map(tmp_path):
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store)
    blocks = store.adapter().read_blocks_analysis(doc.doc_dir, doc.version_id)
    assert blocks is not None
    assert len(blocks["blocks"]) == 2


def test_v2_only_optimization(tmp_path):
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store)
    opt_path = store.adapter().latest_dir(doc.doc_dir, doc.version_id) / "optimization.json"
    opt = json.loads(opt_path.read_text(encoding="utf-8"))
    assert opt["items"][0]["id"] == "o1"


def test_v2_only_export_source_lookup(tmp_path, monkeypatch):
    store = build_v2_only_store(tmp_path, create_empty_legacy=True)
    doc = add_v2_document(store, document_code="DOC-EXP")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    pdf = wiring.v2_source_pdf("DOC-EXP", "v001", v2_root=store.v2_root)
    assert pdf is not None and pdf.name == "DOC-EXP.pdf"
    assert pdf.resolve().is_relative_to(store.v2_root.resolve())
    # legacy пуст — источник найден из v2
    assert not any(store.legacy_root.iterdir())
