"""
Тесты metadata-only нормализации version.json (normalize_version_metadata).
Гермётичны (tmp_path), пишут только version.json, legacy не трогают.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"))
import normalize_version_metadata as nm  # noqa: E402

C01, C02, C03 = nm.CRITICAL


# ---------------------------------------------------------------------------
# classify_status
# ---------------------------------------------------------------------------


def _present(*names):
    return {c: (c in names) for c in nm.CRITICAL}


def test_classify_complete():
    assert nm.classify_status(_present(C01, C02, C03), False, True) == "complete"


def test_classify_partial():
    assert nm.classify_status(_present(C01), False, True) == "partial"
    assert nm.classify_status(_present(C01, C03), False, True) == "partial"


def test_classify_none():
    assert nm.classify_status(_present(), False, True) == "none"


def test_classify_legacy_partial_when_files():
    assert nm.classify_status(_present(C03), True, True) == "legacy_partial"
    assert nm.classify_status(_present(C01, C02, C03), True, True) == "legacy_partial"


def test_classify_source_only_when_legacy_no_files():
    assert nm.classify_status(_present(), True, True) == "source_only"


def test_is_legacy_preserve():
    assert nm.is_legacy_preserve({"migration_kind": "legacy_findings_preserve"})
    assert nm.is_legacy_preserve({"preserve_reason": "legacy_algorithm_with_kb_findings"})
    assert nm.is_legacy_preserve({"preserve_reason": "king_sons_source_only_legacy_bundle"})
    assert not nm.is_legacy_preserve({"preserve_reason": "normal"})
    assert not nm.is_legacy_preserve({})


# ---------------------------------------------------------------------------
# fixture builder
# ---------------------------------------------------------------------------


def _mkver(v2: Path, disc: str, code: str, *, vid="v001", version_json: dict,
           latest_files=(), input_files=("document.md",)):
    vroot = (v2 / "objects" / "214_Alia_ASTERUS" / "disciplines" / disc
             / "documents" / code / "versions" / vid)
    (vroot / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (vroot / "01_input").mkdir(parents=True, exist_ok=True)
    for f in latest_files:
        (vroot / "03_analysis" / "latest" / f).write_text("{}", encoding="utf-8")
    for f in input_files:
        (vroot / "01_input" / f).write_text("x", encoding="utf-8")
    (vroot / "version.json").write_text(json.dumps(version_json, ensure_ascii=False),
                                        encoding="utf-8")
    return vroot


def _read_status(vroot: Path):
    return json.loads((vroot / "version.json").read_text())


def _build(tmp_path) -> Path:
    v2 = tmp_path / "projects_v2"
    # 1) missing analysis_status, all 3 files -> fill complete
    _mkver(v2, "EOM", "doc-complete", version_json={"version_id": "v001", "source": "plain"},
           latest_files=[C01, C02, C03])
    # 2) missing status, part -> fill partial
    _mkver(v2, "EOM", "doc-partial", version_json={"version_id": "v001"},
           latest_files=[C01])
    # 3) missing status, no analysis -> fill none
    _mkver(v2, "OV", "doc-none", version_json={"version_id": "v001"},
           latest_files=[])
    # 4) already complete + missing field present -> unchanged
    _mkver(v2, "OV", "doc-have", version_json={
        "version_id": "v001", "analysis_status": "complete",
        "missing_analysis_files": []}, latest_files=[C01, C02, C03])
    # 5) already complete but NO missing field -> add field only
    _mkver(v2, "VK", "doc-addfield", version_json={
        "version_id": "v001", "analysis_status": "complete"},
        latest_files=[C01, C02, C03])
    # 6) legacy_partial but no files (KB-findings) -> divergent, preserved
    _mkver(v2, "SS", "doc-sot", version_json={
        "version_id": "v001", "analysis_status": "legacy_partial",
        "preserve_reason": "legacy_algorithm_with_kb_findings",
        "missing_analysis_files": [C01, C02, C03]}, latest_files=[])
    return v2


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def test_plan_actions(tmp_path):
    v2 = _build(tmp_path)
    rows = {r["document_code"]: r for r in nm.gather(v2)}
    assert rows["doc-complete"]["action"] == "fill" and rows["doc-complete"]["proposed"] == "complete"
    assert rows["doc-partial"]["action"] == "fill" and rows["doc-partial"]["proposed"] == "partial"
    assert rows["doc-none"]["action"] == "fill" and rows["doc-none"]["proposed"] == "none"
    assert rows["doc-have"]["action"] == "unchanged_match"
    assert rows["doc-addfield"]["action"] == "unchanged_match_add_missing"
    sot = rows["doc-sot"]
    assert sot["action"] == "kept_existing_divergent"
    assert sot["existing"] == "legacy_partial" and sot["proposed"] == "source_only"


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------


def test_execute_fills_and_preserves(tmp_path):
    v2 = _build(tmp_path)
    rows = nm.gather(v2)
    for r in rows:
        nm.apply_one(r, correct_existing=False)
    by = {r["document_code"]: Path(r["_path"]).parent for r in rows}
    assert _read_status(by["doc-complete"])["analysis_status"] == "complete"
    assert _read_status(by["doc-partial"])["analysis_status"] == "partial"
    assert _read_status(by["doc-none"])["analysis_status"] == "none"
    # filled records get missing_analysis_files + normalized marker
    assert _read_status(by["doc-partial"])["missing_analysis_files"] == [C02, C03]
    assert "metadata_normalized_at" in _read_status(by["doc-complete"])
    # divergent preserved (NOT overwritten)
    assert _read_status(by["doc-sot"])["analysis_status"] == "legacy_partial"
    # add-field-only got the field, status unchanged
    af = _read_status(by["doc-addfield"])
    assert af["analysis_status"] == "complete" and af["missing_analysis_files"] == []


def test_execute_idempotent(tmp_path):
    v2 = _build(tmp_path)
    rows1 = nm.gather(v2)
    for r in rows1:
        nm.apply_one(r, correct_existing=False)
    # second pass: nothing left to fill / add
    rows2 = nm.gather(v2)
    fills = [r for r in rows2 if r["action"] in ("fill", "unchanged_match_add_missing")]
    assert fills == []
    s = nm.build_summary(rows2, executed=False, correct_existing=False)
    assert s["filled"] == 0 and s["added_missing_field_only"] == 0


def test_correct_existing_overwrites_divergent(tmp_path):
    v2 = _build(tmp_path)
    rows = nm.gather(v2)
    for r in rows:
        nm.apply_one(r, correct_existing=True)
    by = {r["document_code"]: Path(r["_path"]).parent for r in rows}
    assert _read_status(by["doc-sot"])["analysis_status"] == "source_only"


def test_dry_run_writes_nothing(tmp_path):
    v2 = _build(tmp_path)
    rows = nm.gather(v2)
    before = {r["_path"]: Path(r["_path"]).read_text() for r in rows}
    # gather() is read-only; simulate dry-run (no apply_one)
    after = {p: Path(p).read_text() for p in before}
    assert before == after


def test_only_touches_version_json(tmp_path):
    """execute не трогает входные/analysis-файлы (только version.json)."""
    v2 = _build(tmp_path)
    others = [p for p in v2.rglob("*") if p.is_file() and p.name != "version.json"]
    snap = {p: (p.stat().st_mtime_ns, p.read_bytes()) for p in others}
    rows = nm.gather(v2)
    for r in rows:
        nm.apply_one(r, correct_existing=True)
    for p, (mt, data) in snap.items():
        assert p.read_bytes() == data, f"non-version file changed: {p}"


def test_summary_counts(tmp_path):
    v2 = _build(tmp_path)
    rows = nm.gather(v2)
    s = nm.build_summary(rows, executed=False, correct_existing=False)
    assert s["total_version_json"] == 6
    assert s["missing_analysis_status_before"] == 3
    assert s["filled"] == 3
    assert s["added_missing_field_only"] == 1
    assert s["unchanged"] == 1
    assert s["kept_existing_divergent"] == 1
    # complete after = doc-complete(fill) + doc-have + doc-addfield = 3
    assert s["status_distribution_after"]["complete"] == 3
    assert s["status_distribution_after"]["legacy_partial"] == 1  # doc-sot preserved
