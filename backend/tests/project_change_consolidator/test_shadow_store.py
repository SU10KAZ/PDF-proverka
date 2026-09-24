"""Phase D: append-only shadow storage + a full shadow run with a fake provider (0 model calls)."""
from __future__ import annotations

import json
import stat

import pytest

from backend.app.services.project_change_consolidator import contracts as C
from backend.app.services.project_change_consolidator import engine as E
from backend.app.services.project_change_consolidator.shadow import run_shadow
from backend.app.services.project_change_consolidator.storage import ShadowStore, ShadowStoreError
from backend.app.services.project_change_v3.provider import FakeProvider
from backend.tests.project_change_consolidator import responses as R
from backend.tests.project_change_consolidator.test_validator import build


def scripted(call_id, pair_id, data, schema, images):
    refs = [c["card_ref"] for c in data["cards"]]
    if data["mode"] == C.MODE_CLUSTER and refs == ["A1", "B1", "C1"]:
        return R.merge(data, material={("R-OTHER", "H001")})
    if data["mode"] == C.MODE_SINGLETON_REVIEW:
        return R.keep_all(data, channel="DOCUMENTARY_CHANGE", flags=["DOCUMENTARY_SUSPECTED"])
    return R.keep_all(data)


def _run(tmp_path, handler=scripted, **kw):
    prepared, *_ = build(tmp_path)
    fake = FakeProvider(handlers={C.STAGE: handler})
    root = tmp_path / "production" / "projectchange_consolidator_shadow"
    src = tmp_path / "source"
    manifest = run_shadow(prepared.bundle, fake, root, max_calls=12, prepared=prepared, watch_roots=[src], **kw)
    return prepared, fake, root, manifest


def test_full_shadow_run_completes_and_is_bound_to_the_source(tmp_path):
    prepared, fake, root, manifest = _run(tmp_path)
    assert manifest["state"] == "COMPLETED" and manifest["frozen"] and manifest["automatic_retries"] == 0
    assert manifest["calls_planned"] == manifest["calls_made"] == len(fake.calls) == 3
    store = ShadowStore(root)
    loaded = store.load_completed(prepared.bundle.source_run_id, manifest["consolidator_run_id"],
                                  source_result_sha256=prepared.bundle.result_sha256)
    result = loaded["result"]
    stats = result["stats"]
    assert stats["source_cards"] == 6 and stats["consolidated_cards"] == 1 and stats["cards_absorbed"] == 3
    assert stats["documentary_changes"] == 1 and stats["output_items"] == 4
    merged = result["engineering_changes"][0]
    assert merged["origin"] == "CONSOLIDATED" and merged["open_conflicts"][0]["hint"]["hint_ref"] == "R-OTHER/H001"
    report = json.loads((root / prepared.bundle.source_run_id / manifest["consolidator_run_id"]
                         / "VALIDATION_REPORT.json").read_text())
    assert [c["result"] for c in report["run_checks"]] == ["PASS"] * 6
    assert [c["code"] for c in report["run_checks"]] == ["X1", "X2", "X3", "X4", "X5", "X6"]


def test_completed_run_is_immutable(tmp_path):
    prepared, _, root, manifest = _run(tmp_path)
    run_dir = root / prepared.bundle.source_run_id / manifest["consolidator_run_id"]
    assert all(not (p.stat().st_mode & stat.S_IWUSR) for p in run_dir.rglob("*") if p.is_file())
    with pytest.raises(PermissionError):
        (run_dir / "SHADOW_RESULT.json").write_text("{}")
    with pytest.raises(ShadowStoreError) as exc:
        ShadowStore(root).load_completed(prepared.bundle.source_run_id, manifest["consolidator_run_id"],
                                         source_result_sha256="0" * 64)
    assert exc.value.code == "SHADOW_SOURCE_MISMATCH"


def test_raw_response_is_stored_before_validation_even_when_rejected(tmp_path):
    def broken(call_id, pair_id, data, schema, images):
        resp = R.merge(data)
        resp["recompositions"][0]["change_summary"] = "Подача 999 м3/ч."  # invented number → C4
        return resp

    prepared, _, root, manifest = _run(tmp_path, handler=broken)
    run_dir = root / prepared.bundle.source_run_id / manifest["consolidator_run_id"]
    responses = sorted(run_dir.glob("calls/*/RESPONSE.json"))
    assert len(responses) == 3
    body = json.loads(responses[0].read_text())
    assert "999" in json.dumps(body["raw_response"], ensure_ascii=False)
    result = json.loads((run_dir / "SHADOW_RESULT.json").read_text())
    assert result["stats"]["consolidated_cards"] == 0 and result["stats"]["rollbacks"] >= 1


def test_shadow_never_writes_source_or_run_artifacts(tmp_path):
    prepared, _, root, manifest = _run(tmp_path)
    names = [p.name for p in root.rglob("*")]
    assert "run_manifest.json" not in names and "current_run.json" not in names
    assert not any(n.startswith("project_change_v3_") for n in names)
    with pytest.raises(ShadowStoreError):
        ShadowStore(tmp_path / "runs")


def test_call_plan_above_limit_writes_nothing(tmp_path):
    prepared, *_ = build(tmp_path)
    root = tmp_path / "shadow"
    fake = FakeProvider(handlers={C.STAGE: scripted})
    with pytest.raises(E.CallPlanExceeded):
        run_shadow(prepared.bundle, fake, root, max_calls=2, prepared=prepared)
    assert fake.calls == [] and not root.exists()


def test_one_shadow_run_per_source_at_a_time(tmp_path):
    store = ShadowStore(tmp_path / "shadow")
    run = store.create_run("src1")
    with pytest.raises(ShadowStoreError) as exc:
        store.create_run("src1")
    assert exc.value.code == "SHADOW_LOCKED"
    run.write("A.json", {"x": 1})
    with pytest.raises(ShadowStoreError) as exc:
        run.write("A.json", {"x": 2})
    assert exc.value.code == "SHADOW_OVERWRITE_REFUSED"
    run.finalize({"state": "FAILED"})
    with pytest.raises(ShadowStoreError):
        run.write("B.json", {})
    second = store.create_run("src1")  # the lock is released by finalize
    assert second.consolidator_run_id != run.consolidator_run_id


def test_failed_run_checks_publish_no_result(tmp_path, monkeypatch):
    from backend.app.services.project_change_consolidator import shadow as S

    real = S.snapshot
    calls = {"n": 0}

    def drifting(roots):
        calls["n"] += 1
        snap = real(roots)
        if calls["n"] == 2:
            snap["/pretend/changed"] = "x"
        return snap

    monkeypatch.setattr(S, "snapshot", drifting)
    prepared, _, root, manifest = _run(tmp_path)
    assert manifest["state"] == "FAILED" and manifest["reason_code"] == "X6"
    assert manifest["shadow_result_sha256"] is None
    with pytest.raises(ShadowStoreError) as exc:
        ShadowStore(root).load_completed(prepared.bundle.source_run_id, manifest["consolidator_run_id"],
                                         source_result_sha256=prepared.bundle.result_sha256)
    assert exc.value.code == "SHADOW_NOT_COMPLETED"
