"""Стенд docnorm: сторож процесса, подделки, заморозка входов, оценка повтора."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from pathlib import Path

from backend.app.services.algorithm_improvement.stand import fakes
from backend.app.services.algorithm_improvement.stand import freeze as F
from backend.app.services.algorithm_improvement.stand.score import score_run

GUARD = Path(__file__).resolve().parents[1] / "app/services/algorithm_improvement/stand/guard.py"


def _run_guarded(tmp_path: Path, allow_spawn: bool = False) -> dict:
    sandbox, forbidden, outside = tmp_path / "sandbox", tmp_path / "prod", tmp_path / "elsewhere"
    for d in (sandbox, forbidden, outside):
        d.mkdir()
    (forbidden / "secret.json").write_text("{}")
    script = textwrap.dedent(f"""
        import importlib.util, json, socket, subprocess
        spec = importlib.util.spec_from_file_location("g", {str(GUARD)!r})
        g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)
        g.install(write_roots=[{str(sandbox)!r}], forbidden_read_roots=[{str(forbidden)!r}],
                  violations_path={str(sandbox / 'v.jsonl')!r}, allow_spawn={allow_spawn}, allowed_exec_names=["true"])
        out = {{}}
        def attempt(name, fn):
            try:
                fn(); out[name] = "ok"
            except Exception as exc:
                out[name] = type(exc).__name__
        attempt("write_inside", lambda: open({str(sandbox / 'a.txt')!r}, "w").write("x"))
        attempt("write_outside", lambda: open({str(outside / 'b.txt')!r}, "w").write("x"))
        attempt("replace_outside", lambda: __import__("os").replace({str(sandbox / 'a.txt')!r}, {str(outside / 'c.txt')!r}))
        attempt("read_forbidden", lambda: open({str(forbidden / 'secret.json')!r}).read())
        attempt("spawn", lambda: subprocess.run(["true"]))
        attempt("socket", lambda: socket.create_connection(("127.0.0.1", 9), timeout=1))
        print(json.dumps(out))
    """)
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_guard_blocks_writes_reads_spawn_and_network_in_dry_mode(tmp_path: Path):
    out = _run_guarded(tmp_path)
    assert out["write_inside"] == "ok"
    assert out["write_outside"] == "GuardViolation"
    assert out["replace_outside"] == "GuardViolation"
    assert out["read_forbidden"] == "GuardViolation"
    assert out["spawn"] == "GuardViolation"
    assert out["socket"] == "GuardViolation"
    assert (tmp_path / "sandbox" / "v.jsonl").read_text().count("\n") == 5
    assert not (tmp_path / "elsewhere" / "b.txt").exists()


def test_guard_real_mode_allows_only_listed_executables(tmp_path: Path):
    assert _run_guarded(tmp_path, allow_spawn=True)["spawn"] == "ok"


def test_fake_docnorm_is_deterministic_and_b_moves_formal_items():
    seeds = [
        {"obs_id": "o1", "problem": "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на плиты."},
        {"obs_id": "o2", "problem": "В ведомости ссылочных документов неверно указано наименование ГОСТ 13996-2019 плитки."},
        {"obs_id": "o3", "problem": "В спецификации материалов указан отменённый ГОСТ 6465-76 для эмали."},
    ]
    a1 = fakes.fake_docnorm_payload(seeds, arm="A", repeat=1)
    assert a1 == fakes.fake_docnorm_payload(seeds, arm="A", repeat=1)
    b1 = fakes.fake_docnorm_payload(seeds, arm="B", repeat=1)
    kept_a = {f["problem"] for f in a1["findings"]}
    kept_b = {f["problem"] for f in b1["findings"]}
    title = seeds[1]["problem"]
    if title in kept_a:
        assert title not in kept_b and any(n["issue"] == "title" for n in b1["formal_notes"])
    assert all(f.get("_stand_fake") for f in a1["findings"])


def _mini_archive(root: Path) -> dict:
    vp = "256_X/disciplines/AR/documents/DOC_V1/versions/v001"
    vdir = root / vp
    run = vdir / "03_analysis" / "runs" / "r1"
    for p, body in {
        root / "256_X/object.json": {"object_id": "o"},
        vdir.parent.parent / "document.json": {"document_code": "DOC_V1"},
        vdir / "01_input/project_info.json": {"section": "AR"},
        vdir / "02_work/document.md": "# md",
        run / "03_findings_codex_base.json": {"findings": [{"id": "F-001", "problem": "база"}]},
        run / "03_findings_targeted_alia_docnorm_audit.json": {"findings": [{"id": "TGT-001"}]},
        run / "03_findings.json": {"findings": [{"id": "F-001", "problem": "итог"}]},
        run / "03a_norms_verified.json": {"findings": []},
        run / "01_blocks_analysis.json": {"blocks": []},
    }.items():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body if isinstance(body, str) else json.dumps(body, ensure_ascii=False), encoding="utf-8")
    return {"version_path": vp, "reviewed_run": "r1",
            "md": {"path": f"{vp}/02_work/document.md", "sha256": F.sha256_file(vdir / "02_work/document.md")},
            "project_info_sha256": F.sha256_file(vdir / "01_input/project_info.json"),
            "shared_inputs": {"03_findings_codex_base.json": {"sha256": F.sha256_file(run / "03_findings_codex_base.json")},
                              "01_blocks_analysis.json": {"sha256": "не-тот-хэш"}}}


def test_freeze_inputs_copies_only_pre_docnorm_artifacts_and_reports_mismatch(tmp_path: Path):
    archive = tmp_path / "objects"
    entry = _mini_archive(archive)
    info = F.freeze_inputs(entry, archive, tmp_path / "pack")
    latest = tmp_path / "pack/projects_v2/objects" / entry["version_path"] / "03_analysis/latest"
    assert (latest / "03_findings_codex_base.json").is_file()
    assert (latest / "01_blocks_analysis.json").is_file()
    # результаты docnorm и последующих этапов не наследуются
    for name in ("03_findings.json", "03_findings_targeted_alia_docnorm_audit.json", "03a_norms_verified.json"):
        assert not (latest / name).exists()
    assert info["pinned_mismatches"] == ["01_blocks_analysis.json"]


def test_score_run_counts_strong_matches_and_retention(tmp_path: Path):
    run = tmp_path / "A_r1"
    (run / "receipts").mkdir(parents=True)
    (run / "result").mkdir()
    (run / "receipts/run.json").write_text(json.dumps({"status": "ok", "checks": {}, "counts": {"docnorm": 1}}))
    target = {"id": "F-002", "source_stage": "alia_docnorm_audit",
              "problem": "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на плиты гипсовые."}
    base = {"id": "F-001", "problem": "Не указана марка перемычки"}
    for name in ("after_verify", "final"):
        (run / "result" / f"{name}.json").write_text(json.dumps({"findings": [base, target]}, ensure_ascii=False))
    known = {
        "version_path_in_sandbox": "projects_v2/objects/x",
        "docnorm_rejected_verified": [{"obs_id": "k1", "finding": {
            "problem": "В ведомости ссылочных документов указан отменённый ГОСТ 6428-83 на гипсовые плиты."}}],
        "other_accepted_verified": [{"obs_id": "k2", "finding": {"problem": "Не указана марка перемычки"}}],
        "base_findings": [base],
    }
    s = score_run(run, known)
    assert s["KR_strong"] == 1 and s["targets"] == 1
    assert s["other_accepted_retained_exact"] == 1
    assert s["non_target_not_in_base"] == 0 and s["base_not_in_final"] == 0
