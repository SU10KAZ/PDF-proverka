#!/usr/bin/env python3
"""Стенд эксперимента docnorm: заморозка копий, планы, повторы, оценка.

  freeze            копии: код релиза (+оверлей модуля), рантайм норм, промпты, app data, флаги службы
  freeze-inputs     входные пакеты версий (до docnorm) + известные решения (для оценки) + затравка подделок
  plan-calibration  план калибровки: 2 документа dev × A,A′,B,B′ + 1 положительный контроль × A,B
  run               повторы плана в отдельных процессах (--mode dry: подделки, вызовы моделей запрещены)
  score             оценка повторов, проверки целостности, фактическая смета

Боевой режим (--mode real) требует --permission-token и такой же AIM_ALLOW_MODEL_CALLS в окружении.
Прод не меняется: источники только читаются, всё пишется в --exp (comparison/algorithm_improvement/...).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.algorithm_improvement.stand import freeze as F  # noqa: E402

DEFAULT_RELEASE = Path("/home/coder/auditmanager/current")
DEFAULT_SHARED_NORMS = Path("/home/coder/auditmanager/shared/norms")
DEFAULT_PROMPTS = Path("/srv/auditmanager/data/prompts")
DEFAULT_APP_DATA = Path("/srv/auditmanager/data/backend/app/data")
DEFAULT_DATA_NORMS = Path("/srv/auditmanager/data/norms/norms_paragraphs.json")
DEFAULT_OBJECTS = Path("/srv/auditmanager/data/projects_v2/objects")
FORBIDDEN_READ_ROOTS = (
    "/srv/auditmanager", "/home/coder/auditmanager", "/home/coder/projects/OSA",
    str(ROOT / "backend"), str(ROOT / "norms"), str(ROOT / "prompts"), str(ROOT / "knowledge_base"),
    str(ROOT / "projects_v2"), str(ROOT / "frontend"), str(ROOT / "scripts"),
)
FORCED_ENV = {
    "AUDIT_OPENROUTER_ENABLED": "0", "PAID_API_ENABLED": "false",
    "DISTRIBUTED_AUDIT_EXECUTION_ENABLED": "0", "DISTRIBUTED_WORKERS_ENABLED": "0",
    "BATCH_AUTO_RESUME_ENABLED": "0", "BLOCK_CROP_EVICTION_ENABLED": "0", "BLOCK_CROP_RESTORE_ENABLED": "0",
    "AUDIT_STORAGE_BACKEND": "projects_v2", "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
    "AUDIT_DISABLE_DOTENV": "1", "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1",
    "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "TOKENIZERS_PARALLELISM": "false",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")


def doc_key(version_path: str) -> str:
    obj, _, _, _, doc, _, ver = Path(version_path).parts[:7]
    return f"{obj[:3]}__{doc}__{ver}".replace("/", "_")


# ─── freeze ────────────────────────────────────────────────────────────────

def cmd_freeze(args) -> int:
    exp = args.exp.resolve()
    frozen = exp / "frozen"
    if (frozen / "FROZEN.json").exists():
        print(f"уже заморожено: {frozen / 'FROZEN.json'}")
        return 2
    frozen.mkdir(parents=True, exist_ok=True)
    pid = args.service_pid or int(subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "--value", "auditmanager-backend.service"],
        capture_output=True, text=True).stdout.strip() or 0)
    flags = F.service_flags(pid) if pid else {}
    _write(frozen / "service_flags.json", {"captured_at_utc": _now(), "pid": pid, "flags": flags,
                                           "note": "белый список префиксов поведения; ключи/секреты отброшены"})
    info = {"frozen_at_utc": _now()}
    t = time.time()
    info["code"] = F.freeze_code(args.release, frozen / "code", ROOT / "backend/app/services/algorithm_improvement")
    info["prompts"] = F.freeze_prompts(args.prompts, frozen / "prompts")
    info["app_data"] = F.freeze_app_data(args.app_data, frozen / "app_data")
    (frozen / "data_norms").mkdir()
    shutil.copy2(args.data_norms, frozen / "data_norms" / "norms_paragraphs.json")
    info["data_norms_paragraphs_sha256"] = F.sha256_file(frozen / "data_norms" / "norms_paragraphs.json")
    info["norms_runtime"] = F.freeze_norms_runtime(args.shared_norms, frozen / "norms_runtime")
    info["service_flags_sha256"] = F.sha256_file(frozen / "service_flags.json")
    info["python"] = {"executable": sys.executable, "version": platform.python_version()}
    freeze_txt = subprocess.run([sys.executable, "-m", "pip", "freeze"], capture_output=True, text=True).stdout
    rel_freeze = Path(args.release).resolve() / "dependencies.freeze.txt"
    ours = {line.split("==")[0].lower(): line for line in freeze_txt.splitlines() if "==" in line}
    theirs = {line.split("==")[0].lower(): line for line in rel_freeze.read_text().splitlines() if "==" in line} \
        if rel_freeze.is_file() else {}
    info["python"]["differs_from_release"] = sorted(k for k in theirs if ours.get(k) != theirs[k])[:200]
    info["python"]["missing_vs_release"] = sorted(k for k in theirs if k not in ours)[:200]
    info["seconds"] = round(time.time() - t, 1)
    F.make_read_only(frozen / "code")
    _write(frozen / "FROZEN.json", info)
    print(json.dumps({k: info[k] for k in ("code", "prompts", "app_data")}, ensure_ascii=False)[:1500])
    return 0


def cmd_refreeze_overlay(args) -> int:
    """Заменить оверлей модуля в замороженном коде — только пока не было ни одного повтора."""
    exp = args.exp.resolve()
    if any((exp / "campaigns").glob("*/run_real_*.json")):
        print("боевые повторы уже были — оверлей не меняется, нужна новая заморозка в новый каталог")
        return 2
    frozen = exp / "frozen"
    info = _json(frozen / "FROZEN.json")
    target = frozen / "code" / "backend/app/services/algorithm_improvement"
    parent_mode = target.parent.stat().st_mode
    target.parent.chmod(parent_mode | 0o200)
    if target.exists():
        for p in [target, *target.rglob("*")]:
            p.chmod(p.stat().st_mode | 0o200)
        shutil.rmtree(target)
    F._copytree(ROOT / "backend/app/services/algorithm_improvement", target)
    F.make_read_only(target)
    target.chmod(target.stat().st_mode & ~0o222)
    target.parent.chmod(parent_mode)
    old = {"code": info["code"]["code"]["sha256"], "overlay": info["code"]["overlay"]["sha256"]}
    info["code"]["code"] = F.tree_sha256(frozen / "code")
    info["code"]["overlay"] = F.tree_sha256(target)
    info.setdefault("overlay_history", []).append({"at_utc": _now(), "reason": args.reason, "previous": old,
                                                   "new_overlay": info["code"]["overlay"]["sha256"]})
    frozen_json = frozen / "FROZEN.json"
    frozen_json.chmod(0o644)
    _write(frozen_json, info)
    print(json.dumps(info["overlay_history"][-1], ensure_ascii=False))
    return 0


def verify_frozen(exp: Path) -> dict:
    frozen = exp / "frozen"
    info = _json(frozen / "FROZEN.json")
    now = {"code": F.tree_sha256(frozen / "code")["sha256"], "prompts": F.tree_sha256(frozen / "prompts")["sha256"],
           "app_data": F.tree_sha256(frozen / "app_data")["sha256"]}
    want = {"code": info["code"]["code"]["sha256"], "prompts": info["prompts"]["tree"]["sha256"],
            "app_data": info["app_data"]["tree"]["sha256"]}
    status_index = F.sha256_file(frozen / "norms_runtime" / "tools" / "status_index.json")
    return {"trees_equal": now == want, "status_index_equal":
            status_index == info["norms_runtime"]["tools_data"]["status_index.json"], "now": now}


# ─── freeze-inputs ─────────────────────────────────────────────────────────

def _ledger_rows(exp: Path, discipline: str) -> list[dict]:
    path = exp / "data" / "ledger_all" / discipline / "ledger.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _entry_for_version(exp: Path, version_path: str, objects_root: Path) -> dict:
    """Запись как в манифесте — для версий вне манифеста (положительные контроли других дисциплин)."""
    from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files
    parts = Path(version_path).parts
    discipline, doc, ver = parts[2], parts[4], parts[6]
    rows = [r for r in _ledger_rows(exp, discipline) if r["document"] == doc and r["version"] == ver]
    vdir = objects_root / version_path
    decided = {r["item_id"]: str((r["finding"] or {}).get("problem") or "").strip() for r in rows if r["finding"]}
    run, agreement = F.find_reviewed_run(vdir, decided)
    info = _json(vdir / "01_input" / "project_info.json")
    md = resolve_version_source_files(vdir, doc, project_info=info).md_path
    shared = {n: {"sha256": F.sha256_file(run / n) if run and (run / n).is_file() else None}
              for n in ("03_findings_codex_base.json", "03_findings_targeted_alia_ar_masonry_audit.json",
                        "01_blocks_analysis.json", "02_text_analysis.json", "document_graph.json")}
    return {"version_path": version_path, "reviewed_run": run.name if run else None,
            "reviewed_run_agreement": agreement, "family": rows[0]["family"] if rows else None,
            "md": {"path": str(md.relative_to(objects_root)), "sha256": F.sha256_file(md)},
            "project_info_sha256": F.sha256_file(vdir / "01_input" / "project_info.json"),
            "shared_inputs": shared, "pinned_at_plan_time": True, "discipline": discipline}


def _known_for(exp: Path, entry: dict) -> dict:
    parts = Path(entry["version_path"]).parts
    discipline, doc, ver = parts[2], parts[4], parts[6]
    rows = [r for r in _ledger_rows(exp, discipline) if r["document"] == doc and r["version"] == ver]
    pc = _json(exp / "positive_controls.json")["controls"]
    pcs = [c for c in pc if c["document"] == doc and c["version"] == ver]
    by_obs = {r["obs_id"]: r for r in rows}
    sel = lambda pred: [{"obs_id": r["obs_id"], "item_id": r["item_id"], "finding": r["finding"]} for r in rows if pred(r)]
    return {
        "version_path": entry["version_path"],
        "version_path_in_sandbox": str(Path("projects_v2/objects") / entry["version_path"]),
        "docnorm_rejected_verified": sel(lambda r: r["origin"] == "alia_docnorm_audit" and r["provenance"] == "human"
                                         and r["decision"] == "rejected" and r["link"] == "verified"),
        "docnorm_rejected_probable": sel(lambda r: r["origin"] == "alia_docnorm_audit" and r["provenance"] == "human"
                                         and r["decision"] == "rejected" and r["link"] == "unverified_stable"),
        "other_accepted_verified": sel(lambda r: r["origin"] != "alia_docnorm_audit" and r["provenance"] == "human"
                                       and r["decision"] == "accepted" and r["link"] == "verified"),
        "positive_controls": [{**c, "finding": (by_obs.get(c["obs_id"]) or {}).get("finding")} for c in pcs],
    }


def cmd_freeze_inputs(args) -> int:
    exp = args.exp.resolve()
    manifest = _json(exp / "manifest.json")
    by_path = {e["version_path"]: e for e in manifest["entries"]}
    for version_path in args.version:
        entry = by_path.get(version_path) or _entry_for_version(exp, version_path, args.objects_root)
        key = doc_key(version_path)
        dest = exp / "inputs" / key
        if dest.exists():
            print(f"{key}: уже заморожен")
            continue
        info = F.freeze_inputs(entry, args.objects_root, dest / "pack")
        known = _known_for(exp, entry)
        base_path = dest / "pack" / Path(known["version_path_in_sandbox"]) / "03_analysis/latest/03_findings_codex_base.json"
        known["base_findings"] = (_json(base_path).get("findings", []) if base_path.is_file() else [])
        dest.chmod(0o755)
        _write(dest / "INPUTS.json", {**info, "entry": entry})
        _write(dest / "known.json", known)
        seeds = [{"obs_id": k["obs_id"], **(k["finding"] or {})} for k in known["docnorm_rejected_verified"]] + \
                [{"obs_id": c["obs_id"], **(c["finding"] or {})} for c in known["positive_controls"]
                 if c.get("input_mode") == "natural" and c.get("finding")]
        _write(dest / "fake_seed.json", seeds)
        print(f"{key}: файлов {len(info['files'])}, расхождений с манифестом: {info['pinned_mismatches'] or 'нет'}, "
              f"известных отклонений docnorm {len(known['docnorm_rejected_verified'])}, "
              f"положительных контролей {len(known['positive_controls'])}")
    return 0


# ─── plan-calibration ──────────────────────────────────────────────────────

def cmd_plan_calibration(args) -> int:
    exp = args.exp.resolve()
    manifest = _json(exp / "manifest.json")
    pcs = _json(exp / "positive_controls.json")["controls"]
    pc_docs = {(c["document"], c["version"]) for c in pcs}
    rank = lambda fam: hashlib.sha256(f"docnorm_exp_v1|{fam}".encode()).hexdigest()
    dev = [e for e in manifest["entries"] if e["split"] == "dev" and (e["document"], e["version"]) not in pc_docs]
    chosen = []
    for obj in ("256", "214"):
        cands = sorted((e for e in dev if e["object"].startswith(obj)), key=lambda e: rank(e["family"]))
        chosen.append(cands[0]["version_path"])
    x1 = next(c for c in pcs if c["case_group"] == "X-1" and c["input_mode"] == "natural")
    pc_path = next(str(p.relative_to(args.objects_root)) for p in
                   args.objects_root.glob(f"{x1['object']}/disciplines/{x1['discipline']}/documents/"
                                          f"{x1['document']}/versions/{x1['version']}"))
    runs = []
    for vp in chosen:
        for arm, rep in (("A", 1), ("B", 1), ("A", 2), ("B", 2)):
            runs.append({"version_path": vp, "doc_key": doc_key(vp), "arm": arm, "repeat": rep, "role": "dev"})
    for arm in ("A", "B"):
        runs.append({"version_path": pc_path, "doc_key": doc_key(pc_path), "arm": arm, "repeat": 1,
                     "role": f"positive_control:{x1['case_group']}:{x1['obs_id']}"})
    plan = {"campaign": args.campaign, "created_at_utc": _now(),
            "manifest_sha256": (exp / "MANIFEST.sha256").read_text().split()[0],
            "variant_sha256": manifest["arms"]["B"]["variant_sha256"], "runs": runs}
    camp = exp / "campaigns" / args.campaign
    if (camp / "plan.json").exists():
        print("план уже существует")
        return 2
    _write(camp / "plan.json", plan)
    print(json.dumps({"documents": sorted({r["version_path"] for r in runs}), "runs": len(runs)}, ensure_ascii=False, indent=1))
    return 0


# ─── run ───────────────────────────────────────────────────────────────────

def build_sandbox(exp: Path, run: dict, mode: str, camp: Path) -> tuple[Path, dict]:
    frozen = exp / "frozen"
    key = run["doc_key"]
    run_dir = camp / "runs" / key / f"{run['arm']}_r{run['repeat']}"
    if run_dir.exists():
        raise FileExistsError(f"{run_dir} уже существует: повтор не перезапускается в том же каталоге")
    (run_dir / "receipts").mkdir(parents=True)
    (run_dir / "tmp").mkdir()
    data = run_dir / "data"
    shutil.copytree(exp / "inputs" / key / "pack" / "projects_v2", data / "projects_v2")
    shutil.copytree(frozen / "prompts", data / "prompts")
    (data / "norms").mkdir()
    shutil.copy2(frozen / "data_norms" / "norms_paragraphs.json", data / "norms" / "norms_paragraphs.json")
    (data / "knowledge_base").mkdir()
    (data / "logs").mkdir()
    shutil.copytree(frozen / "app_data", run_dir / "app_data")
    (run_dir / "norms_cache").mkdir()
    shutil.copy2(frozen / "norms_runtime" / "norms_paragraphs.json", run_dir / "norms_cache" / "norms_paragraphs.json")
    for root in (data, run_dir / "app_data", run_dir / "norms_cache"):
        for p in [root, *root.rglob("*")]:
            if not p.is_symlink():
                p.chmod(p.stat().st_mode | 0o200)
    version_dir = data / "projects_v2" / "objects" / run["version_path"]
    obj = _json(data / "projects_v2" / "objects" / Path(run["version_path"]).parts[0] / "object.json")
    spec = {
        "run_dir": str(run_dir), "mode": mode, "arm": run["arm"], "repeat": run["repeat"], "doc_key": key,
        "project_id": Path(run["version_path"]).parts[4], "version_id": Path(run["version_path"]).parts[6],
        "object_id": obj.get("object_id"), "version_dir": str(version_dir),
        "output_dir": str(version_dir / "03_analysis" / "latest"), "data_dir": str(data),
        "app_data_dir": str(run_dir / "app_data"), "code_root": str(frozen / "code"),
        "forbidden_read_roots": list(FORBIDDEN_READ_ROOTS),
        "fake_seed": str(exp / "inputs" / key / "fake_seed.json") if mode == "dry" else None,
        "allowed_exec_names": ["codex", "claude"] if mode == "real" else [],
    }
    _write(run_dir / "spec.json", spec)
    tools = frozen / "norms_runtime" / "tools"
    flags = _json(frozen / "service_flags.json")["flags"]
    env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": os.environ.get("HOME", "/home/coder"),
           "LANG": "C.UTF-8", "PYTHONPATH": str(frozen / "code"), "TMPDIR": str(run_dir / "tmp"),
           **flags, **FORCED_ENV,
           "AUDIT_DATA_DIR": str(data), "AUDIT_APP_DATA_DIR": str(run_dir / "app_data"),
           "AUDIT_PROMPTS_DIR": str(data / "prompts"),
           "NORMS_TOOLS_PATH": str(tools), "NORMS_STATUS_INDEX_PATH": str(tools / "status_index.json"),
           "NORMS_VAULT_PATH": str(frozen / "norms_runtime" / "vault"),
           "NORMS_MCP_PYTHON": str(tools / "venv" / "bin" / "python"),
           "NORMS_PARAGRAPHS_PATH": str(run_dir / "norms_cache" / "norms_paragraphs.json")}
    return run_dir, env


def cmd_run(args) -> int:
    exp = args.exp.resolve()
    camp = exp / "campaigns" / args.campaign
    plan = _json(camp / "plan.json")
    if args.mode == "real":
        token = os.environ.get("AIM_ALLOW_MODEL_CALLS")
        if not args.permission_token or token != args.permission_token:
            print("боевой режим требует --permission-token и такой же AIM_ALLOW_MODEL_CALLS")
            return 3
    pre = verify_frozen(exp)
    if not (pre["trees_equal"] and pre["status_index_equal"]):
        print(f"замороженные копии изменились: {pre}")
        return 4
    plan["mode"] = args.mode
    results = []
    runs = plan["runs"][args.skip: args.skip + args.limit] if args.limit else plan["runs"][args.skip:]
    for run in runs:
        run_dir, env = build_sandbox(exp, run, args.mode, camp)
        if args.mode == "real":
            env["AIM_ALLOW_MODEL_CALLS"] = args.permission_token
            env["PATH"] = os.environ.get("PATH", env["PATH"])
        t = time.time()
        # Запуск по пути файла, а не через -m: -m импортирует пакеты backend раньше сторожа.
        child = exp / "frozen" / "code" / "backend/app/services/algorithm_improvement/stand/child.py"
        proc = subprocess.run(
            [sys.executable, "-B", str(child), str(run_dir / "spec.json")],
            cwd=str(exp / "frozen" / "code"), env=env, capture_output=True, text=True, timeout=args.timeout)
        (run_dir / "receipts" / "child_stdout.txt").write_text(proc.stdout[-200000:], encoding="utf-8")
        (run_dir / "receipts" / "child_stderr.txt").write_text(proc.stderr[-200000:], encoding="utf-8")
        results.append({**run, "exit_code": proc.returncode, "seconds": round(time.time() - t, 1)})
        print(f"{run['doc_key']} {run['arm']}_r{run['repeat']}: exit {proc.returncode}, {round(time.time() - t)} с")
    post = verify_frozen(exp)
    _write(camp / f"run_{args.mode}_{int(time.time())}.json",
           {"mode": args.mode, "started_from_plan": plan["created_at_utc"], "results": results,
            "frozen_before": pre, "frozen_after": post})
    return 0 if all(r["exit_code"] == 0 for r in results) and post["trees_equal"] else 1


# ─── score ─────────────────────────────────────────────────────────────────

def cmd_score(args) -> int:
    from backend.app.services.algorithm_improvement.stand.score import score_run, summarize
    exp = args.exp.resolve()
    camp = exp / "campaigns" / args.campaign
    plan = _json(camp / "plan.json")
    scores = []
    for run in plan["runs"]:
        run_dir = camp / "runs" / run["doc_key"] / f"{run['arm']}_r{run['repeat']}"
        if not (run_dir / "receipts" / "run.json").is_file():
            continue
        known = _json(exp / "inputs" / run["doc_key"] / "known.json")
        s = score_run(run_dir, known)
        violations = (run_dir / "receipts" / "violations.jsonl")
        s.update({"doc_key": run["doc_key"], "arm": run["arm"], "repeat": run["repeat"], "role": run["role"],
                  "violations": violations.read_text(encoding="utf-8").count("\n") if violations.is_file() else 0})
        pcs = known.get("positive_controls") or []
        if pcs:
            from backend.app.services.algorithm_improvement import docnorm_match as M
            after = json.loads((run_dir / "result" / "after_verify.json").read_text(encoding="utf-8"))["findings"]
            targets = [f for f in after if f.get("source_stage") == "alia_docnorm_audit"]
            res = M.match(targets, [{**(c["finding"] or {}), "id": c["obs_id"]} for c in pcs if c.get("finding")])
            s["positive_controls"] = res["right"]
        scores.append(s)
    plan["mode"] = plan.get("mode") or args.mode_hint
    summary = summarize(camp, scores, plan)
    summary["integrity"] = verify_frozen(exp)
    summary["violations_total"] = sum(s["violations"] for s in scores)
    _write(camp / "scores.json", scores)
    _write(camp / "summary.json", summary)
    print(json.dumps({k: summary[k] for k in ("runs", "statuses", "violations_total", "calls_per_run", "integrity")},
                     ensure_ascii=False, indent=1))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("freeze")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--release", type=Path, default=DEFAULT_RELEASE)
    p.add_argument("--shared-norms", type=Path, default=DEFAULT_SHARED_NORMS)
    p.add_argument("--prompts", type=Path, default=DEFAULT_PROMPTS)
    p.add_argument("--app-data", type=Path, default=DEFAULT_APP_DATA)
    p.add_argument("--data-norms", type=Path, default=DEFAULT_DATA_NORMS)
    p.add_argument("--service-pid", type=int, default=0)
    p.set_defaults(fn=cmd_freeze)
    p = sub.add_parser("refreeze-overlay")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--reason", required=True)
    p.set_defaults(fn=cmd_refreeze_overlay)
    p = sub.add_parser("freeze-inputs")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--objects-root", type=Path, default=DEFAULT_OBJECTS)
    p.add_argument("--version", action="append", required=True, help="путь версии от objects/")
    p.set_defaults(fn=cmd_freeze_inputs)
    p = sub.add_parser("plan-calibration")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--objects-root", type=Path, default=DEFAULT_OBJECTS)
    p.add_argument("--campaign", default="calibration_v1")
    p.set_defaults(fn=cmd_plan_calibration)
    p = sub.add_parser("run")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--campaign", required=True)
    p.add_argument("--mode", choices=("dry", "real"), required=True)
    p.add_argument("--permission-token", default=None)
    p.add_argument("--timeout", type=int, default=3600)
    p.add_argument("--limit", type=int, default=0, help="только N повторов плана (для отладки)")
    p.add_argument("--skip", type=int, default=0, help="пропустить первые N повторов плана")
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("score")
    p.add_argument("--exp", type=Path, required=True)
    p.add_argument("--campaign", required=True)
    p.add_argument("--mode-hint", default="dry")
    p.set_defaults(fn=cmd_score)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
