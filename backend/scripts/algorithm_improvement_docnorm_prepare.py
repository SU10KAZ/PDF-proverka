#!/usr/bin/env python3
"""Заморозка набора для эксперимента по проходу `alia_docnorm_audit` (АР).

Только чтение боевых данных, без вызовов моделей. Пишет в --out:

  manifest.json      зафиксированный набор: версии, проверенный прогон, хэши общих
                     входов, разбиение dev/контроль, глобальные закрепления
  norms_db_snapshot.json  копия базы норм, которую получат оба плеча
  MANIFEST.sha256    хэш манифеста (после заморозки файл не меняется)

Положительные контроли подбираются отдельно (positive_controls.json) — после
заморозки варианта промпта.

Пример:
  python backend/scripts/algorithm_improvement_docnorm_prepare.py \\
      --ledger comparison/algorithm_improvement/docnorm_exp_v1/data/ledger_all/AR/ledger.jsonl \\
      --objects-root /srv/auditmanager/data/projects_v2/objects \\
      --release /home/coder/auditmanager/current \\
      --app-data /srv/auditmanager/data/backend/app/data \\
      --out comparison/algorithm_improvement/docnorm_exp_v1
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.algorithm_improvement import docnorm_variant as V  # noqa: E402
from backend.app.services.algorithm_improvement.ledger import sha256_file  # noqa: E402
from backend.app.services.storage.projects_v2_source_resolver import (  # noqa: E402
    resolve_version_source_files,
)

DOCNORM = "alia_docnorm_audit"
SPLIT_SALT = "docnorm_exp_v1"
CONTROL_SHARE = 0.4
TARGETED_FILES = (
    "03_findings_targeted_alia_docnorm_audit.json",
    "03_findings_targeted_alia_ar_masonry_audit.json",
    "03_findings_targeted_alia_mark_system_audit.json",
)
SHARED_INPUTS = (
    "03_findings_codex_base.json",          # база свода = «уже найденные Codex findings» для docnorm
    "03_findings_targeted_alia_ar_masonry_audit.json",
    "03_findings_targeted_alia_mark_system_audit.json",
    "01_blocks_analysis.json",              # нужен enforce_stage01_atomicity
    "02_text_analysis.json",
    "document_graph.json",
)


def _load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _items(data) -> dict[str, dict]:
    if isinstance(data, dict):
        data = data.get("findings", data.get("items", []))
    return {str(x["id"]): x for x in data or [] if isinstance(x, dict) and x.get("id")}


def reviewed_run(vdir: Path, decided: dict[str, str]) -> tuple[Path | None, float]:
    """Прогон, чьи замечания совпадают с теми, по которым есть решения (по item_id и тексту)."""
    best, best_score = None, -1.0
    for run in sorted(p for p in (vdir / "03_analysis" / "runs").glob("*") if p.is_dir()):
        if not all((run / name).is_file() for name in ("03_findings_codex_base.json", TARGETED_FILES[0])):
            continue
        items = _items(_load(run / "03_findings.json"))
        hits = sum(1 for iid, problem in decided.items()
                   if str((items.get(iid) or {}).get("problem") or "").strip()[:200] == problem[:200])
        score = hits / max(1, len(decided))
        if score > best_score:
            best, best_score = run, score
    return best, round(best_score, 3)


def family_rank(family: str) -> str:
    return hashlib.sha256(f"{SPLIT_SALT}|{family}".encode("utf-8")).hexdigest()


def split_families(families_by_object: dict[str, list[str]], *, forced_control: set[str]) -> dict[str, str]:
    """Детерминированное разбиение: в каждом объекте ⌈40 %⌉ семейств — контроль."""
    out: dict[str, str] = {}
    for obj, fams in sorted(families_by_object.items()):
        ordered = sorted(set(fams), key=family_rank)
        n_control = math.ceil(CONTROL_SHARE * len(ordered))
        control = set(f for f in ordered if f in forced_control)
        for f in ordered:
            if len(control) >= n_control:
                break
            control.add(f)
        for f in ordered:
            out[f] = "control" if f in control else "dev"
    return out


def codex_cli_version() -> dict:
    try:
        from backend.app.services.llm.codex_runner import find_codex_cli
        cli = find_codex_cli()
        ver = subprocess.run([str(cli), "--version"], capture_output=True, text=True, timeout=20).stdout.strip()
        return {"path": str(cli), "version": ver}
    except Exception as exc:  # фиксируем как есть: версия CLI снимается повторно перед запуском
        return {"error": f"{type(exc).__name__}: {exc}"}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ledger", type=Path, required=True)
    ap.add_argument("--objects-root", type=Path, required=True)
    ap.add_argument("--release", type=Path, required=True, help="боевой релиз (current)")
    ap.add_argument("--app-data", type=Path, required=True, help="боевой каталог app data")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--forced-control", type=Path, default=None,
                    help="JSON-список семейств, которые обязаны попасть в контроль (семейства положительных контролей)")
    args = ap.parse_args(argv)

    objects_root = args.objects_root.resolve()
    out = args.out.resolve()
    if out == objects_root or objects_root in out.parents:
        ap.error("--out не может лежать внутри боевых данных")
    if (out / "MANIFEST.sha256").exists():
        ap.error(f"манифест уже заморожен: {out / 'MANIFEST.sha256'} — новая заморозка только в новый каталог")
    out.mkdir(parents=True, exist_ok=True)

    obs = [json.loads(line) for line in args.ledger.read_text(encoding="utf-8").splitlines() if line.strip()]
    by_version: dict[tuple, list[dict]] = defaultdict(list)
    for o in obs:
        by_version[(o["object"], o["document"], o["version"])].append(o)

    entries, families_by_object = [], defaultdict(list)
    for (obj, doc, ver), rows in sorted(by_version.items()):
        target = [o for o in rows if o["origin"] == DOCNORM and o["provenance"] == "human"
                  and o["decision"] == "rejected" and o["link"] == "verified"]
        if not target:
            continue
        vdir = next(objects_root.glob(f"{obj}/disciplines/*/documents/{doc}/versions/{ver}"))
        decided = {o["item_id"]: str((o["finding"] or {}).get("problem") or "").strip()
                   for o in rows if o["finding"] and o["decision"]}
        run, agreement = reviewed_run(vdir, decided)
        info = _load(vdir / "01_input" / "project_info.json") or _load(vdir / "project_info.json") or {}
        sources = resolve_version_source_files(vdir, doc, project_info=info)
        md_path = sources.md_path
        shared = {name: {"path": str((run / name).relative_to(objects_root)) if run else None,
                         "sha256": sha256_file(run / name) if run and (run / name).is_file() else None}
                  for name in SHARED_INPUTS}
        entries.append({
            "object": obj, "document": doc, "version": ver, "family": rows[0]["family"],
            "version_path": str(vdir.relative_to(objects_root)),
            "reviewed_run": run.name if run else None, "reviewed_run_agreement": agreement,
            "md": {"path": str(md_path.relative_to(objects_root)) if md_path else None,
                   "sha256": sha256_file(md_path) if md_path else None},
            "project_info_sha256": sha256_file(vdir / "01_input" / "project_info.json"),
            "shared_inputs": shared,
            "archived_docnorm_output_sha256": sha256_file(run / TARGETED_FILES[0]) if run else None,
            "known": {
                "docnorm_rejected_verified": [o["obs_id"] for o in target],
                "docnorm_rejected_probable": [o["obs_id"] for o in rows if o["origin"] == DOCNORM
                                              and o["provenance"] == "human" and o["link"] == "unverified_stable"],
                "other_accepted_verified": [o["obs_id"] for o in rows if o["origin"] != DOCNORM
                                            and o["provenance"] == "human" and o["decision"] == "accepted"
                                            and o["link"] == "verified"],
                "other_rejected_verified": [o["obs_id"] for o in rows if o["origin"] != DOCNORM
                                            and o["provenance"] == "human" and o["decision"] == "rejected"
                                            and o["link"] == "verified"],
            },
        })
        families_by_object[obj[:3]].append(rows[0]["family"])

    forced = set(json.loads(args.forced_control.read_text(encoding="utf-8"))) if args.forced_control else set()
    split = split_families(families_by_object, forced_control=forced)
    for e in entries:
        e["split"] = split[e["family"]]

    release = args.release.resolve()
    norms_db = release / "app" / "norms" / "norms_db.json"
    shutil.copy2(norms_db, out / "norms_db_snapshot.json")
    stage_models = _load(args.app_data / "stage_models.json") or {}
    manifest = {
        "experiment": "docnorm_exp_v1",
        "frozen_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_head": subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True,
                                   text=True).stdout.strip(),
        "release": release.name,
        "arms": {
            "A": {"prompt": "боевой _docnorm_messages (release == checkout)",
                  "builder_module_sha256": sha256_file(
                      release / "app/backend/app/pipeline/stages/prepare/codex_targeted_findings.py")},
            "B": {"prompt": V.VARIANT_ID, "variant_sha256": V.variant_sha256()},
        },
        "model": {"findings_merge": stage_models.get("findings_merge"), "norm_fix": stage_models.get("norm_fix"),
                  "stage_models_sha256": sha256_file(args.app_data / "stage_models.json"),
                  "codex_model_substitution_sha256": sha256_file(args.app_data / "codex_model_substitution.json")},
        "codex_cli": codex_cli_version(),
        "norms_db": {"source": str(norms_db), "sha256": sha256_file(out / "norms_db_snapshot.json")},
        "split_rule": {"salt": SPLIT_SALT, "control_share": CONTROL_SHARE, "unit": "family",
                       "stratified_by": "object", "forced_control": sorted(forced)},
        "counts": {
            "versions": len(entries),
            "dev": sum(1 for e in entries if e["split"] == "dev"),
            "control": sum(1 for e in entries if e["split"] == "control"),
            "docnorm_rejected_verified": sum(len(e["known"]["docnorm_rejected_verified"]) for e in entries),
            "other_accepted_verified": sum(len(e["known"]["other_accepted_verified"]) for e in entries),
        },
        "entries": entries,
    }
    text = json.dumps(manifest, ensure_ascii=False, indent=1)
    (out / "manifest.json").write_text(text, encoding="utf-8")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    (out / "MANIFEST.sha256").write_text(f"{digest}  manifest.json\n", encoding="utf-8")
    print(json.dumps(manifest["counts"], ensure_ascii=False), digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
