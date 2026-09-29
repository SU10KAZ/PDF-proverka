"""Frozen bench for ProjectChange V3 deterministic layers (0 model calls).

Replays new deterministic passes over frozen V3 results of the DEV5 pair
(Astra a3672d0c, Opus 5 a631b49a, Opus 5.5 caf4e5d9) and the frozen DEV5
source package, and compares with the 29.09 review labels.

    python scripts/projectchange_frozen_bench.py dedupe
    python scripts/projectchange_frozen_bench.py checks [--json OUT]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CA = Path("/home/coder/auditmanager/corpus-audits")
SOURCE = CA / "20260929_v3_dev5_opus55/artifacts/source"
RUNS = {
    "ASTRA": {
        "checkpoint": CA / "20260924_dev5_astra_v31_final_freeze/frozen/miner/checkpoint_14_regions.json",
        "dedupe": CA / "20260924_dev5_astra_v31_final_freeze/DEDUPE_RAW_RESPONSE_1.json",
        "result": CA / "20260924_dev5_astra_v31_final_freeze/FINAL_RESULT.json",
        "review": CA / "20260929_dev5_uniform_rereview/astra",
    },
    "OPUS5": {
        "miner": CA / "20260921_v3_dev5_opus_freeze_a631b49aaaac4db0af66a495c155c629/artifacts/project_change_v3_miner_results.json",
        "result": CA / "20260921_v3_dev5_opus_freeze_a631b49aaaac4db0af66a495c155c629/artifacts/project_change_v3_result.json",
        "review": CA / "20260929_dev5_uniform_rereview/opus5",
    },
    "OPUS55": {
        "miner": CA / "20260929_v3_dev5_opus55/resume_2/artifacts/project_change_v3_miner_results.json",
        "result": CA / "20260929_v3_dev5_opus55/resume_2/artifacts/project_change_v3_result.json",
        "review": CA / "20260929_dev5_opus55_quality_review",
    },
}


def J(path: Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_run(name: str) -> dict:
    spec = RUNS[name]
    result = J(spec["result"])
    if name == "ASTRA":
        regions = [r["result"] for r in J(spec["checkpoint"])["regions"]]
        changes = [c for r in regions for c in r["projectchanges"]]
        dedupe_raw = J(spec["dedupe"])["parsed"]
    else:
        miner = J(spec["miner"])
        changes = miner["projectchanges"]
        dedupe_raw = result["dedupe"]
    return {"name": name, "changes": changes, "dedupe_raw": dedupe_raw, "result": result}


def load_pages() -> dict:
    pages = {}
    for side in ("old", "new"):
        for path in sorted((SOURCE / side).glob("p*/page.json")):
            page = J(path)
            pages[(page["side"], int(page["physical_page"]))] = page
    return pages


def ordinal_by_id(name: str) -> dict[str, int]:
    """Card ordinals used by the 29.09 reviews."""
    spec = RUNS[name]
    rows = J(spec["review"] / "INPUT_CARDS.json")
    return {row["projectchange_id"]: row["ordinal"] for row in rows}


def cmd_dedupe(_args) -> int:
    from backend.app.services.project_change_v3.dedupe import apply_dedupe

    for name in RUNS:
        run = load_run(name)
        legacy = apply_dedupe("bench", run["changes"], run["dedupe_raw"])
        lossless = apply_dedupe("bench", run["changes"], run["dedupe_raw"], lossless=True)
        assert [c["projectchange_id"] for c in legacy] == [c["projectchange_id"] for c in run["result"]["projectchanges"]], name
        ords = ordinal_by_id(name)
        merges = [c for c in lossless if len(c["dedupe_lineage"]) > 1]
        print(f"== {name}: {len(run['changes'])} → {len(lossless)} карточек, слияний {len(merges)}")
        for old, new in zip(legacy, lossless):
            if len(new["dedupe_lineage"]) < 2:
                continue
            ordinal = ords.get(old["projectchange_id"], "?")
            flag = "ГЛАВНАЯ СМЕНЕНА" if new["dedupe_canonical_reselected"] else "главная та же"
            residual = sum(len(v) for v in new["dedupe_residual"].values())
            params = (len(old["changed_parameters"]), len(new["changed_parameters"]))
            print(f"  #{ordinal} {old['projectchange_id']} → {new['projectchange_id']} | {flag} | "
                  f"остаток {residual} ток. | параметров {params[0]}→{params[1]} | {new['engineering_subject'][:70]}")
    return 0


def review_labels(name: str) -> dict[str, dict]:
    """support / error types per projectchange_id from the 29.09 reviews (after adjudication)."""
    ords = ordinal_by_id(name)
    by_ord: dict[int, dict] = {}
    if name == "OPUS55":
        for f in sorted((RUNS[name]["review"] / "batches").glob("BATCH_*.json")):
            for c in J(f)["cards"]:
                by_ord[c["ordinal"]] = {"support": c["support"], "errors": c.get("error_types") or []}
    else:
        final = J(CA / "20260929_dev5_uniform_rereview/FINAL_UNIFORM_ADJUDICATED.json")["astra" if name == "ASTRA" else "opus5"]
        by_ord = {int(k): v for k, v in final.items()}
    return {pid: {"ordinal": o, **by_ord.get(o, {})} for pid, o in ords.items()}


def cmd_checks(args) -> int:
    from backend.app.services.project_change_v3.source_checks import FLAGS, run_source_checks

    pages = load_pages()
    report = {}
    for name in RUNS:
        run = load_run(name)
        labels = review_labels(name)
        res = run_source_checks(projectchanges=run["result"]["projectchanges"],
                                unresolved_hints=run["result"].get("unresolved_hints") or [],
                                page_records=pages, enabled={k: True for k in FLAGS})
        wrong_old = {pid for pid, l in labels.items() if "WRONG_OLD_STATE" in (l.get("errors") or [])}
        flagged_absence = {f["projectchange_id"] for f in res["findings"] if f["check"] == "ABSENCE_CONTRADICTED"}
        print(f"== {name}: находок {res['summary']['by_check']}; WRONG_OLD_STATE по ревью {len(wrong_old)}, "
              f"из них пойманы проверкой отсутствия {len(wrong_old & flagged_absence)}")
        for f in res["findings"]:
            l = labels.get(f.get("projectchange_id") or "", {})
            tag = f"#{l.get('ordinal', '-')} {str(l.get('support', ''))[:4]} {','.join(l.get('errors') or [])}" if l else "—"
            print(f"  {f['check']:<22} {tag:<45} | {f['claim'][:110]}")
            if args.verbose:
                for s in f["source"][:2]:
                    print(f"      {s['side']}{s['physical_page']}: {s['quote'][:150]}")
        report[name] = res
    if args.json:
        Path(args.json).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("dedupe")
    chk = sub.add_parser("checks")
    chk.add_argument("--json")
    chk.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    return {"dedupe": cmd_dedupe, "checks": cmd_checks}[args.cmd](args)


if __name__ == "__main__":
    raise SystemExit(main())
