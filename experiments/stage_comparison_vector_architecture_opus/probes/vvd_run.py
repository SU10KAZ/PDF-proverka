#!/usr/bin/env python3
"""VVD — ARM 1 runner: run the full vv case manifest through vv_harness.verify.

Every raw call record is stored under ``artifacts/vvd_runs/<tag>/<case_id>.json`` so
nothing has to be re-run.  Resumable: a case whose record already parsed a verdict is
skipped unless ``--force``.

    cd /home/coder/projects/PDF-proverka
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvd_run --tag main
    python -m ...vvd_run --tag main --cases vv001 vv002
    python -m ...vvd_run --tag rotfix --crop-map artifacts/vvd_rotfix_crops.json --cases vv017
"""
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv

RUNS = vv.ARTIFACTS / "vvd_runs"


def run_case(case: dict, manifest: dict, out_dir: Path, crop_map: dict, timeout: int,
             repeat_index: int | None = None) -> dict:
    crop_key = case["case_id"]
    crop_rel = crop_map.get(crop_key) or crop_map.get(case["block"]) or case["crop_png"]
    crop = Path(crop_rel)
    if not crop.is_absolute():
        crop = vv.ROOT / crop
    sheet = {"text": case["fact_sheet"]["text"],
             "characters": case["fact_sheet"]["characters"],
             "block_id": manifest["blocks"][case["block"]]["block_id"]}
    name = case["case_id"] if repeat_index is None else f"{case['case_id']}_r{repeat_index}"
    out = out_dir / f"{name}.json"
    record = vv.verify(crop, sheet, None, timeout=timeout, retries=1)
    record.update({
        "case_id": case["case_id"],
        "record_name": name,
        "block": case["block"],
        "family": case["family"],
        "mutation": case["mutation"],
        "expected_status": case["expected_status"],
        "acceptable_status": case["acceptable_status"],
        "must_name": case["must_name"],
        "changed_claims": case["changed_claims"],
        "disclose_limits": case["disclose_limits"],
        "strength": case.get("strength"),
        "synthetic": case.get("synthetic"),
        "crop_used": str(crop.relative_to(vv.ROOT)) if str(crop).startswith(str(vv.ROOT)) else str(crop),
        "crop_is_manifest_default": str(crop) == str(vv.ROOT / case["crop_png"]),
        "run_tag": out_dir.name,
    })
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="main")
    ap.add_argument("--cases", nargs="*", default=None, help="case ids; default = all 60")
    ap.add_argument("--family", nargs="*", default=None)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--timeout", type=int, default=420)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--crop-map", default=None, help="json {case_id|block: crop path}")
    ap.add_argument("--repeat", type=int, default=None, help="repeat index suffix _rN")
    args = ap.parse_args()

    manifest = json.loads(vv.CASES_JSON.read_text(encoding="utf-8"))
    cases = manifest["cases"]
    if args.cases:
        want = set(args.cases)
        cases = [c for c in cases if c["case_id"] in want]
        missing = want - {c["case_id"] for c in cases}
        if missing:
            raise SystemExit(f"unknown case ids: {sorted(missing)}")
    if args.family:
        cases = [c for c in cases if c["family"] in set(args.family)]

    crop_map = {}
    if args.crop_map:
        p = Path(args.crop_map)
        if not p.is_absolute():
            p = vv.EXP_DIR / args.crop_map
        crop_map = json.loads(p.read_text(encoding="utf-8"))

    out_dir = RUNS / args.tag
    out_dir.mkdir(parents=True, exist_ok=True)

    todo = []
    for c in cases:
        name = c["case_id"] if args.repeat is None else f"{c['case_id']}_r{args.repeat}"
        f = out_dir / f"{name}.json"
        if f.exists() and not args.force:
            try:
                if json.loads(f.read_text(encoding="utf-8")).get("status"):
                    continue
            except Exception:
                pass
        todo.append(c)
    print(f"tag={args.tag} selected={len(cases)} to_run={len(todo)} workers={args.workers}", flush=True)

    started = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=min(args.workers, 6)) as pool:
        futs = {pool.submit(run_case, c, manifest, out_dir, crop_map, args.timeout, args.repeat): c
                for c in todo}
        for fut in as_completed(futs):
            c = futs[fut]
            done += 1
            try:
                r = fut.result()
                print(f"[{done}/{len(todo)}] {c['case_id']} {c['mutation']:16s} "
                      f"-> {r.get('status')} (exp {c['expected_status']}) "
                      f"{r.get('wall_seconds')}s pay={r.get('usage_payload_attributable')}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"[{done}/{len(todo)}] {c['case_id']} ERROR {exc!r}", flush=True)
    print(f"elapsed {time.time()-started:.0f}s", flush=True)


if __name__ == "__main__":
    main()
