#!/usr/bin/env python3
"""ARM 2 — Pipeline A (compare, escalate to Vision on a difficult diff) versus
Pipeline B (verify each description against its crop, then compare) on the 10 Track A pairs.

Research only. Writes only inside
experiments/stage_comparison_vector_architecture_opus/artifacts/.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path("/home/coder/projects/PDF-proverka")
sys.path.insert(0, str(ROOT))

from experiments.stage_comparison_vector_architecture_opus.probes import vv_harness as vv  # noqa: E402

EXP = ROOT / "experiments" / "stage_comparison_vector_architecture_opus"
ART = EXP / "artifacts"
TRACK_A = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"

VERIFY_DIR = ART / "vvp_verify"
VISION_DIR = ART / "vvp_vision"
SHEETS_JSON = ART / "vvp_sheets.json"

PAIR_ORDER = [
    "ss_simple_node", "ar_plan", "ar_wall_sections", "ss_plan_dense", "ss_table_graphic",
    "vk_plan", "vk_node_plan", "vk_nodes", "ss_scheme_text_changed", "eom_singleline_changed",
]


# ------------------------------------------------------------------ loading

def load_pairs() -> dict[str, dict[str, Any]]:
    data = json.loads((TRACK_A / "block_pairs.json").read_text(encoding="utf-8"))
    return {p["pair_id"]: p for p in data["pairs"]}


def load_comparison(pair_id: str) -> dict[str, Any]:
    return json.loads((TRACK_A / "comparisons" / pair_id / "comparison.json").read_text(encoding="utf-8"))


def load_description(pair_id: str, side: str) -> dict[str, Any]:
    return json.loads((TRACK_A / "descriptions" / pair_id / side / "vector_block.json").read_text(encoding="utf-8"))


# ------------------------------------------------- Pipeline A difficulty gate

def difficulty(pair_id: str) -> dict[str, Any]:
    """DETERMINISTIC 'difficult diff' predicate for Pipeline A.

    Fires when the deterministic comparison cannot be trusted on its own:

      D0  status == INSUFFICIENT_VECTOR_DATA
      D1  status != IDENTICAL  and  text layer not reliable
      D2  either side's vector_quality != GOOD  (extractor admits truncation / too little data)
      D3  segment coverage never cleared 0.985 at any tolerance
      D4  status != IDENTICAL and coverage only cleared at the coarsest 1.0 % tolerance

    escalate := D0 or D1 or D2 or D3 or D4
    """
    cmp_ = load_comparison(pair_id)
    left = load_description(pair_id, "left")
    right = load_description(pair_id, "right")
    status = cmp_["status"]
    geo = cmp_["geometry"]
    reasons = []
    if status == "INSUFFICIENT_VECTOR_DATA":
        reasons.append("D0:insufficient_vector_data")
    if status != "IDENTICAL" and not cmp_["text"]["reliable"]:
        reasons.append("D1:text_layer_unreliable")
    sides = {"left": left, "right": right}
    bad = [s for s in ("left", "right") if sides[s]["vector_quality"] != "GOOD"]
    if bad:
        reasons.append("D2:quality_not_good(" + ",".join(f"{s}:{sides[s]['vector_quality']}" for s in bad) + ")")
    if geo["similarity"] < 0.985:
        reasons.append(f"D3:coverage_never_cleared({geo['similarity']:.3f})")
    if status != "IDENTICAL" and float(geo["selected_tolerance"]) >= 0.01:
        reasons.append("D4:coarsest_tolerance_only")
    return {"pair_id": pair_id, "status": status, "escalate": bool(reasons), "reasons": reasons}


# ------------------------------------------------------------- vision prompts

COMPARE_PROMPT = """You are comparing TWO raster crops of the SAME block of a Russian engineering drawing: ./left.png is the earlier version, ./right.png is the later version. Read BOTH image files with the Read tool.

{context}

Your job: state what CHANGED from left to right, the way a Russian design-documentation expert would write it — «Добавлены два ответвления», «Количество аппаратов 12 -> 14», «Номинал 250 -> 315 A», «Появился новый проём». Never write «добавлено 37 line segments».

RULES:
- Do NOT invent coordinates. Do NOT invent exact numbers you cannot read in the picture.
- If the two crops differ only in padding / how much of the sheet the crop caught, say so with the "crop" flag instead of calling it a project change.
- List at most 8 changes, most important first. If nothing changed, return an empty list.
- Write each change in Russian, one short sentence.

Classify the pair with exactly one of:
  IDENTICAL - the same drawing, no visible difference at all
  NEAR_IDENTICAL - the same drawing, only presentation/crop noise
  STRUCTURE_SAME_VALUES_CHANGED - same layout and connections, but designations/values/labels changed
  STRUCTURE_CHANGED - the drawing itself was redrawn: elements added/removed, topology different

Answer with a single JSON object and nothing else:
{{"verdict": "IDENTICAL"|"NEAR_IDENTICAL"|"STRUCTURE_SAME_VALUES_CHANGED"|"STRUCTURE_CHANGED", "changes": [{{"text": "...", "kind": "project"|"crop"|"presentation", "confidence": "high"|"medium"|"low"}}], "confidence": "high"|"medium"|"low"}}"""

GAPFILL_PROMPT = """You are comparing TWO raster crops of the SAME block of a Russian engineering drawing: ./left.png is the earlier version, ./right.png is the later version. Read BOTH image files with the Read tool.

A deterministic vector comparison of the two versions has ALREADY been run. It produced this verdict and these difference lines:

VERDICT: {status}
{diff_lines}

A separate verification step checked each side's machine description against its picture and reported these NAMED GAPS - things visible in the picture that the description does not cover, or claims it could not confirm:

{gaps}

Your job is NARROW: look at the two pictures and resolve ONLY those named gaps. Say whether the thing named in each gap is the SAME in both versions or DIFFERENT, and if different, how.

RULES:
- Do NOT re-describe the drawings. Do NOT produce your own full inventory.
- Do NOT invent coordinates and do NOT invent exact numbers you cannot read.
- Address only the named gaps. If a gap turns out to be the same on both sides, say so.
- Write in Russian, one short sentence per gap.

Answer with a single JSON object and nothing else:
{{"verdict_after_gapfill": "IDENTICAL"|"NEAR_IDENTICAL"|"STRUCTURE_SAME_VALUES_CHANGED"|"STRUCTURE_CHANGED", "gap_findings": [{{"gap": "...", "same_in_both": true|false, "text": "...", "kind": "project"|"crop"|"presentation"}}], "confidence": "high"|"medium"|"low"}}"""


def _call_claude(prompt: str, files: dict[str, Path], out_json: Path | None,
                 *, timeout: int = 420, retries: int = 1) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    for attempt in range(retries + 1):
        workdir = Path(tempfile.mkdtemp(prefix="vvp_"))
        try:
            for name, path in files.items():
                shutil.copy2(path, workdir / name)
            cmd = ["claude", "-p", prompt, "--allowed-tools", "Read", "--output-format", "json"]
            started = time.time()
            with open(os.devnull, "rb") as devnull:
                proc = subprocess.run(cmd, cwd=workdir, stdin=devnull, capture_output=True,
                                      text=True, timeout=timeout)
            wall = time.time() - started
            envelope = None
            try:
                envelope = json.loads(proc.stdout)
            except Exception:
                pass
            result_text = (envelope or {}).get("result") if isinstance(envelope, dict) else None
            parsed = vv.parse_verdict(result_text or proc.stdout)
            if parsed is None:
                parsed = _parse_any_json(result_text or proc.stdout)
            attempts.append({
                "attempt": attempt,
                "returncode": proc.returncode,
                "wall_seconds": round(wall, 2),
                "duration_ms": (envelope or {}).get("duration_ms") if isinstance(envelope, dict) else None,
                "usage_raw": (envelope or {}).get("usage") if isinstance(envelope, dict) else None,
                "model_text": result_text,
                "stderr_tail": proc.stderr[-800:] if proc.stderr else "",
                "parsed": parsed,
            })
            if proc.returncode == 0 and parsed is not None:
                break
        except subprocess.TimeoutExpired:
            attempts.append({"attempt": attempt, "returncode": None, "error": "timeout",
                             "wall_seconds": timeout})
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
    last = attempts[-1]
    usage = last.get("usage_raw")
    record = {
        "attempts": attempts,
        "ok": bool(last.get("parsed")),
        "answer": last.get("parsed"),
        "usage_raw": usage,
        "usage_payload_attributable": vv._payload_tokens(usage),
        "usage_note": ("cache_read_input_tokens is dominated by the Claude Code system prompt "
                       "(~50k) and is NOT attributable to our payload; "
                       "usage_payload_attributable = input + cache_creation + output"),
        "duration_ms": last.get("duration_ms"),
        "wall_seconds": last.get("wall_seconds"),
        "prompt": prompt,
        "files": {k: str(v) for k, v in files.items()},
        "file_bytes": {k: Path(v).stat().st_size for k, v in files.items()},
    }
    if out_json:
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record


def _parse_any_json(text: str | None) -> dict[str, Any] | None:
    if not text:
        return None
    import re
    for candidate in re.findall(r"\{.*\}", text, re.DOTALL):
        try:
            value = json.loads(candidate)
        except Exception:
            continue
        if isinstance(value, dict) and ("verdict" in value or "verdict_after_gapfill" in value):
            return value
    return None


# ------------------------------------------------------------------ commands

def cmd_sheets(_args) -> None:
    out: dict[str, Any] = {}
    for pair_id in PAIR_ORDER:
        out[pair_id] = {}
        for side in ("left", "right"):
            desc = load_description(pair_id, side)
            sheet = vv.fact_sheet(desc, disclose_limits=True)
            out[pair_id][side] = sheet
            print(f"--- {pair_id}:{side}  ({sheet['characters']} chars, {len(sheet['claims'])} claims)")
            print(sheet["text"])
            print()
    SHEETS_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("wrote", SHEETS_JSON)


def cmd_gate(_args) -> None:
    rows = [difficulty(p) for p in PAIR_ORDER]
    (ART / "vvp_gate.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for r in rows:
        print(f"{r['pair_id']:26s} {r['status']:30s} escalate={r['escalate']!s:5s} {r['reasons']}")


def _verify_one(job: tuple[str, str]) -> tuple[str, dict[str, Any]]:
    pair_id, side = job
    sheets = json.loads(SHEETS_JSON.read_text(encoding="utf-8"))
    sheet = sheets[pair_id][side]
    crop = vv.crop_for(pair_id, side)
    key = f"{pair_id}__{side}"
    rec = vv.verify(crop, sheet, VERIFY_DIR / f"{key}.json", timeout=420, retries=1)
    return key, rec


def cmd_verify(args) -> None:
    jobs = [(p, s) for p in (args.pairs or PAIR_ORDER) for s in ("left", "right")]
    jobs = [j for j in jobs if args.force or not (VERIFY_DIR / f"{j[0]}__{j[1]}.json").exists()]
    print(f"{len(jobs)} verification calls, workers={args.workers}")
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for key, rec in pool.map(_verify_one, jobs):
            print(f"  {key:36s} {rec.get('status')!s:10s} ok={rec['ok']} "
                  f"tok={rec.get('usage_payload_attributable')} {rec.get('wall_seconds')}s")
    print(f"wall {time.time()-t0:.1f}s")


def _vision_compare_one(pair_id: str) -> tuple[str, dict[str, Any]]:
    pairs = load_pairs()
    p = pairs[pair_id]
    ctx = (f"Context: discipline {p['discipline']}; block type: {p['type']}. "
           f"left.png is version {p['left']['version']}, right.png is version {p['right']['version']}.")
    prompt = COMPARE_PROMPT.format(context=ctx)
    files = {"left.png": vv.crop_for(pair_id, "left"), "right.png": vv.crop_for(pair_id, "right")}
    rec = _call_claude(prompt, files, VISION_DIR / f"compare__{pair_id}.json")
    return pair_id, rec


def cmd_vision(args) -> None:
    targets = args.pairs or [p for p in PAIR_ORDER if difficulty(p)["escalate"]]
    targets = [t for t in targets if args.force or not (VISION_DIR / f"compare__{t}.json").exists()]
    print(f"{len(targets)} vision-compare calls: {targets}")
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for pair_id, rec in pool.map(_vision_compare_one, targets):
            ans = rec.get("answer") or {}
            print(f"  {pair_id:26s} {ans.get('verdict')!s:32s} changes={len(ans.get('changes') or [])} "
                  f"tok={rec.get('usage_payload_attributable')} {rec.get('wall_seconds')}s")
    print(f"wall {time.time()-t0:.1f}s")


def _gapfill_one(job: tuple[str, list[str]]) -> tuple[str, dict[str, Any]]:
    pair_id, gaps = job
    cmp_ = load_comparison(pair_id)
    lines = "\n".join("- " + d for d in cmp_["differences"][:12]) or "- (no difference lines)"
    prompt = GAPFILL_PROMPT.format(status=cmp_["status"], diff_lines=lines,
                                   gaps="\n".join("- " + g for g in gaps))
    files = {"left.png": vv.crop_for(pair_id, "left"), "right.png": vv.crop_for(pair_id, "right")}
    rec = _call_claude(prompt, files, VISION_DIR / f"gapfill__{pair_id}.json")
    return pair_id, rec


def cmd_gapfill(args) -> None:
    jobs = json.loads(Path(args.jobs).read_text(encoding="utf-8"))
    jobs = [(k, v) for k, v in jobs.items()
            if args.force or not (VISION_DIR / f"gapfill__{k}.json").exists()]
    print(f"{len(jobs)} gap-fill calls: {[j[0] for j in jobs]}")
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for pair_id, rec in pool.map(_gapfill_one, jobs):
            ans = rec.get("answer") or {}
            print(f"  {pair_id:26s} {ans.get('verdict_after_gapfill')!s:32s} "
                  f"findings={len(ans.get('gap_findings') or [])} tok={rec.get('usage_payload_attributable')}")
    print(f"wall {time.time()-t0:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sheets"); s.set_defaults(func=cmd_sheets)
    s = sub.add_parser("gate"); s.set_defaults(func=cmd_gate)
    s = sub.add_parser("verify"); s.add_argument("--pairs", nargs="*"); s.add_argument("--workers", type=int, default=6)
    s.add_argument("--force", action="store_true"); s.set_defaults(func=cmd_verify)
    s = sub.add_parser("vision"); s.add_argument("--pairs", nargs="*"); s.add_argument("--workers", type=int, default=5)
    s.add_argument("--force", action="store_true"); s.set_defaults(func=cmd_vision)
    s = sub.add_parser("gapfill"); s.add_argument("--jobs", required=True); s.add_argument("--workers", type=int, default=5)
    s.add_argument("--force", action="store_true"); s.set_defaults(func=cmd_gapfill)
    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
