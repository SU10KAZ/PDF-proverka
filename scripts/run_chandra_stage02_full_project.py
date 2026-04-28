#!/usr/bin/env python3
"""Run a Chandra vision model over every block in a project and compare against stage-02 baseline."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

import sys

sys.path.insert(0, str(ROOT))

from scripts.run_chandra_stage02_reference_compare import (  # noqa: E402
    _keyword_set,
    _parse_json_block,
    _stage02_prompt,
)
from scripts.run_chandra_v1_diagnostics import (  # noqa: E402
    _chat_image,
    _load_model,
    _unload_all,
)
from scripts.run_chandra_vision_model_eval import (  # noqa: E402
    _load_blocks_index,
    _prepare_images,
    _ts,
)


def _save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_stage02_baseline(project_dir: Path) -> dict[str, dict[str, Any]]:
    path = project_dir / "_output" / "02_blocks_analysis.json"
    obj = json.loads(path.read_text(encoding="utf-8"))
    rows = obj.get("block_analyses") or []
    return {row["block_id"]: row for row in rows if row.get("block_id")}


def _run_one(
    model: str,
    image_path: Path,
    block_id: str,
    page: int,
    label: str,
    timeout: float,
    prompt_variant: str,
    max_output_tokens: int,
) -> dict[str, Any]:
    prompt = _stage02_prompt(block_id, label, page, variant=prompt_variant)
    result = _chat_image(model, image_path, prompt, timeout=timeout, max_output_tokens=max_output_tokens)
    parsed, parse_error = _parse_json_block(result.get("content") or "")
    return {
        **result,
        "parsed": parsed,
        "parse_error": parse_error,
    }


def _baseline_verdict(baseline: dict[str, Any] | None, candidate: dict[str, Any] | None) -> tuple[str, list[str]]:
    notes: list[str] = []
    if not candidate:
        return "fail_parse", ["candidate JSON parse failed"]
    if baseline is None:
        return "baseline_missing", ["no stage-02 baseline block to compare against"]

    base_findings = baseline.get("findings") or []
    cand_findings = candidate.get("findings") or []
    base_kv = baseline.get("key_values_read") or []
    cand_kv = candidate.get("key_values_read") or []
    summary = candidate.get("summary") or ""
    cand_label = candidate.get("label") or ""
    overlap = len(_keyword_set(baseline.get("label") or "") & _keyword_set(cand_label + " " + summary))

    if base_findings and not cand_findings:
        notes.append("baseline has findings, candidate has none")
        return "likely_degraded", notes
    if not base_findings and cand_findings:
        notes.append("candidate found issues where stage-02 baseline had none")
        return "uncertain", notes
    if len(base_kv) >= 6 and len(cand_kv) < max(3, int(0.35 * len(base_kv))):
        notes.append("candidate key_values_read much shorter than stage-02 baseline")
        return "likely_degraded", notes
    if overlap == 0:
        notes.append("candidate label/summary poorly aligned with baseline block type")
        return "uncertain", notes
    if len(cand_findings) > len(base_findings) + 2:
        notes.append("candidate produced noticeably more findings than baseline")
        return "uncertain", notes
    if len(cand_findings) >= len(base_findings) and len(cand_kv) > len(base_kv):
        notes.append("candidate preserved findings presence and captured more key values")
        return "likely_improved", notes
    notes.append("candidate broadly aligned with stage-02 baseline")
    return "equivalent", notes


def _median(values: list[float]) -> float:
    return round(statistics.median(values), 2) if values else 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    ap.add_argument("--image-max-side", type=int, default=1024)
    ap.add_argument("--context-length", type=int, default=8192)
    ap.add_argument("--load-timeout", type=float, default=900)
    ap.add_argument("--chat-timeout", type=float, default=480)
    ap.add_argument("--prompt-variant", default="compact_recall", choices=["standard", "recall_safe", "compact_recall"])
    ap.add_argument("--max-output-tokens", type=int, default=900)
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    exp_dir = project_dir / "_experiments" / "chandra_stage02_full_project" / _ts()
    exp_dir.mkdir(parents=True, exist_ok=True)

    blocks = _load_blocks_index(project_dir)
    block_ids = [b.get("block_id") for b in blocks if b.get("block_id")]
    baseline = _load_stage02_baseline(project_dir)
    prepared = _prepare_images(
        project_dir=project_dir,
        exp_dir=exp_dir / "prepared",
        blocks=blocks,
        block_ids=block_ids,
        native_crops=True,
        native_max_long_side=args.image_max_side,
    )
    prepared_by_id = {img.block_id: img for img in prepared}

    _save_json(exp_dir / "manifest.json", {
        "project_dir": str(project_dir),
        "model": args.model,
        "total_blocks_index": len(block_ids),
        "prepared_blocks": len(prepared_by_id),
        "baseline_blocks": len(baseline),
        "image_max_side": args.image_max_side,
        "api": "/api/v1/chat",
        "reasoning": "off",
        "prompt_variant": args.prompt_variant,
        "max_output_tokens": args.max_output_tokens,
    })

    rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    _unload_all(exp_dir, "initial_cleanup")
    try:
        load = _load_model(args.model, context_length=args.context_length, timeout=args.load_timeout)
        rows.append({"request": "load", "model": args.model, **load.__dict__})
        if not load.ok:
            raise RuntimeError(f"load failed: {load.error}")

        for idx, bid in enumerate(block_ids, start=1):
            img = prepared_by_id[bid]
            raw = _run_one(
                args.model,
                img.image_file,
                bid,
                img.page,
                img.label,
                args.chat_timeout,
                args.prompt_variant,
                args.max_output_tokens,
            )
            raw["model"] = args.model
            raw["block_id"] = bid
            raw["width"] = img.width
            raw["height"] = img.height
            raw["run_index"] = idx
            rows.append(raw)

            candidate = raw.get("parsed")
            base_row = baseline.get(bid)
            verdict, notes = _baseline_verdict(base_row, candidate)
            compare_row = {
                "block_id": bid,
                "page": img.page,
                "label_source": img.label,
                "baseline_present": base_row is not None,
                "baseline_label": base_row.get("label") if base_row else None,
                "candidate_label": candidate.get("label") if candidate else None,
                "baseline_findings": len(base_row.get("findings") or []) if base_row else None,
                "candidate_findings": len(candidate.get("findings") or []) if candidate else 0,
                "baseline_kv": len(base_row.get("key_values_read") or []) if base_row else None,
                "candidate_kv": len(candidate.get("key_values_read") or []) if candidate else 0,
                "elapsed_s": raw.get("elapsed_s"),
                "parse_ok": candidate is not None,
                "verdict": verdict,
                "notes": notes,
                "baseline_summary": base_row.get("summary") if base_row else None,
                "candidate_summary": candidate.get("summary") if candidate else None,
                "candidate_sheet_type": candidate.get("sheet_type") if candidate else None,
                "candidate_unreadable_text": candidate.get("unreadable_text") if candidate else None,
            }
            compare_rows.append(compare_row)

            _save_json(exp_dir / "per_block" / f"{bid}.json", {
                "baseline": base_row,
                "candidate_raw": raw,
                "compare": compare_row,
            })
            if idx % 10 == 0 or idx == len(block_ids):
                _save_json(exp_dir / "candidate_results.json", rows)
                _save_json(exp_dir / "comparison_vs_stage02.json", compare_rows)
                print(f"[{idx}/{len(block_ids)}] parse_ok={sum(1 for row in compare_rows if row['parse_ok'])} findings_blocks={sum(1 for row in compare_rows if row['candidate_findings'] > 0)}", flush=True)
    finally:
        _unload_all(exp_dir, "final_cleanup")

    _save_json(exp_dir / "candidate_results.json", rows)
    _save_json(exp_dir / "comparison_vs_stage02.json", compare_rows)

    candidate_rows = [r for r in rows if r.get("request") == "image_smoke"]
    elapsed_values = [float(r["elapsed_s"]) for r in candidate_rows if isinstance(r.get("elapsed_s"), (int, float))]
    verdict_counts: dict[str, int] = {}
    for row in compare_rows:
        verdict_counts[row["verdict"]] = verdict_counts.get(row["verdict"], 0) + 1

    baseline_with_findings = sum(1 for row in compare_rows if (row.get("baseline_findings") or 0) > 0)
    candidate_with_findings = sum(1 for row in compare_rows if (row.get("candidate_findings") or 0) > 0)
    missed_baseline_findings = sum(
        1 for row in compare_rows if (row.get("baseline_findings") or 0) > 0 and (row.get("candidate_findings") or 0) == 0
    )
    candidate_only_findings = sum(
        1 for row in compare_rows if (row.get("baseline_findings") or 0) == 0 and (row.get("candidate_findings") or 0) > 0
    )
    parse_fail_ids = [row["block_id"] for row in compare_rows if not row["parse_ok"]]
    degraded_ids = [row["block_id"] for row in compare_rows if row["verdict"] == "likely_degraded"]
    uncertain_ids = [row["block_id"] for row in compare_rows if row["verdict"] == "uncertain"]

    summary_lines = [
        "# Chandra Stage02 Full Project",
        "",
        f"- Model: `{args.model}`",
        f"- Total blocks in index: `{len(block_ids)}`",
        f"- Prepared images: `{len(prepared_by_id)}`",
        f"- Stage-02 baseline blocks: `{len(baseline)}`",
        f"- Prompt variant: `{args.prompt_variant}`",
        f"- Max output tokens: `{args.max_output_tokens}`",
        f"- Avg elapsed: `{round(statistics.mean(elapsed_values), 2) if elapsed_values else 0}` sec/block",
        f"- Median elapsed: `{_median(elapsed_values)}` sec/block",
        f"- Parse ok: `{sum(1 for row in compare_rows if row['parse_ok'])}` / `{len(compare_rows)}`",
        f"- Blocks with findings: candidate `{candidate_with_findings}`, baseline `{baseline_with_findings}`",
        f"- Blocks where baseline had findings but candidate had none: `{missed_baseline_findings}`",
        f"- Blocks where candidate had findings but baseline had none: `{candidate_only_findings}`",
        f"- Verdict counts vs stage-02 baseline: `{verdict_counts}`",
        "",
        "| Verdict | Count |",
        "|---|---:|",
    ]
    for key in sorted(verdict_counts):
        summary_lines.append(f"| `{key}` | {verdict_counts[key]} |")
    summary_lines.extend([
        "",
        "## Shortlists",
        "",
        f"- Parse fails: {', '.join(parse_fail_ids[:20]) if parse_fail_ids else 'none'}",
        f"- Likely degraded: {', '.join(degraded_ids[:20]) if degraded_ids else 'none'}",
        f"- Uncertain: {', '.join(uncertain_ids[:20]) if uncertain_ids else 'none'}",
    ])
    (exp_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    suspicious = ["# Suspicious Shortlist", ""]
    for row in compare_rows:
        if row["verdict"] not in {"fail_parse", "likely_degraded", "uncertain"}:
            continue
        suspicious.extend([
            f"## {row['block_id']}",
            "",
            f"- Verdict: `{row['verdict']}`",
            f"- Page: `{row['page']}`",
            f"- Baseline findings: `{row['baseline_findings']}`",
            f"- Candidate findings: `{row['candidate_findings']}`",
            f"- Notes: {'; '.join(row['notes'])}",
            f"- Baseline label: {row['baseline_label']}",
            f"- Candidate label: {row['candidate_label']}",
            "",
        ])
    (exp_dir / "suspicious_shortlist.md").write_text("\n".join(suspicious) + "\n", encoding="utf-8")

    rec_lines = [
        "# Recommendation",
        "",
        f"Model tested: `{args.model}`",
        "",
    ]
    if missed_baseline_findings <= 10 and verdict_counts.get("fail_parse", 0) <= 2:
        rec_lines.append("Full-project result is usable as a cheap exploratory layer, but still not strong enough to replace the project baseline on its own.")
    else:
        rec_lines.append("Full-project result confirms the model is not safe as a standalone stage-02 engine on this project. It may still be useful as a cheap prefilter or descriptor.")
    (exp_dir / "winner_recommendation.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")

    print(f"Artifacts: {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
