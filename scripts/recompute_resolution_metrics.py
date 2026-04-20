#!/usr/bin/env python3
"""Recompute resolution A/B metrics from existing shadow runs.

Используется если в исходном прогоне quality metrics были посчитаны по старому
полю `unreadable`, а block_analyses содержат `unreadable_text`. Перечитывает
block_batch_*.json в `<exp>/runs/<run_id>/shadow/_output/` и пересчитывает
metrics.json, subset_summary.*, subset_side_by_side.*, gate_report.json,
resolution_recommendation.md.

Usage:
    python scripts/recompute_resolution_metrics.py \\
        --experiment-dir "<project>/_experiments/block_resolution_ab/<ts>"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.run_block_resolution_matrix import (  # noqa: E402
    compute_quality_metrics,
    compute_crop_stats,
    compute_plan_stats,
    build_plan_single_block,
    build_plan_baseline_full,
    build_subset_comparison_matrix,
    write_subset_side_by_side_md,
    write_subset_divergence_report,
    write_subset_summary,
    write_crop_stats_artifacts,
    write_recommendation,
    write_full_validation_artifacts,
    subset_quality_gate,
    pick_candidate,
    full_validation_gate,
    _collect_payload_index,
    DEFAULT_BASELINE_PROFILE,
    RESOLUTION_PROFILES,
)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--experiment-dir", required=True,
                    help="Путь к <project>/_experiments/block_resolution_ab/<ts>")
    args = ap.parse_args()

    exp_dir = Path(args.experiment_dir).expanduser()
    if not exp_dir.exists():
        print(f"[ERROR] experiment_dir не существует: {exp_dir}")
        sys.exit(2)

    # subset ids
    subset_file = exp_dir / "fixed_subset_block_ids.json"
    if not subset_file.exists():
        print(f"[ERROR] fixed_subset_block_ids.json не найден")
        sys.exit(2)
    subset_ids = json.loads(subset_file.read_text(encoding="utf-8"))

    # crop roots per profile
    crop_stats_by_profile: dict = {}
    index_blocks_by_profile: dict = {}
    predicted_plans: dict = {}
    predicted_plan_stats: dict = {}
    for pid in RESOLUTION_PROFILES:
        crop_root = exp_dir / "crop_roots" / pid / "blocks"
        idx_path = crop_root / "index.json"
        if not idx_path.exists():
            print(f"[SKIP] {pid}: нет index.json в {crop_root}")
            continue
        idx = json.loads(idx_path.read_text(encoding="utf-8"))
        blocks_list = idx.get("blocks", [])
        index_blocks_by_profile[pid] = blocks_list
        crop_stats_by_profile[pid] = compute_crop_stats(blocks_list)
        plan = build_plan_baseline_full(blocks_list)
        predicted_plans[pid] = plan
        predicted_plan_stats[pid] = compute_plan_stats(plan)

    if crop_stats_by_profile:
        write_crop_stats_artifacts(exp_dir, crop_stats_by_profile, predicted_plan_stats)
        print(f"[OK] crop_stats_by_profile updated")

    # subset runs → recompute quality
    per_profile_subset: dict = {}
    for pid in list(index_blocks_by_profile.keys()):
        run_dir = exp_dir / "runs" / f"{pid}_subset_single"
        shadow_out = run_dir / "shadow" / "_output"
        if not shadow_out.exists():
            print(f"[SKIP] {pid}_subset_single: shadow dir отсутствует")
            continue
        plan = build_plan_single_block(index_blocks_by_profile[pid], subset_ids)
        quality = compute_quality_metrics(shadow_out, plan)
        plan_stats = compute_plan_stats(plan)

        # load runtime from existing metrics.json (not recomputed)
        metrics_path = run_dir / "metrics.json"
        runtime = {}
        if metrics_path.exists():
            try:
                prev = json.loads(metrics_path.read_text(encoding="utf-8"))
                runtime = prev.get("runtime", {})
            except Exception:
                pass

        # save updated metrics
        new_metrics = {
            "run_id": f"{pid}_subset_single",
            "profile_id": pid,
            "batch_mode": "single_block",
            "plan_stats": plan_stats,
            "runtime": runtime,
            "quality": quality,
            "recomputed": True,
        }
        metrics_path.write_text(
            json.dumps(new_metrics, ensure_ascii=False, indent=2), encoding="utf-8",
        )

        payload = _collect_payload_index(shadow_out)
        per_profile_subset[pid] = {
            "profile_id": pid,
            "crop_stats": crop_stats_by_profile.get(pid, {}),
            "plan_stats": plan_stats,
            "runtime": runtime,
            "quality": quality,
            "payload": payload,
            "plan": plan,
            "index_blocks": index_blocks_by_profile[pid],
        }
        print(f"[OK] {pid}_subset_single quality recomputed: "
              f"coverage={quality['coverage_pct']}%, unreadable={quality['unreadable_count']}, "
              f"findings={quality['total_findings']}, median_kv={quality['median_key_values_count']}")

    # subset summary + side-by-side + divergence
    if per_profile_subset:
        write_subset_summary(exp_dir, per_profile_subset, subset_ids)
        if len(per_profile_subset) >= 2:
            cmp = build_subset_comparison_matrix(
                subset_ids, per_profile_subset, crop_stats_by_profile,
            )
            (exp_dir / "subset_side_by_side.json").write_text(
                json.dumps(cmp, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            write_subset_side_by_side_md(exp_dir / "subset_side_by_side.md", cmp)
            write_subset_divergence_report(exp_dir / "subset_divergence_report.md", cmp)
        print(f"[OK] subset_summary + side_by_side + divergence updated")

    # Gate decision
    decision = {"candidate": None, "reason": "Нет phase A результатов"}
    if per_profile_subset and DEFAULT_BASELINE_PROFILE in per_profile_subset:
        baseline = {
            "profile_id": DEFAULT_BASELINE_PROFILE,
            "quality": per_profile_subset[DEFAULT_BASELINE_PROFILE]["quality"],
        }
        candidates = [
            {"profile_id": pid, "quality": info["quality"]}
            for pid, info in per_profile_subset.items()
            if pid != DEFAULT_BASELINE_PROFILE
        ]
        plan_stats_map = {pid: predicted_plan_stats[pid] for pid in per_profile_subset}
        decision = pick_candidate(baseline, candidates, plan_stats_map)
        (exp_dir / "gate_report.json").write_text(
            json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        print(f"[GATE] {decision.get('reason')}")
        print(f"       candidate = {decision.get('candidate')}")

    # Full validation (если runs сохранены)
    full_runs: dict = {}
    full_gate_report = None
    for pid in index_blocks_by_profile:
        run_dir = exp_dir / "runs" / f"{pid}_full_baseline_p3"
        shadow_out = run_dir / "shadow" / "_output"
        if not shadow_out.exists():
            continue
        plan = predicted_plans[pid]
        quality = compute_quality_metrics(shadow_out, plan)
        plan_stats = compute_plan_stats(plan)
        metrics_path = run_dir / "metrics.json"
        runtime = {}
        if metrics_path.exists():
            try:
                prev = json.loads(metrics_path.read_text(encoding="utf-8"))
                runtime = prev.get("runtime", {})
            except Exception:
                pass
        new_metrics = {
            "run_id": f"{pid}_full_baseline_p3",
            "profile_id": pid,
            "batch_mode": "baseline_full",
            "plan_stats": plan_stats,
            "runtime": runtime,
            "quality": quality,
            "recomputed": True,
        }
        metrics_path.write_text(
            json.dumps(new_metrics, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        full_runs[pid] = new_metrics
        print(f"[OK] {pid}_full_baseline_p3 quality recomputed")

    if DEFAULT_BASELINE_PROFILE in full_runs and len(full_runs) >= 2:
        cand_pid = decision.get("candidate")
        if cand_pid and cand_pid in full_runs:
            b = {"quality": full_runs[DEFAULT_BASELINE_PROFILE]["quality"],
                 "runtime": full_runs[DEFAULT_BASELINE_PROFILE].get("runtime", {})}
            c = {"quality": full_runs[cand_pid]["quality"],
                 "runtime": full_runs[cand_pid].get("runtime", {})}
            full_gate_report = full_validation_gate(b, c)
            write_full_validation_artifacts(exp_dir, full_runs, full_gate_report)
            print(f"[FULL GATE] passed={full_gate_report.get('passed')}, "
                  f"reasons={full_gate_report.get('reasons')}")

    # Final recommendation
    final = DEFAULT_BASELINE_PROFILE
    notes = []
    if full_gate_report and full_gate_report.get("passed"):
        final = decision["candidate"]
        notes.append(
            f"Candidate {final} прошёл full-validation gate. "
            f"baseline_elapsed={full_gate_report.get('baseline_elapsed')}s, "
            f"candidate_elapsed={full_gate_report.get('candidate_elapsed')}s."
        )
    elif full_gate_report:
        notes.append(
            f"Candidate {decision.get('candidate')} НЕ прошёл full-validation gate: "
            f"{full_gate_report.get('reasons')}."
        )
    elif decision.get("candidate"):
        notes.append(
            f"Subset phase выбрал {decision['candidate']}, но full validation не запускалась. "
            "Рекомендация носит предварительный характер."
        )
    else:
        notes.append("Subset gate никто не прошёл — baseline сохранён.")

    write_recommendation(
        exp_dir, crop_stats_by_profile, per_profile_subset, decision,
        full_runs, full_gate_report,
        baseline_profile=DEFAULT_BASELINE_PROFILE,
        final_production_recommendation=final,
        notes=notes,
    )
    print(f"[OK] resolution_recommendation.md updated -> final={final}")


if __name__ == "__main__":
    main()
