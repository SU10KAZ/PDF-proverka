#!/usr/bin/env python3
"""Acceptance audit Step 8 (Selective Critic) на реальных проектах."""
import json
import sys
import os
from pathlib import Path

# Fix console encoding on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from webapp.services.grounding_service import classify_grounding_level


def audit_project(findings_path: str):
    with open(findings_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    findings = data.get("findings", [])
    proj_name = findings_path.split("_output")[0].rstrip("/\\")

    print(f"\n{'='*70}")
    print(f"  {os.path.basename(proj_name)} — {len(findings)} findings")
    print(f"{'='*70}")

    # 1. Grounding levels
    levels = {"grounded_strong": [], "grounded_weak": [], "ungrounded": []}
    for f in findings:
        level = classify_grounding_level(f)
        f["_level"] = level
        levels[level].append(f)

    print(f"\n  GROUNDING LEVELS:")
    for lv, flist in levels.items():
        pct = 100 * len(flist) / max(len(findings), 1)
        print(f"    {lv}: {len(flist)}/{len(findings)} ({pct:.0f}%)")

    # 2. Selective Critic simulation
    risky = []
    skipped = []
    for f in findings:
        level = f["_level"]
        confidence = f.get("norm_confidence", 1.0)
        if level != "grounded_strong":
            risky.append(f)
        elif confidence is not None and confidence < 0.8:
            risky.append(f)
        else:
            skipped.append(f)

    print(f"\n  SELECTIVE CRITIC:")
    print(f"    Skip:   {len(skipped)}")
    print(f"    Review: {len(risky)}")

    reasons = {}
    for f in risky:
        lv = f["_level"]
        conf = f.get("norm_confidence", 1.0)
        if lv == "ungrounded":
            r = "ungrounded"
        elif lv == "grounded_weak":
            r = "weak_grounding"
        elif conf is not None and conf < 0.8:
            r = "low_norm_confidence"
        else:
            r = "other"
        reasons[r] = reasons.get(r, 0) + 1
    for r, c in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"      {r}: {c}")

    # 3. Strong findings audit
    print(f"\n  STRONG FINDINGS AUDIT ({len(levels['grounded_strong'])} шт.):")
    false_strong = []
    for f in levels["grounded_strong"]:
        fid = f.get("id", "?")
        source = f.get("source_block_ids", [])
        evidence = f.get("evidence", [])
        related = f.get("related_block_ids", [])
        real_img = [
            e for e in evidence
            if isinstance(e, dict) and e.get("type") == "image"
            and e.get("source") != "grounding_service"
        ]
        if not source and not real_img:
            false_strong.append(f)

    print(f"    FALSE STRONG: {len(false_strong)}")
    for fs in false_strong[:5]:
        print(f"      {fs.get('id')}: no source AND no real image evidence")
        print(f"        related: {fs.get('related_block_ids', [])[:3]}")
        print(f"        evidence: {fs.get('evidence', [])[:2]}")

    # Field coverage
    n = max(len(levels["grounded_strong"]), 1)
    stats = {}
    for key in ["source_block_ids", "selected_text_block_ids", "evidence_text_refs", "merge_source_g_ids"]:
        cnt = sum(1 for f in levels["grounded_strong"] if f.get(key))
        stats[key] = cnt
    cnt_real = sum(
        1 for f in levels["grounded_strong"]
        if any(
            isinstance(e, dict) and e.get("type") == "image" and e.get("source") != "grounding_service"
            for e in f.get("evidence", [])
        )
    )
    stats["real_image_evidence"] = cnt_real

    print(f"\n    Field coverage (strong):")
    for k, v in stats.items():
        print(f"      {k}: {v}/{n} ({100*v/n:.0f}%)")

    # 4. Weak/ungrounded audit
    false_review = []
    for f in levels["grounded_weak"] + levels["ungrounded"]:
        ev = f.get("evidence", [])
        rel = f.get("related_block_ids", [])
        real_img = [e for e in ev if isinstance(e, dict) and e.get("type") == "image"
                    and e.get("source") != "grounding_service"]
        if real_img and rel:
            false_review.append(f)

    print(f"\n  WEAK/UNGROUNDED AUDIT:")
    print(f"    Possible false review: {len(false_review)}")
    for fr in false_review[:5]:
        print(f"      {fr.get('id')}: real_ev={len([e for e in fr.get('evidence',[]) if isinstance(e,dict) and e.get('type')=='image' and e.get('source')!='grounding_service'])}, related={len(fr.get('related_block_ids', []))}")

    # 5. Samples
    print(f"\n  SAMPLE SKIPPED (10):")
    for f in skipped[:10]:
        fid = f.get("id", "?")
        sev = f.get("severity", "?")[:4]
        conf = f.get("norm_confidence", "?")
        src = f.get("source_block_ids", [])
        rel = f.get("related_block_ids", [])
        ev_n = len(f.get("evidence", []))
        prob = (f.get("problem", "") or "")[:55]
        print(f"    {fid:6} {sev:4} c={conf} src={len(src)} rel={len(rel)} ev={ev_n} | {prob}")

    print(f"\n  SAMPLE REVIEWED (10):")
    for f in risky[:10]:
        fid = f.get("id", "?")
        sev = f.get("severity", "?")[:4]
        lv = f["_level"][:6]
        conf = f.get("norm_confidence", "?")
        src = f.get("source_block_ids", [])
        rel = f.get("related_block_ids", [])
        ev_n = len(f.get("evidence", []))
        prob = (f.get("problem", "") or "")[:50]
        print(f"    {fid:6} {sev:4} {lv:6} c={conf} src={len(src)} rel={len(rel)} ev={ev_n} | {prob}")

    # 6. Rule safety checks
    print(f"\n  RULE SAFETY CHECKS:")
    # 6a: skip только по одному related без evidence/source
    unsafe_skip = [
        f for f in skipped
        if not f.get("source_block_ids")
        and not any(isinstance(e, dict) and e.get("type") == "image"
                    and e.get("source") != "grounding_service"
                    for e in f.get("evidence", []))
    ]
    print(f"    Skip без source+evidence: {len(unsafe_skip)}")
    for us in unsafe_skip[:3]:
        print(f"      {us.get('id')}: related={us.get('related_block_ids',[])} evidence={us.get('evidence',[])[:1]}")

    # 6b: skip с high confidence но weak grounding
    high_conf_weak = [
        f for f in skipped
        if (f.get("norm_confidence") or 0) >= 0.9
        and f["_level"] != "grounded_strong"
    ]
    print(f"    Skip high conf + weak grounding: {len(high_conf_weak)}")

    return {
        "project": os.path.basename(proj_name),
        "total": len(findings),
        "strong": len(levels["grounded_strong"]),
        "weak": len(levels["grounded_weak"]),
        "ungrounded": len(levels["ungrounded"]),
        "skipped": len(skipped),
        "reviewed": len(risky),
        "false_strong": len(false_strong),
        "false_review": len(false_review),
        "unsafe_skip": len(unsafe_skip),
    }


if __name__ == "__main__":
    import glob
    findings_files = sorted(glob.glob("projects/**/_output/03_findings.json", recursive=True))

    results = []
    for fp in findings_files:
        with open(fp, "r", encoding="utf-8") as f:
            data = json.load(f)
        if len(data.get("findings", [])) < 5:
            continue
        results.append(audit_project(fp))

    print(f"\n\n{'='*70}")
    print(f"  СВОДКА")
    print(f"{'='*70}")
    total_f = sum(r["total"] for r in results)
    total_strong = sum(r["strong"] for r in results)
    total_weak = sum(r["weak"] for r in results)
    total_ung = sum(r["ungrounded"] for r in results)
    total_skip = sum(r["skipped"] for r in results)
    total_rev = sum(r["reviewed"] for r in results)
    total_fs = sum(r["false_strong"] for r in results)
    total_fr = sum(r["false_review"] for r in results)
    total_us = sum(r["unsafe_skip"] for r in results)

    print(f"  Проектов: {len(results)}")
    print(f"  Findings: {total_f}")
    print(f"  Strong:   {total_strong} ({100*total_strong/max(total_f,1):.0f}%)")
    print(f"  Weak:     {total_weak} ({100*total_weak/max(total_f,1):.0f}%)")
    print(f"  Unground: {total_ung} ({100*total_ung/max(total_f,1):.0f}%)")
    print(f"  Skip:     {total_skip}")
    print(f"  Review:   {total_rev}")
    print(f"  FALSE strong: {total_fs}")
    print(f"  FALSE review: {total_fr}")
    print(f"  UNSAFE skip:  {total_us}")
