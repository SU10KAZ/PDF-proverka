#!/usr/bin/env python3
"""Correlate Critic v2 UI scores with expert decisions."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

dec_log = json.loads((ROOT / "knowledge_base/decisions_log.json").read_text(encoding="utf-8"))
entries = dec_log.get("entries", dec_log if isinstance(dec_log, list) else [])
expert_by_key: dict[tuple[str, str], str] = {}
for e in entries:
    if e.get("item_type") != "finding":
        continue
    expert_by_key[
        (str(e.get("source_project", "")).strip(), str(e.get("item_id", "")).strip())
    ] = str(e.get("expert_decision", "")).lower()

rows: list[tuple[int, str]] = []
for ui_path in (ROOT / "projects_v2").rglob("critic_v2_triage_ui.json"):
    try:
        data = json.loads(ui_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    pid = ""
    sp = data.get("source_project")
    if isinstance(sp, dict):
        pid = str(sp.get("project_id") or sp.get("project_name") or "").strip()
    parts = ui_path.parts
    if not pid and "documents" in parts:
        pid = parts[parts.index("documents") + 1]
    for item in data.get("items") or []:
        fid = str(item.get("finding_id") or item.get("item_id") or "").strip()
        score = item.get("display_score")
        if score is None:
            score = item.get("score")
        if not fid or score is None:
            continue
        score = int(score)
        expert = expert_by_key.get((pid, fid)) or expert_by_key.get((pid.replace("TX/", ""), fid))
        if not expert:
            continue
        rows.append((score, expert))

print("matched", len(rows))
print("expert", dict(Counter(r[1] for r in rows)))
paired = [(s, e) for s, e in rows if e in ("accepted", "rejected")]
acc = [s for s, e in paired if e == "accepted"]
rej = [s for s, e in paired if e == "rejected"]
if acc:
    print("accepted n", len(acc), "mean", round(sum(acc) / len(acc), 1))
if rej:
    print("rejected n", len(rej), "mean", round(sum(rej) / len(rej), 1))
print("\nBuckets (accept% / reject%):")
for lo, hi in [(0, 20), (21, 40), (41, 60), (61, 80), (81, 100)]:
    sub = [(s, e) for s, e in paired if lo <= s <= hi]
    if len(sub) < 5:
        continue
    ar = sum(1 for s, e in sub if e == "accepted") / len(sub)
    rr = sum(1 for s, e in sub if e == "rejected") / len(sub)
    print(f"  {lo:2d}-{hi:2d}: n={len(sub):4d}  accept={ar*100:5.1f}%  reject={rr*100:5.1f}%")
if acc and rej:
    pairs = len(acc) * len(rej)
    wins = sum(1 for a in acc for b in rej if a > b) + 0.5 * sum(1 for a in acc for b in rej if a == b)
    print("\nrank_accuracy P(accepted_score > rejected_score):", round(wins / pairs, 3))
    print("mean_diff (accepted - rejected):", round(sum(acc) / len(acc) - sum(rej) / len(rej), 1))
hi = [e for s, e in paired if s >= 85]
lo = [e for s, e in paired if s < 85]
if hi:
    print("score>=85: n", len(hi), "accept_rate%", round(sum(1 for e in hi if e == "accepted") / len(hi) * 100, 1))
if lo:
    print("score<85:  n", len(lo), "accept_rate%", round(sum(1 for e in lo if e == "accepted") / len(lo) * 100, 1))
vl = [(s, e) for s, e in paired if s <= 20]
if vl:
    print("score<=20: n", len(vl), "reject_rate%", round(sum(1 for s, e in vl if e == "rejected") / len(vl) * 100, 1))
