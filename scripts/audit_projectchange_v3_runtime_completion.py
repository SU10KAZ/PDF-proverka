#!/usr/bin/env python3
"""Leakage / research-path / A-B hardcode audits for V3 runtime package."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PKG = REPO / "backend/app/services/project_change_v3"
HM = REPO / "backend/app/api/routers/human_mapping.py"
OUT = Path("/home/coder/auditmanager/deployment-receipts")
OUT.mkdir(parents=True, exist_ok=True)

LEAK_TOKENS = ["REAL15", "PROVEN10", "F13", "CORRECT", "PARTIAL", "FALSE"]
RESEARCH = re.compile(r"experiments/|corpus-audits/|20260914_project_change_272")

def scan():
    leak_hits = []
    research_hits = []
    pair_a = []
    pair_b = []
    stub_hits = []
    for path in list(PKG.rglob("*.py")) + [HM]:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = str(path.relative_to(REPO))
        for i, line in enumerate(text.splitlines(), 1):
            if any(tok in line for tok in LEAK_TOKENS) and "tokens" not in line.lower():
                # ignore audit scripts listing tokens
                if "LEAK_TOKENS" in line or "tokens =" in line:
                    continue
                leak_hits.append({"file": rel, "line": i, "text": line.strip()[:200]})
            if RESEARCH.search(line) and not line.strip().startswith("#"):
                research_hits.append({"file": rel, "line": i, "text": line.strip()[:200]})
            if "v3_inference_not_implemented" in line:
                stub_hits.append({"file": rel, "line": i, "text": line.strip()[:200]})
            # Runtime hardcode of pair enum A/B in schemas / loaders
            if 'enum": ["A", "B"]' in line or "enum': ['A', 'B']" in line:
                if "PAIR_KEYS" not in line:
                    if "A" in line:
                        pair_a.append({"file": rel, "line": i, "text": line.strip()[:200]})
                    if "B" in line:
                        pair_b.append({"file": rel, "line": i, "text": line.strip()[:200]})
    return leak_hits, research_hits, pair_a, pair_b, stub_hits

leak, research, pa, pb, stub = scan()
leakage = {
    "schema": "source-truth-leakage/1",
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "hit_count": len(leak),
    "hits": leak,
    "pass": len(leak) == 0,
    "model_calls": 0,
}
research_audit = {
    "schema": "runtime-research-path-audit/1",
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "RUNTIME_RESEARCH_PATH_DEPENDENCIES": len(research),
    "hits": research,
    "pass": len(research) == 0,
}
hardcode = {
    "schema": "pair-ab-hardcode-audit/1",
    "checked_at": datetime.now(timezone.utc).isoformat(),
    "PAIR_A_HARDCODE_IN_RUNTIME": len(pa),
    "PAIR_B_HARDCODE_IN_RUNTIME": len(pb),
    "stub_hits": stub,
    "pair_a": pa,
    "pair_b": pb,
    "pass": len(pa) == 0 and len(pb) == 0 and len(stub) == 0,
}
for name, payload in [
    ("projectcomparison_v3_runtime_SOURCE_TRUTH_LEAKAGE.json", leakage),
    ("projectcomparison_v3_runtime_RESEARCH_PATH_AUDIT.json", research_audit),
    ("projectcomparison_v3_runtime_AB_HARDCODE_AUDIT.json", hardcode),
]:
    (OUT / name).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(name, json.dumps({k: payload[k] for k in payload if k in ("hit_count", "RUNTIME_RESEARCH_PATH_DEPENDENCIES", "PAIR_A_HARDCODE_IN_RUNTIME", "PAIR_B_HARDCODE_IN_RUNTIME", "pass", "stub_hits")}, default=str))
