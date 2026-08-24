#!/usr/bin/env python3
"""Сравнение двух SYSTEM_GRAPH боевой пары ГРЩ."""
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
from g2_comparator import compare_system_graphs  # noqa: E402

A = Path(__file__).resolve().parents[1] / "artifacts"
left = json.loads((A / "grsh_left_graph.json").read_text(encoding="utf-8"))
right = json.loads((A / "grsh_right_graph.json").read_text(encoding="utf-8"))
res = compare_system_graphs(left, right)
(A / "grsh_comparison.json").write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
print("ВЕРДИКТ ОСТОВА:", res["backbone_verdict"])
print("качество сопоставления:", json.dumps(res["quality"]["identity_match_rate"]),
      "надёжных:", res["quality"]["high_confidence_match_rate"])
print("\nУРОВЕНЬ A:", json.dumps({k: v for k, v in res["levels"]["A"].items()
                                 if k != "source_paths"}, ensure_ascii=False))
for p in res["levels"]["A"]["source_paths"]:
    print("   путь:", p["section"], p["left_path"], "⇒", p["right_path"],
          "|", p["left_source_subclass"], "→", p["right_source_subclass"])
print("\nУРОВЕНЬ C:", json.dumps({k: v for k, v in res["levels"]["C"].items() if k != "matches"},
                                 ensure_ascii=False))
print(f"\nИЗМЕНЕНИЯ ({len(res['changes'])}):")
for c in res["changes"]:
    print(f"  [{c['change_id']}] {c['type']:28s} {c['confidence']:6s} {c['summary']}")
    if c.get("note"):
        print(f"        · {c['note']}")
