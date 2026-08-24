#!/usr/bin/env python3
"""Прогон PoC-движка на РЕАЛЬНОЙ паре ГРЩ П↔РД. Никаких ручных CASES."""
from __future__ import annotations
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from g2_engine import build_system_graph  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "artifacts"
PAIR = {
    "left": ("projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_1/"
             "documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия/versions/v001/02_work",
             "blk_039909ec039649a1b8209f059c95167b"),
    "right": ("projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_2/"
              "documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1/versions/v001/02_work",
              "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6"),
}

def main():
    for side, (work, bid) in PAIR.items():
        w = ROOT / work
        rec = next(b for b in json.loads((w / "blocks.json").read_text(encoding="utf-8"))["blocks"]
                   if b["block_id"] == bid)
        g = build_system_graph(w / "document.pdf", rec, side=side)
        (OUT / f"grsh_{side}_graph.json").write_text(
            json.dumps(g, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"===== {side.upper()} =====")
        print("профиль:", g["profile"]["id"], g["profile"]["why"])
        print("качество:", json.dumps(g["quality"], ensure_ascii=False))
        for wmsg in g["warnings"]:
            print("  ⚠", wmsg)
        by = {}
        for n in g["nodes"]:
            by.setdefault(n["type"], []).append(n)
        for t in ("SOURCE", "SERVICE_NODE", "INPUT_DEVICE", "BUS_SECTION", "SECTION_DEVICE",
                  "METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP", "UNKNOWN_NODE"):
            for n in by.get(t, []):
                print(f"  {t:20s} {str(n.get('display_label')):22s} "
                      f"sub={n.get('subclass')} id={n['canonical_identity']} conf={n['confidence']}")
        outs = by.get("OUTGOING_DEVICE", [])
        print(f"  OUTGOING_DEVICE ×{len(outs)}")
        for n in outs:
            print(f"      {n['label']:8s} [{n.get('section')}] ident={str(n['canonical_identity']):16s} "
                  f"disp={str(n.get('display_label'))[:28]:28s} {n['attrs']['status']:7s} {n['confidence']}")

if __name__ == "__main__":
    main()
