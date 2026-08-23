# -*- coding: utf-8 -*-
"""`hyb` — контрольный замер руки A БЕЗ материализации в PDF.

Зачем: в patch-режиме подмена материализуется белым прямоугольником поверх участка и
перерисовкой. Для УДАЛЕНИЯ объекта (C1) это ловушка: белый прямоугольник закрывает
краску на КАРТИНКЕ, но пути удалённого объекта остаются в контент-потоке страницы, и
`extract_block` их по-прежнему читает. То есть вход руки A для C1 испорчен МОЕЙ
материализацией, а не свойством компаратора.

Здесь тот же реестр считается на извлечениях в памяти (ex vs ex2) — как их вернул
`v03_counterfactual.apply`. Это честный замер самого компаратора; замер на общих PDF
остаётся в hyb_armA_cf.jsonl как «то, что видели все три руки».
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H          # noqa: E402
import hyb_build_cf as B        # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as CF  # noqa: E402
import grp_common as G          # noqa: E402
import grp_match as M           # noqa: E402
import loc_common as L          # noqa: E402

KW = {cf: kw for cf, _n, kw in B.PLAN}


def main():
    cases = H.load("hyb_cf_cases.json")["cases"]
    out = []
    for c in cases:
        if c["mode"] != "patch":
            continue
        cid = c["cand_id"]
        cf_id = c["cf_id"]
        kw = KW.get(cf_id, {})
        r = c["carrier"]
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        t0 = time.time()
        row = {"case_id": cid, "cf_id": cf_id, "truth": c["expected_verdict"]}
        try:
            ex = G.extract(pb)
            Lay = O.build_objects(ex)
            if kw.get("obj"):
                oi = int(re.search(r"_(\d+)$", cid).group(1))
                B._fit_text_sizes(ex, ex)          # no-op for geometry
                ex2, man = CF.apply(ex, Lay, cf_id)
            else:
                ex2, man = CF.apply(ex, Lay, cf_id, **(kw.get("params") or {}))
            dx, dy, score = M.register(ex.segments, ex2.segments, {(0.0, 0.0)})
            LA, LB, meta = L.layers(ex, ex2)
            led = L.ledger(ex, ex2, off=(dx, dy), LA=LA, LB=LB, meta=meta)
            recs = led["records"]
            interior = [x["change_len"] for x in recs if not x["at_boundary"]]
            row.update({
                "n_seg_a": len(ex.segments), "n_seg_b": len(ex2.segments),
                "sim": led["scalar"]["ink_similarity"],
                "n_records": led["n_records"], "n_interior": len(interior),
                "max_interior_len": round(max(interior), 2) if interior else 0.0,
                "verdict": ("GRAPHIC_CHANGE" if any(v >= H.T_RECORD_PT for v in interior)
                            else "NO_GRAPHIC_CHANGE"),
                "change_bbox_pt": man.get("change_bbox_pt"),
                "records": [{"type": x["type"], "bbox_pt": [round(v, 2) for v in x["bbox_pt"]],
                             "change_len": round(x["change_len"], 2),
                             "at_boundary": bool(x["at_boundary"])} for x in recs[:20]],
            })
        except Exception as e:                       # noqa: BLE001
            row["error"] = repr(e)
        row["t_sec"] = round(time.time() - t0, 1)
        out.append(row)
        print(cid, row.get("verdict"), row.get("max_interior_len"), row.get("error", ""), flush=True)
    H.dump({"note": "ledger on in-memory extracts (no PDF materialisation)", "rows": out},
           "hyb_armA_direct.json")
    ok = [r for r in out if "error" not in r]
    acc = sum(1 for r in ok if r["verdict"] == r["truth"]) / max(len(ok), 1)
    fp = sum(1 for r in ok if r["truth"] == "NO_GRAPHIC_CHANGE" and r["verdict"] == "GRAPHIC_CHANGE")
    print("direct:", len(ok), "acc", round(acc, 3), "false_graphic_changes", fp)


if __name__ == "__main__":
    main()
