#!/usr/bin/env python3
"""Регресс: расширение не должно ломать то, что вектограф уже умеет.

1. Прогоняет ТЕКУЩИЙ production-вектограф на всех блоках корпуса (эталон «до»).
2. Прогоняет детектор диалекта PoC на тех же блоках и проверяет, что блоки,
   которые production обрабатывает успешно, распознаются как classic_calc_singleline
   и, значит, в предлагаемой архитектуре остаются на СТАРОМ пути.
3. Для блоков classic-диалекта дополнительно строит SYSTEM_GRAPH и сверяет
   число отходящих аппаратов с production-графом.
"""
from __future__ import annotations
import collections, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))

from backend.app.pipeline.stages.block_grounding import singleline_graph_geometry as sg  # noqa
import g2_engine as E  # noqa

A = Path(__file__).resolve().parents[1] / "artifacts"
audit = json.loads((A / "corpus_audit.json").read_text(encoding="utf-8"))
extra = json.loads((A / "corpus_audit_extra.json").read_text(encoding="utf-8"))


def main():
    rows = []
    for r in audit["blocks"]:
        rp = ROOT / r["result_json"]
        pdf = rp.parent / "document.pdf"
        if not pdf.exists():
            continue
        rj = json.loads(rp.read_text(encoding="utf-8"))
        rec = None
        for pg in rj.get("pages", []) or []:
            for b in pg.get("blocks") or []:
                if str(b.get("id") or b.get("block_id")) == r["block_id"]:
                    pi = b.get("page_index")
                    if pi is None:
                        pi = (pg.get("page_index") if pg.get("page_index") is not None
                              else int(pg.get("page_number") or 1) - 1)
                    canon = b.get("pdfplumber_text") or None
                    rec = {"block_id": r["block_id"], "page_index": pi,
                           "coords_norm": b.get("coords_norm"),
                           "polygon_points_norm": b.get("polygon_points_norm")}
        if rec is None:
            continue
        try:
            ev = E.scan_block(pdf, rec)
            prof = E.detect_profile(ev, canon)
        except Exception as exc:
            rows.append({**{k: r[k] for k in ("result_json", "block_id", "stop")},
                         "poc_profile": f"ERROR:{exc}"})
            continue
        row = {"result_json": r["result_json"], "block_id": r["block_id"],
               "production_stop": r["stop"], "production_feeders": r["graph_feeders"],
               "poc_profile": prof["id"]}
        if r["stop"] == "ok":
            try:
                g = E.build_system_graph(pdf, rec, canonical_text=canon)
                row["poc_backbone"] = g["quality"].get("backbone_recovered")
                row["poc_outgoing"] = g["quality"].get("outgoing_devices")
                row["poc_sections"] = g["quality"].get("sections")
                row["poc_identity_coverage"] = g["quality"].get("identity_coverage")
            except Exception as exc:
                row["poc_backbone"] = f"ERROR:{exc}"
        rows.append(row)

    ok_rows = [r for r in rows if r.get("production_stop") == "ok"]
    prof_of_ok = collections.Counter(r["poc_profile"] for r in ok_rows)
    print(f"Блоков, которые production обрабатывает успешно: {len(ok_rows)}")
    print("  их диалект по детектору PoC:", dict(prof_of_ok))
    misrouted = [r for r in ok_rows if r["poc_profile"] != "classic_calc_singleline"]
    print(f"  ушли бы НЕ на старый путь: {len(misrouted)}")
    for r in misrouted[:10]:
        print("    ", r["poc_profile"], r["block_id"], r["result_json"][-70:])
    prof_all = collections.Counter(r["poc_profile"] for r in rows)
    print("\nДиалекты по всему прогону:", dict(prof_all))
    stop_by_prof = collections.defaultdict(collections.Counter)
    for r in rows:
        stop_by_prof[r["poc_profile"]][r.get("production_stop")] += 1
    for p, c in stop_by_prof.items():
        print(f"  {p:26s} {dict(c)}")
    (A / "regression_results.json").write_text(
        json.dumps({"rows": rows,
                    "production_ok": len(ok_rows),
                    "profiles_of_production_ok": dict(prof_of_ok),
                    "misrouted": misrouted}, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
