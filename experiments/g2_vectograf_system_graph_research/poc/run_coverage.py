#!/usr/bin/env python3
"""Покрытие корпуса предлагаемой маршрутизацией «classic-first, затем профиль».

Правило маршрутизации (важный вывод регресса): производственный структурер сам по
себе — самый специфичный детектор своего диалекта. Он либо находит ≥3 построчных
якоря, либо нет. Поэтому профиль выбирается ПОСЛЕ его отказа, а не вместо него:
классический путь остаётся нетронутым по построению (регресс = 0).
"""
from __future__ import annotations
import collections, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(Path(__file__).resolve().parent))
import fitz  # noqa
from backend.app.pipeline.stages.block_grounding import singleline_structurer as ss  # noqa
from backend.app.pipeline.stages.block_grounding import singleline_graph_geometry as sg  # noqa
import g2_engine as E  # noqa

A = Path(__file__).resolve().parents[1] / "artifacts"


def blocks_from_result_json():
    for rp in sorted(ROOT.glob("projects_v2/objects/*/**/02_work/result.json")):
        try:
            rj = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for pg in rj.get("pages", []) or []:
            for b in pg.get("blocks") or []:
                text = b.get("pdfplumber_text") or ""
                if len(text) < 200:
                    continue
                import re
                if len({t for t in re.split(r"\s+", text)
                        if re.fullmatch(r"\d{0,2}QF\d+(?:\.\d+)*", t)}) < 3:
                    continue
                pi = b.get("page_index")
                if pi is None:
                    pi = (pg.get("page_index") if pg.get("page_index") is not None
                          else int(pg.get("page_number") or 1) - 1)
                yield (rp.parent / "document.pdf", {
                    "block_id": str(b.get("id") or b.get("block_id")),
                    "page_index": pi, "coords_norm": b.get("coords_norm"),
                    "polygon_points_norm": b.get("polygon_points_norm")},
                    text, str(rp.relative_to(ROOT)))


def blocks_from_blocks_json():
    for bp in sorted(ROOT.glob("projects_v2/objects/*/**/02_work/blocks.json")):
        if (bp.parent / "result.json").exists():
            continue
        pdf = bp.parent / "document.pdf"
        if not pdf.exists():
            continue
        try:
            data = json.loads(bp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for rec in data.get("blocks") or []:
            if str(rec.get("block_type") or "").lower() not in ("image", "scheme", ""):
                continue
            yield pdf, rec, None, str(bp.relative_to(ROOT))


def main():
    rows = []
    for pdf, rec, canon, src in list(blocks_from_result_json()) + list(blocks_from_blocks_json()):
        try:
            ev = E.scan_block(pdf, rec)
        except Exception:
            continue
        text = canon if canon is not None else ev.text()
        import re
        if len({t for t in re.split(r"\s+", text) if re.fullmatch(r"\d{0,2}QF\d+(?:\.\d+)*", t)}) < 3:
            continue
        base = None
        try:
            base = ss.structure_singleline_text(text)
        except Exception:
            pass
        classic = bool(base and base.get("feeder_total", 0) >= 3)
        row = {"source": src, "block_id": rec.get("block_id"),
               "rotation": ev.rotation, "route": "CLASSIC" if classic else None}
        if classic:
            g = None
            try:
                g = sg.build_singleline_graph(pdf, text, panel_hint="ВРУ",
                                              bbox_norm=rec.get("coords_norm"),
                                              polygon_norm=(rec.get("polygon_points")
                                                            or rec.get("polygon_points_norm")))
            except Exception:
                pass
            row["classic_graph"] = bool(g)
            row["classic_gate_use"] = sg.evaluate_vectograf_gate(g).get("use")
        else:
            prof = E.detect_profile(ev, text)
            row["route"] = "PROFILE:" + prof["id"]
            try:
                g2 = E.build_system_graph(pdf, rec, canonical_text=text)
                row["backbone"] = g2["quality"].get("backbone_recovered")
                row["quality"] = {k: g2["quality"].get(k) for k in
                                  ("sections", "inputs", "outgoing_devices",
                                   "identity_coverage", "source_confidence",
                                   "bus_confidence", "section_confidence")}
            except Exception as exc:
                row["backbone"] = False
                row["error"] = str(exc)[:120]
        rows.append(row)

    routes = collections.Counter(r["route"] for r in rows)
    print(f"ВСЕГО блоков-кандидатов: {len(rows)}")
    print("\nМАРШРУТИЗАЦИЯ:")
    for k, v in routes.most_common():
        print(f"  {k:34s} {v}")
    classic = [r for r in rows if r["route"] == "CLASSIC"]
    prof = [r for r in rows if r["route"] != "CLASSIC"]
    print(f"\nКлассический путь: {len(classic)}; из них граф построен "
          f"{sum(1 for r in classic if r.get('classic_graph'))}, гейт пропустил "
          f"{sum(1 for r in classic if r.get('classic_gate_use'))}")
    ok_bb = [r for r in prof if r.get("backbone")]
    print(f"Профильный путь: {len(prof)}; остов восстановлен {len(ok_bb)} "
          f"({len(ok_bb) / max(len(prof), 1):.0%})")
    two_sec = [r for r in ok_bb if (r.get("quality") or {}).get("sections", 0) >= 2]
    print(f"  из них двухсекционных: {len(two_sec)}")
    idc = [(r.get("quality") or {}).get("identity_coverage") or 0 for r in ok_bb]
    if idc:
        idc.sort()
        print(f"  покрытие идентичности: медиана {idc[len(idc)//2]:.2f}, "
              f"≥0.8 у {sum(1 for x in idc if x >= 0.8)} из {len(idc)}")
    (A / "coverage_results.json").write_text(
        json.dumps({"total": len(rows), "routes": dict(routes), "rows": rows},
                   ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
