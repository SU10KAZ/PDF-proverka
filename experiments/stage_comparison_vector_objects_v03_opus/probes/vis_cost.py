#!/usr/bin/env python3
"""visscore S4 — цена точечного Vision против «Vision на весь блок» на ТЕХ ЖЕ случаях.

Площадь блока берётся из подготовленного блока (v03_foundation.block_frame),
площадь окна — из прямоугольника случая. Формула цены — из v0.2, не переизмеряется.
"""
from __future__ import annotations
import json, math, os, sys, statistics as st
from pathlib import Path

EXP = Path(__file__).resolve().parents[1]
ART = EXP / "artifacts"
sys.path.insert(0, str(EXP / "probes"))
import v03_foundation as F      # noqa: E402
import grp_common as G          # noqa: E402

TOK_CAP = 3051.0
TARGET = 700          # длинная сторона рендера набора


def tokens(w, h):
    return min(1.2014 * math.ceil(w / 32) * math.ceil(h / 32) + 48.67, TOK_CAP)


def block_rect_pt(pb):
    fr = F.block_frame(pb.pdf_path, pb.page_index, pb.coords_px, pb.page_px_w, pb.page_px_h)
    r = fr.clip_display
    return [float(r.x0), float(r.y0), float(r.x1), float(r.y1)]


def main():
    truth = json.load(open(ART / "vis_truth.json", encoding="utf-8"))["truth"]
    mine = {p["pair_id"]: p for p in json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]}
    out = []
    for t in truth:
        cid = t["case_id"]
        if t["source_kind"] == "REAL":
            p = mine[t["pair_id"]]
            bx = p["side_a"]["clip_pt"]
            nseg = p["side_a"].get("segments")
        else:
            pb = G.prepared_block(t["doc_id"], t["version"], t["block_id"])
            bx = block_rect_pt(pb)
            nseg = None
        bw, bh = bx[2] - bx[0], bx[3] - bx[1]
        r = t["rect_a_pt"]
        if r is None:                      # роли B/C: окно = весь блок
            ww, wh = bw, bh
            whole = True
        else:
            ww, wh = r[2] - r[0], r[3] - r[1]
            whole = False
        pxw, pxh = t["px_left"]
        zoom = pxw / ww if ww else None            # px на точку PDF в тесном кропе
        # 1) весь блок при ТОМ ЖЕ зуме
        Wz, Hz = bw * zoom, bh * zoom
        tok_same_zoom = tokens(Wz, Hz)
        # 2) весь блок в том же конверте рендера (длинная сторона TARGET px)
        s = TARGET / max(bw, bh)
        tok_env = tokens(bw * s, bh * s)
        # 3) какой зум ещё влезает в потолок 3051 для всего блока
        #    tokens = 1.2014*(W/32)*(H/32)+48.67 <= 3051 → W*H <= (3051-48.67)/1.2014*1024
        area_px_cap = (TOK_CAP - 48.67) / 1.2014 * 1024
        zoom_cap = math.sqrt(area_px_cap / (bw * bh))
        s_env = TARGET / max(bw, bh)
        reg_px_w, reg_px_h = ww * s_env, wh * s_env
        out.append(dict(
            case_id=cid, source_kind=t["source_kind"], role=t["role"], truth=t["truth"],
            block_pt=[round(v, 2) for v in bx], block_w_pt=round(bw, 2), block_h_pt=round(bh, 2),
            block_area_pt2=round(bw * bh, 1), win_w_pt=round(ww, 2), win_h_pt=round(wh, 2),
            win_area_pt2=round(ww * wh, 1), area_ratio=round((bw * bh) / (ww * wh), 2),
            whole_block_case=whole, n_seg_block=nseg,
            crop_zoom_px_per_pt=round(zoom, 3) if zoom else None,
            tokens_case=round(t["tokens_left"] + t["tokens_right"], 1),
            tokens_whole_same_zoom_pair=round(2 * tok_same_zoom, 1),
            capped_same_zoom=(tok_same_zoom >= TOK_CAP - 1e-6),
            tokens_whole_envelope_pair=round(2 * tok_env, 1),
            zoom_cap_px_per_pt=round(zoom_cap, 3),
            zoom_loss_at_cap=round(zoom / zoom_cap, 2) if zoom else None,
            region_px_in_whole_envelope=[round(reg_px_w, 1), round(reg_px_h, 1)],
            region_px_in_tight_crop=[pxw, pxh],
            linear_zoom_advantage=round(pxw / reg_px_w, 2) if reg_px_w else None,
            win_area_share_of_block_pct=round(100.0 * (ww * wh) / (bw * bh), 3),
        ))
    ratios = [o["area_ratio"] for o in out if not o["whole_block_case"]]
    zl = [o["zoom_loss_at_cap"] for o in out if not o["whole_block_case"]]
    summ = dict(
        n=len(out),
        tokens_case_sum=round(sum(o["tokens_case"] for o in out), 1),
        tokens_case_median=st.median([o["tokens_case"] for o in out]),
        tokens_whole_same_zoom_sum=round(sum(o["tokens_whole_same_zoom_pair"] for o in out), 1),
        tokens_whole_same_zoom_median=st.median([o["tokens_whole_same_zoom_pair"] for o in out]),
        n_capped_same_zoom=sum(1 for o in out if o["capped_same_zoom"]),
        tokens_whole_envelope_sum=round(sum(o["tokens_whole_envelope_pair"] for o in out), 1),
        tokens_whole_envelope_median=st.median([o["tokens_whole_envelope_pair"] for o in out]),
        area_ratio_median=st.median(ratios) if ratios else None,
        area_ratio_min=min(ratios) if ratios else None,
        area_ratio_max=max(ratios) if ratios else None,
        zoom_loss_median=st.median(zl) if zl else None,
        zoom_loss_max=max(zl) if zl else None,
        n_windows=len(ratios),
        region_px_w_in_whole_envelope_median=st.median([o["region_px_in_whole_envelope"][0]
                                                        for o in out if not o["whole_block_case"]]),
        region_px_w_in_whole_envelope_min=min([o["region_px_in_whole_envelope"][0]
                                               for o in out if not o["whole_block_case"]]),
        linear_zoom_advantage_median=st.median([o["linear_zoom_advantage"]
                                                for o in out if not o["whole_block_case"]]),
        linear_zoom_advantage_max=max([o["linear_zoom_advantage"]
                                       for o in out if not o["whole_block_case"]]),
        win_area_share_of_block_pct_median=st.median([o["win_area_share_of_block_pct"]
                                                      for o in out if not o["whole_block_case"]]),
    )
    json.dump(dict(summary=summ, cases=out), open(ART / "vis_cost.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(summ, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
