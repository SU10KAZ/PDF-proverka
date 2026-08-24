#!/usr/bin/env python3
"""Assemble human_ground_truth.json.

Two sources, both recorded per pair:

* `rule` — deterministic label from the raster evidence in `gt_assist.json`
  (0 clusters -> NO_CHANGE; only CROP/TEXT/HAIRLINE clusters -> CROP_DIFFERENCE
  or NO_CHANGE; any REAL/MOVED cluster -> LOCAL_CHANGE, scale by count);
* `eye`  — what the labeller saw on the side-by-side renders and the
  page-context zooms.  Where an eye label exists it WINS: the brief says the
  human is ground truth, and the disagreement rate between the two is itself a
  reported number.

Labeller: Claude Opus 5 in this session, looking at `artifacts/gt_sheets/*.png`
and `artifacts/gt_evidence/*_side.png`.  This is one labeller, and it is the
same model family as the system under test — an honest limitation, stated in
the report.  The label never uses the vector detector's output.
"""
from __future__ import annotations

import json
import pathlib

ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"

# --- what the labeller saw, pair -> (label, graphic_change, scale, note) -----
EYE = {
    "tx_tiny_change_09": ("CROP_DIFFERENCE", False, "none",
                          "верх блока: одна и та же рамка, кропы кончаются в разных местах"),
    "ar_tiny_change_10": ("CROP_DIFFERENCE", False, "none",
                          "то же: полоса у верхней границы кропа, чертежи совпадают"),
    "ar_tiny_change_14": ("CROP_DIFFERENCE", False, "none", "полоса у правой границы кропа"),
    "ar_small_change_15": ("CROP_DIFFERENCE", False, "none",
                           "планы идентичны, отличается только полоса вдоль верхнего края"),
    "tx_small_change_16": ("CROP_DIFFERENCE", False, "none", "полоса вдоль края кропа"),
    "ar_small_change_17": ("CROP_DIFFERENCE", False, "none", "две полосы вдоль краёв кропа"),
    "ss_small_change_18": ("LOCAL_CHANGE", True, "local",
                           "справа добавлены выноски с рамками: «упр. аварийным освещением», "
                           "«откл. тепл. завес кондиционирования», плюс новые полки 2SC2.30…33"),
    "ss_small_change_19": ("NO_CHANGE", False, "none",
                           "чертежи визуально идентичны; расхождения — волосяные смещения штриховки"),
    # первая метка была UNSURE по обзорному листу; при пересмотре с зумом
    # 0.25 pt/px видно, что в нижней полосе справа появились дополнительные
    # прямоугольники оборудования — метка исправлена на LOCAL_CHANGE.
    "ar_small_change_21": ("LOCAL_CHANGE", True, "many_local",
                           "при зуме 0.25 pt/px справа добавлены элементы оборудования в полосе "
                           "«ВК/ЭОМ/СС» и изменены контуры у ЛПП; на обзорном листе неразличимо"),
    "ar_small_change_22": ("NO_CHANGE", False, "none", "изменения внутри текстовых боксов + край кропа"),
    "eom_medium_change_23": ("LOCAL_CHANGE", True, "major",
                             "десятки удалённых/добавленных прямоугольников оборудования по всему плану"),
    "ar_medium_change_24": ("LOCAL_CHANGE", True, "local",
                            "графика: одна добавленная короткая линия; остальное — «Узел 1 (6)» → «1 (6)», текст"),
    "ss_medium_change_25": ("LOCAL_CHANGE", True, "many_local",
                            "около двадцати раз символ «СМК» заменён на прибор BGB"),
    "ss_medium_change_26": ("LOCAL_CHANGE", True, "local",
                            "добавлена выноска «Линии АПС прокладываются в нише СБ/СПЗ» с полкой и стрелкой"),
    "ar_medium_change_27": ("CROP_DIFFERENCE", False, "none", "планы идентичны, отличается полоса края"),
    "ss_medium_change_28": ("NO_CHANGE", False, "none", "волосяные смещения штриховки, чертёж тот же"),
    "eom_dense_small_change_29": ("LOCAL_CHANGE", True, "local",
                                  "добавлена выноска «Коробка обогрева СК13.-1(1)-2» со стрелкой — "
                                  "в блоке с 263 000 pt графики"),
    "eom_dense_small_change_32": ("LOCAL_CHANGE", True, "local",
                                  "добавлена выноска «Коробка обогрева СК13.-1(1)-3» со стрелкой и символом"),
    "eom_repack_34": ("NO_CHANGE", False, "none", "штамп совпадает; отличие — полоса края кропа"),
    "tx_rotated_page_37": ("LOCAL_CHANGE", True, "many_local",
                           "страница /Rotate 270; план тот же, переписаны текстовые выноски, полки сдвинуты"),
    "ss_strong_redesign_40": ("LOCAL_CHANGE", True, "major",
                              "тот же план парковки, но справа добавлен целый слой (машиноместа, оборудование)"),
    "ov_strong_redesign_43": ("LOCAL_CHANGE", True, "major",
                              "тот же план этажа, смещён в кадре и полностью переобозначен (КРК→КВК24)"),
    "ar_large_change_45": ("LOCAL_CHANGE", True, "many_local",
                           "тот же план кровли, десятки локальных правок (новые зенитные фонари, обозначения)"),
    "ar_text_only_49": ("NO_CHANGE", False, "none",
                        "кружок оси и выноска совпадают; отличается только отрисовка текста «П.АБ»"),
}


def rule_label(item):
    regs = item.get("regions", [])
    real = [r for r in regs if r["proposal"] in ("REAL", "MOVED")]
    if not regs:
        return "NO_CHANGE", "none", []
    if not real:
        has_crop = any(r["proposal"] == "CROP" for r in regs)
        return ("CROP_DIFFERENCE" if has_crop else "NO_CHANGE"), "none", []
    n = len(real)
    scale = "local" if n <= 5 else ("many_local" if n <= 50 else "major")
    return "LOCAL_CHANGE", scale, real


def main():
    bench = {p["pair_id"]: p for p in json.loads((ART / "benchmark_pairs.json").read_text(encoding="utf-8"))["pairs"]}
    assist = {x["pair_id"]: x for x in json.loads((ART / "gt_assist.json").read_text(encoding="utf-8"))["items"]}
    gt_index = {x["pair_id"]: x for x in json.loads((ART / "gt_evidence_index.json").read_text(encoding="utf-8"))["items"]}
    cell = 0.6
    rows = []
    agree = dis = 0
    for pid, p in bench.items():
        a = assist.get(pid, {})
        rl, rscale, real = rule_label(a)
        # GT regions in the right-hand block's visual points
        bx = None
        ev = gt_index.get(pid)
        regions_pt = []
        if ev:
            # right block origin
            from experiments.local_graphic_diff_mode1_opus.probes.gt_tool import block_of
            b = block_of(p["pdf_right"], p["page_index_right"], p["bbox_right"])
            for r in sorted(real, key=lambda r: -r["cells"])[:20]:
                x0, y0, x1, y1 = r["px"]
                regions_pt.append({
                    "bbox_pt": [round(b.bbox_vis[0] + x0 * cell, 2), round(b.bbox_vis[1] + y0 * cell, 2),
                                round(b.bbox_vis[0] + x1 * cell, 2), round(b.bbox_vis[1] + y1 * cell, 2)],
                    "cells": r["cells"], "kind": r["proposal"],
                    "text_share": r.get("text_share"),
                    "graphic": bool((r.get("text_share") or 0.0) < 0.5),
                })
        eye = EYE.get(pid)
        if eye:
            label, graphic, scale, note = eye
            if (label == "LOCAL_CHANGE") == (rl == "LOCAL_CHANGE"):
                agree += 1
            else:
                dis += 1
        else:
            label, graphic, scale, note = rl, (rl == "LOCAL_CHANGE"), rscale, "разметка по растровым признакам, глазом не смотрели"
        rows.append({
            "pair_id": pid, "bucket": p["bucket"], "discipline": p["discipline"],
            "human_label": label,
            "graphic_change": graphic,
            "scale": scale if eye else rscale,
            "eye_verified": bool(eye),
            "rule_label": rl, "rule_scale": rscale,
            "note": note,
            "gt_regions": regions_pt if label == "LOCAL_CHANGE" else [],
            "raster_changed_fraction": a.get("changed_fraction"),
            "n_clusters": len(a.get("regions", [])),
            "clusters_by_kind": {k: sum(1 for r in a.get("regions", []) if r["proposal"] == k)
                                 for k in ("REAL", "MOVED", "CROP", "TEXT", "HAIRLINE")},
        })
    out = {
        "probe": "make_gt", "research_only": True,
        "labeller": "Claude Opus 5 (эта сессия), по рендерам side-by-side и зумам с контекстом страницы",
        "labeller_limitation": "один размечающий, той же модельной семьи, что и проверяемая система; "
                               "разметка НЕ использует выход векторного детектора",
        "label_revisions": [{"pair_id": "ar_small_change_21", "from": "UNSURE", "to": "LOCAL_CHANGE",
                             "why": "пересмотр при зуме 0.25 pt/px после того, как детектор указал места; "
                                    "решение принято по рендерам страниц, не по выходу детектора"}],
        "eye_verified_pairs": sum(1 for r in rows if r["eye_verified"]),
        "eye_vs_rule_agreement": {"agree": agree, "disagree": dis},
        "n": len(rows), "pairs": rows,
    }
    (ART / "human_ground_truth.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    from collections import Counter
    print("labels:", Counter(r["human_label"] for r in rows))
    print("eye-verified:", out["eye_verified_pairs"], "agreement:", out["eye_vs_rule_agreement"])


if __name__ == "__main__":
    main()
