#!/usr/bin/env python3
"""G2 research probe: почему production Вектограф возвращает None на паре ГРЩ.

Читает уже подготовленные блоки (blocks.json), берёт вектор-текст ВНУТРИ полигона
блока теми же средствами, что и production (_clip_words_to_polygon), и прогоняет
цепочку production-функций, фиксируя КАЖДЫЙ отказ.
Ничего в production не меняет.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import fitz  # noqa: E402

from backend.app.pipeline.stages.block_grounding import singleline_structurer as ss  # noqa: E402
from backend.app.pipeline.stages.block_grounding import singleline_graph_geometry as sg  # noqa: E402

OUT = Path(__file__).resolve().parent / "artifacts"
OUT.mkdir(parents=True, exist_ok=True)

CASES = {
    "left": {
        "work": ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_1/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия/versions/v001/02_work",
        "block_id": "blk_039909ec039649a1b8209f059c95167b",
        "stage": "P",
    },
    "right": {
        "work": ROOT / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison/stage_2/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1/versions/v001/02_work",
        "block_id": "blk_2d72a6705eaf4d8c9ee1d6ff459b15a6",
        "stage": "RD",
    },
}


def load_block(work: Path, block_id: str):
    data = json.loads((work / "blocks.json").read_text(encoding="utf-8"))
    rec = next(b for b in data["blocks"] if b["block_id"] == block_id)
    return data, rec


def words_in_block(pdf: Path, rec: dict):
    doc = fitz.open(str(pdf))
    pg = doc[int(rec.get("page_index") or 0)]
    words = pg.get_text("words")
    pw, ph = float(pg.rect.width), float(pg.rect.height)
    poly = rec.get("polygon_points")
    if poly:
        clipped = sg._clip_words_to_polygon(words, poly, pw, ph)
    else:
        clipped = sg._clip_words_to_bbox(words, rec["coords_norm"], pw, ph)
    info = {
        "page_rect": [pw, ph],
        "rotation": pg.rotation,
        "words_page": len(words),
        "words_block": len(clipped),
    }
    doc.close()
    return clipped, info


def words_to_text(words):
    """Собрать построчный текст в reading order (как вектор-текст блока)."""
    rows = {}
    for w in words:
        key = round(w[1] / 3.0)
        rows.setdefault(key, []).append(w)
    lines = []
    for key in sorted(rows):
        row = sorted(rows[key], key=lambda w: w[0])
        lines.append(" ".join(w[4] for w in row))
    return "\n".join(lines)


def main():
    report = {}
    for side, case in CASES.items():
        work = case["work"]
        pdf = work / "document.pdf"
        data, rec = load_block(work, case["block_id"])
        words, info = words_in_block(pdf, rec)
        text = words_to_text(words)
        (OUT / f"grsh_{side}_block_text.txt").write_text(text, encoding="utf-8")

        # ── Блокер 1: text structurer ──
        base = ss.structure_singleline_text(text)
        lines = text.split("\n")
        param_joined = [l for l in lines if ss.PARAM_RE.match(l.strip())]
        param_sep = [l for l in lines if ss.SEPARATE_PARAM_RE.match(l.strip())]
        sep_codes = [l for l in lines if ss.SEPARATE_CODE_RE.fullmatch(l.strip())]

        # ── Блокер 2: _QF_RE ──
        qf_prod = [w[4] for w in words if sg._QF_RE.fullmatch(w[4])]
        qf_loose = sorted({w[4] for w in words if re.fullmatch(r"\d*QF\d+(?:\.\d+)*", w[4])})
        qs_loose = sorted({w[4] for w in words if re.fullmatch(r"\d*QS\d+(?:\.\d+)*", w[4])})

        # ── Блокер 3: сам production-вход ──
        graph = sg.build_singleline_graph(
            pdf, text, panel_hint="ГРЩ",
            bbox_norm=rec.get("coords_norm"),
            polygon_norm=rec.get("polygon_points"),
        )
        gate = sg.evaluate_vectograf_gate(graph)

        # словарь токенов, полезный для диалект-детектора
        toks = [w[4] for w in words]
        report[side] = {
            "stage": case["stage"],
            "block_id": case["block_id"],
            **info,
            "text_lines": len(lines),
            "text_chars": len(text),
            "structurer_result": None if base is None else {
                "feeder_total": base["feeder_total"],
                "section_markers": base["section_markers_count"],
            },
            "param_re_joined_hits": len(param_joined),
            "param_re_separate_hits": len(param_sep),
            "separate_code_hits": len(sep_codes),
            "qf_prod_re_hits": len(qf_prod),
            "qf_prod_re_unique": sorted(set(qf_prod)),
            "qf_loose_unique": qf_loose,
            "qs_loose_unique": qs_loose,
            "graph_built": graph is not None,
            "gate": gate,
            "sample_kwt_lines": [l for l in lines if "кВт" in l][:10],
            "token_sample": toks[:60],
        }
        print(f"--- {side} ({case['stage']}) ---")
        print(json.dumps(report[side], ensure_ascii=False, indent=2)[:3000])

    (OUT / "probe_blockers_raw.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
