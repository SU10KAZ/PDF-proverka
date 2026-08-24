#!/usr/bin/env python3
"""G2 research: вторая половина аудита — документы НОВОГО формата (blocks.json, без result.json).

Именно в нём живёт боевая пара ГРЩ. Вектор-текст блока собирается из PDF с учётом
поворота страницы (production вектограф этого не делает — см. отчёт).
"""
from __future__ import annotations

import collections
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
QF_LOOSE = re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$")
QS_LOOSE = re.compile(r"^\d{0,2}QS\d+(?:\.\d+)*$")


def visual_words(pg):
    m = pg.rotation_matrix
    out = []
    for w in pg.get_text("words"):
        r = fitz.Rect(w[:4]) * m
        out.append((min(r.x0, r.x1), min(r.y0, r.y1), max(r.x0, r.x1), max(r.y0, r.y1), w[4]))
    return out


def words_text(words):
    rows = {}
    for w in words:
        rows.setdefault(round(w[1] / 3.0), []).append(w)
    return "\n".join(" ".join(x[4] for x in sorted(rows[k], key=lambda t: t[0])) for k in sorted(rows))


def main():
    rows = []
    for bp in sorted(ROOT.glob("projects_v2/objects/*/**/02_work/blocks.json")):
        if (bp.parent / "result.json").exists():
            continue  # уже покрыто первым проходом
        pdf = bp.parent / "document.pdf"
        if not pdf.exists():
            continue
        try:
            data = json.loads(bp.read_text(encoding="utf-8"))
        except Exception:
            continue
        try:
            doc = fitz.open(str(pdf))
        except Exception:
            continue
        for rec in data.get("blocks") or []:
            if str(rec.get("block_type") or "").lower() not in ("image", "scheme", ""):
                continue
            pi = int(rec.get("page_index") or 0)
            if pi >= doc.page_count:
                continue
            pg = doc[pi]
            vw = visual_words(pg)
            pw, ph = float(pg.rect.width), float(pg.rect.height)
            poly = rec.get("polygon_points")
            clipped = (sg._clip_words_to_polygon(vw, poly, pw, ph) if poly
                       else sg._clip_words_to_bbox(vw, rec.get("coords_norm") or [0, 0, 1, 1], pw, ph))
            toks = [w[4] for w in clipped]
            qf = sorted({t for t in toks if QF_LOOSE.match(t)})
            if len(qf) < 3:
                continue
            text = words_text(clipped)
            # текст, как его увидел бы ТЕКУЩИЙ production (без поворота)
            raw_words = pg.get_text("words")
            raw_clip = (sg._clip_words_to_polygon(raw_words, poly, pw, ph) if poly
                        else sg._clip_words_to_bbox(raw_words, rec.get("coords_norm") or [0, 0, 1, 1], pw, ph))
            prod_text = words_text(raw_clip)
            base = ss.structure_singleline_text(prod_text)
            graph = None
            try:
                graph = sg.build_singleline_graph(
                    pdf, prod_text, panel_hint="ГРЩ",
                    bbox_norm=rec.get("coords_norm"), polygon_norm=poly)
            except Exception:
                graph = None
            gate = sg.evaluate_vectograf_gate(graph)
            stop = ("structurer_none" if base is None else
                    "structurer_lt3_feeders" if base.get("feeder_total", 0) < 3 else
                    "geometry_none" if graph is None else
                    "gate_rejected" if not gate.get("use") else "ok")
            rows.append({
                "blocks_json": str(bp.relative_to(ROOT)),
                "page_index": pi,
                "page_rotation": pg.rotation,
                "block_id": rec.get("block_id"),
                "words_block_visual": len(clipped),
                "words_block_production": len(raw_clip),
                "rotation_word_loss": len(clipped) - len(raw_clip),
                "text_chars": len(text),
                "qf_unique": len(qf),
                "qf_dotted": sum(1 for t in qf if "." in t),
                "qf_prefixed": sum(1 for t in qf if re.match(r"^\d", t)),
                "qs_unique": len({t for t in toks if QS_LOOSE.match(t)}),
                "param_joined": sum(1 for l in text.split("\n") if ss.PARAM_RE.match(l.strip())),
                "param_separate": sum(1 for l in text.split("\n") if ss.SEPARATE_PARAM_RE.match(l.strip())),
                "kv_params": len(re.findall(r"(?:Рр|Pp|Ру|Py|Iр|Ip|Iрасч|cos\s*f|соs)\s*[=,]", text)),
                "structurer_feeders": (base or {}).get("feeder_total"),
                "graph_feeders": (graph or {}).get("feeders_total"),
                "gate_use": gate.get("use"),
                "gate_reasons": gate.get("reasons"),
                "stop": stop,
            })
            r = rows[-1]
            print(f"{stop:22s} rot={pg.rotation:3d} loss={r['rotation_word_loss']:4d} qf={r['qf_unique']:3d} "
                  f"dot={r['qf_dotted']:3d} pre={r['qf_prefixed']:3d} pj={r['param_joined']:3d} "
                  f"kv={r['kv_params']:4d}  {r['blocks_json'][-95:]}")
        doc.close()
    stats = collections.Counter(r["stop"] for r in rows)
    print("\n=== STOP DISTRIBUTION (blocks.json) ===")
    for k, v in stats.most_common():
        print(f"  {k:24s} {v}")
    (OUT / "corpus_audit_blocksjson.json").write_text(
        json.dumps({"total": len(rows), "stop_distribution": dict(stats), "blocks": rows},
                   ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
