#!/usr/bin/env python3
"""G2 research: аудит корпуса подготовленных ЭОМ-блоков однолинейных/щитовых схем.

Для каждого блока-кандидата (в вектор-тексте есть QF/QS-обозначения) прогоняет
ТЕКУЩИЙ production-вектограф и фиксирует, что он вернул. Ничего не меняет.
"""
from __future__ import annotations

import collections
import json
import re
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding import singleline_structurer as ss  # noqa: E402
from backend.app.pipeline.stages.block_grounding import singleline_graph_geometry as sg  # noqa: E402

OUT = Path(__file__).resolve().parent / "artifacts"
OUT.mkdir(parents=True, exist_ok=True)

QF_LOOSE = re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$")
QS_LOOSE = re.compile(r"^\d{0,2}QS\d+(?:\.\d+)*$")
MIN_QF = 3


def candidates():
    for rp in sorted(ROOT.glob("projects_v2/objects/*/**/02_work/result.json")):
        try:
            rj = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            continue
        for pg in rj.get("pages", []) or []:
            for b in (pg.get("blocks") or []):
                text = b.get("pdfplumber_text") or ""
                if len(text) < 200:
                    continue
                toks = re.split(r"\s+", text)
                qf = [t for t in toks if QF_LOOSE.match(t)]
                if len(set(qf)) < MIN_QF:
                    continue
                yield rp, pg, b, text, qf


def main():
    rows = []
    seen = set()
    for rp, pg, b, text, qf in candidates():
        bid = str(b.get("id") or b.get("block_id") or "")
        key = (str(rp), bid)
        if key in seen:
            continue
        seen.add(key)
        pdf = rp.parent / "document.pdf"
        if not pdf.exists():
            continue
        toks = re.split(r"\s+", text)
        qs = [t for t in toks if QS_LOOSE.match(t)]
        qf_uniq = sorted(set(qf))
        qf_dotted = [t for t in qf_uniq if "." in t]
        qf_prefixed = [t for t in qf_uniq if re.match(r"^\d", t)]
        base = None
        try:
            base = ss.structure_singleline_text(text)
        except Exception:
            base = None
        graph = None
        err = None
        try:
            graph = sg.build_singleline_graph(
                pdf, text, panel_hint="ВРУ",
                bbox_norm=b.get("coords_norm"),
                polygon_norm=b.get("polygon_points_norm"),
            )
        except Exception:
            err = traceback.format_exc(limit=2)
        gate = sg.evaluate_vectograf_gate(graph)
        # какая стадия отказала
        if base is None:
            stop = "structurer_none"
        elif base.get("feeder_total", 0) < 3:
            stop = "structurer_lt3_feeders"
        elif graph is None:
            stop = "geometry_none"
        elif not gate.get("use"):
            stop = "gate_rejected"
        else:
            stop = "ok"
        rows.append({
            "result_json": str(rp.relative_to(ROOT)),
            "page_index": pg.get("page_index"),
            "block_id": bid,
            "text_chars": len(text),
            "qf_unique": len(qf_uniq),
            "qf_dotted": len(qf_dotted),
            "qf_prefixed": len(qf_prefixed),
            "qs_unique": len(set(qs)),
            "param_joined": sum(1 for l in text.split("\n") if ss.PARAM_RE.match(l.strip())),
            "param_separate": sum(1 for l in text.split("\n") if ss.SEPARATE_PARAM_RE.match(l.strip())),
            "kv_params": len(re.findall(r"(?:Рр|Pp|Ру|Py|Iр|Ip|Iрасч|cos\s*f|соs)\s*[=,]", text)),
            "structurer_feeders": (base or {}).get("feeder_total"),
            "graph_feeders": (graph or {}).get("feeders_total"),
            "gate_use": gate.get("use"),
            "gate_reasons": gate.get("reasons"),
            "gate_metrics": gate.get("metrics"),
            "stop": stop,
            "error": err,
        })
        print(f"{stop:24s} qf={len(qf_uniq):3d} dot={len(qf_dotted):3d} pre={len(qf_prefixed):3d} "
              f"pj={rows[-1]['param_joined']:3d} ps={rows[-1]['param_separate']:3d} "
              f"kv={rows[-1]['kv_params']:4d}  {rows[-1]['result_json'][:90]} {bid[:12]}")

    stats = collections.Counter(r["stop"] for r in rows)
    print("\n=== STOP DISTRIBUTION ===")
    for k, v in stats.most_common():
        print(f"  {k:24s} {v}")
    (OUT / "corpus_audit.json").write_text(
        json.dumps({"total": len(rows), "stop_distribution": dict(stats), "blocks": rows},
                   ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
