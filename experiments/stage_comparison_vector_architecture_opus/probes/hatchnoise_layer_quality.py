"""How USEFUL is the PDF layer signal where it exists?

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_layer_quality
"""
from __future__ import annotations

import collections
import json
import re
import time
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "hatchnoise_layer_quality.json"

# Regexes for layer names that name background / hatch / furniture / underlay,
# written from CAD naming practice (AIA layer standard + Russian CAD habits).
HATCH_RE = re.compile(r"(PATT|HATCH|штрих|ШТРИХ|Штрих|IZOLAT|ИЗОЛЯ|заливк|Заливк)", re.IGNORECASE)
FURNITURE_RE = re.compile(r"(мебел|МЕБЕЛ|FURN|Мебел|оборуд.*интерьер|растен|озелен)", re.IGNORECASE)
UNDERLAY_RE = re.compile(r"(XREF|подоснов|Подоснов|ПОДОСНОВ|underlay)", re.IGNORECASE)


def main() -> None:
    pdfs = sorted(ROOT.glob("projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.pdf"))
    rows = []
    t0 = time.time()
    for path in pdfs:
        try:
            doc = fitz.open(path)
        except Exception:
            continue
        try:
            ocgs = doc.get_ocgs() or {}
        except Exception:
            ocgs = {}
        if not ocgs or len(doc) == 0:
            doc.close()
            continue
        page_index = len(doc) // 2
        try:
            drawings = doc[page_index].get_drawings()
        except Exception:
            doc.close()
            continue
        counter = collections.Counter(str(d.get("layer") or "") for d in drawings)
        named = sum(v for k, v in counter.items() if k)
        if named == 0:
            doc.close()
            rows.append({
                "pdf": str(path.relative_to(ROOT)), "page": page_index, "drawings": len(drawings),
                "named": 0, "distinct": 0, "top_share": None, "hatch_share": None, "verdict": "no_layer_on_page",
            })
            continue
        named_counter = {k: v for k, v in counter.items() if k}
        top = max(named_counter.values()) / named
        hatch = sum(v for k, v in named_counter.items() if HATCH_RE.search(k))
        furn = sum(v for k, v in named_counter.items() if FURNITURE_RE.search(k))
        under = sum(v for k, v in named_counter.items() if UNDERLAY_RE.search(k))
        default_layer = sum(v for k, v in named_counter.items() if k.strip() in {"0", "Layer0", "layer0"})
        verdict = "informative"
        if default_layer / named > 0.9:
            verdict = "degenerate_layer0"
        elif top > 0.9 and len(named_counter) <= 2:
            verdict = "degenerate_single_layer"
        rows.append({
            "pdf": str(path.relative_to(ROOT)),
            "page": page_index,
            "drawings": len(drawings),
            "named": named,
            "distinct": len(named_counter),
            "top_share": round(top, 3),
            "hatch_share": round(hatch / named, 3),
            "furniture_share": round(furn / named, 3),
            "underlay_share": round(under / named, 3),
            "default_layer0_share": round(default_layer / named, 3),
            "verdict": verdict,
            "top_layers": collections.Counter(named_counter).most_common(6),
        })
        doc.close()
        if time.time() - t0 > 480:
            break

    informative = [r for r in rows if r["verdict"] == "informative"]
    payload = {
        "probe": "hatchnoise_layer_quality",
        "elapsed_s": round(time.time() - t0, 1),
        "ocg_pdfs_examined": len(rows),
        "pages_without_layer_names": sum(1 for r in rows if r["named"] == 0),
        "pages_degenerate_layer0": sum(1 for r in rows if r["verdict"] == "degenerate_layer0"),
        "pages_degenerate_single": sum(1 for r in rows if r["verdict"] == "degenerate_single_layer"),
        "pages_informative": len(informative),
        "informative_median_hatch_share": (
            sorted(r["hatch_share"] for r in informative)[len(informative) // 2] if informative else None
        ),
        "informative_mean_hatch_share": (
            round(sum(r["hatch_share"] for r in informative) / len(informative), 3) if informative else None
        ),
        "rows": rows,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in payload.items() if k != "rows"}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
