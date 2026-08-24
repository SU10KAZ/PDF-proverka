#!/usr/bin/env python3
"""G2 research: определение диалектов ЭОМ-однолинеек по реальному корпусу.

Собирает подготовленные графические блоки ЭОМ из ОБОИХ форматов хранения
(result.json и blocks.json), считает сигнатуры и раскладывает по диалектам.
Ничего в production не меняет.
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
OUT.mkdir(parents=True, exist_ok=True)
TEXTS = OUT / "corpus_texts"
TEXTS.mkdir(exist_ok=True)

QF_LOOSE = re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$")
QS_LOOSE = re.compile(r"^\d{0,2}QS\d+(?:\.\d+)*$")
# key-value расчётная подпись ГРЩ/ВРУ: «Рр=157,5кВт, cosf=0,67, Iрасч=360А»
KV_RE = re.compile(r"(?:Рр|Pp|Рp|Ру|Py|Iр|Ip|Iрасч|Iр асч)\s*=\s*[\d.,/]+")
BUS_COMMA = re.compile(r"L1,\s*L2,\s*L3")
BUS_DASH = re.compile(r"L1\s*-\s*L2\s*-\s*L3")


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


def signals(text, words=None):
    toks = re.split(r"\s+", text)
    qf = sorted({t for t in toks if QF_LOOSE.match(t)})
    qs = sorted({t for t in toks if QS_LOOSE.match(t)})
    lines = text.split("\n")
    dotted = sum(1 for t in qf if "." in t)
    prefixed = sum(1 for t in qf if re.match(r"^\d", t))
    return {
        "qf_unique": len(qf),
        "qf_dotted_frac": round(dotted / max(len(qf), 1), 3),
        "qf_prefixed_frac": round(prefixed / max(len(qf), 1), 3),
        "qf_prefix_groups": sorted({re.match(r"^(\d*)QF", t).group(1) for t in qf if re.match(r"^\d", t)}),
        "qs_unique": len(qs),
        "qs_labels": qs[:6],
        "param_joined": sum(1 for l in lines if ss.PARAM_RE.match(l.strip())),
        "param_separate": sum(1 for l in lines if ss.SEPARATE_PARAM_RE.match(l.strip())),
        "kv_params": len(KV_RE.findall(text)),
        "bus_marker_comma": len(BUS_COMMA.findall(text)),
        "bus_marker_dash": len(BUS_DASH.findall(text)),
        "pen_marker": text.count("PEN"),
        "ta_tokens": len(re.findall(r"\b[12]?[ТT][ТTA]\d", text)),
        "avr_marker": len(re.findall(r"АВР", text)),
        "aukrm_marker": len(re.findall(r"АУКРМ|УКРМ|КРМ\b", text)),
        "vru_family": len(re.findall(r"\bВРУ[\w.\-]*", text)),
        "shu_family": len(re.findall(r"\bШУ[\w.\-]*|\bЩ[А-Я][\w.\-]*", text)),
        "transformer_marker": len(re.findall(r"(?<![А-Яа-я])Т[12](?![\w])|ТП\d", text)),
    }


def classify(s):
    """Минимальный набор диалектов. Возвращает (dialect, why)."""
    why = []
    has_calc_anchor = s["param_joined"] >= 3 or s["param_separate"] >= 3
    has_kv = s["kv_params"] >= 6
    dotted = s["qf_dotted_frac"] >= 0.6
    prefixed = s["qf_prefixed_frac"] >= 0.6
    if has_calc_anchor and dotted:
        why.append("расчётный якорь строкой + точечная нумерация QF")
        return "classic_calc_singleline", why
    if prefixed and (s["qs_unique"] or s["bus_marker_dash"] or s["bus_marker_comma"]) and s["qf_unique"] >= 10:
        why.append("секционная нумерация <секция>QF<N> + маркер шин/секционный аппарат")
        return "dense_sectioned_board", why
    if has_kv and not has_calc_anchor:
        why.append("key-value расчётная подпись без построчного якоря")
        return "kv_annotated_singleline", why
    if s["qf_unique"] >= 3 and not has_calc_anchor and not has_kv:
        why.append("QF есть, расчётных подписей нет — схема без расчёта")
        return "bare_device_scheme", why
    why.append("сигналов не хватает")
    return "unknown_singleline", why


def iter_result_json():
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
                if len({t for t in re.split(r"\s+", text) if QF_LOOSE.match(t)}) < 3:
                    continue
                yield {
                    "store": "result.json",
                    "path": str(rp.relative_to(ROOT)),
                    "page_index": pg.get("page_index"),
                    "page_rotation": 0,
                    "block_id": str(b.get("id") or b.get("block_id") or ""),
                    "text": text,
                }


def iter_blocks_json():
    for bp in sorted(ROOT.glob("projects_v2/objects/*/**/02_work/blocks.json")):
        if (bp.parent / "result.json").exists():
            continue
        pdf = bp.parent / "document.pdf"
        if not pdf.exists():
            continue
        try:
            data = json.loads(bp.read_text(encoding="utf-8"))
            doc = fitz.open(str(pdf))
        except Exception:
            continue
        for rec in data.get("blocks") or []:
            pi = int(rec.get("page_index") or 0)
            if pi >= doc.page_count:
                continue
            pg = doc[pi]
            vw = visual_words(pg)
            pw, ph = float(pg.rect.width), float(pg.rect.height)
            poly = rec.get("polygon_points")
            clipped = (sg._clip_words_to_polygon(vw, poly, pw, ph) if poly
                       else sg._clip_words_to_bbox(vw, rec.get("coords_norm") or [0, 0, 1, 1], pw, ph))
            text = words_text(clipped)
            if len({t for t in re.split(r"\s+", text) if QF_LOOSE.match(t)}) < 3:
                continue
            yield {
                "store": "blocks.json",
                "path": str(bp.relative_to(ROOT)),
                "page_index": pi,
                "page_rotation": pg.rotation,
                "block_id": rec.get("block_id"),
                "text": text,
            }
        doc.close()


def main():
    rows = []
    for item in list(iter_result_json()) + list(iter_blocks_json()):
        s = signals(item["text"])
        dialect, why = classify(s)
        rows.append({k: v for k, v in item.items() if k != "text"} | {
            "signals": s, "dialect": dialect, "why": why,
            "text_chars": len(item["text"]),
        })
        name = f"{item['block_id']}_{abs(hash(item['path'])) % 10**6}.txt"
        (TEXTS / name).write_text(item["text"], encoding="utf-8")
        rows[-1]["text_dump"] = f"corpus_texts/{name}"

    dist = collections.Counter(r["dialect"] for r in rows)
    by_project = collections.defaultdict(collections.Counter)
    for r in rows:
        proj = r["path"].split("/")[2] if r["path"].startswith("projects_v2/objects/") else "?"
        by_project[proj][r["dialect"]] += 1
    print("=== DIALECT DISTRIBUTION (n=%d) ===" % len(rows))
    for k, v in dist.most_common():
        print(f"  {k:28s} {v}")
    print("\n=== BY PROJECT ===")
    for p, c in by_project.items():
        print(f"  {p}: {dict(c)}")
    (OUT / "dialects.json").write_text(json.dumps({
        "total": len(rows),
        "distribution": dict(dist),
        "by_project": {k: dict(v) for k, v in by_project.items()},
        "blocks": rows,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
