# -*- coding: utf-8 -*-
"""pd_census — полная перепись стадий (П / Р / РД / ...) по всему корпусу result.json.

Единица переписи — ВЕРСИЯ документа (один result.json).
Стадия берётся из stamp_data/ocr_json блоков со штампом (поле "stage").
"""
from __future__ import annotations
import json, os, sys, time, collections
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa

PROJ = ROOT / "projects_v2" / "objects"
OUT = Path(__file__).resolve().parents[1] / "artifacts" / "pd_stage_census.json"


def parse_json_field(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            j = json.loads(v)
            return j if isinstance(j, dict) else None
        except Exception:
            return None
    return None


def norm_stage(s):
    if s is None:
        return None
    s = str(s).strip().strip('.').upper().replace("РД", "РД")
    if not s:
        return None
    return s


def vector_probe(pdf_path, page_indices):
    """Есть ли вектор-слой: число path-объектов на выборке страниц."""
    try:
        doc = F.open_doc(pdf_path)
    except Exception:
        return None
    tot = 0
    n = 0
    for pi in page_indices:
        if pi < 0 or pi >= doc.page_count:
            continue
        try:
            tot += len(doc[pi].get_drawings())
            n += 1
        except Exception:
            pass
    if n == 0:
        return None
    return {"pages_probed": n, "drawings_total": tot, "drawings_per_page": round(tot / n, 1)}


def main():
    t0 = time.time()
    rjs = sorted(PROJ.glob("*/disciplines/*/documents/*/versions/*/02_work/result.json"))
    docs = []
    for i, rj in enumerate(rjs):
        rel = str(rj.relative_to(ROOT))
        parts = rj.parts
        obj = parts[parts.index("objects") + 1]
        disc = parts[parts.index("disciplines") + 1]
        doc_id = parts[parts.index("documents") + 1]
        ver = parts[parts.index("versions") + 1]
        pdf = rj.parent / "document.pdf"
        try:
            data = json.load(open(rj, encoding="utf-8"))
        except Exception as e:
            docs.append({"result_json": rel, "error": repr(e)})
            continue
        pages = data.get("pages") or []
        stage_hist = collections.Counter()
        code_hist = collections.Counter()
        org_hist = collections.Counter()
        sheets = []   # (page_number, stage, sheet_number, sheet_name, document_code)
        n_img = 0
        n_txt = 0
        page_numbers = []
        for pg in pages:
            pn = pg.get("page_number")
            page_numbers.append(pn)
            best = None
            for b in pg.get("blocks") or []:
                bt = b.get("block_type")
                if bt == "image":
                    n_img += 1
                elif bt == "text":
                    n_txt += 1
                sd = parse_json_field(b.get("stamp_data")) or parse_json_field(b.get("ocr_json"))
                if sd and ("stage" in sd or "sheet_name" in sd or "document_code" in sd):
                    if best is None:
                        best = sd
            if best:
                st = norm_stage(best.get("stage"))
                if st:
                    stage_hist[st] += 1
                if best.get("document_code"):
                    code_hist[str(best["document_code"]).strip()] += 1
                if best.get("organization"):
                    org_hist[str(best["organization"]).strip()] += 1
                sheets.append({
                    "page_number": pn,
                    "stage": st,
                    "sheet_number": best.get("sheet_number"),
                    "sheet_name": (best.get("sheet_name") or None),
                    "document_code": (best.get("document_code") or None),
                })
        vec = vector_probe(str(pdf), [0, len(pages) // 2, max(0, len(pages) - 1)]) if pdf.exists() else None
        docs.append({
            "result_json": rel,
            "obj_id": obj, "discipline": disc, "doc_id": doc_id, "version": ver,
            "pdf_exists": pdf.exists(),
            "n_pages": len(pages),
            "n_image_blocks": n_img,
            "n_text_blocks": n_txt,
            "coordinate_space": data.get("coordinate_space"),
            "source": data.get("source"),
            "stage_hist": dict(stage_hist),
            "stage_dominant": (stage_hist.most_common(1)[0][0] if stage_hist else None),
            "stage_dominant_share": (round(stage_hist.most_common(1)[0][1] / sum(stage_hist.values()), 3) if stage_hist else None),
            "n_stamped_pages": len(sheets),
            "document_code_hist": dict(code_hist),
            "organization_hist": dict(org_hist),
            "vector": vec,
            "sheets": sheets,
        })
        if (i + 1) % 50 == 0:
            print(f"{i+1}/{len(rjs)} {time.time()-t0:.0f}s", file=sys.stderr)
            F.clear_caches()
    stage_docs = collections.Counter()
    stage_pages = collections.Counter()
    for d in docs:
        if d.get("stage_dominant"):
            stage_docs[d["stage_dominant"]] += 1
        for s, c in (d.get("stage_hist") or {}).items():
            stage_pages[s] += c
    summary = {
        "n_result_json": len(rjs),
        "n_docs_with_stage": sum(1 for d in docs if d.get("stage_dominant")),
        "n_docs_no_stamp": sum(1 for d in docs if not d.get("stage_dominant")),
        "stage_docs_hist": dict(stage_docs.most_common()),
        "stage_pages_hist": dict(stage_pages.most_common()),
        "elapsed_s": round(time.time() - t0, 1),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    OUT.write_text(json.dumps({"summary": summary, "documents": docs}, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
