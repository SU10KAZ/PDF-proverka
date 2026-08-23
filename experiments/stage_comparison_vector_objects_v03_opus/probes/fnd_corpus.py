# -*- coding: utf-8 -*-
"""Build the corpus index for the fnd probe: every prepared graphic block, with rotation.

Writes artifacts/fnd_corpus_index.json (documents) and artifacts/fnd_blocks.jsonl (blocks).
Read-only over projects_v2/.
"""
from __future__ import annotations

import glob, json, os, sys, time
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
ART.mkdir(parents=True, exist_ok=True)

PAT = "projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/result.json"


def main(limit: int | None = None) -> None:
    files = sorted(glob.glob(PAT))
    if limit:
        files = files[:limit]
    docs = []
    t0 = time.time()
    n_blocks = 0
    with open(ART / "fnd_blocks.jsonl", "w", encoding="utf-8") as out:
        for i, f in enumerate(files):
            pdf = str(Path(f).parent / "document.pdf")
            rec = {"result_json": f, "pdf": pdf, "pdf_exists": Path(pdf).exists()}
            try:
                blocks = F.iter_prepared_blocks(f, graphic_only=True,
                                                resolve_rotation_from_pdf=rec["pdf_exists"])
            except Exception as exc:
                rec["error"] = f"{type(exc).__name__}: {exc}"
                docs.append(rec)
                continue
            rot_hist: dict[str, int] = {}
            n_conflict = 0
            n_aspect_bad = 0
            src_hist: dict[str, int] = {}
            shape_hist: dict[str, int] = {}
            for b in blocks:
                n_conflict += bool(b.page_index_conflict)
                n_aspect_bad += (b.page_aspect_ok is False)
                rot_hist[str(b.rotation)] = rot_hist.get(str(b.rotation), 0) + 1
                src_hist[b.rotation_source] = src_hist.get(b.rotation_source, 0) + 1
                shape_hist[str(b.shape_type)] = shape_hist.get(str(b.shape_type), 0) + 1
                out.write(json.dumps({
                    "block_id": b.block_id, "page_number": b.page_number,
                    "page_index": b.page_index, "coords_px": list(b.coords_px),
                    "page_px": [b.page_px_w, b.page_px_h], "rotation": b.rotation,
                    "rotation_source": b.rotation_source, "shape_type": b.shape_type,
                    "page_index_field": b.page_index_field,
                    "page_index_conflict": b.page_index_conflict,
                    "page_aspect_ok": b.page_aspect_ok,
                    "category_code": b.category_code, "has_polygon": b.polygon_points is not None,
                    "ocr_len": len(b.ocr_text), "pdf": b.pdf_path, "result_json": f,
                    "doc_id": b.doc_id, "version": b.version, "discipline": b.discipline,
                    "obj_id": b.obj_id,
                }, ensure_ascii=False) + "\n")
                n_blocks += 1
            rec.update({
                "n_graphic_blocks": len(blocks),
                "rotation_hist": rot_hist,
                "rotation_source_hist": src_hist,
                "shape_hist": shape_hist,
                "n_page_index_conflict": n_conflict,
                "n_page_aspect_bad": n_aspect_bad,
                "discipline": blocks[0].discipline if blocks else None,
                "doc_id": blocks[0].doc_id if blocks else Path(f).parents[2].name,
                "version": blocks[0].version if blocks else Path(f).parents[1].name,
                "obj_id": blocks[0].obj_id if blocks else None,
            })
            docs.append(rec)
            F.clear_caches()
            if (i + 1) % 50 == 0:
                print(f"  {i+1}/{len(files)} docs, {n_blocks} blocks, {time.time()-t0:.0f}s", flush=True)
    summary = {
        "n_result_json": len(files),
        "n_graphic_blocks": n_blocks,
        "elapsed_s": round(time.time() - t0, 1),
    }
    rot_total: dict[str, int] = {}
    src_total: dict[str, int] = {}
    shape_total: dict[str, int] = {}
    for d in docs:
        for k, v in (d.get("rotation_hist") or {}).items():
            rot_total[k] = rot_total.get(k, 0) + v
        for k, v in (d.get("rotation_source_hist") or {}).items():
            src_total[k] = src_total.get(k, 0) + v
        for k, v in (d.get("shape_hist") or {}).items():
            shape_total[k] = shape_total.get(k, 0) + v
    summary["rotation_hist"] = rot_total
    summary["rotation_source_hist"] = src_total
    summary["shape_hist"] = shape_total
    summary["blocks_page_index_conflict"] = sum(d.get("n_page_index_conflict", 0) for d in docs)
    summary["blocks_page_aspect_bad"] = sum(d.get("n_page_aspect_bad", 0) for d in docs)
    summary["docs_missing_pdf"] = sum(1 for d in docs if not d.get("pdf_exists"))
    summary["docs_error"] = sum(1 for d in docs if d.get("error"))
    (ART / "fnd_corpus_index.json").write_text(
        json.dumps({"summary": summary, "documents": docs}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else None)
