# -*- coding: utf-8 -*-
"""F4 addendum — which page does a prepared block actually live on?

result.json carries BOTH ``pages[].page_number`` (1-based) and ``blocks[].page_index``
(documented 0-based).  In part of the corpus they disagree: page_index equals
page_number, i.e. it is 1-based and points one page too far.  Production
(``crop_from_pdf``) uses ``page_number - 1``.  Any probe that trusts ``page_index``
reads a different sheet.

Referee: the page aspect ratio.  result.json stores the page pixel size; the PDF page
rect must have the same aspect.  Whichever candidate index matches wins.
"""
from __future__ import annotations

import json, os, sys, time
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F  # noqa: E402
import fitz  # noqa: E402

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")
TOL = 0.01   # relative aspect tolerance


def main():
    idx = json.loads((ART / "fnd_corpus_index.json").read_text(encoding="utf-8"))
    cnt = Counter()
    doc_rows = []
    t0 = time.time()
    for k, docrec in enumerate(idx["documents"]):
        rj = docrec["result_json"]
        if not docrec.get("pdf_exists"):
            cnt["docs_no_pdf"] += 1
            continue
        pdf = docrec["pdf"]
        try:
            data = json.loads(Path(rj).read_text(encoding="utf-8"))
            doc = F.open_doc(pdf)
            npages = doc.page_count
            rects = [(doc[i].rect.width, doc[i].rect.height) for i in range(npages)]
        except Exception:
            cnt["docs_error"] += 1
            continue
        d = {"result_json": rj, "n_pdf_pages": npages, "pages": 0, "shift": Counter()}
        for pg in data.get("pages") or []:
            try:
                pn = int(pg.get("page_number"))
            except Exception:
                continue
            pw, ph = pg.get("width") or 0, pg.get("height") or 0
            blocks = [b for b in (pg.get("blocks") or []) if b.get("block_type") == "image"]
            if not blocks:
                continue
            d["pages"] += 1
            try:
                pi_field = int(blocks[0].get("page_index"))
            except Exception:
                pi_field = None
            cand = {"page_number_minus_1": pn - 1, "page_index_field": pi_field}
            ar_json = (pw / ph) if ph else None
            verdict = {}
            for name, i in cand.items():
                if i is None or i < 0 or i >= npages or not ar_json:
                    verdict[name] = None
                    continue
                w, h = rects[i]
                ar = w / h if h else 0
                verdict[name] = abs(ar - ar_json) / ar_json <= TOL
            v_pn = verdict["page_number_minus_1"]
            cnt["pn_aspect_" + ("true" if v_pn is True else "false" if v_pn is False else "none")] += 1
            if v_pn is not True:
                cnt["blocks_pn_aspect_not_true"] += len(blocks)
            agree = (pi_field is not None and pi_field == pn - 1)
            cnt["pages_total"] += 1
            cnt["blocks_total"] += len(blocks)
            if agree:
                cnt["pages_agree"] += 1
                cnt["blocks_agree"] += len(blocks)
                if verdict["page_number_minus_1"] is True:
                    cnt["pages_agree_aspect_ok"] += 1
                elif verdict["page_number_minus_1"] is False:
                    cnt["pages_agree_aspect_bad"] += 1
            else:
                cnt["pages_conflict"] += 1
                cnt["blocks_conflict"] += len(blocks)
                d["shift"][str((pi_field - (pn - 1)) if pi_field is not None else "none")] += 1
                if verdict["page_number_minus_1"] and not verdict["page_index_field"]:
                    cnt["conflict_page_number_wins"] += 1
                    cnt["blocks_conflict_page_number_wins"] += len(blocks)
                elif verdict["page_index_field"] and not verdict["page_number_minus_1"]:
                    cnt["conflict_page_index_wins"] += 1
                    cnt["blocks_conflict_page_index_wins"] += len(blocks)
                elif verdict["page_index_field"] and verdict["page_number_minus_1"]:
                    cnt["conflict_ambiguous_both_match"] += 1
                    cnt["blocks_conflict_ambiguous"] += len(blocks)
                else:
                    cnt["conflict_neither_matches"] += 1
                    cnt["blocks_conflict_neither"] += len(blocks)
        if d["shift"]:
            d["shift"] = dict(d["shift"])
            doc_rows.append(d)
        else:
            d.pop("shift")
        F.clear_caches()
        if (k + 1) % 100 == 0:
            print(f"  {k+1}/{len(idx['documents'])} {time.time()-t0:.0f}s", flush=True)

    summary = dict(cnt)
    summary["share_pages_conflict"] = cnt["pages_conflict"] / max(1, cnt["pages_total"])
    summary["share_blocks_conflict"] = cnt["blocks_conflict"] / max(1, cnt["blocks_total"])
    summary["n_docs_with_conflict"] = len(doc_rows)
    summary["aspect_tolerance"] = TOL
    (ART / "fnd_page_index.json").write_text(json.dumps(
        {"summary": summary, "conflicting_documents": doc_rows[:200]},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
