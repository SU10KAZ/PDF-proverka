# -*- coding: utf-8 -*-
"""pd_pairs — поиск пар документов, описывающих ОДИН И ТОТ ЖЕ предмет.

Ось 1 (целевая): П ↔ РД.
Ось 2 (запасная): одинаковый предмет при одинаковой стадии (разные документы/ревизии).

Обоснование пары — код документа из штампа + названия листов (sheet_name), не догадка.
"""
from __future__ import annotations
import json, re, sys, itertools, collections
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
CENSUS = json.load(open(BASE / "artifacts" / "pd_stage_census.json", encoding="utf-8"))
OUT = BASE / "artifacts" / "pd_doc_pairs.json"

STOP = set("и в на по для с из до от the".split())


def norm_name(s):
    if not s:
        return ""
    s = str(s).lower().replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9]+", " ", s)
    return " ".join(w for w in s.split() if w not in STOP)


def toks(s):
    return set(norm_name(s).split())


def code_stem(code):
    """Код документа без хвостовых номеров листов/секций: ПД-00542664-ЭМ2-1.3 -> пд 00542664 эм"""
    c = norm_name(code)
    c = re.sub(r"\d+", " ", c)
    return " ".join(c.split())


def doc_profile(d):
    names = collections.Counter()
    for s in d.get("sheets") or []:
        n = norm_name(s.get("sheet_name"))
        if n:
            names[n] += 1
    codes = collections.Counter(d.get("document_code_hist") or {})
    return names, codes


def jac(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def main():
    docs = [d for d in CENSUS["documents"] if d.get("n_pages")]
    for d in docs:
        d["_names"], d["_codes"] = doc_profile(d)
        d["_name_tokens"] = set()
        for n in d["_names"]:
            d["_name_tokens"] |= set(n.split())
        d["_code_stems"] = {code_stem(c) for c in d["_codes"]}
        d["_code_tokens"] = set()
        for c in d["_codes"]:
            d["_code_tokens"] |= toks(c)

    pairs = []
    by_obj = collections.defaultdict(list)
    for d in docs:
        by_obj[d.get("obj_id")].append(d)

    for obj, group in by_obj.items():
        for a, b in itertools.combinations(group, 2):
            if a["result_json"] == b["result_json"]:
                continue
            sn = jac(a["_name_tokens"], b["_name_tokens"])
            ct = jac(a["_code_tokens"], b["_code_tokens"])
            shared_names = set(a["_names"]) & set(b["_names"])
            comparable = sum(min(a["_names"][n], b["_names"][n]) for n in shared_names)
            stages_a = set((a.get("stage_hist") or {}).keys())
            stages_b = set((b.get("stage_hist") or {}).keys())
            cross_stage = bool(({"П", "ДП"} & stages_a) and (stages_b - {"П", "ДП"})) or \
                          bool(({"П", "ДП"} & stages_b) and (stages_a - {"П", "ДП"}))
            if comparable == 0 and sn < 0.35 and ct < 0.5:
                continue
            pairs.append({
                "obj_id": obj,
                "a": {"doc_id": a["doc_id"], "version": a["version"], "discipline": a["discipline"],
                      "result_json": a["result_json"], "stage_hist": a["stage_hist"],
                      "n_pages": a["n_pages"], "n_image_blocks": a["n_image_blocks"],
                      "pdf_exists": a["pdf_exists"], "codes": list(a["_codes"])[:3]},
                "b": {"doc_id": b["doc_id"], "version": b["version"], "discipline": b["discipline"],
                      "result_json": b["result_json"], "stage_hist": b["stage_hist"],
                      "n_pages": b["n_pages"], "n_image_blocks": b["n_image_blocks"],
                      "pdf_exists": b["pdf_exists"], "codes": list(b["_codes"])[:3]},
                "same_discipline": a["discipline"] == b["discipline"],
                "sheet_name_jaccard": round(sn, 3),
                "code_token_jaccard": round(ct, 3),
                "n_shared_sheet_names": len(shared_names),
                "comparable_sheets": comparable,
                "shared_sheet_names_sample": sorted(shared_names)[:8],
                "claims_cross_stage": cross_stage,
                "code_stem_equal": bool(a["_code_stems"] & b["_code_stems"]),
            })

    pairs.sort(key=lambda p: (-p["comparable_sheets"], -p["sheet_name_jaccard"]))
    cross = [p for p in pairs if p["claims_cross_stage"]]
    summary = {
        "n_docs": len(docs),
        "n_candidate_pairs": len(pairs),
        "n_pairs_claiming_cross_stage": len(cross),
        "n_pairs_cross_stage_both_pdf": sum(1 for p in cross if p["a"]["pdf_exists"] and p["b"]["pdf_exists"]),
        "n_pairs_cross_stage_comparable_ge1": sum(1 for p in cross if p["comparable_sheets"] >= 1),
        "note": "claims_cross_stage — по ЗАЯВЛЕННОМУ полю stamp_data.stage; проверка стадии глазами — pd_stage_verify.json",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    OUT.write_text(json.dumps({"summary": summary, "cross_stage_candidates": cross[:200],
                               "same_stage_top": [p for p in pairs if not p["claims_cross_stage"]][:200]},
                              ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
