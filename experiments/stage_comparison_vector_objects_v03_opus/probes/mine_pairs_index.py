"""mine · step 1 — index document version pairs and match their pages.

Only reads.  Writes artifacts/mine_pair_index.json.
Page matching rule R1: sheet key from the stamp block's ocr_json
(sheet_number + sheet_name, normalised); rule R2: page_number.
Both are computed so their agreement can be measured.
"""
import ast, json, os, re, sys, time, collections
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa: E402

ART = Path(__file__).resolve().parents[1] / "artifacts"


def _parse_maybe(s):
    if not s:
        return {}
    if isinstance(s, dict):
        return s
    t = str(s).strip()
    if t.startswith("```"):
        t = re.sub(r"^```(json)?", "", t).strip()
        t = re.sub(r"```$", "", t).strip()
    for fn in (json.loads, ast.literal_eval):
        try:
            v = fn(t)
            if isinstance(v, dict):
                return v
        except Exception:
            pass
    return {}


def _norm(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def page_sheet_key(page):
    """sheet identity of a page, from any stamp block on it."""
    for b in page.get("blocks") or []:
        if (b.get("category_code") or "") != "stamp":
            continue
        j = _parse_maybe(b.get("ocr_json"))
        if not j:
            j = _parse_maybe(b.get("ocr_text"))
        sn = _norm(j.get("sheet_name"))
        num = _norm(j.get("sheet_number"))
        if sn or num:
            return f"{num}||{sn}"
    return ""


def doc_pages(rj):
    with open(rj, encoding="utf-8") as fh:
        d = json.load(fh)
    out = []
    for p in d.get("pages") or []:
        try:
            pn = int(p.get("page_number"))
        except Exception:
            continue
        imgs = [b for b in (p.get("blocks") or []) if b.get("block_type") == "image"]
        out.append({
            "page_number": pn,
            "w": int(p.get("width") or 0), "h": int(p.get("height") or 0),
            "sheet_key": page_sheet_key(p),
            "n_img": len(imgs),
            "n_txt": len([b for b in (p.get("blocks") or []) if b.get("block_type") == "text"]),
        })
    return out


def main():
    idx = json.load(open(ART / "fnd_corpus_index.json", encoding="utf-8"))
    docs = [x for x in idx["documents"] if x["pdf_exists"]]
    by = collections.defaultdict(list)
    for x in docs:
        by[(x["obj_id"], x["discipline"], x["doc_id"])].append(x)
    multi = {k: sorted(v, key=lambda y: y["version"]) for k, v in by.items() if len(v) >= 2}

    pairs = []
    t0 = time.time()
    for k, versions in sorted(multi.items(), key=lambda kv: tuple(str(x) for x in kv[0])):
        for a, b in zip(versions, versions[1:]):
            ra, rb = a["result_json"], b["result_json"]
            pa, pb = a["pdf"], b["pdf"]
            try:
                sha_a, sha_b = F.pdf_sha256(pa), F.pdf_sha256(pb)
            except Exception as e:
                continue
            PA, PB = doc_pages(ra), doc_pages(rb)
            # R1: sheet key
            ka = collections.defaultdict(list)
            for p in PA:
                if p["sheet_key"]:
                    ka[p["sheet_key"]].append(p["page_number"])
            kb = collections.defaultdict(list)
            for p in PB:
                if p["sheet_key"]:
                    kb[p["sheet_key"]].append(p["page_number"])
            r1 = {}
            for key, la in ka.items():
                lb = kb.get(key)
                if lb and len(la) == 1 and len(lb) == 1:
                    r1[la[0]] = lb[0]
            # R2: page_number
            setb = {p["page_number"] for p in PB}
            r2 = {p["page_number"]: p["page_number"] for p in PA if p["page_number"] in setb}
            both = set(r1) & set(r2)
            agree = sum(1 for x in both if r1[x] == r2[x])
            pairs.append({
                "obj_id": k[0], "discipline": k[1], "doc_id": k[2],
                "ver_a": a["version"], "ver_b": b["version"],
                "result_a": ra, "result_b": rb, "pdf_a": pa, "pdf_b": pb,
                "sha_a": sha_a, "sha_b": sha_b, "same_pdf": sha_a == sha_b,
                "n_pages_a": len(PA), "n_pages_b": len(PB),
                "n_img_a": sum(p["n_img"] for p in PA), "n_img_b": sum(p["n_img"] for p in PB),
                "sheetkey_cov_a": round(len([p for p in PA if p["sheet_key"]]) / max(1, len(PA)), 3),
                "sheetkey_cov_b": round(len([p for p in PB if p["sheet_key"]]) / max(1, len(PB)), 3),
                "r1_matched": len(r1), "r2_matched": len(r2),
                "r1r2_overlap": len(both), "r1r2_agree": agree,
                "page_map_r1": {str(x): y for x, y in sorted(r1.items())},
                "page_map_r2": {str(x): y for x, y in sorted(r2.items())},
            })
    summary = {
        "n_docs_multi_version": len(multi),
        "n_version_pairs": len(pairs),
        "n_pairs_same_pdf_sha256": sum(1 for p in pairs if p["same_pdf"]),
        "r1r2_overlap_total": sum(p["r1r2_overlap"] for p in pairs),
        "r1r2_agree_total": sum(p["r1r2_agree"] for p in pairs),
        "elapsed_s": round(time.time() - t0, 1),
    }
    summary["r1r2_agreement_rate"] = round(
        summary["r1r2_agree_total"] / max(1, summary["r1r2_overlap_total"]), 4)
    out = {"schema_version": "mine_pair_index/1", "research_only": True,
           "summary": summary, "pairs": pairs}
    (ART / "mine_pair_index.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
