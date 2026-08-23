"""mine · step 1b — how accurate is the page-matching rule?

Independent arbiter R3: best match by page text-token Jaccard (PDF text layer),
accepted only when top1 - top2 >= 0.10 and top1 >= 0.30.  R1 (stamp sheet key)
and R2 (page_number) are then scored against R3.
Writes artifacts/mine_pagematch.json
"""
import json, re, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa
import fitz

ART = Path(__file__).resolve().parents[1] / "artifacts"
TOK = re.compile(r"[0-9A-Za-zА-Яа-яЁё\.\-/]{3,}")


def page_tokens(pdf, n):
    doc = F.open_doc(pdf)
    out = []
    for i in range(min(n, doc.page_count)):
        t = doc[i].get_text("text") or ""
        out.append(set(x.lower() for x in TOK.findall(t)))
    return out


def jac(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def main(limit=0):
    idx = json.load(open(ART / "mine_pair_index.json", encoding="utf-8"))
    pairs = [p for p in idx["pairs"] if not p["same_pdf"]]
    if limit:
        pairs = pairs[:limit]
    rows = []
    t0 = time.time()
    for p in pairs:
        try:
            ta = page_tokens(p["pdf_a"], 400)
            tb = page_tokens(p["pdf_b"], 400)
        except Exception as e:
            rows.append({"doc": p["doc_id"], "error": str(e)[:120]})
            continue
        r3 = {}
        for i, sa in enumerate(ta):
            sc = sorted(((jac(sa, sb), j) for j, sb in enumerate(tb)), reverse=True)
            if not sc:
                continue
            top1 = sc[0]
            top2 = sc[1][0] if len(sc) > 1 else 0.0
            if top1[0] >= 0.30 and (top1[0] - top2) >= 0.10:
                r3[i + 1] = top1[1] + 1          # 1-based page numbers
        r1 = {int(k): v for k, v in p["page_map_r1"].items()}
        r2 = {int(k): v for k, v in p["page_map_r2"].items()}
        def score(rule):
            ov = set(rule) & set(r3)
            return len(ov), sum(1 for x in ov if rule[x] == r3[x])
        c1, a1 = score(r1)
        c2, a2 = score(r2)
        rows.append({
            "doc": p["doc_id"], "disc": p["discipline"], "ver": f"{p['ver_a']}->{p['ver_b']}",
            "n_pages_a": p["n_pages_a"], "n_pages_b": p["n_pages_b"],
            "r3_confident": len(r3),
            "r1_cov": c1, "r1_ok": a1, "r2_cov": c2, "r2_ok": a2,
            "r3": {str(k): v for k, v in sorted(r3.items())},
        })
        F.clear_caches()
    S = {
        "n_pairs": len(rows),
        "r3_confident_total": sum(r.get("r3_confident", 0) for r in rows),
        "r1_cov": sum(r.get("r1_cov", 0) for r in rows), "r1_ok": sum(r.get("r1_ok", 0) for r in rows),
        "r2_cov": sum(r.get("r2_cov", 0) for r in rows), "r2_ok": sum(r.get("r2_ok", 0) for r in rows),
        "elapsed_s": round(time.time() - t0, 1),
    }
    S["r1_accuracy"] = round(S["r1_ok"] / max(1, S["r1_cov"]), 4)
    S["r2_accuracy"] = round(S["r2_ok"] / max(1, S["r2_cov"]), 4)
    out = {"schema_version": "mine_pagematch/1", "research_only": True, "summary": S, "rows": rows}
    (ART / "mine_pagematch.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(S, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
