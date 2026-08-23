"""mine · step 4b — second batch for the vector/text/raster pass.

The first shortlist was change-driven and therefore blind to classes that live in
QUIET pairs (different_packaging, no_labels, raster_graphics).  This adds:
  * every pair with diff_frac_block < 0.0005 at equal scale,
  * the 150 pairs with the least OCR text,
  * a seeded random sample of 250 pairs,
minus whatever mine_extract.jsonl already covers.  Output: mine_shortlist.jsonl
(the input file of mine_extract.py).
"""
import hashlib, json, random
from pathlib import Path

ART = Path(__file__).resolve().parents[1] / "artifacts"
SEED = 7


def pid(r):
    h = hashlib.sha1(f"{r['doc_id']}|{r['ver_a']}|{r['ver_b']}|{r['block_a']}|{r['block_b']}".encode()).hexdigest()[:8]
    return f"{r['discipline']}-{h}"


def main():
    rows = [json.loads(l) for l in open(ART / "mine_align2.jsonl", encoding="utf-8")]
    rows = [r for r in rows if "align2" in r and not r.get("same_pdf")]
    for r in rows:
        r["pair_id"] = pid(r)
    have = set()
    p = ART / "mine_extract.jsonl"
    if p.exists():
        have = {json.loads(l)["pair_id"] for l in open(p, encoding="utf-8")}
    quiet = [r for r in rows if r["align2"]["diff_frac_block"] < 0.0005]
    lowocr = sorted(rows, key=lambda r: r["ocr_len_a"] + r["ocr_len_b"])[:150]
    rnd = random.Random(SEED).sample(rows, 250)
    sel = {}
    for r in quiet + lowocr + rnd:
        if r["pair_id"] in have:
            continue
        sel[r["pair_id"]] = r
    with open(ART / "mine_shortlist.jsonl", "w", encoding="utf-8") as fh:
        for r in sel.values():
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("extra pairs to extract:", len(sel))


if __name__ == "__main__":
    main()
