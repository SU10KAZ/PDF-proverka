"""mine · step 4 — vector / text / raster properties of shortlisted pairs.

Everything is read through v03_foundation.extract_block (ink filter on, correct
derotation).  Used to label the classes the raster screen cannot see: dense_block,
no_labels / with_labels, different_packaging, raster_graphics, text_only_change.

argv: chunk_index n_chunks   (input artifacts/mine_shortlist.jsonl)
"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v03_foundation as F  # noqa

ART = Path(__file__).resolve().parents[1] / "artifacts"


def side(rec, s):
    ex = F.extract_block(rec["pdf_" + s], rec["page_index_" + s], rec["coords_" + s],
                         rec["page_px_" + s][0], rec["page_px_" + s][1])
    txt = [t.get("text", "") for t in ex.texts]
    return {
        "segments": len(ex.segments),
        "segments_raw": ex.segments_raw_count,
        "invisible_dropped": ex.invisible_dropped,
        "paths_total": ex.paths_total,
        "n_text_lines": len(ex.texts),
        "n_text_chars": sum(len(t) for t in txt),
        "text_join": " ".join(txt)[:3000],
        "n_images": len(ex.images),
        "quality": ex.quality,
    }


def main(argv):
    ci, nc = int(argv[0]), int(argv[1])
    rows = [json.loads(l) for l in open(ART / "mine_shortlist.jsonl", encoding="utf-8")]
    rows = [r for i, r in enumerate(rows) if i % nc == ci]
    (ART / "mine_extract_parts").mkdir(exist_ok=True)
    t0 = time.time()
    with open(ART / "mine_extract_parts" / f"{ci:02d}.jsonl", "w", encoding="utf-8") as out:
        for k, r in enumerate(rows):
            rec = dict(r)
            try:
                A = side(r, "a"); B = side(r, "b")
                ta = set(A["text_join"].split()); tb = set(B["text_join"].split())
                rec["EA"], rec["EB"] = A, B
                rec["text_jaccard"] = round(len(ta & tb) / max(1, len(ta | tb)), 4)
                rec["seg_ratio"] = round(min(A["segments"], B["segments"]) /
                                         max(1, max(A["segments"], B["segments"])), 4)
            except Exception as e:
                rec["extract_error"] = str(e)[:160]
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if k % 20 == 0:
                F.clear_caches()
    print(f"chunk {ci}: {len(rows)} rows {time.time()-t0:.1f}s", flush=True)


if __name__ == "__main__":
    main(sys.argv[1:])
