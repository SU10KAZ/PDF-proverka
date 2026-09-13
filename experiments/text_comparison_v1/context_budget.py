"""Post-run exact tokenizer measurement, independent of frozen candidate scoring."""
from pathlib import Path
import argparse
import os
import statistics
import sys

from .common import canonical, file_hash, read, section_text, write


def measure(root):
    root = Path(root)
    sys.path.insert(0, str(root / "tokenizer_deps"))
    os.environ["TIKTOKEN_CACHE_DIR"] = str(root / "tokenizer_cache")
    import tiktoken
    encoder = tiktoken.get_encoding("o200k_base")
    count = lambda text: len(encoder.encode(text, disallowed_special=()))
    rows = []
    for pair in read(root / "FROZEN_PROJECT_INPUTS.json")["pairs"]:
        texts, sections, full = {}, {}, {}
        for side in ("old", "new"):
            doc = pair[side]
            mat = read(root / "run1/documents" / doc["document_version"] / "sections.json")
            sections[side] = {s["instance_id"]: s for s in mat["sections"]}
            raw = Path(doc["artifacts"]["work_md"]["path"]).read_text()
            lines = raw.splitlines()
            orphan = "\n".join(lines[r["source_ref"]["markdown_line"] - 1] for r in mat["ownership"]
                                if r["route"] == "TEXT" and not (r["section_owner"] or r.get("candidate_owner")))
            text = "\n".join(section_text(s) for s in mat["sections"]) + "\n" + orphan
            texts[side] = text
            full[side] = {"narrative_tokens": count(text), "narrative_utf8_bytes": len(text.encode()),
                          "raw_markdown_tokens_count_only": count(raw)}
        relations = read(root / "run1/pairs" / pair["pair_key"] / "relations.json")
        local = []
        for rel in relations:
            if rel["status"] != "PROVEN" or not rel["old_sections"] or not rel["new_sections"]:
                continue
            a = [sections["old"][i] for i in rel["old_sections"]]
            b = [sections["new"][i] for i in rel["new_sections"]]
            text = "\n".join(section_text(s) for s in a + b)
            body = {"old": [{"heading_path": s["heading_path"], "text": section_text(s),
                             "document_version": s["document_version"], "page_span": s["page_span"]} for s in a],
                    "new": [{"heading_path": s["heading_path"], "text": section_text(s),
                             "document_version": s["document_version"], "page_span": s["page_span"]} for s in b]}
            total = sum(x["narrative_tokens"] for x in full.values())
            local.append({"relation_id": rel["relation_id"], "narrative_present": bool(text.strip()),
                          "full_narrative_tokens": total, "local_body_tokens": count(text),
                          "local_with_titles_and_refs_tokens": count(canonical(body).decode()),
                          "body_reduction": 1 - count(text) / total if total else None,
                          "local_titles": [s["section_title"] for s in a + b]})
        rows.append({"pair_key": pair["pair_key"], "source_pair_id": pair["source_pair_id"], "full": full, "local": local})
    all_local = [x for r in rows for x in r["local"]]
    meaningful = [x for x in all_local if x["narrative_present"]]
    result = {"tokenizer": "tiktoken", "version": tiktoken.__version__, "encoding": "o200k_base",
              "vocabulary_cache": {p.name: file_hash(p) for p in sorted((root / "tokenizer_cache").glob("*")) if p.is_file()},
              "pairs": rows,
              "summary": {"full_narrative_once_tokens": sum(s["narrative_tokens"] for r in rows for s in r["full"].values()),
                          "raw_markdown_once_count_only_tokens": sum(s["raw_markdown_tokens_count_only"] for r in rows for s in r["full"].values()),
                          "matched_local_body_tokens": sum(x["local_body_tokens"] for x in all_local),
                          "meaningful_comparisons": len(meaningful), "empty_body_comparisons": len(all_local) - len(meaningful),
                          "full_repeated_for_meaningful_comparisons_tokens": sum(x["full_narrative_tokens"] for x in meaningful),
                          "local_for_meaningful_comparisons_tokens": sum(x["local_body_tokens"] for x in meaningful),
                          "median_full_per_meaningful_comparison": statistics.median(x["full_narrative_tokens"] for x in meaningful),
                          "median_local_per_meaningful_comparison": statistics.median(x["local_body_tokens"] for x in meaningful),
                          "median_body_reduction": statistics.median(x["body_reduction"] for x in meaningful),
                          "actual_model_tokens": 0},
              "caveat": "No AI was run. These are exact tokenizer counts of potential context, not cost savings or semantic-quality proof. Unmatched/review text is not included in local consumption; always report coverage."}
    write(root / "reports/CONTEXT_BUDGET_EXACT.json", result)
    print(result["summary"], flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    measure(p.parse_args().root)
