"""Post-freeze blind, source-selected SectionRelation holdout preparation only.

Selection never imports the candidate section materializer or relation matcher.
Foundation source headings anchor local windows. Missing/ambiguous candidates
stay in the packet; no predicted-correctness filter exists.
"""
from collections import defaultdict
from pathlib import Path
import argparse
import datetime
import re

from experiments.semantic_foundation_v3.dev_packet import prepare_source, page_hashes
from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.page_model import PageModel
from experiments.semantic_foundation_v3.models import HeadingModel

from .common import digest, file_hash, read, write, similarity

SEED = "fresh-section-relation-after-freeze-20260913-v1"


def build(root, repo, dev_documents, baseline_documents, pair_count=4, cases_per_pair=4):
    root, repo = Path(root), Path(repo)
    freeze = read(root / "CANDIDATE_FREEZE.json")
    for p, expected in freeze["files"].items():
        if file_hash(repo / p) != expected:
            raise ValueError("Candidate changed after freeze")
    output = root / "holdout"
    if output.exists():
        raise ValueError("Holdout already frozen")
    excluded = read(dev_documents) + read(baseline_documents)
    excluded_codes = {d["document_code"] for d in excluded}
    excluded_hashes = {a["sha256"] for d in excluded for a in d["artifacts"].values() if isinstance(a, dict) and "sha256" in a}
    excluded_pages = {h for d in excluded for h in page_hashes(Path(d["artifacts"]["work_md"]["path"]).read_text()).values()}
    grouped = defaultdict(list)
    for p in repo.glob("projects_v2/objects/*/disciplines/*/documents/*/versions/*/02_work/document.md"):
        if p.parents[3].name not in excluded_codes and "/272_" not in str(p):
            grouped[str(p.parents[2])].append(p)
    universe, rejected = [], defaultdict(int)
    for family, paths in sorted(grouped.items()):
        paths = sorted(paths)
        if len(paths) < 2:
            continue
        a, b = paths[0], paths[-1]
        if not all(p.with_name("document.pdf").exists() and
                   (p.with_name("blocks.json").exists() or p.with_name("result.json").exists()) for p in (a, b)):
            rejected["missing_source"] += 1
            continue
        hashes = [file_hash(p) for p in (a, b)]
        if hashes[0] == hashes[1] or set(hashes) & excluded_hashes:
            rejected["identical_or_seen_source"] += 1
            continue
        texts = [p.read_text(errors="replace") for p in (a, b)]
        source_headings = [[line for line in t.splitlines() if re.match(r"^#{4,6}\s+", line)] for t in texts]
        # Source-only narrative support; exclude tables, images, giant source
        # documents from a small local human packet. These are fixed size quotas.
        if any(len(h) < cases_per_pair for h in source_headings) or any(len(t) > 800000 for t in texts):
            rejected["source_heading_or_size_quota"] += 1
            continue
        if any(set(page_hashes(t).values()) & excluded_pages for t in texts):
            rejected["seen_source_page"] += 1
            continue
        universe.append({"family": family, "old_path": str(a), "new_path": str(b), "md_hashes": hashes,
                         "rank": digest([SEED, family, hashes])})
    chosen, packets, selected_pairs = [], [], []
    for candidate in sorted(universe, key=lambda x: x["rank"]):
        # PDF duplicates checked before accepting; no semantic output inspected.
        if any(file_hash(Path(candidate[k]).with_name("document.pdf")) in excluded_hashes for k in ("old_path", "new_path")):
            rejected["seen_pdf"] += 1
            continue
        docs = [prepare_source(candidate[k], output / "sources") for k in ("old_path", "new_path")]
        models = []
        for d in docs:
            d["artifacts"]["pdf"] = {"path": d["source_refs"]["source_pdf"], "sha256": file_hash(d["source_refs"]["source_pdf"])}
            ledger, raw = LineLedger.read(d)
            page_model = PageModel(ledger, raw)
            heading_model = HeadingModel(ledger, page_model)
            ids = [i for i, h in heading_model.records.items() if h["tier"] == "STRONG"
                   and page_model.eligible(ledger.lines[i].page) and ledger.lines[i].block_type == "text"]
            models.append((ledger, page_model, ids))
        # No fall-through based on candidate output: Foundation eligible heading
        # inventory is a source routing criterion, recorded for every rejection.
        if any(len(m[2]) < cases_per_pair for m in models):
            rejected["foundation_source_route_quota"] += 1
            continue
        pair_id = "fresh_pair_" + digest(candidate)[0:20]
        old_ledger, old_pages, old_ids = models[0]
        new_ledger, new_pages, new_ids = models[1]

        def window(ledger, pages, ids, i):
            after = next((j for j in ids if j > i), len(ledger.lines))
            lines = []
            for j in range(i, min(after, i + 90)):
                line = ledger.lines[j]
                if line.block_type != "text" or not pages.eligible(line.page) or line.text.count("|") >= 2:
                    continue
                lines.append({"text": line.text, "source_ref": ledger.anchor(j, "LINE")})
            return {"document_version": ledger.document["document_version"], "title": ledger.clean[i],
                    "anchor": ledger.anchor(i, "FIRST"), "source_lines": lines,
                    "window_truncated": after > i + 90, "scope": "BLIND_SOURCE_WINDOW_NOT_CANDIDATE_PREDICTION"}

        selected = sorted(old_ids, key=lambda i: digest([SEED, candidate["md_hashes"][0], old_ledger.anchor(i, "FIRST")]))[:cases_per_pair]
        for i in selected:
            title = old_ledger.clean[i]
            candidates = sorted(new_ids, key=lambda j: (-similarity(title, new_ledger.clean[j]),
                                digest([SEED, new_ledger.anchor(j, "FIRST")])) )[:3]
            packet = {"case_id": "fresh_relation_" + digest([pair_id, old_ledger.anchor(i, "FIRST")])[:20],
                      "pair_id": pair_id, "old": window(old_ledger, old_pages, old_ids, i),
                      "new_candidates": [window(new_ledger, new_pages, new_ids, j) for j in candidates],
                      "answer": None, "human_annotation_requested": False,
                      "allowed_answers": ["SAME_SECTION", "SPLIT", "MERGE", "RELATED_BUT_DIFFERENT", "NO_RELATION", "UNSURE"],
                      "allow_missing_candidate_or_additional_old_sections": True}
            packets.append(packet)
        chosen.append(candidate)
        selected_pairs.append({"pair_id": pair_id, "old": docs[0], "new": docs[1]})
        if len(chosen) == pair_count:
            break
    if not packets:
        raise ValueError("No independent source cohort available")
    manifest = {"schema": "fresh-section-relation-holdout.v1", "selected_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "candidate_freeze_sha256": file_hash(root / "CANDIDATE_FREEZE.json"), "candidate_commit": freeze["commit"],
                "seed": SEED, "N": len(packets), "document_pairs": len(chosen), "selected_sources": chosen,
                "source_universe_count": len(universe), "source_universe_sha256": digest(universe),
                "rejections": dict(rejected), "dev_document_overlap": 0, "old_evaluation_document_overlap": 0,
                "dev_or_eval_page_hash_overlap": 0, "candidate_correctness_used": False,
                "candidate_predictions_used": False, "human_annotations": 0, "human_requested": False,
                "limitations": "Source windows are blind; human may add omitted candidates/OLD siblings. No labelled accuracy or guaranteed split/merge quota.",
                "cases": [{"case_id": p["case_id"], "pair_id": p["pair_id"], "old_anchor": p["old"]["anchor"]} for p in packets]}
    write(output / "source_universe.json", universe)
    write(output / "pairs.json", selected_pairs)
    write(output / "BLIND_SECTION_RELATION_PACKET.json", {"instructions_ru": "Определить связи по исходному тексту; можно добавить пропущенные разделы. Ответов и прогнозов алгоритма нет.", "cases": packets})
    write(root / "reports/FRESH_SECTION_HOLDOUT_SELECTION.json", manifest)
    print({"N": len(packets), "pairs": len(chosen), "universe": len(universe), "rejections": dict(rejected)}, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--repo", required=True)
    p.add_argument("--dev-documents", required=True)
    p.add_argument("--baseline-documents", required=True)
    a = p.parse_args()
    build(a.root, a.repo, a.dev_documents, a.baseline_documents)
