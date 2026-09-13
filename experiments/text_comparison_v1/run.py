"""Frozen document-pair offline execution; no page-pair data or production imports."""
from collections import Counter
from pathlib import Path
import argparse
import math
import resource
import time

from .common import canonical, digest, file_hash, read, section_text, write
from .sections import materialize
from .relations import relate, ai_package
from .facts import extract, compare
from .native import recover


def tokens(text):
    # An explicit model-neutral estimate, not billed/model-token telemetry.
    # Exact byte and character measurements are retained alongside it.
    return math.ceil(len(text.encode("utf-8")) / 4)


def prepare(baseline_documents, pair_root, output):
    docs = read(baseline_documents)
    by_path = {str(Path(d["artifacts"]["work_md"]["path"]).resolve()): d for d in docs}
    pairs = []
    for p in sorted(Path(pair_root).glob("*/pairs/*/pair.json")):
        pair = read(p)
        sides = []
        for side in ("left", "right"):
            d = by_path[str(Path(pair[side]["md_path"]).resolve())]
            for name in ("work_md", "blocks", "pdf"):
                a = d["artifacts"][name]
                if file_hash(a["path"]) != a["sha256"]:
                    raise ValueError(f"Frozen V002 input changed: {a['path']}")
            sides.append(d)
        pairs.append({"pair_key": "doc_pair_" + digest([sides[0]["document_version"], sides[1]["document_version"]])[:20],
                      "source_pair_id": pair["id"], "source_pair_path": str(p), "source_pair_sha256": file_hash(p),
                      "old": sides[0], "new": sides[1]})
    write(output, {"schema": "text-project-inputs.v1", "source_manifest": str(baseline_documents),
                   "source_manifest_sha256": file_hash(baseline_documents), "pairs": pairs,
                   "pair_selection": "All frozen V002 document pairs; no page pair results or truth labels read"})
    print({"pairs": len(pairs), "documents": len({d[s]["document_version"] for d in pairs for s in ("old", "new")})})


def run(inputs, output, *, materializer=materialize, relation_matcher=relate,
        extractor=extract, comparator=compare):
    output = Path(output)
    if output.exists():
        raise ValueError("Immutable run already exists")
    manifest = read(inputs)
    started = time.perf_counter()
    summaries, performances, all_changes = [], [], []
    for pair in manifest["pairs"]:
        tick = time.perf_counter()
        pk = pair["pair_key"]
        mats, facts, source_sizes = {}, {}, {}
        for side in ("old", "new"):
            d = pair[side]
            mat = materializer(d)
            mats[side] = mat
            facts[side] = {s["instance_id"]: extractor(s) for s in mat["sections"]}
            native_witnesses = recover(d, [f for e in facts[side].values() for f in e["facts"]])
            write(output / "documents" / d["document_version"] / "sections.json", mat)
            write(output / "documents" / d["document_version"] / "facts.json", facts[side])
            write(output / "documents" / d["document_version"] / "native_witnesses.json", native_witnesses)
            # Full-document baseline is narrative-only; tables/graphics are excluded
            # from both sides of the comparison, not used to inflate reduction.
            text = "\n".join(section_text(s) for s in mat["sections"])
            # Include explicit unowned narrative lines in full-document accounting.
            raw_lines = Path(d["artifacts"]["work_md"]["path"]).read_text().splitlines()
            orphan_text = "\n".join(raw_lines[r["source_ref"]["markdown_line"] - 1] for r in mat["ownership"]
                                    if r["route"] == "TEXT" and not (r["section_owner"] or r.get("candidate_owner")))
            text += "\n" + orphan_text
            source_sizes[side] = {"estimated_tokens": tokens(text), "characters": len(text), "utf8_bytes": len(text.encode())}
        qa, qb = mats["old"]["quality"], mats["new"]["quality"]
        complete = lambda q: q["review_narrative_lines"] == 0 and q["narrative_lines"] > 0
        relations = relation_matcher(mats["old"]["sections"], mats["new"]["sections"],
                           old_complete=complete(qa), new_complete=complete(qb))
        changes, local_budget, fallback = [], [], []
        for rel in relations:
            a = [s for s in mats["old"]["sections"] if s["instance_id"] in rel["old_sections"]]
            b = [s for s in mats["new"]["sections"] if s["instance_id"] in rel["new_sections"]]
            delta = comparator(rel, a, b, [facts["old"][s["instance_id"]] for s in a],
                            [facts["new"][s["instance_id"]] for s in b])
            for c in delta:
                c["pair_key"] = pk
                c["old_document_code"], c["new_document_code"] = pair["old"]["document_code"], pair["new"]["document_code"]
            changes.extend(delta)
            local_text = "\n".join(section_text(s) for s in a + b)
            if rel["status"] == "PROVEN" and a and b:
                local_budget.append({"relation_id": rel["relation_id"], "local_estimated_tokens": tokens(local_text),
                                     "local_characters": len(local_text), "local_utf8_bytes": len(local_text.encode()),
                                     "full_document_estimated_tokens": sum(v["estimated_tokens"] for v in source_sizes.values())})
            if rel["status"] == "REVIEW":
                package = ai_package(a, b)
                fallback.append({"relation_id": rel["relation_id"], "status": "NOT_CALLED",
                                 "reason": "OFFLINE_DETERMINISTIC_ONLY" if package else "LOCAL_PACKAGE_LIMIT_OR_CARDINALITY",
                                 "input_hash": digest(package) if package else None, "package": package})
        files = {"relations.json": relations, "changes.json": changes, "context.json": local_budget,
                 "ai_candidates.json": fallback}
        for filename, data in files.items():
            write(output / "pairs" / pk / filename, data)
        relation_counts = Counter(r["kind"] for r in relations)
        counts = {"matched": relation_counts["ONE_TO_ONE"], "split": relation_counts["ONE_TO_N"],
                  "merge": relation_counts["N_TO_ONE"],
                  "removed": sum(r["direction"] == "REMOVED" for r in relations),
                  "added": sum(r["direction"] == "ADDED" for r in relations), "review": relation_counts["REVIEW"]}
        row = {"pair_key": pk, "source_pair_id": pair["source_pair_id"],
               "old_document": pair["old"]["document_code"], "new_document": pair["new"]["document_code"],
               "old_version": pair["old"]["document_version"], "new_version": pair["new"]["document_version"],
               "sections_old": len(mats["old"]["sections"]), "sections_new": len(mats["new"]["sections"]),
               "relations": counts, "changes": dict(Counter(c["category"] for c in changes)),
               "fact_quality_old": dict(sum((Counter(e["quality"] | {"human_precision": 0, "human_recall": 0}) for e in facts["old"].values()), Counter())),
               "fact_quality_new": dict(sum((Counter(e["quality"] | {"human_precision": 0, "human_recall": 0}) for e in facts["new"].values()), Counter())),
               "quality_old": qa, "quality_new": qb, "context": {"full_document": source_sizes, "matched_local": local_budget}}
        # Human precision/recall are unavailable, never 0 from an aggregation trick.
        for side in ("old", "new"):
            row["fact_quality_" + side].update(human_precision=None, human_recall=None)
        summaries.append(row)
        all_changes.extend(changes)
        size = sum(p.stat().st_size for p in (output / "pairs" / pk).glob("*.json"))
        for side in ("old", "new"):
            size += sum(p.stat().st_size for p in (output / "documents" / pair[side]["document_version"]).glob("*.json"))
        performance = {"pair_key": pk, "seconds": time.perf_counter() - tick, "model_calls": 0,
                       "input_tokens": 0, "output_tokens": 0,
                       "process_peak_rss_kib_so_far": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                       "artifact_bytes": size}
        performances.append(performance)
        print({"pair": pair["source_pair_id"], "sections": [row["sections_old"], row["sections_new"]],
               "relations": counts, "changes": row["changes"], "seconds": round(performance["seconds"], 2)}, flush=True)
    write(output / "project.json", {"input_sha256": file_hash(inputs), "pairs": summaries,
                                   "changes": all_changes, "table_content_compared": False, "graphic_content_compared": False})
    write(output / "performance.json", {"pairs": performances, "seconds": time.perf_counter() - started,
          "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
          "model_calls": 0, "model_input_tokens": 0, "model_output_tokens": 0,
          "artifact_bytes": sum(p.stat().st_size for p in output.rglob("*.json"))})


def replay(a, b, output):
    a, b = Path(a), Path(b)
    aa = {str(p.relative_to(a)): file_hash(p) for p in a.rglob("*.json") if p.name != "performance.json"}
    bb = {str(p.relative_to(b)): file_hash(p) for p in b.rglob("*.json") if p.name != "performance.json"}
    differences = sorted(p for p in aa.keys() | bb.keys() if aa.get(p) != bb.get(p))
    result = {"pass": not differences, "files_compared": len(aa), "differences": differences,
              "sha256_by_path": aa, "excluded": ["performance.json: wall time and process RSS"],
              "ai_calls": 0, "ai_replay": "No AI stage executed; input packages preserved"}
    write(output, result)
    print({k: result[k] for k in ("pass", "files_compared", "differences")})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare")
    p.add_argument("--documents", required=True)
    p.add_argument("--pair-root", required=True)
    p.add_argument("--output", required=True)
    p = commands.add_parser("run")
    p.add_argument("--inputs", required=True)
    p.add_argument("--output", required=True)
    p = commands.add_parser("replay")
    p.add_argument("--left", required=True)
    p.add_argument("--right", required=True)
    p.add_argument("--output", required=True)
    a = parser.parse_args()
    if a.command == "prepare":
        prepare(a.documents, a.pair_root, a.output)
    elif a.command == "run":
        run(a.inputs, a.output)
    else:
        replay(a.left, a.right, a.output)
