"""Freeze the tested Foundation before DEV packet selection. Never reads human answers."""
from collections import Counter
from pathlib import Path
import io
import statistics
import subprocess
import unittest

from . import frozen_v1 as v1
from .run import read, write, receipt
from .ledger import PRODUCER_VERSION

ROOT = Path("/home/coder/auditmanager/corpus-audits/20260911_semantic_foundation_v3")
OLD = ROOT.parent / "20260910_section_table_materialization_v2"

CONTRACTS = {
    "FOUNDATION_V3_ARCHITECTURE.md": """# Foundation V3 architecture

Ledger-First + V3-Gap discipline. Offline only, no AI/network/model calls.
Order: LineLedger → cached PageModel → Heading evidence → FurnitureModel →
one positional CaptionModel → thin ownership assemblers → BoundaryDecision.
Foundation imports unchanged V1 normalization, parsing, classifier and SHEET
identity primitives. It never invokes V1's section/table materialization pipeline.
Page evidence construction is factored from V1 to reuse facts exactly once per page;
complete page evidence and SHEET artifacts are checked against untouched V1.

All semantic references are document-local ledger indices. Normalized text exists
once in ledger columns, raw text once in the immutable source. Non-semantic metadata
and source blocks are retained by hashed source reference. A source block with no
Markdown content has an explicit registry record. There are no empty-line units.

Section fragments and table segments demonstrate total ownership, not full V3
materialization. Tables are never joined in Foundation; all consecutive segment
edges publish REVIEW with observations. Number-chain and repeated-heading rules
are not promoted. Relation Matcher remains blocked. Production modules/flags,
Sheet Matcher/v4, Astra, remote refs, and deployment are outside this experiment.

Existing frozen human truth is diagnostic only. Its individual answers were not
opened. Audit aggregates are architecture input; future release needs a fresh
independent holdout selected after DEV and before evaluation.
""",
    "LINE_LEDGER_CONTRACT.md": """# LineLedger contract

One ledger per document. Only non-empty exported block-content lines are atoms.
`line_id = ordinal = zero-based column index`, stable for the same input and
producer. The enclosing ledger provides document_version and producer_version.
`block_ref` resolves page, source block id, type, header line, and raw block indices.
`markdown_line` (one based) and `within_block_line` resolve the exact source line.
Raw text ref is (sources.work_md, markdown_line); the pinned source SHA protects it.
`normalized_text`, `kind`, `owner`, `evidence_codes` are stored once per line.
External DEV anchors additionally hash the exact raw line and include its locator.

Owners are local unit indices or explicit FURNITURE / EXCLUDED / REVIEW / UNOWNED
states. UNOWNED is an assembly-only initial state and must be absent from output.
Source lines in REVIEW fragments have a provisional owner, not a proven section.
Units contain anchors and metadata, never duplicate ownership lists or cell refs.
Stamp metadata / created and crop metadata / block headers are source envelope
metadata, not content atoms. Empty parsed content lines are counted per block.
Raw-only image and other source blocks remain registered as NO_MARKDOWN_CONTENT.
""",
    "OWNERSHIP_INVARIANTS.md": """# Ownership invariants

Every non-empty source atom appears exactly once; every source block is represented.
claim() rejects second assignment, including assignments to exclusion states.
Every owner resolves to exactly one unit, or to an explicit state with evidence.
Furniture has no semantic owner. SHEET content routes to frozen V1 sheet identity;
front matter and unknown pages are explicit exclusions and reset section scope.
REVIEW boundaries start separate provisional fragments; REVIEW never licenses a
join. Tables remain individual segments. Captions never independently open sections.

Independent verification scans original Markdown and raw block indices, then checks
persisted artifacts, every normalization, all V1 page properties and SHEET output.
This proves conservation and routing, not engineering correctness of the skeletons.
""",
    "FURNITURE_MODEL.md": """# FurnitureModel

Centralized evidence: stamp/title_block source type; isolated numeric first/last
content line (1–4 digits); repeated edge text using V1's first/last four lines and
max(3, floor(page_count * 0.15)) distinct-page threshold. Duplicate edge occurrences
on short pages do not inflate the distinct-page count. Table rows and numbered
heading candidates are protected from repeated-text suppression. Unnumbered running
headings may be furniture; their heading evidence is retained.

Furniture remains in the ledger, with FURNITURE state and reason codes. It is
transparent for local caption distance and table adjacency observations. A change
to this policy requires a new producer freeze and DEV validation. No evidence is deleted.
""",
    "CAPTION_HEADING_CONTRACT.md": """# Caption / Heading contract

Heading evidence is collected before caption classification. Strong forms are V1
Markdown headings and explicitly bold numbered headings. Plain NUMBER_CHAIN,
ALL-CAPS and unnumbered bold are candidates; they produce REVIEW, never a promoted
heading rule. A currently open repeated heading is a candidate SAME and remains
REVIEW in this Foundation skeleton. Page change alone never supplies NEW evidence.

Exactly one caption regex and one decision point: table-like lexical prefix plus
table start in the next <=3 meaningful same-page lines, counting the table start
as distance one. Blank/furniture lines do not count. Another strong heading stops
the search. With no nearby table, lexical wording alone cannot make a caption.
Plain captions belong to the following table and cannot open a section.
Strong heading plus positional caption supplies both JOIN and SPLIT evidence:
REVIEW(CONFLICT), retaining heading evidence and a provisional section fragment.
No automatic transfer of such a Markdown heading to a table is permitted.

FirstRowKind is TEXTUAL / MULTI_ROW / DATA_LIKE / NONE / UNKNOWN. It describes
surface structure only. TEXTUAL does not prove a header; MULTI_ROW requires explicit
rowspan/colspan markup; DATA_LIKE records numeric/empty leading cell evidence.
Separator offsets are retained as exporter structure, never semantic header proof.
""",
    "BOUNDARY_DECISION_CONTRACT.md": """# BoundaryDecision

Fields: kind, left_anchor, right_anchor, decision SAME|NEW|REVIEW, basis
PROVEN|DEFAULT, evidence_codes, conflict; plus explicit join_evidence and
split_evidence sets (serialized as sorted tuples/lists). Anchors are ledger indices;
left may be null only at the start of a source scope (not an annotatable pair).

Collect all evidence first. Only JOIN → SAME/PROVEN; only SPLIT → NEW/PROVEN;
both → REVIEW/DEFAULT with CONFLICT; neither → REVIEW/DEFAULT with NO_EVIDENCE.
Observations do not automatically become proof. Permuting or duplicating input
evidence cannot change the result. No first-matching-regex decision chain exists.

REVIEW is fail-closed: keep separate provisional ownership and publish the edge.
The scorer cannot infer SAME merely from total provisional ownership. Table
observations include adjacency, width, repeated first row, caption and first-row
kind; promoting those observations into continuation rules is deferred to Table V3.
""",
    "SCORER_V3_CONTRACT.md": """# Scorer V3

For scored labels SAME/NEW, A = correct PROVEN / all PROVEN; B = all PROVEN /
all scored labels; C = all correct decisions / all scored labels, with REVIEW
always incorrect. Empty denominators yield null, not fabricated zero accuracy.
Unanswered and UNSURE are reported separately and excluded from scored labels.
Always report label counts, REVIEW distribution and majority-class share.

Resolve source anchors against pinned Markdown and exact line hashes, not producer
unit keys. Invalid/version-mismatched/type-mismatched anchors return REVIEW.
A path through a REVIEW edge stays REVIEW. A provisional fragment is not evidence
of SAME. Predictions are written with exclusive creation and a SHA receipt before
a separate scorer invocation reads answers. Namespace and packet hashes must match.
Foundation CLI accepts only SEMANTIC_FOUNDATION_V3_DEV; it has no frozen-EVAL input
path or imports. Ownership questions (T7) are separate and not scored as boundaries.
Automatic controls are contract expectations, not human annotations or holdout truth.
""",
}


def main():
    reports = ROOT / "reports"
    if (reports / "FOUNDATION_FREEZE.json").exists():
        raise ValueError("Foundation already frozen")
    tests = unittest.defaultTestLoader.loadTestsFromName("experiments.semantic_foundation_v3.test_foundation")
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=2).run(tests)
    write(reports / "FOUNDATION_TEST_RESULTS.json", {"pass": result.wasSuccessful(), "tests": result.testsRun,
          "failures": len(result.failures), "errors": len(result.errors), "log": stream.getvalue(),
          "traceability": read(reports / "TRACEABILITY_AUDIT.json")["pass"], "replay": read(reports / "REPLAY_DETERMINISM.json")["pass"]})
    if not result.wasSuccessful() or not read(reports / "TRACEABILITY_AUDIT.json")["pass"] or not read(reports / "REPLAY_DETERMINISM.json")["pass"]:
        raise ValueError("Foundation checks failed")
    for name, contents in CONTRACTS.items():
        (reports / name).write_text(contents)
    runs = {p: [read(ROOT / f"{p}_run{n}" / "performance.json") for n in (1, 2)] for p in ("v1", "foundation")}
    perf = {p: {"seconds_median": statistics.median(r["seconds"] for r in rows),
                "peak_rss_kib_median": statistics.median(r["peak_rss_kib"] for r in rows),
                "artifact_bytes": rows[0]["artifact_bytes"]} for p, rows in runs.items()}
    perf["historical_v2_artifact_bytes"] = sum(p.stat().st_size for p in (OLD / "v2_run1").glob("*/*_v1.json"))
    f, b = perf["foundation"], perf["v1"]
    perf["ratios"] = {"runtime_vs_v1": f["seconds_median"] / b["seconds_median"],
                      "rss_vs_v1": f["peak_rss_kib_median"] / b["peak_rss_kib_median"],
                      "bytes_vs_v1": f["artifact_bytes"] / b["artifact_bytes"],
                      "bytes_vs_v2": f["artifact_bytes"] / perf["historical_v2_artifact_bytes"]}
    perf["raw_runs"] = runs
    write(reports / "PERFORMANCE.json", perf)
    (reports / "PERFORMANCE_REPORT.md").write_text(f"""# Performance report

52 documents / 3,042 pages, two fresh processes per producer, sequential execution
(V1, Foundation, Foundation, V1). Same input manifest, SHA validation, canonical
JSON serializer, disk writes and artifact hashing. Median wall time and median
process peak RSS; OS cache is warm. Semantic artifacts only in size totals.

| Metric | V1 | Foundation | Ratio |
|---|---:|---:|---:|
| Runtime, seconds | {b['seconds_median']:.3f} | {f['seconds_median']:.3f} | {perf['ratios']['runtime_vs_v1']:.3f} |
| RSS, KiB | {b['peak_rss_kib_median']:.0f} | {f['peak_rss_kib_median']:.0f} | {perf['ratios']['rss_vs_v1']:.3f} |
| Artifact bytes | {b['artifact_bytes']} | {f['artifact_bytes']} | {perf['ratios']['bytes_vs_v1']:.3f} |

Historical frozen V2 files: {perf['historical_v2_artifact_bytes']} bytes;
Foundation/V2 = {perf['ratios']['bytes_vs_v2']:.3f}. V2 was not rerun or modified.
Foundation is a skeleton and deliberately omits full table row/cell materialization;
these numbers validate Foundation costs, not eventual full Table/Section V3 costs.
Page facts/front-matter are computed once per page: 3,042, not 9,022. Source
normalization once per non-empty atom: 77,705. SHEET identity retains V1 primitives.
""")
    code = [receipt(p) for p in sorted(Path(__file__).parent.glob("*.py"))]
    freeze = {"producer_version": PRODUCER_VERSION, "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "code": code, "documents": receipt(OLD / "baseline_documents.json"),
              "truth_answers_read": False, "ready_for_dev_selection": True,
              "architecture_scope": "FOUNDATION_ONLY", "performance": perf["ratios"]}
    write(reports / "FOUNDATION_FREEZE.json", freeze)
    with (reports / "CHECKPOINT.md").open("a") as handle:
        handle.write("\n## PERFORMANCE / FOUNDATION FREEZE\n\nIndependent persisted-source audit and full V1 page/SHEET equality passed. 156/156 semantic artifacts replay identically. Two-process median timings and RSS recorded. Foundation code frozen before DEV case selection.\n")
    print({"freeze": str(reports / "FOUNDATION_FREEZE.json"), "performance": perf["ratios"]})


if __name__ == "__main__":
    main()
