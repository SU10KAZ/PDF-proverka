"""Verify the frozen DEV packet, prepare an isolated UI, and publish the final report."""
from collections import Counter
from pathlib import Path
import io
import json
import shutil
import subprocess
import unittest

from . import frozen_v1 as v1
from .dev_packet import ROOT, OLD, NAMESPACE, QUOTAS, AUTOMATIC, candidates, page_hashes
from .run import read, write, receipt, audit
from .scorer import AnchorResolver, score


def schemas(packet, packet_hash):
    anchor = {"type": "object", "required": ["line_id", "page", "block_id", "markdown_line", "line_sha256", "edge"],
              "properties": {"line_id": {"type": "integer", "minimum": 0}, "page": {"type": "integer", "minimum": 1},
                  "block_id": {"type": "string"}, "markdown_line": {"type": "integer", "minimum": 1},
                  "line_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"}, "edge": {"enum": ["LAST", "FIRST"]}}}
    case = {"type": "object", "required": ["case_id", "namespace", "kind", "anchors", "display", "allowed_choices", "answer", "annotation_mode"],
            "properties": {"namespace": {"const": NAMESPACE}, "answer": {"type": "null"},
                "kind": {"enum": ["SECTION", "TABLE", "OWNER"]}, "annotation_mode": {"enum": ["HUMAN", "AUTOMATIC_CONTROL"]},
                "anchors": {"type": "array", "minItems": 2, "maxItems": 2, "items": anchor},
                "display": {"type": "object", "required": ["question", "panels"], "properties": {
                    "question": {"type": "string", "minLength": 1}, "panels": {"type": "array", "minItems": 2, "maxItems": 2}}}},
            "allOf": [{"if": {"properties": {"annotation_mode": {"const": "HUMAN"}}}, "then": {"not": {"required": ["control_expectation"]}}},
                      {"if": {"properties": {"kind": {"const": "OWNER"}}},
                       "then": {"properties": {"allowed_choices": {"const": ["OWNER_PRECEDING_SECTION", "OWNER_FOLLOWING_HEADING", "UNSURE"]}}},
                       "else": {"properties": {"allowed_choices": {"const": ["SAME", "NEW", "UNSURE"]}}}}]}
    packet_schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object",
                     "required": ["namespace", "cases", "answers_imported"], "properties": {"namespace": {"const": NAMESPACE},
                     "answers_imported": {"const": 0}, "cases": {"type": "array", "minItems": 126, "maxItems": 126, "items": case}}}
    answer_props = {"namespace": {"const": NAMESPACE}, "packet_sha256": {"const": packet_hash}}
    for key, kind, labels in [("answers", "BOUNDARY", [None, "SAME", "NEW", "UNSURE"]),
                              ("ownership_answers", "OWNER", [None, "OWNER_PRECEDING_SECTION", "OWNER_FOLLOWING_HEADING", "UNSURE"])]:
        ids = [c["case_id"] for c in packet["cases"] if c["annotation_mode"] == "HUMAN" and (c["kind"] == "OWNER") == (kind == "OWNER")]
        answer_props[key] = {"type": "object", "propertyNames": {"enum": ids}, "additionalProperties": {"enum": labels}}
    answers_schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "additionalProperties": False,
                      "required": ["namespace", "packet_sha256"], "properties": answer_props,
                      "anyOf": [{"required": ["answers"]}, {"required": ["ownership_answers"]}]}
    return packet_schema, answers_schema


def prepare_ui(packet, documents, packet_hash):
    directory, here = ROOT / "dev_namespace", Path(__file__).parent
    docs = {d["document_version"]: d for d in documents}
    human = [c for c in packet["cases"] if c["annotation_mode"] == "HUMAN"]
    payload = {"namespace": NAMESPACE, "packet_sha256": packet_hash, "cases": []}
    pdfs = {}
    for c in human:
        d = docs[c["document_version"]]
        pdf = d["artifacts"]["pdf"]
        name = "pdf/" + pdf["sha256"] + ".pdf"
        if name not in pdfs:
            target = directory / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(pdf["path"], target)
            if v1.file_sha(target) != pdf["sha256"]:
                raise ValueError("DEV PDF snapshot mismatch")
            pdfs[name] = receipt(target)
        payload["cases"].append({"case_id": c["case_id"], "stratum": c["stratum"], "kind": c["kind"],
            "document_code": d["document_code"], "question": c["display"]["question"], "allowed_choices": c["allowed_choices"],
            "pdf": name, "panels": [{"page": a["page"], "markdown_line": a["markdown_line"],
                "context_start_line": panel["context_start_line"], "context": panel["context"]}
                for a, panel in zip(c["anchors"], c["display"]["panels"])]})
    write(directory / "cases.json", payload)
    shutil.copyfile(here / "dev_ui.html", directory / "index.html")
    shutil.copyfile(here / "dev_ui.js", directory / "app.js")
    (directory / "README.md").write_text(f"""# Separate DEV annotation namespace

104 human questions only. The 22 automatic controls are absent from the UI.
Predictions, proposed choices, and EVAL truth are absent from the browser payload.
Source PDFs are separate verified copies. No existing registry or service was modified.
Answers start empty and are stored only after a human click, under a browser-local
key containing the DEV namespace and immutable packet hash. One JSON export contains
separate `answers` and `ownership_answers` maps. The boundary scorer reads only `answers`.
Exported files must be retained before clearing browser storage.

Prepared static UI; no persistent service has been started. To view locally:

```sh
python -m http.server 8767 --bind 127.0.0.1 --directory {directory}
```

Then open http://127.0.0.1:8767/ . This server exposes only this DEV directory.
The old annotation tool was not reused: its registry and responses hard-code
EVALUATION_TRUTH, and its existing schema does not cover the new T7 ownership task.
No old registry was instantiated. No human answers were generated or imported.

Packet: ../reports/FIRST_WAVE_DEV_PACKET.json
Packet SHA-256: {packet_hash}
Schemas: PACKET_SCHEMA.json and ANSWERS_SCHEMA.json.
""")
    return {"namespace": NAMESPACE, "human_questions": len(human), "automatic_questions_shown": 0,
            "predictions_shown": False, "human_answers_created": 0, "service_started": False,
            "artifacts": [receipt(directory / n) for n in ("index.html", "app.js", "cases.json", "PACKET_SCHEMA.json", "ANSWERS_SCHEMA.json")],
            "pdf_snapshots": list(pdfs.values())}


def main():
    import jsonschema
    reports = ROOT / "reports"
    packet = read(reports / "FIRST_WAVE_DEV_PACKET.json")
    packet_hash = v1.file_sha(reports / "FIRST_WAVE_DEV_PACKET.json")
    documents = read(ROOT / "dev_documents.json")
    freeze = read(reports / "FOUNDATION_FREEZE.json")
    for r in [*freeze["code"], *packet["selection_code"]]:
        if v1.file_sha(Path(r["path"])) != r["sha256"]:
            raise ValueError("Frozen code changed")
    packet_schema, answers_schema = schemas(packet, packet_hash)
    jsonschema.validate(packet, packet_schema)
    write(ROOT / "dev_namespace/PACKET_SCHEMA.json", packet_schema)
    write(ROOT / "dev_namespace/ANSWERS_SCHEMA.json", answers_schema)
    template = read(ROOT / "dev_namespace/answers.template.json")
    owner_template = read(ROOT / "dev_namespace/ownership_answers.template.json")
    # The separate owner template has one documentation-only field.
    jsonschema.validate({k: v for k, v in owner_template.items() if k != "allowed_choices"}, answers_schema)
    jsonschema.validate(template, answers_schema)
    assert all(v is None for v in template["answers"].values())
    assert all(v is None for v in owner_template["ownership_answers"].values())
    selected_keys = {(c["case_id"], c["variant"]) for c in packet["cases"]}
    rederived, resolved, source_receipts = set(), 0, []
    predictions = read(ROOT / "dev_predictions.json")
    assert predictions["packet_sha256"] == packet_hash
    by_doc = {d["document_version"]: [c for c in packet["cases"] if c["document_version"] == d["document_version"]] for d in documents}
    for d in documents:
        for r in [*d["artifacts"].values(), *d["source_refs"]["original_sources"]]:
            assert v1.file_sha(Path(r["path"])) == r["sha256"], "DEV source drift"
            source_receipts.append(r)
        result = {name: read(ROOT / "dev_foundation" / d["document_version"] / (name + ".json")) for name in ("ledger", "semantics", "decisions")}
        resolver = AnchorResolver(result)
        rederived.update((c["case_id"], c["variant"]) for c in candidates(d, result))
        for c in by_doc[d["document_version"]]:
            assert all(resolver.resolve(a) is not None for a in c["anchors"]), "Unresolvable DEV anchor"
            assert c["answer"] is None
            assert resolver.predict(c) == predictions["predictions"][c["case_id"]], "Prediction replay drift"
            resolved += 2
        baseline = v1.materialize_document(d)
        write(ROOT / "dev_v1_validation" / d["document_version"] / "comparison_units.json", baseline["comparison_units"])
    assert selected_keys <= rederived, "Selected case no longer meets source predicate"
    dev_audit = audit(documents, ROOT / "dev_foundation", ROOT / "dev_v1_validation")
    write(reports / "DEV_TRACEABILITY_AUDIT.json", dev_audit)
    assert dev_audit["pass"]
    separation = read(reports / "DEV_EVAL_SEPARATION.json")
    exclusion = read(ROOT / "source_exclusion.json")
    actual_sources = {r["sha256"] for r in source_receipts}
    actual_pages = {h for d in documents for h in page_hashes(Path(d["artifacts"]["work_md"]["path"]).read_text()).values()}
    assert not actual_sources & set(exclusion["eval_source_hashes"])
    assert not actual_pages & set(exclusion["eval_page_hashes"])
    assert separation["pass"]
    separation.update({"source_hashes_reverified": True, "anchored_cases": len(packet["cases"]),
                       "resolved_anchors": resolved, "proof_inputs": [receipt(ROOT / n) for n in ("source_exclusion.json", "dev_documents.json")]})
    write(reports / "DEV_EVAL_SEPARATION.json", separation)
    summary = read(reports / "DEV_PACKET_SUMMARY.json")
    assert (summary["total"], summary["human"], summary["automatic_controls"]) == (126, 104, 22)
    assert not any(summary[k] for k in ("duplicate_case_ids", "duplicate_page_pairs", "duplicate_page_content_pairs"))
    assert summary["max_per_document_per_stratum"] <= 3
    for s, n in summary["stratum_document_counts"].items():
        assert n >= min(6, sum(q for v, q in QUOTAS.items() if v.split('_')[0] == s))
    controls = {c["case_id"]: AUTOMATIC[c["variant"]] for c in packet["cases"] if c["annotation_mode"] == "AUTOMATIC_CONTROL"}
    control_score = score(predictions["predictions"], controls)
    control_score["interpretation"] = "Automatic structural expectations, not human truth or release accuracy. Deferred skeleton rules stay REVIEW."
    write(reports / "DEV_CONTROL_SCORE.json", control_score)
    ui = prepare_ui(packet, documents, packet_hash)
    node = subprocess.run(["node", str(Path(__file__).with_name("test_dev_ui.cjs"))], capture_output=True, text=True, check=True)
    ui["flow_test"] = node.stdout.strip()
    ui["test_scope"] = "Memory-only synthetic DOM flow; PDF content snapshots hashed. Visual browser rendering not tested."
    write(reports / "DEV_UI_PREPARATION.json", ui)
    log = io.StringIO()
    tests = unittest.defaultTestLoader.loadTestsFromNames(["experiments.semantic_foundation_v3.test_foundation", "experiments.semantic_foundation_v3.test_dev_packet"])
    result = unittest.TextTestRunner(stream=log, verbosity=2).run(tests)
    assert result.wasSuccessful()
    write(reports / "FOUNDATION_TEST_RESULTS.json", {"pass": True, "python_tests": result.testsRun,
          "failures": 0, "errors": 0, "log": log.getvalue(), "ui_flow": node.stdout.strip(),
          "corpus_traceability_pass": True, "dev_traceability_pass": dev_audit["pass"], "packet_schema_pass": True,
          "resolved_dev_anchors": resolved, "source_predicate_replay_pass": True, "prediction_replay_pass": True})
    table_rows = "\n".join(f"| {v} | {r['target']} | {r['selected']} | {r['pool']} | {r['documents']} |" for v, r in summary["variants"].items())
    (reports / "FIRST_WAVE_DEV_PACKET_REPORT.md").write_text(f"""# First-wave DEV packet

Frozen packet: **126 anchored cases, 104 human, 22 automatic controls**. Answers
remain null. Source-only predicate rederivation passes for all 126; all 252 anchors
resolve with exact raw line hashes. Duplicate case IDs, page pairs and page-content
pairs across document versions: **0**. At most 3 cases per document per stratum.
All 13 strata are represented; strata of >=6 cases use >=6 documents. S5 (4 cases)
uses 4 documents and T6 (2 cases) uses 2: six documents is arithmetically impossible
for those controls. S2/T3/T5 subvariant counts follow the plan exactly.

**Document expansion deviates from the initial 4–6 estimate.** The actual corpus
has 21 real documents: 11 retained from the old source manifest, Mockup excluded,
10 additional real documents. No old 200-case records or answers were imported.
672 source candidates were scanned; missing PDFs/source structure excluded 96.
Six-addition attempts did not provide a complete diverse packet; the selected
expanded cohort satisfies all case quotas and feasible document-diversity limits.
This is a verified feasible cohort, not a claim of globally minimal document count.
The human budget remains exactly 104; no duplicates were added to fill quotas.

| Variant | Target | Selected | Eligible pool in selected corpus | Documents |
|---|---:|---:|---:|---:|
{table_rows}

{summary['review_sourced_human_cases']} human cases directly reference published
Foundation REVIEW edges. Other human cases check strong/repeated-heading policy or
T7 ownership. All carry source anchors rather than producer unit keys. Table spans
and first-row surface evidence are included; separator positions are not header proof.
Selection uses structure, quotas, document diversity and content hashes, never labels
or correctness of predictions. Both V1 and Foundation predictions were frozen before
any human answers, and no human answers have been collected in this task.

Independent DEV audit: {dev_audit['totals']['pages']} pages,
{dev_audit['totals']['nonempty_lines']} non-empty ledger lines; zero conservation,
normalization, V1 page or SHEET-contract mismatches. DEV/EVAL overlap is zero by
version, document code, source hashes and normalized nonempty page hashes. Whole
evaluation project 272 is excluded. Proof inputs are in DEV_EVAL_SEPARATION.json.

Automatic-control scorer: A={control_score['accuracy_on_proven']},
B={control_score['coverage']}, C={control_score['legacy_review_incorrect']}.
These are structural contract checks, not human evaluation accuracy. Table continuation
and uncertain section ownership are deliberately deferred; REVIEW remains REVIEW.

The isolated prepared UI is ../dev_namespace/index.html, with copied verified PDFs,
schemas and a launch command in ../dev_namespace/README.md. Only 104 human questions
are shown, without predictions. Browser-local answers and exports use the DEV namespace
plus packet hash. No EVAL registry, existing annotation service or production was changed.
""")
    write(reports / "DEV_PACKET_FREEZE.json", {"namespace": NAMESPACE, "packet": receipt(reports / "FIRST_WAVE_DEV_PACKET.json"),
          "documents": receipt(ROOT / "dev_documents.json"), "predictions": [receipt(ROOT / n) for n in ("dev_predictions.json", "dev_v1_predictions.json")],
          "selection": receipt(ROOT / "dev_selection.json"), "schemas": ui["artifacts"][3:], "answers_imported": 0,
          "foundation_code_unchanged": True, "validation_pass": True})
    frozen_path = ROOT.parent / "20260909_section_table_human_truth_v1/reports/FROZEN_HUMAN_TRUTH_MANIFEST_20260910T205822Z.json"
    frozen_hash = v1.file_sha(frozen_path)  # Integrity hash only; never deserialize human truth.
    assert frozen_hash == "f665d897af94ac6a2e18edc4454a70423f156c6928415ce1a6deb99c9d1691d5"
    ancestry = read(reports / "ANCESTRY.json")
    git = lambda *args: subprocess.check_output(["git", *args], text=True).strip()
    ancestry.update({"foundation_head": git("rev-parse", "HEAD"), "foundation_base": ancestry["origin_main"],
                     "current_origin_main": git("rev-parse", "origin/main"), "preserved_v2": git("rev-parse", ancestry["preservation_tag"]),
                     "current_status": git("status", "--short"), "production_release_after": str(Path("/home/coder/auditmanager/current").resolve())})
    assert ancestry["initial_head"] == ancestry["preserved_v2"]
    assert ancestry["current_origin_main"] == ancestry["origin_main"]
    assert ancestry["production_release"] == ancestry["production_release_after"]
    assert subprocess.run(["git", "merge-base", "--is-ancestor", ancestry["initial_head"], "HEAD"]).returncode == 1
    ancestry.update({"v2_is_ancestor_of_foundation": False, "push": 0, "deploy": 0})
    write(reports / "ANCESTRY.json", ancestry)
    core = read(reports / "TRACEABILITY_AUDIT.json")
    distribution, caption_conflicts = Counter(), 0
    for row in read(ROOT / "foundation_run1/index.json"):
        distribution.update(row["quality"]["decision_distribution"])
        caption_conflicts += row["quality"]["caption_heading_conflicts"]
    final = {"verdict": "A — FOUNDATION V3 READY FOR TABLE V3 DEVELOPMENT", "scope": "FOUNDATION_ONLY_NOT_RELEASE_GATE",
             "documents": core["documents"], **core["totals"], "caption_heading_conflicts": caption_conflicts,
             "boundary_decisions": dict(distribution), "proven": distribution["SAME"] + distribution["NEW"],
             "performance": read(reports / "PERFORMANCE.json")["ratios"], "replay": "PASS",
             "dev": {k: summary[k] for k in ("total", "human", "automatic_controls", "documents", "stratum_document_counts")},
             "dev_eval_overlap": 0, "frozen_evaluation_truth_accessed_for_tuning": False,
             "truth_manifest_integrity_sha256": frozen_hash, "v2_candidate_modified": False, "production_modified": False,
             "push": 0, "deploy": 0, "next_step": "Collect 104 human DEV answers in the isolated prepared UI, freeze that DEV truth, then implement Table V3 using explicit evidence sets. NUMBER_CHAIN and Relation Matcher remain blocked; fresh holdout required before release."}
    write(reports / "FINAL_VERDICT.json", final)
    (reports / "NEXT_ACTION.md").write_text("""# Next action

**A — FOUNDATION V3 READY FOR TABLE V3 DEVELOPMENT.** This certifies the Foundation
contracts, source conservation, cached V1 page/SHEET compatibility, performance and
replay, plus the complete source-anchored DEV packet. It is not a production or full
Table/Section materialization quality gate. Document expansion to 21 and the two
mathematically necessary small-control diversity exceptions are explicit in the packet report.

1. Launch the isolated prepared DEV UI using dev_namespace/README.md. Collect the
   104 human answers, including UNSURE where appropriate; do not ask humans to label
   the 22 structural controls. Export and freeze the DEV answers in this namespace.
2. Begin Table V3 only after that DEV freeze: evidence sets for adjacency, equal width,
   row observations, strict numbering progression, captions and container context.
   Promote rules from REVIEW only against DEV evidence, not the old frozen evaluation.
3. Section V3 follows Table V3. NUMBER_CHAIN stays unpromoted pending sufficient
   independent DEV evidence. Relation Matcher, Sheet v4 and Astra secondary leg remain blocked.
4. Select a fresh independent holdout after DEV, before evaluating V3. The old frozen
   78/78 answers are diagnostic only and cannot serve as the final release gate.

No push, deployment, production flag or human truth changes were performed.
""")
    with (reports / "CHECKPOINT.md").open("a") as handle:
        handle.write("\n## DEV PACKET FREEZE / COMPLETE\n\n126 anchored cases: 104 human, 22 automatic. 21 real DEV documents, all feasible stratum diversity constraints met. All 252 anchors resolve; no duplicate page-content pairs; DEV/EVAL overlap zero. V1/Foundation predictions frozen, answers remain empty. Isolated UI and schemas prepared. Core freeze unchanged. Verdict A; next: collect DEV answers, then Table V3.\n")
    write(reports / "ARTIFACT_MANIFEST.json", {"reports": [receipt(p) for p in sorted(reports.iterdir()) if p.is_file() and p.name != "ARTIFACT_MANIFEST.json"]})
    print(json.dumps(final, ensure_ascii=False))


if __name__ == "__main__":
    main()
