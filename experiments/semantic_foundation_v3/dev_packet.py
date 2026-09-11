"""Post-freeze structural DEV selection. No imports of V2, truth cases, or answers."""
from collections import Counter, defaultdict
from pathlib import Path
import hashlib
import json
import re

from . import frozen_v1 as v1
from .freeze import ROOT, OLD
from .ledger import LineLedger
from .materialize import materialize_document
from .models import NUMBER_CHAIN
from .run import read, write, receipt
from .scorer import AnchorResolver, freeze_predictions

NAMESPACE = "SEMANTIC_FOUNDATION_V3_DEV"
QUOTAS = {"S1": 16, "S2_NUMBER": 12, "S2_CAPS": 8, "S2_BOLD": 4, "S3": 12,
          "S4_PLAIN": 4, "S4_FURNITURE": 4, "S5": 4, "T1": 8, "T1b": 8,
          "T2_CROSS": 6, "T2_WITHIN": 2, "T3_BOTH": 6, "T3_ONE": 6,
          "T4": 6, "T5_CROSS": 6, "T5_WITHIN": 4, "T6": 2, "T7": 8}
AUTOMATIC = {"S4_PLAIN": "SAME", "S4_FURNITURE": "SAME", "S5": "NEW",
             "T2_CROSS": "NEW", "T2_WITHIN": "NEW", "T6": "SAME"}


def adapt(text):
    # Only envelope syntax changes; never consume a newline or reorder content.
    text = re.sub(r"^## СТРАНИЦА (\d+)[ \t]*$", r"## Page \1", text, flags=re.M)
    return re.sub(r"^### BLOCK \[([^]]+)\]:[ \t]*(\S+)[ \t]*$", r"### BLOCK #0 [\1]: \2", text, flags=re.M)


def page_hashes(text):
    chunks = re.split(r"^## Page (\d+)[ \t]*$", adapt(text), flags=re.M)
    out = {}
    for number, body in zip(chunks[1::2], chunks[2::2]):
        lines = [x for x in body.splitlines() if x.strip() and not x.startswith(("### BLOCK", "> **", "<!--", "---"))]
        normalized = v1.normalize("\n".join(lines))
        if normalized:
            out[int(number)] = hashlib.sha256(normalized.encode()).hexdigest()
    return out


def prepare_source(original, output):
    original, output = Path(original).resolve(), Path(output)
    text = original.read_text(errors="replace")
    source = original.with_name("blocks.json")
    if source.exists():
        raw = read(source)
    else:
        source = original.with_name("result.json")
        exported = read(source)
        pages, blocks = [], []
        for p in exported.get("pages", []):
            number = int(p.get("page_number", p.get("page_index", 0) + 1))
            pages.append({"page_index": number - 1, "width_px": p.get("width", p.get("width_px")),
                          "height_px": p.get("height", p.get("height_px")), "rotation": p.get("rotation", 0)})
            for b in p.get("blocks", []):
                blocks.append({**b, "page_index": number - 1, "block_id": b.get("id", b.get("block_id"))})
        raw = {"pages": pages, "blocks": blocks}
    sources = [receipt(original), receipt(source)]
    version = v1.digest({"namespace": NAMESPACE, "source_hashes": [s["sha256"] for s in sources]})
    directory = output / version
    directory.mkdir(parents=True, exist_ok=True)
    md, blocks = directory / "document.md", directory / "blocks.json"
    md.write_text(adapt(text))
    write(blocks, raw)
    pdf = original.with_name("document.pdf")
    doc = {"document_version": version, "document_code": original.parents[3].name,
           "version_id": original.parents[1].name, "stage": NAMESPACE,
           "artifacts": {"work_md": receipt(md), "blocks": receipt(blocks)},
           "source_refs": {"original_sources": sources, "source_pdf": str(pdf),
                           "adapter": "Envelope syntax only, preserving original line count; geometry from blocks.json or result.json"}}
    if len(text.splitlines()) != len(md.read_text().splitlines()):
        raise ValueError("Adapter changed source line count")
    return doc


def candidates(document, result=None):
    """Anchors are source lines; structural features never use unit identity keys."""
    ledger, _ = LineLedger.read(document)
    result = materialize_document(document) if result is None else result
    sem, col = result["semantics"], result["ledger"]["columns"]
    headings = {int(i): h for i, h in sem["headings"].items()}
    captions = {int(i): c for i, c in sem["captions"].items()}
    eligible = {int(n) for n, p in sem["pages"].items() if p["classification"]["unit_type"] in {"TABLE", "TEXT_SECTION"}}
    meaningful = {n: [i for i in ids if col["kind"][i] != "FURNITURE"] for n, ids in ledger.page_lines.items() if n in eligible}
    text_ids = {n: [i for i in ids if col["kind"][i] not in {"TABLE_ROW", "TABLE_SEPARATOR"}] for n, ids in meaningful.items()}
    norms = col["normalized_text"]
    out = []
    edge_index = {(d["kind"], d["right_anchor"]): d for d in result["decisions"]["decisions"]}
    def add(variant, a, b, kind, evidence):
        if a is None or a >= b:
            return
        if col["kind"][a] == "FURNITURE" or col["kind"][b] == "FURNITURE":
            return
        lp, rp = ledger.lines[a].page, ledger.lines[b].page
        if lp not in eligible or rp not in eligible:
            return
        key = {"document_version": document["document_version"], "kind": kind,
               "anchors": [ledger.anchor(a, "LAST"), ledger.anchor(b, "FIRST")]}
        out.append({**key, "case_id": "sfv3dev_" + v1.digest(key)[:24], "variant": variant,
                    "stratum": variant.split("_")[0], "page_pair": [lp, rp], "structural_evidence": evidence,
                    "foundation_edge": edge_index.get((kind, b))})

    previous_text = None
    source_context, context_at = None, {}
    for n in sorted(ledger.pages):
        if n not in eligible:
            source_context, previous_text = None, None
            continue
        ids = meaningful.get(n, [])
        prior_headings = {norms[i] for i in meaningful.get(n - 1, []) if "MD_HEADING" in headings.get(i, {}).get("evidence_codes", [])}
        for i in ids:
            h, cap = headings.get(i), captions.get(i)
            context_at[i] = source_context
            is_text = col["kind"][i] not in {"TABLE_ROW", "TABLE_SEPARATOR"}
            if not is_text:
                continue
            a = previous_text
            if h and "MD_HEADING" in h["evidence_codes"] and norms[i] in prior_headings:
                add("S1", a, i, "SECTION", ["REPEATED_MD_ON_PREVIOUS_PAGE"])
            if h and h["tier"] == "CANDIDATE" and "NUMBER_CHAIN_CANDIDATE" in h["evidence_codes"]:
                # Paragraph start, not each line in a numbered paragraph.
                raw_before = ledger.lines[i - 1] if i else None
                paragraph_start = raw_before is None or raw_before.block_id != ledger.lines[i].block_id or ledger.lines[i].markdown_line > raw_before.markdown_line + 1
                if paragraph_start:
                    add("S2_NUMBER", a, i, "SECTION", ["PLAIN_NUMBERED_PARAGRAPH_START"])
            if h and "ALL_CAPS_CANDIDATE" in h["evidence_codes"] and "MD_HEADING" not in h["evidence_codes"] and ids and i == ids[0]:
                add("S2_CAPS", a, i, "SECTION", ["ALL_CAPS_PAGE_START"])
            if h and "BOLD_NUMBERED_HEADING" in h["evidence_codes"]:
                add("S2_BOLD", a, i, "SECTION", ["BOLD_NUMBERED"])
            if cap and cap["decision"] == "REVIEW" and h and "MD_HEADING" in h["evidence_codes"]:
                add("S3", a, i, "SECTION", ["POSITIONAL_CAPTION_MD_CONFLICT"])
            if h and h["tier"] == "STRONG" and not cap:
                source_context = i
            previous_text = i
        previous = text_ids.get(n - 1, [])
        current = text_ids.get(n, [])
        if previous and current and ids and current[0] == ids[0]:
            a, b = previous[-1], current[0]
            h, cap = headings.get(b), captions.get(b)
            if not h and not cap:
                initial_furniture = any(j < b and col["kind"][j] == "FURNITURE" for j in ledger.page_lines[n])
                add("S4_FURNITURE" if initial_furniture else "S4_PLAIN", a, b, "SECTION", ["ORDINARY_PAGE_START", "NO_HEADING_EVIDENCE"])
            elif h and "MD_HEADING" in h["evidence_codes"] and h["number"] and not cap and norms[b] not in prior_headings:
                add("S5", a, b, "SECTION", ["FRESH_NUMBERED_MD_PAGE_START"])

    tables = [u for u in sem["units"] if u["kind"] == "TABLE_SEGMENT" and u["first_content_line"] is not None]
    def cells(table, first=True):
        i = table["first_content_line"] if first else table["last_content_line"]
        return v1.parse_cells(ledger.lines[i].text)
    def partial(table):
        return any(not c.strip() for c in cells(table)) or table["first_row"]["kind"] in {"DATA_LIKE", "UNKNOWN"}
    def ordinal(table, first):
        ids = range(table["first_line"], table["last_line"] + 1)
        rows = [j for j in ids if col["kind"][j] == "TABLE_ROW"]
        if not first:
            rows.reverse()
        for j in rows:
            first_cell = v1.parse_cells(ledger.lines[j].text)[0].strip()
            if re.fullmatch(r"\d+", first_cell):
                return int(first_cell)
            if col["kind"][j] == "TABLE_ROW" and j != table["first_content_line"]:
                return None
        return None
    for left, right in zip(tables, tables[1:]):
        a, b = left["last_content_line"], right["first_content_line"]
        lp, rp = ledger.lines[a].page, ledger.lines[b].page
        if rp not in {lp, lp + 1}:
            continue
        cross = rp != lp
        gap = list(range(left["last_line"] + 1, right["first_line"]))
        meaningful_gap = [i for i in gap if col["kind"][i] != "FURNITURE"]
        headed = [i for i in gap if headings.get(i, {}).get("tier") == "STRONG" or i in captions]
        same = [v1.normalize(c) for c in cells(left)] == [v1.normalize(c) for c in cells(right)]
        lc, rc = context_at.get(left["first_line"]), context_at.get(right["first_line"])
        if cross and same and not meaningful_gap:
            if lc is not None and rc == lc:
                add("T1", a, b, "TABLE", ["REPEATED_FIRST_ROW", "SAME_NONEMPTY_SOURCE_HEADING_CONTEXT", "NO_MEANINGFUL_GAP"])
            if lc is None and rc is None:
                add("T1b", a, b, "TABLE", ["REPEATED_FIRST_ROW", "NO_SOURCE_HEADING_CONTEXT", "NO_MEANINGFUL_GAP"])
        if same and headed:
            add("T2_CROSS" if cross else "T2_WITHIN", a, b, "TABLE", ["REPEATED_FIRST_ROW", "FRESH_HEADING_OR_CAPTION_GAP"])
        if cross and (partial(left) or partial(right)):
            add("T3_BOTH" if partial(left) and partial(right) else "T3_ONE", a, b, "TABLE", ["PARTIAL_FIRST_ROW_SURFACE"])
        if cross and (right["first_row"]["content_row_count"] <= 2 or not right["first_row"]["separator_offsets"]):
            add("T4", a, b, "TABLE", ["AT_MOST_TWO_CONTENT_ROWS_OR_NO_SEPARATOR"])
        if gap and not headed:
            add("T5_CROSS" if cross else "T5_WITHIN", a, b, "TABLE", ["NONHEADING_GAP_INCLUDING_FURNITURE"])
        last_number, first_number = ordinal(left, False), ordinal(right, True)
        if cross and not meaningful_gap and left["first_row"]["width"] == right["first_row"]["width"] and last_number is not None and last_number > 1 and first_number == last_number + 1:
            add("T6", a, b, "TABLE", ["ADJACENT_EQUAL_WIDTH_NO_GAP", "STRICT_NUMBER_PROGRESSION"])
    for table in tables:
        i, n = table["first_content_line"], ledger.lines[table["first_content_line"]].page
        page_headings = [j for j in meaningful.get(n, []) if headings.get(j, {}).get("tier") == "STRONG" and j not in captions]
        if page_headings and i < page_headings[0] and context_at.get(i) is not None:
            add("T7", i, page_headings[0], "OWNER", ["TABLE_BEFORE_FIRST_PAGE_HEADING", "PRECEDING_SOURCE_HEADING_EXISTS"])
    return out


def select_cases(pool):
    selected, pages_used, ids_used, per_doc, counts = [], set(), set(), Counter(), Counter()
    # Scarce variants first; no answer- or prediction-dependent ranking.
    by_variant = {v: [c for c in pool if c["variant"] == v] for v in QUOTAS}
    order = sorted(QUOTAS, key=lambda v: (len(by_variant[v]) / QUOTAS[v], v))
    for variant in order:
        choices = by_variant[variant]
        while counts[variant] < QUOTAS[variant]:
            available = [c for c in choices if c["case_id"] not in ids_used
                         and tuple(c.get("page_content_hashes", [c["document_version"], *c["page_pair"]])) not in pages_used
                         and per_doc[(c["stratum"], c["document_version"])] < 3]
            if not available:
                break
            c = min(available, key=lambda c: (per_doc[(c["stratum"], c["document_version"])], c["case_id"]))
            selected.append(c)
            ids_used.add(c["case_id"])
            pages_used.add(tuple(c.get("page_content_hashes", [c["document_version"], *c["page_pair"]])))
            per_doc[(c["stratum"], c["document_version"])] += 1
            counts[variant] += 1
    return sorted(selected, key=lambda c: (c["stratum"], c["variant"], c["case_id"])), dict(counts)


def load_pool():
    pool = read(ROOT / "source_pool_index.json")
    for d in pool:
        for c in d["cases"]:
            c["page_content_hashes"] = [d["page_hashes"][str(n)] for n in c["page_pair"]]
    return pool


def baseline_prediction(case, result):
    """Original V1 boundary metric over source anchors; no legacy audit import."""
    if case["kind"] == "OWNER":
        return {"decision": "REVIEW", "basis": "DEFAULT", "evidence_codes": ["OWNERSHIP_QUESTION_NOT_BOUNDARY"]}
    owners = []
    for a in case["anchors"]:
        found = set()
        if case["kind"] == "SECTION":
            for u in result["text_sections"]["sections"]:
                if any(all(r[k] == a[k] for k in ("page", "block_id", "markdown_line", "line_sha256")) for r in u["fragment_refs"]):
                    found.add(u["text_section_key"])
        else:
            for u in result["table_identities"]["tables"]:
                if any(s["page"] == a["page"] and a["block_id"] in s["block_ids"] and s["markdown_line_span"][0] <= a["markdown_line"] <= s["markdown_line_span"][1] for s in u["continuation_segments"]):
                    found.add(u["table_key"])
        owners.append(found)
    if any(len(o) != 1 for o in owners):
        return {"decision": "REVIEW", "basis": "DEFAULT", "evidence_codes": ["V1_UNRESOLVED"]}
    return {"decision": "SAME" if owners[0] == owners[1] else "NEW", "basis": "PROVEN",
            "evidence_codes": ["V1_LEGACY_DETERMINISTIC_NOT_CALIBRATED"]}


def scan_sources():
    freeze = read(ROOT / "reports/FOUNDATION_FREEZE.json")
    for r in freeze["code"]:
        if v1.file_sha(Path(r["path"])) != r["sha256"]:
            raise ValueError("Foundation changed after freeze")
    evaluation = read(OLD / "baseline_documents.json")
    eval_versions = {d["document_version"] for d in evaluation}
    eval_codes = {d["document_code"] for d in evaluation}
    eval_sources = {a["sha256"] for d in evaluation for a in d["artifacts"].values() if isinstance(a, dict) and a.get("sha256")}
    eval_pages = {h for d in evaluation for h in page_hashes(Path(d["artifacts"]["work_md"]["path"]).read_text()).values()}
    old_docs = read(OLD / "dev_documents.json")
    originals = {Path(d["source_refs"]["original_sources"][0]["path"]).resolve() for d in old_docs if not d["document_code"].casefold().startswith("mockup")}
    paths = sorted(Path("projects_v2/objects").glob("*/disciplines/*/documents/*/versions/*/02_work/document.md"))
    pool, rejected = [], Counter()
    for path in paths:
        path = path.resolve()
        if "/272_" in str(path) or path.parents[3].name in eval_codes:
            rejected["eval_project_or_document_code"] += 1
            continue
        if not path.with_name("document.pdf").exists() or not (path.with_name("blocks.json").exists() or path.with_name("result.json").exists()):
            rejected["missing_source"] += 1
            continue
        text = path.read_text(errors="replace")
        hashes = page_hashes(text)
        if len(hashes) < 4 or (set(hashes.values()) & eval_pages):
            rejected["insufficient_pages_or_eval_page_overlap"] += 1
            continue
        # Read source structure without requiring old result.json or any materializer truth.
        doc = prepare_source(path, ROOT / "source_pool")
        original_hashes = {s["sha256"] for s in doc["source_refs"]["original_sources"]}
        if original_hashes & eval_sources or doc["document_version"] in eval_versions:
            rejected["eval_source_or_version_overlap"] += 1
            continue
        cases = candidates(doc)
        pool.append({"document": doc, "cases": cases, "base": path in originals,
                     "page_hashes": hashes, "original": str(path)})
    write(ROOT / "source_pool_index.json", pool)
    write(ROOT / "source_exclusion.json", {"rejected": dict(rejected), "eval_versions": sorted(eval_versions),
          "eval_codes": sorted(eval_codes), "eval_source_hashes": sorted(eval_sources), "eval_page_hashes": sorted(eval_pages),
          "original_dev_documents": len(old_docs), "base_documents_requested": len(originals)})
    print({"source_candidates": len(pool), "base": sum(c["base"] for c in pool), "rejected": dict(rejected)})


def build_packet():
    if (ROOT / "reports/FIRST_WAVE_DEV_PACKET.json").exists():
        raise ValueError("DEV packet already frozen")
    pool = load_pool()
    exclusion = read(ROOT / "source_exclusion.json")
    eval_sources, eval_pages = set(exclusion["eval_source_hashes"]), set(exclusion["eval_page_hashes"])
    selected, codes, pdf_hashes = [], set(), set()
    def accept(candidate):
        d = candidate["document"]
        if d["document_code"] in codes:
            return False
        pdf = receipt(Path(d["source_refs"]["source_pdf"]))
        if pdf["sha256"] in pdf_hashes or pdf["sha256"] in eval_sources:
            return False
        d["artifacts"]["pdf"] = pdf
        codes.add(d["document_code"])
        pdf_hashes.add(pdf["sha256"])
        selected.append(candidate)
        return True
    selection = read(ROOT / "dev_selection.json")
    chosen_versions = set(selection["documents"])
    for candidate in sorted([c for c in pool if c["document"]["document_version"] in chosen_versions], key=lambda c: c["document"]["document_version"]):
        if not accept(candidate):
            raise ValueError("Selected DEV source duplicates another PDF/code or overlaps EVAL; reselect explicitly")
    base_count = sum(c["base"] for c in selected)
    documents = [c["document"] for c in selected]
    all_cases = [case for c in selected for case in c["cases"]]
    chosen_keys = {(c["case_id"], c["variant"]) for c in selection["cases"]}
    chosen = sorted([c for c in all_cases if (c["case_id"], c["variant"]) in chosen_keys], key=lambda c: (c["stratum"], c["variant"], c["case_id"]))
    counts = Counter(c["variant"] for c in chosen)
    if len(chosen) != len(chosen_keys):
        raise ValueError("Selection is not fully source-resolvable")
    by_doc = defaultdict(list)
    for c in chosen:
        by_doc[c["document_version"]].append(c)
    predictions, baseline_predictions = {}, {}
    for d in documents:
        result = materialize_document(d)
        baseline = v1.materialize_document(d)  # Separate benchmark producer, never nested in Foundation.
        for name in ("ledger", "semantics", "decisions"):
            write(ROOT / "dev_foundation" / d["document_version"] / (name + ".json"), result[name])
        resolver = AnchorResolver(result)
        raw = Path(d["artifacts"]["work_md"]["path"]).read_text().splitlines()
        for c in by_doc[d["document_version"]]:
            variant = c["variant"]
            c["namespace"] = NAMESPACE
            c["annotation_mode"] = "AUTOMATIC_CONTROL" if variant in AUTOMATIC else "HUMAN"
            c["answer"] = None
            c["allowed_choices"] = (["OWNER_PRECEDING_SECTION", "OWNER_FOLLOWING_HEADING", "UNSURE"]
                                    if c["kind"] == "OWNER" else ["SAME", "NEW", "UNSURE"])
            question = ("К какому разделу относится таблица: к предшествующему или к заголовку ниже?"
                        if c["kind"] == "OWNER" else "По двум исходным якорям: это одна таблица или разные?" if c["kind"] == "TABLE"
                        else "По двум исходным якорям: это один раздел или разные?")
            c["display"] = {"question": question, "panels": [{"focus_pages": [a["page"]], "anchor": a,
                "source_pdf": d["artifacts"]["pdf"], "source_markdown": d["artifacts"]["work_md"],
                "context_start_line": max(1, a["markdown_line"] - 4),
                "context": raw[max(0, a["markdown_line"] - 5):a["markdown_line"] + 4]} for a in c["anchors"]]}
            if c["kind"] == "TABLE":
                col = result["ledger"]["columns"]
                c["source_segment_spans"] = []
                for a in c["anchors"]:
                    i = resolver.resolve(a)
                    u = result["semantics"]["units"][col["owner"][i]]
                    c["source_segment_spans"].append({"page": a["page"], "block_id": a["block_id"],
                        "markdown_line_span": [col["markdown_line"][u["first_line"]], col["markdown_line"][u["last_line"]]],
                        "first_row": u["first_row"]})
            if variant in AUTOMATIC:
                c["control_expectation"] = {"decision": AUTOMATIC[variant], "basis": "STRUCTURAL_CONTRACT_NOT_HUMAN_TRUTH"}
            predictions[c["case_id"]] = resolver.predict(c)
            baseline_predictions[c["case_id"]] = baseline_prediction(c, baseline)
    write(ROOT / "dev_documents.json", documents)
    packet = {"schema": "semantic-foundation-dev-packet.v3", "namespace": NAMESPACE, "answers_imported": 0,
              "foundation_freeze": receipt(ROOT / "reports/FOUNDATION_FREEZE.json"), "cases": chosen,
              "selection_code": [receipt(Path(__file__)), receipt(Path(__file__).with_name("select_dev.py"))],
              "selection_manifest": receipt(ROOT / "dev_selection.json"), "quotas": QUOTAS}
    write(ROOT / "reports/FIRST_WAVE_DEV_PACKET.json", packet)
    packet_hash = v1.file_sha(ROOT / "reports/FIRST_WAVE_DEV_PACKET.json")
    freeze_predictions(ROOT / "dev_predictions.json", predictions, NAMESPACE, packet_hash)
    freeze_predictions(ROOT / "dev_v1_predictions.json", baseline_predictions, NAMESPACE, packet_hash)
    write(ROOT / "dev_namespace/answers.template.json", {"namespace": NAMESPACE, "packet_sha256": packet_hash,
          "answers": {c["case_id"]: None for c in chosen if c["annotation_mode"] == "HUMAN" and c["kind"] != "OWNER"}})
    write(ROOT / "dev_namespace/ownership_answers.template.json", {"namespace": NAMESPACE, "packet_sha256": packet_hash,
          "ownership_answers": {c["case_id"]: None for c in chosen if c["kind"] == "OWNER"},
          "allowed_choices": ["OWNER_PRECEDING_SECTION", "OWNER_FOLLOWING_HEADING", "UNSURE"]})
    versions = {d["document_version"] for d in documents}
    source_hashes = {s["sha256"] for d in documents for s in [*d["artifacts"].values(), *d["source_refs"]["original_sources"]]}
    dev_pages = {h for c in selected for h in c["page_hashes"].values()}
    separation = {"namespace": NAMESPACE, "evaluation_namespace": "FROZEN_EVALUATION_ONLY",
                  "version_overlap": sorted(versions & set(exclusion["eval_versions"])),
                  "document_code_overlap": sorted(codes & set(exclusion["eval_codes"])),
                  "source_hash_overlap": sorted(source_hashes & eval_sources), "page_hash_overlap": sorted(dev_pages & eval_pages),
                  "eval_exclusion_documents": len(exclusion["eval_versions"]), "dev_documents": len(documents),
                  "base_documents": base_count, "added_documents": len(documents) - base_count,
                  "eval_project_excluded": True, "frozen_answers_accessed": False,
                  "page_hash_definition": "SHA256(V1 normalized nonempty nonmetadata page text); empty content excluded"}
    separation["pass"] = not any(separation[k] for k in ("version_overlap", "document_code_overlap", "source_hash_overlap", "page_hash_overlap"))
    write(ROOT / "reports/DEV_EVAL_SEPARATION.json", separation)
    variants = {v: {"target": QUOTAS[v], "selected": counts.get(v, 0), "pool": sum(c["variant"] == v for c in all_cases),
                    "documents": len({c["document_version"] for c in chosen if c["variant"] == v})} for v in QUOTAS}
    human = sum(c["annotation_mode"] == "HUMAN" for c in chosen)
    summary = {"total": len(chosen), "human": human, "automatic_controls": len(chosen) - human,
               "documents": len(documents), "variants": variants,
               "duplicate_case_ids": len(chosen) - len({c["case_id"] for c in chosen}),
               "duplicate_page_pairs": len(chosen) - len({(c["document_version"], *c["page_pair"]) for c in chosen}),
               "duplicate_page_content_pairs": len(chosen) - len({tuple(c["page_content_hashes"]) for c in chosen}),
               "max_per_document_per_stratum": max(Counter((c["document_version"], c["stratum"]) for c in chosen).values(), default=0),
               "stratum_document_counts": {s: len({c["document_version"] for c in chosen if c["stratum"] == s}) for s in sorted({v.split('_')[0] for v in QUOTAS})},
               "review_sourced_human_cases": sum(c["annotation_mode"] == "HUMAN" and c.get("foundation_edge") is not None and c["foundation_edge"]["decision"] == "REVIEW" for c in chosen),
               "prediction_distribution": dict(Counter(p["decision"] for p in predictions.values()))}
    write(ROOT / "reports/DEV_PACKET_SUMMARY.json", summary)
    print(summary)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["scan", "build"])
    args = parser.parse_args()
    scan_sources() if args.phase == "scan" else build_packet()
