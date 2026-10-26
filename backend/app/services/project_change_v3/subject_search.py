"""Source-grounded subject discovery with bounded local retrieval and explicit gaps.

No shell tools, PDF text extraction, network calls or truth inputs. All model
requests go through the caller's existing provider and its permission gates.
The catalog is routing data; detailed Mapper/Miner still receive original pages.
"""
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import hashlib
import json
import re
import time
import unicodedata

from . import contracts, mapper_portions as mp, subject_contracts as sc, transport
from .provider import build_codex_payload
from .run_storage import read
from .source_prep import load_page_record, mapping_images
from .validate import validate_map

MAX_SEARCH_ROUNDS = 2
MAX_SEARCH_QUERIES = 12
MAX_SEARCH_HITS = 12
CHECKPOINT = "project_change_v3_subject_checkpoint"
ARTIFACT = "project_change_v3_subject_search"


def validate_schema(answer, schema):
    import jsonschema
    jsonschema.validate(answer, schema)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def canonical(value, work_dir, replacement="$SOURCES"):
    if isinstance(value, str):
        root = str(work_dir)
        return replacement + value[len(root):] if value.startswith(root + "/") else value
    if isinstance(value, list):
        return [canonical(v, work_dir, replacement) for v in value]
    if isinstance(value, dict):
        return {k: canonical(v, work_dir, replacement) for k, v in value.items()}
    return value


def normalize(text):
    # Search only: never change authoritative MD or evidence fragments.
    text = unicodedata.normalize("NFKC", str(text)).casefold().replace("ё", "е")
    text = text.translate(str.maketrans("abcehkmoptxy", "авсенкмортху"))
    return " ".join(re.findall(r"[\w]+", text))


class SourceIndex:
    def __init__(self, pages):
        self.pages = {(p["side"], p["physical_page"]): p for p in pages}
        self.text = {key: normalize(" ".join(b.get("structured_md", "") for b in p["blocks"]))
                     for key, p in self.pages.items()}

    def search(self, terms, side="BOTH", limit=MAX_SEARCH_HITS):
        phrases = set(filter(None, (normalize(term) for term in terms)))
        matches = []
        for key, text in self.text.items():
            if side != "BOTH" and side != key[0]:
                continue
            score = sum(1 for phrase in phrases if f" {phrase} " in f" {text} ")
            if score:
                matches.append((score, key))
        matches.sort(key=lambda row: (-row[0], row[1]))
        return {"hits": [list(key) for _, key in matches[:limit]], "total_hits": len(matches),
                "truncated": len(matches) > limit}


def validate_catalog(answer, pages):
    validate_schema(answer, sc.CATALOG_SCHEMA)
    known = {(p["side"], p["physical_page"], b["block_id"]): b["modality"]
             for p in pages for b in p["blocks"]}
    seen, ids = set(), set()

    def ref(item):
        key = (item["side"], item["physical_page"], item["block_id"])
        if key not in known or item["block_type"] != known[key]:
            raise ValueError("catalog cites an unavailable block")
        seen.add(key)

    for row in answer["observations"]:
        if not row["local_id"] or row["local_id"] in ids or not row["evidence"]:
            raise ValueError("catalog requires distinct IDs and source evidence")
        ids.add(row["local_id"])
        for item in row["evidence"]:
            ref(item)
        owned = {(x["side"], x["physical_page"], x["block_id"]) for x in row["evidence"]}
        for parameter in row["parameters"]:
            if not parameter["evidence"]:
                raise ValueError("catalog parameter has no evidence")
            for item in parameter["evidence"]:
                ref(item)
                if (item["side"], item["physical_page"], item["block_id"]) not in owned:
                    raise ValueError("parameter evidence is outside its subject")
    for item in answer["other_blocks"]:
        if not item["reason"].strip():
            raise ValueError("catalog exclusion requires a reason")
        ref(item["evidence"])
    if seen != set(known):
        raise ValueError("catalog has unaccounted source blocks")


def validate_route(answer, old, new):
    validate_schema(answer, sc.ROUTE_SCHEMA)
    old_ids, new_ids = {r["id"] for r in old}, {r["id"] for r in new}
    covered = set(answer["unresolved_ids"])
    if not covered <= old_ids | new_ids:
        raise ValueError("route has unknown unresolved IDs")
    for link in answer["links"]:
        a, b = set(link["old_ids"]), set(link["new_ids"])
        if not a or not b or not a <= old_ids or not b <= new_ids:
            raise ValueError("route cites an unavailable subject")
        covered |= a | b
    if covered != old_ids | new_ids:
        raise ValueError("route omitted subjects without an explicit disposition")


def route_card(row):
    return {key: row[key] for key in ("id", "name", "aliases", "function", "locations",
                                     "source_role", "evidence", "uncertainties")}


class SubjectSearch:
    def __init__(self, *, pair_id, run_id, structure, work_dir, source_manifest,
                 profile, invoke, save, progress, cancelled=lambda: False, donor_checkpoint=None):
        self.pair_id, self.run_id = pair_id, run_id
        self.work_dir, self.profile = Path(work_dir), profile
        self.invoke, self.save, self.progress, self.cancelled = invoke, save, progress, cancelled
        self.index = SourceIndex(structure)
        self.images = transport.PlanningImageCache()
        self.records = {key: load_page_record(self.work_dir, *key) for key in self.index.pages}
        self.source_refs = {}
        self.expected_images = {}
        for key, p in self.records.items():
            for block in p['blocks']:
                if block.get('graphic_crop_ref'):
                    actual = self.images(block['graphic_crop_ref'])['sha256']
                    if actual != block['graphic_crop_sha256']:
                        raise ValueError('graphic source changed after preparation')
                    self.expected_images[block['graphic_crop_ref']] = actual
            self.source_refs[f"{key[0]}:{key[1]}"] = {
                "side": key[0], "physical_page": key[1], "pdf_sha256": p["source_pdf_sha256"],
                "origin": p.get("origin"),
                "blocks": [{"block_id": b["block_id"],
                            "content_sha256": digest({k: v for k, v in b.items() if k != "graphic_crop_ref"}),
                            "graphic_crop_sha256": b.get("graphic_crop_sha256")}
                           for b in p["blocks"]],
            }
        identity = {"structure": canonical(structure, self.work_dir), "sources": self.source_refs,
                    "pdf": {key: source_manifest[key] for key in ("old_pdf_sha256", "new_pdf_sha256")},
                    "profile": asdict(profile), "reasoning": contracts.REASONING,
                    "contract": sc.CONTRACT_SHA256, "mapper_prompt": contracts.MAPPER_PROMPT_SHA256,
                    "mapper_schema": contracts.MAP_SCHEMA,
                    "source_packaging": contracts.SOURCE_PACKAGING_VERSION,
                    "payload_version": contracts.SOURCE_PAYLOAD_VERSION,
                    "limits": [MAX_SEARCH_ROUNDS, MAX_SEARCH_QUERIES, MAX_SEARCH_HITS]}
        self.identity = digest(identity)
        self.cache = {}
        if donor_checkpoint:
            if donor_checkpoint.get("identity") != self.identity:
                raise ValueError("subject checkpoint sources, model or contract changed")
            for row in donor_checkpoint["entries"]:
                if row.get("answer_sha256") != digest(row["answer"]):
                    raise ValueError("subject checkpoint answer changed")
                if row["key"] in self.cache:
                    raise ValueError("duplicate checkpoint request key")
                self.cache[row["key"]] = row
        self.entries = []
        self.attempts = []
        self.ledger = {"schema": sc.VERSION, "run_id": run_id, "comparison_mode": "subjects",
                       "source_refs": self.source_refs, "catalog": [], "links": [], "groups": [],
                       "queries": [], "gaps": [], "detailed_packets": [],
                       "coverage_contract": "candidate_search", "semantic_completeness_proven": False}
        self.started = time.monotonic()

    def persist(self):
        checkpoint = {"schema": sc.VERSION, "run_id": self.run_id, "pair_id": self.pair_id,
                      "identity": self.identity, "entries": self.entries, "attempts": self.attempts}
        self.save(CHECKPOINT, checkpoint)
        self.save(ARTIFACT, self.ledger)

    def packet(self, prompt, data, schema, images):
        payload, paths, _ = build_codex_payload(prompt, data, images,
                                               image_digest=lambda p: self.images(p)["sha256"])
        measured = transport.measure_request(payload, paths, schema, selection=self.profile.selection,
                                              read_image=self.images)
        for path in paths:
            if str(path) in self.expected_images and self.images(path)['sha256'] != self.expected_images[str(path)]:
                raise ValueError('graphic source changed after planning')
        return measured

    def ask(self, phase, prompt, data, schema, images, validator):
        if self.cancelled():
            raise RuntimeError("subject search cancelled")
        # Re-hash immediately before use. A stale planning cache cannot authorize a call.
        self.images = transport.PlanningImageCache()
        measured = self.packet(prompt, data, schema, images)
        if not measured["fits"]:
            raise ValueError(f"{phase}: full source packet exceeds provider envelope")
        image_hashes = [row["sha256"] for row in measured["image_rows"]]
        key = digest({"identity": self.identity, "phase": phase, "prompt": prompt, "schema": schema,
                      "data": canonical(data, self.work_dir), "images": image_hashes})
        if key in self.cache:
            answer = canonical(self.cache[key]["answer"], "$SOURCES", str(self.work_dir))
            validate_schema(answer, schema)
            validator(answer)
            if not any(row['key'] == key for row in self.entries):
                self.entries.append(self.cache[key])
            self.persist()
            return answer
        self.progress(f"По инженерным предметам: {phase}, принято запросов {len(self.entries)}", "MAPPING")
        call_id = f"{self.pair_id}_SUBJECT_{key[:20]}"
        self.persist()  # durable identity even if the very first request is cancelled
        answer, error = None, None
        try:
            answer = self.invoke(stage="MAPPING", call_id=call_id, pair_id=self.pair_id,
                                 prompt=prompt, data=data, schema=schema, images=images)
            validate_schema(answer, schema)
            validator(answer)
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            row = {"key": key, "call_id": call_id, "phase": phase, "accepted": error is None,
                   "answer": canonical(answer, self.work_dir), "error": error,
                   "estimated_input_tokens": measured.get("estimated_input_tokens")}
            row["answer_sha256"] = digest(row["answer"])
            if error is None:
                self.entries.append(row)
                self.cache[key] = row
            self.attempts.append(row)
            self.persist()
        return answer

    def chunks(self, pages, prompt, schema, extra=None, *, max_pages=16):
        """Plan every exact full-source packet; never clip a page to fit."""
        def data(rows):
            return {"pair": self.pair_id, "pages": rows, **(extra or {})}
        if not pages:
            return []
        if len(pages) <= max_pages and self.packet(prompt, data(pages), schema, mapping_images(pages))["fits"]:
            return [pages]
        if len(pages) == 1:
            raise ValueError(f"source page {pages[0]['side']}:{pages[0]['physical_page']} cannot fit")
        mid = len(pages) // 2
        return (self.chunks(pages[:mid], prompt, schema, extra, max_pages=max_pages) +
                self.chunks(pages[mid:], prompt, schema, extra, max_pages=max_pages))

    def catalog(self):
        jobs = []
        for side in ("OLD", "NEW"):
            pages = [p for key, p in sorted(self.index.pages.items()) if key[0] == side]
            jobs.extend(self.chunks(pages, sc.CATALOG_PROMPT, sc.CATALOG_SCHEMA))
        # All source packets preflighted before the first inference.
        for pages in jobs:
            answer = self.ask("каталог", sc.CATALOG_PROMPT, {"pair": self.pair_id, "pages": pages},
                              sc.CATALOG_SCHEMA, mapping_images(pages), lambda a: validate_catalog(a, pages))
            batch_id = digest([(p["side"], p["physical_page"]) for p in pages])[:12]
            for row in answer["observations"]:
                self.ledger["catalog"].append({**row, "id": f"{batch_id}:{row['local_id']}",
                                                "side": pages[0]["side"]})
            self.ledger.setdefault("other_blocks", []).extend(answer["other_blocks"])
        self.persist()

    def route(self):
        old = [route_card(r) for r in self.ledger["catalog"] if r["side"] == "OLD"]
        new = [route_card(r) for r in self.ledger["catalog"] if r["side"] == "NEW"]
        jobs = []

        def plan(a, b):
            if not a or not b:
                return
            data = {"pair": self.pair_id, "old": a, "new": b}
            if len(a) + len(b) <= 128 and self.packet(sc.ROUTE_PROMPT, data, sc.ROUTE_SCHEMA, [])["fits"]:
                jobs.append((a, b))
            elif len(a) == len(b) == 1:
                raise ValueError("catalog card pair exceeds envelope")
            elif len(a) >= len(b) and len(a) > 1:
                mid = len(a) // 2
                plan(a[:mid], b)
                plan(a[mid:], b)
            else:
                mid = len(b) // 2
                plan(a, b[:mid])
                plan(a, b[mid:])
        plan(old, new)
        for a, b in jobs:
            answer = self.ask("поиск связей", sc.ROUTE_PROMPT,
                              {"pair": self.pair_id, "old": a, "new": b}, sc.ROUTE_SCHEMA, [],
                              lambda answer: validate_route(answer, a, b))
            self.ledger["links"].extend(answer["links"])
        # A common page is NOT a reason to merge subjects. Only explicit links
        # involving the same observation form a connected candidate group.
        by_id = {r["id"]: r for r in self.ledger["catalog"]}
        parents = {key: key for key in by_id}
        def root(key):
            while parents[key] != key:
                key = parents[key]
            return key
        for link in self.ledger["links"]:
            ids = sorted(set(link["old_ids"] + link["new_ids"]))
            for key in ids[1:]:
                parents[root(key)] = root(ids[0])
        groups = {}
        for key in sorted(by_id):
            groups.setdefault(root(key), []).append(key)
        return [rows for _, rows in sorted(groups.items())]

    def research(self, ids):
        by_id = {r["id"]: r for r in self.ledger["catalog"]}
        group_id = "S-" + digest(ids)[:16]
        keys = {(ref["side"], ref["physical_page"]) for key in ids for ref in by_id[key]["evidence"]}
        history = []
        for round_no in range(MAX_SEARCH_ROUNDS):
            pages = [self.index.pages[key] for key in sorted(keys)]
            # Source reads stay bounded even if a subject spans many volumes.
            # The full catalog is not repeated here. The investigator reads the
            # original pages without an expected correspondence to confirm.
            extra = {"subject_id": group_id, "search_history": history}
            chunks = self.chunks(pages, sc.SEARCH_PROMPT, sc.SEARCH_SCHEMA, extra)
            queries, unresolved = [], []
            for chunk in chunks:
                answer = self.ask("добор источников", sc.SEARCH_PROMPT,
                                  {"pair": self.pair_id, "pages": chunk, **extra},
                                  sc.SEARCH_SCHEMA, mapping_images(chunk), lambda a: None)
                queries.extend(answer["queries"])
                unresolved.extend(answer["unresolved"])
            if len(queries) > MAX_SEARCH_QUERIES:
                self.ledger["gaps"].append({"subject": group_id, "reason": "query_budget",
                                            "pending_queries": queries[MAX_SEARCH_QUERIES:]})
            new_keys = set()
            for query in queries[:MAX_SEARCH_QUERIES]:
                result = self.index.search(query["terms"], query["side"])
                row = {"subject": group_id, "round": round_no, **query, **result}
                history.append(row)
                self.ledger["queries"].append(row)
                new_keys.update(tuple(key) for key in result["hits"])
                if result["truncated"] or not result["hits"]:
                    self.ledger["gaps"].append({"subject": group_id, "reason": "search_truncated" if
                                                result["truncated"] else "search_no_hits", "query": row})
            added = new_keys - keys
            keys |= new_keys
            if not added:
                if unresolved:
                    self.ledger["gaps"].append({"subject": group_id, "reason": "unresolved", "questions": unresolved})
                break
            if round_no == MAX_SEARCH_ROUNDS - 1:
                self.ledger["gaps"].append({"subject": group_id, "reason": "search_round_budget",
                                            "questions": unresolved, "unreviewed_hits": sorted(added)})
        self.ledger["groups"].append({"subject_id": group_id, "observation_ids": ids, "pages": sorted(keys)})
        self.persist()
        return group_id, [self.index.pages[key] for key in sorted(keys)]

    def detailed(self, group_id, pages):
        if not all(any(p["side"] == side for p in pages) for side in ("OLD", "NEW")):
            self.ledger["gaps"].append({"subject": group_id, "reason": "no_candidate_on_other_side"})
            return []
        from .mapper_planning import MapperPlanner
        planner = MapperPlanner(self.pair_id, pages, self.profile, cancelled=self.cancelled)
        choice = planner.plan(mode="balanced")
        portions = choice.portions
        packets = [mp.portion_data(self.pair_id, pages, p) for p in portions] if portions else [
            {"pair": self.pair_id, "pages": pages}]
        results = []
        for data in packets:
            answer = self.ask("подробное сопоставление", contracts.MAPPER_PROMPT, data,
                              contracts.MAP_SCHEMA, mapping_images(data["pages"]),
                              lambda a: validate_map(self.pair_id, a, data["pages"]))
            self.ledger["detailed_packets"].append({"subject": group_id,
                "old": [p["physical_page"] for p in data["pages"] if p["side"] == "OLD"],
                "new": [p["physical_page"] for p in data["pages"] if p["side"] == "NEW"]})
            results.extend(answer["regions"])
        return results

    def run(self):
        self.catalog()
        groups = self.route()
        regions, seen = [], set()
        for ids in groups:
            group_id, pages = self.research(ids)
            for region in self.detailed(group_id, pages):
                signature = digest({k: v for k, v in region.items() if k != "region_id"})
                if signature in seen:
                    continue
                seen.add(signature)
                regions.append({**region, "region_id": "S-R" + signature[:16]})
        unmatched = {}
        for side in ("OLD", "NEW"):
            covered = {p for r in regions for p in r[side.lower() + "_pages"]}
            unmatched[side] = sorted(p for s, p in self.index.pages if s == side and p not in covered)
        mapping = {"pair": self.pair_id, "regions": sorted(regions, key=lambda r: r["region_id"]),
                   "unmatched_old": unmatched["OLD"], "unmatched_new": unmatched["NEW"],
                   "coverage_notes": ["Экспериментальный поиск по инженерным предметам. unmatched означает, "
                                      "что связь пока не найдена; непроверенные пары не доказывают отсутствие связи."]}
        validate_map(self.pair_id, mapping, list(self.index.pages.values()))
        # Sparse per-OLD rows avoid materializing millions of Python tuples.
        coverage = {p: set() for side, p in self.index.pages if side == "OLD"}
        for packet in self.ledger["detailed_packets"]:
            for old in packet["old"]:
                coverage[old].update(packet["new"])
        total = len(coverage) * sum(s == "NEW" for s, _ in self.index.pages)
        covered = sum(map(len, coverage.values()))
        self.ledger["covered_pairs_by_old"] = {str(p): sorted(rows) for p, rows in coverage.items()}
        self.ledger["summary"] = {"catalog_pages": len(self.index.pages), "observations": len(self.ledger["catalog"]),
            "candidate_groups": len(groups), "detailed_packets": len(self.ledger["detailed_packets"]),
            "accepted_requests": len(self.entries), "new_requests": len(self.attempts),
            "reused_requests": len(self.entries) - len(self.attempts), "open_search_gaps": len(self.ledger["gaps"]),
            "page_pairs_total": total, "page_pairs_reviewed": covered, "page_pairs_unreviewed": total - covered,
            "full_pair_coverage": covered == total, "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "semantic_completeness_proven": False, "unmatched_is_provisional": True}
        self.persist()
        return mapping, self.ledger


def load_checkpoint(session_id, pair_id, run_id):
    """Explicit recovery only, from this pair's failed run; no cache auto-discovery."""
    from . import run_storage
    directory = run_storage.run_dir(session_id, pair_id, run_id)
    state = read(directory / "state.json") or {}
    manifest = read(directory / "run_manifest.json") or {}
    if (state.get("status") != "FAILED" or state.get("pipeline_current") != "MAPPING"
            or (state.get("provenance") or {}).get("comparison_mode") != "subjects"
            or (manifest.get("session_id"), manifest.get("pair_id"), manifest.get("run_id")) !=
               (session_id, pair_id, run_id)):
        raise ValueError("not a failed subject-search mapping run of this pair")
    path = directory / (CHECKPOINT + ".json")
    checkpoint = read(path)
    expected = (state.get("provenance") or {}).get("subject_checkpoint_sha256")
    if not checkpoint or digest(checkpoint) != expected or checkpoint.get("run_id") != run_id:
        raise ValueError("subject checkpoint is missing or changed")
    return checkpoint, state


def verification_context(changes, ledger, structure):
    """Retrieve original counterevidence across both stages; record search gaps."""
    index = SourceIndex(structure)
    context = {}
    for change in changes:
        evidence = {(r['side'], r['physical_page'], r['block_id']) for r in change.get('evidence_items', [])}
        observations = [row for row in ledger['catalog'] if evidence & {
            (r['side'], r['physical_page'], r['block_id']) for r in row['evidence']}]
        ids = {row['id'] for row in observations}
        keys = {(side, page) for side, page, _ in evidence}
        for group in ledger['groups']:
            if ids & set(group['observation_ids']):
                keys.update(tuple(key) for key in group['pages'])
        terms = [change.get('engineering_subject', '')]
        for row in observations:
            terms.extend([row['name'], *row['aliases']])
        result = index.search(terms)
        keys.update(tuple(key) for key in result['hits'])
        ledger.setdefault('verification_queries', []).append({
            'projectchange_id': change['projectchange_id'], 'terms': terms, **result})
        if result['truncated']:
            ledger['gaps'].append({'reason': 'verification_search_truncated',
                                   'projectchange_id': change['projectchange_id'], **result})
        context[change['projectchange_id']] = sorted(keys)
    ledger['summary']['open_search_gaps'] = len(ledger['gaps'])
    return context
