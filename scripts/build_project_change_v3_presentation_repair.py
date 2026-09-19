#!/usr/bin/env python3
"""Build the presentation repair for the sealed V3 ProjectChange snapshot + its audit.

The sealed snapshot (``backend/app/data/project_change_preview_snapshot_v3``) is
read only.  Its builder wrote Cyrillic literals as ASCII ``?`` (commit 3240ee86),
so four card fields and one manifest field arrived corrupted.  This script:

1. scans every string of the sealed snapshot for corruption (``??`` runs,
   UTF-8-as-Latin-1 mojibake, U+FFFD);
2. for each corrupted field class looks up an UNCORRUPTED authoritative source,
   in a fixed precedence order, and copies the text from it verbatim;
3. marks a value corrupted in every source as UNRESOLVED (never reconstructs it);
4. writes the sibling repair file bound to the snapshot by sha256 and
   ``CYRILLIC_PRESENTATION_AUDIT.json`` (before/after counts, source per class).

The byte signature (every Cyrillic letter of a candidate replaced by ``?``) is
recorded as corroboration only; it never selects a value.  Zero model calls.

    python scripts/build_project_change_v3_presentation_repair.py --audit OUT.json [--write]
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from backend.app.services.project_change_preview import presentation_adapter as adapter  # noqa: E402

DATA = REPO / "backend/app/data"
SNAPSHOT = DATA / "project_change_preview_snapshot_v3"
REPAIR = DATA / "project_change_preview_snapshot_v3_repair.json"
PRIOR = DATA / "project_change_preview_snapshot"
HM_FIXTURES = DATA / "human_mapping_fixtures"
REGISTRY = REPO / "prompts/disciplines/_registry.json"
STAGE_DOCS = REPO / "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
RESEARCH_UI = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272/"
                   "projectchange_v3_ai_first_ui_snapshot")
BUILDER = "scripts/build_project_change_v3_production_snapshot.py"
CORRUPTING_COMMIT = "3240ee86"

QMARK_RUN = re.compile(r"\?{2,}")
MOJIBAKE = re.compile("[\u00d0\u00d1][\u0080-\u00bf]|\u00c3[\u0080-\u00bf]|\u00e2\u20ac|\ufffd")
CYRILLIC = re.compile(r"[А-Яа-яЁё]")
UNRESOLVED_RU = ("Пояснение не восстановлено: при сборке снимка кириллица была заменена знаками «?», "
                 "а неповреждённого источника этого текста нет.")


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_blob(path: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), "rev-parse", f"HEAD:{path}"],
                          capture_output=True, text=True, check=True).stdout.strip()


def rel(path: Path) -> str:
    return str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path)


def corrupted(value: Any) -> bool:
    return isinstance(value, str) and bool(QMARK_RUN.search(value) or MOJIBAKE.search(value))


def signature_matches(candidate: str, corrupted_value: str) -> bool:
    return CYRILLIC.sub("?", candidate) == corrupted_value


def walk(value: Any, path: str = ""):
    if isinstance(value, dict):
        for k, v in value.items():
            yield from walk(v, f"{path}.{k}" if path else k)
    elif isinstance(value, list):
        for v in value:
            yield from walk(v, f"{path}[]")
    else:
        yield path, value


def scan(document: Any) -> dict[str, Counter]:
    out: dict[str, Counter] = defaultdict(Counter)
    for path, value in walk(document):
        if corrupted(value):
            out[path][value] += 1
    return out


def strings_scanned(document: Any) -> int:
    return sum(isinstance(v, str) for _p, v in walk(document))


def item_pair(item: dict[str, Any]) -> str:
    pairs = {e["pair_id"] for e in item["evidence"]}
    assert len(pairs) == 1, item["id"]
    return pairs.pop()


def hm_labels() -> dict[str, dict[str, Any]]:
    """Release-bundled production pair labels, keyed by the real pair key."""
    out = {}
    for path in sorted(HM_FIXTURES.glob("UI_DATA_PAIR_*.json")):
        d = json.loads(path.read_text(encoding="utf-8"))
        out[d["pair_key"]] = {"value": d["label"], "path": rel(path), "sha256": sha_file(path),
                              "json_path": "label", "pair_key_field": "pair_key"}
    return out


def research_labels() -> dict[str, dict[str, Any]]:
    out = {}
    for path in sorted(RESEARCH_UI.glob("PAIR_*_UI_DATA.json")):
        d = json.loads(path.read_text(encoding="utf-8"))
        out[d["pair_key"]] = {"value": d["label"], "path": str(path), "sha256": sha_file(path)}
    return out


def prior_production(snapshot_docs: dict[str, Any]) -> dict[str, Any]:
    """Field values of the previous production presentation, per pair with byte-identical PDFs."""
    manifest = PRIOR / "MANIFEST.json"
    data = json.loads((PRIOR / "presentation.json").read_text(encoding="utf-8"))
    per_pair: dict[str, dict[str, Counter]] = defaultdict(lambda: defaultdict(Counter))
    namespace: dict[str, Counter] = defaultdict(Counter)
    for item in data["envelope"]["items"]:
        pair = item_pair(item)
        for field in ("cipher", "discipline", "review_question", "review_explanation_ru"):
            per_pair[pair][field][item.get(field)] += 1
            namespace[field][item.get(field)] += 1
    identical = {}
    for key, doc in snapshot_docs.items():
        prior_doc = data["documents"].get(key)
        identical[key] = bool(prior_doc) and prior_doc["source_sha256"] == doc["source_sha256"]
    return {"path": rel(PRIOR / "presentation.json"), "sha256": sha_file(PRIOR / "presentation.json"),
            "manifest_sha256": sha_file(manifest), "git_blob": git_blob(rel(PRIOR / "presentation.json")),
            "namespace": data["envelope"]["decision_namespace"], "candidate_version": data["envelope"]["candidate_version"],
            "per_pair": per_pair, "all": namespace, "pdf_identical": identical}


def stage_discipline(pair: dict[str, Any], registry: dict[str, str]) -> dict[str, Any] | None:
    codes = {}
    paths = {}
    for side, stage in (("left", "stage_1"), ("right", "stage_2")):
        path = STAGE_DOCS / stage / "documents" / pair[side]["document_code"] / "document.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        codes[side], paths[side] = doc.get("discipline"), rel(path)
    if codes["left"] != codes["right"] or codes["left"] not in registry:
        return None
    return {"value": registry[codes["left"]], "code": codes["left"], "documents": paths,
            "registry": rel(REGISTRY), "registry_git_blob": git_blob(rel(REGISTRY))}


def build(audit_path: Path, write: bool) -> int:
    manifest_path, presentation_path = SNAPSHOT / "MANIFEST.json", SNAPSHOT / "presentation.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    data = json.loads(presentation_path.read_text(encoding="utf-8"))
    manifest_sha, presentation_sha = sha_file(manifest_path), sha_file(presentation_path)
    assert manifest["files"]["presentation.json"] == presentation_sha, "sealed snapshot drifted"
    registry = {code: d["name"] for code, d in json.loads(REGISTRY.read_text(encoding="utf-8"))["disciplines"].items()}

    before_presentation = scan(data)
    before_manifest = scan({k: v for k, v in manifest.items() if k != "files"})
    items = data["envelope"]["items"]
    pairs = {pid: entry["pair"] for pid, entry in data["pairs"].items()}
    by_pair = Counter(item_pair(i) for i in items)
    labels, research = hm_labels(), research_labels()
    prior = prior_production(data["documents"])

    rules: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []

    def corrupted_values(field: str, pair_id: str) -> Counter:
        return Counter(i[field] for i in items if item_pair(i) == pair_id and corrupted(i.get(field)))

    for pair_id in sorted(pairs):
        pdfs_identical = all(prior["pdf_identical"].get(f"{pair_id}:{s}") for s in ("old", "new"))
        # cipher: production pair-label registry of Human Mapping (keyed by the real pair key).
        for value, count in corrupted_values("cipher", pair_id).items():
            src = labels.get(pair_id)
            corroboration = {"research_ui_label": research.get(pair_id, {}).get("value"),
                             "prior_production_cipher": sorted(prior["per_pair"][pair_id]["cipher"]) if pdfs_identical else [],
                             "byte_signature_matches": bool(src) and signature_matches(src["value"], value)}
            if src:
                rules.append({"id": f"cipher:{pair_id}", "target": "item", "field": "cipher", "pair_id": pair_id,
                              "match": "EXACT", "corrupted": value, "status": "REPAIRED", "value": src["value"],
                              "source": {"class": "PRODUCTION_PAIR_LABEL_REGISTRY", **src}, "corroboration": corroboration})
            decisions.append({"field": "cipher", "pair_id": pair_id, "items": count, "resolved": bool(src)})
        # discipline: prior production presentation of the same pair (byte-identical PDFs),
        # else stage document metadata code (both sides agree) through the discipline registry.
        for value, count in corrupted_values("discipline", pair_id).items():
            prior_values = prior["per_pair"][pair_id]["discipline"] if pdfs_identical else Counter()
            stage = stage_discipline(pairs[pair_id], registry)
            source = None
            if len(prior_values) == 1:
                chosen = next(iter(prior_values))
                source = {"class": "PRIOR_PRODUCTION_PRESENTATION", "path": prior["path"], "sha256": prior["sha256"],
                          "git_blob": prior["git_blob"], "json_path": "envelope.items[pair_id].discipline",
                          "pair_id": pair_id, "items_with_value": prior_values[chosen], "pdfs_byte_identical": True}
            elif stage:
                chosen = stage["value"]
                source = {"class": "STAGE_DOCUMENT_METADATA_VIA_DISCIPLINE_REGISTRY", **stage}
            corroboration = {"stage_metadata": stage and {"code": stage["code"], "registry_name": stage["value"]},
                             "stage_metadata_codes_disagree_or_unknown": stage is None,
                             "byte_signature_matches": bool(source) and signature_matches(chosen, value)}
            if stage is None:
                codes = {}
                for side, st in (("left", "stage_1"), ("right", "stage_2")):
                    p = STAGE_DOCS / st / "documents" / pairs[pair_id][side]["document_code"] / "document.json"
                    codes[side] = json.loads(p.read_text(encoding="utf-8")).get("discipline")
                corroboration["stage_metadata_codes"] = codes
            elif source and source["class"] == "PRIOR_PRODUCTION_PRESENTATION" and stage["value"] != chosen:
                corroboration["stage_metadata_conflict"] = (
                    f"stage document.json code {stage['code']} ({stage['value']}) disagrees with the prior "
                    "production label of this pair; not used")
            if source:
                rules.append({"id": f"discipline:{pair_id}", "target": "item", "field": "discipline", "pair_id": pair_id,
                              "match": "EXACT", "corrupted": value, "status": "REPAIRED", "value": chosen,
                              "source": source, "corroboration": corroboration})
            decisions.append({"field": "discipline", "pair_id": pair_id, "items": count, "resolved": bool(source)})

    # review_question: the one production question of the same presentation namespace.
    for value, count in Counter(i["review_question"] for i in items if corrupted(i.get("review_question"))).items():
        values = prior["all"]["review_question"]
        ok = len(values) == 1
        chosen = next(iter(values)) if ok else None
        if ok:
            rules.append({"id": "review_question:*", "target": "item", "field": "review_question", "pair_id": "*",
                          "match": "EXACT", "corrupted": value, "status": "REPAIRED", "value": chosen,
                          "source": {"class": "PRIOR_PRODUCTION_PRESENTATION", "path": prior["path"],
                                     "sha256": prior["sha256"], "git_blob": prior["git_blob"],
                                     "json_path": "envelope.items[].review_question",
                                     "namespace": prior["namespace"], "items_with_value": values[chosen]},
                          "corroboration": {"byte_signature_matches": signature_matches(chosen, value)}})
        decisions.append({"field": "review_question", "pair_id": "*", "items": count, "resolved": ok})

    # review_explanation_ru: search every known source for an uncorrupted text.
    for value, count in Counter(i["review_explanation_ru"] for i in items if corrupted(i.get("review_explanation_ru"))).items():
        prior_matches = [v for v in prior["all"]["review_explanation_ru"] if signature_matches(v, value)]
        rules.append({"id": "review_explanation_ru:*", "target": "item", "field": "review_explanation_ru", "pair_id": "*",
                      "match": "EXACT", "corrupted": value, "status": "UNRESOLVED", "display_ru": UNRESOLVED_RU,
                      "searched_sources": [
                          {"source": f"{BUILDER} @ HEAD (literal is ASCII '?' bytes since {CORRUPTING_COMMIT})",
                           "result": "CORRUPTED"},
                          {"source": "git history: git log --all -S 'V3 production preview'",
                           "result": f"only {CORRUPTING_COMMIT}, already corrupted"},
                          {"source": "cursor_handoff_projectchange_v3_production_20260919_091858 (files/, git_diff.patch)",
                           "result": "CORRUPTED"},
                          {"source": prior["path"], "result": "text absent (73 values, none with this byte signature)",
                           "signature_matches": prior_matches},
                          {"source": str(RESEARCH_UI), "result": "field absent (research UI data has no review explanation)"},
                      ]})
        decisions.append({"field": "review_explanation_ru", "pair_id": "*", "items": count, "resolved": False})

    # MANIFEST.pdf_sanitization: only the corrupted pair labels are replaced (same label source as cipher).
    for path_key, values in before_manifest.items():
        for value in values:
            repaired = value
            used = []
            for pair_id in sorted(pairs):
                cipher_rule = next((r for r in rules if r["id"] == f"cipher:{pair_id}"), None)
                if cipher_rule and cipher_rule["corrupted"] in repaired:
                    repaired = repaired.replace(cipher_rule["corrupted"], cipher_rule["value"], 1)
                    used.append(cipher_rule["id"])
            ok = not corrupted(repaired)
            if ok:
                rules.append({"id": f"manifest:{path_key}", "target": "manifest", "field": path_key, "pair_id": "*",
                              "match": "EXACT", "corrupted": value, "status": "REPAIRED", "value": repaired,
                              "source": {"class": "PRODUCTION_PAIR_LABEL_REGISTRY", "composed_from_rules": used}})
            decisions.append({"field": f"MANIFEST.{path_key}", "pair_id": "*", "items": 1, "resolved": ok})

    repair = {
        "schema": adapter.REPAIR_SCHEMA,
        "repair_id": "pcv3-snapshot-cyrillic-1",
        "snapshot": rel(SNAPSHOT),
        "snapshot_manifest_sha256": manifest_sha,
        "snapshot_presentation_sha256": presentation_sha,
        "first_corrupted_layer": {"file": BUILDER, "commit": CORRUPTING_COMMIT,
                                  "evidence": "string literals are 0x3F bytes in the committed source"},
        "rules": rules,
    }
    served = adapter.adapt(copy.deepcopy(data["envelope"]), repair)
    served_manifest = adapter.repair_manifest({k: v for k, v in manifest.items() if k != "files"}, repair)
    after = scan(served)
    after_manifest = scan(served_manifest)
    unresolved_marked = sum(1 for i in served["items"] if i.get("presentation_repair"))
    per_pair_served = Counter(item_pair(i) for i in served["items"])

    fixture_scan = {rel(p): sum(sum(c.values()) for c in scan(json.loads(p.read_text(encoding="utf-8"))).values())
                    for p in sorted(HM_FIXTURES.glob("*.json"))}
    total_before = sum(sum(c.values()) for c in before_presentation.values()) + sum(sum(c.values()) for c in before_manifest.values())
    repaired_count = sum(d["items"] for d in decisions if d["resolved"])
    unresolved_count = sum(d["items"] for d in decisions if not d["resolved"])
    audit = {
        "schema": "cyrillic-presentation-audit/1",
        "model_calls": 0,
        "sealed_snapshot": {"path": rel(SNAPSHOT), "manifest_sha256": manifest_sha, "presentation_sha256": presentation_sha,
                            "modified": False},
        "detector": {"question_mark_run": QMARK_RUN.pattern, "mojibake_or_replacement": MOJIBAKE.pattern},
        "trace": [
            {"layer": "research source data (miner/final + research UI snapshot)", "state": "CLEAN",
             "evidence": {k: v["value"] for k, v in research.items()}},
            {"layer": f"{BUILDER} (source code, commit {CORRUPTING_COMMIT})", "state": "CORRUPTED — FIRST CORRUPTED LAYER",
             "evidence": "PAIRS cipher/discipline and review_* literals are ASCII '?' (0x3F)"},
            {"layer": "sealed snapshot presentation.json / MANIFEST.json", "state": "CORRUPTED (copied from builder)"},
            {"layer": "PreviewService.envelope → /api/stage-comparison/objects/4f3e5916/project-changes",
             "state": "CORRUPTED before this change (passed through as-is)"},
            {"layer": "browser", "state": "renders the '?' it receives (no client-side corruption)"},
        ],
        "scanned": {
            "presentation_strings": strings_scanned(data),
            "manifest_strings": strings_scanned({k: v for k, v in manifest.items() if k != "files"}),
            "fields_with_corruption_before": {k: sum(c.values()) for k, c in sorted(before_presentation.items())}
            | {f"MANIFEST.{k}": sum(c.values()) for k, c in before_manifest.items()},
            "human_mapping_fixture_corruption": fixture_scan,
        },
        "counts": {"corrupted_before": total_before, "repaired": repaired_count, "unresolved_marked": unresolved_count,
                   "corrupted_after_in_served_text": sum(sum(c.values()) for c in after.values())
                   + sum(sum(c.values()) for c in after_manifest.values()),
                   "items_with_unresolved_marker": unresolved_marked},
        "per_class": [{**d, "rule": next(({k: r[k] for k in r if k not in ("corrupted",)}
                                          for r in rules if r["field"] in (d["field"], d["field"].removeprefix("MANIFEST."))
                                          and r["pair_id"] == d["pair_id"]), None)} for d in decisions],
        "served_cards_per_pair": dict(per_pair_served),
        "sealed_cards_per_pair": dict(by_pair),
        "repair_file": rel(REPAIR),
    }
    assert audit["counts"]["corrupted_after_in_served_text"] == 0, after
    assert total_before == repaired_count + unresolved_count, (total_before, repaired_count, unresolved_count)
    if write:
        REPAIR.write_text(json.dumps(repair, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        audit["repair_file_sha256"] = sha_file(REPAIR)
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(audit["counts"], ensure_ascii=False), audit.get("repair_file_sha256", "(dry run)"))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    raise SystemExit(build(args.audit, args.write))
