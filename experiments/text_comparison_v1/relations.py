"""Deterministic section relations, including conservative split/merge and abstention."""
from collections import Counter, defaultdict
import itertools
import re

from .common import digest, norm, section_text, similarity, words


def clauses(section):
    # Order, page wrapping and Markdown styling carry no engineering identity.
    return Counter(x.strip(" .;!?") for x in re.split(r"[.;!?]+\s+|\n\s*[-•]\s+", norm(section_text(section))) if x.strip(" .;!?"))


def parent_path(s):
    return [(p["number"], p["normalized_title"]) for p in s["heading_path"][:-1]]


def evidence(a, b):
    title = similarity(a["normalized_title"], b["normalized_title"])
    content = similarity(section_text(a), section_text(b))
    same_number = a["section_number"] is not None and a["section_number"] == b["section_number"]
    same_title = a["normalized_title"] == b["normalized_title"]
    hierarchy = parent_path(a) == parent_path(b)
    codes = []
    if same_number:
        codes.append("SAME_NUMBER")
    if same_title:
        codes.append("SAME_NORMALIZED_TITLE")
    if hierarchy:
        codes.append("SAME_PARENT_PATH")
    exact_content = bool(section_text(a).strip()) and clauses(a) == clauses(b)
    if exact_content:
        codes.append("SAME_CONTENT_CLAUSES")
    marks = sorted(set(re.findall(r"\b[А-ЯA-Z]{1,8}[-.]?\d+(?:[.-]\d+)*\b", section_text(a))) &
                   set(re.findall(r"\b[А-ЯA-Z]{1,8}[-.]?\d+(?:[.-]\d+)*\b", section_text(b))))
    if marks:
        codes.append("SHARED_ENGINEERING_MARKS")
    strong = ((same_number and hierarchy and title >= .65) or
              (same_title and hierarchy) or (same_title and content >= .7) or
              (exact_content and len(section_text(a)) >= 100 and title >= .3))
    if "NON_UNIQUE_SEMANTIC_KEY" in a["review_reasons"] + b["review_reasons"]:
        strong = False
    plausible = strong or same_number or title >= .3 or content >= .55 or bool(marks and title >= .15)
    return {"strong": strong, "plausible": plausible, "codes": codes, "title_similarity": round(title, 6),
            "content_similarity": round(content, 6), "shared_marks": marks,
            "old_heading_ref": a["source_refs"][0], "new_heading_ref": b["source_refs"][0]}


def relate(old, new, *, old_complete=True, new_complete=True):
    edges = {(i, j): evidence(a, b) for i, a in enumerate(old) for j, b in enumerate(new)}
    used_old, used_new, result = set(), set(), []

    def add(kind, oi, nj, status, reasons, direction=None):
        a, b = [old[i]["instance_id"] for i in oi], [new[j]["instance_id"] for j in nj]
        result.append({"relation_id": "rel_" + digest([a, b, kind])[:24], "kind": kind, "status": status,
                       "old_sections": a, "new_sections": b, "direction": direction,
                       "evidence_codes": reasons,
                       "evidence": [edges[i, j] for i in oi for j in nj], "producer_version": "section-relation-v1.0.0"})
        used_old.update(oi)
        used_new.update(nj)

    # Exact multiset content conservation supplies a narrow, testable split/merge
    # proof. Ambiguous overlapping hypotheses remain REVIEW as a whole.
    proposals = []
    for reverse in (False, True):
        singles, multiples = (new, old) if reverse else (old, new)
        for i, single in enumerate(singles):
            target = clauses(single)
            if not target or len(section_text(single)) < 80:
                continue
            candidates = []
            for j, other in enumerate(multiples):
                edge = edges[j, i] if reverse else edges[i, j]
                part = clauses(other)
                # At least one whole clause contributed by every member.
                if edge["plausible"] and part and not (part - target):
                    candidates.append(j)
            if len(candidates) > 6:
                continue
            for n in range(2, min(4, len(candidates)) + 1):
                for js in itertools.combinations(candidates, n):
                    combined = sum((clauses(multiples[j]) for j in js), Counter())
                    if combined == target:
                        proposals.append((list(js), [i]) if reverse else ([i], list(js)))
    for oi, nj in proposals:
        conflicts = sum(bool(set(oi) & set(a) or set(nj) & set(b)) for a, b in proposals)
        if conflicts == 1 and not (set(oi) & used_old or set(nj) & used_new):
            add("N_TO_ONE" if len(oi) > 1 else "ONE_TO_N", oi, nj, "PROVEN", ["EXACT_CLAUSE_MULTISET_CONSERVATION"])
    strong_old, strong_new = defaultdict(list), defaultdict(list)
    for (i, j), edge in edges.items():
        if i not in used_old and j not in used_new and edge["strong"]:
            strong_old[i].append(j)
            strong_new[j].append(i)
    for i in sorted(strong_old):
        js = strong_old[i]
        if len(js) == 1 and len(strong_new[js[0]]) == 1:
            j = js[0]
            add("ONE_TO_ONE", [i], [j], "PROVEN", edges[i, j]["codes"])
    # Remaining plausible edges form components. No greedy forced bijection.
    todo_old, todo_new = set(range(len(old))) - used_old, set(range(len(new))) - used_new
    while todo_old or todo_new:
        oi, nj = ({min(todo_old)}, set()) if todo_old else (set(), {min(todo_new)})
        while True:
            a = oi | {i for i in todo_old if any(edges[i, j]["plausible"] for j in nj)}
            b = nj | {j for j in todo_new if any(edges[i, j]["plausible"] for i in oi)}
            if a == oi and b == nj:
                break
            oi, nj = a, b
        todo_old -= oi
        todo_new -= nj
        absent = not oi or not nj
        complete = old_complete if not oi else new_complete
        proven_single = all(s["status"] == "PROVEN" for s in
                            ([old[i] for i in oi] + [new[j] for j in nj]))
        if absent and complete and proven_single:
            add("NO_RELATION", sorted(oi), sorted(nj), "PROVEN", ["NO_PLAUSIBLE_SECTION_IN_COMPLETE_OPPOSITE_TEXT"],
                "ADDED" if not oi else "REMOVED")
        else:
            add("REVIEW", sorted(oi), sorted(nj), "REVIEW",
                ["AMBIGUOUS_RELATION" if not absent else "OPPOSITE_COVERAGE_OR_OWNERSHIP_UNCERTAIN"])
    return result


AI_PROMPT_VERSION = "section-relation-local-v1"
AI_LABELS = {"SAME_SECTION", "RELATED_BUT_DIFFERENT", "NO_RELATION", "UNSURE"}


def ai_package(old, candidates, max_chars=16000):
    """Only unresolved local candidates; caller must never auto-batch a document."""
    if len(old) != 1 or not 1 <= len(candidates) <= 4:
        return None
    def item(s):
        return {"instance_id": s["instance_id"], "heading_path": s["heading_path"],
                "document_version": s["document_version"], "text_blocks": s["ordered_text_blocks"]}
    payload = {"prompt_version": AI_PROMPT_VERSION, "old": [item(s) for s in old],
               "new_candidates": [item(s) for s in candidates], "allowed_labels": sorted(AI_LABELS),
               "instruction": "Return label and exact quotes with side, section_id and source_ref; unsure stays review."}
    if sum(len(section_text(s)) for s in old + candidates) > max_chars:
        return None
    return payload


def validate_ai(package, response, model):
    """Untrusted model evidence cannot establish a deterministic relation proof."""
    valid = response.get("label") in AI_LABELS and bool(response.get("evidence"))
    witnessed = set()
    for ev in response.get("evidence", []):
        side = ev.get("side")
        sections = package["old"] if side == "OLD" else package["new_candidates"] if side == "NEW" else []
        found = False
        for s in sections:
            for b in s["text_blocks"]:
                if (s["instance_id"] == ev.get("section_id") and ev.get("quote")
                        and ev["quote"] in b["text"] and ev.get("source_ref") in b["source_refs"]):
                    found = True
        valid &= found
        if found:
            witnessed.add(side)
    valid &= witnessed == {"OLD", "NEW"}
    return {"input_hash": digest(package), "model": model, "prompt_version": AI_PROMPT_VERSION,
            "output_hash": digest(response), "evidence": response.get("evidence", []),
            "label": response.get("label") if valid else "UNSURE", "status": "REVIEW",
            "reason": "AI_EVIDENCE_REQUIRES_REVIEW" if valid else "INVALID_OR_UNGROUNDED_AI_RESPONSE"}
