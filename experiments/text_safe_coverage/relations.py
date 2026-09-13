"""Fail-closed relation proof: exact identity or exact clause conservation.

Fuzzy similarities retrieve review candidates only. Identity and scope closure
are independently recorded, and neither key collisions nor unknown owners can
be accepted as an automatic relation. No absence inference from missing edges.
"""
from experiments.text_comparison_v1.relations import relate as proposals
from experiments.text_comparison_v1.relations import parent_path, clauses
from experiments.text_comparison_v1.common import digest, norm, section_text


def relate(old, new, *, old_complete=True, new_complete=True):
    result = proposals(old, new, old_complete=old_complete, new_complete=new_complete)
    aa = {s["instance_id"]: s for s in old}
    bb = {s["instance_id"]: s for s in new}
    for rel in result:
        a = [aa[k] for k in rel["old_sections"]]
        b = [bb[k] for k in rel["new_sections"]]
        rel["proposal_kind"] = rel["kind"]
        complete = all(s["status"] == "PROVEN" and s["source_refs"] for s in a + b)
        rel["scope_status"] = "PROVEN" if complete else "REVIEW"
        reasons = []
        if not complete:
            reasons.append("SEMANTIC_SCOPE_OWNERSHIP_UNRESOLVED")
        if rel["kind"] == "ONE_TO_ONE":
            x, y = a[0], b[0]
            exact_identity = (x["section_key"] == y["section_key"]
                              and parent_path(x) == parent_path(y)
                              and x["section_number"] == y["section_number"]
                              and x["normalized_title"] == y["normalized_title"])
            exact_body = (bool(norm(section_text(x))) and clauses(x) == clauses(y)
                          and x["normalized_title"] == y["normalized_title"])
            if not (exact_identity or exact_body):
                reasons.append("FUZZY_SIMILARITY_IS_NOT_RELATION_PROOF")
        if rel["kind"] == "NO_RELATION":
            # No matching extraction/heading is not positive evidence that a
            # semantic section or fact was removed. A search receipt alone is
            # insufficient without a closed corresponding scope witness.
            reasons.append("NO_POSITIVE_ABSENCE_WITNESS")
        if reasons and rel["status"] == "PROVEN":
            rel["status"], rel["kind"], rel["direction"] = "REVIEW", "REVIEW", None
            rel["evidence_codes"] = sorted(set(rel["evidence_codes"] + reasons))
            rel["relation_id"] = "rel_" + digest([rel["old_sections"], rel["new_sections"], "REVIEW"])[:24]
        rel["producer_version"] = "text-safe-relation-v1"
    return result
