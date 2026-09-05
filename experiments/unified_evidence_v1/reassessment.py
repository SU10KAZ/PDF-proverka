"""Lineage reassessment without a model — §6, read-only over the evidence profiles.

The selector is not run, no prompt exists here, and no candidate is scored.
The questions are about the *evidence*: does the new layer give any candidate
positive facts the old channels did not, does it tell same-scope candidates
apart, and does it ever do so by suppressing a legitimate candidate rather than
by supporting one.  Where the corpus carries a research reference, the
referenced candidate is looked at as a hypothesis, never as truth.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .profiles import CERTIFIED_FUNCTION_FACTS_BOTH_SIDES


def _new_evidence(candidate: Mapping[str, Any]) -> int:
    """Shared facts the old channels could not have produced.

    Old channels are the passport's declared values.  New evidence is a shared
    fact certified on both sides, or one the native layer corroborates on both
    sides — the two things this line of research built.
    """
    return int(candidate["shared_certified_both_sides"]) + int(candidate["shared_natively_corroborated_both_sides"])


def reassess(profiles: Mapping[str, Any]) -> dict[str, Any]:
    rows = profiles["rows"]
    per_relation: dict[str, Counter] = defaultdict(Counter)
    per_derivability: dict[str, Counter] = defaultdict(Counter)
    totals: Counter = Counter()
    identity: Counter = Counter()
    references: Counter = Counter()
    suppression: Counter = Counter()
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for task in rows:
        candidates = task["candidates"]
        totals["tasks"] += 1
        totals["candidates"] += len(candidates)
        supported = [c for c in candidates if c["shared_count"] > 0]
        certified_supported = [c for c in candidates if c["shared_certified_both_sides"] > 0]
        new_supported = [c for c in candidates if _new_evidence(c) > 0]
        contradicted = [c for c in candidates if c["explicit_contradictions"]]
        totals["tasks_with_a_supported_candidate"] += int(bool(supported))
        totals["tasks_with_a_certified_shared_fact"] += int(bool(certified_supported))
        totals["tasks_with_new_evidence_on_a_candidate"] += int(bool(new_supported))
        totals["tasks_with_an_explicit_contradiction"] += int(bool(contradicted))
        totals["candidates_with_an_explicit_contradiction"] += len(contradicted)

        # same-scope ambiguity: candidates whose shared profiles are identical
        profiles_seen: dict[str, list[str]] = defaultdict(list)
        for c in candidates:
            signature = "|".join(sorted(f"{item['field']}={item['value']}" for item in c["shared_positive_facts"]))
            profiles_seen[signature].append(c["candidate_id"])
        tied = [ids for ids in profiles_seen.values() if len(ids) > 1]
        if len(candidates) > 1:
            totals["multi_candidate_tasks"] += 1
            totals["multi_candidate_tasks_with_identical_profiles"] += int(bool(tied))
            # positive-only discrimination: exactly one candidate carries new evidence
            if len(new_supported) == 1:
                totals["multi_candidate_tasks_discriminated_by_new_evidence_alone"] += 1
            if len(supported) == 1:
                totals["multi_candidate_tasks_with_exactly_one_supported_candidate"] += 1

        # identity facts on both sides
        marks_left = {item["value"] for c in candidates for item in c["shared_positive_facts"] if item["field"] == "board_mark"}
        both_marks = any(
            "board_mark" in c["positive_facts_left"] and "board_mark" in c["positive_facts_right"] for c in candidates)
        if both_marks:
            identity["tasks_with_a_mark_on_both_sides"] += 1
            if marks_left:
                identity["tasks_where_the_marks_agree_for_some_candidate"] += 1
            if any(any(x["field"] == "board_mark" for x in c["explicit_contradictions"]) for c in candidates):
                identity["tasks_where_the_marks_disagree_for_some_candidate"] += 1

        # research references as hypotheses
        reference_ids = set(task.get("reference_candidate_ids") or [])
        if reference_ids:
            references["tasks_with_a_reference"] += 1
            referenced = [c for c in candidates if c["candidate_id"] in reference_ids]
            others = [c for c in candidates if c["candidate_id"] not in reference_ids]
            if any(c["shared_count"] > 0 for c in referenced):
                references["reference_supported_by_a_shared_fact"] += 1
            if any(_new_evidence(c) > 0 for c in referenced):
                references["reference_supported_by_new_evidence"] += 1
            if any(c["explicit_contradictions"] for c in referenced):
                references["reference_explicitly_contradicted"] += 1
                examples["reference_contradicted"].append({
                    "task_id": task["task_id"],
                    "contradictions": [c["explicit_contradictions"] for c in referenced][:2],
                })
            if others and any(_new_evidence(c) > 0 for c in others) and not any(_new_evidence(c) > 0 for c in referenced):
                references["new_evidence_favours_a_non_reference_candidate"] += 1
                examples["new_evidence_elsewhere"].append({"task_id": task["task_id"]})
            best_other = max((c["shared_count"] for c in others), default=0)
            best_ref = max((c["shared_count"] for c in referenced), default=0)
            if best_ref > best_other:
                references["reference_has_strictly_more_shared_facts"] += 1
            elif best_ref == best_other and best_ref > 0:
                references["reference_ties_with_another_candidate"] += 1

        # would a consumer that counted shared facts suppress a legitimate candidate?
        # legitimate here = plausible per the frozen generator; we only count ties.
        if len(candidates) > 1 and supported:
            top = max(c["shared_count"] for c in candidates)
            at_top = [c for c in candidates if c["shared_count"] == top]
            suppression["tasks_where_several_candidates_tie_at_the_top"] += int(len(at_top) > 1)
            suppression["tasks_where_one_candidate_leads_on_declared_facts_only"] += int(
                len(at_top) == 1 and _new_evidence(at_top[0]) == 0)

        for relation in task["relation_types"]:
            per_relation[relation]["tasks"] += 1
            per_relation[relation]["with_a_supported_candidate"] += int(bool(supported))
            per_relation[relation]["with_new_evidence"] += int(bool(new_supported))
            per_relation[relation]["with_an_explicit_contradiction"] += int(bool(contradicted))
        for derivability in task["group_derivability_classes"] or ["UNCLASSIFIED"]:
            per_derivability[derivability]["tasks"] += 1
            per_derivability[derivability]["with_a_supported_candidate"] += int(bool(supported))
            per_derivability[derivability]["with_new_evidence"] += int(bool(new_supported))

    both = profiles["by_coverage_class"].get(CERTIFIED_FUNCTION_FACTS_BOTH_SIDES, 0)
    return {
        "read_only": True,
        "selector_changed": False,
        "prompt_changed": False,
        "candidate_generator_changed": False,
        "totals": dict(sorted(totals.items())),
        "identity": dict(sorted(identity.items())),
        "references": dict(sorted(references.items())),
        "tie_and_suppression": dict(sorted(suppression.items())),
        "by_relation_type": {key: dict(sorted(value.items())) for key, value in sorted(per_relation.items())},
        "by_group_derivability": {key: dict(sorted(value.items())) for key, value in sorted(per_derivability.items())},
        "certified_function_facts_both_sides": both,
        "answer": {
            "question": (
                "does the new evidence improve the correct candidate without artificially "
                "suppressing other legitimate candidates?"
            ),
            "tasks_where_new_evidence_singles_out_one_candidate": totals[
                "multi_candidate_tasks_discriminated_by_new_evidence_alone"],
            "tasks_where_new_evidence_reaches_any_candidate": totals["tasks_with_new_evidence_on_a_candidate"],
            "tasks_where_certified_facts_meet_on_both_sides": both,
            "reading": (
                "the new layer never removes a candidate; where it singles one out it does so "
                "by a positive fact the others do not carry, and the count above says how often "
                "that happens on this corpus"
            ),
        },
        "examples": {key: value[:8] for key, value in examples.items()},
    }


__all__ = ["reassess"]
