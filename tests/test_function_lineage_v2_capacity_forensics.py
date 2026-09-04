from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.function_lineage_v2 import capacity_forensics as forensics


@pytest.fixture(scope="module")
def artifact() -> dict:
    return forensics.build()


def test_forensics_makes_no_model_calls(artifact: dict) -> None:
    assert artifact["model_calls"] == 0
    assert artifact["schema_version"] == forensics.SCHEMA_VERSION


def test_replay_is_byte_identical() -> None:
    first = forensics.build()
    second = forensics.build()

    assert forensics._json_bytes(first) == forensics._json_bytes(second)


def test_every_reported_v2_5_conflict_is_reconstructed(artifact: dict) -> None:
    reported = artifact["reported_v2_5_safety"]

    assert reported["FUNCTION_FRAGMENT_CONFLICT"] == 9
    assert reported["RIGHT_MAP_CONFLICT"] == 0
    assert artifact["observed"]["unique_conflicts"] == 9
    assert artifact["observed"]["observation_repeats"] == 22
    assert all(
        row["verdict"]["root_cause_class"] in forensics.ROOT_CAUSE_CLASSES
        for row in artifact["observed"]["conflicts"]
    )


def test_all_observed_conflicts_are_true_incompatible_reuse(artifact: dict) -> None:
    observed = artifact["observed"]

    assert observed["true_conflicts"] == 9
    assert observed["false_conflicts"] == 0
    for row in observed["conflicts"]:
        verdict = row["verdict"]
        assert verdict["root_cause_class"] == "A_TRUE_FUNCTION_FRAGMENT_CONFLICT"
        assert verdict["scope_relationship"] == "UNRELATED"
        assert verdict["certified_exact_union_parents"] == []
        assert not set(verdict["left_fragment_ids_for_key"]).intersection(
            verdict["right_fragment_ids_for_key"]
        )


def test_no_observed_conflict_is_a_hierarchical_or_duplicate_artifact(artifact: dict) -> None:
    counts = artifact["observed"]["counts"]

    assert counts["B_HIERARCHICAL_DUPLICATE"] == 0
    assert counts["B_LICENSED_EXACT_CHILD_UNION"] == 0
    assert counts["C_TASK_DUPLICATION"] == 0
    assert counts["G_UNKNOWN"] == 0


def test_latent_false_conflicts_exist_and_are_confined_to_proven_classes(artifact: dict) -> None:
    latent = artifact["latent"]
    by_relation = latent["counts_by_scope_relation"]

    # The realisable false-conflict surface the current accounting would reject.
    assert latent["counts"]["B_LICENSED_EXACT_CHILD_UNION"] == 117
    assert latent["counts"]["B_HIERARCHICAL_DUPLICATE"] == 251
    assert latent["counts"]["D_FRAGMENTATION_DEFECT"] == 105
    assert latent["counts"]["C_TASK_DUPLICATION"] == 0
    assert latent["counts"]["G_UNKNOWN"] == 0
    # Unrelated scopes never produce a hierarchical duplicate, and nested
    # scopes never produce a true conflict.
    assert by_relation["UNRELATED"]["B_HIERARCHICAL_DUPLICATE"] == 0
    assert by_relation["PARENT_CHILD"]["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"] == 0
    # Overlapping scopes can assert incompatible merge arities ({A,B} -> R
    # against {B,C} -> R). That is a true conflict, not a fragmentation defect.
    assert by_relation["OVERLAP"]["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"] == 46
    assert by_relation["OVERLAP"]["D_FRAGMENTATION_DEFECT"] == 3
    assert by_relation["PARENT_CHILD"]["D_FRAGMENTATION_DEFECT"] == 102


def test_exact_child_union_exposure_is_measured(artifact: dict) -> None:
    exposure = artifact["exact_child_union_exposure"]

    assert exposure["certified_exact_child_union_groups"] == 120
    assert exposure["groups_exposed_to_false_sibling_conflict"] == 117
    assert exposure["unrelated_sibling_pairs_sharing_capacity"] == 117


def test_classifier_and_capacity_rule_agree_on_the_whole_population(artifact: dict) -> None:
    """The A class must be exactly what the capacity rule rejects."""
    import itertools

    from backend.app.services.stage_comparison import function_lineage_shadow as lineage
    from experiments.function_lineage_v2 import stratified

    latent = artifact["latent"]
    licensed = latent["classified_collisions"] - latent["counts"][
        "A_TRUE_FUNCTION_FRAGMENT_CONFLICT"
    ]

    sources = forensics.load_sources()
    evaluation = forensics.load_evaluation()
    rejected = 0
    granted = 0
    for corpus in stratified.CORPUS_ORDER:
        pair_id = stratified.PROJECT_PAIRS[corpus]
        candidates = sources["raw_candidates"][pair_id]
        licences = lineage.exact_child_union_licences(candidates)
        rows = sorted(
            (value for value in evaluation["population"]["tasks"]
             if value["corpus"] == corpus),
            key=lambda value: str(value["task_id"]),
        )
        keys = {
            str(value): frozenset(candidates[str(value)]["right_capacity_keys"])
            for row in rows for value in row["candidate_ids"]
            if str(value) in candidates
        }
        for left_row, right_row in itertools.combinations(rows, 2):
            for left in sorted(left_row["candidate_ids"]):
                if str(left) not in keys:
                    continue
                for right in sorted(right_row["candidate_ids"]):
                    if str(left) == str(right) or str(right) not in keys:
                        continue
                    shared = keys[str(left)] & keys[str(right)]
                    if not shared:
                        continue
                    errors = lineage.verify_capacity(
                        [{"candidate_id": str(left)}, {"candidate_id": str(right)}],
                        candidates, licences=licences,
                    )
                    for key in shared:
                        if any(key in error for error in errors):
                            rejected += 1
                        else:
                            granted += 1

    assert rejected == latent["counts"]["A_TRUE_FUNCTION_FRAGMENT_CONFLICT"]
    assert granted == licensed


def test_root_causes_are_ranked_by_impact(artifact: dict) -> None:
    ranked = artifact["ranked_root_causes"]

    assert [row["rank"] for row in ranked] == [1, 2, 3]
    assert ranked[0]["class"] == "B_LICENSED_EXACT_CHILD_UNION"
    assert all(row["statement"] for row in ranked)
