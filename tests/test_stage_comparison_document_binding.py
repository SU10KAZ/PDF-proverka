"""G2.4.4.2 — graphic blocks must be provably bound to the pair's documents.

The corpus used here is real: the ИОС pair ``p26c08b83a6`` and the AR pair
``p570d156f57`` from session ``121d764109184c13``, with the SYSTEM_GRAPH blocks
of the ГРЩ single-line drawing.  Tests that need the live document store skip
when it is not installed, exactly like the other real-corpus suites.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.unified_entity_bridge import (
    build_graphic_coverage,
    build_scope_join,
    build_side_entity_links,
    build_side_graph_entities,
    build_text_entities,
    compare_document_identity,
    document_descriptor_for_block,
    normalize_graphic_scope_groups,
    pair_documents_from_pair_artifact,
    produce_graphic_scope_groups,
    verify_document_binding,
)
from backend.app.services.stage_comparison.unified_entity_bridge.document_binding import (
    BINDING_MISMATCH,
    BINDING_PROVEN,
    BINDING_UNPROVEN,
    DocumentBindingValidationError,
    PROVENANCE_ABSENT,
    PROVENANCE_ARTIFACT,
    normalize_document_descriptor,
)

ROOT = Path(__file__).resolve().parents[1]
SESSION = ROOT / "comparison/sessions/121d764109184c13/pairs"
DENSE = ROOT / "experiments/g2_dense_sectioned_board"
IOS = ROOT / "experiments/g2_4_4_scope_side_coverage/ios"
STORE = ROOT / (
    "projects_v2/objects/272_Sadovnicheskaya_76_Balchug_Esteyt/comparison"
)
LEFT_EXTRACT = STORE / "stage_1/documents/Страница_52_из_АА_БЭ-03-ДС3-ИОС1.1"
RIGHT_EXTRACT = (
    STORE / "stage_2/documents/Страница_21_из_АА-БЭ-03-ДС3-ИОС1.1_—_копия"
)


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _require(*paths: Path) -> None:
    for path in paths:
        if not path.exists():
            pytest.skip("real comparison corpus is not installed")


def _descriptor(
    code: str,
    path: str | None = None,
    *,
    version_id: str = "v001",
    storage_identity: str | None = None,
) -> dict:
    return normalize_document_descriptor(
        {
            "document_code": code,
            "version_id": version_id,
            "storage_identity": storage_identity,
            "source_path": path,
            "provenance": PROVENANCE_ARTIFACT,
        },
        "test descriptor",
    )


def _group(ledger, comparison, left=None, right=None):
    raw = {"ledger": ledger, "comparison_result": comparison}
    if left is not None:
        raw["left_document"] = left
    if right is not None:
        raw["right_document"] = right
    return produce_graphic_scope_groups([raw])


def _build(stage53, groups, left_graph, right_graph, pair_documents):
    text = build_text_entities(stage53, None)
    graphs = build_side_graph_entities(
        left_graphs=[left_graph] if left_graph else [],
        right_graphs=[right_graph] if right_graph else [],
    )
    links = build_side_entity_links(
        text,
        graphs,
        current_stage53_artifact=stage53,
        current_text_evidence_index=None,
        current_system_graphs={
            "LEFT": [left_graph] if left_graph else [],
            "RIGHT": [right_graph] if right_graph else [],
        },
    )
    scope_join = build_scope_join(
        stage53,
        text,
        graphs,
        groups,
        current_text_evidence_index=None,
        pair_documents=pair_documents,
    )
    coverage = build_graphic_coverage(stage53, text, graphs, links, scope_join, groups)
    return scope_join, coverage


@pytest.fixture(scope="module")
def ios_corpus():
    _require(
        SESSION / "p26c08b83a6/high_level_project_changes.json",
        DENSE / "left_system_graph.json",
        IOS / "comparison_result.json",
    )
    return {
        "stage53": _read(SESSION / "p26c08b83a6/high_level_project_changes.json"),
        "pair": _read(SESSION / "p26c08b83a6/pair.json"),
        "left_graph": _read(DENSE / "left_system_graph.json"),
        "right_graph": _read(DENSE / "right_system_graph.json"),
        "comparison": _read(IOS / "comparison_result.json"),
        "ledger": _read(IOS / "graphic_change_ledger.json"),
    }


@pytest.fixture(scope="module")
def block_owners(ios_corpus):
    """Resolve each ledger block to the document that really contains it."""
    _require(
        LEFT_EXTRACT / "versions/v001/02_work/blocks.json",
        RIGHT_EXTRACT / "versions/v001/02_work/blocks.json",
    )
    scope = ios_corpus["ledger"]["comparison_scope"]
    left_block = scope["left_blocks"][0]["block_id"]
    right_block = scope["right_blocks"][0]["block_id"]
    owners = {}
    for document in (LEFT_EXTRACT, RIGHT_EXTRACT):
        payload = _read(document / "versions/v001/02_work/blocks.json")
        ids = {item["block_id"] for item in payload["blocks"]}
        for block in (left_block, right_block):
            if block in ids:
                owners[block] = (payload, document.name, document)
    if len(owners) != 2:
        pytest.skip("ledger blocks are not resolvable in the live document store")
    return {"left_block": left_block, "right_block": right_block, "owners": owners}


def _owner_descriptor(block_owners, block_id):
    payload, code, document = block_owners["owners"][block_id]
    return document_descriptor_for_block(
        payload,
        block_id,
        document_code=code,
        version_id="v001",
        source_path=str(document / "versions/v001/02_work/document.pdf"),
    )


# --------------------------------------------------------------------------
# Pure verdict function
# --------------------------------------------------------------------------


def test_binding_proven_when_every_block_matches_its_pair_document():
    groups = [
        {
            "block_pairs": [
                {
                    "left": {"document": _descriptor("DOC_L")},
                    "right": {"document": _descriptor("DOC_R")},
                }
            ]
        }
    ]
    result = verify_document_binding(
        {"LEFT": _descriptor("DOC_L"), "RIGHT": _descriptor("DOC_R")}, groups
    )

    assert result["state"] == BINDING_PROVEN
    assert result["sides"]["LEFT"]["observed_documents"] == [_descriptor("DOC_L")]


def test_same_document_code_and_same_version_is_proven():
    state, reasons = compare_document_identity(
        _descriptor("DOC", version_id="v017"),
        _descriptor("DOC", version_id="v017"),
    )

    assert state == BINDING_PROVEN
    assert reasons == ["document_and_version_identity_equal"]


def test_same_document_code_different_version_is_not_same_identity():
    state, reasons = compare_document_identity(
        _descriptor("DOC", version_id="v017"),
        _descriptor("DOC", version_id="v018"),
    )

    assert state == BINDING_MISMATCH
    assert reasons == ["version_id_differs"]


def test_source_path_is_not_semantic_identity():
    state, _ = compare_document_identity(
        _descriptor("DOC", "/old/location.pdf"),
        _descriptor("DOC", "/new/location.pdf"),
    )

    assert state == BINDING_PROVEN


def test_different_identity_channels_do_not_prove_a_match():
    state, reasons = compare_document_identity(
        _descriptor("DOC", version_id="v001"),
        _descriptor(
            "IGNORED",
            version_id="v001",
            storage_identity="stored-document-1",
        )
        | {"document_code": None},
    )

    assert state == BINDING_UNPROVEN
    assert reasons == ["document_identity_not_comparable"]


def test_binding_mismatch_is_not_the_same_as_unproven():
    groups = [
        {
            "block_pairs": [
                {
                    "left": {"document": _descriptor("FOREIGN")},
                    "right": {"document": _descriptor("DOC_R")},
                }
            ]
        }
    ]
    mismatch = verify_document_binding(
        {"LEFT": _descriptor("DOC_L"), "RIGHT": _descriptor("DOC_R")}, groups
    )
    unproven = verify_document_binding(None, groups)

    assert mismatch["state"] == BINDING_MISMATCH
    assert unproven["state"] == BINDING_UNPROVEN
    assert mismatch["state"] != unproven["state"]


def test_missing_document_identity_is_unproven_never_mismatch():
    groups = [{"block_pairs": [{"left": {}, "right": {}}]}]

    result = verify_document_binding(
        {"LEFT": _descriptor("DOC_L"), "RIGHT": _descriptor("DOC_R")}, groups
    )

    assert result["state"] == BINDING_UNPROVEN
    assert "left:block_document_version_identity_incomplete" in result["reason_codes"]


def test_no_graphic_groups_is_unproven_not_mismatch():
    result = verify_document_binding(
        {"LEFT": _descriptor("DOC_L"), "RIGHT": _descriptor("DOC_R")}, []
    )

    assert result["state"] == BINDING_UNPROVEN
    assert "no_graphic_scope_groups" in result["reason_codes"]


def test_verdict_is_independent_of_block_pair_order():
    def group(order):
        pairs = [
            {
                "left": {"document": _descriptor(f"DOC_L{i}")},
                "right": {"document": _descriptor(f"DOC_R{i}")},
            }
            for i in order
        ]
        return [{"block_pairs": pairs}]

    documents = {"LEFT": _descriptor("DOC_L1"), "RIGHT": _descriptor("DOC_R1")}
    forward = verify_document_binding(documents, group([1, 2, 3]))
    backward = verify_document_binding(documents, group([3, 2, 1]))
    shuffled = verify_document_binding(documents, group([2, 1, 3]))

    canonical = json.dumps(forward, ensure_ascii=False, sort_keys=True)
    assert canonical == json.dumps(backward, ensure_ascii=False, sort_keys=True)
    assert canonical == json.dumps(shuffled, ensure_ascii=False, sort_keys=True)


def test_descriptor_rejects_code_without_provenance_and_unknown_fields():
    with pytest.raises(DocumentBindingValidationError):
        normalize_document_descriptor(
            {
                "document_code": "A",
                "version_id": "v001",
                "provenance": PROVENANCE_ABSENT,
            },
            "where",
        )
    with pytest.raises(DocumentBindingValidationError):
        normalize_document_descriptor({"document_code": "A", "nope": 1}, "where")


def test_block_descriptor_refuses_a_block_absent_from_the_index():
    payload = {"blocks": [{"block_id": "blk_a"}]}

    with pytest.raises(DocumentBindingValidationError):
        document_descriptor_for_block(payload, "blk_b", document_code="DOC")


def test_pair_artifact_must_describe_the_same_pair_as_stage53():
    pair = {"id": "pOTHER", "left": {"document_code": "A"}, "right": {"document_code": "B"}}

    with pytest.raises(DocumentBindingValidationError):
        pair_documents_from_pair_artifact(pair, {"pair_id": "p26c08b83a6"})


# --------------------------------------------------------------------------
# Real corpus, end to end
# --------------------------------------------------------------------------


def test_ios_binding_proven_against_the_documents_that_own_the_blocks(
    ios_corpus, block_owners
):
    left = _owner_descriptor(block_owners, block_owners["left_block"])
    right = _owner_descriptor(block_owners, block_owners["right_block"])
    groups = _group(ios_corpus["ledger"], ios_corpus["comparison"], left, right)

    scope_join, coverage = _build(
        ios_corpus["stage53"],
        groups,
        ios_corpus["left_graph"],
        ios_corpus["right_graph"],
        {"LEFT": left, "RIGHT": right},
    )

    assert scope_join["document_binding"]["state"] == BINDING_PROVEN
    # A proven binding changes nothing about what was actually checked.
    assert coverage["summary"]["by_state"] == {
        "CHECKED": 76,
        "CHECK_BLOCKED": 0,
        "NOT_APPLICABLE": 1785,
        "NOT_CHECKED": 995,
    }


def test_ios_foreign_document_yields_mismatch_and_no_checked_record(
    ios_corpus, block_owners
):
    left = _owner_descriptor(block_owners, block_owners["left_block"])
    right = _owner_descriptor(block_owners, block_owners["right_block"])
    groups = _group(ios_corpus["ledger"], ios_corpus["comparison"], left, right)

    # The pair's own documents are the full ИОС1.1 volumes, which do not
    # contain these single-page extract blocks at all.
    scope_join, coverage = _build(
        ios_corpus["stage53"],
        groups,
        ios_corpus["left_graph"],
        ios_corpus["right_graph"],
        pair_documents_from_pair_artifact(ios_corpus["pair"], ios_corpus["stage53"]),
    )

    assert scope_join["document_binding"]["state"] == BINDING_MISMATCH
    assert not [item for item in scope_join["scopes"] if item["status"] == "RESOLVED"]
    assert any(
        "graphic_page_identity_unresolved" in item["reason_codes"]
        for item in scope_join["scopes"]
    )
    assert coverage["summary"]["by_state"]["CHECKED"] == 0
    assert not [item for item in coverage["coverage"] if item["state"] == "CHECKED"]
    assert any(
        "document_binding_mismatch" in item["reason_codes"]
        for item in coverage["scope_processing"]
    )


def test_ios_without_descriptors_is_unproven_and_forbids_checked_coverage(ios_corpus):
    groups = _group(ios_corpus["ledger"], ios_corpus["comparison"])

    scope_join, coverage = _build(
        ios_corpus["stage53"],
        groups,
        ios_corpus["left_graph"],
        ios_corpus["right_graph"],
        None,
    )

    assert scope_join["document_binding"]["state"] == BINDING_UNPROVEN
    assert coverage["summary"]["by_state"]["CHECKED"] == 0
    assert any(
        "document_binding_unproven" in item["reason_codes"]
        for item in coverage["scope_processing"]
    )


def test_ar_pair_without_any_graphic_is_unproven_and_unchanged():
    _require(SESSION / "p570d156f57/high_level_project_changes.json")
    stage53 = _read(SESSION / "p570d156f57/high_level_project_changes.json")

    scope_join, coverage = _build(stage53, [], None, None, None)

    assert scope_join["document_binding"]["state"] == BINDING_UNPROVEN
    assert "no_graphic_scope_groups" in scope_join["document_binding"]["reason_codes"]
    assert coverage["summary"]["by_state"] == {
        "CHECKED": 0,
        "CHECK_BLOCKED": 0,
        "NOT_APPLICABLE": 1020,
        "NOT_CHECKED": 612,
    }


def test_pair_without_descriptors_keeps_its_identifiers_unchanged(
    ios_corpus, block_owners
):
    """Enrichment is additive: an unenriched pair must keep byte-identical ids."""
    plain = normalize_graphic_scope_groups(
        _group(ios_corpus["ledger"], ios_corpus["comparison"])
    )
    enriched = normalize_graphic_scope_groups(
        _group(
            ios_corpus["ledger"],
            ios_corpus["comparison"],
            _owner_descriptor(block_owners, block_owners["left_block"]),
            _owner_descriptor(block_owners, block_owners["right_block"]),
        )
    )

    assert "document" not in plain[0]["block_pairs"][0]["left"]
    assert "document" in enriched[0]["block_pairs"][0]["left"]
    # The block pair reference is keyed on blocks and evidence, never on the
    # document descriptor, so adding provenance never renumbers a pair.
    assert (
        plain[0]["block_pairs"][0]["block_pair_ref"]
        == enriched[0]["block_pairs"][0]["block_pair_ref"]
    )


def test_binding_verdict_is_byte_identical_across_repeated_builds(
    ios_corpus, block_owners
):
    left = _owner_descriptor(block_owners, block_owners["left_block"])
    right = _owner_descriptor(block_owners, block_owners["right_block"])
    documents = {"LEFT": left, "RIGHT": right}
    groups = _group(ios_corpus["ledger"], ios_corpus["comparison"], left, right)

    first, _ = _build(
        ios_corpus["stage53"],
        groups,
        ios_corpus["left_graph"],
        ios_corpus["right_graph"],
        documents,
    )
    second, _ = _build(
        ios_corpus["stage53"],
        groups,
        ios_corpus["left_graph"],
        ios_corpus["right_graph"],
        documents,
    )

    assert json.dumps(first, ensure_ascii=False, sort_keys=True) == json.dumps(
        second, ensure_ascii=False, sort_keys=True
    )
