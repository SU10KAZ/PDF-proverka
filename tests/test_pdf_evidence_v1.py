"""Controls for PDF Evidence V1.

Everything here runs on synthetic pages built in the test.  Nothing opens a
PDF, calls a model, or reads a project directory: the rules of the contract
have to hold on their own terms, not because one corpus happens to satisfy
them.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiments.pdf_evidence_v1 import (
    completeness,
    contract,
    decoding,
    geometry,
    layer as layer_module,
    regression,
    structure,
)
from experiments.pdf_evidence_v1.extraction import PageSource


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _span(index, text, bbox, size=10.0, font="ISOCPEUR", vertical=False,
          decoding_status=contract.DECODED_NATIVE):
    return {
        "index": index,
        "text": text,
        "bbox": list(bbox),
        "size": size,
        "font": font,
        "vertical": vertical,
        "decoding": decoding_status,
        "repaired_chars": 0,
    }


def _page(spans=(), segments=None, annotations=(), width=1000.0, height=800.0, page=1):
    spans = list(spans)
    lines = [
        {
            "index": index,
            "text": span["text"],
            "bbox": list(span["bbox"]),
            "size": span["size"],
            "font": span["font"],
            "vertical": span["vertical"],
            "decoding": span["decoding"],
            "repaired_chars": 0,
            "span_indices": [span["index"]],
        }
        for index, span in enumerate(spans)
    ]
    array = np.asarray(segments, dtype=np.float64) if segments is not None else np.zeros((0, 4))
    source = PageSource(
        page=page, rotation=0, width=width, height=height,
        spans=spans, lines=lines, annotations=list(annotations),
        geometry=geometry.compact(array, []),
    )
    return source


def _box(x0, y0, x1, y1):
    return [
        (x0, y0, x1, y0), (x1, y0, x1, y1), (x1, y1, x0, y1), (x0, y1, x0, y0),
    ]


def _lattice(x0, y0, x1, y1, rows, columns):
    segments = []
    for index in range(rows + 1):
        y = y0 + (y1 - y0) * index / rows
        segments.append((x0, y, x1, y))
    for index in range(columns + 1):
        x = x0 + (x1 - x0) * index / columns
        segments.append((x, y0, x, y1))
    return segments


# ---------------------------------------------------------------------------
# the contract
# ---------------------------------------------------------------------------


def test_the_claim_vocabulary_has_no_absence():
    assert contract.CLAIMS == ("POSITIVE_PRESENCE", "SUPPORT_ONLY")
    for term in contract.FORBIDDEN_CLAIM_TERMS:
        assert term not in contract.CLAIMS


def test_a_payload_naming_an_absence_is_refused():
    payload = {"pages": [{"page": 3, "status": "REMOVED"}]}
    with pytest.raises(contract.ContractViolation):
        contract.assert_no_absence_vocabulary(payload)
    assert contract.absence_vocabulary_violations(payload)[0]["value"] == "REMOVED"


def test_a_key_may_describe_reading_while_a_value_may_not_claim_absence():
    payload = {"pages_without_a_markdown_section": [25], "status": "INSUFFICIENT"}
    contract.assert_no_absence_vocabulary(payload)


def test_a_claim_outside_the_vocabulary_is_refused():
    with pytest.raises(contract.ContractViolation):
        contract.assert_closed_claims({"unit": {"claim": "PROBABLY_PRESENT"}})


def test_native_text_with_geometry_asserts_presence():
    assert contract.derive_claim(
        decoding=contract.DECODED_NATIVE, bbox=(0, 0, 10, 10), page=1,
        applicability="SHEET_SHARED", ownership=contract.STAMP_ZONE,
    ) == contract.POSITIVE_PRESENCE


def test_a_fragment_local_claim_needs_structural_ownership():
    assert contract.derive_claim(
        decoding=contract.DECODED_NATIVE, bbox=(0, 0, 10, 10), page=1,
        applicability="FRAGMENT_LOCAL", ownership=contract.NO_OWNERSHIP,
    ) == contract.SUPPORT_ONLY


def test_text_without_geometry_only_supports():
    assert contract.derive_claim(
        decoding=contract.DECODED_NATIVE, bbox=None, page=1,
        applicability="UNKNOWN", ownership=contract.NO_OWNERSHIP,
    ) == contract.SUPPORT_ONLY


def test_unresolved_decoding_only_supports():
    assert contract.derive_claim(
        decoding=contract.DECODED_CAD_UNRESOLVED, bbox=(0, 0, 10, 10), page=1,
        applicability="SHEET_SHARED", ownership=contract.STAMP_ZONE,
    ) == contract.SUPPORT_ONLY


def test_scope_discipline_refuses_a_fragment_claim_without_a_drawn_relation():
    unit = contract.EvidenceUnit(
        unit_id="u", document="d", page=1, provenance="NATIVE_PDF_TEXT",
        decoding=contract.DECODED_NATIVE, text="ВРУ-1", bbox=(0, 0, 10, 10),
        applicability="FRAGMENT_LOCAL", ownership=contract.NO_OWNERSHIP,
    )
    with pytest.raises(contract.ContractViolation):
        contract.assert_scope_discipline([unit])


def test_the_contract_document_carries_the_asymmetry():
    document = contract.contract_document()
    statements = " ".join(rule["statement"] for rule in document["rules"])
    assert "may never assert an absence" in statements
    assert "does not refute positive native evidence" in statements


# ---------------------------------------------------------------------------
# decoding
# ---------------------------------------------------------------------------


def test_the_corpus_constant_recovers_a_drawing_title():
    raw = "ǙǯǸǹǶǳǸǰǴǸǫȊ\x01ǻǫǼȂǰǽǸǫȊ\x01ǼȀǰǷǫ"
    repaired, moved = decoding.apply_shift(raw, decoding.CORPUS_CAD_SHIFT)
    assert repaired == "Однолинейная расчетная схема"
    assert moved > 0


def test_the_yield_optimal_shift_is_not_the_right_one():
    """565 scores as well as 581 and produces nothing a reader could use."""
    raw = "ǙǯǸǹǶǳǸǰǴǸǫȊ"
    assert decoding.apply_shift(raw, 565)[0] != "Однолинейная"
    assert decoding.apply_shift(raw, decoding.CORPUS_CAD_SHIFT)[0] == "Однолинейная"


def test_one_codepoint_never_identifies_a_displacement():
    """``Ʃ=60м`` is a cable total; its font must not be repaired into ``А=60м``."""
    profile = decoding.build_profile(
        [{"font": "ArialMT", "text": " Ʃ=60м", "page": 1} for _ in range(60)]
    )
    font = profile.font("ArialMT")
    assert font.distinct_codes == 1
    assert font.proven is False
    text, status, moved = profile.decode(" Ʃ=60м", "ArialMT")
    assert text == " Ʃ=60м"
    assert status == contract.DECODED_CAD_UNRESOLVED
    assert moved == 0


def test_a_covered_font_is_repaired():
    spans = [{"font": "ISOCPEUR", "text": "ǙǯǸǹǶǳǸǰǴǸǫȊ", "page": 1}]
    profile = decoding.build_profile(spans)
    assert profile.font("ISOCPEUR").proven is True
    text, status, moved = profile.decode("ǙǯǸǹǶǳǸǰǴǸǫȊ", "ISOCPEUR")
    assert text == "Однолинейная"
    assert status == contract.DECODED_CAD_REPAIRED
    assert moved == 12


def test_a_partly_covered_font_is_carried_by_an_independent_confirmation():
    """Coverage or confirmation — either may carry a font, nothing else may."""
    # Five distinct block codepoints spell ``Выпуск``; the sixth, ``Ⱦ``, is the
    # codepoint this subset maps outside the run, so coverage falls below the
    # gate while the repair is plainly right.
    spans = [{"font": "ISOCPEUR", "text": "ǍȆǺуǼǵ Ⱦ", "page": 7}] * 2
    refused = decoding.build_profile(spans)
    assert refused.font("ISOCPEUR").cyrillic_yield < decoding.MIN_CYRILLIC_YIELD
    assert refused.font("ISOCPEUR").proven is False
    confirmed = decoding.build_profile(spans, {7: "Выпуск водопроводного ввода"})
    assert confirmed.font("ISOCPEUR").markdown_confirmations >= 1
    assert confirmed.font("ISOCPEUR").proven is True


def test_undecodable_characters_are_never_repaired():
    profile = decoding.build_profile([{"font": "X", "text": "", "page": 1}])
    assert profile.decode("", "X")[1] == contract.UNDECODABLE


# ---------------------------------------------------------------------------
# geometry
# ---------------------------------------------------------------------------


def test_collinear_strokes_weld_into_one_edge():
    segments = np.array([[0, 0, 50, 0], [50, 0, 120, 0], [120, 0, 200, 0]], dtype=float)
    horizontal, vertical = geometry.axis_edges(segments)
    assert len(horizontal) == 1
    assert len(vertical) == 0
    assert horizontal[0].tolist() == [0.0, 0.0, 200.0, 0.0]


def test_a_slanted_stroke_is_never_welded_and_is_counted_as_unstructured():
    segments = np.array([[0, 0, 100, 100]], dtype=float)
    horizontal, vertical = geometry.axis_edges(segments)
    assert len(horizontal) == 0 and len(vertical) == 0
    assert geometry.slanted_ink_share(segments) == pytest.approx(1.0)


def test_compaction_reports_its_own_ratio():
    compact = geometry.compact(np.asarray(_box(0, 0, 100, 100), dtype=float), [])
    report = compact.compaction()
    assert report["raw_segments"] == 4
    assert report["welded_edges"] == 4
    assert report["compression"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# structure
# ---------------------------------------------------------------------------


def test_text_inside_a_drawn_box_is_owned_by_the_box():
    source = _page(
        spans=[_span(0, "ВРУ-1", (110, 110, 160, 125))],
        segments=_box(100, 100, 300, 200),
    )
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["ownership"] == contract.DIRECT_CONTAINMENT
    assert result["applicability"] == "FRAGMENT_LOCAL"


def test_a_page_sized_frame_never_owns_anything():
    source = _page(
        spans=[_span(0, "ВРУ-1", (110, 110, 160, 125))],
        segments=_box(5, 5, 995, 795),
    )
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["applicability"] == "UNKNOWN"
    assert result["ownership"] == contract.NO_OWNERSHIP


def test_a_lattice_cell_owns_its_text_and_reports_a_rectangle():
    source = _page(
        spans=[_span(0, "630А", (210, 210, 250, 225))],
        segments=_lattice(100, 100, 500, 400, rows=4, columns=4),
    )
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["ownership"] == contract.TABLE_CELL
    assert result["cell"] is not None
    assert result["cell_bbox"] is not None
    cells = structure.table_cells(regions)
    assert len(cells) == 16
    assert all(len(cell["bbox"]) == 4 for cell in cells)


def test_a_stroke_that_merely_passes_nearby_attributes_nothing():
    """The rule that separates a leader from the nearest stroke."""
    source = _page(
        spans=[_span(0, "ГРЩ1-РП1-3", (200, 200, 300, 212))],
        segments=[(150, 216, 160, 216), *_box(600, 600, 700, 700)],
    )
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["ownership"] == contract.NO_OWNERSHIP


def test_a_leader_drawn_along_the_label_attaches_it():
    source = _page(
        spans=[_span(0, "ГРЩ1-РП1-3", (200, 200, 300, 212))],
        segments=[
            (200, 214, 300, 214), (200, 214, 200, 260),
            (200, 260, 300, 260), (300, 214, 300, 260),
        ],
    )
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["ownership"] == contract.CONNECTED_CALLOUT
    assert result["applicability"] == "FRAGMENT_LOCAL"


def test_stamp_zone_text_stays_sheet_shared():
    source = _page(spans=[_span(0, "Корпус 3", (700, 730, 800, 745))])
    regions = structure.build_regions(source)
    index = structure.build_index(source, regions)
    result = structure.attribute(source, index, source.spans[0])
    assert result["ownership"] == contract.STAMP_ZONE
    assert result["applicability"] == "SHEET_SHARED"


# ---------------------------------------------------------------------------
# the layer
# ---------------------------------------------------------------------------


def test_spans_of_one_line_join_only_when_they_share_an_owner():
    """A cable mark joins; two table columns on one baseline do not."""
    source = _page(
        spans=[
            _span(0, "ГРЩ1-РП1-3", (210, 210, 260, 222)),
            _span(1, "5х150мм²", (266, 210, 300, 222)),
            _span(2, "630А", (410, 210, 440, 222)),
        ],
        segments=_lattice(200, 200, 600, 400, rows=4, columns=4),
    )
    source.lines = [{
        "index": 0,
        "text": "ГРЩ1-РП1-3 5х150мм² 630А",
        "bbox": [210, 210, 440, 222],
        "size": 10.0, "font": "ISOCPEUR", "vertical": False,  # one baseline
        "decoding": contract.DECODED_NATIVE, "repaired_chars": 0,
        "span_indices": [0, 1, 2],
    }]
    page = layer_module.build_page("d", source)
    texts = sorted(unit.text for unit in page.units)
    assert "ГРЩ1-РП1-3 5х150мм²" in texts
    assert "630А" in texts
    assert not any("630А" in text and "ГРЩ1" in text for text in texts)


def test_an_annotation_becomes_a_unit_with_its_own_rectangle():
    source = _page(
        annotations=[{
            "text": "Ip=314,43 А",
            "bbox": [210, 210, 260, 222],
            "annotation_type": "Square",
            "annotation_title": "AutoCAD SHX Text",
            "printed_by_the_drawing": True,
        }],
        segments=_box(200, 200, 300, 300),
    )
    page = layer_module.build_page("d", source)
    unit = page.units[0]
    assert unit.provenance == "NATIVE_PDF_ANNOTATION"
    assert unit.claim == contract.POSITIVE_PRESENCE
    assert unit.ownership == contract.DIRECT_CONTAINMENT
    assert "printed_by_the_drawing" in unit.notes


def test_a_reviewers_annotation_is_marked_as_not_printed():
    source = _page(annotations=[{
        "text": "проверил",
        "bbox": [210, 210, 260, 222],
        "annotation_type": "Stamp",
        "annotation_title": "bushmin",
        "printed_by_the_drawing": False,
    }])
    page = layer_module.build_page("d", source)
    assert "not_printed_by_the_drawing" in page.units[0].notes


def test_a_title_block_string_on_several_sheets_becomes_document_shared():
    pages = [
        layer_module.build_page("d", _page(
            spans=[_span(0, "АА-БЭ-03-ДС3-ИОС1.1", (700, 730, 900, 745))], page=number,
        ))
        for number in (1, 2, 3)
    ]
    promoted = layer_module.promote_document_shared(pages)
    assert promoted == 3
    assert all(page.units[0].applicability == "DOCUMENT_SHARED" for page in pages)


def test_a_title_block_string_on_one_sheet_stays_sheet_shared():
    pages = [layer_module.build_page("d", _page(
        spans=[_span(0, "План 3 этажа", (700, 730, 900, 745))], page=1,
    ))]
    assert layer_module.promote_document_shared(pages) == 0
    assert pages[0].units[0].applicability == "SHEET_SHARED"


def test_a_string_printed_outside_a_title_block_is_never_promoted():
    pages = [
        layer_module.build_page("d", _page(
            spans=[_span(0, "Корпус 3", (700, 730, 900, 745))], page=1,
        )),
        layer_module.build_page("d", _page(
            spans=[_span(0, "Корпус 3", (100, 100, 300, 115))], page=2,
        )),
    ]
    assert layer_module.promote_document_shared(pages) == 0


# ---------------------------------------------------------------------------
# completeness
# ---------------------------------------------------------------------------


def test_a_page_with_no_native_text_is_unknown_not_incomplete():
    page = layer_module.build_page("d", _page())
    row = completeness.page_completeness(page, "какой-то текст")
    assert row["status"] == completeness.UNKNOWN


def test_a_page_the_markdown_never_saw_is_insufficient():
    page = layer_module.build_page("d", _page(
        spans=[_span(0, "Однолинейная схема ГРЩ", (100, 100, 300, 115))]
    ))
    row = completeness.page_completeness(page, None)
    assert row["status"] == completeness.INSUFFICIENT
    assert row["reason"] == "page_has_no_markdown_section"


def test_a_fully_read_page_is_sufficient():
    page = layer_module.build_page("d", _page(
        spans=[_span(0, "Однолинейная схема ГРЩ", (100, 100, 300, 115))]
    ))
    row = completeness.page_completeness(page, "## Page 1\nОднолинейная схема ГРЩ")
    assert row["status"] == completeness.SUFFICIENT
    assert row["read_share"] == 1.0


def test_completeness_never_speaks_of_the_document():
    page = layer_module.build_page("d", _page(
        spans=[_span(0, "Однолинейная схема ГРЩ", (100, 100, 300, 115))]
    ))
    report = completeness.audit([(_fake_layer([page]), {})])
    contract.assert_no_absence_vocabulary(report)


class _fake_layer:
    def __init__(self, pages):
        self.document = "d"
        self.pages = pages


# ---------------------------------------------------------------------------
# regression
# ---------------------------------------------------------------------------


def test_the_producer_emits_no_absence_and_only_closed_claims():
    page = layer_module.build_page("d", _page(
        spans=[_span(0, "ВРУ-1", (110, 110, 160, 125))],
        segments=_box(100, 100, 300, 200),
    ))
    layer = _fake_layer([page])
    layer.units = page.units
    guards = regression.producer_guards({("p", "LEFT"): layer}, {"page": page.to_dict()})
    assert set(guards["claims_emitted"]) <= set(contract.CLAIMS)
    for control in guards["controls"]:
        assert control["observed"] == control["expected"], control["control"]


def test_the_forbidden_words_are_the_ones_the_defect_used():
    for term in ("REMOVED", "DELETED", "ABSENT"):
        assert term in contract.FORBIDDEN_CLAIM_TERMS


def test_structural_ownership_never_grows_a_proximity_channel():
    assert contract.STRUCTURAL_OWNERSHIP == frozenset({
        "TABLE_CELL", "DIRECT_CONTAINMENT", "CONNECTED_CALLOUT",
    })
    assert contract.STAMP_ZONE not in contract.STRUCTURAL_OWNERSHIP
    assert contract.NO_OWNERSHIP not in contract.STRUCTURAL_OWNERSHIP


def test_a_page_that_prints_nothing_yields_no_units():
    """Decision item 4: nothing found is not a finding.

    The layer has no way to say "this was not here": with no printed string
    there is simply no unit, and a consumer that wants an absence has to get it
    from somewhere the contract does not provide.
    """
    page = layer_module.build_page("d", _page(segments=_box(100, 100, 300, 200)))
    assert page.units == []
    assert page.regions  # the geometry was read; only the claims are absent


def test_markdown_silence_does_not_downgrade_native_presence():
    """Decision item 3, in the only place it can be enforced: the claim itself."""
    page = layer_module.build_page("d", _page(
        spans=[_span(0, "ГРЩ1-РП1-3 5х150мм²", (110, 110, 260, 125))],
        segments=_box(100, 100, 300, 200),
    ))
    unit = page.units[0]
    assert unit.claim == contract.POSITIVE_PRESENCE
    assert unit.applicability == "FRAGMENT_LOCAL"
    # Nothing in the derivation consults a recognized layer at all.
    assert contract.derive_claim(
        decoding=unit.decoding, bbox=unit.bbox, page=unit.page,
        applicability=unit.applicability, ownership=unit.ownership,
    ) == contract.POSITIVE_PRESENCE


def test_the_contract_passes_its_own_guards():
    """A dictionary of banned words is a declaration, not a claim.

    The first run of this audit failed both vocabulary guards on the contract
    document itself: it lists the forbidden terms, and it declares the allowed
    claims as a list under a ``claim_semantics`` key.  A guard that reads its
    own schema as data reports itself.
    """
    payload = {"contract": contract.contract_document()}
    contract.assert_no_absence_vocabulary(payload, ignore_paths=contract.DECLARATION_PATHS)
    contract.assert_closed_claims(payload)


def test_the_exclusion_is_one_path_and_not_a_licence():
    """The declaration exclusion must not let an absence through anywhere else."""
    payload = {
        "contract": contract.contract_document(),
        "layer": {"units": [{"claim": "POSITIVE_PRESENCE", "status": "REMOVED"}]},
    }
    violations = contract.absence_vocabulary_violations(
        payload, ignore_paths=contract.DECLARATION_PATHS
    )
    assert [row["path"] for row in violations] == ["$.layer.units[0].status"]
