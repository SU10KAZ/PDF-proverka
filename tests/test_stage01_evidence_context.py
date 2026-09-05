from backend.app.pipeline.stages.block_analysis.document_retrieval import (
    retrieve_document_context,
    retrieve_targeted_document_context,
)
from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import (
    gate_findings,
)
import json

from backend.app.pipeline.stages.block_grounding.block_source_router import (
    _discipline_hint,
)
from backend.app.pipeline.stages.block_grounding.architecture_geometry import (
    PROFILE_FURNITURE,
    PROFILE_WALL_ELEVATION,
    classify_ar_profile,
)
from backend.app.services.text_analysis.document_type_detector import (
    detect_document_type,
)


def _finding(**overrides):
    base = {
        "severity": "ЭКСПЛУАТАЦИОННОЕ",
        "category": "door_marking",
        "finding": "Марка Д16 имеет противоречащий ведомости предел EI.",
        "norm_quote": None,
        "value_found": "Д16 EI 30",
        "recommendation": "Согласовать одно значение.",
        "claim_type": "contradiction",
        "problem_class": "door_fire_rating_mismatch",
        "affected_entity": "дверь Д16",
        "evidence_quote": "Д16 EI 30 / Д16 EI 60",
        "evidence_kind": "document_retrieval",
        "context_status": "sufficient",
        "confidence": 0.93,
        "counterevidence_checked": True,
        "required_context": [],
    }
    base.update(overrides)
    return base


def test_evidence_gate_publishes_proved_and_defers_context_gap():
    proved = _finding()
    context_gap = _finding(
        finding="Требуется проверить наличие сертификата.",
        claim_type="context_gap",
        context_status="external_only",
        confidence=0.55,
        counterevidence_checked=False,
        evidence_kind="none",
        evidence_quote="",
        required_context=["сертификат изделия"],
    )

    published, deferred, report = gate_findings([proved, context_gap])

    assert len(published) == 1
    assert published[0]["affected_entity"] == "дверь Д16"
    assert len(deferred) == 1
    assert "insufficient_context" in deferred[0]["_evidence_gate"]["reasons"]
    assert report["published"] == 1
    assert report["deferred"] == 1


def test_evidence_gate_deduplicates_same_problem_and_entity():
    first = _finding(confidence=0.91)
    stronger = _finding(confidence=0.97, finding="Д16: EI 30 против EI 60.")

    published, deferred, report = gate_findings([first, stronger])

    assert len(published) == 1
    assert published[0]["confidence"] == 0.97
    assert len(deferred) == 1
    assert report["reason_counts"]["block_duplicate"] == 1


def test_document_retrieval_finds_mark_on_other_sheet_and_has_receipt():
    graph = {
        "pages": [
            {"page": 1, "text_blocks": [{"text": "План с дверью Д16"}]},
            {
                "page": 7,
                "sheet_no": "6",
                "text_blocks": [{"text": "Ведомость дверей: марка Д16, EI 60, доводчик."}],
            },
            {"page": 8, "text_blocks": [{"text": "Общие указания без нужной марки."}]},
        ]
    }

    text, receipt = retrieve_document_context(graph, "Дверь Д16 EI 30", 1)

    assert "Д16" in text
    assert "EI 60" in text
    assert receipt["status"] == "hits"
    assert receipt["selected_pages"] == [7]


def test_targeted_document_retrieval_adds_bounded_discipline_vocabulary():
    graph = {
        "pages": [
            {"page": 1, "text_blocks": [{"text": "Фрагмент вводного щита"}]},
            {
                "page": 4,
                "text_blocks": [
                    {
                        "text": (
                            "Ведомость аппаратов: трансформатор тока ТТ1; "
                            "расчётный ток Ip 142 А; спецификация щита."
                        )
                    }
                ],
            },
        ]
    }

    text, receipt = retrieve_targeted_document_context(
        graph,
        "Вводной щит ТТ1",
        1,
        discipline="EOM",
    )

    assert "Ip 142 А" in text
    assert receipt["profile"] == "discipline_targeted_v3"
    assert receipt["discipline"] == "EOM"
    assert "трансформатор тока" in receipt["discipline_terms"]
    assert receipt["selected_pages"] == [4]


def test_ai_storage_path_routes_to_architecture_profile():
    path = "/tmp/projects_v2/objects/O/disciplines/AI/documents/D/versions/v1/out"
    assert _discipline_hint(path) == "АР"


def test_ai_experiment_copy_uses_project_info_discipline(tmp_path):
    version = tmp_path / "work_version"
    output = version / "03_analysis" / "out"
    output.mkdir(parents=True)
    info_dir = version / "01_input"
    info_dir.mkdir()
    (info_dir / "project_info.json").write_text(
        json.dumps({"section": "AI"}), encoding="utf-8"
    )

    assert _discipline_hint(output) == "АР"


def test_wall_elevation_wins_over_incidental_door_and_scheme_words():
    description = (
        "Схема содержит развертки стен помещений 19 и 20, дверной проем Д12 "
        "и габаритные размеры 580, 580 и 1155."
    )
    assert classify_ar_profile(description) == PROFILE_WALL_ELEVATION


def test_rd_filename_beats_specification_tables_in_content():
    doc_type, confidence = detect_document_type(
        {"pdf_file": "1141-КИС-РД-М-АИ-П_V1.pdf", "section": "AI"},
        "# Спецификация\n| поз. | дверь | сечение |",
    )
    assert doc_type == "full_rd"
    assert confidence == 0.80


def test_furniture_plan_wins_over_incidental_door_openings():
    description = (
        "Фрагмент плана расстановки мебели. Показаны M-17, M-18 и дверные проемы."
    )
    assert classify_ar_profile(description) == PROFILE_FURNITURE


def test_evidence_gate_defers_internal_metadata_comparison_and_vague_count():
    metadata = _finding(
        finding="Тип фрагмента в переданной текстовой разметке противоречит чертежу.",
        recommendation="Исправить разметку и индексацию.",
    )
    vague = _finding(
        finding="Количество извещателей не совпадает со спецификацией.",
        evidence_quote="На плане показано явно больше 3 точек.",
    )

    published, deferred, report = gate_findings([metadata, vague])

    assert published == []
    reasons = {reason for item in deferred for reason in item["_evidence_gate"]["reasons"]}
    assert "internal_metadata_comparison" in reasons
    assert "unquantified_visual_count" in reasons
    assert report["deferred"] == 2
