import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.findings.rejected_audit_service as service
import backend.scripts.audit_rejected_findings_codex as audit_cli


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _minimal_context(
    *,
    text: str = "",
    finding_text: str = "",
    images: list[dict] | None = None,
) -> dict:
    images = list(images or [])
    block_ids = [str(row.get("block_id") or "") for row in images]
    text_excerpt = "\n\n---\n\n".join(
        value for value in (finding_text, text) if value
    )
    return {
        "route": (
            "mixed"
            if images and text_excerpt
            else "graphic"
            if images
            else "text"
            if text_excerpt
            else "missing"
        ),
        "images": images,
        "blocks": [],
        "text_excerpt": text_excerpt,
        "document_text_excerpt": text,
        "finding_evidence_text": finding_text,
        "document_text_path": "",
        "norm_context": {},
        "graphic_block_ids": block_ids,
        "text_block_ids": [],
        "source_block_ids": block_ids,
        "source_block_count": len(block_ids),
        "images_truncated": False,
        "context_error": "",
    }


def _write_project(
    root: Path,
    decisions: list[dict],
    findings: list[dict],
    reviewer: str = "Эксперт",
) -> tuple[Path, Path]:
    object_dir = root / "objects" / "object-folder"
    document_dir = object_dir / "disciplines" / "AR" / "documents" / "DOC-1"
    version_dir = document_dir / "versions" / "v001"
    review_path = version_dir / "04_review" / "expert_review.json"

    _write_json(
        object_dir / "object.json",
        {"object_id": "object-1", "display_name": "Тестовый объект"},
    )
    _write_json(
        document_dir / "document.json",
        {"document_code": "DOC-1"},
    )
    _write_json(
        version_dir / "03_analysis" / "latest" / "03_findings.json",
        {"findings": findings},
    )
    _write_json(
        review_path,
        {
            "project_id": "DOC-1",
            "reviewer": reviewer,
            "decisions": decisions,
        },
    )
    return version_dir, review_path


def _case(
    case_id: str,
    *,
    input_hash: str | None = None,
    reason: str = "Эксперт проверил исходный документ",
    problem: str = "Исходное замечание",
    text: str = "",
    images: list[dict] | None = None,
) -> dict:
    return {
        "case_id": case_id,
        "input_hash": input_hash or f"hash-{case_id}",
        "source_quality": "same_version_artifact",
        "decision_origin": "human",
        "expert_reason": reason,
        "finding": {"id": "F-001", "problem": problem},
        "context": _minimal_context(
            text=text,
            finding_text=problem,
            images=images,
        ),
        "object_id": "object-1",
        "object_name": "Тестовый объект",
        "discipline": "AR",
        "document": "DOC-1",
        "project_id": "DOC-1",
        "version_id": "v001",
        "item_id": "F-001",
        "expert_timestamp": "2026-07-10T09:00:00+00:00",
    }


def _raw_review(
    case: dict,
    *,
    verdict: str,
    binding_status: str = "exact",
    factual_verdict: str = "unsupported",
    report_value: str = "remove",
    reason_quality: str = "substantiated",
    integrity_flags: list[str] | None = None,
    evidence: list[dict] | None = None,
    reviewed_sources: list[str] | None = None,
    missing_context: list[str] | None = None,
    decision_effect: str | None = None,
    rejection_basis: str = "factual",
    practical_impact: str | None = None,
    source_alignment: str = "not_visual",
    scope_context_status: str = "not_needed",
) -> dict:
    normalized_evidence: list[dict] = []
    for item in evidence or []:
        row = dict(item)
        source = str(row.get("source") or "")
        row.setdefault(
            "source_id",
            "primary"
            if source == "document_text"
            else f"graphic:{row.get('block_id')}"
            if source == "graphic_block"
            else f"text:{row.get('block_id')}"
            if source == "text_block"
            else "",
        )
        row.setdefault(
            "observation_basis",
            "raster"
            if source == "graphic_block"
            else "pdf_text_layer"
            if source in {"text_block", "document_text", "related_document"}
            else "derived",
        )
        row.setdefault("verification_state", "corroborated")
        row.setdefault("claim_type", "other")
        row.setdefault("absence_scope", "none")
        normalized_evidence.append(row)
    if decision_effect is None:
        decision_effect = {
            "expert_correct": "supports_rejection",
            "expert_may_be_wrong": "changes_rejection",
        }.get(verdict, "unclear")
    if practical_impact is None:
        practical_impact = "none" if report_value == "remove" else "medium"
    return {
        "case_id": case["case_id"],
        "verdict": verdict,
        "confidence": "high",
        "binding_status": binding_status,
        "factual_verdict": factual_verdict,
        "report_value": report_value,
        "reason_quality": reason_quality,
        "decision_effect": decision_effect,
        "rejection_basis": rejection_basis,
        "practical_impact": practical_impact,
        "impact_assessment": "Практическое влияние проверено для тестового кейса",
        "source_alignment": source_alignment,
        "scope_context_status": scope_context_status,
        "integrity_flags": list(integrity_flags or []),
        "reason_assessment": "Проверена причина эксперта",
        "finding_assessment": "Проверено исходное замечание",
        "norm_assessment": "Нормативный контекст не требуется",
        "decisive_evidence": normalized_evidence,
        "reviewed_sources": list(reviewed_sources or []),
        "missing_context": list(missing_context or []),
        "recommended_action": "manual_recheck",
    }


def _mismatch_review(case: dict) -> dict:
    finding_quote = str(case["finding"]["problem"])
    reason_quote = str(case["expert_reason"])
    return _raw_review(
        case,
        verdict="expert_may_be_wrong",
        binding_status="conflict",
        factual_verdict="supported",
        report_value="include",
        reason_quality="contradicted",
        integrity_flags=["reason_item_mismatch"],
        evidence=[
            {
                "source": "finding",
                "image_index": 0,
                "block_id": "",
                "locator": "finding.problem",
                "quote": finding_quote,
                "implication": "Фиксирует предмет текущего замечания",
            },
            {
                "source": "expert_reason",
                "image_index": 0,
                "block_id": "",
                "locator": "expert_reason",
                "quote": reason_quote,
                "implication": "Причина относится к другому предмету",
            },
        ],
        reviewed_sources=["finding", "expert_reason"],
    )


def test_collect_rejected_cases_uses_moscow_half_open_month(tmp_path, monkeypatch):
    root = tmp_path / "projects_v2"
    decisions = [
        {
            "item_id": "F-001",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "До начала июля",
            "timestamp": "2026-06-30T20:59:59Z",
        },
        {
            "item_id": "F-002",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Ровно начало июля",
            "timestamp": "2026-06-30T21:00:00Z",
        },
        {
            "item_id": "F-003",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Последняя секунда июля",
            "timestamp": "2026-07-31T20:59:59Z",
        },
        {
            "item_id": "F-004",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Ровно начало августа",
            "timestamp": "2026-07-31T21:00:00Z",
        },
    ]
    findings = [
        {"id": f"F-{number:03d}", "problem": f"Проблема {number}"}
        for number in range(1, 5)
    ]
    _write_project(root, decisions, findings)
    monkeypatch.setattr(
        service,
        "build_case_context",
        lambda *_args, **_kwargs: _minimal_context(text="Контекст документа"),
    )

    cases, inventory = service.collect_rejected_cases(
        month="2026-07",
        projects_v2_root=root,
        timezone_name="Europe/Moscow",
    )

    assert [case["item_id"] for case in cases] == ["F-002", "F-003"]
    assert inventory["interval"] == {
        "from": "2026-07-01T00:00:00+03:00",
        "to_exclusive": "2026-08-01T00:00:00+03:00",
        "from_utc": "2026-06-30T21:00:00+00:00",
        "to_exclusive_utc": "2026-07-31T21:00:00+00:00",
    }
    assert inventory["counts"]["outside_period"] == 2
    assert inventory["counts"]["selected_cases"] == 2
    assert inventory["audit_contract_version"] == service.AUDIT_CONTRACT_VERSION


def test_collect_rejected_cases_accepts_an_inclusive_date_slice(tmp_path, monkeypatch):
    root = tmp_path / "projects_v2"
    decisions = [
        {
            "item_id": "F-001",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "До среза",
            "timestamp": "2026-08-09T20:59:59Z",
        },
        {
            "item_id": "F-002",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Начало среза",
            "timestamp": "2026-08-09T21:00:00Z",
        },
        {
            "item_id": "F-003",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Последняя секунда среза",
            "timestamp": "2026-08-16T20:59:59Z",
        },
        {
            "item_id": "F-004",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "После среза",
            "timestamp": "2026-08-16T21:00:00Z",
        },
    ]
    findings = [
        {"id": f"F-{number:03d}", "problem": f"Проблема {number}"}
        for number in range(1, 5)
    ]
    _write_project(root, decisions, findings)
    monkeypatch.setattr(
        service,
        "build_case_context",
        lambda *_args, **_kwargs: _minimal_context(text="Контекст документа"),
    )

    cases, inventory = service.collect_rejected_cases(
        month="2026-08",
        projects_v2_root=root,
        timezone_name="Europe/Moscow",
        date_from="2026-08-10",
        date_to="2026-08-16",
    )

    assert [case["item_id"] for case in cases] == ["F-002", "F-003"]
    assert inventory["interval"] == {
        "from": "2026-08-10T00:00:00+03:00",
        "to_exclusive": "2026-08-17T00:00:00+03:00",
        "from_utc": "2026-08-09T21:00:00+00:00",
        "to_exclusive_utc": "2026-08-16T21:00:00+00:00",
    }
    assert inventory["filters"]["date_from"] == "2026-08-10"
    assert inventory["filters"]["date_to"] == "2026-08-16"


def test_collect_rejected_cases_uses_only_canonical_reviews_and_exclusions(
    tmp_path,
    monkeypatch,
):
    root = tmp_path / "projects_v2"
    july = "2026-07-10T09:00:00Z"
    decisions = [
        {"item_id": "F-001", "item_type": "finding", "decision": "rejected", "rejection_reason": "Ручное решение", "timestamp": july},
        {"item_id": "OPT-001", "item_type": "optimization", "decision": "rejected", "rejection_reason": "Оптимизация", "timestamp": july},
        {"item_id": "F-002", "item_type": "finding", "decision": "rejected", "rejection_reason": "Автоперенос", "timestamp": july, "carried_over": True},
        {"item_id": "F-003", "item_type": "finding", "decision": "accepted", "timestamp": july},
        {"item_id": "F-004", "item_type": "note", "decision": "rejected", "rejection_reason": "Неизвестный тип", "timestamp": july},
        {"item_type": "finding", "decision": "rejected", "rejection_reason": "Нет ID", "timestamp": july},
        {"item_id": "F-005", "item_type": "finding", "decision": "rejected", "rejection_reason": "Нет даты"},
        {"item_id": "F-006", "item_type": "finding", "decision": "rejected", "rejection_reason": "Июнь", "timestamp": "2026-06-10T09:00:00Z"},
        {"item_id": "F-007", "item_type": "finding", "decision": "rejected", "rejection_reason": "↩ Сверьте с текущим и примите решение", "timestamp": july},
        {"item_id": "F-008", "item_type": "finding", "decision": "rejected", "rejection_reason": None, "timestamp": july},
    ]
    findings = [
        {"id": item_id, "problem": f"Проблема {item_id}"}
        for item_id in ("F-001", "F-007", "F-008")
    ]
    version_dir, _review_path = _write_project(root, decisions, findings)
    _write_json(
        version_dir / "_output" / "expert_review.json",
        {
            "decisions": [
                {
                    "item_id": "F-999",
                    "item_type": "finding",
                    "decision": "rejected",
                    "rejection_reason": "Не канонический файл",
                    "timestamp": july,
                }
            ]
        },
    )
    monkeypatch.setattr(
        service,
        "build_case_context",
        lambda *_args, **_kwargs: _minimal_context(text="Контекст документа"),
    )

    cases, inventory = service.collect_rejected_cases(
        month="2026-07",
        projects_v2_root=root,
    )

    assert [case["item_id"] for case in cases] == ["F-001", "F-007", "F-008"]
    assert {case["item_id"]: case["decision_origin"] for case in cases} == {
        "F-001": "human",
        "F-007": "suspected_carryover",
        "F-008": "human",
    }
    counts = inventory["counts"]
    assert counts["review_files_scanned"] == 1
    assert counts["rejected_decisions_seen"] == 9
    assert counts["excluded_optimizations"] == 1
    assert counts["excluded_carried_over"] == 1
    assert counts["excluded_unknown_item_type"] == 1
    assert counts["excluded_missing_item_id"] == 1
    assert counts["excluded_missing_timestamp"] == 1
    assert counts["outside_period"] == 1
    assert counts["suspected_carryover_reason"] == 1
    assert counts["missing_expert_reason"] == 1
    assert counts["source_item_found"] == 3
    assert counts["selected_cases"] == 3


def test_collect_rejected_cases_filters_by_reviewer(tmp_path, monkeypatch):
    root = tmp_path / "projects_v2"
    july = "2026-07-10T09:00:00Z"
    decisions = [
        {
            "item_id": "F-001",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Решение рецензента из файла",
            "timestamp": july,
        },
        {
            "item_id": "F-002",
            "item_type": "finding",
            "decision": "rejected",
            "rejection_reason": "Решение другого рецензента",
            "timestamp": july,
            "reviewer": "Другой эксперт",
        },
    ]
    findings = [
        {"id": "F-001", "problem": "Проблема 1"},
        {"id": "F-002", "problem": "Проблема 2"},
    ]
    _write_project(root, decisions, findings, reviewer="Кульдяев Ф. С.")
    monkeypatch.setattr(
        service,
        "build_case_context",
        lambda *_args, **_kwargs: _minimal_context(text="Контекст документа"),
    )

    cases, inventory = service.collect_rejected_cases(
        month="2026-07",
        projects_v2_root=root,
        reviewers={"Кульдяев Ф. С."},
    )

    assert [case["item_id"] for case in cases] == ["F-001"]
    assert cases[0]["expert_reviewer"] == "Кульдяев Ф. С."
    assert inventory["filters"]["reviewers"] == ["Кульдяев Ф. С."]
    assert inventory["counts"]["excluded_reviewer"] == 1
    assert inventory["by_reviewer"] == {"Кульдяев Ф. С.": 1}


def test_load_exact_source_item_prefers_primary_and_flags_verified_conflict(
    tmp_path,
):
    version_dir = tmp_path / "versions" / "v001"
    output_dir = version_dir / "03_analysis" / "latest"
    primary = {
        "id": "F-001",
        "problem": "В основном finding отсутствует аварийное освещение",
        "sheet": "АР-12",
        "page": 12,
        "category": "Безопасность",
        "primary_only": True,
    }
    verified = {
        "id": "F-001",
        "problem": "В verified finding отсутствует рабочее освещение",
        "sheet": "АР-12",
        "page": 12,
        "category": "Безопасность",
        "verification_status": "checked",
    }
    _write_json(output_dir / "03_findings.json", {"findings": [primary]})
    _write_json(
        output_dir / "03a_norms_verified.json",
        {"findings": [verified]},
    )

    item, source_path, resolved_output_dir, source_quality = (
        service.load_exact_source_item(version_dir, "F-001")
    )

    assert item == primary
    assert item != verified
    assert source_path == output_dir / "03_findings.json"
    assert resolved_output_dir == output_dir
    assert source_quality == "same_version_artifact_conflict"


def test_image_index_deduplicates_paths_and_rejects_cross_case_block(tmp_path):
    shared = tmp_path / "shared.png"
    second = tmp_path / "second.png"
    shared.write_bytes(b"shared")
    second.write_bytes(b"second")
    case_a = _case(
        "case-a",
        images=[
            {"path": str(shared), "block_id": "block-A", "page": 1},
            {"path": str(second), "block_id": "block-A2", "page": 2},
        ],
    )
    case_b = _case(
        "case-b",
        images=[{"path": str(shared), "block_id": "block-B", "page": 3}],
    )

    paths, aligned = service.align_batch_images([case_a, case_b])

    assert paths == [str(shared.resolve()), str(second.resolve())]
    assert [(row["image_index"], row["block_id"]) for row in aligned["case-a"]] == [
        (1, "block-A"),
        (2, "block-A2"),
    ]
    assert [(row["image_index"], row["block_id"]) for row in aligned["case-b"]] == [
        (1, "block-B"),
    ]
    assert aligned["case-a"][0]["asset_role"] == "crop"
    assert aligned["case-a"][0]["source_id"] == "graphic:block-A"
    assert aligned["case-a"][0]["sha256"] == service._file_sha256(shared)

    wrong_evidence = {
        "source": "graphic_block",
        "image_index": 1,
        "block_id": "block-A",
        "locator": "изображение 1",
        "quote": "видимый фрагмент",
        "implication": "Попытка сослаться на блок другого кейса",
    }
    normalized, errors = service.normalize_batch_output(
        [case_b],
        {"reviews": [_raw_review(case_b, verdict="expert_correct", evidence=[wrong_evidence], reviewed_sources=["graphic_block"])]},
        image_alignment=aligned,
    )
    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["decisive_evidence"] == []
    assert any("not attached to case-b" in value for value in normalized[0]["guard_adjustments"])

    valid_evidence = {**wrong_evidence, "block_id": "block-B"}
    normalized, errors = service.normalize_batch_output(
        [case_b],
        {"reviews": [_raw_review(case_b, verdict="expert_correct", evidence=[valid_evidence], reviewed_sources=["graphic_block"])]},
        image_alignment=aligned,
    )
    assert errors == []
    assert normalized[0]["verdict"] == "expert_correct"
    assert normalized[0]["decisive_evidence"][0]["block_id"] == "block-B"


def test_fabricated_document_quote_is_rejected_and_verdict_downgraded():
    case = _case("case-text", text="В документе указана итоговая площадь 61,7 м².")
    fabricated = {
        "source": "document_text",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md:42",
        "quote": "В документе указана итоговая площадь 99,9 м².",
        "implication": "Такой цитаты в переданном контексте нет",
    }

    normalized, errors = service.normalize_batch_output(
        [case],
        {"reviews": [_raw_review(case, verdict="expert_correct", evidence=[fabricated], reviewed_sources=["document_text"])]},
    )

    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["decisive_evidence"] == []
    assert any("quote not found" in value for value in normalized[0]["guard_adjustments"])


def test_reason_item_mismatch_is_valid_proof_without_external_evidence():
    case = _case(
        "case-mismatch",
        problem="Спецификация напольных покрытий неполна",
        reason="В предыдущей версии не указан класс пожарной опасности стен",
    )

    normalized, errors = service.normalize_batch_output(
        [case],
        {"reviews": [_mismatch_review(case)]},
    )

    assert errors == []
    row = normalized[0]
    assert row["verdict"] == "expert_may_be_wrong"
    assert row["binding_status"] == "conflict"
    assert row["integrity_flags"] == ["reason_item_mismatch"]
    assert {item["source"] for item in row["decisive_evidence"]} == {
        "finding",
        "expert_reason",
    }
    assert row["recommended_action"] == "manual_recheck"
    assert row["guard_adjustments"] == []


def test_conflicting_binding_cannot_keep_raw_expert_correct():
    document_quote = "В документе указана площадь помещения 61,7 м²."
    case = _case(
        "case-conflicting-expert-correct",
        problem="В экспликации отсутствует категория помещения",
        reason="Площадь помещения указана корректно",
        text=document_quote,
    )
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md:12",
        "quote": document_quote,
        "implication": "Документ подтверждает число из причины, но не её привязку к finding",
    }

    normalized, errors = service.normalize_batch_output(
        [case],
        {
            "reviews": [
                _raw_review(
                    case,
                    verdict="expert_correct",
                    binding_status="conflict",
                    evidence=[evidence],
                    reviewed_sources=["document_text"],
                )
            ]
        },
    )

    assert errors == []
    row = normalized[0]
    assert row["raw_verdict"] == "expert_correct"
    assert row["binding_status"] == "conflict"
    assert row["decisive_evidence"] == service._clean_evidence(
        _raw_review(case, verdict="expert_correct", evidence=[evidence])[
            "decisive_evidence"
        ]
    )
    assert row["verdict"] != "expert_correct"
    assert row["recommended_action"] in {"manual_recheck", "collect_context"}
    assert row["recommended_action"] != "keep_rejected"


def test_document_text_evidence_uses_only_document_surface():
    finding_quote = "В замечании заявлено отсутствие аварийного освещения."
    document_quote = "В ведомости указано аварийное освещение коридора."
    case = _case(
        "case-separated-text-surfaces",
        problem=finding_quote,
        text=document_quote,
    )

    masked_evidence = {
        "source": "document_text",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md:20",
        "quote": finding_quote,
        "implication": "Текст finding ошибочно выдан за цитату из документа",
    }
    normalized, errors = service.normalize_batch_output(
        [case],
        {
            "reviews": [
                _raw_review(
                    case,
                    verdict="expert_correct",
                    evidence=[masked_evidence],
                    reviewed_sources=["document_text"],
                )
            ]
        },
    )

    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["decisive_evidence"] == []
    assert any(
        "document_text: quote not found" in value
        for value in normalized[0]["guard_adjustments"]
    )

    real_evidence = {**masked_evidence, "quote": document_quote}
    normalized, errors = service.normalize_batch_output(
        [case],
        {
            "reviews": [
                _raw_review(
                    case,
                    verdict="expert_correct",
                    evidence=[real_evidence],
                    reviewed_sources=["document_text"],
                )
            ]
        },
    )

    assert errors == []
    assert normalized[0]["verdict"] == "expert_correct"
    assert normalized[0]["recommended_action"] == "keep_rejected"
    assert normalized[0]["decisive_evidence"] == service._clean_evidence(
        _raw_review(case, verdict="expert_correct", evidence=[real_evidence])[
            "decisive_evidence"
        ]
    )


def test_exact_text_blocks_are_loaded_from_russian_page_headings(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    output_dir = version_dir / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    document_md = version_dir / "02_work" / "document.md"
    document_md.parent.mkdir(parents=True)
    irrelevant_prefix = "НЕЦЕЛЕВОЙ ПРЕФИКС\n" * 1000
    document_md.write_text(
        "# Тестовый документ\n\n"
        "## СТРАНИЦА 1\n\n"
        f"{irrelevant_prefix}\n"
        "### BLOCK [TEXT]: EXTRA-BLOCK\n"
        "Этот блок не относится к замечанию.\n\n"
        "## СТРАНИЦА 5\n\n"
        "### BLOCK [TEXT]: LPAM-UR69-RGT\n"
        "Пункт 14 выполняется сторонней организацией.\n\n"
        "## СТРАНИЦА 6\n\n"
        "### BLOCK [TEXT]: 67TL-NJEF-C3L\n"
        "Итого по экспликации: 666,20 м².\n\n"
        "### BLOCK [IMAGE]: IMAGE-BLOCK\n"
        "Описание изображения.\n",
        encoding="utf-8",
    )
    finding = {
        "id": "F-CTX",
        "problem": "Проверить итог экспликации и пункт 14",
        "evidence_text_refs": [
            {"text_block_id": "LPAM-UR69-RGT"},
            {"text_block_id": "67TL-NJEF-C3L"},
        ],
    }

    context = service.build_case_context(
        output_dir,
        finding,
        project_id="DOC-1",
        section="AR",
        max_images=0,
    )

    assert "Пункт 14 выполняется сторонней организацией" in context["document_text_excerpt"]
    assert "Итого по экспликации: 666,20 м²" in context["document_text_excerpt"]
    assert "НЕЦЕЛЕВОЙ ПРЕФИКС" not in context["document_text_excerpt"]
    assert context["document_pages_loaded"] == [5, 6]
    assert context["text_block_ids"] == ["LPAM-UR69-RGT", "67TL-NJEF-C3L"]
    assert [row["block_id"] for row in context["blocks"]] == [
        "LPAM-UR69-RGT",
        "67TL-NJEF-C3L",
    ]
    assert context["images_truncated"] is False

    case = _case("case-exact-text-blocks")
    case["context"] = context
    valid_evidence = {
        "source": "text_block",
        "image_index": 0,
        "block_id": "67TL-NJEF-C3L",
        "locator": "блок 67TL-NJEF-C3L, страница 6",
        "quote": "666,20 м²",
        "implication": "Подтверждает переданный итог экспликации",
    }
    accepted, rejected = service._validate_case_evidence(
        case,
        [valid_evidence],
        {case["case_id"]: []},
    )
    assert accepted == [valid_evidence]
    assert rejected == []

    cross_block_evidence = {
        **valid_evidence,
        "quote": "сторонней организацией",
    }
    accepted, rejected = service._validate_case_evidence(
        case,
        [cross_block_evidence],
        {case["case_id"]: []},
    )
    assert accepted == []
    assert any("quote not found" in reason for reason in rejected)


def test_expert_correct_remove_with_critical_missing_context_is_downgraded(tmp_path):
    generic = tmp_path / "generic.png"
    legend = tmp_path / "legend.png"
    generic.write_bytes(b"generic")
    legend.write_bytes(b"legend")
    case = _case(
        "case-generic-scheme",
        images=[
            {"path": str(generic), "block_id": "GENERIC", "page": 1},
            {"path": str(legend), "block_id": "LEGEND", "page": 2},
        ],
    )
    _paths, alignment = service.align_batch_images([case])
    evidence = [
        {
            "source": "graphic_block",
            "image_index": 1,
            "block_id": "GENERIC",
            "locator": "изображение 1",
            "quote": "min 2500",
            "implication": "Показывает значение на типовой схеме",
        },
        {
            "source": "graphic_block",
            "image_index": 2,
            "block_id": "LEGEND",
            "locator": "изображение 2",
            "quote": "М4 6000x3600",
            "implication": "Показывает размеры типа в легенде",
        },
    ]
    review = _raw_review(
        case,
        verdict="expert_correct",
        factual_verdict="contradicted",
        report_value="remove",
        reason_quality="partial",
        evidence=evidence,
        reviewed_sources=["graphic_block"],
        missing_context=[
            "Не передана однозначная привязка типов из легенды к местам на плане"
        ],
    )
    review["recommended_action"] = "keep_rejected"

    normalized, errors = service.normalize_batch_output(
        [case],
        {"reviews": [review]},
        image_alignment=alignment,
    )

    assert errors == []
    row = normalized[0]
    assert row["raw_verdict"] == "expert_correct"
    assert row["verdict"] == "insufficient_evidence"
    assert row["report_value"] == "unclear"
    assert row["recommended_action"] == "collect_context"
    assert row["decisive_evidence"] == service._clean_evidence(
        review["decisive_evidence"]
    )
    assert any(
        "decision-critical context is missing" in adjustment
        for adjustment in row["guard_adjustments"]
    )


def test_reason_only_error_with_removed_report_item_keeps_rejection():
    quote = "Детали ограждения являются заданием для выполнения РД КМ."
    case = _case("case-reason-only", text=quote)
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md, страница 1",
        "quote": quote,
        "implication": "Подтверждает передачу детальной проработки в КМ",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_may_be_wrong",
        factual_verdict="supported",
        report_value="remove",
        reason_quality="partial",
        evidence=[evidence],
        reviewed_sources=["document_text"],
        decision_effect="reason_only",
        rejection_basis="report_value",
        practical_impact="none",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    row = normalized[0]
    assert row["raw_verdict"] == "expert_may_be_wrong"
    assert row["verdict"] == "expert_correct"
    assert row["recommended_action"] == "keep_rejected"
    assert row["review_priority"] == "none"
    assert any("reason-only" in value for value in row["guard_adjustments"])


def test_v4_output_schema_requires_calibration_and_provenance_fields():
    schema = service.output_schema(["case-v4"])
    review_schema = schema["properties"]["reviews"]["items"]
    evidence_schema = review_schema["properties"]["decisive_evidence"]["items"]

    assert {
        "decision_effect",
        "rejection_basis",
        "practical_impact",
        "impact_assessment",
        "source_alignment",
        "scope_context_status",
    } <= set(review_schema["required"])
    assert {
        "source_id",
        "observation_basis",
        "verification_state",
        "claim_type",
        "absence_scope",
    } <= set(evidence_schema["required"])
    assert service.AUDIT_CONTRACT_VERSION.endswith(".v4")


def test_ocr_only_visual_dimension_cannot_support_determinate_verdict(tmp_path):
    image = tmp_path / "dimension.png"
    image.write_bytes(b"dimension")
    case = _case(
        "case-ocr-only",
        images=[{"path": str(image), "block_id": "DIM-1", "page": 5}],
    )
    _paths, alignment = service.align_batch_images([case])
    evidence = {
        "source": "graphic_block",
        "source_id": "",
        "image_index": 1,
        "block_id": "DIM-1",
        "locator": "лист 5, размерная цепочка",
        "quote": "850",
        "implication": "OCR прочитал размер 850",
        "observation_basis": "ocr",
        "verification_state": "single_source",
        "claim_type": "dimension",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_may_be_wrong",
        factual_verdict="supported",
        report_value="include",
        evidence=[evidence],
        reviewed_sources=["graphic_block"],
        source_alignment="ocr_only_visual_claim",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}, image_alignment=alignment
    )

    assert errors == []
    row = normalized[0]
    assert row["verdict"] == "insufficient_evidence"
    assert row["factual_verdict"] == "unclear"
    assert row["report_value"] == "unclear"
    assert row["confidence"] == "low"
    assert row["recommended_action"] == "collect_context"
    assert any("text layer" in value for value in row["missing_context"])


def test_related_document_quote_cannot_cross_primary_surface():
    primary = "Основной том: отметка 0,000."
    related = "Связанный том: класс бетона B30."
    case = _case("case-related-cross", text=primary)
    case["context"]["document_text_excerpt"] = (
        primary + "\n\n[СВЯЗАННЫЙ ДОКУМЕНТ: КР-1; версия v002]\n" + related
    )
    case["context"]["document_pages_loaded"] = [2]
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "основной документ, страница 2",
        "quote": related,
        "implication": "Ошибочная попытка выдать связанный том за основной",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }

    normalized, errors = service.normalize_batch_output(
        [case],
        {"reviews": [_raw_review(case, verdict="expert_correct", evidence=[evidence])]},
    )

    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["decisive_evidence"] == []
    assert any("quote not found" in value for value in normalized[0]["guard_adjustments"])


def test_historically_bound_related_document_has_separate_evidence_surface():
    case = _case("case-related-exact", text="Основной том.")
    case["context"]["related_documents"] = [{
        "source_id": "related:KJ:KR-1:v002",
        "object_id": case["object_id"],
        "document": "KR-1",
        "discipline": "KJ",
        "version": "v002",
        "version_relation": "explicit_historical_match",
        "pages": [2],
        "excerpt": "Класс бетона B30 принят для конструкций ниже 0,000.",
    }]
    evidence = {
        "source": "related_document",
        "source_id": "related:KJ:KR-1:v002",
        "image_index": 0,
        "block_id": "",
        "locator": "документ KR-1, версия v002, страница 2",
        "quote": "Класс бетона B30",
        "implication": "Подтверждает параметр в исторически привязанном КР",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_correct",
        factual_verdict="contradicted",
        report_value="remove",
        evidence=[evidence],
        reviewed_sources=["related_document"],
        rejection_basis="scope_stage",
        practical_impact="none",
        scope_context_status="verified_same_version",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    assert normalized[0]["verdict"] == "expert_correct"
    assert normalized[0]["decisive_evidence"] == [evidence]

    wrong_page = {**evidence, "locator": "документ KR-1, версия v002, страница 5"}
    normalized, errors = service.normalize_batch_output(
        [case],
        {"reviews": [_raw_review(
            case,
            verdict="expert_correct",
            factual_verdict="contradicted",
            report_value="remove",
            evidence=[wrong_page],
            practical_impact="none",
            scope_context_status="verified_same_version",
        )]},
    )
    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["decisive_evidence"] == []
    assert any("selected pages" in value for value in normalized[0]["guard_adjustments"])


def test_current_related_snapshot_is_not_decisive_for_past_rejection():
    case = _case("case-related-current", text="Основной том.")
    case["context"]["related_documents"] = [{
        "source_id": "related:PZ:PZ-1:v010",
        "object_id": case["object_id"],
        "document": "PZ-1",
        "discipline": "PZ",
        "version": "v010",
        "version_relation": "current_snapshot",
        "pages": [3],
        "excerpt": "Класс агрессивности среды — слабоагрессивная.",
    }]
    evidence = {
        "source": "related_document",
        "source_id": "related:PZ:PZ-1:v010",
        "image_index": 0,
        "block_id": "",
        "locator": "документ PZ-1, версия v010, страница 3",
        "quote": "Класс агрессивности среды",
        "implication": "Текущий снимок содержит требуемый параметр",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_correct",
        factual_verdict="contradicted",
        report_value="remove",
        evidence=[evidence],
        reviewed_sources=["related_document"],
        rejection_basis="scope_stage",
        practical_impact="none",
        scope_context_status="verified_same_version",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    row = normalized[0]
    assert row["verdict"] == "insufficient_evidence"
    assert row["recommended_action"] == "collect_context"
    assert any("decision-time version" in value for value in row["guard_adjustments"])


def test_downgraded_low_impact_candidate_gets_low_review_priority():
    quote = "В штампе одного листа указан 29 этаж."
    case = _case("case-low-priority", text=quote)
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md, страница 1",
        "quote": quote,
        "implication": "Подтверждает редакционное расхождение",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_may_be_wrong",
        factual_verdict="supported",
        report_value="downgrade",
        evidence=[evidence],
        decision_effect="changes_rejection",
        rejection_basis="report_value",
        practical_impact="low",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    assert normalized[0]["recommended_action"] == "manual_recheck"
    assert normalized[0]["review_priority"] == "low"


def test_remove_is_rejected_when_practical_impact_is_not_low():
    quote = "Марка изменяет заказную позицию изделия."
    case = _case("case-remove-impact-conflict", text=quote)
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md, страница 4",
        "quote": quote,
        "implication": "Показывает влияние на закупку",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_correct",
        factual_verdict="supported",
        report_value="remove",
        evidence=[evidence],
        decision_effect="supports_rejection",
        rejection_basis="report_value",
        practical_impact="high",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["report_value"] == "unclear"
    assert any("remove requires" in value for value in normalized[0]["guard_adjustments"])


def test_unverified_scope_context_cannot_support_rejection():
    quote = "Расчёт следует искать в разделе КР."
    case = _case("case-scope-missing", text=quote)
    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "document.md, страница 1",
        "quote": quote,
        "implication": "Основной том лишь отсылает к разделу КР",
        "observation_basis": "pdf_text_layer",
        "verification_state": "corroborated",
        "claim_type": "text_token",
        "absence_scope": "none",
    }
    review = _raw_review(
        case,
        verdict="expert_correct",
        factual_verdict="contradicted",
        report_value="remove",
        evidence=[evidence],
        decision_effect="supports_rejection",
        rejection_basis="scope_stage",
        practical_impact="none",
        scope_context_status="missing",
    )

    normalized, errors = service.normalize_batch_output(
        [case], {"reviews": [review]}
    )

    assert errors == []
    assert normalized[0]["verdict"] == "insufficient_evidence"
    assert normalized[0]["recommended_action"] == "collect_context"
    assert any("связанный документ" in value for value in normalized[0]["missing_context"])


@pytest.mark.asyncio
async def test_run_codex_audit_resumes_by_case_id_and_input_hash(tmp_path, monkeypatch):
    case_1 = _case("case-1", input_hash="hash-current-1")
    case_2 = _case("case-2", input_hash="hash-current-2")
    cases_by_id = {case["case_id"]: case for case in (case_1, case_2)}
    results_path = tmp_path / "results.jsonl"
    service.append_result(
        results_path,
        {"case_id": "case-1", "status": "success", "input_hash": "hash-current-1"},
    )
    service.append_result(
        results_path,
        {"case_id": "case-2", "status": "success", "input_hash": "hash-stale-2"},
    )
    calls: list[list[str]] = []

    async def fake_runner(_messages, **kwargs):
        case_ids = kwargs["output_schema"]["properties"]["reviews"]["items"]["properties"]["case_id"]["enum"]
        calls.append(list(case_ids))
        return SimpleNamespace(
            is_error=False,
            error_message="",
            text="",
            json_data={"reviews": [_mismatch_review(cases_by_id[case_id]) for case_id in case_ids]},
            model="codex/test",
            cost_source="subscription",
        )

    monkeypatch.setattr(service, "build_messages", lambda *_args, **_kwargs: [])

    summary = await service.run_codex_audit(
        [case_1, case_2],
        results_path=results_path,
        runner=fake_runner,
        batch_size=2,
    )

    assert calls == [["case-2"]]
    assert summary["halted_reason"] == ""
    assert summary["counts"]["selected_pending"] == 1
    assert summary["counts"]["planned_calls"] == 1
    assert summary["counts"]["completed"] == 1
    latest, malformed = service.load_latest_results(results_path)
    assert malformed == 0
    assert latest["case-1"]["input_hash"] == "hash-current-1"
    assert latest["case-2"]["input_hash"] == "hash-current-2"
    assert latest["case-2"]["verdict"] == "expert_may_be_wrong"


@pytest.mark.asyncio
async def test_run_codex_audit_halts_after_subscription_limit(tmp_path, monkeypatch):
    case_1 = _case("case-1")
    case_2 = _case("case-2")
    results_path = tmp_path / "results.jsonl"
    calls = 0

    async def limited_runner(_messages, **_kwargs):
        nonlocal calls
        calls += 1
        return SimpleNamespace(
            is_error=True,
            error_message="usage limit reached; try again later",
            text="",
            json_data=None,
        )

    monkeypatch.setattr(service, "build_messages", lambda *_args, **_kwargs: [])

    summary = await service.run_codex_audit(
        [case_1, case_2],
        results_path=results_path,
        runner=limited_runner,
        batch_size=1,
    )

    assert calls == 1
    assert summary["halted_reason"] == "subscription_or_rate_limit"
    assert summary["counts"]["selected_pending"] == 2
    assert summary["counts"]["planned_calls"] == 2
    assert summary["counts"]["errors"] == 1
    latest, malformed = service.load_latest_results(results_path)
    assert malformed == 0
    assert set(latest) == {"case-1"}
    assert latest["case-1"]["status"] == "error"
    assert latest["case-1"]["input_hash"] == case_1["input_hash"]


def test_generate_report_ignores_stale_success_with_mismatched_input_hash(tmp_path):
    case = _case("case-stale-report", input_hash="hash-current")
    case["period"] = "2026-07"
    output_dir = tmp_path / "report"
    results_path = output_dir / "results.jsonl"
    service.append_result(
        results_path,
        {
            "case_id": case["case_id"],
            "status": "success",
            "input_hash": "hash-stale",
            "verdict": "expert_may_be_wrong",
            "recommended_action": "manual_recheck",
            "confidence": "high",
        },
    )

    summary = service.generate_report(
        [case],
        output_dir=output_dir,
        results_path=results_path,
    )

    assert summary["selected_cases"] == 1
    assert summary["completed"] == 0
    assert summary["remaining"] == 1
    assert summary["completion_pct"] == 0.0
    assert summary["manual_recheck_candidates"] == 0
    assert summary["verdicts"] == {}
    candidates = json.loads(
        (output_dir / "candidates.json").read_text(encoding="utf-8")
    )
    assert candidates["candidates"] == []
    assert "case-stale-report,pending" in (output_dir / "results.csv").read_text(
        encoding="utf-8-sig"
    )


@pytest.mark.asyncio
async def test_run_codex_audit_hard_caps_images_for_single_oversized_case(
    tmp_path,
    monkeypatch,
):
    image_paths = [tmp_path / f"image-{index}.png" for index in range(5)]
    for path in image_paths:
        path.write_bytes(path.name.encode("utf-8"))
    case = _case(
        "case-many-images",
        images=[
            {"path": str(path), "block_id": f"block-{index}", "page": index}
            for index, path in enumerate(image_paths)
        ],
    )
    planned = service.plan_batches(
        [case],
        batch_size=4,
        max_batch_images=2,
    )
    assert [[row["case_id"] for row in batch] for batch in planned] == [
        [case["case_id"]]
    ]

    received_image_paths: list[list[str]] = []

    async def fake_runner(_messages, **kwargs):
        received_image_paths.append(list(kwargs["image_paths"]))
        case_ids = kwargs["output_schema"]["properties"]["reviews"]["items"][
            "properties"
        ]["case_id"]["enum"]
        return SimpleNamespace(
            is_error=False,
            error_message="",
            text="",
            json_data={
                "reviews": [_mismatch_review(case) for _case_id in case_ids]
            },
            model="codex/test",
            cost_source="subscription",
        )

    monkeypatch.setattr(service, "build_messages", lambda *_args, **_kwargs: [])

    summary = await service.run_codex_audit(
        [case],
        results_path=tmp_path / "results.jsonl",
        runner=fake_runner,
        batch_size=4,
        max_batch_images=2,
    )

    assert received_image_paths == [
        [str(path.resolve()) for path in image_paths[:2]]
    ]
    assert summary["counts"]["completed"] == 1
    assert summary["halted_reason"] == ""


@pytest.mark.parametrize(
    ("requested_month", "requested_objects", "expected_error"),
    [
        ("2026-08", "object-1", "month: frozen='2026-07'"),
        ("2026-07", "object-2", "object filter differs from frozen manifest"),
    ],
)
def test_cli_reuse_manifest_rejects_mismatched_frozen_scope(
    tmp_path,
    capsys,
    requested_month,
    requested_objects,
    expected_error,
):
    output_dir = tmp_path / "frozen-audit"
    _write_json(
        output_dir / "inventory.json",
        {
            "period": "2026-07",
            "timezone": "Europe/Moscow",
            "audit_contract_version": service.AUDIT_CONTRACT_VERSION,
            "filters": {
                "object_ids": ["object-1"],
                "disciplines": ["AR"],
                "explicit_carried_over_excluded": True,
                "include_optimizations": False,
            },
        },
    )
    (output_dir / "manifest.jsonl").write_text("{}\n", encoding="utf-8")

    exit_code = audit_cli.main(
        [
            "run",
            "--month",
            requested_month,
            "--output-dir",
            str(output_dir),
            "--reuse-manifest",
            "--objects",
            requested_objects,
            "--disciplines",
            "AR",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "frozen manifest scope mismatch" in captured.err
    assert expected_error in captured.err


def test_cli_reuse_manifest_rejects_mismatched_frozen_reviewer(
    tmp_path,
    capsys,
):
    output_dir = tmp_path / "frozen-reviewer-audit"
    _write_json(
        output_dir / "inventory.json",
        {
            "period": "2026-07",
            "timezone": "Europe/Moscow",
            "filters": {
                "object_ids": [],
                "disciplines": [],
                "reviewers": ["Кульдяев Ф. С."],
                "explicit_carried_over_excluded": True,
                "include_optimizations": False,
            },
        },
    )
    (output_dir / "manifest.jsonl").write_text("{}\\n", encoding="utf-8")

    exit_code = audit_cli.main(
        [
            "run",
            "--month",
            "2026-07",
            "--output-dir",
            str(output_dir),
            "--reuse-manifest",
            "--reviewers",
            "Другой эксперт",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "reviewer filter differs from frozen manifest" in captured.err


def test_cli_reuse_manifest_rejects_previous_audit_contract(tmp_path, capsys):
    output_dir = tmp_path / "frozen-v3-audit"
    _write_json(
        output_dir / "inventory.json",
        {
            "period": "2026-07",
            "timezone": "Europe/Moscow",
            "audit_contract_version": "rejected_finding_expert_audit.v3",
            "filters": {
                "object_ids": [],
                "disciplines": [],
                "reviewers": [],
                "explicit_carried_over_excluded": True,
                "include_optimizations": False,
            },
        },
    )
    (output_dir / "manifest.jsonl").write_text("{}\n", encoding="utf-8")

    exit_code = audit_cli.main([
        "run",
        "--month",
        "2026-07",
        "--output-dir",
        str(output_dir),
        "--reuse-manifest",
    ])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "audit contract differs from frozen manifest" in captured.err
    assert "prepare a new output directory" in captured.err


def test_cli_run_returns_nonzero_when_audit_records_errors(
    tmp_path,
    monkeypatch,
    capsys,
):
    output_dir = tmp_path / "run-with-errors"
    _write_json(
        output_dir / "inventory.json",
        {
            "period": "2026-07",
            "timezone": "Europe/Moscow",
            "audit_contract_version": service.AUDIT_CONTRACT_VERSION,
            "filters": {
                "object_ids": [],
                "disciplines": [],
                "explicit_carried_over_excluded": True,
                "include_optimizations": False,
            },
        },
    )
    (output_dir / "manifest.jsonl").write_text("{}\n", encoding="utf-8")
    case = _case("case-cli-error")
    observed: dict = {}

    async def fake_run(cases, **kwargs):
        observed["cases"] = cases
        observed["results_path"] = kwargs["results_path"]
        return {
            "counts": {"selected_pending": 1, "errors": 1},
            "halted_reason": "",
        }

    import backend.app.services.llm.codex_runner as codex_runner

    monkeypatch.setattr(audit_cli, "load_manifest", lambda _path: [case])
    monkeypatch.setattr(audit_cli, "run_codex_audit", fake_run)
    monkeypatch.setattr(
        audit_cli,
        "generate_report",
        lambda *_args, **_kwargs: {"completed": 0, "remaining": 1},
    )
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/fake/codex")

    exit_code = audit_cli.main(
        [
            "run",
            "--month",
            "2026-07",
            "--output-dir",
            str(output_dir),
            "--reuse-manifest",
            "--confirm-external-codex",
        ]
    )

    assert exit_code == 5
    assert observed["cases"] == [case]
    assert observed["results_path"] == output_dir / "results.jsonl"
    assert '"errors": 1' in capsys.readouterr().out


def test_hybrid_second_pass_selects_determinate_and_guarded_current_results(
    tmp_path,
):
    cases = [
        _case("case-determinate", input_hash="hash-determinate"),
        _case("case-guard", input_hash="hash-guard"),
        _case("case-skip", input_hash="hash-skip"),
        _case("case-stale", input_hash="hash-current"),
    ]
    rows = [
        {
            "case_id": "case-determinate",
            "input_hash": "hash-determinate",
            "status": "success",
            "verdict": "expert_correct",
            "guard_adjustments": [],
        },
        {
            "case_id": "case-guard",
            "input_hash": "hash-guard",
            "status": "success",
            "verdict": "insufficient_evidence",
            "guard_adjustments": ["evidence rejected"],
        },
        {
            "case_id": "case-skip",
            "input_hash": "hash-skip",
            "status": "success",
            "verdict": "insufficient_evidence",
            "guard_adjustments": [],
        },
        {
            "case_id": "case-stale",
            "input_hash": "hash-old",
            "status": "success",
            "verdict": "expert_may_be_wrong",
            "guard_adjustments": [],
        },
    ]
    results_path = tmp_path / "luna-results.jsonl"
    results_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )

    selected = audit_cli._hybrid_second_pass_case_ids(results_path, cases)

    assert selected == {"case-determinate", "case-guard"}


def test_retrieval_enriches_same_version_document_and_recomputes_hash(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    _write_json(
        output_dir / "document_graph.json",
        {
            "pages": [
                {
                    "page": 1,
                    "sheet_name": "План",
                    "text_blocks": [{"text": "На плане показана марка КР-17."}],
                },
                {
                    "page": 2,
                    "sheet_name": "Спецификация",
                    "text_blocks": [
                        {
                            "text": (
                                "Марка КР-17 приведена в спецификации: "
                                "масса изделия 46,05 кг."
                            )
                        }
                    ],
                },
            ]
        },
    )
    case = _case(
        "case-retrieval-text",
        input_hash="source-hash",
        problem="Для марки КР-17 не подтверждена масса",
        text="На плане показана марка КР-17.",
    )
    case["output_dir"] = str(output_dir)
    case["finding"]["page"] = 1
    case["context"]["document_pages_loaded"] = [1]
    first_result = {
        "case_id": case["case_id"],
        "input_hash": case["input_hash"],
        "status": "success",
        "verdict": "insufficient_evidence",
        "binding_status": "exact",
        "missing_context": ["Спецификация марки КР-17 с массой изделия"],
        "reason_assessment": "Нужна спецификация изделия",
    }
    original = json.loads(json.dumps(case, ensure_ascii=False))
    monkeypatch.setattr(service, "_collect_retrieval_graphics", lambda _case: [])

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        first_result,
        max_images=0,
    )

    assert case == original
    assert receipt["found"] is True
    assert receipt["document"]["selected_pages"] == [2]
    assert "масса изделия 46,05 кг" in enriched["context"]["document_text_excerpt"]
    assert enriched["context"]["document_pages_loaded"] == [1, 2]
    assert enriched["source_input_hash"] == "source-hash"
    assert enriched["input_hash"] != "source-hash"
    assert enriched["previous_audit"]["verdict"] == "insufficient_evidence"

    evidence = {
        "source": "document_text",
        "source_id": "primary",
        "image_index": 0,
        "block_id": "",
        "locator": "страница 2, спецификация",
        "quote": "масса изделия 46,05 кг",
        "implication": "Подтверждает значение для марки КР-17",
    }
    accepted, rejected = service._validate_case_evidence(
        enriched,
        [evidence],
        {enriched["case_id"]: []},
    )
    assert accepted == [evidence]
    assert rejected == []


def test_prepare_retrieval_cases_uses_only_current_safe_insufficient(
    monkeypatch,
):
    current = _case("case-current", input_hash="hash-current")
    determinate = _case("case-determinate", input_hash="hash-determinate")
    stale = _case("case-stale", input_hash="hash-current-stale")
    unsafe = _case("case-unsafe", input_hash="hash-unsafe")
    unsafe["source_quality"] = "same_version_artifact_conflict"
    cases = [current, determinate, stale, unsafe]
    results = {
        "case-current": {
            "case_id": "case-current",
            "input_hash": "hash-current",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен лист 2"],
        },
        "case-determinate": {
            "case_id": "case-determinate",
            "input_hash": "hash-determinate",
            "status": "success",
            "verdict": "expert_correct",
            "binding_status": "exact",
            "missing_context": ["Нужен лист 2"],
        },
        "case-stale": {
            "case_id": "case-stale",
            "input_hash": "old-hash",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен лист 2"],
        },
        "case-unsafe": {
            "case_id": "case-unsafe",
            "input_hash": "hash-unsafe",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен лист 2"],
        },
    }

    def fake_enrich(case, _result, *, max_images):
        enriched = json.loads(json.dumps(case, ensure_ascii=False))
        enriched["source_input_hash"] = case["input_hash"]
        enriched["input_hash"] = "retrieval-hash-" + case["case_id"]
        return enriched, {"found": True, "max_images": max_images}

    monkeypatch.setattr(service, "enrich_case_for_retrieval", fake_enrich)

    selected, stats = service.prepare_retrieval_cases(
        cases,
        results,
        limit=10,
        max_images_per_case=3,
    )

    assert [case["case_id"] for case in selected] == ["case-current"]
    assert selected[0]["input_hash"] == "retrieval-hash-case-current"
    assert stats["attempted"] == 1
    assert stats["found"] == 1
    assert stats["non_insufficient_result"] == 1
    assert stats["stale_result"] == 1
    assert stats["unsafe_source_quality"] == 1


def test_retrieval_attaches_explicit_missing_graphic_block(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    image_path = tmp_path / "block_MISS-123-ABC.png"
    image_path.write_bytes(b"retrieved image")
    case = _case("case-retrieval-image", input_hash="source-image-hash")
    case["output_dir"] = str(output_dir)
    first_result = {
        "verdict": "insufficient_evidence",
        "binding_status": "exact",
        "missing_context": ["Нужен графический блок MISS-123-ABC"],
    }
    monkeypatch.setattr(
        service,
        "_collect_retrieval_graphics",
        lambda _case: [
            {
                "block_id": "MISS-123-ABC",
                "page": 7,
                "label": "Узел с размером 250 мм",
                "searchable": "Узел MISS-123-ABC, размер 250 мм",
                "image_path": str(image_path),
            }
        ],
    )

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        first_result,
        max_images=1,
    )

    assert receipt["found"] is True
    assert receipt["graphics"]["selected_block_ids"] == ["MISS-123-ABC"]
    assert enriched["context"]["images"] == [
        {"path": str(image_path), "block_id": "MISS-123-ABC", "page": 7}
    ]
    paths, alignment = service.align_batch_images([enriched])
    assert paths == [str(image_path.resolve())]
    evidence = {
        "source": "graphic_block",
        "source_id": "graphic:MISS-123-ABC",
        "image_index": 1,
        "block_id": "MISS-123-ABC",
        "locator": "изображение 1, страница 7",
        "quote": "250 мм",
        "implication": "Показывает проверяемый размер",
    }
    accepted, rejected = service._validate_case_evidence(
        enriched,
        [evidence],
        alignment,
    )
    assert accepted == [evidence]
    assert rejected == []


def test_retrieval_uses_same_version_vector_graph_when_png_is_missing(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    vector_path = output_dir / "block_vector_graphs" / "MISS-123-ABC.json"
    _write_json(
        vector_path,
        {
            "block_id": "MISS-123-ABC",
            "page": 9,
            "profile_id": "ar_opening_plan",
            "classification": {
                "block_title": "План отверстий",
                "description": "На плане указан радиус закругления 1200 мм.",
            },
            "markdown": "# План отверстий\n\nРадиус закругления 1200 мм.",
        },
    )
    case = _case("case-retrieval-vector", input_hash="source-vector-hash")
    case["output_dir"] = str(output_dir)
    first_result = {
        "verdict": "insufficient_evidence",
        "binding_status": "exact",
        "missing_context": ["Нужен графический блок MISS-123-ABC"],
    }
    monkeypatch.setattr(service, "_collect_retrieval_graphics", lambda _case: [])

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        first_result,
        max_images=1,
    )

    assert receipt["found"] is True
    assert receipt["graphics"]["selected_block_ids"] == []
    assert receipt["vector_blocks"] == {
        "selected_block_ids": ["MISS-123-ABC"],
        "selected_pages": [9],
        "paths": [str(vector_path.resolve())],
    }
    assert enriched["context"]["images"] == []
    assert enriched["context"]["text_block_ids"] == ["MISS-123-ABC"]
    assert enriched["context"]["source_block_ids"] == ["MISS-123-ABC"]
    assert enriched["context"]["route"] == "text"

    evidence = {
        "source": "text_block",
        "image_index": 0,
        "block_id": "MISS-123-ABC",
        "locator": "векторный блок MISS-123-ABC, страница 9",
        "quote": "радиус закругления 1200 мм",
        "implication": "Подтверждает требуемый радиус",
    }
    accepted, rejected = service._validate_case_evidence(
        enriched,
        [evidence],
        {enriched["case_id"]: []},
    )
    assert accepted == [evidence]
    assert rejected == []


def test_cli_retrieve_refuses_same_source_and_destination(tmp_path, capsys):
    exit_code = audit_cli.main(
        [
            "retrieve",
            "--month",
            "2026-07",
            "--source-output-dir",
            str(tmp_path),
            "--output-dir",
            str(tmp_path),
            "--limit",
            "10",
        ]
    )

    assert exit_code == 2
    assert "source и destination совпадают" in capsys.readouterr().err


def test_cli_accepts_max_reasoning_effort():
    args = audit_cli.build_parser().parse_args(
        ["report", "--month", "2026-07", "--reasoning-effort", "max"]
    )

    assert args.reasoning_effort == "max"


def test_explicit_block_ids_support_modern_and_legacy_ids():
    assert service._explicit_block_ids(
        "Нужны MISS-123-ABC и blk_7216fa4476b144feb85e074f2e728aea"
    ) == ["MISS-123-ABC", "blk_7216fa4476b144feb85e074f2e728aea"]


@pytest.mark.parametrize(
    ("url", "block_id", "expected"),
    [
        (
            "https://pub-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.r2.dev/"
            "tree_docs/11111111-1111-1111-1111-111111111111/crops/MISS-123-ABC.pdf",
            "MISS-123-ABC",
            True,
        ),
        (
            "https://pub-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.r2.dev/"
            "tree_docs/11111111-1111-1111-1111-111111111111/crops/OTHER-123-ABC.pdf",
            "MISS-123-ABC",
            False,
        ),
        (
            "https://vibe.cloud-ip.cc/api/crops/t17S7YnIr1HTgkj38U3H",
            "blk_7216fa4476b144feb85e074f2e728aea",
            True,
        ),
        (
            "http://vibe.cloud-ip.cc/api/crops/t17S7YnIr1HTgkj38U3H",
            "blk_7216fa4476b144feb85e074f2e728aea",
            False,
        ),
        (
            "https://user@vibe.cloud-ip.cc/api/crops/t17S7YnIr1HTgkj38U3H",
            "blk_7216fa4476b144feb85e074f2e728aea",
            False,
        ),
    ],
)
def test_remote_crop_url_allowlist(url, block_id, expected):
    assert service._validate_remote_crop_url(url, block_id)[0] is expected


def test_remote_crop_terminal_404_is_cached_for_the_run(monkeypatch):
    calls = []

    def fake_get(url, block_id, *, budget=None):
        calls.append((url, block_id, budget))
        return 404, "text/html", b"not found"

    monkeypatch.setattr(service, "_safe_remote_crop_get", fake_get)
    budget = {"run_used": 0}
    url = "https://vibe.cloud-ip.cc/api/crops/t17S7YnIr1HTgkj38U3H"

    first = service._cached_remote_crop_get(url, "BLOCK-1", budget=budget)
    second = service._cached_remote_crop_get(url, "BLOCK-1", budget=budget)

    assert first == second == (404, "text/html", b"not found")
    assert len(calls) == 1


def test_retrieval_pins_source_image_upgrades_text_and_freezes_assets(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    source_image = tmp_path / "source.png"
    source_image.write_bytes(b"source image")
    extra_image = tmp_path / "extra.png"
    extra_image.write_bytes(b"extra image")
    vector_path = output_dir / "block_vector_graphs" / "SRC-111-AAA.json"
    _write_json(
        vector_path,
        {
            "block_id": "SRC-111-AAA",
            "page": 2,
            "classification": {
                "description": "Полное векторное описание исходного блока с размером 250 мм."
            },
            "markdown": "Исходный блок SRC-111-AAA содержит размер 250 мм.",
        },
    )
    case = _case(
        "case-source-pin",
        input_hash="source-pin-hash",
        images=[
            {
                "path": str(source_image),
                "block_id": "SRC-111-AAA",
                "page": 2,
            }
        ],
    )
    case["output_dir"] = str(output_dir)
    case["version_dir"] = str(output_dir.parent.parent)
    case["finding"].update({
        "source_block_ids": ["SRC-111-AAA"],
        "evidence": [{"type": "image", "block_id": "SRC-111-AAA", "page": 2}],
    })
    case["context"]["blocks"] = [{
        "block_id": "SRC-111-AAA",
        "page": 2,
        "ocr_label": "",
        "ocr_or_description": "коротко",
        "image_path": str(source_image),
    }]
    monkeypatch.setattr(
        service,
        "_collect_retrieval_graphics",
        lambda _case: [{
            "block_id": "EXTR-222-BBB",
            "page": 3,
            "label": "Похожий, но дополнительный блок",
            "searchable": "Дополнительный блок",
            "image_path": str(extra_image),
        }],
    )

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        {
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен EXTR-222-BBB"],
        },
        max_images=1,
        asset_dir=tmp_path / "snapshot_assets",
    )

    assert receipt["found"] is True
    assert [row["block_id"] for row in enriched["context"]["images"]] == [
        "SRC-111-AAA"
    ]
    frozen_path = Path(enriched["context"]["images"][0]["path"])
    assert frozen_path.is_relative_to((tmp_path / "snapshot_assets").resolve())
    assert frozen_path.read_bytes() == b"source image"
    source_block = next(
        row
        for row in enriched["context"]["blocks"]
        if row["block_id"] == "SRC-111-AAA"
    )
    assert "250 мм" in source_block["ocr_or_description"]
    assert enriched["context"]["images_truncated"] is False


def test_remote_crop_is_rendered_and_reused_from_snapshot_cache(
    tmp_path,
    monkeypatch,
):
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    page = document.new_page(width=300, height=200)
    page.insert_text((30, 50), "MISS-123-ABC 250")
    pdf_bytes = document.tobytes()
    document.close()

    row = {
        "block_id": "MISS-123-ABC",
        "page": 1,
        "page_width": 300,
        "page_height": 200,
        "coords_px": [0, 0, 300, 200],
        "crop_url": (
            "https://pub-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.r2.dev/"
            "tree_docs/11111111-1111-1111-1111-111111111111/"
            "crops/MISS-123-ABC.pdf"
        ),
    }
    output_dir = tmp_path / "latest"
    output_dir.mkdir()
    asset_dir = tmp_path / "context_assets"
    calls = []
    monkeypatch.setattr(
        service,
        "_safe_remote_crop_get",
        lambda url, block_id: (
            calls.append((url, block_id)) or (200, "application/pdf", pdf_bytes)
        ),
    )

    first, first_receipt = service._materialize_retrieval_graphic(
        row,
        output_dir=output_dir,
        asset_dir=asset_dir,
        allow_remote_crops=True,
        source_pdf_path=None,
    )
    assert first is not None
    assert Path(first["image_path"]).is_file()
    assert first_receipt["status"] == "ok"
    assert len(calls) == 1

    def fail_fetch(*_args):
        raise AssertionError("cache hit must not fetch")

    monkeypatch.setattr(service, "_safe_remote_crop_get", fail_fetch)
    second, second_receipt = service._materialize_retrieval_graphic(
        row,
        output_dir=output_dir,
        asset_dir=asset_dir,
        allow_remote_crops=True,
        source_pdf_path=None,
    )
    assert second is not None
    assert second_receipt["cache_hit"] is True


def test_cli_auto_retry_prepares_disclosure_without_external_run(
    tmp_path,
    monkeypatch,
    capsys,
):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    case = _case("case-auto", input_hash="hash-auto")
    source_manifest = source_dir / "manifest.jsonl"
    source_results = source_dir / "results.jsonl"
    source_manifest.write_text(
        json.dumps(case, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (source_dir / "inventory.json").write_text(
        json.dumps({
            "period": "2026-07",
            "timezone": "Europe/Moscow",
            "filters": {},
        }),
        encoding="utf-8",
    )
    source_results.write_text(
        json.dumps({
            "case_id": "case-auto",
            "input_hash": "hash-auto",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен исходный блок"],
        }, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    manifest_before = source_manifest.read_bytes()
    results_before = source_results.read_bytes()

    def fake_prepare(cases, results, **_kwargs):
        selected = json.loads(json.dumps(cases, ensure_ascii=False))
        selected[0]["source_input_hash"] = selected[0]["input_hash"]
        selected[0]["input_hash"] = "auto-hash"
        selected[0]["context"]["retrieval_receipt"] = {"found": True}
        return selected, {"attempted": 1, "found": 1, "selected_cases": 1}

    async def fail_run(*_args, **_kwargs):
        raise AssertionError("external run must not start during preflight")

    monkeypatch.setattr(audit_cli, "prepare_retrieval_cases", fake_prepare)
    monkeypatch.setattr(audit_cli, "run_codex_audit", fail_run)
    output_dir = tmp_path / "auto-retry-v1"
    base_args = [
        "auto-retry",
        "--month",
        "2026-07",
        "--source-output-dir",
        str(source_dir),
        "--output-dir",
        str(output_dir),
        "--no-remote-crops",
    ]

    assert audit_cli.main(base_args) == 6
    disclosure_path = output_dir / "external_codex_disclosure.json"
    assert disclosure_path.is_file()
    disclosure_sha = audit_cli._sha256_file(disclosure_path)
    assert (output_dir / "manifest.jsonl").is_file()
    assert not (output_dir / "results.jsonl").exists()
    assert source_manifest.read_bytes() == manifest_before
    assert source_results.read_bytes() == results_before

    capsys.readouterr()
    assert audit_cli.main(
        base_args
        + [
            "--confirm-external-codex",
            "--confirm-disclosure-sha256",
            "0" * 64,
        ]
    ) == 6
    assert "не совпадает" in capsys.readouterr().err
    assert audit_cli._sha256_file(disclosure_path) == disclosure_sha

    captured_run = {}

    async def capture_run(*_args, **kwargs):
        captured_run.update(kwargs)
        return {"counts": {"errors": 0}, "halted_reason": None}

    from backend.app.services.llm import codex_runner

    monkeypatch.setattr(audit_cli, "run_codex_audit", capture_run)
    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/usr/bin/codex")
    monkeypatch.setattr(audit_cli, "generate_report", lambda *_args, **_kwargs: {})
    capsys.readouterr()
    assert audit_cli.main(
        base_args
        + [
            "--limit",
            "100",
            "--confirm-external-codex",
            "--confirm-disclosure-sha256",
            disclosure_sha,
        ]
    ) == 0
    assert captured_run["limit"] == 100



def test_retrieval_recovers_stale_exact_source_image(tmp_path, monkeypatch):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    valid_source = tmp_path / "valid-source.png"
    valid_source.write_bytes(b"valid source")
    case = _case(
        "case-stale-source",
        input_hash="stale-source-hash",
        images=[{
            "path": str(tmp_path / "gone.png"),
            "block_id": "SRC-111-AAA",
            "page": 2,
        }],
    )
    case["output_dir"] = str(output_dir)
    case["version_dir"] = str(output_dir.parent.parent)
    case["finding"].update({
        "source_block_ids": ["SRC-111-AAA"],
        "page": 2,
    })
    monkeypatch.setattr(
        service,
        "_collect_retrieval_graphics",
        lambda _case: [{
            "block_id": "SRC-111-AAA",
            "page": 2,
            "block_type": "image",
            "label": "Точный исходный блок",
            "searchable": "Точный исходный блок с размером 250 мм",
            "image_path": str(valid_source),
        }],
    )

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        {
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен SRC-111-AAA"],
        },
        max_images=1,
        asset_dir=tmp_path / "snapshot",
    )

    assert receipt["found"] is True
    assert receipt["graphics"]["missing_source_image_ids"] == []
    assert enriched["context"]["images_truncated"] is False
    assert [row["block_id"] for row in enriched["context"]["images"]] == [
        "SRC-111-AAA"
    ]
    frozen = Path(enriched["context"]["images"][0]["path"])
    assert frozen.is_file()
    assert frozen.read_bytes() == b"valid source"


def test_missing_source_image_is_not_reported_as_capacity_truncation(
    tmp_path,
    monkeypatch,
):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    case = _case("case-source-unavailable", input_hash="source-unavailable")
    case["output_dir"] = str(output_dir)
    case["version_dir"] = str(output_dir.parent.parent)
    case["finding"].update({
        "source_block_ids": ["MISS-111-AAA"],
        "evidence": [{
            "type": "image",
            "block_id": "MISS-111-AAA",
            "page": 2,
        }],
        "page": 2,
    })
    monkeypatch.setattr(service, "_collect_retrieval_graphics", lambda _case: [])
    monkeypatch.setattr(
        service,
        "_verified_archived_block_png",
        lambda _block_id: (None, {"status": "not_found"}),
    )

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        {
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужно изображение MISS-111-AAA"],
        },
        max_images=3,
        asset_dir=tmp_path / "snapshot",
    )

    assert enriched["context"]["images"] == []
    assert enriched["context"]["images_truncated"] is False
    assert enriched["context"]["source_images_unavailable"] == ["MISS-111-AAA"]
    assert receipt["graphics"]["capacity_truncated_source_ids"] == []
    assert receipt["graphics"]["unavailable_source_image_ids"] == ["MISS-111-AAA"]


def test_semantic_target_ranking_prefers_exact_coordinated_plans():
    catalog = [
        {
            "block_id": "PART-111-AAA",
            "page": 5,
            "label": "План монтажа перегородок в павильоне №2 с общими размерами",
            "searchable": "План монтажа перегородок в павильоне №2 с общими и локальными размерами",
        },
        {
            "block_id": "FURN-222-BBB",
            "page": 6,
            "label": "План расстановки мебели в павильоне №2",
            "searchable": "План расстановки мебели; размеры 14600, 4250, 5000 и 5350",
        },
        {
            "block_id": "WALL-333-CCC",
            "page": 8,
            "label": "План отделки стен павильона №2",
            "searchable": "План отделки стен; показана зона расстановки мебели и привязки",
        },
    ]

    ranked = service._semantic_target_candidates(
        catalog,
        ["Графические области планов монтажа перегородок и расстановки мебели с общими и привязочными размерами павильона №2"],
        graph={},
        target_pages=[8],
        limit=3,
    )

    assert {row["block_id"] for row in ranked[:2]} == {
        "PART-111-AAA",
        "FURN-222-BBB",
    }
    assert ranked[-1]["block_id"] == "WALL-333-CCC"


def test_full_page_fallback_rejects_ambiguous_sheet_mapping(tmp_path):
    source_pdf = tmp_path / "source.pdf"
    source_pdf.write_bytes(b"%PDF-1.4")
    graph = {
        "pages": [
            {"page": 4, "sheet_no_normalized": "2"},
            {"page": 9, "sheet_no_normalized": "2"},
        ]
    }

    row, receipt = service._render_requested_full_page(
        query="Полное читаемое изображение листа 2 со штампом",
        graph=graph,
        source_pdf_path=source_pdf,
        asset_dir=tmp_path / "assets",
        preferred_pages=[],
    )

    assert row is None
    assert receipt["status"] == "ambiguous_sheet_mapping"
    assert receipt["candidate_pages"] == [4, 9]


def test_disclosure_verification_detects_tamper_and_raises_image_cap(tmp_path):
    output_dir = tmp_path / "auto-retry"
    image_dir = output_dir / "context_assets" / "case-eight" / "images"
    image_dir.mkdir(parents=True)
    images = []
    for index in range(8):
        path = image_dir / f"{index:02d}.png"
        path.write_bytes(f"image-{index}".encode())
        images.append({
            "path": str(path.resolve()),
            "block_id": f"B{index:02d}-AAA-BBB",
            "page": index + 1,
        })
    case = _case("case-eight", images=images)
    manifest_path = output_dir / "manifest.jsonl"
    inventory_path = output_dir / "inventory.json"
    manifest_path.write_text(json.dumps(case, ensure_ascii=False) + "\n", encoding="utf-8")
    inventory_path.write_text(json.dumps({"period": "2026-07"}), encoding="utf-8")
    disclosure_path = output_dir / "external_codex_disclosure.json"
    disclosure = audit_cli._build_external_disclosure(
        [case],
        manifest_path=manifest_path,
        inventory_path=inventory_path,
    )
    audit_cli._write_new_json(disclosure_path, disclosure)

    verified_cases, image_cap = audit_cli._verify_external_disclosure(
        output_dir=output_dir,
        disclosure_path=disclosure_path,
        batch_size=4,
        requested_max_batch_images=6,
    )
    assert [row["case_id"] for row in verified_cases] == ["case-eight"]
    assert image_cap == 8

    Path(images[7]["path"]).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="изменился"):
        audit_cli._verify_external_disclosure(
            output_dir=output_dir,
            disclosure_path=disclosure_path,
            batch_size=4,
            requested_max_batch_images=6,
        )


def test_output_destination_rejects_projects_tree_and_symlink(tmp_path):
    projects_root = tmp_path / "projects_v2"
    projects_root.mkdir()
    with pytest.raises(ValueError, match="projects_v2"):
        audit_cli._validate_output_destination(
            projects_root / "object" / "report",
            projects_root,
        )

    safe_root = tmp_path / "safe"
    safe_root.mkdir()
    link = tmp_path / "report-link"
    link.symlink_to(safe_root, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        audit_cli._validate_output_destination(link / "child", projects_root)



def test_spatial_composite_uses_page_crops_and_stamp_placeholder(tmp_path, monkeypatch):
    pillow = pytest.importorskip("PIL.Image")
    first = tmp_path / "first.png"
    second = tmp_path / "second.png"
    pillow.new("RGB", (120, 80), "red").save(first)
    pillow.new("RGB", (100, 100), "blue").save(second)
    catalog = [
        {
            "block_id": "PLAN-111-AAA",
            "page": 30,
            "coords_px": [0, 0, 400, 300],
            "image_path": str(first),
        },
        {
            "block_id": "VIEW-222-BBB",
            "page": 30,
            "coords_px": [400, 0, 800, 300],
            "image_path": str(second),
        },
        {
            "block_id": "STMP-333-CCC",
            "page": 30,
            "block_type": "stamp",
            "coords_px": [600, 300, 800, 500],
        },
    ]

    def fake_materialize(row, **_kwargs):
        if row.get("image_path"):
            return row, {"block_id": row["block_id"], "status": "ok"}
        return None, {"block_id": row["block_id"], "status": "not_found"}

    monkeypatch.setattr(service, "_materialize_retrieval_graphic", fake_materialize)
    row, receipt, material_receipts = service._render_spatial_page_composite(
        page_number=30,
        sheet_number="22",
        graph={"pages": [{"page": 30, "page_width": 800, "page_height": 500}]},
        catalog=catalog,
        output_dir=tmp_path,
        asset_dir=tmp_path / "assets",
        allow_remote_crops=False,
        source_pdf_path=None,
        remote_budget=None,
    )

    assert row is not None
    assert Path(row["image_path"]).is_file()
    assert receipt["status"] == "composite_ok"
    assert receipt["included_block_ids"] == ["PLAN-111-AAA", "VIEW-222-BBB"]
    assert receipt["placeholder_block_ids"] == ["STMP-333-CCC"]
    assert len(material_receipts) == 3


def test_deep_recovery_renders_current_and_ranked_full_pages(tmp_path, monkeypatch):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    _write_json(output_dir / "document_graph.json", {
        "pages": [
            {"page": 4, "text_blocks": [{"text": "Исходный план марки КР-17."}]},
            {"page": 9, "text_blocks": [{"text": "Спецификация КР-17 содержит массу 46,05 кг."}]},
        ]
    })
    case = _case("case-deep-pages", input_hash="deep-source")
    case["output_dir"] = str(output_dir)
    case["version_dir"] = str(output_dir.parent.parent)
    case["finding"].update({"page": 4, "problem": "Проверить массу КР-17"})

    def fake_render(*, page_number, source_pdf_path, asset_dir):
        path = tmp_path / f"page-{page_number}.png"
        path.write_bytes(f"page {page_number}".encode())
        return ({
            "block_id": f"full_page_{page_number}",
            "page": page_number,
            "label": f"Страница {page_number}",
            "searchable": f"Страница {page_number}",
            "image_path": str(path),
        }, {"status": "ok", "page": page_number})

    monkeypatch.setattr(service, "_render_pdf_page", fake_render)
    monkeypatch.setattr(service, "_collect_retrieval_graphics", lambda _case: [])
    monkeypatch.setattr(service, "_related_document_context", lambda *_args, **_kwargs: ("", {"status": "no_hits", "selected": []}))
    monkeypatch.setattr(service, "_deep_norm_context", lambda *_args, **_kwargs: {})

    enriched, receipt = service.enrich_case_for_retrieval(
        case,
        {
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Лист спецификации КР-17 с массой"],
        },
        max_images=3,
        asset_dir=tmp_path / "assets",
        recovery_mode=True,
    )

    assert receipt["contract_version"] == service.DEEP_RETRIEVAL_CONTRACT_VERSION
    assert [row["page"] for row in receipt["recovery_full_pages"]] == [9, 4]
    assert {row["block_id"] for row in enriched["context"]["images"]} == {
        "full_page_4",
        "full_page_9",
    }


def test_prepare_recovery_classifies_unsafe_and_recovers_empty_request(monkeypatch):
    recoverable = _case("case-recoverable", input_hash="hash-recoverable")
    unsafe = _case("case-unsafe", input_hash="hash-unsafe")
    unsafe["source_quality"] = "missing_source_item"
    results = {
        "case-recoverable": {
            "case_id": "case-recoverable",
            "input_hash": "hash-recoverable",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": [],
        },
        "case-unsafe": {
            "case_id": "case-unsafe",
            "input_hash": "hash-unsafe",
            "status": "success",
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Нужен источник"],
        },
    }

    def fake_enrich(case, _result, **_kwargs):
        enriched = json.loads(json.dumps(case))
        enriched["input_hash"] = "recovered-hash"
        return enriched, {
            "found": True,
            "material_delta": {"document_hits": 1},
        }

    monkeypatch.setattr(service, "enrich_case_for_retrieval", fake_enrich)
    selected, stats, rows = service.prepare_recovery_cases(
        [recoverable, unsafe],
        results,
    )

    assert [case["case_id"] for case in selected] == ["case-recoverable"]
    assert stats["selected_cases"] == 1
    assert stats["unsafe_source_quality"] == 1
    assert {row["category"] for row in rows} == {
        "recovered_context",
        "unsafe_source_quality",
    }


def test_exact_missing_block_request_does_not_add_semantic_neighbors(tmp_path, monkeypatch):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    output_dir.mkdir(parents=True)
    exact = tmp_path / "exact.png"
    decoy = tmp_path / "decoy.png"
    exact.write_bytes(b"exact")
    decoy.write_bytes(b"decoy")
    case = _case("case-exact-only", input_hash="exact-only")
    case["output_dir"] = str(output_dir)
    case["version_dir"] = str(output_dir.parent.parent)
    case["finding"].update({"source_block_ids": ["EXCT-111-AAA"], "page": 1})
    monkeypatch.setattr(
        service,
        "_collect_retrieval_graphics",
        lambda _case: [
            {
                "block_id": "EXCT-111-AAA",
                "page": 1,
                "block_type": "image",
                "label": "Точный требуемый блок",
                "searchable": "Точный требуемый блок",
                "image_path": str(exact),
            },
            {
                "block_id": "DCOY-222-BBB",
                "page": 1,
                "block_type": "image",
                "label": "Похожий соседний блок",
                "searchable": "Похожий соседний блок",
                "image_path": str(decoy),
            },
        ],
    )

    enriched, _receipt = service.enrich_case_for_retrieval(
        case,
        {
            "verdict": "insufficient_evidence",
            "binding_status": "exact",
            "missing_context": ["Фактическое изображение блока EXCT-111-AAA"],
        },
        max_images=3,
        asset_dir=tmp_path / "snapshot",
    )

    assert [row["block_id"] for row in enriched["context"]["images"]] == [
        "EXCT-111-AAA"
    ]



def test_norm_recovery_collects_all_direct_clauses_and_semantic_candidates(
    monkeypatch,
):
    import norms._core as norms_core
    norms_tools = str(_ROOT / "norms" / "tools")
    if norms_tools not in sys.path:
        sys.path.insert(0, norms_tools)
    import norms_api

    monkeypatch.setattr(
        norms_core,
        "extract_norms_from_text",
        lambda _text: ["СП 4.13130.2013", "ФЗ-123"],
    )
    monkeypatch.setattr(
        norms_api,
        "get_norm_status",
        lambda code: {
            "query": code,
            "found": code != "ФЗ-123",
            "matched_code": code,
            "status": "active",
            "doc_status": "active",
            "edition_status": "current",
            "authoritative": True,
            "resolution_reason": "exact",
            "file": f"{code}.md",
            "source": "vault",
        },
    )
    monkeypatch.setattr(
        norms_api,
        "get_paragraph",
        lambda code, paragraph, max_lines=12: {
            "found": paragraph in {"8.6", "90"},
            "matched_code": code,
            "paragraph": paragraph,
            "text": f"{code} точный пункт {paragraph}",
            "file": f"{code}.md",
            "line": 10,
            "status": "active",
            "doc_status": "active",
            "edition_status": "current",
            "authoritative": True,
            "replacement_doc": None,
            "truncated": False,
        },
    )
    monkeypatch.setattr(
        service,
        "_literal_norm_article",
        lambda code, article, status: {
            "code": status.get("matched_code") or code,
            "locator_kind": "article",
            "article": article,
            "text": "Буквальный текст статьи.",
            "file": status.get("file"),
        },
    )
    monkeypatch.setattr(
        norms_api,
        "semantic_search",
        lambda _query, top=5: [{
            "code": "СП 99.13330.2024",
            "paragraph": "7.2",
            "text": "Локально найденный кандидат нормы.",
            "file": "СП 99.13330.2024.md",
            "line": 42,
            "score": 0.9,
            "dense_score": 0.7,
        }],
    )

    context = service._authoritative_norm_context(
        {
            "norm": "СП 4.13130.2013, п. 8.6; ст. 90 ФЗ-123",
            "problem": "Проверить пожарный проезд",
        },
        semantic_query="требуемая ширина пожарного проезда",
    )

    assert context["kind"] == "norm_authoritative_bundle"
    assert {row.get("paragraph") or row.get("article") for row in context["clauses"]} == {"8.6", "90"}
    assert context["semantic_candidates"][0]["paragraph"] == "7.2"
    assert context["semantic_candidates"][0]["role"] == (
        "candidate_clause_from_local_norms_search"
    )


def test_norm_recovery_rebinds_only_unique_literal_match(monkeypatch):
    import norms._core as norms_core
    norms_tools = str(_ROOT / "norms" / "tools")
    if norms_tools not in sys.path:
        sys.path.insert(0, norms_tools)
    import norms_api

    codes = ["СП 4.13130.2013", "СП 70.13330.2012"]
    monkeypatch.setattr(
        norms_core,
        "extract_norms_from_text",
        lambda _text: codes,
    )
    monkeypatch.setattr(
        norms_api,
        "get_norm_status",
        lambda code: {
            "query": code,
            "found": True,
            "matched_code": code,
            "status": "active",
            "doc_status": "active",
            "edition_status": "current",
            "authoritative": True,
            "resolution_reason": "exact",
            "file": f"{code}.md",
            "source": "vault",
        },
    )
    monkeypatch.setattr(
        norms_api,
        "get_paragraph",
        lambda code, paragraph, max_lines=12: {
            "found": code == "СП 70.13330.2012" and paragraph == "5.18.7",
            "matched_code": code,
            "paragraph": paragraph,
            "text": "Единственный буквальный текст пункта.",
            "file": f"{code}.md",
            "line": 10,
            "status": "active",
            "doc_status": "active",
            "edition_status": "current",
            "authoritative": True,
            "replacement_doc": None,
            "truncated": False,
        },
    )
    monkeypatch.setattr(norms_api, "semantic_search", lambda *_args, **_kwargs: [])

    context = service._authoritative_norm_context({
        "norm": "п. 5.18.7 СП 4.13130.2013; СП 70.13330.2012",
    })

    assert context["unresolved_locators"] == []
    assert context["clauses"][0]["code"] == "СП 70.13330.2012"
    assert context["clauses"][0]["association"] == "fallback_unique_exact_match"
    assert context["clauses"][0]["originally_assigned_code"] == "СП 4.13130.2013"


def test_tail_specification_context_prioritizes_last_schedule_pages():
    graph = {
        "pages": [
            {"page": 1, "text_blocks": [{"text": "Общие данные проекта"}]},
            {"page": 8, "text_blocks": [{"text": "План расположения КР-17"}]},
            {
                "page": 19,
                "sheet_name": "Ведомость деталей",
                "text_blocks": [{
                    "text": "Ведомость деталей. Позиция КР-17. Масса 46,05 кг."
                }],
            },
            {
                "page": 20,
                "sheet_name": "Спецификация",
                "text_blocks": [{
                    "text": "Спецификация элементов. Обозначение и количество."
                }],
            },
        ]
    }

    text, receipt = service._tail_specification_context(
        graph,
        "Нужна спецификация и ведомость позиции КР-17",
    )

    assert receipt["status"] == "selected"
    assert receipt["selected_pages"] == [19, 20]
    assert "Масса 46,05 кг" in text


def test_graph_geometry_rehydrates_missing_page_dimensions(tmp_path):
    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "latest"
    input_dir = output_dir.parent.parent / "01_input"
    input_dir.mkdir(parents=True)
    _write_json(
        input_dir / "source_result.json",
        {
            "blocks": [{
                "block_id": "CROP-111-AAA",
                "page": 3,
                "type": "image",
                "coords_px": [100, 200, 300, 400],
            }]
        },
    )
    case = _case("case-geometry")
    case["version_dir"] = str(output_dir.parent.parent)
    graph = {
        "pages": [{
            "page": 3,
            "page_width": 4961,
            "page_height": 3508,
        }]
    }

    rows, _paths = service._load_input_graphics_catalog(case, output_dir, graph)
    row = next(item for item in rows if item["block_id"] == "CROP-111-AAA")

    assert row["page_width"] == 4961
    assert row["page_height"] == 3508


def test_archived_block_png_requires_hash_consistency(tmp_path, monkeypatch):
    first = tmp_path / "experiments" / "one" / "block_blk_exact.png"
    second = tmp_path / "experiments" / "two" / "block_blk_exact.png"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_bytes(b"same")
    second.write_bytes(b"same")
    monkeypatch.setattr(service, "ROOT_DIR", tmp_path)
    service._archived_block_png_index.cache_clear()
    service._verified_archived_block_png.cache_clear()

    path, receipt = service._verified_archived_block_png("blk_exact")

    assert path
    assert receipt["status"] == "ok"
    assert receipt["verified_copy_count"] == 2

    second.write_bytes(b"different")
    service._verified_archived_block_png.cache_clear()
    path, receipt = service._verified_archived_block_png("blk_exact")

    assert path is None
    assert receipt["status"] == "hash_conflict"
    service._archived_block_png_index.cache_clear()
    service._verified_archived_block_png.cache_clear()


def test_default_auto_retry_output_advances_or_resumes_compatible_snapshot(tmp_path):
    source = tmp_path / "source"
    legacy = source / "auto-retry-v1"
    legacy.mkdir(parents=True)
    assert audit_cli._default_auto_retry_output_dir(source) == source / "auto-retry-v2"

    current = source / "auto-retry-v2"
    current.mkdir()
    (current / "manifest.jsonl").write_text("{}\n", encoding="utf-8")
    (current / "inventory.json").write_text(
        json.dumps({
            "auto_retry": {
                "contract_version": audit_cli.AUTO_RETRY_CONTRACT_VERSION,
                "retrieval_contract_version": service.AUTO_RETRIEVAL_CONTRACT_VERSION,
            }
        }),
        encoding="utf-8",
    )
    (current / "external_codex_disclosure.json").write_text(
        json.dumps({"schema_version": audit_cli.DISCLOSURE_SCHEMA_VERSION}),
        encoding="utf-8",
    )

    assert audit_cli._default_auto_retry_output_dir(source) == current


def test_norm_reference_map_binds_locators_to_nearest_code():
    text = (
        "п. 8.6 СП 4.13130.2013; ст. 90 ФЗ-123; "
        "таблица 21 СП 2.13130.2020 и таблица 5.1 СП 468.1325800.2019"
    )

    mapping = service._norm_reference_map(
        text,
        [
            "СП 4.13130.2013",
            "ФЗ-123",
            "СП 2.13130.2020",
            "СП 468.1325800.2019",
        ],
    )

    assert mapping["СП 4.13130.2013"] == [
        {"kind": "paragraph", "value": "8.6"}
    ]
    assert mapping["ФЗ-123"] == [{"kind": "article", "value": "90"}]
    assert mapping["СП 2.13130.2020"] == [
        {"kind": "table", "value": "21"}
    ]
    assert mapping["СП 468.1325800.2019"] == [
        {"kind": "table", "value": "5.1"}
    ]


def test_literal_norm_table_is_extracted_from_local_vault(tmp_path, monkeypatch):
    vault = tmp_path / "norms" / "vault"
    vault.mkdir(parents=True)
    source = vault / "СП 2_document.md"
    source.write_text(
        "##### Т а б л и ц а 21\n"
        "| Степень огнестойкости | Предел |\n"
        "| I | REI 150 |\n"
        "##### Таблица 22\n"
        "| Следующая | Таблица |\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "ROOT_DIR", tmp_path)

    table = service._literal_norm_table(
        "СП 2.13130.2020",
        "21",
        {
            "source": "vault",
            "file": source.name,
            "matched_code": "СП 2.13130.2020",
            "status": "active",
            "doc_status": "active",
            "edition_status": "current",
            "authoritative": True,
        },
    )

    assert table["table"] == "21"
    assert "REI 150" in table["text"]
    assert "Следующая" not in table["text"]


def test_requested_sheet_pages_supports_lists_and_ranges():
    graph = {
        "pages": [
            {"page": number + 10, "sheet_no_normalized": str(number)}
            for number in range(1, 13)
        ]
    }

    pages = service._requested_sheet_pages(
        "Полные листы 4–6, а также листы 9 и 10",
        graph,
    )

    assert pages == [14, 15, 16, 19, 20]


def test_related_document_cues_cover_requested_project_sections():
    requested = service._requested_related_disciplines(
        "Нужны ПЗУ, гидравлический расчёт ИОС, ППР и чертежи ЭОМ"
    )

    assert requested == ["GP", "PZ", "VK", "POS", "EOM"]

def test_expanded_locator_values_supports_nested_paragraph_range():
    assert service._expanded_locator_values("5.18.7–5.18.10") == [
        "5.18.7",
        "5.18.8",
        "5.18.9",
        "5.18.10",
    ]


def test_norm_reference_map_keeps_semicolon_segments_and_sanpin():
    text = (
        "СП 42.13330.2016 (действует), табл. 7.5; "
        "СанПиН 2.1.3684-21, п. 2.6"
    )

    mapping = service._norm_reference_map(
        text,
        ["СП 42.13330.2016", "СанПиН 2.1.3684-21"],
    )

    assert mapping["СП 42.13330.2016"] == [
        {"kind": "table", "value": "7.5"}
    ]
    assert mapping["СанПиН 2.1.3684-21"] == [
        {"kind": "paragraph", "value": "2.6"}
    ]


def test_literal_norm_article_is_extracted_from_local_vault(tmp_path, monkeypatch):
    vault = tmp_path / "norms" / "vault"
    vault.mkdir(parents=True)
    source = vault / "123-ФЗ_document.md"
    source.write_text(
        "###### Статья 90. Обеспечение деятельности\n"
        "1. Должны быть обеспечены пожарные проезды.\n"
        "###### Статья 91. Следующая статья\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "ROOT_DIR", tmp_path)

    article = service._literal_norm_article(
        "ФЗ-123",
        "90",
        {
            "source": "vault",
            "file": source.name,
            "matched_code": "ФЗ 123-ФЗ",
            "status": "active",
            "authoritative": True,
        },
    )

    assert article["article"] == "90"
    assert "пожарные проезды" in article["text"]
    assert "Следующая статья" not in article["text"]


def test_norm_status_with_alias_resolves_reversed_federal_law_number():
    def lookup(code):
        return {
            "found": code == "123-ФЗ",
            "matched_code": "ФЗ 123-ФЗ" if code == "123-ФЗ" else None,
            "source": "vault" if code == "123-ФЗ" else "not_found",
        }

    status = service._norm_status_with_alias("ФЗ-123", lookup)

    assert status["found"] is True
    assert status["matched_code"] == "ФЗ 123-ФЗ"
    assert status["query_alias"] == "ФЗ-123"


def test_norm_reference_map_keeps_repeated_paragraph_abbreviations():
    mapping = service._norm_reference_map(
        "СП 32.13330.2018, п. 6.2.1, п. 7.2, п. 8.1.14",
        ["СП 32.13330.2018"],
    )

    assert mapping["СП 32.13330.2018"] == [
        {"kind": "paragraph", "value": "6.2.1"},
        {"kind": "paragraph", "value": "7.2"},
        {"kind": "paragraph", "value": "8.1.14"},
    ]
