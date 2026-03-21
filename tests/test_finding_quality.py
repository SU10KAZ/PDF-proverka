"""Tests for deterministic finding quality heuristics."""

import json

from webapp.services.finding_quality import (
    enrich_findings_file,
    enrich_findings_payload,
    evaluate_finding_practicality,
    should_review_practicality,
)


def test_engineering_issue_scores_high():
    finding = {
        "severity": "КРИТИЧЕСКОЕ",
        "category": "evacuation",
        "problem": "Ширина лестничного марша 1000 мм при нормативном минимуме 1200 мм",
        "description": "Дефицит 200 мм подтвержден на двух листах и требует переделки.",
        "risk": "Угроза безопасности при эвакуации, срыв согласования и переделка.",
        "solution": "Увеличить ширину марша и скорректировать конструкцию.",
        "evidence": [{"type": "image", "block_id": "b1", "page": 12}],
        "related_block_ids": ["b1"],
    }

    quality = evaluate_finding_practicality(finding)

    assert quality["engineering_relevance"] == "high"
    assert quality["practicality_score"] >= 70
    assert quality["likely_formal_only"] is False
    assert "safety" in quality["impact_axes"]


def test_formal_reference_typo_scores_low():
    finding = {
        "severity": "КРИТИЧЕСКОЕ",
        "category": "normative_refs",
        "problem": "Опечатка в номере СП",
        "description": "В ведомости ссылочных документов указан неверный номер документа.",
        "risk": "Формальное замечание экспертизы.",
        "solution": "Исправить номер СП в ведомости ссылочных документов.",
        "evidence": [{"type": "image", "block_id": "b1", "page": 1}],
        "related_block_ids": ["b1"],
    }

    quality = evaluate_finding_practicality(finding)

    assert quality["engineering_relevance"] == "low"
    assert quality["likely_formal_only"] is True
    assert quality["severity_mismatch"] is True
    assert should_review_practicality({"severity": "КРИТИЧЕСКОЕ", "quality": quality}) is True


def test_documentation_conflict_with_real_risk_is_not_formal_only():
    finding = {
        "severity": "КРИТИЧЕСКОЕ",
        "category": "documentation",
        "problem": "Расхождение расчетной нагрузки на анкерную пластину",
        "description": "В таблице 15 кН, в примечании 12 кН, расхождение 25%.",
        "risk": "Неправильный подбор анкеров может привести к аварийному обрушению подъемника.",
        "solution": "Уточнить расчет и скорректировать рабочую документацию.",
        "evidence": [{"type": "image", "block_id": "b1", "page": 7}],
        "related_block_ids": ["b1"],
    }

    quality = evaluate_finding_practicality(finding)

    assert quality["engineering_relevance"] in {"high", "medium"}
    assert quality["likely_formal_only"] is False
    assert "safety" in quality["impact_axes"]


def test_enrich_payload_and_file(tmp_path):
    payload = {
        "meta": {},
        "findings": [
            {
                "id": "F-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "documentation",
                "problem": "Опечатка в тексте",
                "description": "В примечании слово написано с ошибкой.",
                "risk": "Минимальный.",
                "solution": "Исправить опечатку.",
            }
        ],
    }

    stats = enrich_findings_payload(payload)
    assert stats["total"] == 1
    assert "quality" in payload["findings"][0]
    assert payload["meta"]["quality_summary"]["total"] == 1

    findings_path = tmp_path / "03_findings.json"
    findings_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    file_stats = enrich_findings_file(findings_path)
    assert file_stats["total"] == 1

    persisted = json.loads(findings_path.read_text(encoding="utf-8"))
    assert persisted["meta"]["quality_summary"]["total"] == 1
    assert "quality" in persisted["findings"][0]
