"""compute_text_evidence терпит обе формы evidence_text_refs.

Регрессия: модель пишет ссылку то словарём, то голой строкой-идентификатором
("page_7_text"). Строковая форма роняла /api/findings/{id}/block-map и
/api/optimization/{id}/block-map с AttributeError: 'str' object has no
attribute 'get' (findings_service.compute_text_evidence). Остальные шесть
потребителей поля уже проверяли форму — здесь guard был пропущен.
"""
from backend.app.services.findings.findings_service import compute_text_evidence


GRAPH = {
    "pages": [
        {"page": 7, "text_blocks": [
            {"id": "blk_aaa", "text": "Ведомость рабочих чертежей"},
            {"id": "blk_bbb", "text": "Спецификация перемычек"},
        ]},
    ]
}


def test_string_ref_does_not_crash_and_is_ignored_when_unresolvable():
    """Голая строка не роняет расчёт; нерезолвимый id просто отбрасывается.

    Именно эта форма ("page_7_text" — выдумка модели, а не id блока графа)
    и встречается в боевых данных: 111 штук в 17 документах.
    """
    findings = [{"id": "F-001", "evidence_text_refs": ["page_7_text"]}]

    result = compute_text_evidence(GRAPH, {}, findings)

    assert "F-001" not in result


def test_string_ref_resolves_when_it_is_a_real_text_block_id():
    findings = [{"id": "F-001", "evidence_text_refs": ["blk_aaa"]}]

    result = compute_text_evidence(GRAPH, {}, findings)

    assert [r["text_block_id"] for r in result["F-001"]] == ["blk_aaa"]
    assert result["F-001"][0]["page"] == 7
    assert result["F-001"][0]["role"] == ""


def test_mixed_list_keeps_dict_refs_intact():
    """Смешанный список — как в боевых 03_findings.json: словари + строки."""
    findings = [{
        "id": "F-024",
        "evidence_text_refs": [
            "page_5_text",
            {"text_block_id": "blk_aaa", "role": "primary", "used_for": "проблема"},
            "page_7_text",
            {"text_block_id": "blk_bbb"},
        ],
    }]

    result = compute_text_evidence(GRAPH, {}, findings)

    refs = result["F-024"]
    assert [r["text_block_id"] for r in refs] == ["blk_aaa", "blk_bbb"]
    assert refs[0]["role"] == "primary"
    assert refs[0]["used_for"] == "проблема"
    assert refs[1]["role"] == ""


def test_dict_ref_falls_back_to_block_id_key():
    """graph_builder читает text_block_id ИЛИ block_id — держим тот же контракт."""
    findings = [{"id": "F-002", "evidence_text_refs": [{"block_id": "blk_bbb"}]}]

    result = compute_text_evidence(GRAPH, {}, findings)

    assert [r["text_block_id"] for r in result["F-002"]] == ["blk_bbb"]


def test_none_and_empty_refs_are_skipped():
    findings = [{"id": "F-003", "evidence_text_refs": [None, "", {}, "blk_aaa"]}]

    result = compute_text_evidence(GRAPH, {}, findings)

    assert [r["text_block_id"] for r in result["F-003"]] == ["blk_aaa"]
