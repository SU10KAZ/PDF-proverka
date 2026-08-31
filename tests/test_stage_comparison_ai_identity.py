"""Разрешение идентичности строк таблиц: модель называет пару, правила её проверяют.

Здесь проверяется главное обещание ветки: ИИ решает МИНИМАЛЬНЫЙ вопрос, а
значения считает существующий детерминированный сравниватель. Всё остальное —
следствия: верификатор не смягчается ради процента разрешённого, добор
доказательств бывает ровно один и только из закрытого справочника, а
доказательство обязано РАЗЛИЧАТЬ выбранную строку от соседних кандидатов.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding import (  # noqa: E402
    electrical_table_diff as etd,
)
from backend.app.services.stage_comparison.ai import (  # noqa: E402
    identity,
    response_contract,
    routing,
)


# ── Фабрики ────────────────────────────────────────────────────────────────

def _value(facet: str, number: float, raw: str) -> dict:
    return {"facet_ref": facet, "values": [number], "unit": "квт", "raw": raw}


def _row(
    row_id: str,
    *,
    side: str,
    designation: str,
    label: str | None = None,
    section=None,
    kind: str = "FEEDER",
    values: list | None = None,
    row_designations: list | None = None,
) -> dict:
    return {
        "row_id": row_id,
        "side": side,
        "consumer_label": label or designation,
        "consumer_designation": designation,
        "own_designations": [designation],
        "row_designations": [
            {"designation": value} for value in (row_designations or ())
        ],
        "feeder_designations": [],
        "section_ref": section,
        "input_number": None,
        "row_kind": kind,
        "mode_label": None,
        "cables": [],
        "values": list(values or ()),
        "binding_status": "BOUND",
        "designation_sources": {"own_label": [designation]},
        "page": 0,
        "bbox": [0, 0, 1, 1],
    }


def _tables(left: list[dict], right: list[dict]) -> dict:
    return {"LEFT": {"rows": left}, "RIGHT": {"rows": right}}


def _inventory(entries: list[dict]) -> dict:
    return {"items": entries, "counts": {}, "decisions": list(routing.DECISIONS)}


def _unproven_entry(row_id: str, candidates: list[str], *, side: str = "LEFT") -> dict:
    return {
        "item_id": row_id,
        "kind": routing.KIND_TABLE_UNPROVEN,
        "decision": routing.ELIGIBLE,
        "unresolved": True,
        "subject": "ШУ-ХЦ",
        "routing_payload": {
            "row_id": row_id, "side": side, "candidate_row_ids": candidates,
        },
    }


def _answer(question, **overrides) -> dict:
    payload = {
        "question_id": question.question_id,
        "verdict": identity.VERDICT_SAME,
        "left_row_ref": "L1",
        "right_row_ref": "R1",
        "shared_identity": None,
        "arithmetic_total": None,
        "arithmetic_addends": [],
        "evidence_quotes": [],
        "confidence": "HIGH",
        "human_question": None,
        "engineering_summary": "—",
        "need_more_evidence": None,
    }
    payload.update(overrides)
    return payload


def _shu_case() -> tuple[identity.IdentityQuestion, dict, dict]:
    """Боевой случай: слева «ШУ-ХЦ», справа колонка «ВРУ-ХЦ», но над ней
    напечатано «ШУ-ХЦ» — независимое доказательство тождества."""
    left = _row(
        "etrow_l", side="LEFT", designation="ШУ-ХЦ", section="РП1",
        values=[_value("demand_active_power_kw", 13.7, "Рр=13,7кВт")],
    )
    right = _row(
        "etrow_r", side="RIGHT", designation="ВРУ-ХЦ", section="РП1",
        label="ГРЩ1-РП1-7 | ВРУ-ХЦ ввод 1", row_designations=["ШУ-ХЦ"],
        values=[_value("demand_active_power_kw", 37.5, "37.5 кВт")],
    )
    tables = _tables([left], [right])
    questions = identity.build_questions(
        inventory=_inventory([_unproven_entry("etrow_l", ["etrow_r"])]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    rows = {"etrow_l": left, "etrow_r": right}
    return questions[0], tables, rows


# ── Схема и контракт ───────────────────────────────────────────────────────

def test_схема_ответа_проверяема_валидатором_контракта():
    """anyOf валидатор не поддерживает и считает НЕПРОВЕРЯЕМЫМ весь ответ.

    Схема, которую нельзя проверить, — это отсутствующая гарантия, выглядящая
    как выполненная.
    """
    question, _tables_, _rows = _shu_case()
    problems = response_contract.validate(
        {"resolutions": [_answer(question, shared_identity="ШУ-ХЦ")]},
        identity.IDENTITY_SCHEMA,
    )
    assert problems == []


def test_вердикт_вне_перечня_отклоняется_контрактом():
    question, _tables_, _rows = _shu_case()
    problems = response_contract.validate(
        {"resolutions": [_answer(question, verdict="ВОЗМОЖНО")]},
        identity.IDENTITY_SCHEMA,
    )
    assert problems


# ── Верификатор ────────────────────────────────────────────────────────────

def test_тождество_по_напечатанному_обозначению_принимается():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(
        question,
        _answer(
            question, shared_identity="ШУ-ХЦ",
            evidence_quotes=[
                {"side": "RIGHT", "row_ref": "R1", "quote": "обозначения: ВРУ-ХЦ, ШУ-ХЦ"},
            ],
        ),
    )
    assert result.ok, result.errors


def test_общая_приставка_обозначением_не_является():
    """«ВРУ» есть и в «ВРУ-А», и в «ВРУ-АПТ»; связывать по ней нельзя."""
    left = _row("etrow_l", side="LEFT", designation="ВРУ-А", section="РП1")
    right = _row("etrow_r", side="RIGHT", designation="ВРУ-АПТ", section="РП1")
    tables = _tables([left], [right])
    questions = identity.build_questions(
        inventory=_inventory([_unproven_entry("etrow_l", ["etrow_r"])]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    result = identity.verify_identity(
        questions[0], _answer(questions[0], shared_identity="ВРУ"),
    )
    assert not result.ok
    assert any("обозначением" in error for error in result.errors)


def test_подпись_общая_у_двух_кандидатов_ничего_не_доказывает():
    """Две линии «2ГРЩ-ВРУ3» на одном листе: подпись не выбирает между ними."""
    left_a = _row(
        "etrow_a", side="LEFT", designation="ВРУ3", section="РП2",
        label="2ГРЩ-ВРУ3 ППГнг(А)-НF 2х(5х95)",
        values=[_value("demand_active_power_kw", 72.7, "Рр=72,7кВт")],
    )
    left_b = _row(
        "etrow_b", side="LEFT", designation="ВРУ3", section="РП2",
        label="2ГРЩ-ВРУ3 ППГнг(А)-НF 3х(5х120)",
        values=[_value("demand_active_power_kw", 183.9, "Рр=183,9кВт")],
    )
    right = _row(
        "etrow_r", side="RIGHT", designation="ВРУ3", section="РП2",
        values=[_value("demand_active_power_kw", 140.9, "140.9 кВт")],
    )
    total = _row(
        "etrow_t", side="LEFT", designation="ВРУ3", kind="CONSUMER_TOTAL",
        values=[_value("demand_active_power_kw", 143.2, "Рр=143,2/176,8кВт")],
    )
    rp1 = _row(
        "etrow_p", side="LEFT", designation="ВРУ3", section="РП1",
        label="1ГРЩ-ВРУ3 ППГнг(А)-НF 2х(5х95)",
        values=[_value("demand_active_power_kw", 70.5, "Рр=70.5кВт")],
    )
    tables = _tables([left_a, left_b, total, rp1], [right])
    questions = identity.build_questions(
        inventory=_inventory([{
            "item_id": "etm_1",
            "kind": routing.KIND_TABLE_BLOCKED,
            "decision": routing.ELIGIBLE,
            "unresolved": True,
            "subject": "ВРУ3",
            "routing_payload": {
                "left_row_ids": ["etrow_a", "etrow_b"],
                "right_row_ids": ["etrow_r"],
            },
        }]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    question = questions[0]

    by_signature = identity.verify_identity(
        question, _answer(question, shared_identity="ВРУ3"),
    )
    assert not by_signature.ok
    assert by_signature.warnings

    total_ref = next(
        line["ref"] for line in question.context
        if line["role"] == "CONSUMER_TOTAL" and "143,2" in line["text"]
    )
    added = identity.expand(
        question,
        {
            "missing_evidence_type": identity.NEED_SAME_DESIGNATION,
            "requested_entity": "ВРУ3",
            "requested_side": "LEFT",
        },
        load_tables=tables,
    )
    rp1_ref = next(line["ref"] for line in added if "1ГРЩ-ВРУ3" in line["text"])

    proven = identity.verify_identity(question, _answer(
        question, left_row_ref="L1", shared_identity="ВРУ3",
        arithmetic_total={"row_ref": total_ref, "value": "143,2"},
        arithmetic_addends=[
            {"row_ref": "L1", "value": "72,7"},
            {"row_ref": rp1_ref, "value": "70.5"},
        ],
    ))
    assert proven.ok, proven.errors

    wrong = identity.verify_identity(question, _answer(
        question, left_row_ref="L2", shared_identity="ВРУ3",
        arithmetic_total={"row_ref": total_ref, "value": "143,2"},
        arithmetic_addends=[
            {"row_ref": "L2", "value": "183,9"},
            {"row_ref": rp1_ref, "value": "70.5"},
        ],
    ))
    assert not wrong.ok
    assert any("не сходится" in error for error in wrong.errors)


def test_выдуманное_число_отклоняется():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(question, _answer(
        question, shared_identity="ШУ-ХЦ",
        arithmetic_total={"row_ref": "L1", "value": "999,9"},
        arithmetic_addends=[
            {"row_ref": "L1", "value": "13,7"},
            {"row_ref": "L1", "value": "13,7"},
        ],
    ))
    assert not result.ok
    assert any("дословно" in error for error in result.errors)


def test_выдуманная_цитата_отклоняет_ответ():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(question, _answer(
        question, shared_identity="ШУ-ХЦ",
        evidence_quotes=[{"side": "LEFT", "row_ref": "L1", "quote": "Рр=999кВт"}],
    ))
    assert not result.ok
    assert any("нет в строке" in error for error in result.errors)


def test_перепутанные_стороны_отклоняются():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(question, _answer(
        question, left_row_ref="R1", right_row_ref="L1", shared_identity="ШУ-ХЦ",
    ))
    assert not result.ok


def test_несуществующая_строка_отклоняется():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(question, _answer(
        question, left_row_ref="L9", shared_identity="ШУ-ХЦ",
    ))
    assert not result.ok
    assert any("в пакете нет" in error for error in result.errors)


def test_внутренний_идентификатор_в_ответе_отклоняется():
    question, _tables_, _rows = _shu_case()
    result = identity.verify_identity(question, _answer(
        question, shared_identity="ШУ-ХЦ",
        engineering_summary="Совпадает с etrow_l",
    ))
    assert not result.ok


def test_строки_разных_секций_одним_объектом_не_объявляются():
    left = _row("etrow_l", side="LEFT", designation="ШУ-ХЦ", section="РП1")
    right = _row("etrow_r", side="RIGHT", designation="ШУ-ХЦ", section="РП2")
    tables = _tables([left], [right])
    questions = identity.build_questions(
        inventory=_inventory([_unproven_entry("etrow_l", ["etrow_r"])]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    result = identity.verify_identity(
        questions[0], _answer(questions[0], shared_identity="ШУ-ХЦ"),
    )
    assert not result.ok
    assert any("секции" in error for error in result.errors)


def test_отказ_модели_ничего_не_публикует_и_не_ругается():
    question, _tables_, _rows = _shu_case()
    for verdict in (identity.VERDICT_DIFFERENT, identity.VERDICT_INSUFFICIENT):
        result = identity.verify_identity(question, _answer(question, verdict=verdict))
        assert result.ok, (verdict, result.errors)


# ── Добор доказательств ────────────────────────────────────────────────────

def test_запрос_вне_справочника_ничего_не_добирает():
    question, tables, _rows = _shu_case()
    before = len(question.context)
    added = identity.expand(
        question,
        {"missing_evidence_type": "ВЕСЬ_ЛИСТ", "requested_entity": "ШУ-ХЦ",
         "requested_side": "BOTH"},
        load_tables=tables,
    )
    assert added == []
    assert len(question.context) == before


def test_добор_ограничен_по_числу_строк():
    left = [
        _row(f"etrow_l{index}", side="LEFT", designation="ШУ-ХЦ", section="РП1")
        for index in range(30)
    ]
    right = [_row("etrow_r", side="RIGHT", designation="ВРУ-ХЦ", section="РП1")]
    tables = _tables(left, right)
    questions = identity.build_questions(
        inventory=_inventory([_unproven_entry("etrow_l0", ["etrow_r"])]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    added = identity.expand(
        questions[0],
        {"missing_evidence_type": identity.NEED_NEIGHBOUR_ROWS,
         "requested_entity": "ШУ-ХЦ", "requested_side": "LEFT"},
        load_tables=tables,
    )
    assert len(added) <= identity.EXPANSION_LIMIT


def test_запрос_добора_проверяется_по_справочнику():
    question, _tables_, _rows = _shu_case()
    bad = identity.verify_identity(question, _answer(
        question, verdict=identity.VERDICT_NEED_EVIDENCE,
        need_more_evidence={
            "missing_evidence_type": "ВСЁ_ПОДРЯД",
            "requested_entity": "ШУ-ХЦ",
            "requested_side": "LEFT",
        },
    ))
    assert not bad.ok
    good = identity.verify_identity(question, _answer(
        question, verdict=identity.VERDICT_NEED_EVIDENCE,
        need_more_evidence={
            "missing_evidence_type": identity.NEED_SECTION_SUMMARY,
            "requested_entity": "ШУ-ХЦ",
            "requested_side": "LEFT",
        },
    ))
    assert good.ok, good.errors


# ── Партии ─────────────────────────────────────────────────────────────────

def test_вопросы_одного_раздела_идут_одной_партией():
    """Партии по семейству потребителя давали по одному вопросу на обращение."""
    left = [
        _row(f"etrow_l{index}", side="LEFT", designation=f"ШУ-{index}", section="РП1")
        for index in range(4)
    ]
    right = [
        _row(f"etrow_r{index}", side="RIGHT", designation=f"ВРУ-{index}", section="РП1")
        for index in range(4)
    ]
    tables = _tables(left, right)
    entries = [
        _unproven_entry(f"etrow_l{index}", [f"etrow_r{index}"]) for index in range(4)
    ]
    for index, entry in enumerate(entries):
        entry["item_id"] = f"etrow_l{index}"
    questions = identity.build_questions(
        inventory=_inventory(entries), load_tables=tables,
    )
    packages = identity.pack(questions, batch_size=10)
    assert len(questions) == 4
    assert len(packages) == 1


def test_общий_контекст_партии_печатается_один_раз():
    left = [
        _row("etrow_l0", side="LEFT", designation="ШУ-1", section="РП1"),
        _row("etrow_l1", side="LEFT", designation="ШУ-2", section="РП1"),
        _row("etrow_t", side="LEFT", designation="ВРУ1", kind="CONSUMER_TOTAL"),
    ]
    right = [
        _row("etrow_r0", side="RIGHT", designation="ВРУ-1", section="РП1"),
        _row("etrow_r1", side="RIGHT", designation="ВРУ-2", section="РП1"),
    ]
    tables = _tables(left, right)
    questions = identity.build_questions(
        inventory=_inventory([
            _unproven_entry("etrow_l0", ["etrow_r0"]),
            {**_unproven_entry("etrow_l1", ["etrow_r1"]), "item_id": "etrow_l1"},
        ]),
        load_tables=tables,
    )
    identity.attach_base_context(questions, load_tables=tables)
    view = identity.pack(questions, batch_size=10)[0].model_view()
    assert view["context_rows"]
    assert all("context_rows" not in item for item in view["questions"])


def test_ни_один_допущенный_элемент_не_теряется_при_сборке():
    left = [
        _row(f"etrow_l{index}", side="LEFT", designation=f"ШУ-{index}", section="РП1")
        for index in range(7)
    ]
    right = [
        _row(f"etrow_r{index}", side="RIGHT", designation=f"ВРУ-{index}", section="РП1")
        for index in range(7)
    ]
    tables = _tables(left, right)
    entries = []
    for index in range(7):
        entry = _unproven_entry(f"etrow_l{index}", [f"etrow_r{index}"])
        entry["item_id"] = f"etrow_l{index}"
        entries.append(entry)
    questions = identity.build_questions(
        inventory=_inventory(entries), load_tables=tables,
    )
    packed = [
        question.question_id
        for package in identity.pack(questions, batch_size=3)
        for question in package.questions
    ]
    assert sorted(packed) == sorted(q.question_id for q in questions)
    assert len(packed) == 7


# ── Изменения считает Python, а не модель ──────────────────────────────────

def test_изменения_по_доказанной_паре_считает_детерминированный_сравниватель():
    question, _tables_, rows = _shu_case()
    answer = _answer(question, shared_identity="ШУ-ХЦ")
    match = identity.match_from(question, answer, rows)
    assert match["method"] == identity.METHOD_AI_IDENTITY
    result = identity.deterministic_changes([match], etd.compare_match)
    assert len(result["changes"]) == 1
    change = result["changes"][0]
    assert change["before_value"] == 13.7
    assert change["after_value"] == 37.5
    # Значения взяты из строк, а не из ответа: в ответе их не было вовсе.
    assert "before_value" not in answer
    assert change["confidence"] == "MEDIUM"
    assert change["resolved_by"] == "AI_IDENTITY"


def test_оговорка_детерминированного_сравнивателя_заменена_честной():
    question, _tables_, rows = _shu_case()
    match = identity.match_from(question, _answer(question, shared_identity="ШУ-ХЦ"), rows)
    change = identity.deterministic_changes([match], etd.compare_match)["changes"][0]
    assert "только по обозначению потребителя" not in " ".join(change["notes"])
    assert any("проверена правилами" in note for note in change["notes"])


def test_обозначение_берётся_из_строк_а_не_из_ответа_модели():
    """Подпись на листе может врать — ровно поэтому вопрос и задавался. Но
    выдумывать её взамен системе нельзя."""
    question, _tables_, rows = _shu_case()
    match = identity.match_from(question, _answer(question, shared_identity="ШУ-ХЦ"), rows)
    assert match["designation"] == "ВРУ-ХЦ"
