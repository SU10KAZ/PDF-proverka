"""Проход слоя по идентичности: партии, добор, бюджеты и происхождение находки.

Модель здесь подменяется инъекцией `call=` — тем же способом, каким её
подменяют остальные тесты ИИ-слоя. Проверяется поведение слоя, а не качество
модели: сколько обращений он делает, что происходит с ответом, который не
прошёл проверку, и остаётся ли элемент у человека, когда доказать ничего не
удалось.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding import (  # noqa: E402
    electrical_table_diff as etd,
)
from backend.app.services.stage_comparison import preliminary_report as pr  # noqa: E402
from backend.app.services.stage_comparison.ai import (  # noqa: E402
    gateway,
    identity,
    resolution as ai_resolution,
    routing,
)


def _value(facet: str, number: float, raw: str) -> dict:
    return {"facet_ref": facet, "values": [number], "unit": "квт", "raw": raw}


def _row(row_id, *, side, designation, section="РП1", kind="FEEDER",
         label=None, values=None, row_designations=None) -> dict:
    return {
        "row_id": row_id, "side": side,
        "consumer_label": label or designation,
        "consumer_designation": designation,
        "own_designations": [designation],
        "row_designations": [{"designation": v} for v in (row_designations or ())],
        "feeder_designations": [], "section_ref": section, "input_number": None,
        "row_kind": kind, "mode_label": None, "cables": [],
        "values": list(values or ()), "binding_status": "BOUND",
        "designation_sources": {"own_label": [designation]},
        "page": 0, "bbox": [0, 0, 1, 1],
    }


def _pair_tables() -> dict:
    return {
        "LEFT": {"rows": [
            _row("etrow_l", side="LEFT", designation="ШУ-ХЦ",
                 values=[_value("demand_active_power_kw", 13.7, "Рр=13,7кВт")]),
        ]},
        "RIGHT": {"rows": [
            _row("etrow_r", side="RIGHT", designation="ВРУ-ХЦ",
                 row_designations=["ШУ-ХЦ"],
                 values=[_value("demand_active_power_kw", 37.5, "37.5 кВт")]),
        ]},
    }


def _inventory(count: int = 1) -> dict:
    items = []
    for index in range(count):
        items.append({
            "item_id": "etrow_l" if index == 0 else f"etrow_l{index}",
            "kind": routing.KIND_TABLE_UNPROVEN,
            "decision": routing.ELIGIBLE,
            "unresolved": True,
            "subject": "ШУ-ХЦ",
            "routing_payload": {
                "row_id": "etrow_l", "side": "LEFT",
                "candidate_row_ids": ["etrow_r"],
            },
        })
    return {"items": items}


def _recorder(builder):
    """Инъекция вызова модели: считает обращения и отвечает по пакету."""
    calls: list[dict] = []

    def call(family, prompt, *, model, schema, reasoning_level, retries,
             cancel, system_prompt=None, images=(), run_id=""):
        body = prompt.split("ВХОДНЫЕ ДАННЫЕ (JSON)\n", 1)[1]
        body = body.rsplit("\n\nОтветь", 1)[0]
        package = json.loads(body)
        calls.append(package)
        return gateway.CallResult(
            provider_family=family, model=model, reasoning_level=reasoning_level,
            ok=True, parsed={"resolutions": builder(package, len(calls))},
            duration_ms=1, attempts=1, session_id="test",
        )

    return call, calls


def _same_entity(package, _attempt):
    return [{
        "question_id": question["question_id"],
        "verdict": identity.VERDICT_SAME,
        "left_row_ref": "L1", "right_row_ref": "R1",
        "shared_identity": "ШУ-ХЦ",
        "arithmetic_total": None, "arithmetic_addends": [],
        "evidence_quotes": [], "confidence": "HIGH", "human_question": None,
        "engineering_summary": "Правый лист печатает «ШУ-ХЦ» над колонкой.",
        "need_more_evidence": None,
    } for question in package["questions"]]


def _layer(call):
    return ai_resolution.AiResolutionLayer(cache_dir=None, call=call, mode="STANDARD")


# ── Доказанное тождество даёт детерминированные изменения ──────────────────

def test_доказанное_тождество_превращается_в_изменения_правилами():
    call, calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    assert len(calls) == 1
    assert section["diagnostics"]["identity_resolved"] == 1
    assert section["diagnostics"]["derived_changes"] == 1
    change = section["derived_changes"][0]
    assert (change["before_value"], change["after_value"]) == (13.7, 37.5)
    assert section["resolved_row_ids"] == ["etrow_l", "etrow_r"]


def test_без_сравнивателя_изменения_не_выдумываются():
    call, _calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(), compare_match=None,
    )
    assert section["diagnostics"]["identity_resolved"] == 1
    assert section["derived_changes"] == []


# ── Провал проверки не публикуется ─────────────────────────────────────────

def test_недоказанное_тождество_остаётся_человеку():
    def wrong(package, _attempt):
        return [{**item, "shared_identity": "ВРУ"} for item in _same_entity(package, 1)]

    call, _calls = _recorder(wrong)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    assert section["diagnostics"]["identity_resolved"] == 0
    assert section["derived_changes"] == []
    record = section["resolutions"][0]
    assert record["status"] == ai_resolution.HUMAN_REQUIRED
    assert record["reason_code"] == ai_resolution.REASON_IDENTITY_UNPROVEN
    assert record["verifier"]["errors"]


def test_ответ_не_по_схеме_не_считается_ни_разрешением_ни_отказом():
    def broken(_package, _attempt):
        return [{"question_id": "нет такого", "verdict": "SAME_ENTITY"}]

    call, _calls = _recorder(broken)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    record = section["resolutions"][0]
    assert record["status"] == ai_resolution.HUMAN_REQUIRED
    assert record["reason_code"] == ai_resolution.REASON_MODEL_FAILED


def test_отказ_модели_закрывает_вопрос_без_находки():
    def declines(package, _attempt):
        return [{
            **item, "verdict": identity.VERDICT_DIFFERENT,
            "left_row_ref": None, "right_row_ref": None, "shared_identity": None,
        } for item in _same_entity(package, 1)]

    call, _calls = _recorder(declines)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    record = section["resolutions"][0]
    assert record["reason_code"] == ai_resolution.REASON_IDENTITY_DIFFERENT
    assert section["derived_changes"] == []


# ── Добор доказательств ────────────────────────────────────────────────────

def test_добор_делает_ровно_один_повтор():
    def asks_then_answers(package, attempt):
        if attempt == 1:
            return [{
                **item,
                "verdict": identity.VERDICT_NEED_EVIDENCE,
                "left_row_ref": None, "right_row_ref": None,
                "shared_identity": None,
                "need_more_evidence": {
                    "missing_evidence_type": identity.NEED_NEIGHBOUR_ROWS,
                    "requested_entity": "ШУ-ХЦ",
                    "requested_side": "LEFT",
                },
            } for item in _same_entity(package, attempt)]
        return _same_entity(package, attempt)

    tables = _pair_tables()
    tables["LEFT"]["rows"].append(
        _row("etrow_n", side="LEFT", designation="ШУ-ХВС")
    )
    call, calls = _recorder(asks_then_answers)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=tables, compare_match=etd.compare_match,
    )
    assert len(calls) == 2
    assert section["diagnostics"]["expansions"] == 1
    assert section["diagnostics"]["expansion_retries"] == 1
    assert section["diagnostics"]["identity_resolved"] == 1


def test_повторный_запрос_добора_закрывает_вопрос_человеку():
    def always_asks(package, attempt):
        return [{
            **item,
            "verdict": identity.VERDICT_NEED_EVIDENCE,
            "left_row_ref": None, "right_row_ref": None, "shared_identity": None,
            "need_more_evidence": {
                "missing_evidence_type": identity.NEED_NEIGHBOUR_ROWS,
                "requested_entity": "ШУ-ХЦ", "requested_side": "LEFT",
            },
        } for item in _same_entity(package, attempt)]

    tables = _pair_tables()
    tables["LEFT"]["rows"].append(_row("etrow_n", side="LEFT", designation="ШУ-ХВС"))
    call, calls = _recorder(always_asks)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=tables, compare_match=etd.compare_match,
    )
    assert len(calls) == 2
    record = section["resolutions"][0]
    assert record["reason_code"] == ai_resolution.REASON_IDENTITY_INSUFFICIENT


# ── Ничего не теряется ─────────────────────────────────────────────────────

def test_каждый_допущенный_вопрос_получает_запись():
    call, _calls = _recorder(_same_entity)
    inventory = _inventory()
    section = _layer(call).resolve_identity(
        inventory=inventory, load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    assert len(section["resolutions"]) == section["diagnostics"]["questions"]
    assert section["diagnostics"]["questions"] == 1


def test_недопущенные_элементы_к_модели_не_едут():
    call, calls = _recorder(_same_entity)
    inventory = _inventory()
    inventory["items"][0]["decision"] = routing.INELIGIBLE_EVIDENCE
    section = _layer(call).resolve_identity(
        inventory=inventory, load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    assert calls == []
    assert section["resolutions"] == []


# ── Происхождение в предварительном отчёте ─────────────────────────────────

def _identity_artifact(section: dict) -> dict:
    return {
        "derived_changes": section["derived_changes"],
        "resolved_row_ids": section["resolved_row_ids"],
    }


def test_отчёт_показывает_происхождение_находки():
    call, _calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis={"changes": [], "review_items": []},
        ai_table_identity=_identity_artifact(section),
    )
    lines = next(
        block for block in report["sections"]
        if block["section_id"] == pr.SECTION_AI_VERIFIED
    )["items"]
    assert len(lines) == 1
    assert lines[0]["status"] == pr.STATUS_AI_VERIFIED
    assert report["summary"]["counts"]["ai_verified"] == 1
    assert any("проверено правилами" in s for s in report["summary"]["sentences"])


def test_доказанная_строка_уходит_из_несравнимых():
    call, _calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    table_changes = {"unproven": [{
        "reason": "row_has_no_counterpart", "side": "LEFT", "row_id": "etrow_l",
        "subject": "ШУ-ХЦ", "section_ref": "РП1",
        "summary": "«ШУ-ХЦ»: строка не имеет доказанной пары.",
    }]}
    before = pr.build_preliminary_report(
        pair_id="p1", synthesis={"changes": [], "review_items": []},
        electrical_table_changes=table_changes,
    )
    after = pr.build_preliminary_report(
        pair_id="p1", synthesis={"changes": [], "review_items": []},
        electrical_table_changes=table_changes,
        ai_table_identity=_identity_artifact(section),
    )
    assert before["summary"]["counts"]["unproven"] == 1
    assert after["summary"]["counts"]["unproven"] == 0


def test_отчёт_не_называет_модель_в_тексте_находки():
    call, _calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=_inventory(), load_tables=_pair_tables(),
        compare_match=etd.compare_match,
    )
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis={"changes": [], "review_items": []},
        ai_table_identity=_identity_artifact(section),
    )
    text = json.dumps(report, ensure_ascii=False).lower()
    for word in ("gpt-5", "codex", "claude-opus", "reasoning_level"):
        assert word not in text, word


def test_отчёт_сам_модель_не_зовёт():
    report = pr.build_preliminary_report(
        pair_id="p1", synthesis={"changes": [], "review_items": []},
        ai_table_identity={"derived_changes": [], "resolved_row_ids": []},
    )
    assert report["constraints"]["uses_model"] is False


# ── Строка входит не больше чем в одну доказанную пару ─────────────────────

def test_одна_строка_не_попадает_в_две_доказанные_пары():
    """На паре ГРЩ правая колонка «ГРЩ1-РП1-7 ВРУ-ХЦ» досталась сразу двум
    вопросам: фидерной строке ШУ-ХЦ и её же суммарной."""
    tables = _pair_tables()
    tables["LEFT"]["rows"].append(_row(
        "etrow_l2", side="LEFT", designation="ШУ-ХЦ", kind="CONSUMER_TOTAL",
        values=[_value("demand_active_power_kw", 27.5, "Рр=27,5 кВт")],
    ))
    inventory = {"items": [
        {
            "item_id": "etrow_l", "kind": routing.KIND_TABLE_UNPROVEN,
            "decision": routing.ELIGIBLE, "unresolved": True, "subject": "ШУ-ХЦ",
            "routing_payload": {"row_id": "etrow_l", "side": "LEFT",
                                "candidate_row_ids": ["etrow_r"]},
        },
        {
            "item_id": "etrow_l2", "kind": routing.KIND_TABLE_UNPROVEN,
            "decision": routing.ELIGIBLE, "unresolved": True, "subject": "ШУ-ХЦ",
            "routing_payload": {"row_id": "etrow_l2", "side": "LEFT",
                                "candidate_row_ids": ["etrow_r"]},
        },
    ]}
    call, _calls = _recorder(_same_entity)
    section = _layer(call).resolve_identity(
        inventory=inventory, load_tables=tables, compare_match=etd.compare_match,
    )
    assert section["diagnostics"]["questions"] == 2
    assert section["diagnostics"]["identity_resolved"] == 1
    taken = [
        record for record in section["resolutions"]
        if record["reason_code"] == ai_resolution.REASON_IDENTITY_ROW_TAKEN
    ]
    assert len(taken) == 1
    right_ids = [
        change["evidence"]["RIGHT"]["row_id"] for change in section["derived_changes"]
    ]
    assert len(set(right_ids)) == len(right_ids)


def test_уже_сопоставленная_детерминированно_строка_в_кандидаты_не_идёт():
    """Иначе в отчёте появляется «ХМ1: мощность увеличена с 157,5 до 335 кВт»
    дважды — как найденное автоматически и как уточнённое ИИ."""
    taken = routing.matched_row_ids({
        "changes": [{"evidence": {
            "LEFT": {"row_id": "etrow_lf"}, "RIGHT": {"row_id": "etrow_rf"},
        }}],
    })
    assert taken == {"etrow_lf", "etrow_rf"}
    candidates = routing.counterpart_candidates(
        _row("etrow_lt", side="LEFT", designation="ХМ1", kind="CONSUMER_TOTAL"),
        [_row("etrow_rf", side="RIGHT", designation="ХМ1")],
        exclude=taken,
    )
    assert candidates == []
