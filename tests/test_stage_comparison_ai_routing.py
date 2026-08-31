"""Инвентаризация маршрутизации: кто уезжает модели и почему остальные — нет.

Проверяется не «слой работает», а три утверждения, каждое из которых стоило
отдельного разбора на боевой паре:

  * ни один нерешённый элемент не исчезает молча — у каждого есть маршрут
    и причина;
  * элемент, у которого противоположной стороны нет в прочитанном виде, к
    модели не едет: она может только отклонить его, и на паре ГРЩ ровно это
    и произошло — семь обращений и сто тридцать пять секунд ради одиннадцати
    отказов;
  * «недостаточно доказательств» и «не положено» — разные вердикты, и первый
    не имеет права утверждать, что чего-то на листе нет.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.ai import routing  # noqa: E402


# ── Фабрики ────────────────────────────────────────────────────────────────

def _review_item(item_id: str, *, before=None, after=None) -> dict:
    return {
        "review_evidence_id": item_id,
        "atom_id": f"tatom_{item_id}",
        "before_value": before,
        "after_value": after,
        "outcome": "REVIEW_REQUIRED",
    }


def _fragment(fragment_id: str, text: str) -> dict:
    return {
        "id": fragment_id,
        "text": text,
        "source": "NATIVE_PDF_TEXT",
        "pdf_page": 1,
    }


def _preparation(left: list[dict] | None = None, right: list[dict] | None = None) -> dict:
    return {"fragments": {"left": list(left or ()), "right": list(right or ())}}


def _row(
    row_id: str,
    *,
    side: str = "LEFT",
    designation: str = "ВРУ1",
    section=None,
    kind: str = "FEEDER",
    label: str | None = None,
    values: list | None = None,
) -> dict:
    return {
        "row_id": row_id,
        "side": side,
        "consumer_label": label or designation,
        "consumer_designation": designation,
        "own_designations": [designation],
        "row_designations": [],
        "section_ref": section,
        "row_kind": kind,
        "values": list(values or ()),
        "cables": [],
        "binding_status": "BOUND",
    }


def _tables(left: list[dict], right: list[dict]) -> dict:
    return {"LEFT": {"rows": left}, "RIGHT": {"rows": right}}


# ── Инвентаризация покрывает всё нерешённое ────────────────────────────────

def test_каждый_нерешённый_элемент_получает_маршрут_и_причину():
    inventory = routing.build_inventory(
        synthesis={"review_items": [_review_item("ur1", after="Новая строка")], "changes": []},
        preparation=_preparation(),
        electrical_table_changes={
            "unproven": [{
                "reason": "row_has_no_counterpart", "side": "LEFT",
                "row_id": "etrow_a", "subject": "ВРУ1", "summary": "нет пары",
            }],
            "blocked": [{
                "reason": "ambiguous_row_match", "match_id": "etm_1",
                "left_row_ids": ["etrow_a"], "right_row_ids": ["etrow_b"],
                "summary": "двусмысленно",
            }],
        },
        document_inconsistencies={"items": [
            {"inconsistency_id": "dinc_1", "verdict": "REVIEW", "summary": "проверить"},
        ]},
        load_tables=_tables([_row("etrow_a")], [_row("etrow_b", side="RIGHT")]),
    )
    kinds = {item["kind"] for item in inventory["items"]}
    assert kinds == {
        routing.KIND_TEXT_REVIEW,
        routing.KIND_TABLE_UNPROVEN,
        routing.KIND_TABLE_BLOCKED,
        routing.KIND_CONSISTENCY_REVIEW,
    }
    for item in inventory["items"]:
        assert item["decision"] in routing.DECISIONS
        assert item["reason"], item
        assert item["human_category"], item


def test_счётчики_считают_только_нерешённое():
    """Уже доказанное противоречие — след, а не задача.

    Сложить его с нерешённым значило бы считать «сколько снято с человека»
    от завышенной базы.
    """
    inventory = routing.build_inventory(
        synthesis={"review_items": [], "changes": []},
        preparation=_preparation(),
        document_inconsistencies={"items": [
            {"inconsistency_id": "d1", "verdict": "CONFIRMED", "summary": "доказано"},
            {"inconsistency_id": "d2", "verdict": "REVIEW", "summary": "проверить"},
        ]},
        load_tables=_tables([], []),
    )
    assert inventory["counts"]["total"] == 2
    assert inventory["counts"]["unresolved_total"] == 1
    assert inventory["counts"][routing.ELIGIBLE] == 1
    proven = next(i for i in inventory["items"] if i["item_id"] == "d1")
    assert proven["unresolved"] is False
    assert proven["decision"] == routing.INELIGIBLE_POLICY
    assert "d1" not in routing.eligible_ids(inventory)


# ── Текстовые свидетельства ────────────────────────────────────────────────

def test_нераспознанная_сторона_к_модели_не_едет():
    inventory = routing.build_inventory(
        synthesis={
            "review_items": [_review_item("ur1", after="Щиты изготовить напольного исполнения")],
            "changes": [],
        },
        preparation=_preparation(left=[_fragment("f1", "Расчетная мощность щита")]),
        load_tables=_tables([], []),
    )
    entry = inventory["items"][0]
    assert entry["decision"] == routing.INELIGIBLE_EVIDENCE
    assert entry["reason_code"] == routing.REASON_SIDE_NOT_RECOGNISED
    assert entry["missing_evidence"]


def test_вердикт_о_нехватке_не_утверждает_отсутствия_на_листе():
    """Инвариант раздела: отсутствие распознанного ≠ доказательство отсутствия."""
    inventory = routing.build_inventory(
        synthesis={"review_items": [_review_item("ur1", after="ИНПАД")], "changes": []},
        preparation=_preparation(left=[_fragment("f1", "Прочее")]),
        load_tables=_tables([], []),
    )
    text = " ".join(inventory["items"][0]["missing_evidence"]).lower()
    assert "прочитанной" in text or "прочитан" in text
    assert "на листе нет" not in text


def test_найденный_на_другой_стороне_текст_открывает_маршрут():
    inventory = routing.build_inventory(
        synthesis={
            "review_items": [
                _review_item("ur1", after="Внутреннее электроснабжение и освещение"),
            ],
            "changes": [],
        },
        preparation=_preparation(
            left=[_fragment("f1", "Часть 1. Внутреннее электроснабжение и освещение.")],
        ),
        load_tables=_tables([], []),
    )
    entry = inventory["items"][0]
    assert entry["decision"] == routing.ELIGIBLE
    assert entry["routing_payload"]["retrieved"]["LEFT"]


def test_рамка_штампа_не_считается_найденной_фамилией():
    """«Проверил» из рамки покрывает запрос целиком, но фамилии там нет."""
    assert not routing.retrieve_lines(
        [_fragment("f1", "Проверил"), _fragment("f2", "Дата")],
        "Проверил Бушмин 02.26",
    )


def test_совпадение_по_числу_не_доказательство():
    """«P 1.1» и шифр «АА/БЭ-03-ДС3-ИОС1.1.ГЧ» делят только «1.1»."""
    assert not routing.retrieve_lines(
        [_fragment("f1", "АА/БЭ-03-ДС3-ИОС1.1.ГЧ")], "P 1.1",
    )


def test_обозначения_различаются_по_цифре():
    """«ВРУ1» и «ВРУ3» — разные потребители, а не одно семейство."""
    assert routing.tokens("ВРУ1") != routing.tokens("ВРУ3")
    assert "вру1" in routing.tokens("2ГРЩ-ВРУ1 ППГнг(А)-НF 3х(5х120)")


# ── Строки таблиц ──────────────────────────────────────────────────────────

def test_строка_без_кандидатов_остаётся_человеку():
    inventory = routing.build_inventory(
        synthesis={"review_items": [], "changes": []},
        preparation=_preparation(),
        electrical_table_changes={"unproven": [{
            "reason": "row_has_no_counterpart", "side": "LEFT",
            "row_id": "etrow_a", "subject": "ВРУ1", "summary": "нет пары",
        }]},
        load_tables=_tables(
            [_row("etrow_a", section="РП1")],
            [_row("etrow_b", side="RIGHT", section="РП2")],
        ),
    )
    entry = inventory["items"][0]
    assert entry["decision"] == routing.INELIGIBLE_EVIDENCE
    assert entry["reason_code"] == routing.REASON_NO_CANDIDATES


def test_строка_с_кандидатами_едет_к_модели():
    inventory = routing.build_inventory(
        synthesis={"review_items": [], "changes": []},
        preparation=_preparation(),
        electrical_table_changes={"unproven": [{
            "reason": "row_has_no_counterpart", "side": "LEFT",
            "row_id": "etrow_a", "subject": "ШУ-ХЦ", "summary": "нет пары",
        }]},
        load_tables=_tables(
            [_row("etrow_a", designation="ШУ-ХЦ", section="РП1")],
            [_row("etrow_b", side="RIGHT", designation="ВРУ-ХЦ", section="РП1")],
        ),
    )
    entry = inventory["items"][0]
    assert entry["decision"] == routing.ELIGIBLE
    assert entry["routing_payload"]["candidate_row_ids"] == ["etrow_b"]


def test_разные_режимы_это_политика_а_не_нехватка_данных():
    inventory = routing.build_inventory(
        synthesis={"review_items": [], "changes": []},
        preparation=_preparation(),
        electrical_table_changes={"blocked": [{
            "reason": "mode_label_mismatch", "match_id": "etm_1",
            "subject": "ВРУ1", "summary": "слева режим, справа нет",
        }]},
        load_tables=_tables([], []),
    )
    entry = inventory["items"][0]
    assert entry["decision"] == routing.INELIGIBLE_POLICY
    assert entry["reason_code"] == routing.REASON_MODE_MISMATCH


def test_кандидаты_ищутся_по_виду_и_разделу_а_не_по_подписи():
    """Подпись как раз и разъехалась — требовать её совпадения значит не найти
    ровно то, ради чего вопрос задаётся."""
    candidates = routing.counterpart_candidates(
        _row("etrow_a", designation="ШУ-ХЦ", section="РП1"),
        [
            _row("etrow_b", side="RIGHT", designation="ВРУ-ХЦ", section="РП1"),
            _row("etrow_c", side="RIGHT", designation="ШУ-ХЦ", section="РП2"),
        ],
    )
    assert [value["row_id"] for value in candidates] == ["etrow_b"]


# ── Находки с неполными доказательствами ───────────────────────────────────

def test_мета_находка_о_сопоставлении_моделью_не_разбирается():
    inventory = routing.build_inventory(
        synthesis={
            "review_items": [],
            "changes": [
                {"change_id": "uchg_1"},
                {"change_id": "uchg_2"},
            ],
        },
        preparation=_preparation(),
        load_tables=_tables([], []),
        change_is_review=lambda change: True,
        change_describe=lambda change: (
            "Часть узлов схемы не удалось сопоставить между редакциями однозначно."
            if change["change_id"] == "uchg_1"
            else "ВРУ1: число параллельных кабелей изменено с 1 до 3."
        ),
    )
    by_id = routing.entries_by_id(inventory)
    assert by_id["uchg_1"]["decision"] == routing.INELIGIBLE_POLICY
    assert by_id["uchg_1"]["reason_code"] == routing.REASON_MATCHER_META
    assert by_id["uchg_2"]["decision"] == routing.ELIGIBLE


def test_инвентаризация_не_обращается_к_модели():
    inventory = routing.build_inventory(
        synthesis={"review_items": [], "changes": []},
        preparation=_preparation(),
        load_tables=_tables([], []),
    )
    assert inventory["constraints"]["uses_model"] is False
    assert inventory["constraints"]["is_deterministic"] is True


# ── Добранные строки доезжают до пакета доказательств ──────────────────────

def test_добранная_строка_попадает_в_окно_доказательств():
    """Корень EVIDENCE_TRUNCATED: у элемента вида «добавлено» левой стороны
    нет якорного фрагмента, окно вокруг несуществующего фрагмента честно
    пусто, и модель читает пустое окно как «доказательств не показали»."""
    from backend.app.services.stage_comparison.ai import evidence

    item = {
        "review_evidence_id": "ur1",
        "atom_id": "tatom_1",
        "scope_ref": "scope_1",
        "source": "TEXT",
        "direction": "ADDED",
        "before_value": None,
        "after_value": "Внутреннее электроснабжение и освещение",
        "evidence_refs": [],
        "provenance": {"source_atom": {
            "stage3_bucket": "added",
            "locations": {"LEFT": [], "RIGHT": [{"page": 1, "fragment_id": "f_r"}]},
        }},
    }
    preparation = {"fragments": {
        "left": [],
        "right": [{
            "id": "f_r", "pdf_page": 1, "order": 0,
            "text": "Внутреннее электроснабжение и освещение",
        }],
    }}
    without = evidence.build_packages(
        review_items=[item], preparation=preparation,
        sheet_relations={}, comparison_groups=[], batch_size=10,
    )[0].items[0]
    assert without.left_context == []

    with_lines = evidence.build_packages(
        review_items=[item], preparation=preparation,
        sheet_relations={}, comparison_groups=[], batch_size=10,
        retrieved={"ur1": {"LEFT": [
            {"fragment_id": "f_l", "text": "Часть 1. Внутреннее электроснабжение и освещение.",
             "score": 0.6, "source": "NATIVE_PDF_TEXT", "page": 1},
        ]}},
    )[0].items[0]
    assert len(with_lines.left_context) == 1
    line = with_lines.left_context[0]
    assert line["ref"] == "L1"
    assert line["side"] == "LEFT"
    # Провенанс урезанных прав виден и модели, и верификатору.
    assert line["source"] == evidence.RETRIEVED_SOURCE


def test_добор_не_затирает_настоящее_окно():
    """Там, где якорный фрагмент есть, окно строится по нему — добор не нужен."""
    from backend.app.services.stage_comparison.ai import evidence

    item = {
        "review_evidence_id": "ur2",
        "atom_id": "tatom_2",
        "scope_ref": "scope_1",
        "source": "TEXT",
        "direction": "REPLACED",
        "before_value": "было",
        "after_value": "стало",
        "evidence_refs": [],
        "provenance": {"source_atom": {
            "stage3_bucket": "changed",
            "locations": {
                "LEFT": [{"page": 1, "fragment_id": "f_l"}],
                "RIGHT": [{"page": 1, "fragment_id": "f_r"}],
            },
        }},
    }
    preparation = {"fragments": {
        "left": [{"id": "f_l", "pdf_page": 1, "order": 0, "text": "было"}],
        "right": [{"id": "f_r", "pdf_page": 1, "order": 0, "text": "стало"}],
    }}
    built = evidence.build_packages(
        review_items=[item], preparation=preparation,
        sheet_relations={}, comparison_groups=[], batch_size=10,
        retrieved={"ur2": {"LEFT": [{"fragment_id": "x", "text": "чужое", "page": 1}]}},
    )[0].items[0]
    assert [line["text"] for line in built.left_context] == ["было"]
    assert built.left_context[0]["source"] == "TEXT"
