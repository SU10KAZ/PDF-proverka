"""Text Fact Owner V1: штамп по структуре, владелец фрагмента, владельческие факты.

Правило «x ≥ 0,72 ⇒ штамп» прятало экспликации помещений, таблицы оборудования
и легенды у правого края листа в раздел «оформление».  Здесь — отрицательные
контроли этого дефекта, положительный контроль настоящего штампа (в том числе
на повёрнутой странице), и три инварианта владельческих фактов: только при
доказанной семантике поля, только ALTERED (ни одного REMOVED/ADDED), никакой
формулы в роли подписи поля.  Страницы объекта 272 здесь не зашиты.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import text_fact_ownership as ownership
from backend.app.services.stage_comparison import text_region_classifier as classifier
from backend.app.services.stage_comparison.human_review_orchestrator import (
    DOCUMENT_METADATA_CHANGE,
    MISSING_EVIDENCE,
    build_human_review_plan,
)
from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
)
from backend.app.services.stage_comparison.text_fact_producer import OWNED_RULES, produce_text_facts

from stage_comparison_recognition_fixtures import native_layer_index


# ── фикстуры ────────────────────────────────────────────────────────────────

def _fragment(fragment_id, side, text, *, kind="table_row", cells=None, order=1, page=None,
              bbox=(0.85, 0.30), block="blk", group=None):
    return {
        "id": fragment_id,
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": page if page is not None else (10 if side == "left" else 24),
        "text": text,
        "canonical_text": text.casefold(),
        "source_block_id": f"{side}-{block}",
        "source_kind": kind,
        "source_group": f"{side}-{group or block}:table" if kind == "table_row" else f"{side}-{block}",
        "location_parts": list(cells) if cells is not None else ([text] if kind == "table_row" else []),
        "order": order,
        "bboxes": [{"x": bbox[0], "y": bbox[1], "width": 0.05, "height": 0.01}] if bbox else [],
        "pdf_canonical_text": text.casefold(),
    }


def _preparation(left, right):
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-1",
        "input_signature": "prepared-input",
        "comparison_groups": [{
            "id": "group-1", "left_pages": [10], "right_pages": [24],
            "relation_type": "MATCHED", "relation_status": "HIGH",
        }],
        "fragments": {"left": left, "right": right},
        "recognition_index": native_layer_index(left, right),
    }


def _facts(left, right):
    preparation = _preparation(left, right)
    differences = build_text_differences_from_preparation(preparation, generated_at="fixed")
    return produce_text_facts(differences, preparation, generated_at="fixed")


def _table(side, rows, *, header=("Наименование", "Марка", "Масса, кг"), title=None, x=0.85, block="tbl"):
    output = []
    order = 1
    if title:
        output.append(_fragment(f"{side}-title", side, title, kind="paragraph", order=order, bbox=(x, 0.20), block=block))
        order += 1
    if header:
        output.append(_fragment(f"{side}-hdr", side, " ".join(header), cells=header, order=order, bbox=(x, 0.22), block=block))
        order += 1
    for index, cells in enumerate(rows):
        output.append(_fragment(f"{side}-r{index}", side, " ".join(cells), cells=cells, order=order, bbox=(x, 0.25 + index * 0.02), block=block))
        order += 1
    return output


def _classify(fragment, *, stamp_blocks=None, native=True, rows=(), block=None):
    table = classifier.table_context(fragment, list(rows), list(block or rows)) if fragment["source_kind"] == "table_row" else None
    return classifier.classify_fragment(fragment, stamp_blocks=stamp_blocks, native_available=native, table=table)


# ── 1. Классификатор штампа: координата — не доказательство ────────────────

def test_a_room_explication_at_the_right_edge_is_not_a_stamp():
    rows = _table("left", [("18.1", "Холл", "15,71"), ("18.2", "Кухня", "45,18"), ("18.3", "Спальня", "21,37")],
                  header=("Номер помещения", "Наименование", "Площадь, м2"), title="Экспликация помещений 2 этажа", x=0.9)
    row = rows[-1]
    result = _classify(row, stamp_blocks=[], native=True, rows=[r for r in rows if r["source_kind"] == "table_row"], block=rows)
    assert result["structure"] == classifier.EXPLICATION
    assert result["is_stamp"] is False


def test_an_equipment_table_at_the_right_edge_is_not_a_stamp():
    rows = _table("left", [("Вентилятор", "ВР-80", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Шумоглушитель", "ГТП", "8,0")], x=0.95)
    result = _classify(rows[-1], stamp_blocks=[], native=True, rows=rows, block=rows)
    assert result["structure"] == classifier.EQUIPMENT_TABLE
    assert result["is_stamp"] is False


def test_plain_text_at_the_right_edge_without_structure_is_other_not_stamp():
    fragment = _fragment("l", "left", "Композитная фасадная панель", kind="paragraph", bbox=(0.93, 0.40))
    result = _classify(fragment, stamp_blocks=[], native=True)
    assert result["structure"] == classifier.OTHER
    assert result["is_stamp"] is False


def test_a_fragment_inside_the_stamp_strip_without_vocabulary_is_not_a_stamp():
    fragment = _fragment("l", "left", "Класс очистки EU3", cells=("Класс очистки", "EU3"), bbox=(0.95, 0.97))
    result = _classify(fragment, stamp_blocks=[{"x0": 0.6, "y0": 0.9, "x1": 0.99, "y1": 0.99, "identity": False, "vocabulary": False}], native=True)
    assert result["is_stamp"] is False


def test_a_real_stamp_field_inside_a_native_identity_block_is_a_stamp():
    fragment = _fragment("l", "left", "Изм. Кол.уч Лист №док. Подп. Дата",
                         cells=("Изм.", "Кол.уч", "Лист", "№док.", "Подп.", "Дата"), bbox=(0.75, 0.95))
    blocks = [{"x0": 0.6, "y0": 0.9, "x1": 0.99, "y1": 0.99, "identity": True, "vocabulary": True}]
    result = _classify(fragment, stamp_blocks=blocks, native=True)
    assert result["structure"] == classifier.STAMP
    assert "stamp_field_vocabulary" in result["evidence"]


def test_the_sheet_title_cell_is_a_title_block_and_a_reference_to_sp_is_not():
    title = _fragment("l", "left", "Корпуса 1, 2. План кровли.", kind="paragraph", bbox=(0.85, 0.98))
    blocks = [{"x0": 0.6, "y0": 0.9, "x1": 0.99, "y1": 0.99, "identity": True, "vocabulary": True}]
    assert _classify(title, stamp_blocks=blocks, native=True)["structure"] == classifier.TITLE_BLOCK
    reference = _fragment("r", "left", "СП 50.13330.2012 с изм. 1 «Тепловая защита зданий»;", kind="paragraph", bbox=(0.85, 0.40))
    assert classifier.has_stamp_field_vocabulary(reference) is False
    assert _classify(reference, stamp_blocks=[], native=True)["is_stamp"] is False


def test_without_a_native_layer_vocabulary_plus_direct_bbox_in_the_strip_is_required():
    stamp = _fragment("l", "left", "ГИП Иванов 02.26", cells=("ГИП Иванов 02.26",), bbox=(0.86, 0.94))
    assert _classify(stamp, stamp_blocks=None, native=False)["structure"] == classifier.STAMP
    same_words_in_body = _fragment("m", "left", "ГИП Иванов 02.26", cells=("ГИП Иванов 02.26",), bbox=(0.86, 0.30))
    assert _classify(same_words_in_body, stamp_blocks=None, native=False)["is_stamp"] is False


def test_a_fragment_without_bbox_is_unknown_never_stamp():
    fragment = _fragment("l", "left", "ГИП Иванов", kind="paragraph", bbox=None)
    result = _classify(fragment, stamp_blocks=None, native=False)
    assert result["structure"] == classifier.UNKNOWN
    assert result["is_stamp"] is False


def test_native_stamp_blocks_follow_the_page_rotation(tmp_path):
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    page = document.new_page(width=1000, height=700)
    # Штамп внизу справа ОТОБРАЖАЕМОЙ страницы; страница хранится повёрнутой на 90°.
    page.set_rotation(90)
    shown = page.rect  # прямоугольник ОТОБРАЖАЕМОЙ страницы (после поворота)
    displayed = fitz.Rect(shown.x1 * 0.60, shown.y1 * 0.90, shown.x1 * 0.99, shown.y1 * 0.99)
    stored = displayed * ~page.rotation_matrix
    stored.normalize()
    # Базовые шрифты PyMuPDF не кодируют кириллицу; словарь проверяется только
    # при наличии системного шрифта, геометрия — всегда.
    font = next((f for f in ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",) if __import__("os").path.isfile(f)), None)
    kwargs = {"fontname": "cyr", "fontfile": font} if font else {}
    page.insert_textbox(stored, "Лист 12 Изм. Кол.уч Лист №док. Подп. Дата", fontsize=9, rotate=page.rotation, **kwargs)
    path = tmp_path / "rotated.pdf"
    document.save(path)
    document.close()
    blocks = classifier.read_stamp_zone_blocks(str(path), 1)
    assert blocks, "штамп повёрнутой страницы не найден в зоне штампа"
    assert all(block["y0"] >= 0.85 and block["x1"] >= 0.55 for block in blocks)
    assert blocks[0]["page_rotation"] == 90
    if font:
        assert any(block["vocabulary"] for block in blocks)
    # тот же текст в середине повёрнутой страницы в зону штампа не попадает
    document = fitz.open()
    page = document.new_page(width=1000, height=700)
    page.set_rotation(90)
    shown = page.rect
    middle = fitz.Rect(shown.x1 * 0.60, shown.y1 * 0.40, shown.x1 * 0.99, shown.y1 * 0.50) * ~page.rotation_matrix
    middle.normalize()
    page.insert_textbox(middle, "Лист 12 Изм. Кол.уч Лист №док. Подп. Дата", fontsize=9, rotate=page.rotation, **kwargs)
    other = tmp_path / "rotated-middle.pdf"
    document.save(other)
    document.close()
    assert classifier.read_stamp_zone_blocks(str(other), 1) == []


def test_unreadable_pdf_gives_no_evidence_not_a_stamp(tmp_path):
    missing = tmp_path / "absent.pdf"
    assert classifier.read_stamp_zone_blocks(str(missing), 1) == []
    index = classifier.build_stamp_zone_index({"documents": {"LEFT": {"pdf": {"path": str(missing)}}}}, [("LEFT", 1)])
    assert index == {("LEFT", 1): []}


# ── 2. План проверки: экспликация справа больше не «оформление» ────────────

def _target(target_id, fragment_id, text, *, side="RIGHT", page=24, bbox=(0.9, 0.3)):
    return {
        "review_evidence_id": target_id,
        "review_status": "REVIEW_REQUIRED",
        "outcome": "REVIEW_REQUIRED",
        "dimension": "UNKNOWN_DIMENSION",
        "before_value": None,
        "after_value": text,
        "evidence_refs": [{"evidence_ref": f"e:{target_id}"}],
        "provenance": {"source_atom": {"locations": {
            "LEFT": [], "RIGHT": [{"page": page, "fragment_id": fragment_id,
                                   "bboxes": [{"x": bbox[0], "y": bbox[1], "width": 0.05, "height": 0.01}]}],
        }}},
    }


def test_human_review_plan_keeps_a_right_edge_explication_row_out_of_the_metadata_section():
    right = _table("right", [("01.27", "Кладовая", "7,16"), ("01.26", "Помещение ТБО", "9,41"), ("01.38", "Помещение ТБО", "6,13")],
                   header=("Номер помещения", "Наименование", "Площадь, м2"), title="Экспликация помещений", x=0.9)
    right[-1]["text"] = "01.38 Помещение ТБО 6,13"
    preparation = {**_preparation([], right), "extraction": {"selected_pages": {"LEFT": [10], "RIGHT": [24]}}}
    synthesis = {"changes": [], "review_items": [_target("row", right[-1]["id"], right[-1]["text"], bbox=(0.9, 0.29))]}
    plan = build_human_review_plan(pair_id="pair-1", synthesis=synthesis, text_preparation=preparation,
                                   stamp_zone_index={("RIGHT", 24): []})
    row = plan["atomic_target_mapping"][0]
    assert row["new_category"] != DOCUMENT_METADATA_CHANGE
    assert row["source_region"]["region"] == "ENGINEERING_TEXT"
    assert row["source_region"]["structure"] == classifier.EXPLICATION
    assert plan["summary"]["metadata_changes"] == 0


def test_human_review_plan_still_files_a_proven_stamp_field_as_metadata():
    stamp = _fragment("r-stamp", "right", "ГИП Иванов 02.26", cells=("ГИП Иванов 02.26",), page=24, bbox=(0.86, 0.94), block="title")
    preparation = {**_preparation([], [stamp]), "extraction": {"selected_pages": {"LEFT": [10], "RIGHT": [24]}}}
    synthesis = {"changes": [], "review_items": [_target("meta", "r-stamp", "ГИП Иванов 02.26", bbox=(0.86, 0.94))]}
    blocks = {("RIGHT", 24): [{"x0": 0.6, "y0": 0.9, "x1": 0.99, "y1": 0.99, "identity": True, "vocabulary": True}]}
    plan = build_human_review_plan(pair_id="pair-1", synthesis=synthesis, text_preparation=preparation, stamp_zone_index=blocks)
    row = plan["atomic_target_mapping"][0]
    assert row["new_category"] == DOCUMENT_METADATA_CHANGE
    assert row["source_region"]["region"] == "TITLE_BLOCK"
    assert row["source_region"]["structure"] == classifier.STAMP


# ── 3. Владелец фрагмента ───────────────────────────────────────────────────

def test_ownership_channels_and_statuses():
    left = _table("left", [("Насос", "ЭЦВ 6-6,5-60", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")], title="Ведомость оборудования")
    left += [
        _fragment("kv", "left", "Юридический адрес: 105005, г. Москва, Плетешковский пер., 2", kind="paragraph", order=20, bbox=(0.2, 0.5), block="txt"),
        _fragment("h", "left", "3. Водоснабжение", kind="heading", order=21, bbox=(0.2, 0.55), block="txt"),
        _fragment("p", "left", "Расход воды принят по СП 30.13330.", kind="paragraph", order=22, bbox=(0.2, 0.6), block="txt"),
        _fragment("free", "left", "Отдельный абзац без заголовка", kind="paragraph", order=30, page=11, bbox=(0.2, 0.7), block="alone"),
    ]
    index = ownership.fragment_ownership_index(_preparation(left, []))
    row = index["left-r0"]
    assert (row["owner_kind"], row["ownership_status"], row["ownership_channel"], row["scope"]) == ("TABLE_ROW", "PROVEN", "EXACT_TABLE_ROW", "TABLE_LOCAL")
    assert row["fields"] == ["Наименование", "Марка", "Масса, кг"]
    assert index["kv"]["ownership_channel"] == "EXPLICIT_KEY_VALUE" and index["kv"]["ownership_status"] == "PROVEN"
    assert index["p"]["ownership_channel"] == "SECTION_SCOPE" and index["p"]["ownership_status"] == "PARTIAL"
    assert index["free"]["ownership_status"] == "UNKNOWN" and index["free"]["scope"] == "UNKNOWN"


def test_a_table_without_a_proven_header_owns_its_rows_but_not_their_fields():
    left = _table("left", [("Насос", "ЭЦВ", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")], header=None)
    index = ownership.fragment_ownership_index(_preparation(left, []))
    row = index["left-r1"]
    assert row["owner_kind"] == "TABLE_ROW" and row["ownership_status"] == "PARTIAL"
    assert row["fields_known"] is False


def test_document_shared_scope_needs_a_proven_repeat_not_a_missing_owner():
    note = "Все размеры уточнить по месту."
    left = [_fragment(f"n{page}", "left", note, kind="paragraph", page=page, bbox=(0.2, 0.9), block=f"b{page}") for page in (10, 11, 12)]
    lonely = _fragment("lonely", "left", "Единственный абзац.", kind="paragraph", page=13, bbox=(0.2, 0.9), block="b13")
    index = ownership.fragment_ownership_index(_preparation(left + [lonely], []))
    assert index["n10"]["scope"] == "DOCUMENT_SHARED" and index["n10"]["repeated_pages"] == 3
    assert index["lonely"]["scope"] == "UNKNOWN"


# ── 4. Семантика подписи: формула — не поле ────────────────────────────────

@pytest.mark.parametrize("label", ["q{\\text{от}}^{\\text{p}}", "M'", "Gi^c \\times b", "$\\sum$", "x_1"])
def test_a_formula_is_never_a_semantic_field_label(label):
    assert ownership.is_semantic_label(label) is False


@pytest.mark.parametrize(("before", "after", "material"), [
    ("95-70", "90-65", True),
    ("2.6595", "2.944", True),
    ("п1ж", "p1ж", False),
    ("Летняя", "Лётная", False),
    ("5190/1125/740", "5190x1125x740", False),
    ("Садовнической", "Садовой", True),
])
def test_material_difference_is_numbers_or_more_than_two_letters(before, after, material):
    assert ownership.material_difference(before, after) is material


# ── 5. Владельческие факты ─────────────────────────────────────────────────

def _paired_tables(left_rows, right_rows, **kw):
    return _table("left", left_rows, **kw), _table("right", right_rows, **kw)


def test_a_changed_cell_under_a_proven_header_becomes_one_owned_fact():
    left, right = _paired_tables(
        [("Насос", "ЭЦВ 6-6,5-60", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
        [("Насос", "ЭЦВ 6-6,5-60", "19,2"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
        title="Ведомость оборудования",
    )
    result = _facts(left, right)
    owned = [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES]
    assert len(owned) == 1
    fact = owned[0]
    assert fact["facet_ref"] == "масса_кг" and fact["before_value"] == "12.5" and fact["after_value"] == "19.2"
    assert fact["direction"] in {"INCREASED", "ALTERED"}
    assert fact["provenance"]["source_fragment_ids"] == {"LEFT": ["left-r0"], "RIGHT": ["right-r0"]}
    assert result["diagnostics"]["owned_facts"] == 1


def test_a_two_column_label_value_table_is_an_owned_fact():
    rows_left = [("Расчетное количество жителей", "269"), ("Дата заполнения", "02.2023"), ("Шифр проекта", "АА/БЭ-03")]
    rows_right = [("Расчетное количество жителей", "255"), ("Дата заполнения", "02.2023"), ("Шифр проекта", "АА/БЭ-03")]
    left, right = _paired_tables(rows_left, rows_right, header=None)
    result = _facts(left, right)
    owned = [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] == "owned_label_value_row"]
    assert [(f["before_value"], f["after_value"]) for f in owned] == [("269", "255")]


def test_one_sided_rows_never_become_removed_or_added_owned_facts():
    left, right = _paired_tables(
        [("Насос", "ЭЦВ", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0"), ("Фильтр", "ФМ", "1,0")],
        [("Насос", "ЭЦВ", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
    )
    result = _facts(left, right)
    assert [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES] == []
    assert all(fact["direction"] not in {"REMOVED", "ADDED"} for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES)
    assert result["diagnostics"]["unresolved_source_evidence"] >= 1


def test_a_one_or_two_letter_difference_is_not_an_owned_fact():
    left, right = _paired_tables(
        [("Установка", "П1Ж", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
        [("Установка", "P1Ж", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
    )
    result = _facts(left, right)
    assert [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES] == []


def test_a_formula_labelled_column_is_excluded_while_the_numeric_column_still_counts():
    header = ("Показатель", "q{\\text{от}}^{\\text{p}}", "Нормативное значение")
    left, right = _paired_tables(
        [("2", "q{\\text{от}}^{\\text{p}};", "0.148"), ("3", "q{\\text{от}}^{\\text{h}};", "0.200"), ("4", "x", "0.300")],
        [("2", "q{от}^p;", "0.142"), ("3", "q{от}^n;", "0.200"), ("4", "x", "0.300")],
        header=header,
    )
    result = _facts(left, right)
    owned = [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES]
    assert [(f["facet_ref"], f["before_value"], f["after_value"]) for f in owned] == [("нормативное_значение", "0.148", "0.142")]


def test_a_tex_value_never_becomes_a_fact_field():
    left, right = _paired_tables(
        [("Формула", "G_{max} = \\sum(G_i)", "1"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
        [("Формула", "G_{max} = \\sum(G_i) \\cdot k", "1"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
    )
    result = _facts(left, right)
    assert [fact for fact in result["facts"] if fact["provenance"]["parser_rule"] in OWNED_RULES] == []


# ── 6. Артефакт владельца ──────────────────────────────────────────────────

def test_the_ownership_artifact_accounts_every_atom_and_uses_no_model():
    left, right = _paired_tables(
        [("Насос", "ЭЦВ", "12,5"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
        [("Насос", "ЭЦВ", "19,2"), ("Клапан", "КВ-1", "3,2"), ("Задвижка", "30ч6бр", "8,0")],
    )
    preparation = _preparation(left, right)
    atoms = {"input_signature": "atoms", "atoms": [{
        "atom_id": "tatom_1", "direction": "ALTERED",
        "provenance": {"semantic_fact_id": None, "locations": {
            "LEFT": [{"page": 10, "fragment_id": "left-r0"}], "RIGHT": [{"page": 24, "fragment_id": "right-r0"}],
        }},
    }]}
    artifact = ownership.build_text_fact_ownership(pair_id="pair-1", atoms_artifact=atoms, text_preparation=preparation, generated_at="fixed")
    assert artifact["kind"] == ownership.KIND
    assert artifact["diagnostics"]["atoms"] == 1 and artifact["diagnostics"]["uses_model"] is False
    record = artifact["ownership"][0]
    assert record["source_text_atom_id"] == "tatom_1"
    assert record["ownership_status"] == "PROVEN" and record["ownership_channel"] == "EXACT_TABLE_ROW"
    assert {ref["side"] for ref in record["evidence_refs"]} == {"LEFT", "RIGHT"}
    again = ownership.build_text_fact_ownership(pair_id="pair-1", atoms_artifact=atoms, text_preparation=preparation, generated_at="fixed")
    assert again["input_signature"] == artifact["input_signature"]
