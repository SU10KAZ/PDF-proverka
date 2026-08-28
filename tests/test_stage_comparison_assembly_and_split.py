"""Состав конструкции и разрезание сдвоенных строк экспликации.

Две правки одного семейства: документ уже сказал, как читать эти строки, —
осталось перестать отказываться его читать.
"""
from __future__ import annotations

from backend.app.services.stage_comparison.production_text_flow import (
    PREPARATION_KIND,
    PREPARATION_SCHEMA_VERSION,
    build_text_differences_from_preparation,
    split_two_up_table_rows,
)
from backend.app.services.stage_comparison.text_fact_producer import produce_text_facts


def _fragment(
    fragment_id: str,
    side: str,
    parts: list[str],
    *,
    order: int,
    kind: str = "table_row",
    group: str | None = None,
    text: str | None = None,
) -> dict:
    body = text if text is not None else " ".join(parts)
    return {
        "id": fragment_id,
        "stage": "stage_1" if side == "left" else "stage_2",
        "pdf_page": 10 if side == "left" else 24,
        "text": body,
        "canonical_text": body.casefold(),
        "source_block_id": f"{side}-block",
        "source_kind": kind,
        "source_group": group or f"{side}-block:table",
        "location_parts": list(parts),
        "order": order,
        "bboxes": [{"x": .1, "y": .2, "width": .3, "height": .04}],
    }


def _room_header(side: str, *, order: int = 1, doubled: bool = False) -> dict:
    unit = ["Номер помещения", "Наименование", "Площадь, м2", "Кат. помещения"]
    return _fragment(f"{side}-header", side, unit * (2 if doubled else 1), order=order)


def _preparation(left: list[dict], right: list[dict]) -> dict:
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": "pair-1",
        "input_signature": "prepared-input",
        "comparison_groups": [{
            "id": "group-1",
            "left_pages": [10],
            "right_pages": [24],
            "relation_type": "MATCHED",
            "relation_status": "HIGH",
        }],
        "fragments": {"left": left, "right": right},
    }


def _facts(left: list[dict], right: list[dict]) -> dict:
    preparation = _preparation(left, right)
    differences = build_text_differences_from_preparation(
        preparation, generated_at="fixed"
    )
    return produce_text_facts(differences, preparation, generated_at="fixed")


# ── Разрезание сдвоенной строки ───────────────────────────────────────────


def test_a_two_up_row_becomes_one_fragment_per_room():
    rows = [
        _room_header("left", doubled=True),
        _fragment(
            "l1", "left",
            ["02.1", "Рампа", "185,03", "B2", "02.42", "Коридор", "44,10"],
            order=2,
        ),
    ]

    output = split_two_up_table_rows(rows)

    assert [item["id"] for item in output] == ["left-header", "l1#0", "l1#1"]
    assert output[1]["location_parts"] == ["02.1", "Рампа", "185,03", "B2"]
    assert output[2]["location_parts"] == ["02.42", "Коридор", "44,10"]
    assert output[2]["split_from"] == {
        "fragment_id": "l1",
        "unit_index": 1,
        "unit_count": 2,
        "rule": "two_up_room_schedule_row",
    }
    # Части остаются на своей странице и в своих рамках: доказательство
    # физически лежит именно в этой строке.
    assert output[2]["pdf_page"] == rows[1]["pdf_page"]
    assert output[2]["bboxes"] == rows[1]["bboxes"]


def test_a_row_with_a_leftover_tail_is_not_split():
    rows = [
        _room_header("left", doubled=True),
        _fragment(
            "l1", "left",
            ["02.1", "Рампа", "185,03", "B2", "02.42", "Коридор"],
            order=2,
        ),
    ]

    assert [item["id"] for item in split_two_up_table_rows(rows)] == [
        "left-header", "l1"
    ]


def test_a_row_outside_a_proven_room_table_is_never_split():
    rows = [
        _fragment(
            "l1", "left",
            ["02.1", "Рампа", "185,03", "B2", "02.42", "Коридор", "44,10"],
            order=2,
        ),
    ]

    assert [item["id"] for item in split_two_up_table_rows(rows)] == ["l1"]


def test_the_second_room_of_a_glued_row_finds_its_own_counterpart():
    """44,10 → 44,14 было невидимо, пока помещение сидело в чужой строке."""
    left = split_two_up_table_rows([
        _room_header("left", doubled=True),
        _fragment(
            "l1", "left",
            ["02.1", "Рампа", "185,03", "B2", "02.42", "Коридор", "44,10"],
            order=2,
        ),
    ])
    right = [
        _room_header("right"),
        _fragment("r1", "right", ["02.1", "Рампа", "185,03 м2", "B2"], order=2),
        _fragment("r2", "right", ["02.42", "Коридор", "44,14 м2"], order=3),
    ]

    result = _facts(left, right)

    areas = {
        fact["provenance"]["entity"]["original"]: (
            fact["before_value"], fact["after_value"]
        )
        for fact in result["facts"]
        if fact["facet_ref"] == "room_area_m2"
    }
    assert areas == {"02.42": ("44.10 м²", "44.14 м²")}
    # Помещение 02.1 не изменилось — и это не находка, а отсутствие находки.
    assert [
        item["reason_code"] for item in result["not_applicable_source_evidence"]
    ].count("structured_values_identical") == 1


# ── Состав конструкции ────────────────────────────────────────────────────


def _pie(side: str, *, heading: str, layers: list[tuple[str, str | None]],
         start: int = 1) -> list[dict]:
    group = f"{side}-block:table"
    output = [_fragment(
        f"{side}-h{start}", side, [], order=start, kind="paragraph",
        group=group, text=heading,
    )]
    for index, (material, thickness) in enumerate(layers, start=start + 1):
        parts = [material] if thickness is None else [material, thickness]
        output.append(_fragment(f"{side}-{index}", side, parts, order=index, group=group))
    return output


_LAYERS = [
    ("Гравий промытый", "-50 мм"),
    ("Дренажная мембрана Вилладрейн или аналог", "-8 мм"),
    ("Геотекстиль Икопал 300", None),
    ("Гидроизоляция Икопал Ультра В, 1 слой", "-5 мм"),
    ("Цементно-песчаная стяжка М150", "-40 мм"),
    ("Пароизоляционная пленка", None),
]


def test_an_assembly_heading_proves_its_layers():
    left = _pie("left", heading="Кровля тип К3 (толщ. 350-550мм)", layers=_LAYERS)
    changed = [*_LAYERS]
    changed[4] = ("Цементно-песчаная стяжка М150", "-60 мм")
    right = _pie("right", heading="Кровля тип К3 (толщ. 350-550мм)", layers=changed)

    result = _facts(left, right)

    assert result["diagnostics"]["facts_by_rule"] == {
        "recognized_assembly_layer_table": 1
    }
    fact = result["facts"][0]
    assert fact["dimension"] == "PARAMETER"
    assert fact["before_value"] == "40 мм"
    assert fact["after_value"] == "60 мм"
    assert fact["provenance"]["entity"]["original"] == "кровля тип к3"


def test_a_layer_without_a_thickness_is_a_structure_fact():
    left = _pie("left", heading="Кровля тип К3 (толщ. 350-550мм)", layers=_LAYERS)
    right = _pie(
        "right", heading="Кровля тип К3 (толщ. 350-550мм)",
        layers=[item for item in _LAYERS if item[0] != "Пароизоляционная пленка"],
    )

    result = _facts(left, right)

    removed = [fact for fact in result["facts"] if fact["direction"] == "REMOVED"]
    assert [fact["dimension"] for fact in removed] == ["STRUCTURE"]
    assert removed[0]["before_value"] == "пароизоляционная пленка"
    assert removed[0]["after_value"] is None


def test_a_material_repeated_inside_one_assembly_is_refused():
    """Два разных значения под одним именем — не факт, а неоднозначность."""
    duplicated = [*_LAYERS, ("Цементно-песчаная стяжка М150", "-30 мм")]
    left = _pie("left", heading="Кровля тип К3", layers=duplicated)
    right = _pie("right", heading="Кровля тип К3", layers=duplicated)

    result = _facts(left, right)

    facets = {fact["facet_ref"] for fact in result["facts"]}
    assert not any("assembly_layer_thickness" in facet for facet in facets)


def test_layers_before_any_heading_are_not_claimed():
    """Без заголовка состав некому назвать — объекта нет, факта нет."""
    def rows(side: str) -> list[dict]:
        output = []
        for index, (material, thickness) in enumerate(_LAYERS, start=1):
            parts = [material] if thickness is None else [material, thickness]
            output.append(_fragment(f"{side}{index}", side, parts, order=index))
        return output

    result = _facts(rows("l"), rows("r"))

    assert result["facts"] == []


def test_two_assemblies_in_one_block_keep_separate_identities():
    left = [
        *_pie("left", heading="Кровля тип К3", layers=_LAYERS, start=1),
        *_pie("left", heading="Кровля тип К5", layers=_LAYERS, start=20),
    ]
    changed = [*_LAYERS]
    changed[0] = ("Гравий промытый", "-70 мм")
    right = [
        *_pie("right", heading="Кровля тип К3", layers=_LAYERS, start=1),
        *_pie("right", heading="Кровля тип К5", layers=changed, start=20),
    ]

    result = _facts(left, right)

    subjects = {
        fact["provenance"]["entity"]["original"] for fact in result["facts"]
    }
    assert subjects == {"кровля тип к5"}
