from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend.app.pipeline.stages.block_grounding import architecture_geometry as ar
from backend.app.pipeline.stages.block_grounding import block_source_router as router
from backend.app.pipeline.stages.block_grounding import low_voltage_geometry as lv


FLAG = "STAGE01_PER_BLOCK_PROFILE_ROUTING_ENABLED"


def test_per_block_profile_flag_is_default_off(monkeypatch):
    monkeypatch.delenv(FLAG, raising=False)
    assert router.per_block_profile_routing_enabled() is False
    monkeypatch.setenv(FLAG, "true")
    assert router.per_block_profile_routing_enabled() is True


def test_prepared_packages_are_separated_between_ab_modes(monkeypatch):
    output = "/tmp/objects/O/disciplines/AI/documents/D/versions/v1/out"
    old_package = {
        "source_kind": "structured_architecture",
        "classification": {},
    }
    on_package = {
        "source_kind": "raw_vector",
        "classification": {
            "profile_routing": {
                "policy": router._PER_BLOCK_PROFILE_ROUTING_POLICY,
                "enabled": True,
            }
        },
    }
    monkeypatch.setattr(router, "_locate", lambda _output: (None, None))

    monkeypatch.setattr(
        router, "load_prepared_package", lambda _output, _bid: old_package
    )
    monkeypatch.setenv(FLAG, "true")
    refreshed_on = router.resolve_block_package(output, "B", 1)
    assert refreshed_on["source_kind"] == "no_sources"
    monkeypatch.delenv(FLAG)
    assert router.resolve_block_package(output, "B", 1) is old_package

    monkeypatch.setattr(
        router, "load_prepared_package", lambda _output, _bid: on_package
    )
    refreshed_off = router.resolve_block_package(output, "B", 1)
    assert refreshed_off["source_kind"] == "no_sources"
    monkeypatch.setenv(FLAG, "true")
    assert router.resolve_block_package(output, "B", 1) is on_package



def test_per_block_profile_flag_is_scoped_to_ai(monkeypatch):
    monkeypatch.setenv(FLAG, "true")
    ai = "/tmp/objects/O/disciplines/AI/documents/D/versions/v1/out"
    ar_path = "/tmp/objects/O/disciplines/AR/documents/D/versions/v1/out"
    assert router._per_block_profile_routing_applies(ai) is True
    assert router._per_block_profile_routing_applies(ar_path) is False


def test_parking_plan_overrides_generic_plan_profile():
    decision = router._per_block_profile_route(
        block_type="План",
        classification_text=(
            "План напольных покрытий паркинга второго подвального уровня. "
            "Показаны стояночные места, криволинейная рампа и проезды."
        ),
        block_text="10.42° 10.20° 7.45°; ширины 5470, 5600 и 5025 мм.",
    )

    assert decision["graphic_profile_id"] == "architectural_plan_or_facade"
    assert decision["selected_source_kind"] == "raw_vector"
    assert decision["signal_source"] == "block_content"
    assert decision["reason"] == "parking_geometry"


@pytest.mark.parametrize(
    ("sheet_name", "block_type", "classification_text"),
    [
        ("Развёртки стен помещений", "Развёртка", ""),
        ("Ведомость отделки помещений", "Таблица", ""),
        ("Эскизы и узлы дверей", "Эскиз", "Двери Д20 и Д22"),
        ("", "Спецификация дверей", "Д14 и Д13.2, ручка антипаника"),
        ("План потолков и чистовой отделки", "План", ""),
        ("", "План", "План потолков и отделки -1 этажа"),
        ("", "Узел", "Конструктивный узел пола для паркинга с отделкой"),
    ],
)
def test_interior_blocks_keep_structured_architecture(
    sheet_name, block_type, classification_text
):
    decision = router._per_block_profile_route(
        sheet_name=sheet_name,
        block_type=block_type,
        classification_text=classification_text,
    )
    assert decision["selected_source_kind"] == "structured_architecture"


def test_reference_sheet_and_sheet_0_1_route_to_raw_vector():
    by_name = router._per_block_profile_route(
        sheet_name="Ведомость ссылочных документов",
        block_type="Таблица",
        classification_text="Ведомость дверей Д20",
    )
    by_number = router._per_block_profile_route(
        sheet_no="0.1",
        block_type="План",
        classification_text="Дверной узел",
    )
    assert by_name["selected_source_kind"] == "raw_vector"
    assert by_name["signal_source"] == "sheet_name"
    assert by_number["selected_source_kind"] == "raw_vector"
    assert by_number["reason"] == "title_or_reference_sheet_0_1"


def test_multiple_decimal_angles_are_geometry_fallback():
    decision = router._per_block_profile_route(
        block_type="План",
        block_text="10.42° 10.20° 7.45° 5470 5600 5025",
    )
    assert decision["selected_source_kind"] == "raw_vector"
    assert decision["signal_source"] == "block_text_geometry"


def test_resolver_routes_specialized_low_voltage_graph_to_ctx_profile(
    tmp_path, monkeypatch
):
    output = tmp_path / "objects/O/disciplines/SS/documents/D/versions/v1/out"
    output.mkdir(parents=True)
    graph_path = output / "document_graph.json"
    graph_path.write_text('{"pages": []}', encoding="utf-8")
    pdf_path = output.parent / "document.pdf"
    page_text = "Структурная схема АПС АЛС1.1 " + ("1A1.1 " * 12)
    block_text = "реконструированный текст слов " + ("фрагмент " * 8)
    captured = {}

    monkeypatch.setattr(router, "_locate", lambda _: (pdf_path, graph_path))
    monkeypatch.setattr(
        router,
        "_extract_block",
        lambda *_args: (page_text, block_text, [0, 0, 1, 1], None, 1),
    )
    monkeypatch.setattr(router, "_load_chandra_description", lambda *_args: None)

    def build_low_voltage(_pdf, vector_text, **_kwargs):
        captured["vector_text"] = vector_text
        captured["bbox_norm"] = _kwargs.get("bbox_norm")
        return {
            "profile_id": "low_voltage_scheme",
            "subtype": "aps_structural",
            "source": {},
            "root": "ПО №1",
            "loops": [{"id": "АЛС1.1", "floors": [{"floor": 1}]}],
            "floors": [{"floor": 1}],
            "devices": [{
                "id": "device-1", "address": "1A1.1", "loop": "АЛС1.1",
                "floor": 1, "status": "present",
            }],
            "edges": [],
            "validation": {},
            "warnings": [],
            "status": "ok",
        }

    monkeypatch.setattr(
        lv,
        "build_low_voltage_graph",
        build_low_voltage,
    )
    monkeypatch.setattr(lv, "evaluate_low_voltage_gate", lambda _graph: {"use": True})
    monkeypatch.setattr(
        lv,
        "render_low_voltage_graph_markdown",
        lambda _graph: "# Структурный CTX-граф АПС\n" + ("связь " * 40),
    )

    package = router.resolve_block_package(
        output, "APS-BLOCK", 1, prefer_prepared=False
    )

    assert package["source_kind"] == "structured_alia_scheme"
    assert package["profile_id"] == "fire_alarm_loop_topology"
    assert package["graph"]["profile_id"] == "fire_alarm_loop_topology"
    assert package["graph"]["nodes"]
    assert package["graph"]["networks"]
    assert package["graph"]["edges"]
    assert package["classification"]["source"] == "vector_block_pdf"
    assert captured["vector_text"] == page_text
    assert captured["bbox_norm"] is None


def _chandra(block_type: str, text: str):
    return SimpleNamespace(
        block_type=block_type,
        classification_text=text,
        short_description=text,
        description=text,
    )


def test_resolver_produces_mixed_sources_and_off_keeps_legacy(
    tmp_path, monkeypatch
):
    output = (
        tmp_path
        / "objects/O/disciplines/AI/documents/D/versions/v1/out"
    )
    output.mkdir(parents=True)
    graph_path = output / "document_graph.json"
    graph_path.write_text(
        json.dumps(
            {
                "pages": [
                    {
                        "page": 1,
                        "page_index": 0,
                        "sheet_name": "План автостоянки и рампы",
                        "image_blocks": [
                            {"id": "PARKING", "type": "План", "coords_norm": [0, 0, 1, 1]}
                        ],
                    },
                    {
                        "page": 2,
                        "page_index": 1,
                        "sheet_name": "Эскизы и узлы дверей",
                        "image_blocks": [
                            {"id": "DOOR", "type": "Эскиз", "coords_norm": [0, 0, 1, 1]}
                        ],
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pdf_path = output.parent / "document.pdf"
    monkeypatch.setattr(router, "_locate", lambda _: (pdf_path, graph_path))

    block_texts = {
        "PARKING": (
            "План автостоянки. Криволинейная рампа: 10.42°, 10.20°, 7.45°. "
            "Ширины проездов 5470, 5600, 5025 мм."
        ),
        "DOOR": (
            "Эскиз двери Д20. Размер 1100х2500. Д14, Д13.2: ручка антипаника. "
            "Размерная цепочка развёртки стен."
        ),
    }
    monkeypatch.setattr(
        router,
        "_extract_block",
        lambda _pdf, _dg, bid: (
            block_texts[bid],
            block_texts[bid],
            [0, 0, 1, 1],
            None,
            1 if bid == "PARKING" else 2,
        ),
    )
    monkeypatch.setattr(
        router,
        "_load_chandra_description",
        lambda _pdf, bid: _chandra(
            "План" if bid == "PARKING" else "Эскиз",
            block_texts[bid],
        ),
    )
    monkeypatch.setattr(ar, "classify_ar_profile", lambda _text: "ar_door_opening_drawing")
    monkeypatch.setattr(
        ar,
        "build_ar_graph_from_source",
        lambda *_args, **_kwargs: {
            "profile_id": "ar_door_opening_drawing",
            "validation": {},
        },
    )
    monkeypatch.setattr(ar, "evaluate_ar_gate", lambda _graph: {"use": True})
    monkeypatch.setattr(
        ar,
        "render_ar_markdown",
        lambda _graph: "# Эталонная текстовая разметка АР\n" + ("дверь " * 40),
    )

    monkeypatch.setenv(FLAG, "true")
    parking = router.resolve_block_package(
        output, "PARKING", 1, prefer_prepared=False
    )
    door = router.resolve_block_package(
        output, "DOOR", 2, prefer_prepared=False
    )

    assert parking["source_kind"] == "raw_vector"
    assert door["source_kind"] == "structured_architecture"
    assert {
        parking["source_kind"], door["source_kind"]
    } == {"raw_vector", "structured_architecture"}
    assert parking["classification"]["profile_routing"]["applied"] is True
    assert door["classification"]["profile_routing"]["applied"] is True
    assert all(
        token in parking["user_text"]
        for token in ("10.42°", "10.20°", "7.45°", "5470", "5600", "5025")
    )

    monkeypatch.delenv(FLAG)
    legacy = router.resolve_block_package(
        output, "PARKING", 1, prefer_prepared=False
    )
    assert legacy["source_kind"] == "structured_architecture"
    assert "profile_routing" not in legacy["classification"]
