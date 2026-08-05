"""Тесты shadow-пилота Вектографа «АР. План потолков и освещения».

Синтетические юниты не используют данные проекта; регрессионная часть
работает на эталонном PDF корпуса. Контрольные количества (10 квартир,
60 марок) — fixture-assert для КОНКРЕТНОГО эталонного листа, а не логика
детектора: сам алгоритм этих чисел не знает (см. test_no_manual_map).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import graph as graph_mod
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import symbols
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.spatial import (
    OccupancyGrid, SpatialIndex, build_chains)

ROOT = Path(__file__).resolve().parent.parent
PKG = ROOT / "backend/app/pipeline/stages/block_grounding/ar_ceiling_lighting"
REFERENCE_PDF = (ROOT / "experiments/блоки разных дисциплин/АР/"
                 "АР — 001 план потолка и освещения — потолок и освещение — YF7P-R6DK-PXT.pdf")


# ------------------------------------------------------------ синтетика

def _span_text(value: str, x0: float, y0: float, *, color="red", vertical=False,
               char_w=4.4, char_h=7.9, gap=0.4, layer="свет нумерация"):
    chars = []
    x, y = x0, y0
    for ch in value:
        chars.append({"c": ch, "bbox": (round(x, 2), round(y, 2),
                                        round(x + char_w, 2), round(y + char_h, 2))})
        if vertical:
            y += char_h + gap
        else:
            x += char_w + gap
    xs0 = min(c["bbox"][0] for c in chars)
    ys0 = min(c["bbox"][1] for c in chars)
    xs1 = max(c["bbox"][2] for c in chars)
    ys1 = max(c["bbox"][3] for c in chars)
    return {"tid": 0, "text": value, "bbox": (xs0, ys0, xs1, ys1),
            "center": ((xs0 + xs1) / 2, (ys0 + ys1) / 2), "layer": layer,
            "color": (1, 0, 0), "color_family": color, "size": char_h,
            "seqno": 1, "opacity": 1.0, "chars": chars}


def _scope_all_block(_bbox):
    return "block"


def test_contiguous_span_stays_one_number():
    inv = {"texts": [_span_text("34", 100, 100)]}
    labels = symbols.split_number_labels(inv, _scope_all_block, None)
    assert [l["value"] for l in labels] == ["34"]


def test_split_span_becomes_two_groups():
    # «34» одним спаном, но знаки разнесены диагонально (две подписи групп)
    span = _span_text("34", 100, 100)
    c2 = span["chars"][1]
    span["chars"][1] = {"c": c2["c"], "bbox": (c2["bbox"][0] + 4.5, c2["bbox"][1] + 6.2,
                                               c2["bbox"][2] + 4.5, c2["bbox"][3] + 6.2)}
    labels = symbols.split_number_labels({"texts": [span]}, _scope_all_block, None)
    assert sorted(l["value"] for l in labels) == ["3", "4"]
    assert all(l["split_from_span"] for l in labels)


def test_vertical_rotated_number_not_split_and_not_reversed():
    inv = {"texts": [_span_text("200", 50, 50, color="black", vertical=True)]}
    labels = symbols.split_number_labels(inv, _scope_all_block, None)
    assert [l["value"] for l in labels] == ["200"]


def test_legend_zone_symbols_excluded():
    span = _span_text("7", 100, 100)
    labels = symbols.split_number_labels({"texts": [span]}, _scope_all_block,
                                         [(90, 90, 120, 120)])
    assert labels == []


FIXTURE_TPL = {"kind": "light_output", "label": "вывод под светильник",
               "signature": {"circles": [6.6], "rects": [], "n_axis_lines": 2,
                             "n_diag_lines": 0, "colors": {"red": 3},
                             "inner_letters": [], "inner_digits": [], "inner_elevations": []}}


def _fixture_cluster(dx=0.0, dy=0.0, d=6.6):
    cx, cy = 200 + dx, 300 + dy
    r = d / 2
    return [
        {"eid": 0, "kind": "circle", "color": "red",
         "bbox": (cx - r, cy - r, cx + r, cy + r), "ref": {"d": d}},
        {"eid": 1, "kind": "line", "color": "red", "bbox": (cx, cy - 6.6, cx, cy + 6.6),
         "ref": {"p1": (cx, cy - 6.6), "p2": (cx, cy + 6.6), "color_family": "red"}},
        {"eid": 2, "kind": "line", "color": "red", "bbox": (cx - 6.6, cy, cx + 6.6, cy),
         "ref": {"p1": (cx - 6.6, cy), "p2": (cx + 6.6, cy), "color_family": "red"}},
    ]


def test_template_match_and_residual():
    def texts_index(bbox, pad=0.0):
        return []
    sig = symbols.cluster_signature(_fixture_cluster(), texts_index)
    tpl, _, _ = symbols.match_template(sig, [FIXTURE_TPL])
    assert tpl is FIXTURE_TPL
    # слишком большой круг → residual, а не «ближайший тип молча»
    sig_bad = symbols.cluster_signature(_fixture_cluster(d=12.0), texts_index)
    tpl_bad, reasons, ambiguous = symbols.match_template(sig_bad, [FIXTURE_TPL])
    assert tpl_bad is None and reasons and not ambiguous


def _blue_quad(qid, x0, y0, w, h):
    return {"qid": qid, "did": qid, "layer": "06_Потолок", "color_family": "blue",
            "kind": "qu", "bbox": (x0, y0, x0 + w, y0 + h), "w": w, "h": h}


def test_ceiling_composite_requires_alignment():
    type_q = _blue_quad(0, 100, 100, 40, 18)
    elev_ok = _blue_quad(1, 100, 120, 40, 13)      # сразу под типом
    elev_far = _blue_quad(2, 260, 220, 40, 13)     # не выровнена
    inv = {"quads": [type_q, elev_ok, elev_far],
           "texts": [_span_text("1", 116, 104, color="blue", layer="06_Потолок"),
                     _span_text("+2.850", 102, 123, color="blue", layer="06_Потолок"),
                     _span_text("+2.850", 262, 223, color="blue", layer="06_Потолок")]}
    markers, unpaired = symbols.detect_ceiling_markers(inv, _scope_all_block, None)
    assert len(markers) == 1
    assert markers[0]["ceiling_type"] == "1"
    assert markers[0]["elevation"] == "+2.850"
    assert len(unpaired) == 1  # одинокая отметка не стала составным маркером


def test_room_name_only_from_schedule_row():
    marks = [
        {"mark": "6.700.1", "building_part": "6", "apartment": "700", "room_suffix": 1,
         "bbox": (0, 0, 10, 10), "center": (5, 5), "layer": "A-AREA-____-IDEN", "tid": 1},
        {"mark": "6.700.2", "building_part": "6", "apartment": "700", "room_suffix": 2,
         "bbox": (0, 20, 10, 30), "center": (5, 25), "layer": "A-AREA-____-IDEN", "tid": 2},
    ]
    ref = {"room_schedule": {"6.700.1": {"name": "Жилая комната", "bbox": (0, 0, 1, 1)}},
           "apartment_cards": []}
    rooms = graph_mod._build_rooms(ref, marks, {"regions": {}})
    assert rooms[0]["name"] == "Жилая комната"
    assert rooms[1]["name"] is None  # соседняя строка ведомости не копируется
    assert rooms[1]["name_binding"]["tier"] == 0


def test_groups_not_merged_between_apartments():
    rooms = [{"mark": "6.700.1", "apartment": "700"}, {"mark": "6.701.1", "apartment": "701"}]
    for r in rooms:
        r.update({"lights": [], "switches": [], "master_switches": []})
    lights = [
        {"id": "light-1", "kind": "light_output", "room": "6.700.1", "groups": ["1"]},
        {"id": "light-2", "kind": "light_output", "room": "6.701.1", "groups": ["1"]},
    ]
    switches = [
        {"id": "switch-1", "kind": "switch_1", "room": "6.700.1", "groups": ["1"]},
    ]
    groups = graph_mod._build_groups(lights, switches, rooms, [])
    ids = sorted(g["group_id"] for g in groups)
    assert ids == ["700:1", "701:1"]
    by_id = {g["group_id"]: g for g in groups}
    assert by_id["700:1"]["state"] == "confirmed"
    assert by_id["701:1"]["state"] == "lights_only"


def test_watershed_contested_band_gets_no_owner():
    grid = OccupancyGrid((0, 0, 100, 25), cell=1.0)
    for j in range(25):  # внешние стены
        grid.blocked.update({(0, j), (99, j)})
    for i in range(100):
        grid.blocked.update({(i, 0), (i, 24)})
    # без внутренней перегородки: две марки в одном объёме
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import rooms as rooms_mod
    marks = [
        {"mark": "6.1.1", "center": (10.0, 12.0)},
        {"mark": "6.1.2", "center": (90.0, 12.0)},
    ]
    regions = {m["mark"]: {"state": "merged", "merged_with": [o["mark"] for o in marks
                                                              if o is not m]} for m in marks}
    floods = {}
    area = {(i, j) for i in range(1, 99) for j in range(1, 24)}
    for m in marks:
        floods[m["mark"]] = set(area)
    seeds = {m["mark"]: grid.cell_of(*m["center"]) for m in marks}
    owner, quality = {}, {}
    rooms_mod._watershed_merged(regions, floods, seeds, owner, quality)
    # у левого края — левая марка, у правого — правая, середина не назначена
    assert owner[(5, 12)] == "6.1.1"
    assert owner[(94, 12)] == "6.1.2"
    mid_cells = [c for c in ((49, 12), (50, 12), (51, 12)) if c in owner]
    assert not mid_cells, "полоса неопределённости не должна получать владельца"


def test_spatial_index_query():
    idx = SpatialIndex(cell=5)
    idx.insert(1, (0, 0, 3, 3))
    idx.insert(2, (50, 50, 53, 53))
    assert idx.query((1, 1, 2, 2)) == [1]
    assert idx.query((0, 0, 60, 60)) == [1, 2]


def test_chains_from_microsegments():
    segs = [{"sid": i, "p1": (i * 10.0, i * 10.0), "p2": (i * 10.0 + 6, i * 10.0 + 6)}
            for i in range(5)]
    chains = build_chains(segs, max_gap=7.0)
    assert len(chains) == 1
    assert chains[0]["length"] > 50


SWITCH2_TPL = {"kind": "switch_2", "label": "выключатель двухклавишный", "source": "sheet_legend",
               "signature": {"circles": [], "rects": [(12.4, 14.2)], "n_axis_lines": 0,
                             "n_diag_lines": 1, "colors": {"red": 2},
                             "inner_letters": [], "inner_digits": [], "inner_elevations": []}}
CHANGEOVER_TPL = {"kind": "switch_changeover", "label": "переключатель", "source": "sheet_legend",
                  "signature": {"circles": [11.9], "rects": [(11.8, 14.3)], "n_axis_lines": 0,
                                "n_diag_lines": 0, "colors": {"red": 2},
                                "inner_letters": [], "inner_digits": [], "inner_elevations": []}}


def _switch2_cluster_with_circled_digits():
    """Рамка + диагональ + два кружка вокруг цифр (оформление подписей)."""
    els = [
        {"eid": 0, "kind": "rect", "color": "red", "bbox": (100, 100, 112.4, 114.2),
         "ref": {"w": 12.4, "h": 14.2, "color_family": "red"}},
        {"eid": 1, "kind": "line", "color": "red", "bbox": (100, 100, 112.4, 114.2),
         "ref": {"p1": (100, 114.2), "p2": (112.4, 100), "color_family": "red"}},
    ]
    for i, cx in enumerate((118.0, 128.0)):
        d = 7.1
        els.append({"eid": 2 + i, "kind": "circle", "color": "red",
                    "bbox": (cx - d / 2, 104 - d / 2, cx + d / 2, 104 + d / 2),
                    "ref": {"d": d, "center": (cx, 104.0), "color_family": "red",
                            "layer": "свет нумерация"}})
    return els


def test_switch2_with_circled_group_numbers_restored():
    digits = [_span_text("3", 116.0, 100.5), _span_text("4", 126.0, 100.5)]

    def texts_index(bbox, pad=0.0):
        out = []
        for t in digits:
            cx = (t["bbox"][0] + t["bbox"][2]) / 2
            cy = (t["bbox"][1] + t["bbox"][3]) / 2
            if bbox[0] - pad <= cx <= bbox[2] + pad and bbox[1] - pad <= cy <= bbox[3] + pad:
                out.append(t)
        return out

    syms = symbols.classify_clusters([_switch2_cluster_with_circled_digits()],
                                     [SWITCH2_TPL, CHANGEOVER_TPL], texts_index)
    assert len(syms) == 1
    assert syms[0]["kind"] == "switch_2", syms[0]
    assert len(syms[0]["label_overlay_circles"]) == 2


def test_changeover_true_circle_not_stripped():
    """Истинная окружность переключателя (без цифры внутри) не снимается."""
    d = 11.9
    cluster = [
        {"eid": 0, "kind": "rect", "color": "red", "bbox": (100, 100, 111.8, 114.3),
         "ref": {"w": 11.8, "h": 14.3, "color_family": "red"}},
        {"eid": 1, "kind": "circle", "color": "red",
         "bbox": (106 - d / 2, 107 - d / 2, 106 + d / 2, 107 + d / 2),
         "ref": {"d": d, "center": (106.0, 107.0), "color_family": "red",
                 "layer": "09_Освещение"}},
    ]

    def texts_index(bbox, pad=0.0):
        return []

    syms = symbols.classify_clusters([cluster], [SWITCH2_TPL, CHANGEOVER_TPL], texts_index)
    assert len(syms) == 1
    assert syms[0]["kind"] == "switch_changeover"
    assert "label_overlay_circles" not in syms[0]


def test_ambiguous_templates_do_not_pick_first():
    """Два сильных шаблона разных видов → неоднозначность, не выбор первого."""
    tpl_a = dict(SWITCH2_TPL)
    tpl_b = {**SWITCH2_TPL, "kind": "master_switch", "label": "мастер"}
    cluster = [
        {"eid": 0, "kind": "rect", "color": "red", "bbox": (100, 100, 112.4, 114.2),
         "ref": {"w": 12.4, "h": 14.2, "color_family": "red"}},
        {"eid": 1, "kind": "line", "color": "red", "bbox": (100, 100, 112.4, 114.2),
         "ref": {"p1": (100, 114.2), "p2": (112.4, 100), "color_family": "red"}},
    ]

    def texts_index(bbox, pad=0.0):
        return []

    syms = symbols.classify_clusters([cluster], [tpl_a, tpl_b], texts_index)
    assert syms[0]["kind"] == "unresolved_symbol"
    assert syms[0]["reason"] == "multiple_templates_match"
    assert syms[0]["matched_kinds"] == ["master_switch", "switch_2"]


def test_run_profile_no_graph_for_alien_pdf(tmp_path):
    """Чужой тип блока → no_graph/profile_not_applicable, не пустой граф."""
    import fitz
    pdf = tmp_path / "alien.pdf"
    doc = fitz.open()
    page = doc.new_page(width=400, height=300)
    page.insert_text((50, 50), "Spec sheet: pump curve", fontsize=12)
    page.insert_text((50, 80), "Discharge head baseline", fontsize=10)
    doc.save(str(pdf))
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import run_profile
    result = run_profile(str(pdf))
    assert result["status"] == "no_graph"
    assert result["reason"] == "profile_not_applicable"
    assert result["graph"] is None


def test_run_profile_rotation_unsupported(tmp_path):
    import fitz
    pdf = tmp_path / "rotated.pdf"
    doc = fitz.open()
    page = doc.new_page(width=400, height=300)
    page.insert_text((50, 50), "any", fontsize=12)
    page.set_rotation(90)
    doc.save(str(pdf))
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import run_profile
    result = run_profile(str(pdf))
    assert result["status"] == "no_graph"
    assert result["reason"] == "rotation_unsupported"


# ------------------------------------------------- запрет ручных карт

def test_no_manual_map_of_this_pdf_in_code():
    forbidden = [r"YF7P", r"6\.70\d", r"\b70[0-9]\s*:\s*", r"Жилая комната"]
    sources = list(PKG.glob("*.py")) + [
        ROOT / "scripts/build_ar_ceiling_lighting_description.py",
        ROOT / "scripts/build_ar_ceiling_lighting_corpus.py",
    ]
    for path in sources:
        text = path.read_text(encoding="utf-8")
        for pattern in forbidden:
            assert not re.search(pattern, text), f"{path.name}: найден шаблон {pattern!r}"


# ------------------------------------------------------------ регресс

@pytest.fixture(scope="module")
def reference_result():
    if not REFERENCE_PDF.is_file():
        pytest.skip("эталонный PDF корпуса недоступен")
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.runner import (
        build_ar_ceiling_lighting_result)
    return build_ar_ceiling_lighting_result(str(REFERENCE_PDF))


def test_reference_apartments_700_709(reference_result):
    g = reference_result["graph"]
    assert [a["id"] for a in g["apartments"]] == [str(n) for n in range(700, 710)]


def test_reference_60_unique_room_marks(reference_result):
    g = reference_result["graph"]
    marks = [r["mark"] for r in g["rooms"]]
    assert len(marks) == 60 and len(set(marks)) == 60
    expected = {"700": 8, "701": 4, "702": 6, "703": 7, "704": 4,
                "705": 6, "706": 7, "707": 5, "708": 6, "709": 7}
    per_apt = {}
    for r in g["rooms"]:
        per_apt[r["apartment"]] = per_apt.get(r["apartment"], 0) + 1
    assert per_apt == expected


def test_reference_each_mark_in_exactly_one_apartment(reference_result):
    g = reference_result["graph"]
    seen = {}
    for apt in g["apartments"]:
        for mark in apt["rooms"]:
            assert mark not in seen, f"{mark} в двух квартирах"
            seen[mark] = apt["id"]
    assert len(seen) == 60


def test_reference_group_numbers_scoped_by_apartment(reference_result):
    g = reference_result["graph"]
    by_number = {}
    for grp in g["groups"]:
        assert grp["group_id"] == f"{grp['apartment']}:{grp['number']}"
        by_number.setdefault(grp["number"], set()).add(grp["apartment"])
    assert any(len(apts) > 1 for apts in by_number.values()), \
        "одинаковые номера в разных квартирах должны существовать раздельно"


def test_reference_legend_symbols_not_counted(reference_result):
    g = reference_result["graph"]
    ref = reference_result["ref"]
    zone = ref["legend_zone"]
    for dev in g["lights"] + g["switches"] + g["master_switches"]:
        cx, cy = dev["center"]
        inside = zone[0] <= cx <= zone[2] and zone[1] <= cy <= zone[3]
        assert not inside, f"{dev['id']} внутри зоны легенды"
    for z in g["ceiling_zones"]:
        cx, cy = z["center"]
        assert not (zone[0] <= cx <= zone[2] and zone[1] <= cy <= zone[3])


def test_reference_no_gray_electrical_edges(reference_result):
    g = reference_result["graph"]
    light_ids = {x["id"] for x in g["lights"]}
    for edge in g["edges"]:
        if edge["source"] in light_ids and edge["target"] in light_ids:
            pytest.fail("ребро между световыми точками — серые диагонали стали связями")
    allowed = {"contains", "member_of_group", "controls", "intended_scope", "dimension_anchor"}
    assert {e["edge_type"] for e in g["edges"]} <= allowed


def test_reference_master_without_group_edges(reference_result):
    g = reference_result["graph"]
    master_ids = {m["id"] for m in g["master_switches"]}
    for edge in g["edges"]:
        if edge["edge_type"] == "controls":
            assert edge["source"] not in master_ids, \
                "мастер-выключатель не должен получать прямые controls-рёбра"
    for m in g["master_switches"]:
        assert m["intended_scope"]["scope"] in ("apartment", None)


def test_reference_ceiling_composites_have_both_fields(reference_result):
    g = reference_result["graph"]
    assert g["ceiling_zones"], "потолочные марки не найдены"
    for z in g["ceiling_zones"]:
        assert z["ceiling_type"], "тип потолка пуст в составном маркере"
        assert re.match(r"^[+\-]\d+\.\d{2,3}$", z["elevation"] or ""), z["elevation"]


def test_reference_room_names_only_from_schedule(reference_result):
    g = reference_result["graph"]
    schedule = reference_result["ref"]["room_schedule"]
    for room in g["rooms"]:
        if room["name"] is not None:
            assert room["mark"] in schedule
            assert schedule[room["mark"]]["name"] == room["name"]


def test_reference_unassigned_goes_to_ledger(reference_result):
    g = reference_result["graph"]
    kinds = {l["kind"] for l in g["semantic_ledger"]}
    assert "unresolved_symbol" in kinds
    assert g["validation"]["ledger_total"] > 0


def test_reference_dimensions_are_full_constructions(reference_result):
    g = reference_result["graph"]
    assert g["dimensions"], "размерные конструкции не найдены"
    for dim in g["dimensions"]:
        assert dim["value_mm"] >= 10
        assert dim["gap_pt"] > 0
    consumed = {d["label_id"] for d in g["dimensions"]}
    loose = [l for l in g["semantic_ledger"] if l["kind"] == "number_without_construction"]
    assert loose, "числа без конструкции должны оставаться в ledger, а не становиться размерами"
    assert not any(l.get("label_id") in consumed for l in loose)


def test_reference_deterministic_two_runs(reference_result):
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.runner import (
        build_ar_ceiling_lighting_result)
    second = build_ar_ceiling_lighting_result(str(REFERENCE_PDF))
    a = json.dumps(reference_result["graph"], ensure_ascii=False, sort_keys=True)
    b = json.dumps(second["graph"], ensure_ascii=False, sort_keys=True)
    assert a == b


def test_reference_runtime_sane(reference_result):
    # стратификация и grid-hash: полный лист (34 тыс. drawings) за десятки
    # секунд максимум, а не квадратичные минуты
    assert reference_result["elapsed_s"] < 60


# ------------------------------------------ регресс с реестром легенд

REGISTRY_JSON = ROOT / "experiments/vectograf/ar_ceiling_lighting/legend_registry.json"


@pytest.fixture(scope="module")
def reference_with_registry():
    if not REFERENCE_PDF.is_file():
        pytest.skip("эталонный PDF корпуса недоступен")
    if not REGISTRY_JSON.is_file():
        pytest.skip("legend_registry.json не построен")
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import run_profile
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.registry import (
        load_legend_registry)
    return run_profile(str(REFERENCE_PDF), legend_registry=load_legend_registry(REGISTRY_JSON))


def test_reference_status_complete(reference_with_registry):
    assert reference_with_registry["status"] == "complete"


def test_reference_wall_lights_restored_from_registry(reference_with_registry):
    """На листе 001 подписи «вывод под настенный светильник» нет — тип
    приходит из кросс-листового реестра (tier 4), а не угадывается."""
    g = reference_with_registry["graph"]
    walls = [x for x in g["lights"] if x["kind"] == "wall_light_output"]
    assert len(walls) == 13
    assert all(x["classification_source"] == "cross_sheet_legend_registry" for x in walls)
    assert any(w.startswith("LEGEND_FROM_REGISTRY") for w in g["warnings"])


def test_reference_two_key_switches_with_circles_restored(reference_with_registry):
    g = reference_with_registry["graph"]
    with_overlays = [s for s in g["switches"] if s.get("label_overlay_circles")]
    assert len(with_overlays) >= 5, "двухклавишные с обведёнными цифрами не восстановлены"
    for sw in with_overlays:
        assert sw["kind"] in ("switch_1", "switch_2", "switch_changeover")
        assert sw["groups"], "цифры внутри кружков должны стать управляемыми группами"


def test_reference_no_unresolved_symbols_with_registry(reference_with_registry):
    g = reference_with_registry["graph"]
    assert g["validation"]["unresolved_symbols_total"] == 0


def test_reference_dimension_tiers_disciplined(reference_with_registry):
    """tier 3 — только подтверждённая выносная цепочка + масштаб; близость
    без цепочки остаётся tier 2 candidate/requires_review."""
    g = reference_with_registry["graph"]
    seen_tier3 = False
    for dev in g["switches"] + g["master_switches"] + g["lights"]:
        for dim in dev.get("dimensions", []):
            if dim["tier"] >= 3:
                seen_tier3 = True
                assert dim["scale_consistent"] is True
                assert dim["binding"] == "extension_chain_confirmed"
            else:
                assert dim["requires_review"] is True
    assert seen_tier3, "ни одной подтверждённой размерной связи"


def test_reference_conflicts_present_everywhere(reference_with_registry):
    """GEOMETRY_CONFLICT виден в графе, метриках и Markdown одновременно."""
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.render_md import (
        render_markdown)
    g = reference_with_registry["graph"]
    if not g["conflicts"]:
        pytest.skip("на эталоне нет конфликтов")
    assert g["validation"]["conflicts_total"] == len(g["conflicts"])
    md = render_markdown(g)
    assert "Геометрические конфликты" in md


def test_reference_markdown_has_all_apartments(reference_with_registry):
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.render_md import (
        render_markdown)
    md = render_markdown(reference_with_registry["graph"])
    for n in range(700, 710):
        assert f"## Квартира {n}" in md
    assert "### Помещение 6.709.1" in md


# ------------------------------------------------- компактное описание

@pytest.fixture(scope="module")
def compact_md(reference_with_registry):
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import (
        render_markdown_compact)
    return render_markdown_compact(reference_with_registry["graph"])


def test_compact_has_first_and_last_apartment(compact_md):
    assert "## Квартира 700" in compact_md
    assert "## Квартира 709" in compact_md


def test_compact_has_all_60_room_marks(compact_md, reference_with_registry):
    marks = {r["mark"] for r in reference_with_registry["graph"]["rooms"]}
    assert len(marks) == 60
    for mark in marks:
        assert f"### {mark}" in compact_md, f"марка {mark} потеряна в compact"


def test_compact_room_6709_1_content(compact_md):
    section = compact_md.split("### 6.709.1")[1].split("###")[0]
    assert "тип 1" in section and "+2.850" in section
    assert "люстру, группа 7" in section
    assert "одноклавишный выключатель группы 7" in section
    assert "200 мм" in section


def test_compact_incomplete_groups_5_6_apartment_709(compact_md):
    apt709 = compact_md.split("## Квартира 709")[1]
    groups_line = [l for l in apt709.split("\n") if l.startswith("**Группы квартиры:**")]
    assert groups_line, "сводка групп квартиры 709 не найдена"
    assert "5 и 6 — найдены только выключатели" in groups_line[0]


def test_compact_contains_both_conflicts(compact_md, reference_with_registry):
    conflicts = reference_with_registry["graph"]["conflicts"]
    assert len(conflicts) == 2
    assert "равноудалён от двух устройств" in compact_md
    assert "пересекается с другим классифицированным устройством" in compact_md


def test_full_markdown_unchanged_and_kept(reference_with_registry):
    """Полный рендер не изменён: прежняя структура и полнота сохраняются."""
    from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.render_md import (
        render_markdown)
    full = render_markdown(reference_with_registry["graph"])
    assert "## Данные листа" in full
    assert "### Сводка групп освещения квартиры 700" in full
    assert "## Контроль результата по всему блоку" in full
    assert "### Помещение 6.709.1" in full
    assert len(full) > 40000


def test_compact_no_sym_ids_and_no_coordinates(compact_md):
    assert not re.search(r"\bsym-\d+\b", compact_md)
    assert not re.search(r"\b\d{3,4}\.\d{2}\s*,\s*\d{3,4}\.\d{2}\b", compact_md)
    assert "bbox" not in compact_md and "tier" not in compact_md


def test_compact_no_spread_phrase(compact_md):
    assert "распространена на всё помещение" not in compact_md


def test_compact_master_note_once(compact_md):
    assert compact_md.count("перечень отключаемых групп") == 1


def test_compact_ambiguity_not_duplicated(compact_md):
    assert compact_md.count("равноудалён от двух устройств") == 1
    assert compact_md.count("пересекается с другим классифицированным устройством") == 1


def test_compact_size_limit(compact_md):
    assert len(compact_md) <= 25000, f"compact {len(compact_md)} символов > 25000"
    assert compact_md.count("\n") < 600
