"""Регресс-тест правила vc1.12 (engine.compute_sheet_pair, класс ``style``).

Правило: регион без пикселей «только A/B», светлых и цветовых отличий — «оформление»
(1–3 px толщины/тона), даже если рядом слова. Тест проверяет, что правило НЕ скрывает
реальные изменения текста, линий и цвета, и что чистое изменение толщины — ``style``.
"""
from __future__ import annotations

from tests.test_version_comparison_engine import base, classes, compare, vc_root  # noqa: F401

REAL = ("text", "added", "removed", "changed", "color", "light")


def _real(r):
    return [g for g in r["regions"] if g["class"] in REAL]


def test_text_change_not_hidden_as_style(vc_root):
    r = compare(vc_root, base(), base(values={"pp": "Рр=324.26кВт"}), tag="t")
    assert any(g["class"] == "text" and g["text_changes"] for g in r["regions"]), classes(r)
    assert r["status"] == "different"


def test_text_with_thickness_change_nearby_stays_text(vc_root):
    r = compare(vc_root, base(), base(values={"pp": "Рр=324.26кВт"}, content={"line_width": 1.2}), tag="tw")
    assert any(g["class"] == "text" for g in r["regions"]), classes(r)


def test_added_and_removed_lines_not_hidden(vc_root):
    r = compare(vc_root, base(), base(content={"extra_line": True}), tag="l")
    assert any(g["class"] == "added" for g in r["regions"]), classes(r)
    r = compare(vc_root, base(), base(content={"remove_line": True}), tag="rl")
    assert any(g["class"] == "removed" for g in r["regions"]), classes(r)


def test_color_change_not_hidden(vc_root):
    r = compare(vc_root, base(), base(content={"line_color": (1, 0, 0)}), tag="c")
    assert any(g["class"] in ("color", "style") for g in r["regions"]) and r["channels"]["color"]["diffs"] > 0, classes(r)
    assert not any(g["class"] == "style" and (g["a_only_px"] or g["b_only_px"] or g["color_px"]) for g in r["regions"])


def test_style_regions_have_no_real_pixels(vc_root):
    for tag, spec in (("w", base(content={"line_width": 1.2})), ("t", base(values={"pp": "Рр=324.26кВт"})),
                      ("x", base(content={"extra_line": True}))):
        r = compare(vc_root, base(), spec, tag=tag)
        for g in r["regions"]:
            if g["class"] == "style" and not g.get("text_changes"):
                assert not (g["a_only_px"] or g["b_only_px"] or g["light_a_px"] or g["light_b_px"]), (tag, g)
