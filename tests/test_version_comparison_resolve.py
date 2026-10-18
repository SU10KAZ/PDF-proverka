"""Этап 2 сопоставления листов (resolve.py): спорные пары разрешаются доказательством, без снижения порогов.

Отличительные слова против каждого конкурента; марки/позиции весомее чисел; перенос строк таблицы — связь
блока, а не пара 1:1; разные разрешения — разные документы; одинаковое название без подтверждения
содержанием пары не даёт; двойники без различий остаются спорными; дубль внутри версии — только при
одинаковой графике.
"""
from __future__ import annotations

from collections import Counter

import numpy as np

from backend.app.services.version_comparison import lineage, resolve, settings  # noqa: F401
from tests.test_version_comparison_api import client  # noqa: F401

V12 = frozenset(settings.MATCH_RULES_V12)


class P:
    """Минимальная страница для resolve: слова, тип, название, номер разрешения, маска."""

    def __init__(self, page, words, kind="drawing", title="", permit_no="", mask=None, designation=""):
        self.page, self.kind, self.title, self.permit_no, self.designation = page, kind, title, permit_no, designation
        self.tokens = Counter(words)
        self.token_total = sum(self.tokens.values())
        self.has_text = self.token_total >= settings.MATCH_TEXT_MIN_TOKENS
        self.mask = mask

    def full_mask(self):
        return self.mask


def _resolve(prev, cur, S):
    S = np.array(S, dtype=float)
    parts = {(i, j): {"text": float(S[i, j])} for i in range(len(prev)) for j in range(len(cur))}
    return resolve.resolve(prev, cur, S, parts, {}, V12)


COMMON = [f"слово{c}" for c in "абвгдежзиклмнопрст"]


def _fillers(n, start):
    return [P(start + k, [f"прочее{k}{c}" for c in "абвгдежзиклм"]) for k in range(n)]


def test_distinct_marks_decide_between_close_candidates():
    # Опросные листы поставщика: общий шаблон и числа; решают марки установки («в10…» против «в12…»).
    a = P(1, COMMON + ["в10", "в10-шв", "в10.1", "20.1", "3.2", "17.5"])
    b1 = P(1, COMMON + ["в10", "в10-шв", "в10.1", "21.4", "3.5", "18.0"])
    b2 = P(2, COMMON + ["в12", "в12-шв", "в12.1", "20.1", "3.2", "17.5"])
    prev, cur = [a] + _fillers(10, 2), [b1, b2] + _fillers(10, 3)
    S = np.full((len(prev), len(cur)), 0.05)
    S[0, 0], S[0, 1] = 0.80, 0.81                      # по общей оценке второй даже чуть выше
    out, reasons = _resolve(prev, cur, S)
    assert out[0][0] == 0 and out[0][2]["resolved"] == "text"     # числа, совпавшие с b2, марки не перевешивают


def test_twins_stay_disputed():
    a = P(1, COMMON); b1 = P(1, COMMON); b2 = P(2, COMMON)
    out, reasons = _resolve([a], [b1, b2], [[0.9, 0.9]])
    assert 0 not in out and reasons[("a", 0)] in ("twin", "no_evidence")


def test_table_rows_moved_between_pages_is_block_not_pair():
    # Позиции листа A разошлись по двум листам B (перенос строк), с обеих сторон — связь блока.
    rows1 = [f"к1.{i}" for i in range(8)]; rows2 = [f"к2.{i}" for i in range(8)]
    a1 = P(1, COMMON + rows1, kind="spec"); a2 = P(2, COMMON + rows2, kind="spec")
    b1 = P(1, COMMON + rows1[:5], kind="spec"); b2 = P(2, COMMON + rows1[5:] + rows2[:5], kind="spec")
    b3 = P(3, COMMON + rows2[5:], kind="spec")
    S = [[0.6, 0.5, 0.3], [0.3, 0.55, 0.5]]
    out, reasons = _resolve([a1, a2], [b1, b2, b3], S)
    # Группа листов таблицы: листы связаны по порядку с пометкой «group» (не «лист в лист»), лишний B3 — без пары.
    assert {i: (j, parts["resolved"]) for i, (j, _s, parts) in out.items()} == {0: (0, "group"), 1: (1, "group")}
    assert reasons[("b", 2)][0] == "block"
    # Без части group — только причина «связь блока», пары 1:1 нет.
    Sa = np.array(S, dtype=float)
    parts_all = {(i, j): {"text": float(Sa[i, j])} for i in range(2) for j in range(3)}
    out0, reasons0 = resolve.resolve([a1, a2], [b1, b2, b3], Sa, parts_all, {}, V12 - {"group"})
    assert any(isinstance(v, tuple) and v[0] == "block" for v in reasons0.values()) and 1 not in out0


def test_different_permits_never_pair():
    a = P(4, COMMON + ["изменение", "лист"], kind="permit", permit_no="853/25")
    b = P(3, COMMON + ["изменение", "лист"], kind="permit", permit_no="1238/25")
    out, reasons = _resolve([a], [b], [[0.7]])
    assert 0 not in out and reasons[("a", 0)] == "permit"
    same = P(3, COMMON + ["изменение", "лист"], kind="permit", permit_no="853/25")
    assert not resolve.permit_conflict(a, same)              # то же разрешение — может быть парой
    plain = P(3, COMMON + ["изменение", "лист"])
    assert resolve.permit_conflict(a, plain)                 # разрешение и обычный лист — нет


def test_same_title_without_content_support_is_not_enough():
    a = P(7, [f"а{i}" for i in range(20)], title="схема систем к13")
    b = P(7, [f"б{i}" for i in range(20)], title="схема систем к13")
    S = np.array([[0.4]])
    out, reasons = resolve.resolve([a], [b], S, {(0, 0): {"title": 1.0}}, {}, V12)
    assert 0 not in out and reasons[("a", 0)] == "title_only"


def test_duplicate_inside_version_needs_same_drawing():
    m = np.zeros((40, 40), np.uint8); m[5:10, 5:30] = 1
    m2 = m.copy(); m2[20:35, 5:35] = 1               # подписи и печать на титуле — другая графика
    a = P(1, COMMON, mask=m); b = P(2, COMMON, mask=m.copy()); c = P(3, COMMON, mask=m2)
    assert resolve.same_drawing(a, b) and not resolve.same_drawing(a, c)


def test_v12_includes_stage1_and_is_default(monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.2")
    assert settings.match_rules() == V12 and {"resolve", "package", "direct", "permit"} <= V12
    monkeypatch.delenv("VERSION_COMPARISON_MATCH_RULES")
    assert settings.match_rules() == V12               # по умолчанию — v1.2 (решение 17.10)
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1")
    assert settings.match_rules() == frozenset()       # прежнее сопоставление — явно


def test_nonadjacent_pairs_checked_directly(client, tmp_path, monkeypatch):  # noqa: F811
    """V1 → V3: пара по цепочке, подтверждённая прямой сверкой, сравнивается автоматически (direct);
    без правила direct та же пара — только по цепочке и не сравнивается (CHAIN_ONLY)."""
    from tests.test_version_comparison_api import OBJ as API_OBJ, _setup_project, _wait
    base = f"/api/version-comparison/objects/{API_OBJ}"

    def run(pid, a, b):
        r = client.post(f"{base}/projects/{pid}/runs", json={"a_version": a, "b_version": b, "scope": "all"}).json()
        return _wait(lambda: client.get(f"{base}/runs/{r['run_id']}").json(), lambda d: d["state"] == "done")

    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.2")
    pid, vids, _ = _setup_project(client, tmp_path)
    _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")
    far = run(pid, vids[0], vids[2])
    compared = [it for it in far["items"] if it["kind"] == "compare"]
    assert compared and all(it["confidence"] == "direct" and "CHAIN_ONLY" not in it["reasons"] for it in compared)
    assert not [it for it in far["items"] if it["kind"] == "chain"]

    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1")
    _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")
    far11 = run(pid, vids[0], vids[2])
    assert [it for it in far11["items"] if it["kind"] == "chain"] and not any(it["kind"] == "compare" for it in far11["items"])


def test_codes_with_separators_weigh_like_marks():
    assert resolve.token_class("1.13.3") == settings.RESOLVE_W_MARK
    assert resolve.token_class("0986-04-24_2") == settings.RESOLVE_W_MARK
    assert resolve.token_class("2025-10-13") == settings.RESOLVE_W_NUMBER       # дата — не обозначение
    assert resolve.token_class("13.836") == settings.RESOLVE_W_NUMBER           # значение


def test_identical_twins_take_nearest():
    m = np.zeros((40, 40), np.uint8); m[5:10, 5:30] = 1
    a = P(5, COMMON, mask=m); a.rel = 0.40
    b1 = P(5, COMMON, mask=m.copy()); b1.rel = 0.41
    b2 = P(9, COMMON, mask=m.copy()); b2.rel = 0.80
    k, how = resolve._winner_content(a, [b2, b1], {t: 1.0 for t in COMMON})
    assert (k, how) == (1, "twin_nearest")


def test_diverged_duplicates_keep_both_pairs(monkeypatch):
    """Одинаковые листы V1 (47 и дубль 52) в V2 разошлись (57 и 62): обе пары остаются, ни одна не затирается."""
    from types import SimpleNamespace

    def page(n):
        return SimpleNamespace(page=n, sheet_no="", designation="", kind="drawing", scan=False, stamp_source="",
                               title="", title_raw="")

    feats = {"v1": [page(47), page(52)], "v2": [page(57), page(62)]}
    monkeypatch.setattr(settings, "match_rules", lambda: V12)
    monkeypatch.setattr(lineage, "_duplicates", lambda pages, rules: {52: 47} if pages is feats["v1"] else {})
    monkeypatch.setattr(lineage, "_assign", lambda prev, cur, rules, rs=None: (
        {0: (0, 0.85, {}), 1: (1, 0.85, {})} if len(prev) == 2 else {}, np.zeros((len(prev), len(cur)))))
    res = lineage.build("obj", [{"version_id": "v1"}, {"version_id": "v2"}], [], features=feats)
    pairs = {(r["cells"]["v1"]["page"], r["cells"]["v2"]["page"]) for r in res["rows"]}
    assert pairs == {(47, 57), (52, 62)} and not res["unassigned"]


def test_twins_nearest_to_neighbours_and_sheet_numbers_decide():
    m = np.zeros((40, 40), np.uint8); m[5:10, 5:30] = 1
    a = P(63, COMMON, mask=m); a.rel = 0.50
    b55 = P(55, COMMON, mask=m.copy()); b55.rel = 0.50
    b59 = P(59, COMMON, mask=m.copy()); b59.rel = 0.60
    # Соседние пары 62→58 и 65→60: ближе та, что на месте между соседями, а не по доле длины файла.
    assert resolve._winner_content(a, [b55, b59], {t: 1.0 for t in COMMON}, expect=(58, 60)) == (1, "twin_nearest")
    # Между соседями оба — решает положение в файле среди них.
    assert resolve._winner_content(a, [b55, b59], {t: 1.0 for t in COMMON}, expect=(54, 60)) == (0, "twin_nearest")
    # Между соседями нет ни одного кандидата — двойник остаётся спорным, положение в файле не решает.
    assert resolve._winner_content(a, [b55, b59], {t: 1.0 for t in COMMON}, expect=(60, 64)) == (None, "twin")
    # Одинаковые схемы разных щитов (АК): разные номера листов — близость не решает, решает только тот же номер.
    for f, no in ((a, "13.4"), (b55, "23.4"), (b59, "25.4")):
        f.sheet_no = no
    assert resolve._winner_content(a, [b55, b59], {t: 1.0 for t in COMMON}, expect=(58, 60)) == (None, "twin_sheet")
    b55.sheet_no = "13.4"
    assert resolve._winner_content(a, [b55, b59], {t: 1.0 for t in COMMON}, expect=(58, 60)) == (0, "twin_sheet")


def test_expected_place_needs_neighbours_on_both_sides():
    prev = [P(n, COMMON) for n in (1, 2, 3, 4, 5, 62, 63, 64, 65)]
    cur = [P(n, COMMON) for n in (1, 2, 3, 4, 5, 6, 58, 59, 60)]
    # V4 → V5 ОВ1.2: перед стр. 3 вставлен лист (3→4, 4→5) — для обложки стр. 1 сосед только справа, места нет.
    # ОВ2.1: 62→58, стр. 64 без пары, 65→60 — место стр. 63 между 58 и 60.
    links = {2: (3, 1.0, {}), 3: (4, 1.0, {}), 4: (5, 1.0, {}), 5: (6, 1.0, {}), 8: (8, 1.0, {})}
    exp_a, _ = resolve._expected(prev, cur, links)
    assert 0 not in exp_a and exp_a[6] == (58, 60)


def test_identical_copies_take_new_pages_by_position(monkeypatch):
    """ВК2: в V9 раздел повторён (39 и копия 86); в V10 исправлена первая копия. Пары крест-накрест
    (39 → 86, 86 → 39) раздаются по положению: 39 → 39, 86 → 86."""
    from types import SimpleNamespace

    def page(n):
        return SimpleNamespace(page=n, sheet_no="", designation="", kind="drawing", scan=False, stamp_source="",
                               title="", title_raw="")

    feats = {"v1": [page(39), page(86)], "v2": [page(39), page(86)]}
    monkeypatch.setattr(settings, "match_rules", lambda: V12)
    monkeypatch.setattr(lineage, "_duplicates", lambda pages, rules: {86: 39} if pages is feats["v1"] else {})
    monkeypatch.setattr(lineage, "_assign", lambda prev, cur, rules, rs=None: (
        {0: (1, 0.95, {}), 1: (0, 0.9, {})} if len(prev) == 2 else {}, np.zeros((len(prev), len(cur)))))
    res = lineage.build("obj", [{"version_id": "v1"}, {"version_id": "v2"}], [], features=feats)
    assert {(r["cells"]["v1"]["page"], r["cells"]["v2"]["page"]) for r in res["rows"]} == {(39, 39), (86, 86)}


def test_v13_is_v12_without_margin(monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.3")
    assert settings.match_rules() == V12 - {"margin"}


def test_table_group_pairs_in_order_between_anchors():
    """Группа листов таблицы (решение 18.10): строки СО разошлись по листам — неразрешённые листы связываются по
    порядку между уже принятыми парами группы; лишний лист на конце отрезка остаётся без пары."""
    floor = np.ones((10, 10), bool)
    # A: 0 1 2 [3→4 опора] 4 5;  B: 0 1 2 3 [4] 5 6 7
    links = {3: (4, 0.9, {})}
    got = resolve._group_pairs([0, 1, 2, 4, 5], [0, 1, 2, 3, 5, 6, 7], [0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4, 5, 6, 7],
                               links, floor)
    assert got == {0: 0, 1: 1, 2: 2, 4: 5, 5: 6}            # B стр. 3 и 7 — без пары (добавлены)
    # Пара поперёк порядка (1→5: раздел переставлен) опорой не служит и группу не ломает: опоры — 0→0, 3→2, 4→4.
    crossed = {0: (0, 0.9, {}), 1: (5, 0.9, {}), 3: (2, 0.9, {}), 4: (4, 0.9, {})}
    assert resolve._group_pairs([2], [1, 3], [0, 1, 2, 3, 4], [0, 1, 2, 3, 4, 5], crossed, floor) == {2: 1}
    floor[0, 0] = False                                      # пара ниже порога кандидата не ставится
    assert 0 not in resolve._group_pairs([0, 1], [0, 1], [0, 1], [0, 1], {}, floor)


def test_group_is_part_of_v12_and_run_confidence():
    from backend.app.services.version_comparison import jobs
    assert "group" in V12
    row = {"cells": {"a": {"state": "matched", "confidence": "origin"},
                     "b": {"state": "matched", "confidence": "group"}}}
    assert jobs._pair_confidence(row, ["a", "b"], "a", "b") == "group"


def test_stamp_field_labels_are_not_sheet_titles():
    from types import SimpleNamespace
    f = SimpleNamespace(page=58, sheet_no="12", designation="133/23-ГК-ЭМ2.СО", kind="spec", title_raw="Формат")
    assert lineage._shown_title(f) == "" and lineage._display_title(f) == "СО л.12 · Спецификация"
    f.title_raw = "Спецификация оборудования"
    assert lineage._display_title(f) == "СО л.12 · Спецификация оборудования"


def test_group_order_follows_sheet_numbers_when_pages_are_shuffled():
    """ЭМ2 V8: в PDF листы СО не по порядку (л.33 на стр. 75, л.27 на стр. 80) — порядок группы по номеру листа."""
    from types import SimpleNamespace as NS
    prev = [NS(page=75, sheet_no="33"), NS(page=76, sheet_no="30"), NS(page=80, sheet_no="27")]
    cur = [NS(page=73, sheet_no="27"), NS(page=76, sheet_no="30"), NS(page=79, sheet_no="33")]
    oa, ob = resolve._group_order(prev, [0, 1, 2]), resolve._group_order(cur, [0, 1, 2])
    assert oa == {2: 0, 1: 1, 0: 2} and ob == {0: 0, 1: 1, 2: 2}
    got = resolve._group_pairs([0, 1, 2], [0, 1, 2], [0, 1, 2], [0, 1, 2], {}, np.ones((3, 3), bool), oa, ob)
    assert got == {2: 0, 1: 1, 0: 2}                     # л.27→л.27, л.30→л.30, л.33→л.33
    cur[1].sheet_no = ""                                 # номер есть не у всех — порядок страниц
    assert resolve._group_order(cur, [0, 1, 2]) == {0: 0, 1: 1, 2: 2}
