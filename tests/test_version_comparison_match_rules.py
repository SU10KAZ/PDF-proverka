"""Правила сопоставления листов этапа 1 (docs/version_comparison_matching_upgrade_plan.md §13.1).

«v1» — прежнее поведение и подпись без тега правил (по умолчанию теперь v1.2); «v1.1» включает:
запрет пары только по порядку, «неизвестно» вместо штрафа за отсутствие текста, «unknown» вместо «title»,
штамп по скрытому тексту SHX, отрыв от конкурентов, CHAIN_ONLY и устаревание элементов прогона.
"""
from __future__ import annotations

import hashlib

import numpy as np

from backend.app.services.version_comparison import jobs, lineage, settings
from tests import version_comparison_fixtures as fx
from tests.test_version_comparison_api import OBJ as API_OBJ, _setup_project, _wait, client  # noqa: F401
from tests.test_version_comparison_lineage import OBJ, TITLES, WORDS, vc_root, version  # noqa: F401

V11 = frozenset(settings.MATCH_RULES_V11)
FRAME = [56.7, 14.2, 1176.4, 827.7]


def _rec(page=1, kind="title", w=1190.0, h=842.0, frame=None, stamp=None):
    return {"page": page, "width": w, "height": h, "kind": kind, "frame": frame, "zones": [],
            "stamp": stamp or {"recognized": False, "form": "none", "zone": None, "designation": "",
                               "sheet_no": "", "title": "", "izm_max": None}}


def _words(n, prefix="слово", source="text", y=200.0):
    return [[100.0 + 30 * (i % 20), y + 12 * (i // 20), 125.0 + 30 * (i % 20), y + 8 + 12 * (i // 20),
             f"{prefix}{i}", source] for i in range(n)]


def _mask(shape=(100, 140), seed=1):
    rng = np.random.default_rng(seed)
    m = np.zeros(shape, np.uint8)
    for _ in range(40):
        y, x = int(rng.integers(5, shape[0] - 10)), int(rng.integers(5, shape[1] - 10))
        m[y:y + 6, x:x + 9] = 1
    return m


def test_v1_rules_and_signature(monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1")
    assert settings.match_rules() == frozenset() and settings.match_rules_tag() == ""
    vs = [{"version_id": "A", "sha256": "a" * 64}, {"version_id": "B", "sha256": "b" * 64}]
    raw = "A:" + "a" * 64 + "|B:" + "b" * 64 + "|" + settings.PROFILE_VERSION + "|" + lineage.LINEAGE_REVISION
    assert lineage.signature(vs) == hashlib.sha256(raw.encode()).hexdigest()   # прежние правила: без тега правил
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1")
    assert settings.match_rules() == V11 and lineage.signature(vs) != hashlib.sha256(raw.encode()).hexdigest()
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "order, text ,bogus")
    assert settings.match_rules() == frozenset({"order", "text"})
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1+distinct")
    assert settings.match_rules() == V11 | {"distinct"}
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1+peel+distinct")
    assert settings.match_rules() == V11 | {"distinct", "peel"}


def test_order_alone_never_makes_a_pair():
    # Разные форматы (маски несравнимы), текста и штампа нет: прежде оценка = порядок (V3 35 → V4 19).
    a = lineage.PageFeatures("A", "a", _rec(), [], _mask((100, 140)), V11)
    b = lineage.PageFeatures("B", "b", _rec(w=1684, h=1191), [], _mask((120, 170)), V11)
    old, _ = lineage.score(lineage.PageFeatures("A", "a", _rec(), [], _mask((100, 140))),
                           lineage.PageFeatures("B", "b", _rec(w=1684, h=1191), [], _mask((120, 170))), 0.5, 0.5)
    assert old >= settings.MATCH_HIGH
    value, parts = lineage.score(a, b, 0.5, 0.5, V11)
    assert value == 0.0 and parts["no_content_evidence"] == 1.0


def test_missing_text_on_one_side_is_unknown_not_mismatch():
    m = _mask()
    a = lineage.PageFeatures("A", "a", _rec(), [], m, V11)                       # растр без текста
    b = lineage.PageFeatures("B", "b", _rec(), _words(60, source="shx"), m, V11)  # тот же рисунок + текст SHX
    old, _ = lineage.score(lineage.PageFeatures("A", "a", _rec(), [], m),
                           lineage.PageFeatures("B", "b", _rec(), _words(60, source="shx"), m), 0.5, 0.5)
    assert old < settings.MATCH_MEDIUM                                            # прежде: текст 0 → пары нет
    value, parts = lineage.score(a, b, 0.5, 0.5, V11)
    assert "text" not in parts and parts["text_unknown"] == 1.0 and value > 0.95


def test_unrecognised_stamp_is_unknown_not_title():
    framed = lineage.PageFeatures("A", "a", _rec(frame=FRAME), _words(3, "в производство"), None, V11)
    assert framed.kind == "unknown"
    cover = lineage.PageFeatures("A", "a", _rec(), [[100, 100, 300, 120, "РАБОЧАЯ", "text"],
                                                   [305, 100, 480, 120, "ДОКУМЕНТАЦИЯ", "text"]], None, V11)
    assert cover.kind == "title"
    assert lineage.PageFeatures("A", "a", _rec(frame=FRAME), [], None).kind == "title"   # v1 — как прежде
    # Тип «unknown» не штрафует пару.
    m = _mask()
    _, parts = lineage.score(framed, lineage.PageFeatures("B", "b", _rec(kind="drawing"), [], m, V11), 0.5, 0.5, V11)
    assert "kind_mismatch" not in parts


def _stamp_words(source: str) -> list[list]:
    """Строка подписей ГОСТ и название в правом нижнем углу листа А3 (1190×842)."""
    y = 842 - 40
    labels = ["Изм.", "Кол.уч.", "Лист", "№док.", "Подп.", "Дата"]
    out = [[700 + 30 * i, y, 725 + 30 * i, y + 6, t, source] for i, t in enumerate(labels)]
    out += [[900, 842 - 30, 960, 842 - 24, "133/23-ГК-ЭО3", source],
            [880, 842 - 18, 920, 842 - 12, "План", source], [925, 842 - 18, 965, 842 - 12, "кровли", source]]
    return out


def test_stamp_from_hidden_shx_text_without_duplicates():
    words = _stamp_words("shx") + _words(30, source="shx")
    rec = _rec(frame=[56.7, 14.2, 1176.4, 827.7])
    f = lineage.PageFeatures("B", "b", rec, words, None, V11)
    assert f.stamp_source == "shx" and f.designation == "133/23-ГК-ЭО3"
    assert lineage.PageFeatures("B", "b", rec, words, None).designation == ""            # v1 — только текстовый слой
    # Слово SHX поверх того же слова текстового слоя не дублируется.
    w = [[10, 10, 50, 20, "щит", "text"], [11, 10, 50, 20, "щит", "shx"], [60, 10, 90, 20, "ВРУ", "shx"]]
    assert [x[4] for x in lineage._merge_shx(w)] == ["щит", "ВРУ"]


def test_equal_candidates_stay_for_review_ordered_by_position(vc_root, monkeypatch):
    # В B два листа с тем же содержанием и названием, номера не совпадают с A: выбрать один произвольно нельзя.
    a = version(vc_root, "A", [fx.sheet("1", TITLES[1], unique=1, marker=WORDS[1]),
                               fx.sheet("2", TITLES[2], unique=2, marker=WORDS[2])])
    b = version(vc_root, "B", [fx.sheet("7", TITLES[1], unique=1, marker=WORDS[1]),
                               fx.sheet("8", TITLES[1], unique=1, marker=WORDS[1]),
                               fx.sheet("2", TITLES[2], unique=2, marker=WORDS[2])])
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1")
    res = lineage.build(OBJ, [a, b])
    row = next(r for r in res["rows"] if r["cells"]["A"].get("page") == 1)
    cell = row["cells"]["B"]
    assert cell["state"] == "unmatched" and [c["page"] for c in cell["candidates"]][:2] == [1, 2]
    assert next(r for r in res["rows"] if r["cells"]["A"].get("page") == 2)["cells"]["B"]["page"] == 3
    assert res["match_rules"].startswith("rules=")


def test_chain_only_and_stale_items(client, tmp_path, monkeypatch):  # noqa: F811
    monkeypatch.setenv("VERSION_COMPARISON_MATCH_RULES", "v1.1")
    pid, vids, _ = _setup_project(client, tmp_path)
    base = f"/api/version-comparison/objects/{API_OBJ}"
    m = _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")

    def run(a, b):
        r = client.post(f"{base}/projects/{pid}/runs", json={"a_version": a, "b_version": b, "scope": "all"}).json()
        return _wait(lambda: client.get(f"{base}/runs/{r['run_id']}").json(), lambda d: d["state"] == "done")
    # Пара через версию — только по цепочке: не сравнивается, пока инженер не подтвердит её сам.
    far = run(vids[0], vids[2])
    chain = [it for it in far["items"] if it["kind"] == "chain"]
    assert chain and not any(it["kind"] == "compare" for it in far["items"]) and far["progress"]["total"] == 0
    assert all(it["reasons"] == ["CHAIN_ONLY"] and it["result_status"] == "unmatched" and it["a_page"] and it["b_page"]
               for it in chain)
    from backend.app.services.version_comparison_ai import jobs as ai_jobs
    assert not any(it["kind"] == "chain" for it in ai_jobs._eligible(far))       # в ИИ-описание не уходит
    pair = {"a_page": chain[0]["a_page"], "b_page": chain[0]["b_page"]}
    r = client.post(f"{base}/projects/{pid}/runs", json={"a_version": vids[0], "b_version": vids[2], "scope": "manual",
                                                         "sheet_pairs": [pair]}).json()
    confirmed = _wait(lambda: client.get(f"{base}/runs/{r['run_id']}").json(), lambda d: d["state"] == "done")
    item = confirmed["items"][0]
    assert item["kind"] == "compare" and item["status"] == "done" and "CHAIN_ONLY" not in item["reasons"]
    near = run(vids[0], vids[1])
    assert all(it.get("link_fingerprint") and not it.get("pair_stale") for it in near["items"] if it["kind"] == "compare")
    assert near["match_rules"].startswith("rules=") and near["matching_signature"]
    # Подтверждение той же пары вручную: отпечаток тот же, устаревания нет.
    row1 = next(r for r in m["rows"] if r["cells"][vids[0]].get("page") == 1)
    client.post(f"{base}/projects/{pid}/links", json={"op": "move", "page": [vids[1], 1], "to_row_anchor": row1["anchor"]})
    _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")
    again = client.get(f"{base}/runs/{near['run_id']}").json()
    assert not any(it.get("pair_stale") for it in again["items"])
    # Лист 2 версии (2) отвязан: устаревает только элемент этой пары в прогоне (1) → (2).
    client.post(f"{base}/projects/{pid}/links", json={"op": "detach", "page": [vids[1], 2]})
    _wait(lambda: client.get(f"{base}/projects/{pid}/matrix").json(), lambda d: d["state"] == "ready")
    again = client.get(f"{base}/runs/{near['run_id']}").json()
    stale = [it["a_page"] for it in again["items"] if it.get("pair_stale")]
    assert stale == [2] and "без пары" in next(it for it in again["items"] if it.get("pair_stale"))["pair_stale"]
    assert jobs.link_fingerprint("x", 1, "y", 2) == jobs.link_fingerprint("x", 1, "y", 2) != jobs.link_fingerprint("x", 1, "y", 3)


def test_distinct_marks_separate_template_sheets():
    """Однотипные листы (общий шаблон) различаются редкими метками: «кв.45» ≠ «кв.16»."""
    abc = "абвгдежзиклмнопрстуфхцчшщэюя"
    template = [[100 + 20 * i, 300, 115 + 20 * i, 308, f"шаблон{abc[i % 28]}{abc[i // 28]}", "text"] for i in range(40)]
    def page(n, marks):
        return lineage.PageFeatures("X", "x", _rec(page=n), template + [[700, 400 + 12 * k, 760, 408 + 12 * k, m, "text"]
                                                                         for k, m in enumerate(marks)], None,
                                    V11 | {"distinct"})
    a = page(1, ["кв.45", "кв.46"])
    b_right, b_other = page(1, ["кв.45", "кв.46"]), page(2, ["кв.16", "кв.17"])
    for f in (a, b_right, b_other):
        f.distinct = set(f.marks)                       # в load_features — с учётом редкости по версии
    s_right, parts = lineage.score(a, b_right, 0.5, 0.5, V11 | {"distinct"})
    s_other, _ = lineage.score(a, b_other, 0.5, 0.5, V11 | {"distinct"})
    assert parts["distinct"] == 1.0 and s_right - s_other >= settings.MATCH_MARGIN
    plain_r, _ = lineage.score(a, b_right, 0.5, 0.5, V11)
    plain_o, _ = lineage.score(a, b_other, 0.5, 0.5, V11)
    assert plain_r - plain_o < s_right - s_other         # без меток шаблон почти неразличим


def test_peel_ignores_competitor_already_paired(monkeypatch):
    """peel: конкурент, уже уверенно принятый в другую пару, не мешает отрыву; взаимно равные пары остаются спорными."""
    class P:
        def __init__(self, page):
            self.page = page

    def run(table, rules):
        monkeypatch.setattr(lineage, "score", lambda a, b, *_: (table[(a.page, b.page)], {}))
        links, _ = lineage._assign([P(1), P(2)], [P(1), P(2)], rules)
        return {i + 1: j + 1 for i, (j, _, _) in links.items()}

    # A1→B1 ясна; у A2→B2 единственный близкий конкурент — B1, уже занятый A1.
    clear = {(1, 1): 0.95, (1, 2): 0.60, (2, 1): 0.85, (2, 2): 0.84}
    assert run(clear, V11) == {1: 1}                       # без peel A2 уходит в «Нужна проверка»
    assert run(clear, V11 | {"peel"}) == {1: 1, 2: 2}
    # Взаимно равные пары: ни одна не отличима — peel ничего не принимает.
    tie = {(1, 1): 0.90, (1, 2): 0.88, (2, 1): 0.88, (2, 2): 0.90}
    assert run(tie, V11 | {"peel"}) == {}
