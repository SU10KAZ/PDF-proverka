"""Тесты многостраничных листов (multipart/multisheet) в штамп-матчере.

Один логический лист может в одной версии занимать одну страницу, а в другой —
несколько (начало/продолжение/конец). Matcher группирует их по
`sheet_group_key` и сопоставляет по ролям, не дублируя страницы.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import stamp_matching as sm


def _md(pages: list[tuple[int, object, object]]) -> str:
    out: list[str] = []
    for pn, sno, snm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        if sno is not None:
            out.append(f"**Лист:** {sno}")
        if snm is not None:
            out.append(f"**Наименование листа:** {snm}")
        out.append(f"текст страницы {pn}")
        out.append("")
    return "\n".join(out)


def _idx(pages):
    return sm.build_sheet_index(_md(pages))


def _align(res):
    """(left_page, right_page) в порядке слотов — для проверки карты."""
    return [(it.get("left_page"), it.get("right_page")) for it in res["suggested_items"]]


# ═══ extract_multipart ════════════════════════════════════════════════════

def test_extract_multipart_group_key():
    for name, role in [("Чертеж 1", None), ("Чертеж 1 (начало)", "start"),
                       ("Чертеж 1 (продолжение)", "continuation"),
                       ("Чертеж 1 (конец)", "end")]:
        gk, r, _ = sm.extract_multipart(sm.normalize_sheet_name(name))
        assert gk == "чертеж 1", (name, gk)
        assert r == role, (name, r)


def test_extract_multipart_part_numbers():
    gk, role, idx = sm.extract_multipart(sm.normalize_sheet_name("Схема ВРУ часть 2"))
    assert gk == "схема вру"
    assert idx == 2 and role == "continuation"
    gk2, role2, idx2 = sm.extract_multipart(sm.normalize_sheet_name("Схема ВРУ часть 1"))
    assert idx2 == 1 and role2 == "start"


def test_multipart_does_not_break_plain_name():
    # «Текстовая часть» без числа — не multipart-маркер, имя не ломается.
    gk, role, idx = sm.extract_multipart(sm.normalize_sheet_name("Текстовая часть"))
    assert gk == "текстовая часть"
    assert role is None and idx is None


# ═══ Case 1 — одна страница слева против трёх справа ══════════════════════

def test_one_left_three_right():
    left = _idx([(1, "1", "Чертеж 1")])
    right = _idx([(10, "10", "Чертеж 1 (начало)"),
                  (11, "11", "Чертеж 1 (продолжение)"),
                  (12, "12", "Чертеж 1 (конец)")])
    res = sm.match_sheet_indexes(left, right)
    assert _align(res) == [(1, 10), (None, 11), (None, 12)]
    anchor = next(it for it in res["suggested_items"] if it["match"])
    assert anchor["match_type"] == "exact_multipart_group"
    conts = [it for it in res["suggested_items"]
             if it["match_type"] == "multipart_continuation"]
    assert len(conts) == 2


# ═══ Case 2 — три страницы слева против одной справа ══════════════════════

def test_three_left_one_right():
    left = _idx([(1, "1", "Чертеж 1 (начало)"),
                 (2, "2", "Чертеж 1 (продолжение)"),
                 (3, "3", "Чертеж 1 (конец)")])
    right = _idx([(10, "10", "Чертеж 1")])
    res = sm.match_sheet_indexes(left, right)
    assert _align(res) == [(1, 10), (2, None), (3, None)]


# ═══ Case 3 — три против трёх ═════════════════════════════════════════════

def test_three_left_three_right():
    left = _idx([(1, "1", "Чертеж 1 (начало)"),
                 (2, "2", "Чертеж 1 (продолжение)"),
                 (3, "3", "Чертеж 1 (конец)")])
    right = _idx([(10, "10", "Чертеж 1 (начало)"),
                  (11, "11", "Чертеж 1 (продолжение)"),
                  (12, "12", "Чертеж 1 (конец)")])
    res = sm.match_sheet_indexes(left, right)
    assert _align(res) == [(1, 10), (2, 11), (3, 12)]


# ═══ Case 4 — разное количество частей (роль-aware) ══════════════════════

def test_asymmetric_parts_role_aware():
    left = _idx([(1, "1", "Чертеж 1 (начало)"),
                 (2, "2", "Чертеж 1 (конец)")])
    right = _idx([(10, "10", "Чертеж 1 (начало)"),
                  (11, "11", "Чертеж 1 (продолжение)"),
                  (12, "12", "Чертеж 1 (конец)")])
    res = sm.match_sheet_indexes(left, right)
    # start↔start, end↔end (role-aware); лишнее продолжение 11 → одностороннее.
    # Порядок слотов: одностороннее 11 ставится рядом с первой сматченной строкой
    # (как в case 1), поэтому карта = [(1,10),(None,11),(2,12)] — это валидная
    # эквивалентная раскладка (важны пары и отсутствие дублей, не позиция слота).
    pairs = {(lp, rp) for lp, rp in _align(res) if lp is not None and rp is not None}
    assert pairs == {(1, 10), (2, 12)}                  # роль-aware пары
    assert (None, 11) in _align(res)                    # продолжение одностороннее
    left_pages = [lp for lp, _ in _align(res) if lp is not None]
    assert sorted(left_pages) == [1, 2]                 # без дублей


# ═══ Случай: одна страница НЕ дублируется ════════════════════════════════

def test_single_page_not_duplicated():
    left = _idx([(1, "1", "Чертеж 1")])
    right = _idx([(10, "10", "Чертеж 1 (начало)"),
                  (11, "11", "Чертеж 1 (продолжение)"),
                  (12, "12", "Чертеж 1 (конец)")])
    res = sm.match_sheet_indexes(left, right)
    left_pages = [lp for lp, _ in _align(res) if lp is not None]
    assert left_pages == [1]  # страница 1 ровно один раз


# ═══ Case 5 — hard-gates внутри multipart ════════════════════════════════

def test_multipart_floor_conflict_not_matched():
    # Этажи различаются → разные sheet_group_key → не группируются.
    left = _idx([(1, "1", "Чертеж план 1 этаж (начало)")])
    right = _idx([(10, "10", "Чертеж план 2 этаж (конец)")])
    res = sm.match_sheet_indexes(left, right)
    assert res["matched_count"] == 0


def test_multipart_hard_gate_called_on_subpair():
    """Прямой тест hard-gate внутри группы: одинаковый group_key, но
    конфликтующий этаж (искусственно) → пара не создаётся."""
    L = _idx([(1, "1", "Чертеж 1 (начало)")])
    R = _idx([(10, "10", "Чертеж 1 (конец)")])
    # group_key совпадает («чертеж 1»); навяжем конфликт этажей в признаках.
    lf = sm.extract_sheet_features(L[0])
    rf = sm.extract_sheet_features(R[0])
    lf.floor_tokens = {"этаж:1"}
    rf.floor_tokens = {"этаж:2"}
    assert lf.sheet_group_key == rf.sheet_group_key == "чертеж 1"
    assert sm.get_hard_conflict(lf, rf) == "floor_conflict"


# ═══ Case 6 — LLM не создаёт multipart-группу сам ════════════════════════

def test_llm_does_not_create_multipart_group():
    # group_key РАЗНЫЕ (разные чертежи) → multipart-пасс не сработает.
    # Даже если LLM вернёт пару, это обычный llm_semantic, не multipart.
    left = _idx([(1, "1", "Чертеж 5 экспликация помещений раздел")])
    right = _idx([(10, "10", "Чертеж 9 совсем другой лист продолжение блок")])

    def fn(rem_left, rem_right, tasks=None):
        # пытается навязать пару — но это не multipart-группа
        return [(1, 10, 0.95, "llm_semantic")]

    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)
    mp = [it for it in res["suggested_items"]
          if it.get("match_type") in ("multipart_group", "exact_multipart_group")]
    assert mp == []  # LLM не создал multipart-группу
