"""Этап 2 сопоставления листов: разрешение спорных пар дополнительными проверками (правила resolve, package).

После основного назначения (оценка пары + отрыв от конкурентов) часть страниц остаётся «Нужна проверка»:
близкие кандидаты (однотипные листы, опросные листы поставщика, повторяющиеся разделы спецификаций).
Здесь пара принимается автоматически только по положительному доказательству, без снижения порогов:

* отличительные слова — победитель должен перевесить КАЖДОГО конкурента по словам, которые есть ровно у
  одного из двух (симметрическая разность), с учётом только слов страницы A. Вес слова — редкость по
  страницам перехода × вид: марка/позиция (буквы и цифры: «в10», «к11.2.1», «ЩР-3») весомее слова,
  слово весомее числа/размера/даты (значения меняются между версиями);
* таблицы (спецификации, журналы) — дополнительно покрытие марок в обе стороны: если позиции листа
  разошлись по нескольким листам, это связь блока (перенос строк), пара 1:1 не назначается;
* страницы без различающего текста (двойники по тексту, листы без текстового слоя) — область изображения,
  где различаются кандидаты: какая из них совпадает с A;
* встречная проверка: со стороны B то же правило должно указать на ту же страницу A; кандидаты — все
  страницы с оценкой не ниже порога кандидата, в том числе уже связанные (если содержание указывает на
  уже связанный лист — пара не принимается);
* оценка пары не ниже MATCH_MEDIUM (порог пары не снижается);
* комплект (package): обозначение A и B не противоречит соответствию обозначений, установленному
  уверенными парами этого перехода.

Порядок страниц не используется. Неразрешённые страницы получают причину (``reasons``).
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from typing import Any

import cv2
import numpy as np

from . import settings
from .settings import MATCH_CANDIDATE_FLOOR, MATCH_MEDIUM

_DIM = re.compile(r"^\d+([.,]\d+)?([xх×*]\d+([.,]\d+)?)+$")
# Обозначение из цифр с двумя и более разделителями («1.13.3», «0986-04-24_2», «3415/502/251») — код, а не значение;
# даты («2025-10-13», «13.10.2025») — не коды.
_CODE = re.compile(r"^\d+([./\-_]\d+){2,}$")
_DATE = re.compile(r"^((19|20)\d\d[-.]\d\d[-.]\d\d|\d\d[-.]\d\d[-.](19|20)?\d\d)$")
TABLE_KINDS = ("spec", "text")

REASON_LABELS = {
    "twin": "одинаковые страницы-двойники: текст и изображение не различают, положение в файле тоже",
    "twin_sheet": "одинаковые по содержанию листы с разными номерами в штампе: номер листа A не совпал ни с одним",
    "block": "перенос строк таблицы между листами — связь блока, не пара 1:1 (позиции сравниваются по спецификации)",
    "weak": "отличительных признаков недостаточно",
    "taken": "содержание указывает на уже связанный лист",
    "not_mutual": "встречная проверка указывает на другой лист",
    "below_pair": "сходство ниже порога пары",
    "package": "разные комплекты (обозначения противоречат установленным парам)",
    "no_evidence": "нет текста, изображение не различает",
    "title_conflict": "содержание указывает на лист с другим названием, а лист с тем же названием есть",
    "title_only": "совпадает только название листа, содержание не подтверждает",
    "page_no_conflict": "номер страницы отчёта не однозначен",
    "permit": "листы разных разрешений на внесение изменений (или разрешение и обычный лист)",
}


def token_class(t: str) -> float:
    has_d = any(ch.isdigit() for ch in t)
    has_a = any(ch.isalpha() for ch in t)
    if has_d and has_a and not _DIM.match(t):
        return settings.RESOLVE_W_MARK
    if has_a and not has_d:
        return settings.RESOLVE_W_WORD if len(t) >= 3 else 0.0
    if _CODE.match(t) and not _DATE.match(t):
        return settings.RESOLVE_W_MARK
    return settings.RESOLVE_W_NUMBER


def weights(pages: list) -> dict[str, float]:
    df = Counter(t for f in pages for t in set(f.tokens))
    n = len(pages)
    return {t: math.log(n / d) * token_class(t) for t, d in df.items()}


def _contest(sa: set, s1: set, sk: set, w: dict[str, float]) -> tuple[float, float, int, int]:
    """Слова A, которые есть ровно у одного из двух кандидатов: вес и число «за» первого и «за» второго."""
    d = (s1 ^ sk) & sa
    d1 = {t for t in d & s1 if w.get(t, 0.0) > 0}
    dk = {t for t in d - s1 if w.get(t, 0.0) > 0}
    return sum(w[t] for t in d1), sum(w[t] for t in dk), len(d1), len(dk)


def _decisive(pro: float, con: float, npro: int, ncon: int) -> bool:
    """Перевес доказан: доля веса за победителя и абсолютный перевес; либо, когда долю сбивает один-два
    редких слова против, — подавляющее большинство отличительных слов за победителя."""
    if pro - con < settings.RESOLVE_MIN_W:
        return False
    frac = pro / (pro + con)
    if frac >= settings.RESOLVE_FRAC:
        return True
    return (frac >= settings.RESOLVE_FRAC_ROBUST and ncon <= settings.RESOLVE_MAX_AGAINST
            and npro / (npro + ncon) >= settings.RESOLVE_COUNT_FRAC)


def _full(x) -> np.ndarray | None:
    f = getattr(x, "full_mask", None)
    return f() if callable(f) else x.mask


def _image_contest(a, c1, ck) -> tuple[int, int] | None:
    """Область, где различаются два кандидата (ink только у одного, без соседства общих линий), по маске
    36 dpi: сколько линий A в ней совпадает с линиями первого и второго. None — масок нет или форматы разные."""
    ms = [_full(x) for x in (a, c1, ck)]
    if any(m is None for m in ms) or not (ms[0].shape == ms[1].shape == ms[2].shape):
        return None
    k = np.ones((3, 3), np.uint8)
    m1, mk = ms[1], ms[2]
    region = (m1 ^ mk) & ~(cv2.dilate(m1, k) & cv2.dilate(mk, k))
    area = int(region.sum())
    if area == 0:
        return 0, 0
    # Только то, что на A нарисовано: отсутствие линии — не довод (в новой версии могли добавить таблицу
    # изменений или подписи), а линия A в области отличий говорит за того кандидата, у кого она тоже есть.
    near_a = cv2.dilate(ms[0], k)
    return int((region & m1 & near_a).sum()), int((region & mk & near_a).sum())


def _title_key(f) -> str:
    """Название листа из штампа (текст или SHX), если оно достаточно содержательное."""
    return f.title if len(f.title) >= settings.RESOLVE_TITLE_MIN_LEN else ""


def footer_counts(pages: list) -> Counter:
    """(шаблон, номер страницы в колонтитуле) → число листов версии: номер решает, только если он единственный."""
    return Counter((f.template, f.footer_no) for f in pages if getattr(f, "footer_no", ""))


def _by_page_no(x, cands: list, idxs: list[int], footers: tuple[Counter, Counter]) -> int | None:
    """Кандидат с тем же номером страницы встроенного отчёта: номер единственный среди листов того же шаблона
    в обеих версиях, шаблоны листов совпадают (слова без цифр)."""
    n = getattr(x, "footer_no", "")
    if not n or footers[0].get((x.template, n), 0) != 1:
        return None
    hit = [k for k in idxs if getattr(cands[k], "footer_no", "") == n]
    if len(hit) != 1:
        return None
    c = cands[hit[0]]
    if footers[1].get((c.template, n), 0) != 1:
        return None
    union = len(x.template | c.template)
    if not union or len(x.template & c.template) / union < settings.RESOLVE_TEMPLATE_SIM:
        return None
    return hit[0]


def _winner(x, cands: list, w: dict[str, float], titles: tuple[Counter, Counter] | None = None,
            footers: tuple[Counter, Counter] | None = None,
            expect: tuple[int, int] | None = None) -> tuple[int | None, str]:
    """Индекс кандидата, перевешивающего каждого другого, и вид доказательства; или (None, причина).
    titles — частоты названий листов в версии x и в версии кандидатов (для правила названия);
    expect — промежуток страниц кандидатов между соседними принятыми парами (для двойников)."""
    if not cands:
        return None, "no_evidence"
    tx = _title_key(x)
    same = [k for k, c in enumerate(cands) if tx and _title_key(c) == tx]
    if titles is not None and tx and titles[0][tx] == 1 and titles[1][tx] == 1 and len(same) == 1:
        # Надёжно прочитанное название листа, единственное в обеих версиях, — решающий довод.
        return same[0], "title"
    k, how = _winner_content(x, cands, w, footers, expect)
    if k is not None and same and k not in same and _title_key(cands[k]):
        return None, "title_conflict"   # содержание указывает на лист с другим названием, а лист с тем же названием есть
    return k, how


def _winner_content(x, cands: list, w: dict[str, float],
                    footers: tuple[Counter, Counter] | None = None,
                    expect: tuple[int, int] | None = None) -> tuple[int | None, str]:
    if len(cands) == 1:
        return 0, "single"
    sx = set(x.tokens)
    sets = [set(c.tokens) for c in cands]
    text_ok = x.has_text and all(c.has_text for c in cands)
    if text_ok:
        best = max(range(len(cands)), key=lambda k: sum(w.get(t, 0.0) for t in sx & sets[k]))
        twin, thin = [], []
        for k in range(len(cands)):
            if k == best:
                continue
            pro, con, npro, ncon = _contest(sx, sets[best], sets[k], w)
            if pro + con <= 0:
                twin.append(k)  # по тексту не различаются — решит номер страницы отчёта или изображение
                continue
            if _decisive(pro, con, npro, ncon):
                continue
            if pro + con < settings.RESOLVE_MIN_W:
                thin.append(k)  # различаются только числами и датами — текст не решает ни в чью пользу
                continue
            return None, "weak"
        if not twin and not thin:
            return best, "text"
        if footers is not None:
            k = _by_page_no(x, cands, [best] + twin + thin, footers)
            if k is not None:
                return k, "page_no"
        if thin:
            return None, "weak"
        candidates = [k for k in range(len(cands)) if not (sets[k] ^ sets[best]) & sx] + [best]
        candidates = sorted(set(candidates))
    else:
        candidates = list(range(len(cands)))
    # Изображение: среди кандидатов, неразличимых по тексту (или без текста), лучший по общей маске
    # должен совпасть с A в области отличий против каждого другого.
    best_img = max(candidates, key=lambda k: _mask_score(x, cands[k]))
    for k in candidates:
        if k == best_img:
            continue
        r = _image_contest(x, cands[best_img], cands[k])
        if r is None:
            return None, "no_evidence"
        pro, con = r
        if pro + con < settings.RESOLVE_MIN_IMAGE_AREA or pro / (pro + con) < settings.RESOLVE_IMAGE_FRAC:
            if not text_ok:
                return None, "no_evidence"
            # Двойники: ни текст, ни изображение не различают — сравнение с любым даст одно и то же; берётся
            # ближайший по положению в файле (решение 17.10). Без текста (планы без слов) правило не применяется.
            group = [cands[q] for q in candidates]
            nums = {_sheet(c) for c in group} - {""}
            if len(nums) > 1:
                # Штамп различает двойников (АК: схемы щитов ЩА-П1 л.13.4, ЩА-П2тех л.23.4, ЩА-П1/В1 л.25.4 —
                # разные щиты): решает только тот же номер листа, близость по положению — нет.
                same_no = [q for q, c in zip(candidates, group) if _sheet(x) and _sheet(c) == _sheet(x)]
                return (same_no[0], "twin_sheet") if len(same_no) == 1 else (None, "twin_sheet")
            near = _nearest(x, group, expect)
            return (candidates[near], "twin_nearest") if near is not None else (None, "twin")
    return best_img, "image"


def _sheet(f) -> str:
    return str(getattr(f, "sheet_no", "") or "").strip()


def _nearest(x, cands: list, expect: tuple[int, int] | None = None) -> int | None:
    """Ближайший по относительному положению в файле среди кандидатов на месте между соседними принятыми
    парами (нет ни одного в промежутке — None); без соседей с обеих сторон — среди всех; при равенстве — None."""
    pool = list(enumerate(cands))
    if expect is not None:
        pool = [(q, c) for q, c in pool if expect[0] < c.page < expect[1]]
        if len(pool) == 1:
            return pool[0][0]
    rx = getattr(x, "rel", None)
    if not pool or rx is None or any(getattr(c, "rel", None) is None for _q, c in pool):
        return None
    d = sorted((abs(c.rel - rx), q) for q, c in pool)
    if len(d) > 1 and d[1][0] - d[0][0] < 1e-9:
        return None
    return d[0][1]


def same_drawing(a, b) -> bool:
    """Графика одинакова и на маске 36 dpi: линий, которые есть только у одной страницы (не рядом с общими),
    меньше RESOLVE_MIN_IMAGE_AREA точек. Подпись и печать титула против таблицы изменений обложки — не дубль."""
    ma, mb = _full(a), _full(b)
    if ma is None or mb is None or ma.shape != mb.shape:
        return False
    k = np.ones((3, 3), np.uint8)
    only = (ma ^ mb) & ~(cv2.dilate(ma, k) & cv2.dilate(mb, k))
    return int(only.sum()) < settings.RESOLVE_MIN_IMAGE_AREA


def _content_supports(parts: dict) -> bool:
    """Содержание подтверждает пару само по себе (без названия и номера листа)."""
    return (parts.get("text", 0) >= 0.15 or parts.get("layout", 0) >= 0.15 or parts.get("image", 0) >= 0.4
            or parts.get("containment", 0) >= 0.25)


def _mask_score(a, b) -> float:
    if a.mask is None or b.mask is None or a.mask.shape != b.mask.shape:
        return 0.0
    k = np.ones((3, 3), np.uint8)
    inter = int((a.mask & cv2.dilate(b.mask, k)).sum()) + int((b.mask & cv2.dilate(a.mask, k)).sum())
    total = int(a.mask.sum()) + int(b.mask.sum())
    return inter / total if total else 1.0


def is_permit(f) -> bool:
    return getattr(f, "kind", "") == "permit" or bool(getattr(f, "permit_no", ""))


def permit_conflict(a, b) -> bool:
    """Разрешение на внесение изменений — отдельный документ: листы разных разрешений (разные номера) и пара
    «разрешение — не разрешение» не связываются."""
    pa, pb = getattr(a, "permit_no", ""), getattr(b, "permit_no", "")
    return bool(pa and pb and pa != pb) or is_permit(a) != is_permit(b)


def _expected(prev: list, cur: list, links: dict) -> tuple[dict[int, tuple[int, int]], dict[int, tuple[int, int]]]:
    """Место пары между соседями: ближайшие принятые пары слева и справа (до RESOLVE_NEIGHBOR_SPAN страниц) дают
    промежуток страниц другой версии (b слева, b справа). Нужно двойникам: «ближе» — к месту соседей, а не к доле
    длины файла (ОВ2.1: 62→58 и 65→60 — между ними только 59). Сосед только с одной стороны не годится: между ним
    и двойником могла быть вставка (ОВ1.2: обложка V4 → V5)."""
    pairs = [(prev[i].page, cur[j].page) for i, (j, _s, _p) in links.items()]
    def near(pages: list, own: list[tuple[int, int]]) -> dict[int, tuple[int, int]]:
        out = {}
        for k, f in enumerate(pages):
            left = max(((a, b) for a, b in own if 0 < f.page - a <= settings.RESOLVE_NEIGHBOR_SPAN), default=None)
            right = min(((a, b) for a, b in own if 0 < a - f.page <= settings.RESOLVE_NEIGHBOR_SPAN), default=None)
            if left and right and left[1] < right[1]:
                out[k] = (left[1], right[1])
        return out
    return near(prev, pairs), near(cur, [(b, a) for a, b in pairs])


def strong_pairs(prev: list, cur: list, S: np.ndarray, links: dict) -> set[int]:
    """Пары, подтверждённые решающим доказательством с обеих сторон (те же проверки, что у спорных пар):
    для прямой сверки несоседних версий без цепочки — общая оценка пары одна доказательством не считается."""
    w = weights(prev + cur)
    tprev = Counter(_title_key(f) for f in prev if _title_key(f))
    tcur = Counter(_title_key(f) for f in cur if _title_key(f))
    fprev, fcur = footer_counts(prev), footer_counts(cur)
    floor = S >= MATCH_CANDIDATE_FLOOR
    exp_a, exp_b = _expected(prev, cur, links)
    out = set()
    for i, (j, _s, _p) in links.items():
        K = [jj for jj in range(len(cur)) if floor[i, jj]]
        Ki = [ii for ii in range(len(prev)) if floor[ii, j]]
        bi, how = _winner(prev[i], [cur[jj] for jj in K], w, (tprev, tcur), (fprev, fcur), exp_a.get(i))
        bj, how_b = _winner(cur[j], [prev[ii] for ii in Ki], w, (tcur, tprev), (fcur, fprev), exp_b.get(j))
        if bi is None or bj is None or K[bi] != j or Ki[bj] != i or how == how_b == "single":
            continue
        if (prev[i].kind in TABLE_KINDS or cur[j].kind in TABLE_KINDS) and min(
                _mark_cover(set(prev[i].tokens), set(cur[j].tokens), w),
                _mark_cover(set(cur[j].tokens), set(prev[i].tokens), w)) < settings.RESOLVE_TABLE_COVER:
            continue
        out.add(i)
    return out


def _mark_cover(sa: set, sb: set, w: dict[str, float]) -> float:
    marks = {t for t in sa if token_class(t) == settings.RESOLVE_W_MARK}
    tot = sum(w.get(t, 0.0) for t in marks)
    return sum(w.get(t, 0.0) for t in marks & sb) / tot if tot else 1.0


def _pages(pages: list, idx: list) -> str:
    """Номера страниц диапазонами: 5–9, 12."""
    ps = sorted(pages[i].page for i in idx)
    out, start = [], None
    for k, p in enumerate(ps):
        if start is None:
            start = p
        if k + 1 == len(ps) or ps[k + 1] != p + 1:
            out.append(str(start) if start == p else f"{start}–{p}")
            start = None
    return ", ".join(out)


def _blocks(prev: list, cur: list, S: np.ndarray, links: dict, used: set, w: dict[str, float]) -> list[tuple[list, list]]:
    """Связь блока таблицы: неразрешённые листы-таблицы A и B, позиции которых разошлись по нескольким листам.
    Связь листов — заметная доля марок одного на другом; блок — связная группа, где хотя бы с одной стороны
    больше одного листа, а марки группы A почти целиком есть в группе B и наоборот."""
    # Группа строится по всем листам-таблицам перехода (уже принятая пара не рвёт цепочку перенесённых строк);
    # блоком помечаются только неразрешённые листы группы.
    fa = [i for i in range(len(prev)) if prev[i].kind in TABLE_KINDS]
    fb = [j for j in range(len(cur)) if cur[j].kind in TABLE_KINDS]
    if not any(i not in links for i in fa) and not any(j not in used for j in fb):
        return []
    sets_a = {i: set(prev[i].tokens) for i in fa}
    sets_b = {j: set(cur[j].tokens) for j in fb}
    adj: dict[tuple, set] = defaultdict(set)
    for i in fa:
        for j in fb:
            if S[i, j] < MATCH_CANDIDATE_FLOOR:
                continue
            if max(_mark_cover(sets_a[i], sets_b[j], w), _mark_cover(sets_b[j], sets_a[i], w)) >= settings.RESOLVE_BLOCK_EDGE:
                adj[("a", i)].add(("b", j))
                adj[("b", j)].add(("a", i))
    out, seen = [], set()
    for node in list(adj):
        if node in seen:
            continue
        comp, stack = set(), [node]
        while stack:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x)
            stack.extend(adj[x] - comp)
        seen |= comp
        ia = sorted(i for side, i in comp if side == "a")
        jb = sorted(j for side, j in comp if side == "b")
        if len(ia) < 2 and len(jb) < 2:
            continue
        ua = set().union(*(sets_a[i] for i in ia))
        ub = set().union(*(sets_b[j] for j in jb))
        if min(_mark_cover(ua, ub, w), _mark_cover(ub, ua, w)) >= settings.RESOLVE_BLOCK_COVER:
            open_a = [i for i in ia if i not in links]
            open_b = [j for j in jb if j not in used]
            if open_a or open_b:
                out.append((open_a, open_b, ia, jb))
    return out


def package_map(prev: list, cur: list, links: dict) -> tuple[dict[str, str], dict[str, str]]:
    """Соответствие обозначений (комплектов) по уверенным парам перехода: обозначение A → обозначение B,
    если так связаны не меньше RESOLVE_PACKAGE_MIN пар и не меньше RESOLVE_PACKAGE_SHARE пар этого обозначения."""
    fwd: dict[str, Counter] = defaultdict(Counter)
    for i, (j, _s, _p) in links.items():
        da, db = prev[i].designation, cur[j].designation
        if da and db:
            fwd[da][db] += 1
    m, r = {}, {}
    for da, c in fwd.items():
        db, n = c.most_common(1)[0]
        if n >= settings.RESOLVE_PACKAGE_MIN and n / sum(c.values()) >= settings.RESOLVE_PACKAGE_SHARE:
            m[da] = db
    rev: dict[str, Counter] = defaultdict(Counter)
    for da, db in m.items():
        rev[db][da] += fwd[da][db]
    for db, c in rev.items():
        if len(c) == 1:
            r[db] = next(iter(c))
    return m, r


def package_conflict(a, b, pmap: tuple[dict, dict]) -> bool:
    """Противоречие комплекту с обеих сторон: обозначение A устойчиво переходит в другое обозначение B,
    и обозначение B устойчиво приходит из другого обозначения A."""
    m, r = pmap
    if not a.designation or not b.designation:
        return False
    return (a.designation in m and m[a.designation] != b.designation) and \
        (b.designation in r and r[b.designation] != a.designation)


def resolve(prev: list, cur: list, S: np.ndarray, parts_all: dict, links: dict,
            rules: frozenset[str]) -> tuple[dict, dict[tuple[str, int], str]]:
    """Дополнить пары основного назначения доказанными парами. Возвращает (пары, причины неразрешённых:
    (сторона 'a'/'b', индекс) → код причины)."""
    n, m = len(prev), len(cur)
    w = weights(prev + cur)
    tprev = Counter(_title_key(f) for f in prev if _title_key(f))
    tcur = Counter(_title_key(f) for f in cur if _title_key(f))
    foot = (footer_counts(prev), footer_counts(cur)) if "pageno" in rules else None
    floor = S >= MATCH_CANDIDATE_FLOOR
    pmap = package_map(prev, cur, links) if "package" in rules else ({}, {})
    taken_c = {j for j, _s, _p in links.values()}
    exp_a, exp_b = _expected(prev, cur, links)
    reasons: dict[tuple[str, int], str] = {}
    out = dict(links)
    # Листы разных разрешений не связываются и в основном назначении.
    for i, (j, _s, _p) in list(out.items()):
        if "permit" in rules and permit_conflict(prev[i], cur[j]):
            del out[i]
            taken_c.discard(j)
            reasons[("a", i)] = reasons[("b", j)] = "permit"
    # Уже принятые пары проверкой комплекта не снимаются: обозначение в штампе на объекте 213 нестабильно
    # (суффикс «.СО» пропадает между версиями, в штамп копируют чужой шифр) — 11 из 12 снятий были ложными.
    decided: dict[int, tuple[int, str]] = {}
    for i in range(n):
        if i in out:
            continue
        K = [j for j in range(m) if floor[i, j]]
        if not K:
            continue
        bi, how = _winner(prev[i], [cur[j] for j in K], w, (tprev, tcur), foot, exp_a.get(i))
        if bi is None:
            reasons[("a", i)] = how
            continue
        j = K[bi]
        if j in taken_c:
            reasons[("a", i)] = "taken"
            continue
        if S[i, j] < MATCH_MEDIUM:
            reasons[("a", i)] = "below_pair"
            continue
        Ki = [ii for ii in range(n) if floor[ii, j]]
        bj, how_b = _winner(cur[j], [prev[ii] for ii in Ki], w, (tcur, tprev), foot[::-1] if foot else None,
                            exp_b.get(j))
        if bj is None or Ki[bj] != i:
            reasons[("a", i)] = "not_mutual"
            continue
        if "permit" in rules and permit_conflict(prev[i], cur[j]):
            reasons[("a", i)] = "permit"       # листы разных разрешений на внесение изменений — разные документы
            continue
        if "title" in (how, how_b) and not _content_supports(parts_all[(i, j)]):
            reasons[("a", i)] = "title_only"   # одинаковое название, но содержание листа не подтверждает
            continue
        if "single" == how == how_b:
            reasons[("a", i)] = "weak"      # единственный кандидат с обеих сторон, но основное правило пару не приняло
            continue
        if (prev[i].kind in TABLE_KINDS or cur[j].kind in TABLE_KINDS) and min(
                _mark_cover(set(prev[i].tokens), set(cur[j].tokens), w),
                _mark_cover(set(cur[j].tokens), set(prev[i].tokens), w)) < settings.RESOLVE_TABLE_COVER:
            reasons[("a", i)] = "block"
            continue
        if "package" in rules and package_conflict(prev[i], cur[j], pmap):
            reasons[("a", i)] = "package"
            continue
        decided[i] = (j, how if how != "single" else how_b)
    for i, (j, how) in decided.items():
        out[i] = (j, float(S[i, j]), parts_all[(i, j)] | {"resolved": how})
        reasons.pop(("a", i), None)
    used = {j for j, _s, _p in out.values()}
    for open_a, open_b, ia, jb in _blocks(prev, cur, S, out, used, w):
        detail = (f"A стр. {_pages(prev, ia)} ↔ B стр. {_pages(cur, jb)}")
        for i in open_a:
            reasons[("a", i)] = ("block", detail)
        for j in open_b:
            reasons[("b", j)] = ("block", detail)
    for j in range(m):
        if j not in used and ("b", j) not in reasons and floor[:, j].any():
            # Причина со стороны B: та же, что у A-страницы, на которую указывает лучший кандидат.
            i = int(np.argmax(S[:, j]))
            reasons[("b", j)] = reasons.get(("a", i), "weak")
    return out, reasons
