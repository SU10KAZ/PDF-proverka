#!/usr/bin/env python3
"""G2 research PoC: расширяемый EOM-движок SYSTEM_GRAPH поверх механик Вектографа.

RESEARCH ONLY. Production-код не меняется и не импортируется на запись.
Переиспользуются production-хелперы Вектографа:
    _clip_words_to_polygon / _clip_words_to_bbox  — клип по области блока
    _bind_codes_columnwise                        — привязка «код ↔ колонка аппарата»
    _median                                       — медиана шага колонок
и контракт подготовленного блока G1 (block_from_record) — ради корректного
поворота страницы (visual-координаты).

Архитектура:
    BACKBONE (общая механика, диалект-независимая)
        evidence_scan → token_layers → device_row → sections → roles
        → column_binding → source_path → functional_groups → gates
    PROFILE (диалект задаёт ТОЛЬКО распознаватели/правила/пороги)
"""
from __future__ import annotations

import collections
import json
import math
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fitz

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _bind_codes_columnwise,
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    _median,
)

SCHEMA_VERSION = "system-graph.v0-research"


# ═══════════════════════════════════════════════════════════════════════════
# 0. ОБЩИЙ СЛОЙ УЛИК (evidence scan) — един для всех диалектов и дисциплин
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class Word:
    x0: float
    y0: float
    x1: float
    y1: float
    text: str

    @property
    def cx(self) -> float:
        return (self.x0 + self.x1) / 2.0

    @property
    def cy(self) -> float:
        return (self.y0 + self.y1) / 2.0

    def bbox(self) -> list[float]:
        return [round(self.x0, 2), round(self.y0, 2), round(self.x1, 2), round(self.y1, 2)]


@dataclass
class BlockEvidence:
    """Сырьё подготовленного блока в ВИЗУАЛЬНЫХ координатах страницы."""
    pdf_path: str
    page_index: int
    rotation: int
    page_size: tuple[float, float]
    block_id: str
    bbox_visual_pt: list[float]
    polygon_visual_pt: Optional[list[list[float]]]
    words: list[Word]
    segments: list[tuple[float, float, float, float]] = field(default_factory=list)

    def text(self) -> str:
        """Построчный текст блока. Строки собираются по Y с допуском в долю
        высоты глифа — иначе соседние строки схлопываются и построчные якоря
        расчёта (`код : … кВт - … - … А`) перестают распознаваться."""
        if not self.words:
            return ""
        heights = sorted(w.y1 - w.y0 for w in self.words)
        h = heights[len(heights) // 2] or 8.0
        tol = max(1.5, 0.6 * h)
        rows: list[list[Word]] = []
        for w in sorted(self.words, key=lambda t: (t.y0, t.x0)):
            if rows and abs(w.y0 - rows[-1][0].y0) <= tol:
                rows[-1].append(w)
            else:
                rows.append([w])
        return "\n".join(" ".join(x.text for x in sorted(r, key=lambda t: t.x0)) for r in rows)


def scan_block(pdf_path: Path, record: dict) -> BlockEvidence:
    """Единая точка входа: подготовленный блок → улики.

    ВАЖНО: слова переводятся в визуальные координаты через page.rotation_matrix.
    Production-вектограф этого НЕ делает — на повёрнутых листах его клип
    обрезает блок неверно (см. отчёт, блокер B4).
    """
    doc = fitz.open(str(pdf_path))
    try:
        pi = int(record.get("page_index") or 0)
        pg = doc[pi]
        matrix = pg.rotation_matrix
        raw = []
        for w in pg.get_text("words"):
            r = fitz.Rect(w[:4]) * matrix
            raw.append((min(r.x0, r.x1), min(r.y0, r.y1), max(r.x0, r.x1), max(r.y0, r.y1), w[4]))
        pw, ph = float(pg.rect.width), float(pg.rect.height)
        poly = record.get("polygon_points") or record.get("polygon_points_norm")
        bbox = record.get("coords_norm")
        if poly:
            clipped = _clip_words_to_polygon(raw, poly, pw, ph)
        elif bbox:
            clipped = _clip_words_to_bbox(raw, bbox, pw, ph)
        else:
            clipped = raw
        words = [Word(*w) for w in clipped]
        polygon_visual = [[p[0] * pw, p[1] * ph] for p in poly] if poly else None
        bbox_visual = ([bbox[0] * pw, bbox[1] * ph, bbox[2] * pw, bbox[3] * ph]
                       if bbox else [0.0, 0.0, pw, ph])
        rotation = int(pg.rotation)
    finally:
        doc.close()
    return BlockEvidence(
        pdf_path=str(pdf_path), page_index=pi, rotation=rotation, page_size=(pw, ph),
        block_id=str(record.get("block_id") or record.get("id") or ""),
        bbox_visual_pt=[round(v, 2) for v in bbox_visual],
        polygon_visual_pt=polygon_visual, words=words,
    )


# ═══════════════════════════════════════════════════════════════════════════
# 1. PROFILE / DIALECT — задаёт ТОЛЬКО распознаватели, правила и пороги
# ═══════════════════════════════════════════════════════════════════════════

LATIN_TO_CYR = str.maketrans({
    "A": "А", "B": "В", "C": "С", "E": "Е", "H": "Н", "K": "К", "M": "М",
    "O": "О", "P": "Р", "T": "Т", "X": "Х", "Y": "У",
})

# «Свитчевые» аппараты: автомат / рубильник / выключатель нагрузки
SWITCHGEAR_RE = re.compile(r"^(?P<sec>\d{0,2})(?P<kind>QF|QS|ВР|ВН|SF)(?P<num>\d+(?:\.\d+)*)$")
SPD_RE = re.compile(r"УЗИП|ОПН|^FV\d|разрядник", re.IGNORECASE)
FUSE_RE = re.compile(r"^FU\d|^ППН")
INDICATOR_RE = re.compile(r"^HL\d")
METER_RE = re.compile(r"^\d?[ТT][ТTA]\d|^Wh\d?$|^PW\d?$|Меркур|НАРТИС|Мультиметр|Анализатор|^МТ-7")
COMPENSATION_RE = re.compile(r"^(АУКРМ|УКРМ|КРМ)[\w\-№.]*$")
RATING_RE = re.compile(r"^(\d{2,5})\s*[АA]$")
POLES_RE = re.compile(r"^[1234][РP]$")
CABLE_RE = re.compile(r"ППГ|ВВГ|КППГ|NYM|КПС|ПуГП", re.IGNORECASE)
RESERVE_RE = re.compile(r"^Резерв", re.IGNORECASE)
BUS_MARK_RE = re.compile(r"^L1[,\-]\s*L2[,\-]\s*L3$|^PEN$")
SECTION_NAME_RE = re.compile(r"^(РП\d+|с\.ш\.?\d*|секц\w*)$", re.IGNORECASE)
BUSWAY_RE = re.compile(r"^Шинопровод|^Шинопр", re.IGNORECASE)
# Источник: внешняя ТП / явный трансформатор / внешний ввод
SOURCE_TP_RE = re.compile(r"^ТП\d*$")
SOURCE_TR_RE = re.compile(r"^\d?[ТT]\d$")
SOURCE_INPUT_RE = re.compile(r"^Ввод$")
AVR_RE = re.compile(r"^АВР$")
MOTOR_RE = re.compile(r"^[МM]$")

# Идентификаторы назначения / потребителя
DEST_CODE_RE = re.compile(r"^\d{0,2}ГРЩ\d{0,2}[-–][\wА-Яа-я.\-]{2,}$")
POSITIONAL_CODE_RE = re.compile(r"^ГРЩ\d+[-–]РП\d+[-–]\d+$")
# Обозначение потребителя = аббревиатура/марка, а НЕ обычное слово («Щит», «Шкаф»).
# Признак обозначения: после первой буквы идёт заглавная, цифра или разделитель.
CONSUMER_RE = re.compile(
    r"^(?:ВРУ|ШУ|ЩУ|ШР|ЩР|ШК|ЩК|ШН|ЩН|ХМ|ДР|АУКРМ|УКРМ|КРМ|ЭБ|ЯСН|ЯТП|ГРЩ|Щ|Ш|Я)"
    r"(?:[0-9OОа-я]|[.\-–_][0-9A-ZА-Яа-я]|[A-ZА-Я][A-ZА-Я0-9OО.\-–_]*)?"
    r"[A-ZА-Яa-zа-я0-9.\-–_№]*$")

PROFILES = {
    "classic_calc_singleline": {
        "device_re": re.compile(r"^QF\d+(?:\.\d+){1,2}$"),
        "min_devices": 5,
        "section_from_label_prefix": False,
        "note": "диалект, который уже умеет production-вектограф (расчётный якорь строкой)",
    },
    "dense_sectioned_board": {
        "device_re": re.compile(r"^\d{1,2}QF\d+$"),
        "min_devices": 8,
        "section_from_label_prefix": True,
        "note": "плотный ГРЩ/ВРУ: нумерация <секция>QF<N>, секционный аппарат в разрыве",
    },
    "kv_annotated_singleline": {
        "device_re": re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$"),
        "min_devices": 4,
        "section_from_label_prefix": True,
        "note": "расчёт подписан key-value (Рр=…, cosf=…, Iрасч=…) вместо построчного якоря",
    },
    "bare_device_scheme": {
        "device_re": re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$"),
        "min_devices": 3,
        "section_from_label_prefix": True,
        "note": "аппараты есть, расчётных подписей нет",
    },
    "unknown_singleline": {
        "device_re": re.compile(r"^\d{0,2}QF\d+(?:\.\d+)*$"),
        "min_devices": 3,
        "section_from_label_prefix": True,
        "note": "профиль не опознан — минимальный набор улик",
    },
}

KV_PARAM_RE = re.compile(r"(?:Рр|Pp|Ру|Py|Iр|Ip|Iрасч)\s*=\s*[\d.,/]+")
JOINED_PARAM_RE = re.compile(
    r"^\S+?\s*:\s*[\d.,]+\s*кВт\s*-\s*[\d.,]+\s*-\s*[\d.,]+\s*-\s*[\d.,]+\s*кВт\s*-\s*[\d.,]+\s*А\s*$")


def detect_profile(ev: BlockEvidence, text: Optional[str] = None) -> dict:
    """Определение диалекта ПО УЛИКАМ, до какого-либо структурирования (§38)."""
    toks = [w.text for w in ev.words]
    devices = [SWITCHGEAR_RE.match(t) for t in toks]
    devices = [m for m in devices if m]
    qf = sorted({m.group(0) for m in devices if m.group("kind") == "QF"})
    dotted = sum(1 for t in qf if "." in t)
    prefixed = sum(1 for t in qf if t[0].isdigit())
    # Канонический текст блока: если конвейер уже сохранил свой (pdfplumber), берём
    # его — иначе диалект определяется по ДРУГОЙ реконструкции строк, чем та, на
    # которой работает production-структурер, и детектор с ним расходится.
    text = text if text is not None else ev.text()
    signals = {
        "device_tokens": len(qf),
        "dotted_frac": round(dotted / max(len(qf), 1), 3),
        "prefixed_frac": round(prefixed / max(len(qf), 1), 3),
        "prefix_groups": sorted({t[0] for t in qf if t[0].isdigit()}),
        "joined_param_lines": sum(1 for l in text.split("\n") if JOINED_PARAM_RE.match(l.strip())),
        "kv_params": len(KV_PARAM_RE.findall(text)),
        "bus_markers": sum(1 for t in toks if BUS_MARK_RE.match(t)),
        "tie_candidates": sum(1 for m in devices if m.group("kind") in ("QS", "ВР", "ВН")),
        "metering_tokens": sum(1 for t in toks if METER_RE.match(t)),
        "compensation_tokens": sum(1 for t in toks if COMPENSATION_RE.match(t)),
    }
    why = []
    if signals["joined_param_lines"] >= 3 and signals["dotted_frac"] >= 0.6:
        pid = "classic_calc_singleline"
        why.append("построчный расчётный якорь + точечная нумерация QF")
    elif (signals["prefixed_frac"] >= 0.6 and signals["device_tokens"] >= 10
          and (signals["bus_markers"] or signals["tie_candidates"])):
        pid = "dense_sectioned_board"
        why.append("нумерация <секция>QF<N> при ≥10 аппаратах + маркер шин/секционный аппарат")
    elif signals["kv_params"] >= 6:
        pid = "kv_annotated_singleline"
        why.append("key-value расчётные подписи без построчного якоря")
    elif signals["device_tokens"] >= 3:
        pid = "bare_device_scheme"
        why.append("аппараты есть, расчётных подписей нет")
    else:
        pid = "unknown_singleline"
        why.append("недостаточно улик")
    return {"id": pid, "signals": signals, "why": why, **PROFILES[pid]}


# ═══════════════════════════════════════════════════════════════════════════
# 2. НОРМАЛИЗАЦИЯ: canonical identity ≠ display label (§19)
# ═══════════════════════════════════════════════════════════════════════════

# Класс смешиваемых префиксов щитов: в РД одну и ту же сущность пишут Ш/Щ.
CONFUSABLE_PREFIX = {"Щ": "Ш"}
# Семейство «щит/шкаф/устройство» — структурный префикс, а не идентичность.
PANEL_FAMILY_RE = re.compile(r"^(ВРУ|ШУ|ШР|ЩУ|ЩР|ШК|ЩК)[.\-–_]?(?P<rest>.*)$")
STAGE_PREFIX_RE = re.compile(r"^\d{0,2}ГРЩ\d{0,2}[-–](?P<rest>.+)$")


ORDINARY_WORD_RE = re.compile(r"^[А-ЯA-Z][а-яa-z]{2,}$")


def looks_like_designation(raw: str) -> bool:
    """Обозначение оборудования, а не обычное слово: «ШУ-ХЦ» да, «Щит» нет."""
    t = (raw or "").strip()
    if ORDINARY_WORD_RE.match(t):
        return False
    return True


def normalize_token(raw: str) -> str:
    s = (raw or "").strip().strip(",;:()[]«»\"'")
    s = s.upper().replace("Ё", "Е").translate(LATIN_TO_CYR)
    s = s.replace(".", "-").replace("–", "-").replace("_", "-")
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if s and s[0] in CONFUSABLE_PREFIX:
        s = CONFUSABLE_PREFIX[s[0]] + s[1:]
    return s


def canonical_identity(raw: str) -> Optional[str]:
    """Функциональная идентичность ветви, независимая от стадии и компоновки.

    Структурные правила (без ручного словаря объектов):
      1. снять стадийно-щитовой префикс `<n>ГРЩ<n>-`;
      2. позиционный код `ГРЩ1-РП1-7` идентичностью НЕ является (это слот);
      3. снять семейный префикс щита (ВРУ/ШУ/ЩУ/…) — если остаток осмысленный;
      4. Ш/Щ считать одним классом, латиницу привести к кириллице.
    """
    if not raw:
        return None
    s = normalize_token(raw)
    if POSITIONAL_CODE_RE.match(s.replace("-", "-")):
        return None
    m = STAGE_PREFIX_RE.match(s)
    if m:
        s = m.group("rest")
    if re.fullmatch(r"РП\d+-\d+", s):
        return None
    m = PANEL_FAMILY_RE.match(s)
    if m:
        rest = m.group("rest")
        # ВРУ1 / ВРУА — номер (или одна литера) И ЕСТЬ идентичность, префикс сохраняем.
        # ВРУ-ХЦ / ШУХЦ / ЩУ.АПТ — суффикс из ≥2 литер это функция, префикс снимаем.
        if rest and not re.fullmatch(r"\d+[А-Я]?|[А-Я]", rest):
            s = rest
    return s or None


# ═══════════════════════════════════════════════════════════════════════════
# 3. BACKBONE — общая геометрическая механика
# ═══════════════════════════════════════════════════════════════════════════

def _cluster_1d(values, gap):
    """Кластеризация по разрывам. values — отсортированный список чисел."""
    if not values:
        return []
    out, cur = [], [values[0]]
    for v in values[1:]:
        if v - cur[-1] > gap:
            out.append(cur)
            cur = [v]
        else:
            cur.append(v)
    out.append(cur)
    return out


def find_device_row(ev: BlockEvidence, profile: dict):
    """Ряд ОТХОДЯЩИХ аппаратов = самый населённый Y-кластер устройств профиля."""
    devs = [w for w in ev.words if profile["device_re"].match(w.text)]
    if len(devs) < profile["min_devices"]:
        return [], []
    ys = sorted(w.cy for w in devs)
    heights = [w.y1 - w.y0 for w in devs] or [8.0]
    tol = max(6.0, 2.5 * _median(heights))
    clusters = _cluster_1d(ys, tol)
    best = max(clusters, key=len)
    lo, hi = min(best) - tol, max(best) + tol
    row = sorted([w for w in devs if lo <= w.cy <= hi], key=lambda w: w.cx)
    rest = [w for w in devs if not (lo <= w.cy <= hi)]
    return row, rest


def partition_sections(row, profile):
    """Секции шин. Две независимые улики, ни одна не абсолютна:

    * префикс метки аппарата (`1QF7` → секция «1») — семантика;
    * положение по X (аппараты одной секции идут подряд) — геометрия.

    Секции на листе идут ПОДРЯД, поэтому разбиение = выбор k−1 точек раскроя
    вдоль X. Выбирается раскрой с максимальным согласием с префиксами меток.
    Так одиночная опечатка CAD («1QF1» посреди второй секции) не растягивает
    границу секции и не рвёт разрыв, а честно попадает в conflicts.
    Если префиксов нет — обычная кластеризация по X-разрывам.
    """
    import itertools
    xs = [w.cx for w in row]
    steps = [xs[i + 1] - xs[i] for i in range(len(xs) - 1)]
    step = _median([t for t in steps if t > 1]) or 60.0
    conflicts = []

    matched = [(w, SWITCHGEAR_RE.match(w.text)) for w in row]
    prefixes = [m.group("sec") for _, m in matched if m and m.group("sec")]
    distinct = sorted(set(prefixes))
    use_prefix = (bool(profile.get("section_from_label_prefix"))
                  and len(prefixes) >= 0.8 * len(row) and 2 <= len(distinct) <= 4)

    if use_prefix:
        k = len(distinct)
        n = len(row)
        own = [(m.group("sec") if m and m.group("sec") else None) for _, m in matched]
        gaps_at = [xs[i] - xs[i - 1] for i in range(1, n)]   # разрыв ПЕРЕД индексом i
        max_gap = max(gaps_at) if gaps_at else 1.0
        best, best_score, best_agree = None, -1e9, 0
        for cuts in itertools.combinations(range(1, n), k - 1):
            bounds = (0,) + cuts + (n,)
            for perm in itertools.permutations(distinct):
                agree = 0
                for gi in range(k):
                    for idx in range(bounds[gi], bounds[gi + 1]):
                        if own[idx] == perm[gi]:
                            agree += 1
                # Одна опечатка CAD не должна перевешивать реальный разрыв компоновки:
                # к доле согласия добавляется нормированный разрыв в точке раскроя.
                gap_bonus = sum(gaps_at[c - 1] for c in cuts) / (max_gap * max(k - 1, 1))
                score = agree / n + 0.5 * gap_bonus
                if score > best_score:
                    best_score, best_agree, best = score, agree, (bounds, perm)
        bounds, perm = best
        best_score = best_agree
        clusters, keys = [], []
        for gi in range(len(perm)):
            cl = row[bounds[gi]:bounds[gi + 1]]
            if not cl:
                continue
            clusters.append(cl)
            keys.append(perm[gi])
            for w, o in zip(cl, own[bounds[gi]:bounds[gi + 1]]):
                if o is not None and o != perm[gi]:
                    conflicts.append({
                        "label": w.text, "x": round(w.cx, 1),
                        "label_section": o, "geometric_section": perm[gi],
                        "note": "префикс метки не совпадает с непрерывным X-раскроем "
                                "секций (вероятная опечатка CAD); принята геометрия",
                    })
        agreement = round(best_score / max(len(prefixes), 1), 3)
    else:
        clusters, cur = [], [row[0]]
        for w in row[1:]:
            if w.cx - cur[-1].cx > max(1.6 * step, step + 25.0):
                clusters.append(cur)
                cur = [w]
            else:
                cur.append(w)
        clusters.append(cur)
        keys = [None] * len(clusters)
        agreement = None

    sections = []
    for i, (cl, key) in enumerate(zip(clusters, keys), 1):
        sections.append({
            "id": f"BS{i}", "index": i, "label_prefix": key,
            "x_range": [round(cl[0].cx, 1), round(cl[-1].cx, 1)],
            "devices": cl, "step": round(step, 1),
            "partition_source": "label_prefix+x_cut" if use_prefix else "x_gap",
            "prefix_agreement": agreement,
        })
    return sections, conflicts, step


def name_sections(ev, sections, row_y):
    """Имя секции из ближайшего токена «РПn / с.ш.n» в полосе вокруг ряда."""
    cands = [w for w in ev.words if SECTION_NAME_RE.match(w.text)]
    for s in sections:
        x0, x1 = s["x_range"]
        near = [w for w in cands if x0 - 2.5 * s["step"] <= w.cx <= x1 + 2.5 * s["step"]]
        near.sort(key=lambda w: abs(w.cy - row_y))
        s["name"] = near[0].text if near else f"секция {s['index']}"
        s["name_evidence"] = ({"token": near[0].text, "bbox": near[0].bbox()} if near else None)
    return sections


def classify_incoming_zone(ev, row, sections, step):
    """Роли аппаратов ВВОДНОЙ зоны (ниже ряда отходящих).

    Правила (без хардкода конкретных обозначений):
      SECTION_DEVICE — аппарат, чей X попадает в РАЗРЫВ между двумя секциями;
      SERVICE_NODE   — аппарат рядом с УЗИП/ОПН/FV или предохранителем (защита);
      INPUT_DEVICE   — по одному на секцию: аппарат с максимальным номиналом
                       в своей секции (номинал берётся из собственной колонки);
      UNKNOWN_NODE   — всё остальное (честно, а не «наверное секционник»).
    """
    row_y = _median([w.cy for w in row])
    below = [w for w in ev.words
             if SWITCHGEAR_RE.match(w.text) and w.cy > row_y + 1.5 * _median([w.y1 - w.y0 for w in row])]
    gaps = []
    for a, b in zip(sections, sections[1:]):
        gaps.append((a["x_range"][1], b["x_range"][0], a["id"], b["id"]))

    def rating_near(w):
        best = None
        for c in ev.words:
            if not RATING_RE.match(c.text):
                continue
            # «своя колонка» — доли шага, а не шаг: на плотном ГРЩ подпись соседнего
            # аппарата стоит в 0.5 шага, и широкое окно даёт один номинал двум узлам
            if abs(c.cx - w.cx) < 0.45 * step and 0 <= (c.cy - w.cy) < 6.0 * (w.y1 - w.y0):
                v = int(RATING_RE.match(c.text).group(1))
                if best is None or v > best[0]:
                    best = (v, c)
        return best

    def marker_near(w, pat, dx=0.4, dy=3.0):
        h = w.y1 - w.y0
        return [c for c in ev.words if pat.search(c.text)
                and abs(c.cx - w.cx) < dx * step and abs(c.cy - w.cy) < dy * h]

    out = []
    for w in below:
        m = SWITCHGEAR_RE.match(w.text)
        rating = rating_near(w)
        spd = marker_near(w, SPD_RE)
        fuse = marker_near(w, FUSE_RE)
        in_gap = next((g for g in gaps if g[0] < w.cx < g[1]), None)
        host = None
        if not in_gap:
            host = next((s for s in sections
                         if s["x_range"][0] - 1.5 * step <= w.cx <= s["x_range"][1] + 1.5 * step), None)
            if host is None:
                host = min(sections, key=lambda s: min(abs(w.cx - s["x_range"][0]),
                                                       abs(w.cx - s["x_range"][1])))
        out.append({
            "word": w, "kind": m.group("kind"), "label": w.text,
            "rating_a": rating[0] if rating else None,
            "rating_evidence": rating[1].bbox() if rating else None,
            "spd_tokens": [c.text for c in spd],
            "fuse_tokens": [c.text for c in fuse],
            "in_gap": in_gap[2:] if in_gap else None,
            "host_section": host["id"] if host else None,
            # Подписи управления (АВР / М / SA / «Секц.») стоят дальше от символа,
            # чем номинал: под приводом, сбоку от аппарата. Своё окно, шире.
            "control": sorted({c.text for c in marker_near(w, AVR_RE, 0.8, 14.0)}
                              | {c.text for c in marker_near(w, MOTOR_RE, 0.35, 14.0)}
                              | {c.text for c in marker_near(
                                  w, re.compile(r"^(SA|SF/SA|Секц\.?)$"), 0.8, 12.0)}),
        })

    # 1) СЕКЦИОННЫЙ аппарат. Улика — не обозначение (QF3 слева, QS1 справа), а
    #    геометрия: он стоит в РАЗРЫВЕ между секциями и ВЫШЕ ряда вводов (шинный
    #    уровень). Поэтому среди кандидатов разрыва берём самый верхний.
    for d in out:
        d["role"] = None
    for gap in gaps:
        pool = [d for d in out if d["in_gap"] == gap[2:]]
        if not pool:
            continue
        pool.sort(key=lambda d: (d["word"].cy, abs(d["word"].cx - (gap[0] + gap[1]) / 2.0)))
        pool[0]["role"] = "SECTION_DEVICE"

    # Аппараты, оставшиеся в разрыве после выбора секционника, — это вводы,
    # нарисованные у самой границы секций. Возвращаем им ближайшую секцию.
    for d in out:
        if d["in_gap"] and d["role"] is None and d["host_section"] is None:
            d["host_section"] = min(
                sections,
                key=lambda s: min(abs(d["word"].cx - s["x_range"][0]),
                                  abs(d["word"].cx - s["x_range"][1])))["id"]

    # 2) ВВОД — по одному на секцию: максимальный номинал в собственной колонке.
    #    Считается ДО защитных узлов: рядом с вводом почти всегда стоят УЗИП/ОПН и
    #    предохранители, и «маркер защиты рядом» роль ввода не отменяет.
    for sec in sections:
        pool = [d for d in out if d["role"] is None and d["host_section"] == sec["id"]]
        if not pool:
            continue
        pool.sort(key=lambda d: (-(d["rating_a"] or 0), d["word"].cy))
        if pool[0]["rating_a"]:
            pool[0]["role"] = "INPUT_DEVICE"
            pool[0]["section"] = sec["id"]

    # 3) защитные узлы (УЗИП/ОПН/FV, предохранители)
    for d in out:
        if d["role"] is None and (d["spd_tokens"] or d["fuse_tokens"]):
            d["role"] = "SERVICE_NODE"

    for d in out:
        if d["role"] is None:
            d["role"] = "UNKNOWN_NODE"
    return out, row_y


def build_source_paths(ev, incoming, step):
    """Путь питания: якорь источника ниже ввода + промежуточные узлы (шинопровод).

    Абстрактный источник (ТП) и раскрытый (Т1 с шинопроводом) описываются ОДНОЙ
    моделью: у пути есть терминальный источник со СВОИМ subclass и список
    промежуточных узлов. Это и позволяет отличить «раскрыли подробнее» от
    «источник изменился» (§14, §22).
    """
    inputs = [d for d in incoming if d["role"] == "INPUT_DEVICE"]
    if not inputs:
        return []
    tp = [w for w in ev.words if SOURCE_TP_RE.match(w.text)]
    tr = [w for w in ev.words if SOURCE_TR_RE.match(w.text)]
    ext = [w for w in ev.words if SOURCE_INPUT_RE.match(w.text)]
    busway = [w for w in ev.words if BUSWAY_RE.match(w.text)]
    paths = []
    for d in inputs:
        iw = d["word"]
        below = lambda ws: [w for w in ws if w.cy > iw.cy and abs(w.cx - iw.cx) < 3.0 * step]
        anchor, subclass = None, None
        cand_tr = below(tr)
        cand_tp = below(tp)
        if cand_tr:
            anchor = min(cand_tr, key=lambda w: abs(w.cx - iw.cx))
            subclass = "TRANSFORMER_EXPLICIT"
        elif cand_tp:
            anchor = min(cand_tp, key=lambda w: abs(w.cx - iw.cx))
            subclass = "UPSTREAM_TP_CONNECTION"
        elif below(ext):
            anchor = min(below(ext), key=lambda w: abs(w.cx - iw.cx))
            subclass = "EXTERNAL_FEEDER"
        intermediates = []
        for w in below(busway):
            if anchor is not None and w.cy > anchor.cy + 1e-6:
                continue
            # узел принадлежит ТОМУ вводу, к которому он ближе всего по X
            owner = min(inputs, key=lambda o: abs(o["word"].cx - w.cx))
            if owner is not d:
                continue
            intermediates.append({"type": "BUSWAY", "label": w.text, "bbox": w.bbox()})
        paths.append({
            "input_label": iw.text,
            "input_bbox": iw.bbox(),
            "section": d.get("section"),
            "source_label": anchor.text if anchor else None,
            "source_subclass": subclass or "UNKNOWN_SOURCE",
            "source_bbox": anchor.bbox() if anchor else None,
            "intermediates": intermediates,
            "input_rating_a": d.get("rating_a"),
            "control": d.get("control"),
        })
    return paths


def bind_column_labels(sections, ev, row_y, step, y_lo, y_hi, pattern, reject=None):
    """Привязать токены-метки к колонкам аппаратов production-механикой.

    Ядро — production `_bind_codes_columnwise` (offset-corrected nearest column
    с перебором δ-алиасов): именно оно уже работает на классическом диалекте.

    Два обобщения поверх него:

    1. Метки колонки лежат в НЕСКОЛЬКИХ Y-рядах (шапка расчётной таблицы, ряд
       обозначений, ряд кодов), и у каждого ряда СВОЙ сдвиг δ. Ряды разделяются
       (как это делает production `_split_codes_by_y_rows`) и привязываются
       независимо; ближний к аппаратам ряд имеет приоритет.
    2. Ряд привязывается СРАЗУ КО ВСЕМ аппаратам листа, а не посекционно.
       Сдвиг подписи — соглашение чертежа, единое для всего ряда; при посекционной
       привязке последняя подпись секции (она смещена вправо на δ) попадает в окно
       СЛЕДУЮЩЕЙ секции и сдвигает там всю привязку на колонку.

    Возвращает (assign{device_key: label}, conflicts, per_row).
    """
    devices = [w for s in sections for w in s["devices"]]
    devices.sort(key=lambda w: w.cx)
    if not devices:
        return {}, {}, {}
    xs_lo = devices[0].cx - 1.5 * step
    xs_hi = devices[-1].cx + 1.5 * step
    cand = [w for w in ev.words
            if pattern.match(w.text) and y_lo <= w.cy <= y_hi
            and xs_lo <= w.cx <= xs_hi
            and not (reject and reject(w.text))]
    if not cand:
        return {}, {}, {}
    pq = sorted([(w.cx, w.cy, w.text + f"@{round(w.cx)}") for w in devices])
    heights = [w.y1 - w.y0 for w in cand] or [8.0]
    tol = max(4.0, 1.2 * _median(heights))
    rows = _cluster_1d(sorted(w.cy for w in cand), tol)
    rows.sort(key=lambda r: abs(_median(r) - row_y))
    result, conflicts, per_row = {}, {}, {}
    for r in rows:
        lo, hi = min(r) - tol, max(r) + tol
        pc = sorted([(w.cx, w.cy, w.text) for w in cand if lo <= w.cy <= hi])
        if not pc:
            continue
        assign, conf = _bind_codes_columnwise(pq, pc)
        per_row[round(_median(r))] = {k: v for k, v in assign.items() if v}
        for key, val in assign.items():
            if val and key not in result:
                result[key] = val
        for key, notes in conf.items():
            conflicts.setdefault(key, []).extend(notes)
    return result, conflicts, per_row


def extract_outgoing(ev, sections, row_y, step, profile):
    """Отходящие линии: аппарат + идентичность + атрибуты, по колонкам."""
    band_h = 40.0
    # Y-полосы меток: всё ВЫШЕ ряда аппаратов внутри блока
    y_hi = row_y - band_h * 0.2
    y_lo = min((w.cy for w in ev.words), default=row_y) - 1.0

    dest, dest_conf, _ = bind_column_labels(sections, ev, row_y, step, y_lo, y_hi, DEST_CODE_RE)
    pos, _, _ = bind_column_labels(sections, ev, row_y, step, y_lo, y_hi, POSITIONAL_CODE_RE)
    # Позиционные коды («ГРЩ1-РП1-7») формально похожи на обозначение потребителя,
    # но идентичностью не являются — иначе их ряд смешивается с рядом обозначений
    # и сдвигает привязку. Отбрасываем их из кандидатов ДО привязки.
    cons, cons_conf, _ = bind_column_labels(
        sections, ev, row_y, step, y_lo, y_hi, CONSUMER_RE,
        reject=lambda t: (canonical_identity(t) is None or bool(DEST_CODE_RE.match(t))
                          or not looks_like_designation(t)))
    res, _, _ = bind_column_labels(sections, ev, row_y, step, y_lo, y_hi, RESERVE_RE)
    # Компенсация НИЖЕ ряда аппаратов (компоновка РД) идентичностью НЕ считается:
    # в той же полосе живут подписи цепей управления («к регулятору АУКРМ №1»),
    # и молча превращать их в идентичность ветви нельзя. Только подсказка.
    comp_hint, _, _ = bind_column_labels(sections, ev, row_y, step,
                                         row_y, row_y + 8.0 * band_h, COMPENSATION_RE)
    comp = {}

    # ── Кросс-рядная сверка (§18): ряд обозначений и ряд кодов назначения должны
    #    указывать на одну и ту же ветвь. Если согласия почти нет, а сдвиг ряда на
    #    одну колонку его восстанавливает — ряд подписей смещён (частая беда CAD),
    #    и его выравнивают. Иначе оставляют как есть, и расхождение честно уходит
    #    в conflicts, а не маскируется.
    def _agree(a, b):
        ca, cb = canonical_identity(a), canonical_identity(b)
        if not ca or not cb:
            return False
        return ca == cb or ca in cb or cb in ca

    shift_notes = []
    for s in sections:
        keys = [w.text + f"@{round(w.cx)}" for w in s["devices"]]
        both = [i for i, k in enumerate(keys) if cons.get(k) and dest.get(k)
                and canonical_identity(dest[k])]
        if len(both) < 2:
            continue
        base = sum(1 for i in both if _agree(cons[keys[i]], dest[keys[i]])) / len(both)
        if base >= 0.5:
            continue
        best = (base, 0)
        for sh in (-1, 1):
            hit = tot = 0
            for i, k in enumerate(keys):
                j = i + sh
                if not cons.get(k) or not (0 <= j < len(keys)) or not dest.get(keys[j]):
                    continue
                tot += 1
                hit += bool(_agree(cons[k], dest[keys[j]]))
            if tot >= 2 and hit / tot > best[0]:
                best = (hit / tot, sh)
        if best[1] and best[0] >= 0.6:
            moved = {}
            for i, k in enumerate(keys):
                j = i + best[1]
                if cons.get(k) and 0 <= j < len(keys):
                    moved[keys[j]] = cons[k]
            for k in keys:
                cons.pop(k, None)
            cons.update(moved)
            shift_notes.append(f"{s['id']}: ряд обозначений смещён на {best[1]:+d} колонки "
                               f"(согласие с кодами назначения {base:.0%} → {best[0]:.0%})")

    feeders = []
    for s in sections:
        for w in s["devices"]:
            key = w.text + f"@{round(w.cx)}"
            # ── multi-signal identity (§18): несколько независимых улик ──
            raw_candidates = [
                ("destination_code", dest.get(key)),
                ("consumer_label", cons.get(key)),
                ("compensation_label", comp.get(key)),
                ("positional_code", pos.get(key)),
            ]
            norm = []
            for role, raw in raw_candidates:
                if not raw:
                    continue
                cid = canonical_identity(raw)
                if cid:
                    norm.append((role, raw, cid))
            identity_set = sorted({c for _, _, c in norm})
            ident = norm[0][2] if norm else None
            display = (cons.get(key) or comp.get(key) or dest.get(key) or pos.get(key))
            evid = [{"kind": "token", "role": role, "value": raw, "canonical": cid}
                    for role, raw, cid in norm]
            for role, raw in raw_candidates:
                if raw and not any(e["value"] == raw for e in evid):
                    evid.append({"kind": "token", "role": role, "value": raw,
                                 "canonical": None,
                                 "note": "позиционный код — идентичностью не является"})
            evid.append({"kind": "geometry", "role": "column",
                         "value": {"x": round(w.cx, 1), "section": s["id"]},
                         "bbox_visual_pt": w.bbox()})
            identity_conflict = len(identity_set) > 1
            if comp_hint.get(key):
                evid.append({"kind": "token", "role": "nearby_compensation_hint",
                             "value": comp_hint[key],
                             "note": "подпись компенсации ниже ряда — подсказка, не идентичность"})
            # атрибуты колонки
            colw = [c for c in ev.words
                    if abs(c.cx - w.cx) < 0.55 * step and y_lo <= c.cy <= row_y + 2 * band_h]
            rating = [int(RATING_RE.match(c.text).group(1)) for c in colw if RATING_RE.match(c.text)]
            cable = [c.text for c in colw if CABLE_RE.search(c.text)]
            reserve = bool(res.get(key)) or any(RESERVE_RE.match(c.text) for c in colw)
            if not identity_set or identity_conflict:
                identity_conf = "LOW"
            elif len(norm) >= 2:
                identity_conf = "HIGH"       # две независимые улики дали одно и то же
            else:
                identity_conf = "MEDIUM"
            notes = list((dest_conf.get(key) or []) + (cons_conf.get(key) or []))
            if identity_conflict:
                notes.append("улики дают разные идентичности: " + ", ".join(identity_set))
            feeders.append({
                "device_label": w.text,
                "device_key": key,
                "section": s["id"],
                "x": round(w.cx, 1),
                "bbox_visual_pt": w.bbox(),
                "display_label": display,
                "canonical_identity": ident,
                "identity_set": identity_set,
                "identity_confidence": identity_conf,
                "status": ("RESERVE" if reserve and not identity_set
                           else "ACTIVE" if identity_set else "UNKNOWN"),
                "rating_a": max(rating) if rating else None,
                "cable": cable[:2],
                "evidence": evid,
                "conflicts": notes,
            })
    return feeders


def extract_functional_groups(ev, sections, incoming, row_y, step):
    """Функциональные группы уровня B: учёт, компенсация, служебные узлы."""
    groups = []
    meter_words = [w for w in ev.words if METER_RE.match(w.text)]
    comp_words = [w for w in ev.words if COMPENSATION_RE.match(w.text)]
    spd_words = [w for w in ev.words if SPD_RE.search(w.text)]

    def bucket(words, kind):
        by_section = collections.defaultdict(list)
        for w in words:
            host = min(sections, key=lambda s: min(abs(w.cx - s["x_range"][0]),
                                                   abs(w.cx - s["x_range"][1]),
                                                   0 if s["x_range"][0] <= w.cx <= s["x_range"][1] else 1e9))
            inside = next((s for s in sections
                           if s["x_range"][0] - 2 * step <= w.cx <= s["x_range"][1] + 2 * step), None)
            by_section[(inside or host)["id"]].append(w)
        for sid, ws in sorted(by_section.items()):
            groups.append({
                "id": f"{kind}:{sid}",
                "type": kind,
                "section": sid,
                "member_tokens": sorted({w.text for w in ws}),
                "member_count": len(ws),
                "evidence": [{"kind": "token", "value": w.text, "bbox_visual_pt": w.bbox()}
                             for w in ws[:12]],
            })

    if meter_words:
        bucket(meter_words, "METERING_GROUP")
    if comp_words:
        bucket(comp_words, "COMPENSATION_GROUP")
    if spd_words:
        bucket(spd_words, "SERVICE_GROUP")
    return groups


# ═══════════════════════════════════════════════════════════════════════════
# 4. СБОРКА SYSTEM_GRAPH
# ═══════════════════════════════════════════════════════════════════════════

def build_system_graph(pdf_path: Path, record: dict, *, side: str = "",
                       canonical_text: Optional[str] = None) -> dict:
    ev = scan_block(Path(pdf_path), record)
    profile = detect_profile(ev, canonical_text)
    row, off_row = find_device_row(ev, profile)
    warnings = []
    if not row:
        return {
            "schema_version": SCHEMA_VERSION, "side": side,
            "block": {"block_id": ev.block_id, "page_index": ev.page_index,
                      "rotation": ev.rotation, "pdf": ev.pdf_path},
            "profile": {k: v for k, v in profile.items() if k in ("id", "signals", "why", "note")},
            "nodes": [], "edges": [], "groups": [],
            "quality": {"backbone_recovered": False},
            "warnings": ["ряд отходящих аппаратов не найден"],
        }
    sections, sec_conflicts, step = partition_sections(row, profile)
    incoming, row_y = classify_incoming_zone(ev, row, sections, step)
    sections = name_sections(ev, sections, row_y)
    paths = build_source_paths(ev, incoming, step)
    feeders = extract_outgoing(ev, sections, row_y, step, profile)
    groups = extract_functional_groups(ev, sections, incoming, row_y, step)

    nodes, edges = [], []

    def add_node(nid, ntype, **kw):
        nodes.append({"id": nid, "type": ntype, **kw})
        return nid

    def add_edge(etype, src, dst, evidence, conf="MEDIUM"):
        edges.append({"id": f"{etype}:{src}->{dst}", "type": etype, "from": src, "to": dst,
                      "evidence": evidence, "confidence": conf})

    for s in sections:
        add_node(s["id"], "BUS_SECTION", level="A", label=s["name"],
                 display_label=s["name"], canonical_identity=f"SECTION#{s['index']}",
                 attrs={"x_range": s["x_range"], "device_count": len(s["devices"]),
                        "label_prefix": s["label_prefix"]},
                 evidence=[{"kind": "geometry", "role": "device_cluster",
                            "value": {"x_range": s["x_range"], "n": len(s["devices"])}}]
                          + ([{"kind": "token", "role": "section_name", **s["name_evidence"]}]
                             if s.get("name_evidence") else []),
                 confidence="HIGH" if s.get("name_evidence") else "MEDIUM")

    for i, p in enumerate(paths, 1):
        sid = p["section"] or (sections[i - 1]["id"] if i <= len(sections) else None)
        src_id = f"SRC{i}"
        add_node(src_id, "SOURCE", level="A", label=p["source_label"],
                 display_label=p["source_label"], subclass=p["source_subclass"],
                 canonical_identity=f"SOURCE_PATH#{sid}",
                 attrs={"abstraction": ("EXPANDED" if p["source_subclass"] == "TRANSFORMER_EXPLICIT"
                                        else "ABSTRACT")},
                 evidence=([{"kind": "token", "role": "source_anchor",
                             "value": p["source_label"], "bbox_visual_pt": p["source_bbox"]}]
                           if p["source_label"] else []),
                 confidence="HIGH" if p["source_label"] else "LOW")
        prev = src_id
        for j, im in enumerate(p["intermediates"], 1):
            nid = f"SRC{i}-INT{j}"
            add_node(nid, "SERVICE_NODE", level="A", subclass=im["type"], label=im["label"],
                     display_label=im["label"], canonical_identity=f"{im['type']}#{sid}",
                     attrs={"on_source_path": True},
                     evidence=[{"kind": "token", "role": "source_path_node",
                                "value": im["label"], "bbox_visual_pt": im["bbox"]}],
                     confidence="MEDIUM")
            add_edge("FEEDS", prev, nid, [{"kind": "geometry", "role": "source_path_order"}])
            prev = nid
        in_id = f"IN{i}"
        add_node(in_id, "INPUT_DEVICE", level="A", label=p["input_label"],
                 display_label=p["input_label"],
                 canonical_identity=f"INPUT#{sid}",
                 attrs={"rating_a": p["input_rating_a"], "control": p["control"]},
                 evidence=[{"kind": "token", "role": "device", "value": p["input_label"],
                            "bbox_visual_pt": p["input_bbox"]}],
                 confidence="HIGH" if p["input_rating_a"] else "MEDIUM")
        add_edge("FEEDS", prev, in_id, [{"kind": "geometry", "role": "source_path_order"}], "HIGH")
        if sid:
            add_edge("FEEDS", in_id, sid, [{"kind": "geometry", "role": "input_to_section"}], "HIGH")

    for d in incoming:
        if d["role"] == "SECTION_DEVICE":
            a, b = d["in_gap"]
            nid = f"TIE:{a}-{b}"
            add_node(nid, "SECTION_DEVICE", level="A", label=d["label"], display_label=d["label"],
                     subclass=("SECTION_SWITCH_DISCONNECTOR" if d["kind"] in ("QS", "ВР", "ВН")
                               else "SECTION_CIRCUIT_BREAKER"),
                     canonical_identity=f"SECTION_TIE#{a}-{b}",
                     attrs={"rating_a": d["rating_a"], "control": d["control"]},
                     evidence=[
                         {"kind": "token", "role": "device", "value": d["label"],
                          "bbox_visual_pt": d["word"].bbox()},
                         {"kind": "geometry", "role": "between_sections",
                          "value": {"left": a, "right": b, "x": round(d["word"].cx, 1)}},
                     ],
                     confidence="HIGH")
            add_edge("TIES_SECTIONS", nid, a, [{"kind": "geometry", "role": "gap_left"}], "HIGH")
            add_edge("TIES_SECTIONS", nid, b, [{"kind": "geometry", "role": "gap_right"}], "HIGH")
        elif d["role"] == "SERVICE_NODE":
            nid = f"SVC:{d['label']}@{round(d['word'].cx)}"
            add_node(nid, "SERVICE_NODE", level="B", label=d["label"], display_label=d["label"],
                     subclass="SURGE_PROTECTION" if d["spd_tokens"] else "FUSE_PROTECTION",
                     canonical_identity=("SPD#" + (d["host_section"] or "?")) if d["spd_tokens"]
                                        else ("FUSE#" + (d["host_section"] or "?")),
                     attrs={"markers": d["spd_tokens"] + d["fuse_tokens"]},
                     evidence=[{"kind": "token", "role": "device", "value": d["label"],
                                "bbox_visual_pt": d["word"].bbox()}]
                              + [{"kind": "token", "role": "protection_marker", "value": t}
                                 for t in d["spd_tokens"] + d["fuse_tokens"]],
                     confidence="MEDIUM")
            if d["host_section"]:
                add_edge("BELONGS_TO_SECTION", nid, d["host_section"],
                         [{"kind": "geometry", "role": "x_within_section"}])
        elif d["role"] == "UNKNOWN_NODE":
            nid = f"UNK:{d['label']}@{round(d['word'].cx)}"
            add_node(nid, "UNKNOWN_NODE", level="B", label=d["label"], display_label=d["label"],
                     canonical_identity=None,
                     attrs={"rating_a": d["rating_a"], "host_section": d["host_section"]},
                     evidence=[{"kind": "token", "role": "device", "value": d["label"],
                                "bbox_visual_pt": d["word"].bbox()}],
                     confidence="LOW")

    for f in feeders:
        nid = f"OUT:{f['device_key']}"
        add_node(nid, "OUTGOING_DEVICE", level="C", label=f["device_label"],
                 display_label=f["display_label"], canonical_identity=f["canonical_identity"],
                 subclass=None, section=f["section"], attrs={
                     "rating_a": f["rating_a"], "cable": f["cable"], "status": f["status"],
                     "identity_set": f["identity_set"]},
                 evidence=f["evidence"], confidence=f["identity_confidence"],
                 conflicts=f["conflicts"])
        add_edge("BELONGS_TO_SECTION", nid, f["section"],
                 [{"kind": "geometry", "role": "column_in_section"}], "HIGH")
        if f["canonical_identity"]:
            lid = f"LOAD:{f['canonical_identity']}@{f['section']}"
            if not any(n["id"] == lid for n in nodes):
                add_node(lid, "LOAD", level="C", label=f["display_label"],
                         display_label=f["display_label"],
                         canonical_identity=f["canonical_identity"], section=f["section"],
                         evidence=[e for e in f["evidence"] if e["kind"] == "token"],
                         confidence=f["identity_confidence"])
            add_edge("FEEDS", nid, lid,
                     [{"kind": "token", "role": "column_identity",
                       "value": f["canonical_identity"]}], f["identity_confidence"])

    for g in groups:
        add_node(g["id"], g["type"], level="B", label=g["id"], display_label=g["id"],
                 canonical_identity=g["id"], section=g["section"],
                 attrs={"member_tokens": g["member_tokens"], "member_count": g["member_count"]},
                 evidence=g["evidence"], confidence="MEDIUM")
        add_edge("BELONGS_TO_SECTION", g["id"], g["section"],
                 [{"kind": "geometry", "role": "x_within_section"}])

    # ── честные раздельные гейты (§40) ──
    n_out = len(feeders)
    ident_ok = sum(1 for f in feeders if f["canonical_identity"])
    unresolved_nodes = sum(1 for n in nodes if n["type"] == "UNKNOWN_NODE"
                           or (n["type"] == "OUTGOING_DEVICE" and not n["canonical_identity"]
                               and n["attrs"]["status"] != "RESERVE"))
    quality = {
        "backbone_recovered": True,
        "source_confidence": round(sum(1 for p in paths if p["source_label"]) / max(len(paths), 1), 3),
        "bus_confidence": round(sum(1 for s in sections if s.get("name_evidence")) / max(len(sections), 1), 3),
        "section_confidence": (1.0 if any(n["type"] == "SECTION_DEVICE" for n in nodes)
                               else (0.0 if len(sections) > 1 else None)),
        "feeder_coverage": round(n_out / max(len(row), 1), 3),
        "identity_coverage": round(ident_ok / max(n_out, 1), 3),
        "unresolved_nodes": unresolved_nodes,
        "unresolved_edges": sum(1 for e in edges if e["confidence"] == "LOW"),
        "section_label_conflicts": len(sec_conflicts),
        "sections": len(sections),
        "inputs": len([p for p in paths]),
        "outgoing_devices": n_out,
    }
    if sec_conflicts:
        warnings.append(f"{len(sec_conflicts)} меток аппаратов противоречат геометрической секции "
                        "(вероятные опечатки CAD) — принята геометрия")
    if quality["identity_coverage"] < 0.8:
        warnings.append(f"идентичность ветвей восстановлена для "
                        f"{quality['identity_coverage']:.0%} отходящих")
    if quality["section_confidence"] == 0.0:
        warnings.append("секций несколько, но секционный аппарат не найден")

    return {
        "schema_version": SCHEMA_VERSION,
        "side": side,
        "block": {"block_id": ev.block_id, "page_index": ev.page_index,
                  "rotation": ev.rotation, "pdf": ev.pdf_path,
                  "bbox_visual_pt": ev.bbox_visual_pt},
        "discipline": "ЭОМ",
        "profile": {k: v for k, v in profile.items() if k in ("id", "signals", "why", "note")},
        "levels": {"A": "источники/вводы/секции/секционник",
                   "B": "функциональные группы", "C": "аппараты и ветви"},
        "nodes": nodes,
        "edges": edges,
        "groups": groups,
        "section_label_conflicts": sec_conflicts,
        "quality": quality,
        "warnings": warnings,
    }


__all__ = ["build_system_graph", "scan_block", "detect_profile", "canonical_identity",
           "SCHEMA_VERSION"]
