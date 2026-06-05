# -*- coding: utf-8 -*-
"""GRSH (ГРЩ/ВРУ) dense single-line feeder extraction — specialized mode.

Default **OFF** (`STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED=false`). When
enabled, a dense ГРЩ single-line block is processed not by one Qwen single-shot
(which compresses the dense scheme into too-poor text) but by:

    block-PDF (crop_url) → text layer (pdfplumber_text) → high-res render
      → overlapping tiles (concurrency=1)
      → per-tile Qwen feeder-JSON (tile-local OCR vocabulary, anti-extrapolation)
      → deterministic merge feeders[] + recall vs text-layer anchors
      → structured feeder table (для enriched MD / Opus)

Принцип: text layer = буквальные значения (denominator + анти-галлюцинация),
Qwen = структура/связи/группировка, backend = merge/recall/validation.

Реализация портирована из проверенного эксперимента
``comparison/qwen_experiments/grsh_pdf_block_feeder_extraction_20260605_142501``
(OLD recall 0.933, NEW recall 1.0, 0 искусственных рядов). Qwen-вызов
инжектируется (``describe_fn``) — модуль не зависит от сети и тестируется
замоканным. Live Qwen зовётся только при включённом флаге из живого pipeline.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


# ─── env ──────────────────────────────────────────────────────────────────


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def grsh_feeder_extraction_enabled() -> bool:
    """Главный включатель режима (default OFF)."""
    return _env_bool("STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED", False)


def grsh_feeder_use_block_pdf() -> bool:
    """Использовать block-PDF (crop_url) как источник рендера (default ON
    внутри режима — но сам режим default OFF)."""
    return _env_bool("STAGE_COMPARISON_GRSH_FEEDER_USE_BLOCK_PDF", True)


@dataclass
class GrshFeederConfig:
    render_long_side: int = 7000
    # 1600 — live-рекомендация после benchmark 2026-06-05: качество как у 2000,
    # но стабильнее по времени (тяжёлый тайл укладывается в read-timeout).
    # 2000 остаётся как override/debug через env.
    tile_long_side: int = 1600
    n_cols: int = 7
    n_rows: int = 2
    tile_w_frac: float = 1500.0 / 7000.0
    row_h_frac: float = 0.66
    max_tiles: int = 16
    max_tokens: int = 9000
    concurrency: int = 1
    min_recall: float = 0.80


def load_grsh_feeder_config() -> GrshFeederConfig:
    return GrshFeederConfig(
        render_long_side=_env_int("STAGE_COMPARISON_GRSH_FEEDER_RENDER_LONG_SIDE", 7000),
        tile_long_side=_env_int("STAGE_COMPARISON_GRSH_FEEDER_TILE_LONG_SIDE", 1600),
        n_cols=_env_int("STAGE_COMPARISON_GRSH_FEEDER_N_COLS", 7),
        n_rows=_env_int("STAGE_COMPARISON_GRSH_FEEDER_N_ROWS", 2),
        max_tiles=_env_int("STAGE_COMPARISON_GRSH_FEEDER_MAX_TILES", 16),
        max_tokens=_env_int("STAGE_COMPARISON_GRSH_FEEDER_MAX_TOKENS", 9000),
        concurrency=max(1, _env_int("STAGE_COMPARISON_GRSH_FEEDER_TILE_CONCURRENCY", 1)),
        min_recall=_env_float("STAGE_COMPARISON_GRSH_FEEDER_MIN_RECALL", 0.80),
    )


# ─── feeder prompt (как в эксперименте) ───────────────────────────────────

GRSH_FEEDER_TILE_PROMPT = r"""Ты читаешь ФРАГМЕНТ (tile) однолинейной схемы ГРЩ (главный распределительный щит) электроснабжения жилого комплекса. Это НЕ вся схема — только вырезанный прямоугольный кусок.

ЗАДАЧА: извлечь СТРОГО то, что РЕАЛЬНО ВИДНО в этом фрагменте. Не достраивай схему, не угадывай соседние фрагменты.

ЖЁСТКИЕ ПРАВИЛА (нарушение = брак):
1. НЕ достраивай ряды. Если видишь ГРЩ1-РП1-1, ГРЩ1-РП1-2 — не добавляй -3..-15. Только реально читаемые номера.
2. НЕ добавляй потребитель/линию, которой нет в этом фрагменте (даже если "должна быть").
3. Не выдумывай типовые номиналы/сечения. Если поле не читается — null + field_state="not_extracted".
4. Если объект виден, но маркировка нечитаема — consumer="[не читается]", confidence<=0.4, занеси в uncertainties.
5. Не переноси текст из штампа/легенды в feeders.
6. OCR_VOCAB ниже — список буквальных надписей, видимых в этом фрагменте (референс). Не считай verified маркировку, которой нет в OCR_VOCAB; помечай её visual_unverified.

ИЗВЛЕКАЙ (то что видно):
- feeders: отходящие линии. Для каждой: потребитель (ВРУ1, ВРУ-ХЦ, ШУ-АПТ, ХМ1, ДР1-ХМ1, АУКРМ №1, ЩНО, Резервные баки ГВС...), обозначение кабельной линии (1ГРЩ-ВРУ4 или ГРЩ1-РП1-1), секция-источник (ГРЩ1 РП1 / ГРЩ1 РП2 / 1ГРЩ / 2ГРЩ), автомат (1QF1, 2QF4, QS1), номинал автомата (3P 800A / 800А / 400А), откл.способность (40кА/50кА), марка кабеля (ППГнг(А)-HF, КППГнг(А)-HF, ПуГПнг(А)-HF), сечение (5х150мм², 2х(5х120), 3х(5х120)), расч.мощность Рр кВт, расч.ток Iр А, режим (рабочий/аварийный/ПП/пожарный), ТТ/коэф (ТШП 1500/5 0.5S, 200/5), учёт (Меркурий 234, Wh).
- connections: связи источник->нагрузка (ТП1->ГРЩ1 РП1 via шинопровод 3L/PEN Al 3200А; Т1->секция; ввод->АВР). Только если связь реально видна линией/подписью.
- equipment: вводы (Ввод 1 к ТП1, Т1 1250кВА), шинопроводы, секции, АВР/ПСВ, УЗИП/ОПН (FU 125А), АУКРМ (Qр кВАр), учёт/анализаторы, мультиметры PW, ТТ-группы, ГЗШ/заземление.
- notes: текст примечаний если виден.

ФОРМАТ ОТВЕТА — ТОЛЬКО JSON, без markdown, без пояснений:
{
 "status":"done",
 "tile_id":"",
 "feeders":[
   {"consumer":"","consumer_normalized":"","designation":null,"source_panel":null,
    "input_side":"input_1|input_2|unknown","breaker":null,"breaker_rating":null,
    "breaking_capacity":null,"cable_mark":null,"cable_section":null,
    "p_calc_kw":null,"i_calc_a":null,"mode":"рабочий|аварийный|ПП|пожарный|unknown",
    "ct_ratio":null,"metering":null,"evidence_text":"",
    "field_state":{"breaker":"present|not_extracted","cable":"present|not_extracted","load":"present|not_extracted"},
    "confidence":0.0}
 ],
 "connections":[{"from":"","to":"","via":null,"evidence_text":"","confidence":0.0}],
 "equipment":[{"name":"","kind":"input|busbar|section|avr|surge|compensation|metering|ct|earthing|other","detail":"","evidence_text":"","confidence":0.0}],
 "notes":[],
 "uncertainties":[{"possible_text":"","why":""}]
}
Если в этом фрагменте нет ничего релевантного — верни status="empty" и пустые массивы. ТОЛЬКО валидный JSON."""


# ─── normalization (как в эксперименте stage6) ────────────────────────────

_LAT2CYR = str.maketrans({
    'A': 'А', 'B': 'В', 'C': 'С', 'E': 'Е', 'H': 'Н', 'K': 'К', 'M': 'М', 'O': 'О',
    'P': 'Р', 'T': 'Т', 'X': 'Х', 'Y': 'У', 'U': 'У', 'V': 'В', 'D': 'Д', 'R': 'Р',
    'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р', 'x': 'х', 'y': 'у'})


def _translit(s: str) -> str:
    return str(s or "").translate(_LAT2CYR)


def norm_desig(s: Optional[str]) -> str:
    if not s:
        return ""
    s = _translit(str(s).upper())
    s = re.sub(r"[ .,·．]", "", s)
    s = re.sub(r"[–—‒-]+", "-", s)
    return s.strip("-")


def norm_consumer(raw: Optional[str]) -> str:
    if not raw:
        return ""
    t = _translit(str(raw).upper())
    t = re.sub(r"[–—‒]", "-", t)

    def has(*subs: str) -> bool:
        return any(s in t for s in subs)

    if has("ЯСН"):
        return "ЯСН-ТП"
    if has("БАКИ ГВС", "РЕЗЕРВНЫЕ БАКИ", "БАКИ"):
        return "Резервные баки ГВС"
    if "АУКРМ" in t or "АKКРМ" in t:
        return "АУКРМ-2" if re.search(r"[№\-\s]?2", t) else "АУКРМ-1"
    if has("ДР2"):
        return "ДР2-ХМ2"
    if has("ДР1"):
        return "ДР1-ХМ1"
    if "ХМ2" in t:
        return "ХМ2"
    if "ХМ1" in t:
        return "ХМ1"
    if has("ШНО", "ЩНО") or has("НАРУЖН"):
        return "ЩНО"
    if has("ИТП"):
        return "ВРУ-ИТП"
    if has("ХЦ", "ХЛАДОЦЕНТР", "ХОЛОДИЛЬН"):
        return "ВРУ-ХЦ"
    if has("АПТ"):
        return "ВРУ-АПТ"
    if has("НСТ", "ХВС", "ХОЗПИТ", "ВОДОСНАБ"):
        return "ВРУ-НСТ"
    if "ХП" in t and "ВРУ" not in t:
        return "ВРУ-НСТ"
    if re.search(r"ВРУ\s*А\b", t) or "ВРУА" in t:
        return "ВРУа"
    m = re.search(r"ВРУ\s*-?\s*([1234])", t)
    if m:
        return f"ВРУ{m.group(1)}"
    return str(raw).strip()


# ─── text-layer anchors (denominator + анти-галлюцинация) ─────────────────

_CONSUMER_DEFS = [
    ("ВРУ1", [r'ВРУ1\b', r'1ГРЩ[\-]ВРУ1\b', r'2ГРЩ[\-]ВРУ1\b']),
    ("ВРУ2", [r'ВРУ2\b', r'ГРЩ[\-]ВРУ2', r'1ГРЩ[\-]ВРУ2', r'2ГРЩ[\-]ВРУ2']),
    ("ВРУ3", [r'ВРУ3\b', r'1ГРЩ[\-]ВРУ3', r'2ГРЩ[\-]ВРУ3']),
    ("ВРУ4", [r'ВРУ4\b', r'1ГРЩ[\-]ВРУ4', r'2ГРЩ[\-]ВРУ4']),
    ("ВРУа", [r'ВРУа\b', r'1ГРЩ[\-]ВРУа', r'ГРЩ[\-]ВРУа']),
    ("ВРУ-ИТП", [r'ВРУ[\-\.]?ИТП', r'ГРЩ[\-]ВРУ\.ИТП']),
    ("ВРУ-ХЦ", [r'ВРУ[\-]?ХЦ', r'[ШЩ]У[\-\.]?Х[ЦП]', r'ШУХЦ']),
    ("ВРУ-АПТ", [r'ВРУ[\-]?АПТ', r'[ШЩ]У[\-\.]?АПТ', r'ЩУ\.АПТ']),
    ("ВРУ-НСТ", [r'ВРУ[\-]?НСТ', r'[ШЩ]У[\-\.]?ХВС', r'ШУ\.ХП']),
    ("ЩНО", [r'ШНО\b', r'ЩНО\b']),
    ("ХМ1", [r'ХМ1\b', r'ГРЩ[\-]ХМ1']),
    ("ХМ2", [r'ХМ2\b']),
    ("ДР1-ХМ1", [r'ДР1[\-]ХМ1', r'ГРЩ[\-]ДР1']),
    ("ДР2-ХМ2", [r'ДР2[\-]ХМ2']),
    ("АУКРМ-1", [r'А[УK]КРМ[\s\-]?[№]?1\b']),
    ("АУКРМ-2", [r'А[УK]КРМ[\s\-]?[№]?2\b']),
    ("Резервные баки ГВС", [r'баки\s*ГВС', r'Резервные\s*баки']),
    ("ЯСН-ТП", [r'ЯСН\s*ТП']),
]


# Серии распределительных/квартирных обозначений и аппаратов, реально
# присутствующие на плотных однолинейных ВРУ/ГРЩ. Whitelist намеренно узкий —
# чтобы НЕ ловить номиналы (380В/25кА/800А/IP31) как обозначения.
#
# КЛЮЧЕВОЕ (2026-06-06): номер серии засчитывается только если ПРИВЯЗАН к марке.
#  * квартирные/распред. ряды (ОДН/АВР/ППУ/ЩО/…) пишутся через ДЕФИС («АВР-35»),
#    поэтому требуют дефис — это отсекает соседний номинал «АВР 250А» (пробел);
#  * аппараты/вводы (QF/QS/ВП/РП) — малые номера прямым примыканием («QF1»,«ВП1»)
#    или дефисом («QF-44»);
#  * negative-lookahead `_NOT_RATING` отсекает прямое примыкание номинала
#    («QF160А» / «АВР250А»): после полного номера не должно идти ещё цифры
#    (иначе backtracking поймал бы «QF16» из «QF160А») или единицы А/В/Р/к/Ω/х/S.
_DASH_SERIES = r'ОДН|АВР|ППУ|ЩО|ЩР|ЩС|НЦВ|НЦГ|НЦ|ЯРП|КНС|КМ'
_ADJ_SERIES = r'QF|QS|ВП|РП'
_SERIES_PREFIX = rf'(?:{_DASH_SERIES}|{_ADJ_SERIES})'
_NOT_RATING = r'(?![\dАAaаВBbвРPpркКΩхxXS])'
_SERIES_DESIG_RE = re.compile(
    rf'((?:\d?ГРЩ\d?[\-])?(?:ВРУ[\-\s]?[А-Я0-9]{{0,3}}[\-\s]?)?'
    rf'(?:(?:{_DASH_SERIES})[\-]|(?:{_ADJ_SERIES})[\-]?))'
    rf'(\d{{1,3}}){_NOT_RATING}(?:[\-]\d{{1,3}})?',
    re.U)
# Серия+номер в КОНЦЕ нормализованного designation (для membership / cap):
# «ВРУ1-ОДН-38» → (ОДН,38); «ОДН-38» → (ОДН,38); «1QF1» → (QF,1).
_SUFFIX_SERIES_RE = re.compile(rf'({_SERIES_PREFIX})[\-]?(\d{{1,3}})$', re.U)


def _suffix_series(norm_d: Optional[str]) -> tuple[Optional[str], Optional[int]]:
    m = _SUFFIX_SERIES_RE.search(norm_d or "")
    if not m:
        return (None, None)
    try:
        return (m.group(1), int(m.group(2)))
    except (TypeError, ValueError):
        return (None, None)


def extract_text_layer_anchors(text: str) -> dict:
    """Якоря из текст-слоя: consumers (canon) + designations + series membership.

    Расширено (2026-06-06): помимо ``ГРЩ-…`` обозначений извлекаются серии
    квартирного/распределительного щита (ОДН/АВР/ППУ/ВП/РП/ЩО/QF…), реально
    присутствующие в текст-слое. Раньше эти серии не ловились → denominator
    recall был 0, а реальные фидеры уходили в rejected_artificial_series.

    Дополнительно возвращает:
      * ``series_nums`` — {series_key: set(int)} реальные номера серий из слоя
        (membership для number-exact верификации);
      * ``series_max`` — {series_key: max(int)} (для cap'а экстраполяции).
    """
    text = text or ""
    consumers = {}
    for canon, pats in _CONSUMER_DEFS:
        if any(re.search(p, text, re.U) for p in pats):
            consumers[canon] = True
    desig = []
    seen = set()
    # 1. существующие ГРЩ кабельные обозначения (поведение без изменений)
    for m in re.findall(r'\d?ГРЩ\d?[\-][А-Яа-яA-Z0-9\.]+(?:[\-]\d+)?', text, re.U):
        nd = norm_desig(m)
        if nd and nd not in seen:
            seen.add(nd)
            desig.append(m)
    # 2. серии квартирного/распределительного щита + аппараты (новое)
    series_nums: dict[str, set] = {}
    for m in _SERIES_DESIG_RE.finditer(text):
        full = m.group(0)
        nd = norm_desig(full)
        if nd and nd not in seen:
            seen.add(nd)
            desig.append(full)
        skey, snum = _suffix_series(nd)
        if skey and snum is not None:
            series_nums.setdefault(skey, set()).add(snum)
    series_max = {k: max(v) for k, v in series_nums.items() if v}
    return {"consumers": sorted(consumers.keys()),
            "consumer_canon": {norm_consumer(c) for c in consumers},
            "cable_designations": desig,
            "designation_norm": {norm_desig(d) for d in desig},
            "series_nums": series_nums,
            "series_max": series_max}


# ─── tiling ───────────────────────────────────────────────────────────────


def make_feeder_tiles(width: int, height: int, cfg: GrshFeederConfig) -> list[dict]:
    """Сетка перекрывающихся tiles (n_cols × n_rows), bbox в render px."""
    n_cols = max(1, cfg.n_cols)
    n_rows = max(1, cfg.n_rows)
    tw = max(1, int(round(width * cfg.tile_w_frac)))
    th = max(1, int(round(height * cfg.row_h_frac)))
    xs = [int(round(i * (width - tw) / max(1, n_cols - 1))) for i in range(n_cols)] if n_cols > 1 else [0]
    ys = [int(round(j * (height - th) / max(1, n_rows - 1))) for j in range(n_rows)] if n_rows > 1 else [0]
    tiles = []
    for ri, y in enumerate(ys):
        for ci, x in enumerate(xs):
            tiles.append({"tile_id": f"r{ri}_c{ci}", "row": ri, "col": ci,
                          "bbox_render_px": [x, y, min(width, x + tw), min(height, y + th)]})
            if len(tiles) >= cfg.max_tiles:
                return tiles
    return tiles


def tile_local_vocabulary(
    text_layer_words: list[dict], tile_bbox_px: list[int],
    *, render_size: tuple[int, int], pdf_page_size: tuple[float, float],
    limit: int = 60,
) -> list[str]:
    """Буквальные надписи текст-слоя, попадающие в bbox этого tile.

    Слова текст-слоя имеют bbox в точках PDF; render — это PDF, масштабированный
    в render px. Переводим bbox слов в render px и фильтруем по tile.
    Если words пусты (текст-слой без координат) — vocabulary пустой (prompt
    работает и без него)."""
    rw, rh = render_size
    pw, ph = pdf_page_size
    if not text_layer_words or pw <= 0 or ph <= 0:
        return []
    sx, sy = rw / pw, rh / ph
    x0, y0, x1, y1 = tile_bbox_px
    out, seen = [], set()
    for w in text_layer_words:
        bb = w.get("bbox")
        if not (isinstance(bb, (list, tuple)) and len(bb) == 4):
            continue
        cx = (bb[0] + bb[2]) / 2 * sx
        cy = (bb[1] + bb[3]) / 2 * sy
        if x0 <= cx <= x1 and y0 <= cy <= y1:
            t = str(w.get("text", "")).strip()
            if t and t.lower() not in seen:
                seen.add(t.lower())
                out.append(t)
                if len(out) >= limit:
                    break
    return out


# ─── per-block extraction (Qwen injected) ─────────────────────────────────

# describe_fn(png_bytes, prompt) -> dict с ключами {parsed: dict|None, status: str}
DescribeFn = Callable[[bytes, str], Awaitable[dict]]


async def extract_feeders_for_block(
    *,
    render_png_bytes: bytes,
    text_layer_words: Optional[list[dict]] = None,
    pdf_page_size: Optional[tuple[float, float]] = None,
    describe_fn: DescribeFn,
    cfg: Optional[GrshFeederConfig] = None,
    crop_fn: Optional[Callable[[bytes, list[int], int], bytes]] = None,
    image_size: Optional[tuple[int, int]] = None,
) -> dict:
    """Прогнать tiled feeder extraction по одному блоку.

    ``crop_fn(png_bytes, bbox_px, tile_long_side) -> tile_png_bytes`` режет tile
    (по умолчанию PIL). ``describe_fn`` — инжектируемый Qwen-вызов. Сетевых
    вызовов модуль сам не делает.
    """
    cfg = cfg or load_grsh_feeder_config()
    crop_fn = crop_fn or _pil_crop_tile
    if image_size is None:
        image_size = _pil_image_size(render_png_bytes)
    W, H = image_size
    tiles = make_feeder_tiles(W, H, cfg)
    sem = asyncio.Semaphore(max(1, cfg.concurrency))

    async def _run_tile(t: dict) -> dict:
        tile_png = crop_fn(render_png_bytes, t["bbox_render_px"], cfg.tile_long_side)
        vocab = tile_local_vocabulary(
            text_layer_words or [], t["bbox_render_px"],
            render_size=(W, H), pdf_page_size=pdf_page_size or (W, H))
        prompt = GRSH_FEEDER_TILE_PROMPT.replace('"tile_id":""', f'"tile_id":"{t["tile_id"]}"')
        if vocab:
            prompt = "OCR_VOCAB (буквальные надписи в этом фрагменте): " + \
                     ", ".join(vocab[:60]) + "\n\n" + prompt
        async with sem:
            res = await describe_fn(tile_png, prompt)
        parsed = res.get("parsed") if isinstance(res, dict) else None
        return {"tile_id": t["tile_id"], "row": t["row"], "col": t["col"],
                "bbox_render_px": t["bbox_render_px"],
                "status": (res or {}).get("status", "error"),
                "tile_local_vocab_count": len(vocab), "parsed": parsed}

    results = await asyncio.gather(*[_run_tile(t) for t in tiles])
    return {"render_size": [W, H], "n_tiles": len(tiles), "tiles": list(results)}


def _pil_image_size(png_bytes: bytes) -> tuple[int, int]:
    import io
    from PIL import Image
    with Image.open(io.BytesIO(png_bytes)) as im:
        return im.size


def _pil_crop_tile(png_bytes: bytes, bbox_px: list[int], tile_long_side: int) -> bytes:
    import io
    from PIL import Image
    with Image.open(io.BytesIO(png_bytes)) as im:
        im = im.convert("RGB")
        crop = im.crop(tuple(bbox_px))
        ls = max(crop.size)
        if ls > tile_long_side:
            sc = tile_long_side / ls
            crop = crop.resize((max(1, int(crop.size[0] * sc)), max(1, int(crop.size[1] * sc))), Image.LANCZOS)
        buf = io.BytesIO()
        crop.save(buf, format="PNG", optimize=True)
        return buf.getvalue()


# ─── merge tile feeders → page-level (как в эксперименте stage6) ───────────


def _first_nonnull(*vals: Any) -> Any:
    for v in vals:
        if v not in (None, "", "null", "unknown"):
            return v
    return None


def merge_tile_feeders(tile_results: dict, anchors: dict, *, cfg: Optional[GrshFeederConfig] = None) -> dict:
    """Слить per-tile feeders в page-level JSON + recall vs text-layer anchors."""
    cfg = cfg or load_grsh_feeder_config()
    ch_desig = set(anchors.get("designation_norm") or set())
    ch_consumer_canon = set(anchors.get("consumer_canon") or set())
    series_nums = anchors.get("series_nums") or {}
    series_max = anchors.get("series_max") or {}

    raw_feeders, raw_conns, raw_equip = [], [], []
    tile_failures = 0
    for t in tile_results.get("tiles", []):
        p = t.get("parsed")
        if not isinstance(p, dict):
            if t.get("status") not in ("done", "empty"):
                tile_failures += 1
            continue
        for f in p.get("feeders", []) or []:
            if isinstance(f, dict):
                f = dict(f)
                f["_tile"] = t["tile_id"]
                raw_feeders.append(f)
        for c in p.get("connections", []) or []:
            if isinstance(c, dict) and (c.get("from") or c.get("to")):
                raw_conns.append(c)
        for e in p.get("equipment", []) or []:
            if isinstance(e, dict) and e.get("name"):
                raw_equip.append(e)

    merged: dict[str, dict] = {}
    for f in raw_feeders:
        des = norm_desig(f.get("designation"))
        cons = norm_consumer(f.get("consumer_normalized") or f.get("consumer"))
        brk = (f.get("breaker") or "").upper().replace(" ", "")
        key = des or (f"{cons}|{brk}" if (cons or brk) else "")
        if not key.strip("|"):
            continue
        if key not in merged:
            merged[key] = {
                "consumer": cons, "designation": f.get("designation"),
                "designation_norm": des or None, "source_panel": f.get("source_panel"),
                "breaker": f.get("breaker"), "breaker_rating": f.get("breaker_rating"),
                "breaking_capacity": f.get("breaking_capacity"),
                "cable_mark": f.get("cable_mark"), "cable_section": f.get("cable_section"),
                "p_calc_kw": f.get("p_calc_kw"), "i_calc_a": f.get("i_calc_a"),
                "ct_ratio": f.get("ct_ratio"), "metering": f.get("metering"),
                "field_state": f.get("field_state", {}), "_tiles": [f.get("_tile")],
            }
        else:
            m = merged[key]
            for fld in ("designation", "source_panel", "breaker", "breaker_rating",
                        "breaking_capacity", "cable_mark", "cable_section",
                        "p_calc_kw", "i_calc_a", "ct_ratio", "metering"):
                m[fld] = _first_nonnull(m.get(fld), f.get(fld))
            m["_tiles"].append(f.get("_tile"))

    rejected_series = []
    for m in merged.values():
        dn = m["designation_norm"]
        cc = norm_consumer(m["consumer"])
        skey, snum = _suffix_series(dn)
        # Over-extrapolation: номер серии выше max-index текст-слоя — выдуманная
        # линия даже на реальном потребителе («ВРУ1-ОДН-46» при max ОДН 44).
        # Перебивает consumer-верификацию.
        over_max = bool(skey and snum is not None and skey in series_max
                        and snum > series_max[skey])
        verified = False
        if not over_max:
            if dn and dn in ch_desig:
                verified = True                                  # точное совпадение нормы
            elif skey and snum is not None and snum in series_nums.get(skey, ()):
                verified = True                                  # number-exact membership серии
            elif cc and cc in ch_consumer_canon:
                verified = True                                  # известный потребитель
            elif dn and skey is None:
                # НЕ-серийные (ГРЩ-кабели и т.п.) — мягкий substring
                verified = any((cd in dn or dn in cd) and len(cd) >= 5 for cd in ch_desig)
        m["anchor_status"] = "verified" if verified else "visual_unverified"
        # Отбраковываем ТОЛЬКО действительно выдуманное: над max-index серии ЛИБО
        # серийноподобное обозначение неизвестной серии. In-range plausible фидеры
        # остаются visual_unverified (НЕ отбраковываются как прежде).
        if not verified and dn:
            if over_max:
                rejected_series.append(dn)
            elif skey and snum is not None:
                pass  # серия известна, номер in-range, но точного членства нет → keep
            elif re.search(r"-\d+$", dn):
                rejected_series.append(dn)

    merged_list = sorted(merged.values(), key=lambda x: (x["consumer"], x["designation_norm"] or ""))
    # recall: сколько ОЖИДАЕМЫХ designations текст-слоя Qwen реально захватил.
    # substring (composite Qwen «ВРУ1-ОДН-38» матчит bare-anchor «ОДН-38»), но
    # number-серии — по точному членству, чтобы «ОДН-46» НЕ матчил «ОДН-4».
    extracted_norms = {m["designation_norm"] for m in merged.values() if m["designation_norm"]}
    extracted_series: dict[str, set] = {}
    for e in extracted_norms:
        ek, en = _suffix_series(e)
        if ek and en is not None:
            extracted_series.setdefault(ek, set()).add(en)

    def _anchor_captured(a: str) -> bool:
        if a in extracted_norms:
            return True
        ak, an = _suffix_series(a)
        if ak and an is not None:
            return an in extracted_series.get(ak, ())
        return any((a in e or e in a) and len(a) >= 5 for e in extracted_norms)

    matched_desig = {a for a in ch_desig if _anchor_captured(a)}
    desig_recall = round(len(matched_desig) / max(1, len(ch_desig)), 3)
    extracted_cons = {norm_consumer(m["consumer"]) for m in merged.values()}
    extracted_cons |= {norm_consumer(e.get("name")) for e in raw_equip if e.get("name")}
    matched_cons = ch_consumer_canon & extracted_cons
    cons_recall = round(len(matched_cons) / max(1, len(ch_consumer_canon)), 3)

    return {
        "sheet_kind": "grsh_singleline",
        "feeders": merged_list, "connections": raw_conns, "equipment": raw_equip,
        "diagnostics": {
            "chandra_expected_designations": len(ch_desig),
            "chandra_expected_consumers": len(ch_consumer_canon),
            "feeders_extracted": len(merged_list),
            "designation_recall": desig_recall, "consumer_recall": cons_recall,
            "matched_consumers": sorted(matched_cons),
            "missing_consumers": sorted(ch_consumer_canon - extracted_cons),
            "missing_text_layer_anchors": sorted(ch_desig - matched_desig),
            "rejected_artificial_series": sorted(set(rejected_series)),
            "connections_count": len(raw_conns), "equipment_count": len(raw_equip),
            "tile_failures": tile_failures, "raw_feeder_rows": len(raw_feeders),
            "meets_min_recall": bool(desig_recall >= cfg.min_recall or cons_recall >= cfg.min_recall),
        },
    }


def render_feeder_table_md(merged: dict) -> str:
    """Структурированная feeder-таблица для enriched MD (вход Opus)."""
    d = merged.get("diagnostics", {})
    lines = ["GRSH_FEEDERS — пофидерная таблица (block-PDF + tiled Qwen, "
             f"designation_recall={d.get('designation_recall')}, "
             f"consumer_recall={d.get('consumer_recall')}):"]
    for f in merged.get("feeders", []):
        parts = [f"потребитель={f.get('consumer')}"]
        if f.get("designation"):
            parts.append(f"линия={f['designation']}")
        if f.get("breaker_rating"):
            parts.append(f"автомат={f.get('breaker')} {f['breaker_rating']}")
        if f.get("cable_mark") or f.get("cable_section"):
            parts.append(f"кабель={f.get('cable_mark') or ''} {f.get('cable_section') or ''}".strip())
        if f.get("p_calc_kw"):
            parts.append(f"Рр={f['p_calc_kw']}кВт")
        if f.get("i_calc_a"):
            parts.append(f"Iр={f['i_calc_a']}А")
        parts.append(f"[{f.get('anchor_status')}]")
        lines.append("- " + " | ".join(str(p) for p in parts))
    return "\n".join(lines)
