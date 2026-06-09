# -*- coding: utf-8 -*-
"""Pipeline V2 — Graphic Block Descriptor (offline, backend-only).

Описывает графические блоки `normalized_document_model` (этап 1) НЕ общими
словами, а структурно и диагностически: что это за блок (схема/план/узел/штамп/
шкаф/однолинейная/структурная), какая дисциплина/система (ЭОМ/СС/СКУД/СОВ/СОТ/
КР/АР/ОВ/ВК), какие первичные токены (оборудование/кабели/питание/этажи/корпуса/
подключения) видны в УЖЕ имеющихся полях, и насколько блок пригоден для
детерминированного diff.

```text
normalized_document_model (+ optional block_matching_report)
  → для каждого графического блока:
        infer_graphic_type / infer_graphic_discipline / infer_graphic_systems
        extract_graphic_tokens / compute_graphic_geometry_metrics
        assess_graphic_diff_readiness
  → graphic_descriptor_report.json
  → [optional] matched_graphic_blocks (совместимость пар по block_matching_report)
```

Это НЕ Qwen/Opus/OCR, НЕ скачивание `crop_url`, НЕ PDF-render, НЕ UI и НЕ diff —
только честная диагностика графики поверх готовых полей. Помогает понять, ПОЧЕМУ
по плотным схемам бывает мало отличий (слабый текст-слой / нет key_entities /
нужна vision-enrichment), и куда блок направить дальше.

Все функции чистые, кроме `write_graphic_descriptor_report` (атомарная запись).
Только stdlib.

См. docs/stage_comparison_pipeline_v2_graphic_block_descriptor.md.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_graphic_block_descriptor"

_LARGE_AREA_RATIO = 0.45
_WS_RE = re.compile(r"\s+")

# ─── нормализация ───────────────────────────────────────────────────────────


def _norm(value: Any) -> str:
    s = "" if value is None else str(value)
    s = unicodedata.normalize("NFKC", s).lower().replace("ё", "е")
    return _WS_RE.sub(" ", s).strip()


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe(value: Any) -> str:
    s = _clean(value) or "na"
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in s)[:48]


# ─── паттерны токенов ────────────────────────────────────────────────────────

_CABLE_RE = re.compile(
    r"utp|ftp|кпсввнг(?:\([а-я]+\))?|кпсвв|lan|cat\.?\s?5e|cat\.?\s?6|frls|lsltx|"
    r"ввгнг(?:\([а-я]+\))?|ввг|вок|\bнг\b|\bls\b|\bhf\b", re.IGNORECASE)

_POWER_VOLT_RE = re.compile(r"\d+(?:[.,]\d+)?\s*в\b", re.IGNORECASE)
_POWER_CURR_RE = re.compile(r"\d+(?:[.,]\d+)?\s*а\b", re.IGNORECASE)
_POWER_UPS_RE = re.compile(r"\bибп\b", re.IGNORECASE)
_POWER_CAT_RE = re.compile(r"\b[iv]+\s*-?\s*(?:я\s+)?категори\w*", re.IGNORECASE)

_FLOOR_RE = re.compile(r"(-?\d+)\s*этаж", re.IGNORECASE)
_FLOOR_WORD_RE = re.compile(r"(последний|цокольн\w*|подвальн\w*|техническ\w*)\s*этаж",
                            re.IGNORECASE)
_LOCATION_KW = ("уэрм", "паркинг", "хладоцентр", "итп", "венткамер", "электрощитов",
                "помещени")
_LOCATION_CONTAINER_RE = re.compile(
    r"(корпус|секци\w*|блок)\s*[№n]?\s*([0-9]+(?:[.,][0-9]+)?|[а-я])", re.IGNORECASE)

_EQUIP_KW = (
    "шк.свн", "шк свн", "poe-коммутатор", "poe коммутатор", "рое-коммутатор",
    "коммутатор", "видеорегистратор", "домовой регистратор", "кросс",
    "патч-панель", "патч панель", "контроллер", "считыватель", "вызывная панель",
    "ибп", "грщ", "вру", "авр", "шинопровод",
)
_EQUIP_TOKEN_RE = re.compile(
    r"\bшк\.?\s?свн\s?\d*\b|\bqf\d*\b|\bqs\d*\b|\bтд\b|\bбп\b|\bpw\b", re.IGNORECASE)

_CONNECTION_KW = ("подключается", "подключение", "подключен", "ethernet", "соединя")
_CONNECTION_PHRASE_RE = re.compile(r"(?:^|\s)(к|от)\s+[a-zа-я0-9.\-]{2,}", re.IGNORECASE)
_INPUT_RE = re.compile(r"ввод\s*~?\s*\d+(?:[.,]\d+)?\s*в", re.IGNORECASE)

# системы
_SYSTEM_PATTERNS = (
    ("СКУД", (r"\bскуд\b", "контроль доступа", "считыватель", "вызывная панель",
              "электромагнитн")),
    ("СОТ", (r"\bсот\b", "видеонаблюден", r"\bкамер", "видеорегистратор",
             r"\bсвн\b", "шк.свн", "шк свн")),
    ("СОВ", (r"\bсов\b", "охрана входов", "домофон")),
    ("СС", ("слаботочн", "сети связи", "втсс", "патч-панель", "патч панель",
            "кросс", "rj45")),
    ("ЭОМ", (r"\bэом\b", r"\bгрщ\b", r"\bвру\b", "шинопровод", r"\bавр\b")),
)

_EOM_MARKERS = (r"\bгрщ\b", r"\bвру\b", r"\bqf\b", r"\bqs\b", r"\bавр\b",
                "шинопровод", "квт", r"\bква\b")
_KR_MARKERS = ("армировани", "бетон", "сечени", "колонн", "монолит", "плита перекрыти")
_AR_MARKERS = ("экспликаци", "фасад", r"\bось\b", r"\bоси\b")
_OV_MARKERS = ("вентиляц", "отоплени", "воздуховод")
_VK_MARKERS = ("водоснабж", "канализаци", "трубопровод", "водоотвед")


def _has_any(blob: str, patterns) -> bool:
    for p in patterns:
        if p.startswith("\\b") or p.endswith("\\b") or "\\" in p:
            if re.search(p, blob):
                return True
        elif p in blob:
            return True
    return False


# ─── сбор текста блока ───────────────────────────────────────────────────────


def _summary(block: dict) -> dict:
    s = block.get("ocr_json_summary")
    return s if isinstance(s, dict) else {}


def _key_entities(block: dict) -> list[str]:
    ke = _summary(block).get("key_entities")
    return [str(x) for x in ke if _clean(x)] if isinstance(ke, list) else []


def _sheet_name(block: dict, page: Optional[dict]) -> str:
    if page and _clean(page.get("sheet_name")):
        return _clean(page.get("sheet_name"))
    sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
    return _clean(sd.get("sheet_name"))


def _document_code(block: dict, page: Optional[dict]) -> str:
    if page and _clean(page.get("document_code")):
        return _clean(page.get("document_code"))
    sd = block.get("stamp_data") if isinstance(block.get("stamp_data"), dict) else {}
    return _clean(sd.get("document_code"))


def _scan_text(block: dict, page: Optional[dict]) -> str:
    """Текст для regex-сканеров токенов — БЕЗ key_entities (их не фрагментируем:
    они уже структурированы и классифицируются отдельно)."""
    summ = _summary(block)
    parts = [
        _sheet_name(block, page),
        _clean(summ.get("content_summary")),
        _clean(summ.get("detailed_description")),
        _clean(block.get("text_excerpt")),
        _clean(block.get("pdfplumber_text_excerpt")),
    ]
    return _norm(" ".join(p for p in parts if p))


def _text_blob(block: dict, page: Optional[dict]) -> str:
    """Полный blob (включая key_entities) — для type/discipline/systems."""
    parts = [_scan_text(block, page), _document_code(block, page),
             " ".join(_key_entities(block))]
    return _norm(" ".join(p for p in parts if p))


# ─── что считать графическим блоком ─────────────────────────────────────────


def _is_graphic_block(block: dict) -> bool:
    if block.get("block_type") == "image":
        return True
    if block.get("semantic_type") in ("scheme", "large_scheme", "plan"):
        return True
    if block.get("crop_url") or block.get("has_crop_pdf"):
        return True
    blob = _text_blob(block, None)
    if any(k in blob for k in ("схема", "план", "чертеж")):
        return True
    return False


# ─── infer_graphic_type ──────────────────────────────────────────────────────


def infer_graphic_type(block: dict, page: Optional[dict] = None) -> str:
    blob = _text_blob(block, page)
    st = block.get("semantic_type")
    geom = compute_graphic_geometry_metrics(block, page)
    area = geom["area_ratio"]

    # 1) штамп
    if st == "stamp" or ((("штамп" in blob) or ("основная надпись" in blob)) and area < 0.12):
        return "stamp"
    # 2) легенда
    if any(k in blob for k in ("условные обозначения", "перечень обозначен", "символ ")):
        return "legend"
    # 3) таблица-схема
    if ("|" in _clean(block.get("text_excerpt")) or "|" in _clean(block.get("pdfplumber_text_excerpt"))
            or any(k in blob for k in ("спецификаци", "ведомост", "таблица"))):
        return "table_scheme"
    # 4) шкаф
    if any(k in blob for k in ("шкаф", "патч-панель", "патч панель", "кросс", "rj45", "портов")):
        return "cabinet_scheme"
    # 5) однолинейная
    if _has_any(blob, (r"\bгрщ\b", r"\bвру\b", r"\bqf\b", r"\bqs\b", r"\bавр\b",
                       "фидер", "шинопровод", "однолинейн", "ква", "квт")):
        return "single_line_scheme"
    # 6) структурная
    if any(k in blob for k in ("структурная схема", "схема сот", "схема скуд",
                               "схема сов", "шк.свн", "шк свн", "уэрм", "коммутатор")) \
            or _has_any(blob, (r"\bсов\b", r"\bсот\b", r"\bскуд\b")):
        return "structural_scheme"
    # 7) подключения
    if any(k in blob for k in ("подключени", "подключается", "ethernet")) \
            or _INPUT_RE.search(blob):
        return "connection_scheme"
    # 8) план
    if any(k in blob for k in ("план", "помещени", "этаж")) or _FLOOR_RE.search(blob):
        return "plan"
    # 9) fallback по semantic_type
    if st == "plan":
        return "plan"
    return "unknown"


# ─── infer_graphic_systems / discipline ─────────────────────────────────────


def infer_graphic_systems(block: dict, page: Optional[dict] = None) -> list[str]:
    blob = _text_blob(block, page)
    out: list[str] = []
    for name, patterns in _SYSTEM_PATTERNS:
        if _has_any(blob, patterns) and name not in out:
            out.append(name)
    return out


def infer_graphic_discipline(block: dict, page: Optional[dict] = None) -> str:
    blob = _text_blob(block, page)
    systems = infer_graphic_systems(block, page)
    specific_lv = [s for s in systems if s in ("СКУД", "СОВ", "СОТ")]
    broad_lv = any(s in systems for s in ("СС", "СКУД", "СОВ", "СОТ"))

    if len(specific_lv) == 1 and "СС" not in systems:
        return {"СКУД": "SKUD", "СОВ": "SOV", "СОТ": "SOT"}[specific_lv[0]]
    if broad_lv:
        return "SS"
    if "ЭОМ" in systems or _has_any(blob, _EOM_MARKERS):
        return "EOM"
    if any(k in blob for k in _KR_MARKERS):
        return "KR"
    if any(k in blob for k in _OV_MARKERS):
        return "OV"
    if any(k in blob for k in _VK_MARKERS):
        return "VK"
    if "план" in blob or _has_any(blob, _AR_MARKERS) or block.get("semantic_type") == "plan":
        return "AR"
    return "unknown"


# ─── classify_graphic_token / extract_graphic_tokens ────────────────────────


def classify_graphic_token(token: Any) -> str:
    """Бакет одного токена: cable|power|connection_hint|floor|system|location|
    equipment|unknown."""
    t = _norm(token)
    if not t:
        return "unknown"
    if _CABLE_RE.search(t):
        return "cable"
    if (_POWER_VOLT_RE.search(t) or _POWER_CURR_RE.search(t)
            or _POWER_UPS_RE.search(t) or _POWER_CAT_RE.search(t)):
        return "power"
    if (any(k in t for k in _CONNECTION_KW) or _INPUT_RE.search(t)
            or re.match(r"^(к|от)\s+\S", t)):
        return "connection_hint"
    if _FLOOR_RE.search(t) or _FLOOR_WORD_RE.search(t):
        return "floor"
    for name, patterns in _SYSTEM_PATTERNS:
        if _has_any(t, patterns):
            # «система» как самостоятельный токен (СКУД/СОВ/СОТ/…)
            if t in ("скуд", "сов", "сот", "свн", "втсс", "сс"):
                return "system"
    if _LOCATION_CONTAINER_RE.search(t) or any(k in t for k in _LOCATION_KW):
        return "location"
    if any(k in t for k in _EQUIP_KW) or _EQUIP_TOKEN_RE.search(t):
        return "equipment"
    return "unknown"


def _dedup(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        x = _clean(x)
        if not x:
            continue
        k = _norm(x)
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _canon_power(token: str) -> str:
    return _WS_RE.sub("", _clean(token))


def extract_graphic_tokens(block: dict, page: Optional[dict] = None) -> dict:
    """Первичные токены графического блока по уже имеющимся полям."""
    blob = _scan_text(block, page)  # regex-сканеры по тексту БЕЗ key_entities
    raw_key_entities = _key_entities(block)

    equipment: list[str] = []
    cables: list[str] = []
    power: list[str] = []
    locations: list[str] = []
    floors: list[str] = []
    connection_hints: list[str] = []

    # 1) key_entities → buckets
    for ke in raw_key_entities:
        bucket = classify_graphic_token(ke)
        if bucket == "cable":
            cables.append(ke)
        elif bucket == "power":
            power.append(_canon_power(ke))
        elif bucket == "connection_hint":
            connection_hints.append(ke)
        elif bucket == "floor":
            floors.append(ke)
        elif bucket == "location":
            locations.append(ke)
        elif bucket == "equipment":
            equipment.append(ke)

    # 2) text-scan по всему blob
    cables += [m.group(0) for m in _CABLE_RE.finditer(blob)]
    for rx in (_POWER_VOLT_RE, _POWER_CURR_RE):
        power += [_canon_power(m.group(0)) for m in rx.finditer(blob)]
    if _POWER_UPS_RE.search(blob):
        power.append("ИБП")
    for m in _POWER_CAT_RE.finditer(blob):
        power.append(_WS_RE.sub(" ", m.group(0)).strip())
    for kw in _EQUIP_KW:
        if kw in blob:
            equipment.append(kw)
    equipment += [m.group(0) for m in _EQUIP_TOKEN_RE.finditer(blob)]
    for m in _FLOOR_RE.finditer(blob):
        floors.append(f"{m.group(1)} этаж")
    for m in _FLOOR_WORD_RE.finditer(blob):
        floors.append(m.group(0).strip())
    for m in _LOCATION_CONTAINER_RE.finditer(blob):
        locations.append(_WS_RE.sub(" ", m.group(0)).strip())
    for kw in _LOCATION_KW:
        if kw in blob:
            locations.append(kw)
    for kw in _CONNECTION_KW:
        if kw in blob:
            connection_hints.append(kw)
    for m in _CONNECTION_PHRASE_RE.finditer(blob):
        connection_hints.append(_WS_RE.sub(" ", m.group(0)).strip())
    for m in _INPUT_RE.finditer(blob):
        connection_hints.append(_WS_RE.sub(" ", m.group(0)).strip())

    return {
        "equipment": _dedup(equipment),
        "cables": _dedup(cables),
        "power": _dedup(power),
        "locations": _dedup(locations),
        "floors": _dedup(floors),
        "systems": infer_graphic_systems(block, page),
        "connection_hints": _dedup(connection_hints),
        "raw_key_entities": _dedup(raw_key_entities),
    }


# ─── geometry ────────────────────────────────────────────────────────────────


def compute_graphic_geometry_metrics(block: dict, page: Optional[dict] = None) -> dict:
    cn = block.get("coords_norm") or []
    area_ratio = 0.0
    aspect_ratio = 0.0
    w = h = 0.0
    if isinstance(cn, list) and len(cn) >= 4:
        try:
            w = abs(float(cn[2]) - float(cn[0]))
            h = abs(float(cn[3]) - float(cn[1]))
        except (TypeError, ValueError):
            w = h = 0.0
    if w <= 0 or h <= 0:
        cp = block.get("coords_px") or []
        pw = float((page or {}).get("width") or 0)
        ph = float((page or {}).get("height") or 0)
        if isinstance(cp, list) and len(cp) >= 4 and pw > 0 and ph > 0:
            try:
                w = abs(float(cp[2]) - float(cp[0])) / pw
                h = abs(float(cp[3]) - float(cp[1])) / ph
            except (TypeError, ValueError):
                w = h = 0.0
    if w > 0 and h > 0:
        area_ratio = max(0.0, min(1.0, w * h))
        aspect_ratio = round(w / h, 4) if h > 0 else 0.0
    is_large = area_ratio >= _LARGE_AREA_RATIO or block.get("semantic_type") == "large_scheme"
    shape = (block.get("shape_type") or "").lower()
    complex_shape = shape == "polygon"
    return {
        "coords_norm": list(cn) if isinstance(cn, list) else [],
        "area_ratio": round(area_ratio, 4),
        "aspect_ratio": aspect_ratio,
        "is_large_block": bool(is_large),
        "has_polygon_or_complex_shape": bool(complex_shape),
    }


# ─── diff readiness ──────────────────────────────────────────────────────────


def _meaningful_count(tokens: dict) -> int:
    return (len(tokens.get("equipment", [])) + len(tokens.get("cables", []))
            + len(tokens.get("power", [])) + len(tokens.get("connection_hints", [])))


def assess_graphic_diff_readiness(block: dict, descriptor: dict,
                                  page: Optional[dict] = None) -> dict:
    src = descriptor.get("sources", {})
    tokens = descriptor.get("tokens", {})
    has_crop = bool(src.get("has_crop_url") or src.get("has_image_file"))
    has_text_layer = bool(src.get("has_pdfplumber_text") or src.get("has_ocr_json_summary"))
    has_keys = bool(src.get("has_key_entities"))
    meaningful = _meaningful_count(tokens)
    st = block.get("semantic_type")

    reasons: list[str] = []
    score = 0.0
    if has_crop:
        score += 0.25
        reasons.append("has_crop_or_image_file")
    if has_text_layer:
        score += 0.20
        reasons.append("has_text_layer_or_summary")
    if has_keys:
        score += 0.20
        reasons.append("has_key_entities")
    if meaningful >= 3:
        score += 0.25
        reasons.append("multiple_meaningful_tokens")
    elif meaningful >= 1:
        score += 0.10
        reasons.append("few_meaningful_tokens")
    if st in ("scheme", "large_scheme", "plan"):
        score += 0.10
        reasons.append("semantic_scheme_or_plan")
    score = round(min(1.0, score), 3)

    # not_usable: image-блок без источников и токенов
    not_usable = (not has_crop and not has_text_layer and meaningful == 0 and not has_keys)
    if not_usable:
        readiness = "not_usable"
        reasons.append("no_sources_no_tokens")
    elif score >= 0.70:
        readiness = "high"
    elif score >= 0.45:
        readiness = "medium"
    else:
        readiness = "low"

    if readiness == "high":
        nxt = "deterministic_diff"
    elif readiness == "medium":
        nxt = "entity_extraction"
    elif readiness == "low":
        nxt = "vision_enrichment" if has_crop else "manual_review"
    else:
        nxt = "vision_enrichment" if has_crop else "manual_review"

    return {
        "usable_for_diff": readiness in ("high", "medium"),
        "readiness": readiness,
        "score": score,
        "reasons": reasons,
        "recommended_next_step": nxt,
    }


# ─── quality flags ───────────────────────────────────────────────────────────


def _quality_flags(block: dict, descriptor: dict) -> list[str]:
    src = descriptor.get("sources", {})
    tokens = descriptor.get("tokens", {})
    geom = descriptor.get("geometry", {})
    readiness = descriptor.get("diff_readiness", {})
    meaningful = _meaningful_count(tokens)
    flags: list[str] = []

    if not (src.get("has_crop_url") or src.get("has_image_file")):
        flags.append("graphic_without_crop")
    if not (src.get("has_pdfplumber_text") or src.get("has_ocr_json_summary")):
        flags.append("graphic_without_text_layer")
    if not src.get("has_key_entities"):
        flags.append("graphic_without_key_entities")
    if meaningful < 2:
        flags.append("low_token_count")
    if descriptor.get("graphic_type") == "stamp":
        flags.append("stamp_like_graphic")
    if descriptor.get("graphic_type") == "unknown":
        flags.append("unknown_graphic_type")
    if descriptor.get("discipline") == "unknown":
        flags.append("unknown_discipline")
    if readiness.get("recommended_next_step") == "vision_enrichment":
        flags.append("needs_vision_enrichment")
    if readiness.get("recommended_next_step") == "manual_review" \
            or readiness.get("readiness") == "not_usable":
        flags.append("manual_review_recommended")
    if geom.get("is_large_block") and (meaningful < 3 or not src.get("has_key_entities")):
        flags.append("large_dense_graphic")
    return sorted(set(flags))


# ─── single block descriptor ────────────────────────────────────────────────


def describe_graphic_block(block: dict, page: Optional[dict] = None,
                           options: Optional[dict] = None) -> dict:
    options = options or {}
    side = options.get("side", "unknown")
    page = page or {}

    summ = _summary(block)
    sources = {
        "has_crop_url": bool(block.get("crop_url") or block.get("has_crop_pdf")),
        "has_image_file": bool(block.get("image_file") or block.get("has_image_file")),
        "has_pdfplumber_text": bool(block.get("pdfplumber_text_excerpt")
                                    or block.get("has_pdfplumber_text")),
        "has_ocr_json_summary": bool(summ),
        "has_key_entities": bool(summ.get("key_entities")),
    }
    tokens = extract_graphic_tokens(block, page)
    geom = compute_graphic_geometry_metrics(block, page)
    block_id = block.get("block_id")
    sp = side[0] if side in ("left", "right") else "x"

    descriptor = {
        "descriptor_id": f"gdesc_{sp}_{_safe(block_id)}",
        "block_id": block_id,
        "page_number": block.get("page_number"),
        "sheet_name": _sheet_name(block, page),
        "document_code": _document_code(block, page),
        "block_type": block.get("block_type"),
        "semantic_type": block.get("semantic_type"),
        "graphic_type": infer_graphic_type(block, page),
        "discipline": infer_graphic_discipline(block, page),
        "systems": infer_graphic_systems(block, page),
        "geometry": geom,
        "sources": sources,
        "tokens": tokens,
        "counts": {
            "equipment": len(tokens["equipment"]),
            "cables": len(tokens["cables"]),
            "power": len(tokens["power"]),
            "connection_hints": len(tokens["connection_hints"]),
            "raw_key_entities": len(tokens["raw_key_entities"]),
        },
    }
    descriptor["diff_readiness"] = assess_graphic_diff_readiness(block, descriptor, page)
    descriptor["quality_flags"] = _quality_flags(block, descriptor)
    return descriptor


def describe_graphic_blocks(model: dict, options: Optional[dict] = None) -> list[dict]:
    """Descriptors для всех графических блоков одной модели."""
    model = model or {}
    pages = {p.get("page_number"): p for p in (model.get("pages") or [])}
    out: list[dict] = []
    for bid, block in (model.get("blocks") or {}).items():
        if not isinstance(block, dict) or not _is_graphic_block(block):
            continue
        page = pages.get(block.get("page_number"))
        out.append(describe_graphic_block(block, page, options))
    return out


# ─── matched graphic blocks ──────────────────────────────────────────────────


def _jaccard(a: list[str], b: list[str]) -> float:
    sa = {_norm(x) for x in (a or []) if _clean(x)}
    sb = {_norm(x) for x in (b or []) if _clean(x)}
    if not sa and not sb:
        return 0.0
    union = len(sa | sb)
    return round(len(sa & sb) / union, 4) if union else 0.0


def _descriptors_by_block_id(model: dict, side: str, options: Optional[dict]) -> dict:
    opts = dict(options or {})
    opts["side"] = side
    return {d["block_id"]: d for d in describe_graphic_blocks(model, opts)}


def describe_matched_graphic_blocks(left_model: dict, right_model: dict,
                                    block_matching_report: Optional[dict],
                                    options: Optional[dict] = None) -> list[dict]:
    """Оценить совместимость графических пар по block_matching_report.

    НЕ делает diff — только graphic_type/discipline match + token overlap +
    risk_flags готовности к будущему graphic diff.
    """
    report = block_matching_report or {}
    left_desc = _descriptors_by_block_id(left_model or {}, "left", options)
    right_desc = _descriptors_by_block_id(right_model or {}, "right", options)

    out: list[dict] = []
    for bm in report.get("block_matches") or []:
        lbid = bm.get("left_block_id")
        rbid = bm.get("right_block_id")
        ld = left_desc.get(lbid)
        rd = right_desc.get(rbid)
        # хотя бы одна сторона должна быть графической
        if ld is None and rd is None:
            continue

        risk: list[str] = []
        if ld is None or rd is None:
            risk.append("missing_descriptor")
        conf = bm.get("confidence")
        if conf == "weak":
            risk.append("weak_block_match")

        gtype_match = bool(ld and rd and ld["graphic_type"] == rd["graphic_type"])
        disc_match = bool(ld and rd and ld["discipline"] == rd["discipline"])
        if ld and rd and not gtype_match:
            risk.append("graphic_type_mismatch")
        if ld and rd and not disc_match:
            risk.append("discipline_mismatch")

        overlap = {}
        if ld and rd:
            for bucket in ("equipment", "cables", "power", "locations"):
                overlap[bucket] = _jaccard(ld["tokens"].get(bucket, []),
                                           rd["tokens"].get(bucket, []))
            present = [v for k, v in overlap.items()
                       if (ld["tokens"].get(k) or rd["tokens"].get(k))]
            if present and (sum(present) / len(present)) < 0.30:
                risk.append("low_token_overlap")
        else:
            overlap = {"equipment": 0.0, "cables": 0.0, "power": 0.0, "locations": 0.0}

        if (ld and not ld["diff_readiness"]["usable_for_diff"]) or \
                (rd and not rd["diff_readiness"]["usable_for_diff"]):
            risk.append("one_side_not_usable")

        out.append({
            "block_match_id": bm.get("match_id"),
            "left_descriptor_id": ld["descriptor_id"] if ld else None,
            "right_descriptor_id": rd["descriptor_id"] if rd else None,
            "left_block_id": lbid,
            "right_block_id": rbid,
            "match_quality": conf if conf in ("strong", "medium", "weak") else "weak",
            "graphic_type_match": gtype_match,
            "discipline_match": disc_match,
            "token_overlap": overlap,
            "risk_flags": sorted(set(risk)),
        })
    return out


# ─── report ──────────────────────────────────────────────────────────────────


def build_graphic_descriptor_report(model: dict, block_matching_report: Optional[dict] = None,
                                    side: Optional[str] = None,
                                    options: Optional[dict] = None) -> dict:
    """Полный отчёт descriptors для модели (+ optional matched_graphic_blocks,
    если в ``options['counterpart_model']`` передана вторая модель)."""
    model = model or {}
    side = side or "left"
    opts = dict(options or {})
    opts["side"] = side

    descriptors = describe_graphic_blocks(model, opts)

    matched: list[dict] = []
    counterpart = opts.get("counterpart_model")
    if block_matching_report and isinstance(counterpart, dict):
        if side == "right":
            matched = describe_matched_graphic_blocks(counterpart, model, block_matching_report, options)
        else:
            matched = describe_matched_graphic_blocks(model, counterpart, block_matching_report, options)

    by_type: Counter = Counter(d["graphic_type"] for d in descriptors)
    by_disc: Counter = Counter(d["discipline"] for d in descriptors)
    by_ready: Counter = Counter(d["diff_readiness"]["readiness"] for d in descriptors)
    usable_total = sum(1 for d in descriptors if d["diff_readiness"]["usable_for_diff"])
    vision_total = sum(1 for d in descriptors if "needs_vision_enrichment" in d["quality_flags"])
    manual_total = sum(1 for d in descriptors if "manual_review_recommended" in d["quality_flags"])

    warnings: list[str] = []
    not_usable = by_ready.get("not_usable", 0)
    if not_usable:
        warnings.append(f"graphic_blocks_not_usable: {not_usable}")
    if vision_total:
        warnings.append(f"graphic_blocks_need_vision_enrichment: {vision_total}")
    unknown_type = by_type.get("unknown", 0)
    if unknown_type:
        warnings.append(f"unknown_graphic_type: {unknown_type}")

    document = model.get("document") or {}
    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "document": {
            "document_code": document.get("document_code", ""),
            "pages_total": len(model.get("pages") or []),
            "blocks_total": len(model.get("blocks") or {}),
        },
        "summary": {
            "graphic_blocks_total": len(descriptors),
            "descriptors_total": len(descriptors),
            "usable_for_diff_total": usable_total,
            "needs_vision_enrichment_total": vision_total,
            "manual_review_recommended_total": manual_total,
            "by_graphic_type": dict(by_type),
            "by_discipline": dict(by_disc),
            "by_readiness": dict(by_ready),
            "warnings_count": len(warnings),
        },
        "descriptors": descriptors,
        "matched_graphic_blocks": matched,
        "warnings": warnings,
    }


def write_graphic_descriptor_report(out_path: str | Path, report: dict) -> Path:
    """Атомарно записать отчёт descriptors в JSON-файл (tmp + ``os.replace``)."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, out_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return out_path


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "describe_graphic_blocks",
    "describe_matched_graphic_blocks",
    "describe_graphic_block",
    "infer_graphic_type",
    "infer_graphic_discipline",
    "infer_graphic_systems",
    "extract_graphic_tokens",
    "classify_graphic_token",
    "compute_graphic_geometry_metrics",
    "assess_graphic_diff_readiness",
    "build_graphic_descriptor_report",
    "write_graphic_descriptor_report",
]
