# -*- coding: utf-8 -*-
"""Pipeline V2 — Graphic Vision Enrichment (offline, runner injectable).

Слой описания графических блоков ПОСЛЕ Visual Equivalence Gate:

```text
visual_equivalence_gate_report.json
  → select_blocks_for_vision      (только send_to_vision / manual_review)
  → build_graphic_vision_enrichment_plan   (items + prompt + crop refs)
  → run_graphic_vision_enrichment(vision_runner=…)
  → graphic_vision_enrichment_report.json
```

Ключевые принципы:

* ``exclude_from_vision`` НЕ отправляется в vision: визуальная идентичность
  после выравнивания — самое сильное свидетельство «не менялось», vision на
  таких блоках генерирует только description-variance (ложные дельты);
* ``send_to_vision`` (видимое изменение) и ``manual_review`` (анти-dilution /
  неуверенность gate) — кандидаты на vision-описание;
* vision runner ИНЪЕКТИРУЕТСЯ (контракт ``vision_runner(prompt,
  left_image_path, right_image_path, options) -> dict``); модуль сам НЕ
  импортирует vision-модели/провайдеров и НЕ делает сетевых вызовов.
  ``vision_runner=None`` → ``skipped_no_runner``: кандидаты выбраны,
  prompt/crop refs записаны, реальных вызовов нет;
* fail-soft: ошибка одного item не валит отчёт; отсутствие visual gate →
  ``skipped_no_visual_gate``, а не исключение.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

from backend.app.services.stage_comparison.pipeline_v2_entity_mapping_overrides import (
    find_override_for_pair,
    index_overrides_for_lookup,
)

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_graphic_vision_enrichment"

VisionRunner = Callable[[str, Optional[str], Optional[str], dict], Any]

_DEFAULT_OPTIONS = {
    "enabled": False,               # в dry-run слой по умолчанию выключен
    "max_items": 5,
    "include_manual_review": True,
    "include_exclude_from_vision": False,
    "write_prompts": True,
    "render_crops": True,           # рендерить PNG-кропы перед вызовом runner
    "render_long_side": 1600,
    "runner_model": "fake",
    # отбор кандидатов: legacy (как было) | entity_aware (scoring v2)
    "candidate_selection": "legacy",
    # entity_aware режимы: enrichment (одна сущность) | link_validation
    # (целенаправленная проверка подозрительных связей)
    "selection_mode": "enrichment",
    "exclude_mismatch_likely": True,    # только для enrichment
    # manual entity mapping overrides → candidate selection (mark-only,
    # default OFF — старое поведение). manual_mapping_mode = в каких
    # selection_mode применять override'ы: enrichment | link_validation | both.
    "use_entity_mapping_overrides": False,
    "manual_mapping_mode": "both",
    "include_confirmed_reorganized": False,
    "manual_mapping_debug": False,      # link_validation: пускать rejected_mapping
}

# render-опции (options["render"]): high_res поднимает long_side для
# плотных типов графики; tiled режет dense-схему на перекрывающиеся плитки и
# гонит vision по каждой паре плиток (мелкие номиналы — см. доку)
_DEFAULT_RENDER_OPTIONS = {
    "mode": "normal",               # normal | high_res | tiled
    "long_side": 1600,
    "dense_long_side": 2400,
    "tile_long_side": 1400,         # целевая длинная сторона ОДНОЙ плитки
    "max_tiles": 6,
    "tile_overlap": 0.12,           # доля перекрытия соседних плиток
    "include_full_image": True,     # сохранять full-crop refs рядом с плитками
}
_DENSE_GRAPHIC_TYPES = {"cabinet_scheme", "single_line_scheme",
                        "dense_scheme", "table_scheme"}

# домены инженерных систем для детекта подмены сущности (pilot v2: legend
# ОЗДС/20кВ/300В ↔ схема квартирных ящиков ШК/ВРУ/Меркурий — РАЗНЫЕ домены,
# хотя обе «схема» и обе содержат bare «вру»). Маркеры буквальные, lower.
_DOMAIN_MARKERS = {
    "security_ozds": ("оздс", "бву", "бпи", "охранно", "охранн"),
    "fire_alarm": ("спс", "соуэ", "пожарн", "дымоуд"),
    "medium_voltage": ("20кв", "10кв", "6кв", "ктп", "ру-10", "ру-20"),
    "apartment_power": ("квартир", "щк-", "уэрм", "яур", "меркурий"),
    "lighting": ("освещен", "светильник"),
    "grounding": ("молниезащит", "заземлен", "гзш"),
}

CANDIDATE_SAME = "same_entity_likely"
CANDIDATE_MISMATCH = "mismatch_likely"
CANDIDATE_VALIDATION = "validation_candidate"
CANDIDATE_UNCERTAIN = "uncertain"
# kind для пар, которые инженер вручную пометил как реорганизацию: НЕ обычная
# same-пара для enrichment, а кандидат на целенаправленную проверку связи
CANDIDATE_MANUAL_REORG = "manual_confirmed_reorganized"

_VALID_CONFIDENCE = {"high", "medium", "low"}

# ─── prompt contract ─────────────────────────────────────────────────────────

VISION_PROMPT_TEMPLATE = """Ты — инженер-эксперт по проектной документации. \
Тебе даны два изображения ОДНОГО И ТОГО ЖЕ графического блока чертежа: \
OLD (старая стадия) и NEW (новая стадия).

Контекст блока:
- тип графики: {graphic_type}
- дисциплина: {discipline}
- лист OLD: стр. {left_page}{left_sheet}
- лист NEW: стр. {right_page}{right_sheet}
- вердикт визуального сравнения: {visual_status}

Задача:
1. Кратко опиши, что изображено на OLD.
2. Кратко опиши, что изображено на NEW.
3. Перечисли ВИДИМЫЕ изменения между OLD и NEW.
4. Выпиши инженерные сущности с ОБЕИХ сторон (буквально, как написано): \
оборудование, кабели/сечения, автоматы/номиналы, линии/подключения, \
обозначения, помещения/оси/этажи (если видны).

Жёсткие правила:
- НЕ придумывай того, чего не видно на изображении.
- НЕ делай юридических/нормативных выводов.
- Если надпись нечитаема — пиши «[нечитаемо]», не угадывай.
- Если изображение нечитабельно целиком — так и напиши в описании.
- Маркировки переписывай буквально (ЩР-1а, не «щит 1»).

Ответ — СТРОГО один JSON-объект без пояснений вокруг:
{{
  "old_description": "…",
  "new_description": "…",
  "observed_changes": ["…"],
  "engineering_entities_old": ["…"],
  "engineering_entities_new": ["…"],
  "possible_risks": ["…"],
  "confidence": "high|medium|low"
}}"""


# ─── helpers ─────────────────────────────────────────────────────────────────


def _opt(options: Optional[dict], key: str) -> Any:
    if isinstance(options, dict) and key in options:
        return options[key]
    return _DEFAULT_OPTIONS.get(key)


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _safe_float(value: Any, fallback: float) -> float:
    try:
        f = float(value)
        return f if f == f else fallback   # NaN-guard
    except (TypeError, ValueError):
        return fallback


def _blocks_by_id(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    blocks = m.get("blocks")
    if isinstance(blocks, dict):
        return {k: v for k, v in blocks.items() if isinstance(v, dict)}
    if isinstance(blocks, list):
        return {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
    return {}


def _pages_by_number(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    out: dict = {}
    for p in m.get("pages") or []:
        if isinstance(p, dict) and p.get("page_number") is not None:
            out[p["page_number"]] = p
    return out


def _model_pdf_path(model: Any) -> Optional[str]:
    m = model if isinstance(model, dict) else {}
    src = m.get("source") if isinstance(m.get("source"), dict) else {}
    return src.get("pdf_path") or None


def _descriptor_for(graphic_report: Any, block_id: Any) -> dict:
    r = graphic_report if isinstance(graphic_report, dict) else {}
    for d in r.get("descriptors") or []:
        if isinstance(d, dict) and d.get("block_id") == block_id:
            return d
    return {}


def _first_nonempty(*values: Any, fallback: str = "unknown") -> str:
    for v in values:
        s = str(v or "").strip()
        if s and s.lower() != "unknown":
            return s
    return fallback


def _sheet_name_of(model_pages: dict, page_no: Any) -> str:
    page = model_pages.get(page_no)
    if isinstance(page, dict):
        return str(page.get("sheet_name") or "").strip()
    return ""


def _crop_source(block: Optional[dict], pdf_path: Optional[str]) -> dict:
    b = block if isinstance(block, dict) else {}
    return {
        "image_file": b.get("image_file"),
        "pdf_path": pdf_path,
        "page_number": b.get("page_number"),
        "bbox_norm": b.get("coords_norm"),
    }


def _crop_ref(source: dict, rendered_png: Optional[str]) -> str:
    if rendered_png:
        return str(rendered_png)
    img = source.get("image_file")
    if img:
        return str(img)
    pdf = source.get("pdf_path")
    page = source.get("page_number")
    if pdf:
        return f"{pdf}#page={page}"
    return ""


# ─── candidate selection v2 (entity-aware) ───────────────────────────────────
#
# Урок real vision pilot ИОС1.1: position-based block matching сводит РАЗНЫЕ
# сущности (схема ВРУ-1 ↔ план ТП, ВРУ-3 ↔ ВРУ-2, ЯК ↔ ЩО-3). Слепой отбор
# «всех send_to_vision» тратит vision-бюджет на пары, где честный ответ —
# «это разные объекты». Scoring v2 ранжирует кандидатов по entity-идентичности
# (имя листа/маркировки оборудования) и разводит enrichment / link_validation.

_ENTITY_FAMILIES = ("ВРУ", "ГРЩ", "ЩЭ", "ЩАО", "ЩА", "ЩО", "ЩР", "ЩС", "ЩК",
                    "ЯК", "АВР", "РУСН", "ИТП", "ВРП", "ШУ", "ЩУ", "ГЗШ",
                    "РП", "ТП")
# family + СЛИТНЫЙ/дефисный хвост; пробельные хвосты («АВР 100А») намеренно
# не захватываются — после family через пробел чаще номинал/количество,
# чем номер единицы (та же логика, что в stamp_matching для «0,4кВ»)
_ENTITY_TOKEN_RE = re.compile(
    r"(?<![А-ЯЁа-яёA-Za-z0-9])(" + "|".join(_ENTITY_FAMILIES) + r")"
    r"([0-9А-ЯЁа-яёA-Za-z.\-–—]*)",
    re.IGNORECASE,
)
_TAIL_FAMILY_RE = re.compile(
    "|".join(_ENTITY_FAMILIES), re.IGNORECASE)
_SHEET_KIND_MARKERS = [
    ("scheme", ("однолинейн", "схема", "принципиальн")),
    ("plan", ("план",)),
    ("table", ("спецификац", "таблиц", "ведомост", "экспликац", "перечень")),
    ("detail", ("узел", "узлы", "разрез", "деталь")),
]


def _entity_id_from_match(fam_raw: str, tail: str) -> Optional[str]:
    """Нормализованный id из совпадения или None (ложное срабатывание).

    Анти-false-positive правила (ревью: «вручную»→ВРУ, «шум»→ШУ, «ТПУ»→ТП):

    * приклеенный буквенный хвост допустим только у ЗАГЛАВНОЙ family и
      только короткий строчный суффикс серии («ВРУа», «ЩРа-1»);
    * строчная/смешанная family принимается лишь bare или с дефис-хвостом
      («вру», «щр-1а» из MD-текста) — «вручную»/«Якорь»/«щуп» отбрасываются.
    """
    fam = fam_raw.upper().replace("Ё", "Е")
    norm = (tail or "").replace("–", "-").replace("—", "-")
    m = re.match(r"[А-ЯЁа-яёA-Za-z]+", norm)
    attached = m.group(0) if m else ""
    if attached:
        if not fam_raw.isupper():
            return None          # часть обычного слова: «вручную», «Якорь»
        if attached.isupper() and not any(ch.isdigit() for ch in norm):
            return None          # аббревиатурный хвост: «ТПУ», «ВРУЧНУЮ»
        if len(attached) > 2 and not any(ch.isdigit() for ch in norm):
            return None
    # «ВРУ2-РП1» — фидерная метка: РП1 это назначение, не идентичность
    # листа; хвост обрезается до вложенной family (leak guard)
    m2 = _TAIL_FAMILY_RE.search(norm)
    if m2:
        norm = norm[:m2.start()]
    norm = norm.strip("-. ").lower()
    return f"{fam}-{norm}" if norm else fam


def extract_entity_ids(*texts: Any) -> set[str]:
    """Извлечь маркировки сущностей (ВРУ-1, ГРЩ, ЩО-3, ЯК5, ЩР-ТХ1…)."""
    out: set[str] = set()
    for t in texts:
        if isinstance(t, (list, tuple, set)):
            out |= extract_entity_ids(*t)
            continue
        s = str(t or "")
        for m in _ENTITY_TOKEN_RE.finditer(s):
            eid = _entity_id_from_match(m.group(1), m.group(2))
            if eid:
                out.add(eid)
    return out


def _entity_families_of(ids: set[str]) -> set[str]:
    return {i.split("-", 1)[0] for i in ids}


def entity_identity_signal(left_ids: set[str],
                           right_ids: set[str]) -> str:
    """match | family_only_match | numbered_conflict | family_conflict | none.

    * match — общая НУМЕРОВАННАЯ маркировка (ВРУ-2 ↔ ВРУ-2);
    * numbered_conflict — общая family, нумерованные ids обеих сторон не
      пересекаются (ВРУ-3 ↔ ВРУ-2) — проверяется РАНЬШЕ bare-пересечения:
      generic-токен «вру» на обеих сторонах не подтверждает идентичность
      (critical-находка ревью на реальных дескрипторах);
    * family_only_match — пересечение только на уровне family/bare
      (ГРЩ ↔ ГРЩ, {ВРУ} ↔ {ВРУ-2}) — слабое подтверждение;
    * family_conflict — families не пересекаются (ЯК ↔ ЩО);
    * none — хотя бы одна сторона без распознанных маркировок.
    """
    if not left_ids or not right_ids:
        return "none"
    numbered_common = {i for i in left_ids & right_ids if "-" in i}
    if numbered_common:
        return "match"
    lf, rf = _entity_families_of(left_ids), _entity_families_of(right_ids)
    common = lf & rf
    if common:
        for fam in common:
            ln = {i for i in left_ids if "-" in i
                  and i.split("-", 1)[0] == fam}
            rn = {i for i in right_ids if "-" in i
                  and i.split("-", 1)[0] == fam}
            if ln and rn and not (ln & rn):
                return "numbered_conflict"
        return "family_only_match"
    return "family_conflict"


def sheet_kind_of(sheet_name: Any) -> Optional[str]:
    """Вид листа по самому РАННЕМУ маркеру в имени («План ТП и схема
    вентиляции» → plan, а не scheme)."""
    s = str(sheet_name or "").lower()
    best: tuple[int, Optional[str]] = (len(s) + 1, None)
    for kind, markers in _SHEET_KIND_MARKERS:
        for m in markers:
            idx = s.find(m)
            if idx >= 0 and idx < best[0]:
                best = (idx, kind)
    return best[1]


def _equipment_token_informative(tok: Any) -> bool:
    """Токен equipment содержателен? Bare family-слово («вру») — нет;
    family с дискриминатором («ВРУ2-РП1») и не-family токены («QF1») — да."""
    s = str(tok or "").strip()
    if not s:
        return False
    m = _ENTITY_TOKEN_RE.fullmatch(s)
    if m is None:
        return True
    eid = _entity_id_from_match(m.group(1), m.group(2))
    return bool(eid and "-" in eid)


def _matched_graphic_index(graphic_matched_report: Any) -> dict:
    """(left_block_id, right_block_id) → matched-graphic entry."""
    entries = []
    if isinstance(graphic_matched_report, list):
        entries = graphic_matched_report
    elif isinstance(graphic_matched_report, dict):
        entries = graphic_matched_report.get("matched_graphic_blocks") or []
    out: dict = {}
    for e in entries:
        if isinstance(e, dict) and e.get("left_block_id") and e.get("right_block_id"):
            out[(e["left_block_id"], e["right_block_id"])] = e
    return out


def _domain_signature(desc: dict) -> set[str]:
    """Множество инженерных доменов из sheet_name + токенов дескриптора."""
    t = desc.get("tokens") if isinstance(desc.get("tokens"), dict) else {}
    blob = " ".join([
        str(desc.get("sheet_name") or ""),
        " ".join(str(x) for x in (t.get("equipment") or [])),
        " ".join(str(x) for x in (t.get("raw_key_entities") or [])),
        " ".join(str(x) for x in (t.get("power") or [])),
    ]).lower()
    return {dom for dom, markers in _DOMAIN_MARKERS.items()
            if any(m in blob for m in markers)}


def score_vision_candidate(pair: dict, *, left_desc: dict, right_desc: dict,
                           matched_entry: Optional[dict] = None) -> dict:
    """Оценить пару gate как кандидата на vision (score/kind/reasons/risks)."""
    reasons: list[str] = []
    risks: list[str] = []
    score = 0.5

    if pair.get("decision") == "send_to_vision":
        score += 0.1
        reasons.append("gate:send_to_vision")
    if pair.get("status") == "changed_visual":
        score += 0.05
        reasons.append("gate:changed_visual")

    lt = left_desc.get("tokens") if isinstance(left_desc.get("tokens"), dict) else {}
    rt = right_desc.get("tokens") if isinstance(right_desc.get("tokens"), dict) else {}
    left_ids = extract_entity_ids(left_desc.get("sheet_name"),
                                  lt.get("equipment"),
                                  lt.get("raw_key_entities"))
    right_ids = extract_entity_ids(right_desc.get("sheet_name"),
                                   rt.get("equipment"),
                                   rt.get("raw_key_entities"))
    identity = entity_identity_signal(left_ids, right_ids)
    # primary-идентичность листа (только sheet_name) перебивает mention-pool:
    # упоминание ГРЩ/РП-1 на схеме ВРУ-2 не должно ни подтверждать, ни
    # маскировать конфликт ВРУ-3↔ВРУ-2 (находка ревью на real data)
    primary = entity_identity_signal(
        extract_entity_ids(left_desc.get("sheet_name")),
        extract_entity_ids(right_desc.get("sheet_name")))
    if primary in ("match", "numbered_conflict", "family_conflict"):
        identity = primary
    if identity == "match":
        score += 0.2
        reasons.append("entity_id_match")
    elif identity == "family_only_match":
        score += 0.05
        reasons.append("entity_family_match")
    elif identity == "numbered_conflict":
        score -= 0.35
        risks.append("entity_id_conflict")
    elif identity == "family_conflict":
        score -= 0.3
        risks.append("entity_family_conflict")

    lk, rk = sheet_kind_of(left_desc.get("sheet_name")), \
        sheet_kind_of(right_desc.get("sheet_name"))
    if lk and rk:
        if lk == rk:
            score += 0.1
            reasons.append(f"sheet_kind_match:{lk}")
        else:
            score -= 0.3
            risks.append(f"sheet_kind_mismatch:{lk}/{rk}")

    lg_t, rg_t = left_desc.get("graphic_type"), right_desc.get("graphic_type")
    me = matched_entry or {}
    type_match = me.get("graphic_type_match")
    if type_match is None and lg_t and rg_t:
        type_match = (lg_t == rg_t)
    if type_match is True:
        score += 0.1
        reasons.append("graphic_type_match")
    elif type_match is False:
        score -= 0.1
        risks.append("graphic_type_mismatch")

    disc_match = me.get("discipline_match")
    if disc_match is None:
        ld_d, rd_d = left_desc.get("discipline"), right_desc.get("discipline")
        if ld_d and rd_d:
            disc_match = (ld_d == rd_d)
    if disc_match is True:
        score += 0.05
        reasons.append("discipline_match")
    elif disc_match is False:
        score -= 0.15
        risks.append("discipline_mismatch")

    overlap = (me.get("token_overlap") or {}).get("equipment")
    if isinstance(overlap, (int, float)):
        # informativeness guard: overlap=1.0 на списках из одних generic
        # family-слов (['вру'] ↔ ['вру']) — бессодержателен, бонус не даётся
        informative = any(
            _equipment_token_informative(tok)
            for tok in (lt.get("equipment") or []) + (rt.get("equipment") or []))
        if overlap >= 0.5 and informative:
            score += 0.15
            reasons.append(f"equipment_overlap:{overlap:.2f}")
        elif overlap >= 0.2 and informative:
            score += 0.1
            reasons.append(f"equipment_overlap:{overlap:.2f}")
        elif overlap == 0 and lt.get("equipment") and rt.get("equipment"):
            score -= 0.1
            risks.append("equipment_overlap_zero")

    quality = me.get("match_quality")
    if quality == "strong":
        score += 0.05
        reasons.append("match_quality:strong")
    elif quality == "weak":
        score -= 0.05
        risks.append("match_quality:weak")
    for f in me.get("risk_flags") or []:
        if f in ("low_token_overlap", "one_side_not_usable") and f not in risks:
            risks.append(f)
            score -= 0.05

    metrics = pair.get("metrics") if isinstance(pair.get("metrics"), dict) else {}
    iou = metrics.get("mask_iou")
    ncc = metrics.get("normalized_correlation")
    if (isinstance(iou, (int, float)) and iou >= 0.05) or \
            (isinstance(ncc, (int, float)) and ncc >= 0.2):
        score += 0.05
        reasons.append("visual_structure_overlap")

    if "duplicate_candidate" in (pair.get("risk_flags") or []):
        score -= 0.1
        risks.append("duplicate_candidate")

    # штамп — та же сущность, но низкая ценность для vision-enrichment
    # (дельты штампа ловит текстовый слой); инженерная графика приоритетнее
    lg_lower = str(left_desc.get("graphic_type") or "").lower()
    rg_lower = str(right_desc.get("graphic_type") or "").lower()
    if "stamp" in (lg_lower, rg_lower):
        score -= 0.35
        risks.append("stamp_block_low_vision_value")

    # домен-конфликт: явно разные инженерные системы с обеих сторон
    # (pilot v2: ОЗДС-легенда ↔ схема квартирных ящиков) — подмена сущности
    ldom, rdom = _domain_signature(left_desc), _domain_signature(right_desc)
    if ldom and rdom and not (ldom & rdom):
        score -= 0.25
        risks.append("domain_mismatch")

    score = max(0.0, min(1.0, round(score, 3)))

    # рассогласование сущности по СИЛЬНЫМ сигналам — дисциплина/домен
    # (разные инженерные системы). graphic_type_mismatch — СЛАБЫЙ: cabinet
    # vs single_line часто одна и та же ГРЩ с разной vision-классификацией,
    # сам по себе SAME не блокирует (pilot v2: 7EMD ГРЩ — настоящая пара).
    strong_identity = (identity == "match")
    strong_soft = [r for r in risks if r.startswith(
        ("discipline_mismatch", "domain_mismatch"))]
    weak_soft = [r for r in risks if r.startswith("graphic_type_mismatch")]
    is_legend = "legend" in (lg_lower, rg_lower)

    hard_mismatch = any(r.startswith(("entity_id_conflict",
                                      "entity_family_conflict",
                                      "sheet_kind_mismatch")) for r in risks)
    if hard_mismatch:
        kind = CANDIDATE_MISMATCH
    elif strong_soft and not strong_identity:
        # разная дисциплина/домен без нумерованной идентичности.
        # подкреплено вторым strong / типом / legend → почти наверняка
        # подмена сущности (legend ОЗДС↔квартиры); одиночный сигнал → review
        kind = (CANDIDATE_MISMATCH
                if len(strong_soft) >= 2 or weak_soft or is_legend
                else CANDIDATE_VALIDATION)
    elif is_legend and not strong_identity:
        # legend-caution: условные обозначения нельзя сводить по одному
        # family/sheet_kind — нужна нумерованная идентичность (ревью pilot v2)
        kind = CANDIDATE_VALIDATION
    # SAME требует корроборации (entity/вид листа), не только score:
    # blank-дескрипторные пары не должны проходить в enrichment по одним
    # gate-бонусам (ревью: позиционный срез reasons[2:] был артефактом)
    elif score >= 0.6 and (identity in ("match", "family_only_match")
                           or (lk and lk == rk)):
        kind = CANDIDATE_SAME
    elif risks:
        kind = CANDIDATE_VALIDATION
    else:
        kind = CANDIDATE_UNCERTAIN

    return {
        "candidate_score": score,
        "candidate_kind": kind,
        "candidate_reasons": reasons,
        "candidate_risk_flags": risks,
    }


# ─── manual entity mapping overrides → candidate selection ───────────────────


def _candidate_primary_label(desc: dict) -> Optional[str]:
    """Primary-метка кандидата (для fallback-матча override'а по labels).

    Lazy import предотвращает цикл: entity_alignment_preview импортирует ЭТОТ
    модуль на старте, поэтому обратную зависимость берём в рантайме.
    """
    try:
        from backend.app.services.stage_comparison.pipeline_v2_entity_alignment_preview import (  # noqa: E501
            extract_entity_labels)
    except Exception:  # noqa: BLE001 — fallback-метка опциональна
        return None
    try:
        lab = extract_entity_labels(
            desc.get("sheet_name"),
            (desc.get("tokens") or {}).get("equipment"))
        return lab.get("primary")
    except Exception:  # noqa: BLE001
        return None


def apply_manual_decision_to_candidate(cand: dict, override: dict, *, mode: str,
                                       include_confirmed_reorganized: bool,
                                       debug: bool = False) -> str:
    """Применить ручное решение override'а к кандидату (мутирует cand).

    Возвращает ``manual_decision``. Выставляет ``manual_mapping``, дополняет
    ``candidate_reasons`` / ``candidate_risk_flags`` и ставит транзиентный
    ``_manual_exclude`` (исключить из ТЕКУЩЕГО режима). Manual decision
    переопределяет авто-классификацию — это видно в reasons.
    """
    decision = override.get("manual_decision")
    cand["manual_mapping"] = {
        "mapping_id": override.get("mapping_id"),
        "decision": decision,
        "comment": override.get("comment"),
        "source": "entity_mapping_overrides",
    }
    reasons = list(cand.get("candidate_reasons") or [])
    risks = list(cand.get("candidate_risk_flags") or [])
    reasons.append(f"manual_mapping:{decision}")
    exclude = False

    if decision == "confirmed_same_entity":
        cand["candidate_kind"] = CANDIDATE_SAME
        cand["candidate_score"] = round(
            min(1.0, _safe_float(cand.get("candidate_score"), 0.5) + 0.4), 3)
        reasons.append("manual_confirmed_same_entity")
    elif decision == "confirmed_rename":
        cand["candidate_kind"] = CANDIDATE_SAME
        cand["candidate_score"] = round(
            min(1.0, _safe_float(cand.get("candidate_score"), 0.5) + 0.3), 3)
        reasons.append("manual_confirmed_rename")
    elif decision == "confirmed_reorganized":
        cand["candidate_kind"] = CANDIDATE_MANUAL_REORG
        if "manual_confirmed_reorganized" not in risks:
            risks.append("manual_confirmed_reorganized")
        if mode == "enrichment":
            if include_confirmed_reorganized:
                if "requires_human_review" not in risks:
                    risks.append("requires_human_review")
            else:
                exclude = True
        # link_validation: оставляем и приоритизируем (см. сортировку)
    elif decision == "rejected_mapping":
        reasons.append("manual_rejected_mapping")
        if mode == "enrichment":
            exclude = True
        elif mode == "link_validation":
            exclude = not debug      # только debug-режим пускает rejected
    elif decision == "no_match":
        reasons.append("manual_no_match")
        exclude = True               # исключён из обоих режимов
    else:
        # неизвестное решение — не трогаем классификацию, только помечаем
        reasons.append("manual_unknown_decision")

    cand["candidate_reasons"] = reasons
    cand["candidate_risk_flags"] = risks
    cand["_manual_exclude"] = exclude
    return decision


def select_vision_candidates_v2(visual_gate_report: Any, *,
                                left_graphic_report: Any = None,
                                right_graphic_report: Any = None,
                                graphic_matched_report: Any = None,
                                overrides_report: Any = None,
                                options: Optional[dict] = None
                                ) -> tuple[list[dict], dict, list[str]]:
    """Entity-aware отбор: score → режим (enrichment/link_validation) → cap.

    Возвращает (selected_pairs_с_candidate_полями, stats, warnings).
    enrichment: same_entity_likely по score desc, затем uncertain, затем
    validation_candidate при недоборе; mismatch_likely исключаются при
    exclude_mismatch_likely=true (default). link_validation: сначала
    mismatch_likely + validation_candidate (самые подозрительные — то, что
    нужно проверить), затем остальные.
    """
    warnings: list[str] = []
    r = visual_gate_report if isinstance(visual_gate_report, dict) else {}
    pairs = [bp for bp in r.get("block_pairs") or [] if isinstance(bp, dict)]
    include_manual = _opt(options, "include_manual_review") is not False
    include_excluded = _opt(options, "include_exclude_from_vision") is True
    mode = str(_opt(options, "selection_mode") or "enrichment").lower()
    if mode not in ("enrichment", "link_validation"):
        warnings.append(f"unknown selection_mode {mode!r} — "
                        "falling back to 'enrichment'")
        mode = "enrichment"
    exclude_mismatch = _opt(options, "exclude_mismatch_likely") is not False

    # manual entity mapping overrides (mark-only, default OFF)
    use_overrides = _opt(options, "use_entity_mapping_overrides") is True
    mm_mode = str(_opt(options, "manual_mapping_mode") or "both").lower()
    include_reorg = _opt(options, "include_confirmed_reorganized") is True
    manual_debug = _opt(options, "manual_mapping_debug") is True
    manual_applies = (use_overrides and isinstance(overrides_report, dict)
                      and (mm_mode in ("both", mode)))
    ov_index = index_overrides_for_lookup(overrides_report) if manual_applies else None

    matched_idx = _matched_graphic_index(graphic_matched_report)
    stats = {"candidates_total": len(pairs), "excluded_by_visual_gate": 0,
             "manual_review_included": 0, "manual_review_skipped": 0,
             "other_skipped": 0, "dropped_by_cap": 0,
             "by_candidate_kind": {}, "mismatch_excluded": 0,
             "selection_mode": mode,
             "manual_mapping_enabled": bool(manual_applies),
             "manual_mapping_applied": 0, "manual_excluded": 0,
             "by_manual_decision": {}}

    eligible: list[dict] = []
    for bp in pairs:
        decision = bp.get("decision")
        if decision == "exclude_from_vision":
            stats["excluded_by_visual_gate"] += 1
            if not include_excluded:
                continue
        elif decision == "manual_review":
            if not include_manual:
                stats["manual_review_skipped"] += 1
                continue
        elif decision != "send_to_vision":
            stats["other_skipped"] += 1
            continue
        lid, rid = bp.get("left_block_id"), bp.get("right_block_id")
        ld = _descriptor_for(left_graphic_report, lid)
        rd = _descriptor_for(right_graphic_report, rid)
        verdict = score_vision_candidate(
            bp, left_desc=ld, right_desc=rd,
            matched_entry=matched_idx.get((lid, rid)))
        cand = {**bp, **verdict}
        if manual_applies:
            override = find_override_for_pair(
                ov_index, left_block_id=lid, right_block_id=rid,
                pair_key=bp.get("pair_key"),
                left_label=_candidate_primary_label(ld),
                right_label=_candidate_primary_label(rd))
            if override:
                dec = apply_manual_decision_to_candidate(
                    cand, override, mode=mode,
                    include_confirmed_reorganized=include_reorg,
                    debug=manual_debug)
                stats["manual_mapping_applied"] += 1
                stats["by_manual_decision"][dec] = \
                    stats["by_manual_decision"].get(dec, 0) + 1
        eligible.append(cand)
        k = cand["candidate_kind"]
        stats["by_candidate_kind"][k] = stats["by_candidate_kind"].get(k, 0) + 1

    # drop manually-excluded кандидаты (rejected/no_match/reorg-default-enrichment)
    if manual_applies:
        kept = [c for c in eligible if c.pop("_manual_exclude", False) is not True]
        stats["manual_excluded"] = len(eligible) - len(kept)
        eligible = kept

    if mode == "link_validation":
        # цель — проверить подозрительные связи: ручная реорганизация и
        # mismatch/validation первыми (manual_confirmed_reorganized — приоритет)
        prio = {CANDIDATE_MANUAL_REORG: -1, CANDIDATE_MISMATCH: 0,
                CANDIDATE_VALIDATION: 1, CANDIDATE_UNCERTAIN: 2,
                CANDIDATE_SAME: 3}
        eligible.sort(key=lambda c: (prio.get(c["candidate_kind"], 9),
                                     -c["candidate_score"]))
    else:
        if exclude_mismatch:
            kept = [c for c in eligible
                    if c["candidate_kind"] != CANDIDATE_MISMATCH]
            stats["mismatch_excluded"] = len(eligible) - len(kept)
            eligible = kept
        # manual_confirmed_reorganized (если include=true) — сразу после SAME
        prio = {CANDIDATE_SAME: 0, CANDIDATE_MANUAL_REORG: 1,
                CANDIDATE_UNCERTAIN: 2, CANDIDATE_VALIDATION: 3,
                CANDIDATE_MISMATCH: 4}
        eligible.sort(key=lambda c: (prio.get(c["candidate_kind"], 9),
                                     -c["candidate_score"]))

    for rank, c in enumerate(eligible, start=1):
        c["candidate_rank"] = rank

    max_items = _safe_int(_opt(options, "max_items"), 5)
    selected = eligible
    if max_items > 0 and len(selected) > max_items:
        stats["dropped_by_cap"] = len(selected) - max_items
        warnings.append(f"selection truncated by max_items={max_items}: "
                        f"{len(selected) - max_items} of {len(selected)} "
                        f"candidates dropped")
        selected = selected[:max_items]
    stats["manual_review_included"] = sum(
        1 for bp in selected if bp.get("decision") == "manual_review")
    return selected, stats, warnings


# ─── selection ───────────────────────────────────────────────────────────────


def select_blocks_for_vision(visual_gate_report: Any,
                             options: Optional[dict] = None
                             ) -> tuple[list[dict], dict, list[str]]:
    """Выбрать пары блоков для vision по решениям visual gate.

    Возвращает ``(selected_pairs, stats, warnings)``. Правила:

    * ``send_to_vision`` — берём всегда (приоритет при cap: у них есть
      подтверждённое визуальное изменение);
    * ``manual_review`` — только при ``include_manual_review=true``;
    * ``exclude_from_vision`` — НЕ берём (опция
      ``include_exclude_from_vision=true`` существует для отладки);
    * cap ``max_items`` — усечение явно warn'ится (no silent caps).
    """
    warnings: list[str] = []
    r = visual_gate_report if isinstance(visual_gate_report, dict) else {}
    pairs = [bp for bp in r.get("block_pairs") or [] if isinstance(bp, dict)]

    include_manual = _opt(options, "include_manual_review") is not False
    include_excluded = _opt(options, "include_exclude_from_vision") is True

    send, manual, excluded_taken = [], [], []
    stats = {"candidates_total": len(pairs), "excluded_by_visual_gate": 0,
             "manual_review_included": 0, "manual_review_skipped": 0,
             "other_skipped": 0, "dropped_by_cap": 0}
    for bp in pairs:
        decision = bp.get("decision")
        if decision == "send_to_vision":
            send.append(bp)
        elif decision == "manual_review":
            if include_manual:
                manual.append(bp)
            else:
                stats["manual_review_skipped"] += 1
        elif decision == "exclude_from_vision":
            stats["excluded_by_visual_gate"] += 1
            if include_excluded:
                excluded_taken.append(bp)
        else:
            stats["other_skipped"] += 1

    selected = send + manual + excluded_taken
    # max_items <= 0 = unlimited (cap выключен)
    max_items = _safe_int(_opt(options, "max_items"), 5)
    if max_items > 0 and len(selected) > max_items:
        stats["dropped_by_cap"] = len(selected) - max_items
        warnings.append(f"selection truncated by max_items={max_items}: "
                        f"{len(selected) - max_items} of {len(selected)} "
                        f"candidates dropped")
        selected = selected[:max_items]
    # счётчик «включённых manual» — по ФАКТИЧЕСКОЙ выборке (после cap),
    # иначе summary противоречил бы items
    stats["manual_review_included"] = sum(
        1 for bp in selected if bp.get("decision") == "manual_review")
    return selected, stats, warnings


# ─── prompt ──────────────────────────────────────────────────────────────────


def build_vision_prompt_for_block_pair(pair: dict, *,
                                       graphic_type: str = "unknown",
                                       discipline: str = "unknown",
                                       left_sheet_name: str = "",
                                       right_sheet_name: str = "") -> str:
    """Собрать строгий vision-prompt для пары графических блоков."""
    p = pair if isinstance(pair, dict) else {}
    lp = p.get("left_page_number")
    rp = p.get("right_page_number")
    return VISION_PROMPT_TEMPLATE.format(
        graphic_type=graphic_type or "unknown",
        discipline=discipline or "unknown",
        left_page="?" if lp is None else lp,
        right_page="?" if rp is None else rp,
        left_sheet=f" ({left_sheet_name})" if left_sheet_name else "",
        right_sheet=f" ({right_sheet_name})" if right_sheet_name else "",
        visual_status=p.get("status") or "unknown",
    )


# ─── plan ────────────────────────────────────────────────────────────────────


def build_graphic_vision_enrichment_plan(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None, overrides_report: Any = None,
        options: Optional[dict] = None) -> dict:
    """Построить план (items без vision-результатов) из готовых артефактов.

    Отсутствующий/непригодный visual gate → ``status=skipped_no_visual_gate``
    с пустыми items (модуль никогда не «угадывает» кандидатов сам — решения
    принимает только gate).
    """
    warnings: list[str] = []
    gate = visual_gate_report if isinstance(visual_gate_report, dict) else None
    if gate is None or not isinstance(gate.get("block_pairs"), list):
        return {
            "status": "skipped_no_visual_gate",
            "items": [],
            "stats": {"candidates_total": 0, "excluded_by_visual_gate": 0,
                      "manual_review_included": 0},
            "warnings": ["visual gate report unavailable (stage disabled, "
                         "failed or not run) — graphic vision enrichment "
                         "skipped"],
        }

    if str(_opt(options, "candidate_selection") or "legacy") == "entity_aware":
        selected, stats, sel_warnings = select_vision_candidates_v2(
            gate, left_graphic_report=left_graphic_report,
            right_graphic_report=right_graphic_report,
            graphic_matched_report=graphic_matched_report,
            overrides_report=overrides_report,
            options=options)
    else:
        selected, stats, sel_warnings = select_blocks_for_vision(gate, options)
    warnings.extend(sel_warnings)

    left_blocks = _blocks_by_id(left_model)
    right_blocks = _blocks_by_id(right_model)
    left_pages = _pages_by_number(left_model)
    right_pages = _pages_by_number(right_model)
    left_pdf = _model_pdf_path(left_model)
    right_pdf = _model_pdf_path(right_model)
    write_prompts = _opt(options, "write_prompts") is not False

    items: list[dict] = []
    prompts_by_item_id: dict[str, str] = {}
    for bp in selected:
        lid, rid = bp.get("left_block_id"), bp.get("right_block_id")
        lb, rb = left_blocks.get(lid), right_blocks.get(rid)
        item_warnings: list[str] = []
        if lb is None:
            item_warnings.append("left block missing in normalized model")
        if rb is None:
            item_warnings.append("right block missing in normalized model")

        ld = _descriptor_for(left_graphic_report, lid)
        rd = _descriptor_for(right_graphic_report, rid)
        graphic_type = _first_nonempty(ld.get("graphic_type"),
                                       rd.get("graphic_type"))
        discipline = _first_nonempty(ld.get("discipline"),
                                     rd.get("discipline"))

        left_page = bp.get("left_page_number")
        if left_page is None:
            left_page = (lb or {}).get("page_number")
        right_page = bp.get("right_page_number")
        if right_page is None:
            right_page = (rb or {}).get("page_number")

        left_source = _crop_source(lb, left_pdf)
        right_source = _crop_source(rb, right_pdf)
        left_source["page_number"] = left_page
        right_source["page_number"] = right_page

        prompt = build_vision_prompt_for_block_pair(
            {**bp, "left_page_number": left_page,
             "right_page_number": right_page},
            graphic_type=graphic_type, discipline=discipline,
            left_sheet_name=_sheet_name_of(left_pages, left_page),
            right_sheet_name=_sheet_name_of(right_pages, right_page))

        item_id = f"gv_{lid}__{rid}"
        # полный prompt строится ВСЕГДА (runner получает его независимо от
        # write_prompts); write_prompts управляет только персистенцией
        prompts_by_item_id[item_id] = prompt
        candidate_fields = {
            k: bp[k] for k in ("candidate_score", "candidate_rank",
                               "candidate_kind", "candidate_reasons",
                               "candidate_risk_flags", "manual_mapping") if k in bp}
        items.append({
            "item_id": item_id,
            **candidate_fields,
            "left_block_id": lid,
            "right_block_id": rid,
            "left_page_number": left_page,
            "right_page_number": right_page,
            "visual_status": bp.get("status"),
            "visual_decision": bp.get("decision"),
            "visual_metrics": dict(bp["metrics"])
                if isinstance(bp.get("metrics"), dict) else None,
            "graphic_type": graphic_type,
            "discipline": discipline,
            "left_crop_source": left_source,
            "right_crop_source": right_source,
            "left_crop_ref": _crop_ref(left_source, None),
            "right_crop_ref": _crop_ref(right_source, None),
            "prompt": prompt if write_prompts else None,
            "vision_status": "pending",
            "result": None,
            "warnings": item_warnings,
        })

    return {"status": "ok", "items": items, "stats": stats,
            "warnings": warnings, "prompts_by_item_id": prompts_by_item_id}


# ─── runner result normalization ─────────────────────────────────────────────


_MAX_LIST_ITEMS = 50
_MAX_ITEM_CHARS = 500


def _str_list(value: Any) -> tuple[list[str], list[str]]:
    """Нормализовать список строк из ответа runner'а.

    Возвращает (список, warnings). Falsy-скаляры (0, False) сохраняются
    строкой; патологически длинные элементы/списки обрезаются с warning'ом
    (no silent caps).
    """
    warnings: list[str] = []
    if isinstance(value, str):
        value = [value] if value.strip() else []
    if not isinstance(value, list):
        return [], warnings
    out: list[str] = []
    for v in value:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if len(s) > _MAX_ITEM_CHARS:
            s = s[:_MAX_ITEM_CHARS] + "…"
            warnings.append(f"list item truncated to {_MAX_ITEM_CHARS} chars")
        out.append(s)
    if len(out) > _MAX_LIST_ITEMS:
        warnings.append(f"list truncated to {_MAX_LIST_ITEMS} of "
                        f"{len(out)} items")
        out = out[:_MAX_LIST_ITEMS]
    return out, warnings


def normalize_vision_runner_result(raw: Any) -> tuple[Optional[dict], list[str]]:
    """Привести сырой ответ runner'а к контракту result.

    Возвращает ``(result|None, warnings)``; None — ответ непригоден
    (item становится failed). Строка с JSON парсится fail-soft.
    """
    warnings: list[str] = []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return None, ["runner returned non-JSON string"]
    if not isinstance(raw, dict):
        return None, [f"runner returned {type(raw).__name__}, expected dict"]

    old_desc = str(raw.get("old_description") or "").strip()
    new_desc = str(raw.get("new_description") or "").strip()
    if not old_desc and not new_desc:
        return None, ["runner result has no descriptions"]

    confidence = str(raw.get("confidence") or "").strip().lower()
    if confidence not in _VALID_CONFIDENCE:
        if confidence:
            warnings.append(f"invalid confidence {confidence!r} → low")
        confidence = "low"

    result = {"old_description": old_desc, "new_description": new_desc,
              "confidence": confidence}
    for key in ("observed_changes", "engineering_entities_old",
                "engineering_entities_new", "possible_risks"):
        values, list_warnings = _str_list(raw.get(key))
        result[key] = values
        warnings.extend(f"{key}: {w}" for w in list_warnings)
    return result, warnings


# ─── render options ──────────────────────────────────────────────────────────


_VALID_RENDER_MODES = ("normal", "high_res", "tiled")


def _render_options(options: Optional[dict]) -> tuple[dict, list[str]]:
    """Слить options["render"] с defaults (+legacy render_long_side).

    Возвращает (opts, warnings); ``opts["mode"]`` — ЭФФЕКТИВНЫЙ режим:
    tiled (зарезервированный контракт) деградирует к high_res с warning'ом
    уже здесь — независимо от того, дойдёт ли прогон до рендера; неизвестный
    режим честно warn'ится и падает в normal (не молча).
    """
    warnings: list[str] = []
    out = dict(_DEFAULT_RENDER_OPTIONS)
    legacy = _opt(options, "render_long_side")
    if legacy:
        out["long_side"] = _safe_int(legacy, out["long_side"])
    raw = (options or {}).get("render")
    _float_keys = {"tile_overlap"}
    _bool_keys = {"include_full_image"}
    if isinstance(raw, dict):
        for k in out:
            if k not in raw:
                continue
            if k == "mode":
                out[k] = raw[k]
            elif k in _float_keys:
                out[k] = _safe_float(raw[k], out[k])
            elif k in _bool_keys:
                out[k] = bool(raw[k])
            else:
                out[k] = _safe_int(raw[k], out[k])
    mode = str(out.get("mode") or "normal").strip().lower()
    if mode not in _VALID_RENDER_MODES:
        warnings.append(f"invalid render mode {out.get('mode')!r} → normal")
        mode = "normal"
    out["mode_requested"] = mode
    out["mode"] = mode               # tiled теперь реализован — без fallback
    return out, warnings


def _item_effective_render_mode(item: dict, render_opts: dict) -> str:
    """Эффективный режим рендера для КОНКРЕТНОГО item'а.

    tiling имеет смысл только для плотных схем; для не-dense типов tiled
    деградирует к high_res (один крупный рендер вместо плиток).
    """
    mode = render_opts.get("mode") or "normal"
    is_dense = (item.get("graphic_type") or "") in _DENSE_GRAPHIC_TYPES
    if mode == "tiled" and not is_dense:
        return "high_res"
    return mode


def _item_render_long_side(item: dict, render_opts: dict) -> int:
    """long_side одиночного рендера item'а (для normal/high_res путей)."""
    base = _safe_int(render_opts.get("long_side"), 1600)
    eff = _item_effective_render_mode(item, render_opts)
    if (eff == "high_res"
            and (item.get("graphic_type") or "") in _DENSE_GRAPHIC_TYPES):
        return _safe_int(render_opts.get("dense_long_side"), 2400)
    return base


# ─── crop rendering (только при наличии runner'а) ────────────────────────────


def _render_crop_png(block: Optional[dict], pages: dict,
                     pdf_path: Optional[str], out_path: Path,
                     long_side: int) -> tuple[Optional[str], Optional[str]]:
    """Срендерить кроп блока в PNG. Возвращает (path|None, error|None).

    Тяжёлые зависимости (cv2/fitz) импортируются лениво и fail-soft:
    их отсутствие — ошибка рендера item'а, не падение модуля.
    """
    if not isinstance(block, dict):
        return None, "block missing in normalized model"
    try:
        import cv2  # noqa: PLC0415 — ленивый импорт по контракту fail-soft
        from backend.app.services.stage_comparison.block_equivalence_precheck import (  # noqa: PLC0415
            EqBlock,
            load_or_render_block_image,
        )
    except Exception as exc:  # noqa: BLE001 — окружение без cv2/fitz
        return None, f"render dependencies unavailable: {exc}"

    page_no = block.get("page_number") or 0
    page = pages.get(page_no) if isinstance(pages.get(page_no), dict) else {}
    eq = EqBlock(
        block_id=str(block.get("block_id") or ""),
        page=_safe_int(page_no, 0),
        block_type=str(block.get("block_type") or "image"),
        coords_norm=block.get("coords_norm"),
        coords_px=block.get("coords_px"),
        page_width=_safe_int(page.get("width"), 0),
        page_height=_safe_int(page.get("height"), 0),
        text="",
        image_file=block.get("image_file"),
        crop_url=block.get("crop_url"),
        raw=block,
    )
    try:
        img, meta = load_or_render_block_image(
            eq, source_pdf_path=pdf_path, render_long_side=long_side)
    except Exception as exc:  # noqa: BLE001 — битый PDF и т.п.
        return None, f"render failed: {exc}"
    if img is None:
        return None, f"render failed: {(meta or {}).get('status')}"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(out_path), img):
        return None, "cv2.imwrite failed"
    return str(out_path), None


# ─── tiled render (MVP: grid + per-tile vision + aggregate) ──────────────────

TILE_PROMPT_TEMPLATE = """Ты — инженер-эксперт. Тебе даны ДВА ФРАГМЕНТА \
(плитка {tile_no} из {tile_total}) ОДНОГО графического блока чертежа: \
OLD (старая стадия) и NEW (новая стадия). Это НЕ вся схема, а только её часть \
(зона {bbox}).

Задача по ЭТОЙ ПЛИТКЕ:
1. Выпиши ТОЛЬКО то, что РЕАЛЬНО ВИДНО на фрагменте: номиналы автоматов (А), \
сечения кабелей (мм²), напряжения (В), обозначения аппаратов (QF, ЩР, ВРУ…).
2. Перечисли видимые изменения между OLD и NEW В ПРЕДЕЛАХ этой плитки.

Жёсткие правила:
- НЕ делай выводов по ВСЕЙ схеме — ты видишь только фрагмент.
- НЕ придумывай того, чего не видно; нечитаемое — «[нечитаемо]».
- Маркировки и номиналы переписывай БУКВАЛЬНО (160А, ЩР-1а, 4х185).

Ответ — строго один JSON-объект:
{{
  "old_description": "что видно на OLD-фрагменте",
  "new_description": "что видно на NEW-фрагменте",
  "observed_changes": ["…"],
  "engineering_entities_old": ["…"],
  "engineering_entities_new": ["…"],
  "possible_risks": ["…"],
  "confidence": "high|medium|low"
}}"""


def build_tile_prompt(tile_no: int, tile_total: int, bbox_norm: list) -> str:
    bb = "[" + ", ".join(f"{round(float(v), 2)}" for v in bbox_norm) + "]"
    return TILE_PROMPT_TEMPLATE.format(tile_no=tile_no, tile_total=tile_total,
                                       bbox=bb)


def plan_tile_grid(width: int, height: int, *, max_tiles: int,
                   overlap: float) -> list[dict]:
    """Сетка перекрывающихся плиток по aspect ratio (MVP).

    Возвращает список ``{tile_id, bbox_norm:[x0,y0,x1,y1]}`` в нормированных
    координатах (0..1). Широкие схемы → больше колонок; всегда ≥1 плитка;
    rows*cols ≤ max_tiles.
    """
    max_tiles = max(1, _safe_int(max_tiles, 6))
    ov = min(0.45, max(0.0, _safe_float(overlap, 0.12)))
    aspect = (float(width) / float(height)) if height else 1.0
    # выбрать cols×rows под aspect, уважая max_tiles
    if aspect >= 2.2:
        cols, rows = 3, 1
    elif aspect >= 1.3:
        cols, rows = 2, 2
    elif aspect <= 0.45:
        cols, rows = 1, 3
    elif aspect <= 0.77:
        cols, rows = 2, 2
    else:
        cols, rows = 2, 2
    while cols * rows > max_tiles and (cols > 1 or rows > 1):
        if cols >= rows and cols > 1:
            cols -= 1
        elif rows > 1:
            rows -= 1
        else:
            break
    tiles: list[dict] = []
    cw, ch = 1.0 / cols, 1.0 / rows
    n = 0
    for r in range(rows):
        for c in range(cols):
            n += 1
            x0 = max(0.0, c * cw - ov * cw)
            y0 = max(0.0, r * ch - ov * ch)
            x1 = min(1.0, (c + 1) * cw + ov * cw)
            y1 = min(1.0, (r + 1) * ch + ov * ch)
            tiles.append({"tile_id": f"tile_{n:03d}",
                          "bbox_norm": [round(x0, 4), round(y0, 4),
                                        round(x1, 4), round(y1, 4)]})
    return tiles


def _render_block_array(block: Optional[dict], pages: dict,
                        pdf_path: Optional[str], long_side: int):
    """Срендерить блок в BGR ndarray. Возвращает (img|None, error|None)."""
    if not isinstance(block, dict):
        return None, "block missing in normalized model"
    try:
        from backend.app.services.stage_comparison.block_equivalence_precheck import (  # noqa: PLC0415
            EqBlock, load_or_render_block_image)
    except Exception as exc:  # noqa: BLE001 — окружение без cv2/fitz
        return None, f"render dependencies unavailable: {exc}"
    page_no = block.get("page_number") or 0
    page = pages.get(page_no) if isinstance(pages.get(page_no), dict) else {}
    eq = EqBlock(
        block_id=str(block.get("block_id") or ""), page=_safe_int(page_no, 0),
        block_type=str(block.get("block_type") or "image"),
        coords_norm=block.get("coords_norm"), coords_px=block.get("coords_px"),
        page_width=_safe_int(page.get("width"), 0),
        page_height=_safe_int(page.get("height"), 0), text="",
        image_file=block.get("image_file"), crop_url=block.get("crop_url"),
        raw=block)
    try:
        img, meta = load_or_render_block_image(
            eq, source_pdf_path=pdf_path, render_long_side=long_side)
    except Exception as exc:  # noqa: BLE001
        return None, f"render failed: {exc}"
    if img is None:
        return None, f"render failed: {(meta or {}).get('status')}"
    return img, None


def _save_tile(img, bbox_norm: list, out_path: Path) -> Optional[str]:
    """Вырезать плитку по bbox_norm и сохранить PNG. None — ошибка."""
    try:
        import cv2  # noqa: PLC0415
        h, w = img.shape[:2]
        x0 = max(0, int(bbox_norm[0] * w)); y0 = max(0, int(bbox_norm[1] * h))
        x1 = min(w, int(bbox_norm[2] * w)); y1 = min(h, int(bbox_norm[3] * h))
        if x1 <= x0 or y1 <= y0:
            return None
        tile = img[y0:y1, x0:x1]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        return str(out_path) if cv2.imwrite(str(out_path), tile) else None
    except Exception:  # noqa: BLE001 — fail-soft по плитке
        return None


def _dedup_keep_order(values: list) -> list:
    seen, out = set(), []
    for v in values:
        key = str(v).strip().lower()
        if key and key not in seen:
            seen.add(key); out.append(v)
    return out


_CONF_RANK = {"low": 0, "medium": 1, "high": 2}
_CONF_NAME = {0: "low", 1: "medium", 2: "high"}


def aggregate_tile_results(tile_results: list[dict]) -> Optional[dict]:
    """Слить per-tile результаты в один result (union+dedup, conf=min)."""
    ok = [t.get("result") for t in tile_results
          if t.get("vision_status") == "ok" and isinstance(t.get("result"), dict)]
    if not ok:
        return None
    olds = [r.get("old_description") for r in ok if r.get("old_description")]
    news = [r.get("new_description") for r in ok if r.get("new_description")]
    changes, eo, en, risks = [], [], [], []
    confs = []
    for r in ok:
        changes += _str_list(r.get("observed_changes"))[0]
        eo += _str_list(r.get("engineering_entities_old"))[0]
        en += _str_list(r.get("engineering_entities_new"))[0]
        risks += _str_list(r.get("possible_risks"))[0]
        c = str(r.get("confidence") or "").lower()
        if c in _CONF_RANK:
            confs.append(_CONF_RANK[c])
    conf = _CONF_NAME[min(confs)] if confs else "low"
    return {
        "old_description": " | ".join(olds[:4]) or "[нечитаемо]",
        "new_description": " | ".join(news[:4]) or "[нечитаемо]",
        "observed_changes": _dedup_keep_order(changes),
        "engineering_entities_old": _dedup_keep_order(eo),
        "engineering_entities_new": _dedup_keep_order(en),
        "possible_risks": _dedup_keep_order(risks),
        "confidence": conf,
        "tile_results_summary": {
            "tiles_total": len(tile_results),
            "tiles_ok": len(ok),
            "tiles_failed": sum(1 for t in tile_results
                                if t.get("vision_status") == "failed"),
        },
    }


def _run_tiled_item(item: dict, *, left_block, right_block, left_pages,
                    right_pages, left_pdf, right_pdf, render_opts: dict,
                    runner_options: dict, crops_dir: Optional[Path],
                    vision_runner: Optional[VisionRunner]) -> str:
    """Tiled-обработка одного item'а. Возвращает vision_status.

    Заполняет ``item['render']`` (full refs + tiles). Fail-soft: упавшая
    плитка пропускается, item падает только если ВСЕ плитки упали.
    """
    tile_long = _safe_int(render_opts.get("tile_long_side"), 1400)
    max_tiles = _safe_int(render_opts.get("max_tiles"), 6)
    overlap = _safe_float(render_opts.get("tile_overlap"), 0.12)
    include_full = render_opts.get("include_full_image") is not False
    render_meta = {"requested_mode": render_opts.get("mode_requested"),
                   "effective_mode": "tiled", "tile_long_side": tile_long,
                   "max_tiles": max_tiles, "tile_overlap": overlap,
                   "full_left_crop_ref": None, "full_right_crop_ref": None,
                   "tiles_total": 0, "tiles": []}
    item["render"] = render_meta

    if crops_dir is None:
        # без места для рендера плиток план остаётся без refs; с runner'ом это
        # честный fail (звать его нечем), без runner'а — plan-only skip
        item["warnings"].append("tiled render requires crops_dir for tile refs")
        return "skipped_no_runner" if vision_runner is None else "failed"
    base = Path(crops_dir)
    # full render: длинная сторона ≈ tile_long × число колонок (детальные плитки)
    full_long = min(6000, tile_long * 3)
    lh = _render_block_array(left_block, left_pages, left_pdf, full_long)
    rh = _render_block_array(right_block, right_pages, right_pdf, full_long)
    left_img, lerr = lh
    right_img, rerr = rh
    for e in (lerr, rerr):
        if e:
            item["warnings"].append(e)
    if left_img is None and right_img is None:
        item["warnings"].append("tiled: no full image rendered")
        return "failed"
    if include_full:
        import cv2  # noqa: PLC0415
        if left_img is not None:
            p = base / f"{item['item_id']}_full_left.png"
            p.parent.mkdir(parents=True, exist_ok=True)
            if cv2.imwrite(str(p), left_img):
                render_meta["full_left_crop_ref"] = str(p)
                item["left_crop_ref"] = str(p)
        if right_img is not None:
            p = base / f"{item['item_id']}_full_right.png"
            if cv2.imwrite(str(p), right_img):
                render_meta["full_right_crop_ref"] = str(p)
                item["right_crop_ref"] = str(p)

    ref_img = left_img if left_img is not None else right_img
    h, w = ref_img.shape[:2]
    grid = plan_tile_grid(w, h, max_tiles=max_tiles, overlap=overlap)
    render_meta["tiles_total"] = len(grid)

    any_ok = any_attempt = False
    for i, g in enumerate(grid, start=1):
        bbox = g["bbox_norm"]
        lt = _save_tile(left_img, bbox, base / f"{item['item_id']}_{g['tile_id']}_left.png") if left_img is not None else None
        rt = _save_tile(right_img, bbox, base / f"{item['item_id']}_{g['tile_id']}_right.png") if right_img is not None else None
        tile = {"tile_id": g["tile_id"], "bbox_norm": bbox,
                "left_tile_ref": lt, "right_tile_ref": rt,
                "vision_status": "pending", "result": None}
        if vision_runner is None:
            tile["vision_status"] = "skipped_no_runner"
            render_meta["tiles"].append(tile)
            continue
        if lt is None and rt is None:
            tile["vision_status"] = "failed"
            tile["error"] = "tile render failed"
            render_meta["tiles"].append(tile)
            continue
        any_attempt = True
        prompt = build_tile_prompt(i, len(grid), bbox)
        try:
            raw = vision_runner(prompt, lt, rt,
                                {**runner_options, "tile": g["tile_id"],
                                 "render_long_side": tile_long})
            result, _w = normalize_vision_runner_result(raw)
        except Exception as exc:  # noqa: BLE001 — плитка не валит item
            tile["vision_status"] = "failed"
            tile["error"] = f"{type(exc).__name__}: {exc}"
            render_meta["tiles"].append(tile)
            continue
        if result is None:
            tile["vision_status"] = "failed"
        else:
            tile["vision_status"] = "ok"
            tile["result"] = result
            any_ok = True
        render_meta["tiles"].append(tile)

    if vision_runner is None:
        return "skipped_no_runner"
    if not any_attempt:
        return "failed"
    agg = aggregate_tile_results(render_meta["tiles"])
    if agg is None:
        return "failed"
    item["result"] = agg
    return "ok"


# ─── main entry ──────────────────────────────────────────────────────────────


def run_graphic_vision_enrichment(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None, overrides_report: Any = None,
        options: Optional[dict] = None,
        vision_runner: Optional[VisionRunner] = None,
        crops_dir: Optional[str | Path] = None) -> dict:
    """Полный прогон слоя: план → (опц.) рендер кропов → (опц.) vision.

    ``vision_runner=None`` → ``skipped_no_runner``: items с prompt/crop refs
    записаны, реальных вызовов нет. Ошибки runner'а/рендера — per-item
    fail-soft. ``overrides_report`` — прочитанный entity_mapping_overrides.json
    (опц.); влияет на отбор только при ``use_entity_mapping_overrides=true``.
    """
    plan = build_graphic_vision_enrichment_plan(
        left_model, right_model, visual_gate_report,
        left_graphic_report=left_graphic_report,
        right_graphic_report=right_graphic_report,
        graphic_matched_report=graphic_matched_report,
        overrides_report=overrides_report,
        options=options)

    warnings = list(plan.get("warnings") or [])
    items = plan.get("items") or []
    stats = plan.get("stats") or {}
    render_opts, render_warnings = _render_options(options)
    if items:
        # render-warnings (tiled fallback / invalid mode) релевантны и для
        # plan-only прогонов (skipped_no_runner) — оператор должен видеть,
        # что запрошенный режим не реализован, до реального запуска
        warnings.extend(render_warnings)
    runner_options = {"model": _opt(options, "runner_model"),
                      "render_long_side": render_opts["long_side"],
                      "render_mode": render_opts["mode"]}
    render_crops = _opt(options, "render_crops") is not False

    attempted = succeeded = failed = skipped = 0

    if plan.get("status") != "skipped_no_visual_gate":
        left_pages = _pages_by_number(left_model)
        right_pages = _pages_by_number(right_model)
        left_pdf = _model_pdf_path(left_model)
        right_pdf = _model_pdf_path(right_model)
        left_blocks = _blocks_by_id(left_model)
        right_blocks = _blocks_by_id(right_model)

        prompts = plan.get("prompts_by_item_id") or {}
        for item in items:
            # планируемая геометрия рендера — у ВСЕХ items (и plan-only):
            # консьюмер отчёта может восстановить, чем рендерили бы
            long_side = _item_render_long_side(item, render_opts)
            item["render_long_side_used"] = long_side

            # tiled-ветка (только для плотных типов; остальные эффективно
            # high_res — см. _item_effective_render_mode). Обрабатывает и
            # no-runner (план плиток без вызовов), и runner per-tile.
            if _item_effective_render_mode(item, render_opts) == "tiled":
                st = _run_tiled_item(
                    item,
                    left_block=left_blocks.get(item["left_block_id"]),
                    right_block=right_blocks.get(item["right_block_id"]),
                    left_pages=left_pages, right_pages=right_pages,
                    left_pdf=left_pdf, right_pdf=right_pdf,
                    render_opts=render_opts, runner_options=runner_options,
                    crops_dir=Path(crops_dir) if crops_dir else None,
                    vision_runner=vision_runner)
                item["vision_status"] = st
                if st == "ok":
                    attempted += 1
                    succeeded += 1
                elif st == "skipped_no_runner":
                    skipped += 1
                else:  # failed
                    attempted += 1
                    failed += 1
                continue

            if vision_runner is None:
                item["vision_status"] = "skipped_no_runner"
                skipped += 1
                continue

            left_png = right_png = None
            if render_crops:
                if crops_dir is None:
                    # рендер запрошен, но писать некуда — честный fail item'а
                    # (а не тихий вызов runner'а без изображений)
                    item["warnings"].append("render_crops requested but "
                                            "crops_dir not provided")
                else:
                    base = Path(crops_dir)
                    left_png, lerr = _render_crop_png(
                        left_blocks.get(item["left_block_id"]), left_pages,
                        left_pdf, base / f"{item['item_id']}_left.png",
                        long_side)
                    right_png, rerr = _render_crop_png(
                        right_blocks.get(item["right_block_id"]), right_pages,
                        right_pdf, base / f"{item['item_id']}_right.png",
                        long_side)
                    for err in (lerr, rerr):
                        if err:
                            item["warnings"].append(err)
                # единая точка приоритета ссылок: rendered → image_file → pdf
                item["left_crop_ref"] = _crop_ref(
                    item.get("left_crop_source") or {}, left_png)
                item["right_crop_ref"] = _crop_ref(
                    item.get("right_crop_source") or {}, right_png)
                if left_png is None and right_png is None:
                    item["vision_status"] = "failed"
                    item["warnings"].append("no crop image available for "
                                            "vision call")
                    failed += 1
                    continue

            attempted += 1
            # полный prompt из плана (write_prompts влияет только на
            # персистенцию item["prompt"], не на вход runner'а)
            prompt = (prompts.get(item["item_id"]) or item.get("prompt")
                      or build_vision_prompt_for_block_pair(
                          {**item, "status": item.get("visual_status")},
                          graphic_type=item.get("graphic_type") or "unknown",
                          discipline=item.get("discipline") or "unknown"))
            try:
                # per-item эффективный long_side (dense high_res ≠ base)
                raw = vision_runner(prompt, left_png, right_png,
                                    {**runner_options,
                                     "render_long_side": long_side})
            except Exception as exc:  # noqa: BLE001 — runner не валит отчёт
                item["vision_status"] = "failed"
                item["warnings"].append(f"vision runner error: "
                                        f"{type(exc).__name__}: {exc}")
                failed += 1
                continue
            result, norm_warnings = normalize_vision_runner_result(raw)
            item["warnings"].extend(norm_warnings)
            if result is None:
                item["vision_status"] = "failed"
                failed += 1
            else:
                item["vision_status"] = "ok"
                item["result"] = result
                succeeded += 1

    if failed:
        # per-item сбои поднимаются на уровень отчёта (иначе dry-run их
        # не видит — warnings собираются только с верхнего уровня)
        warnings.append(f"vision items failed: {failed} of {len(items)}")

    # ── статус отчёта ──
    if plan.get("status") == "skipped_no_visual_gate":
        status = "skipped_no_visual_gate"
    elif vision_runner is None and items:
        status = "skipped_no_runner"
    elif vision_runner is not None and items and failed and not succeeded:
        # ВСЁ упало (включая render-фейлы до вызова) — это failed,
        # а не «предупреждения»
        status = "failed"
    elif failed or warnings or any(i.get("warnings") for i in items):
        status = "completed_with_warnings"
    else:
        status = "ok"

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": {
            "candidates_total": stats.get("candidates_total", 0),
            "selected_total": len(items),
            "excluded_by_visual_gate": stats.get("excluded_by_visual_gate", 0),
            "manual_review_included": stats.get("manual_review_included", 0),
            "manual_review_skipped": stats.get("manual_review_skipped", 0),
            "other_skipped": stats.get("other_skipped", 0),
            "dropped_by_cap": stats.get("dropped_by_cap", 0),
            "vision_calls_attempted": attempted,
            "vision_calls_succeeded": succeeded,
            "vision_calls_failed": failed,
            "skipped_no_runner": skipped,
            "runner_model": runner_options["model"],
            "candidate_selection": str(_opt(options, "candidate_selection")
                                       or "legacy"),
            "selection_mode": stats.get("selection_mode"),
            "by_candidate_kind": stats.get("by_candidate_kind") or {},
            "mismatch_excluded": stats.get("mismatch_excluded", 0),
            "render_mode": render_opts["mode"],
            "render_mode_requested": render_opts.get("mode_requested",
                                                     render_opts["mode"]),
            "tiled_items": sum(1 for i in items
                               if isinstance(i.get("render"), dict)
                               and i["render"].get("effective_mode") == "tiled"),
            "tiles_total": sum(int(i["render"].get("tiles_total") or 0)
                               for i in items
                               if isinstance(i.get("render"), dict)),
        },
        "items": items,
        "warnings": warnings,
    }


def write_graphic_vision_enrichment_report(out_path: str | Path,
                                           report: dict) -> Path:
    """Атомарно записать отчёт (tmp + os.replace)."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, out)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "VISION_PROMPT_TEMPLATE",
    "CANDIDATE_SAME",
    "CANDIDATE_MISMATCH",
    "CANDIDATE_VALIDATION",
    "CANDIDATE_UNCERTAIN",
    "extract_entity_ids",
    "entity_identity_signal",
    "sheet_kind_of",
    "score_vision_candidate",
    "select_vision_candidates_v2",
    "select_blocks_for_vision",
    "build_vision_prompt_for_block_pair",
    "build_graphic_vision_enrichment_plan",
    "normalize_vision_runner_result",
    "run_graphic_vision_enrichment",
    "write_graphic_vision_enrichment_report",
]
