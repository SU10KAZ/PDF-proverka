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
}

# render-опции (options["render"]): high_res поднимает long_side для
# плотных типов графики; tiled — зарезервированный контракт (см. доку)
_DEFAULT_RENDER_OPTIONS = {
    "mode": "normal",               # normal | high_res | tiled
    "long_side": 1600,
    "dense_long_side": 2400,
    "tile_long_side": 1400,
    "max_tiles": 6,
}
_DENSE_GRAPHIC_TYPES = {"cabinet_scheme", "single_line_scheme",
                        "dense_scheme", "table_scheme"}

CANDIDATE_SAME = "same_entity_likely"
CANDIDATE_MISMATCH = "mismatch_likely"
CANDIDATE_VALIDATION = "validation_candidate"
CANDIDATE_UNCERTAIN = "uncertain"

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
    if "stamp" in (str(left_desc.get("graphic_type") or "").lower(),
                   str(right_desc.get("graphic_type") or "").lower()):
        score -= 0.35
        risks.append("stamp_block_low_vision_value")

    score = max(0.0, min(1.0, round(score, 3)))

    hard_mismatch = any(r.startswith(("entity_id_conflict",
                                      "entity_family_conflict",
                                      "sheet_kind_mismatch")) for r in risks)
    # SAME требует корроборации (entity/вид листа), не только score:
    # blank-дескрипторные пары не должны проходить в enrichment по одним
    # gate-бонусам (ревью: позиционный срез reasons[2:] был артефактом)
    if hard_mismatch:
        kind = CANDIDATE_MISMATCH
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


def select_vision_candidates_v2(visual_gate_report: Any, *,
                                left_graphic_report: Any = None,
                                right_graphic_report: Any = None,
                                graphic_matched_report: Any = None,
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

    matched_idx = _matched_graphic_index(graphic_matched_report)
    stats = {"candidates_total": len(pairs), "excluded_by_visual_gate": 0,
             "manual_review_included": 0, "manual_review_skipped": 0,
             "other_skipped": 0, "dropped_by_cap": 0,
             "by_candidate_kind": {}, "mismatch_excluded": 0,
             "selection_mode": mode}

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
        verdict = score_vision_candidate(
            bp,
            left_desc=_descriptor_for(left_graphic_report, lid),
            right_desc=_descriptor_for(right_graphic_report, rid),
            matched_entry=matched_idx.get((lid, rid)))
        eligible.append({**bp, **verdict})
        k = verdict["candidate_kind"]
        stats["by_candidate_kind"][k] = stats["by_candidate_kind"].get(k, 0) + 1

    if mode == "link_validation":
        # цель — проверить подозрительные связи: mismatch/validation первыми
        prio = {CANDIDATE_MISMATCH: 0, CANDIDATE_VALIDATION: 1,
                CANDIDATE_UNCERTAIN: 2, CANDIDATE_SAME: 3}
        eligible.sort(key=lambda c: (prio.get(c["candidate_kind"], 9),
                                     -c["candidate_score"]))
    else:
        if exclude_mismatch:
            kept = [c for c in eligible
                    if c["candidate_kind"] != CANDIDATE_MISMATCH]
            stats["mismatch_excluded"] = len(eligible) - len(kept)
            eligible = kept
        prio = {CANDIDATE_SAME: 0, CANDIDATE_UNCERTAIN: 1,
                CANDIDATE_VALIDATION: 2, CANDIDATE_MISMATCH: 3}
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
        graphic_matched_report: Any = None,
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
                               "candidate_risk_flags") if k in bp}
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
    if isinstance(raw, dict):
        for k in out:
            if k in raw:
                out[k] = raw[k] if k == "mode" else _safe_int(raw[k], out[k])
    mode = str(out.get("mode") or "normal").strip().lower()
    if mode not in _VALID_RENDER_MODES:
        warnings.append(f"invalid render mode {out.get('mode')!r} → normal")
        mode = "normal"
    out["mode_requested"] = mode
    if mode == "tiled":
        warnings.append("render mode 'tiled' not implemented yet — "
                        "falling back to high_res")
        mode = "high_res"
    out["mode"] = mode
    return out, warnings


def _item_render_long_side(item: dict, render_opts: dict) -> int:
    """long_side рендера для item'а (mode уже эффективный)."""
    base = _safe_int(render_opts.get("long_side"), 1600)
    if (render_opts.get("mode") == "high_res"
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


# ─── main entry ──────────────────────────────────────────────────────────────


def run_graphic_vision_enrichment(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        graphic_matched_report: Any = None,
        options: Optional[dict] = None,
        vision_runner: Optional[VisionRunner] = None,
        crops_dir: Optional[str | Path] = None) -> dict:
    """Полный прогон слоя: план → (опц.) рендер кропов → (опц.) vision.

    ``vision_runner=None`` → ``skipped_no_runner``: items с prompt/crop refs
    записаны, реальных вызовов нет. Ошибки runner'а/рендера — per-item
    fail-soft.
    """
    plan = build_graphic_vision_enrichment_plan(
        left_model, right_model, visual_gate_report,
        left_graphic_report=left_graphic_report,
        right_graphic_report=right_graphic_report,
        graphic_matched_report=graphic_matched_report,
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
