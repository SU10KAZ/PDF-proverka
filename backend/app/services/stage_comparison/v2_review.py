"""V2-режим вкладки «Расхождения» — pair-scoped список изменений + ручная
верификация инженера.

Зачем отдельный слой поверх `unified_findings.build_unified_flat`:

    1. **Scope = ТОЛЬКО текущая PDF-пара.** V2 никогда не подмешивает
       изменения других пар сессии. Источник данных — существующий
       `comparison_result.json` конкретной пары (через build_unified_flat
       с `pair_id`), без запуска Qwen/Opus/unified-analysis.

    2. **Ручные статусы хранятся отдельно.** `comparison_result.json` —
       production-артефакт, его мутировать нельзя. Решения инженера
       (подтверждено/отклонено/комментарий/…) лежат в
       `pairs/<pid>/v2_review_status.json` и накладываются на лету.

    3. **Стабильный id.** Чтобы статус «прилипал» к изменению между
       перестроениями списка, id детерминирован:
       `v2_<sha1(pair_id :: raw_id|content)>`. Если у изменения есть
       стабильный `chg_…` id — он берётся за основу; иначе хэшируется
       контент (title + old/new + evidence). build_unified_flat для
       безымянных изменений генерирует случайный `uf_…` id — его мы НЕ
       используем как основу (он не стабилен).

    4. **Quality label не выдумывается.** В production `comparison_result`
       нет поля `quality_label`, поэтому метка ВЫВОДИТСЯ детерминированно
       из реальных полей: `requires_human_review`, `evidence_verified`
       (если есть в fallback-changes) и `confidence`. Ничего не
       синтезируется «из воздуха».
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime
from typing import Any, Optional

from . import paths as paths_mod
from . import unified_findings as unified_findings_mod

logger = logging.getLogger(__name__)

VERSION = 1
_lock = threading.RLock()

# Допустимые статусы ручной верификации (review_status).
VALID_REVIEW_STATUSES = {
    "not_reviewed",
    "confirmed",
    "rejected",
    "needs_clarification",
    "cost_impact",
    "no_cost_impact",
    "send_to_designer",
    "send_to_estimate",
}

# Метки качества, которые мы умеем выводить детерминированно.
QUALITY_LABELS = {
    "good",
    "needs_human_review",
    "questionable",
}

# Маппинг legacy-вердиктов эксперта («Расхождения», expert_review.json) в
# review_status V2. Используется ТОЛЬКО как fallback на чтении, когда у строки
# ещё нет канонического статуса в v2_review_status.json (см. _resolve_review).
_EXPERT_DECISION_TO_STATUS = {
    "accepted": "confirmed",
    "rejected": "rejected",
}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Stable id ───────────────────────────────────────────────────────────


def _evidence_quote(value: Any) -> str:
    """Извлекает текстовую цитату из evidence_left/right (dict|str|None)."""
    if isinstance(value, dict):
        return str(value.get("quote") or "")
    if value is None:
        return ""
    return str(value)


def make_v2_id(pair_id: str, item: dict) -> str:
    """Детерминированный стабильный id изменения в рамках пары.

    Приоритет — стабильный `chg_…` id из comparison_result. Если его нет
    (build_unified_flat подставил случайный `uf_…`), хэшируем контент.
    """
    raw_id = str(item.get("id") or "").strip()
    if raw_id and not raw_id.startswith("uf_"):
        base = raw_id
    else:
        base = "".join([
            str(item.get("title") or ""),
            str(item.get("old_value") or ""),
            str(item.get("new_value") or ""),
            _evidence_quote(item.get("evidence_left")),
            _evidence_quote(item.get("evidence_right")),
        ])
    digest = hashlib.sha1(f"{pair_id}::{base}".encode("utf-8")).hexdigest()[:16]
    return f"v2_{digest}"


# ─── Impact classification (инженерная значимость vs admin/оформление/шум) ──

# Все допустимые значения impact_class.
IMPACT_CLASSES = {
    # инженерно значимые (остаются в основной V2-ведомости):
    "construction_cost_impact",
    "construction_technical_impact",
    "procurement_impact",
    "schedule_or_risk_impact",
    "design_solution_impact",
    "engineering_system_impact",
    "manual_review_required",
    # исключаемые из основной ведомости:
    "admin_only",
    "documentation_only",
    "cosmetic_or_noise",
    # не удалось классифицировать — НЕ исключаем (консервативно):
    "unknown",
}

# Классы, скрываемые из основной инженерной ведомости.
EXCLUDED_IMPACT_CLASSES = {"admin_only", "documentation_only", "cosmetic_or_noise"}

# Типы изменений из comparison_result, заведомо инженерные.
_ENGINEERING_TYPES = {
    "material_changed", "equipment_changed", "calculation_changed",
    "requirement_changed", "design_logic_changed", "scheme_sequence_changed",
}

# Ключевые маркеры (нормализованный текст: lower + ё→е). Инженерная значимость
# имеет ПРИОРИТЕТ над admin/cosmetic — см. classify_impact. ВАЖНО: маркеры
# должны быть достаточно длинными/специфичными, чтобы не ловить случайные
# подстроки (например «ось» ловит «изменилОСЬ», «вру» — «вРУчную»).
_ENGINEERING_MARKERS = (
    "материал", "оборудован", "кабел", "автомат", "выключател", "насос",
    "схем", "однолинейн", "нагрузк", "расчет", "геометр", "толщин",
    "сечени", "диаметр", "бетон", "армат", "арматур", "класс прочн",
    "вентиляц", "отоплен", "водоснабж", "канализац", "электроснабж",
    "освещен", "заземлен", "молниезащит", "дренаж", "трубопровод", "труба",
    "задвижк", "клапан", "фундамент", "перекрыти", "монолит", "колонн",
    "ригел", "узел", "узл ", "щит", "распределительн",
    "вру-", "вру ", "врп-", "щр-", "що-", "грщ", "авр", "кровл",
    "мощност", "ампер", "квт", "напряжен", "вольт", "давлен",
    "расход", "спецификац", "ведомость материал", "отметк",
    "ведомость оборудован", " мм", "мм2", "мм²", "м3", "м³",
    "производительн", "этаж", "помещен", "проем", "лестниц",
)

# Маркеры административных / реквизитных изменений.
_ADMIN_MARKERS = (
    "штамп", "стади", "организац", "заказчик", "разработчик", "подрядчик",
    "гип", "гап", "главный инженер проекта", "главный архитектор",
    "ген. директор", "генеральный директор", "директор", "подпис",
    "разраб.", "пров.", "н.контр", "нормоконтр", "утвердил", "согласов",
    "реквизит", "инн", "огрн", "адрес", "телефон", "лицензи", "сро",
    "наименование организации", "правообладател",
)
# Маркеры «только оформление документации».
_DOCUMENTATION_MARKERS = (
    "шифр", "номер листа", "№ листа", "лист ", "обозначение документа",
    "титульн", "титул", "дата", "год выпуска", "год издания",
    "ведомость изменени", "состав тома", "содержание тома", "содержание",
    "оглавлен", "номер тома", " том ", "номер раздела", "наименование раздела",
    "колонтитул", "рамк", "основная надпись",
)
# Маркеры косметики / шума / переформулировки / OCR.
_COSMETIC_MARKERS = (
    "переформулир", "формулировк", "опечатк", "орфограф", "пунктуац",
    "регистр букв", "порядок слов", "порядок текста", "порядок строк",
    "перестановк", "layout", "лэйаут", "макет", "верстк",
    "пробел", "перенос строк", "ocr", "распознаван", "артефакт распознав",
    "значение не изменилось", "без изменения смысла", "смысл не изменил",
    "идентичн", "тождествен", "только формулировк", "только оформлен",
    "косметич",
)

# «Жёсткие» маркеры отсутствия смысловых изменений — перебивают слабое
# инженерное ключевое слово (но НЕ severity=high / cost_impact).
_HARD_NOISE_MARKERS = (
    "значение не изменилось", "значения не изменились", "без изменения смысла",
    "смысл не изменился", "смысл не изменилс", "по сути не изменил",
    "фактически не изменил",
)
# «Сильные» однозначно-административные / документационные маркеры. Изменение,
# содержащее их и НЕ содержащее ни одного инженерного маркера, не влияет на
# стройку независимо от того, как Opus оценил severity (он систематически
# переоценивает админ-изменения как high). Поэтому такие изменения исключаются
# даже при severity=high — но НЕ при cost_impact possible/likely (там оставляем
# на ручную проверку: вдруг есть реальная стоимость).
_STRONG_ADMIN_MARKERS = (
    "организац", "заказчик", "застройщик", "разработчик", "подрядчик",
    "гип", "гап", "главный инженер проекта", "главный архитектор",
    "генеральный директор", "ген. директор", "директор", "подпис",
    "штамп", "реквизит", "нормоконтр", "согласован", "заверение",
    "правообладател",
)
_STRONG_DOC_MARKERS = (
    "шифр", "номер тома", "номер раздела", "состав тома", "содержание тома",
    "перечень отклонени", "положительн заключени", "заключени экспертиз",
    "титульн", "титул",
)

# Маркеры стоимости / закупки / сроков — уточняют инженерный подкласс.
_COST_MARKERS = ("стоимост", "цена", "цены", "смет", "объем работ", "объём работ", "удорожан")
_PROCUREMENT_MARKERS = ("закуп", "поставк", "поставщик", "вендор", "производител", "аналог", "замен")
_SCHEDULE_MARKERS = ("срок", "график", "очередност", "этап строит", "риск")
_DESIGN_MARKERS = ("проектн решени", "компоновк", "планировк", "трасс", "расположен",
                   "конструктивн решени", "технологическ решени")
_SYSTEM_MARKERS = ("вентиляц", "отоплен", "водоснабж", "канализац",
                   "электроснабж", "слаботочн", "схем", "кабел",
                   "щит", "насос", "нагрузк", "инженерн систем")


def _norm_text(*parts: Any) -> str:
    return " ".join(str(p or "") for p in parts).lower().replace("ё", "е")


def _has(text: str, markers) -> bool:
    return any(m in text for m in markers)


def _engineering_subclass(text: str, typ: str, cost: str) -> str:
    """Выбрать инженерный подкласс по маркерам (детерминированно)."""
    if cost in ("possible", "likely") or _has(text, _COST_MARKERS):
        # стоимость/закупка разводятся по маркерам
        if _has(text, _PROCUREMENT_MARKERS):
            return "procurement_impact"
        return "construction_cost_impact"
    if _has(text, _PROCUREMENT_MARKERS):
        return "procurement_impact"
    if _has(text, _SCHEDULE_MARKERS):
        return "schedule_or_risk_impact"
    if typ in ("scheme_sequence_changed", "equipment_changed") or _has(text, _SYSTEM_MARKERS):
        return "engineering_system_impact"
    if typ in ("design_logic_changed", "requirement_changed") or _has(text, _DESIGN_MARKERS):
        return "design_solution_impact"
    return "construction_technical_impact"


# Человекочитаемые причины исключения (для аудита и UI).
_EXCLUSION_REASONS = {
    "admin_only": "Административное изменение (штамп/организация/реквизиты/подписи) — "
                  "не влияет на строительство, стоимость, закупки, сроки или проектные решения.",
    "documentation_only": "Изменение касается только оформления документации "
                          "(шифр/лист/титул/дата/состав тома) без технического содержания.",
    "cosmetic_or_noise": "Косметическое изменение / OCR-шум / переформулировка / порядок "
                         "текста / layout без изменения проектного смысла.",
}


def classify_impact(item: dict) -> tuple[str, Optional[str]]:
    """Детерминированно определить impact_class и причину исключения.

    Возвращает (impact_class, exclusion_reason|None). exclusion_reason
    заполнен только для admin_only / documentation_only / cosmetic_or_noise.

    Приоритеты (см. ТЗ):
      1. Инженерные маркеры/типы → инженерный класс (НЕ admin).
      2. severity=high или cost_impact possible/likely → не относить к
         admin/cosmetic автоматически (→ manual_review_required).
      5. requires_human_review и не явно admin/cosmetic → manual_review_required.
    """
    typ = str(item.get("type") or "").lower()
    sev = str(item.get("severity") or "").lower()
    cost = str(item.get("cost_impact") or "").lower()
    rhr = bool(item.get("requires_human_review"))
    text = _norm_text(
        item.get("title"), item.get("summary"),
        item.get("old_value"), item.get("new_value"),
        _evidence_quote(item.get("evidence_left")),
        _evidence_quote(item.get("evidence_right")),
        item.get("category"), typ,
    )

    eng = (typ in _ENGINEERING_TYPES) or _has(text, _ENGINEERING_MARKERS)
    admin = (typ == "stamp_changed") or _has(text, _ADMIN_MARKERS)
    documentation = _has(text, _DOCUMENTATION_MARKERS)
    cosmetic = _has(text, _COSMETIC_MARKERS)
    hard_noise = _has(text, _HARD_NOISE_MARKERS)
    high_or_cost = (sev == "high") or (cost in ("possible", "likely"))

    # 0. Явное «смысл/значение не изменилось» перебивает слабый инженерный
    #    маркер (но НЕ высокую важность / влияние на стоимость).
    if hard_noise and not high_or_cost:
        return "cosmetic_or_noise", _EXCLUSION_REASONS["cosmetic_or_noise"]

    # 1. Инженерная значимость — высший приоритет, никогда не исключаем.
    if eng:
        return _engineering_subclass(text, typ, cost), None

    # 1b. «Сильное» админ/документационное изменение без инженерного контента и
    #     без признака стоимости — исключаем даже при severity=high (Opus
    #     переоценивает админ-изменения; инженер их стабильно бракует как
    #     «нет влияния на стройку»). cost possible/likely оставляем на ручную.
    cost_present = cost in ("possible", "likely")
    strong_admin = _has(text, _STRONG_ADMIN_MARKERS)
    strong_doc = _has(text, _STRONG_DOC_MARKERS)
    if (strong_admin or strong_doc) and not cost_present:
        if strong_doc and not strong_admin:
            return "documentation_only", _EXCLUSION_REASONS["documentation_only"]
        return "admin_only", _EXCLUSION_REASONS["admin_only"]

    # 2/4. Высокая важность или влияние на стоимость — не прячем автоматически.
    if high_or_cost:
        return "manual_review_required", None

    # 3. Административное / оформительское — исключаем.
    if admin or documentation:
        # documentation_only, если есть оформительский маркер и нет «жёсткого»
        # admin-реквизита; иначе admin_only.
        if documentation and not admin:
            cls = "documentation_only"
        else:
            cls = "admin_only"
        return cls, _EXCLUSION_REASONS[cls]

    # 3. Косметика / шум — исключаем.
    if cosmetic:
        return "cosmetic_or_noise", _EXCLUSION_REASONS["cosmetic_or_noise"]

    # 5. Требует ручной проверки и не классифицировано как admin/cosmetic.
    if rhr:
        return "manual_review_required", None

    # Не удалось классифицировать — оставляем в ведомости (консервативно).
    return "unknown", None


# ─── Quality label (выводится, не выдумывается) ──────────────────────────


def derive_quality_label(item: dict) -> str:
    """Вывести метку качества из реальных полей изменения.

    - requires_human_review=True       → needs_human_review
    - disputed=True                     → questionable (r5: спорная дельта Opus)
    - evidence_verified is False        → questionable (есть в fallback-changes)
    - 0 < confidence < 0.5              → questionable
    - иначе                             → good
    """
    if bool(item.get("requires_human_review")):
        return "needs_human_review"
    if bool(item.get("disputed")):
        return "questionable"
    ev = item.get("evidence_verified")
    if ev is False:
        return "questionable"
    try:
        conf = float(item.get("confidence") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    if 0.0 < conf < 0.5:
        return "questionable"
    return "good"


# ─── Persisted manual statuses ───────────────────────────────────────────


def _empty_status_file() -> dict:
    return {"version": VERSION, "updated_at": None, "items": {}}


def _read_status_file(session_id: str, pair_id: str) -> dict:
    p = paths_mod.v2_review_status_path(session_id, pair_id)
    if not p.exists():
        return _empty_status_file()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("not a dict")
        if not isinstance(data.get("items"), dict):
            data["items"] = {}
        data.setdefault("version", VERSION)
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return _empty_status_file()


def _write_status_file(session_id: str, pair_id: str, payload: dict) -> dict:
    p = paths_mod.v2_review_status_path(session_id, pair_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload.setdefault("version", VERSION)
    payload["updated_at"] = _utc_now()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


# ─── Legacy expert_review.json fallback (read-only) ──────────────────────────


def _read_expert_decisions(session_id: str) -> dict:
    """Прочитать решения эксперта из expert_review.json (read-only).

    Возвращает map составных ключей `<pair_id>::<raw_id>` → entry. Используется
    как fallback для review_status, когда у строки нет канонического статуса в
    v2_review_status.json (приоритет по ТЗ: v2_review_status → expert_review →
    not_reviewed). Никогда не пишет и не мутирует expert_review.json. Fail-soft:
    любая ошибка чтения → пустой map (строки остаются not_reviewed).
    """
    try:
        from . import expert_review as expert_review_mod  # lazy: избегаем цикла
        data = expert_review_mod.load(session_id) or {}
        decisions = data.get("decisions") or {}
        return decisions if isinstance(decisions, dict) else {}
    except Exception:  # noqa: BLE001 — fallback не должен ронять build
        logger.debug("v2_review: expert_review fallback unavailable for %s", session_id)
        return {}


def _expert_key(pair_id: str, cid: str) -> str:
    """Составной ключ expert_review.json `<pair_id>::<raw_id>`."""
    return f"{str(pair_id).strip()}::{str(cid).strip()}"


# ─── Build pair-scoped V2 changes ────────────────────────────────────────


def _resolve_review(pair_id: str, v2_id: str, raw_id: str,
                    status_map: dict, expert_map: Optional[dict]) -> dict:
    """Разрешить review_status/comment по приоритету (ТЗ Задача 2):

      1. v2_review_status.json (канонический) — если статус явный (не not_reviewed);
      2. expert_review.json (legacy «Расхождения») — accepted→confirmed,
         rejected→rejected; comment ← rejection_reason; НЕ перезаписывает (1);
      3. default not_reviewed.

    `review_source` показывает, откуда взят статус: `v2_review_status` |
    `expert_review` | `none`. Сама expert_review.json не мутируется.
    """
    st = status_map.get(v2_id) if isinstance(status_map.get(v2_id), dict) else {}
    review_status = str((st or {}).get("review_status") or "not_reviewed")
    if review_status not in VALID_REVIEW_STATUSES:
        review_status = "not_reviewed"
    review_comment = str((st or {}).get("review_comment") or "")
    reviewed_by = str((st or {}).get("reviewed_by") or "")
    reviewed_at = str((st or {}).get("reviewed_at") or "")
    review_source = "v2_review_status" if review_status != "not_reviewed" else "none"

    # Fallback на legacy-вердикт эксперта, только если канонического статуса нет.
    if review_status == "not_reviewed" and expert_map:
        fb = None
        # v2-native решение приоритетнее классического (по raw_id) двойника.
        for cid in (v2_id, raw_id):
            if not cid:
                continue
            cand = expert_map.get(_expert_key(pair_id, cid))
            if isinstance(cand, dict) and (cand.get("decision") or "").lower() in _EXPERT_DECISION_TO_STATUS:
                fb = cand
                break
        if fb:
            review_status = _EXPERT_DECISION_TO_STATUS[(fb.get("decision") or "").lower()]
            review_source = "expert_review"
            if not review_comment:
                review_comment = str(fb.get("rejection_reason") or "")
            if not reviewed_by:
                reviewed_by = str(fb.get("reviewer") or "")
            if not reviewed_at:
                reviewed_at = str(fb.get("timestamp") or "")

    return {
        "review_status": review_status,
        "review_comment": review_comment,
        "reviewed_by": reviewed_by,
        "reviewed_at": reviewed_at,
        "review_source": review_source,
    }


def _flat_item_to_v2(pair_id: str, it: dict, status_map: dict,
                     expert_map: Optional[dict] = None) -> dict:
    v2_id = make_v2_id(pair_id, it)
    quality = derive_quality_label(it)
    impact_class, exclusion_reason = classify_impact(it)
    excluded = impact_class in EXCLUDED_IMPACT_CLASSES
    raw_id = str(it.get("id") or "").strip()
    review = _resolve_review(pair_id, v2_id, raw_id, status_map, expert_map)
    return {
        "id": v2_id,
        "pair_id": pair_id,
        "raw_id": str(it.get("id") or ""),
        "pair_label": str(it.get("pair_label") or ""),
        "sheet": it.get("sheet") or "",
        "page": it.get("page"),
        # Характер изменения (усложнение/упрощение/нейтрально) — для чипа в «№»,
        # как в классическом виде «Расхождения».
        "change_direction": str(it.get("change_direction") or "unknown"),
        # Денежный эффект (удорожание/удешевление/нейтрально) — чип в «№».
        "cost_direction": str(it.get("cost_direction") or "unknown"),
        # location-поля сохраняются для кнопки «Перейти к месту».
        "left_page": it.get("left_page"),
        "right_page": it.get("right_page"),
        "alignment_slot": it.get("alignment_slot"),
        "source_layer": str(it.get("source_layer") or "text"),
        "type": str(it.get("type") or "changed"),
        "category": str(it.get("category") or "general"),
        "severity": str(it.get("severity") or "unknown"),
        "title": str(it.get("title") or ""),
        "summary": str(it.get("summary") or ""),
        "old_value": str(it.get("old_value") or ""),
        "new_value": str(it.get("new_value") or ""),
        "construction_impact": str(it.get("construction_impact") or ""),
        "cost_impact": str(it.get("cost_impact") or "unknown"),
        "evidence_left": _evidence_quote(it.get("evidence_left")),
        "evidence_right": _evidence_quote(it.get("evidence_right")),
        "quality_label": quality,
        # Инженерная значимость: основной фильтр V2-ведомости.
        "impact_class": impact_class,
        "excluded_from_main": excluded,
        "exclusion_reason": exclusion_reason or "",
        "requires_human_review": bool(it.get("requires_human_review") or False),
        "confidence": float(it.get("confidence") or 0.0),
        "review_status": review["review_status"],
        "review_comment": review["review_comment"],
        "reviewed_by": review["reviewed_by"],
        "reviewed_at": review["reviewed_at"],
        # Откуда взят статус: v2_review_status | expert_review | none.
        "review_source": review["review_source"],
        # Non-destructive merge: новая находка свежего сравнения (бейдж «NEW»).
        "is_new": bool(it.get("is_new") or False),
        "change_origin": str(it.get("change_origin") or ""),
    }


def _build_all_v2_items(session_id: str, pair_id: str) -> list[dict]:
    """Полный список V2-изменений пары (со всеми impact_class), без фильтра."""
    flat = unified_findings_mod.build_unified_flat(session_id, pair_id=pair_id)
    raw_items = flat.get("items") or []
    status_map = (_read_status_file(session_id, pair_id) or {}).get("items") or {}
    # Legacy-вердикты «Расхождений» как fallback для review_status (read-only).
    expert_map = _read_expert_decisions(session_id)

    items: list[dict] = []
    seen_ids: set[str] = set()
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        v2 = _flat_item_to_v2(pair_id, it, status_map, expert_map)
        # Коллизия id (одинаковый контент) — добавим суффикс по порядку,
        # чтобы строки не схлопывались в таблице.
        if v2["id"] in seen_ids:
            v2["id"] = f"{v2['id']}_{len(items)}"
        seen_ids.add(v2["id"])
        items.append(v2)
    return items


def _write_excluded_changes_file(session_id: str, pair_id: str, excluded_items: list[dict]) -> None:
    """Записать аудит-снимок исключённых изменений (derived, идемпотентно).

    НЕ трогает comparison_result.json. Fail-soft: ошибка записи не валит build.
    """
    payload = {
        "version": VERSION,
        "updated_at": _utc_now(),
        "items": {
            it["id"]: {
                "impact_class": it.get("impact_class"),
                "exclusion_reason": it.get("exclusion_reason") or "",
                "source_title": it.get("title") or "",
                "old_value": it.get("old_value") or "",
                "new_value": it.get("new_value") or "",
                "sheet": it.get("sheet") or "",
                "page": it.get("page"),
                "review_status": it.get("review_status") or "not_reviewed",
            }
            for it in excluded_items
        },
    }
    try:
        p = paths_mod.v2_excluded_changes_path(session_id, pair_id)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)
    except OSError:  # pragma: no cover — аудит-снимок не критичен для ответа
        logger.warning("v2_review: failed to write excluded audit file for %s/%s",
                       session_id, pair_id)


def build_pair_v2_changes(session_id: str, pair_id: str, include_excluded: bool = False) -> dict:
    """Собрать V2-список изменений ТОЛЬКО для одной PDF-пары.

    Read-only по отношению к comparison_result. Накладывает сохранённые
    ручные статусы. По умолчанию (`include_excluded=False`) из `items`
    исключены административные / только-оформление / косметика-шум —
    остаётся инженерно значимая ведомость. `include_excluded=True` возвращает
    всё (у каждого item есть `impact_class`, `excluded_from_main`,
    `exclusion_reason`).

    Побочный эффект: пишет аудит-снимок исключённых в
    `v2_excluded_changes.json` (derived, идемпотентно). Бросает KeyError,
    если сессия не найдена.
    """
    all_items = _build_all_v2_items(session_id, pair_id)
    excluded_items = [it for it in all_items if it.get("excluded_from_main")]
    kept_items = [it for it in all_items if not it.get("excluded_from_main")]

    # Аудит-снимок исключённых (не теряем их при скрытии из ведомости).
    _write_excluded_changes_file(session_id, pair_id, excluded_items)

    returned = all_items if include_excluded else kept_items
    summary = compute_summary(returned)
    summary.update(_exclusion_breakdown(all_items, kept_items, excluded_items))

    return {
        "session_id": session_id,
        "pair_id": pair_id,
        "include_excluded": bool(include_excluded),
        "summary": summary,
        "items": returned,
    }


def _exclusion_breakdown(all_items: list[dict], kept_items: list[dict],
                         excluded_items: list[dict]) -> dict:
    """Разбивка по impact_class для summary (всегда по полному набору)."""
    by_class: dict[str, int] = {}
    for it in all_items:
        cls = str(it.get("impact_class") or "unknown")
        by_class[cls] = by_class.get(cls, 0) + 1
    return {
        "engineering_total": len(kept_items),
        "excluded_total": len(excluded_items),
        "excluded_admin_only": by_class.get("admin_only", 0),
        "excluded_documentation_only": by_class.get("documentation_only", 0),
        "excluded_cosmetic_or_noise": by_class.get("cosmetic_or_noise", 0),
        "impact_class_counts": by_class,
    }


def compute_summary(items: list[dict]) -> dict:
    summary = {
        "total": 0,
        "high": 0, "medium": 0, "low": 0,
        "good": 0, "needs_human_review": 0, "questionable": 0,
        "confirmed": 0, "rejected": 0, "not_reviewed": 0,
    }
    for it in items:
        summary["total"] += 1
        sev = str(it.get("severity") or "").lower()
        if sev in ("high", "medium", "low"):
            summary[sev] += 1
        ql = str(it.get("quality_label") or "")
        if ql in ("good", "needs_human_review", "questionable"):
            summary[ql] += 1
        rs = str(it.get("review_status") or "not_reviewed")
        if rs == "confirmed":
            summary["confirmed"] += 1
        elif rs == "rejected":
            summary["rejected"] += 1
        elif rs == "not_reviewed":
            summary["not_reviewed"] += 1
    return summary


# ─── Mutations (manual review statuses) ──────────────────────────────────


def _apply_patch_to_entry(entry: dict, patch: dict) -> dict:
    """Применить частичный patch к одной записи статуса. Идемпотентно."""
    out = dict(entry or {})
    touched = False
    if "review_status" in patch and patch["review_status"] is not None:
        rs = str(patch["review_status"])
        if rs not in VALID_REVIEW_STATUSES:
            raise ValueError(f"invalid review_status: {rs}")
        out["review_status"] = rs
        touched = True
    if "review_comment" in patch and patch["review_comment"] is not None:
        out["review_comment"] = str(patch["review_comment"])
        touched = True
    if "reviewed_by" in patch and patch["reviewed_by"] is not None:
        out["reviewed_by"] = str(patch["reviewed_by"])
        touched = True
    if touched:
        out["reviewed_at"] = _utc_now()
    return out


def patch_change(session_id: str, pair_id: str, change_id: str, patch: dict) -> dict:
    """Обновить статус одного изменения. Возвращает обновлённую запись.

    Бросает KeyError, если change_id не принадлежит текущей паре —
    защищает от записи статусов на «фантомные» id.
    """
    with _lock:
        # include_excluded=True: ручной статус можно ставить и на исключённые
        # строки (их не теряем — они видны при include_excluded=true в UI).
        built = build_pair_v2_changes(session_id, pair_id, include_excluded=True)
        valid_ids = {it["id"] for it in built["items"]}
        if change_id not in valid_ids:
            raise KeyError(change_id)
        data = _read_status_file(session_id, pair_id)
        entry = _apply_patch_to_entry(data["items"].get(change_id) or {}, patch)
        data["items"][change_id] = entry
        _write_status_file(session_id, pair_id, data)
        return entry


def bulk_patch(session_id: str, pair_id: str, ids: list[str], patch: dict) -> dict:
    """Пакетное обновление статусов. Применяется ТОЛЬКО к id текущей пары.

    Возвращает {"updated": [ids], "skipped": [ids]}.
    """
    with _lock:
        built = build_pair_v2_changes(session_id, pair_id, include_excluded=True)
        valid_ids = {it["id"] for it in built["items"]}
        data = _read_status_file(session_id, pair_id)
        updated: list[str] = []
        skipped: list[str] = []
        for cid in (ids or []):
            cid = str(cid)
            if cid not in valid_ids:
                skipped.append(cid)
                continue
            data["items"][cid] = _apply_patch_to_entry(data["items"].get(cid) or {}, patch)
            updated.append(cid)
        if updated:
            _write_status_file(session_id, pair_id, data)
        return {"updated": updated, "skipped": skipped}


__all__ = [
    "VERSION",
    "VALID_REVIEW_STATUSES",
    "QUALITY_LABELS",
    "IMPACT_CLASSES",
    "EXCLUDED_IMPACT_CLASSES",
    "make_v2_id",
    "derive_quality_label",
    "classify_impact",
    "build_pair_v2_changes",
    "compute_summary",
    "patch_change",
    "bulk_patch",
]
