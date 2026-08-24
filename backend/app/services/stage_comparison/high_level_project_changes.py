"""Stage 5.3 high-level synthesis over immutable Stage 4/5 text evidence.

The module deliberately does not read PDFs or mutate an earlier artifact.  It
pre-groups Stage 5 atomic evidence, suppresses service/detail-only noise, and
only asks the production model about coherent groups which can still affect a
project-level conclusion.  Every published statement is rebuilt and checked
against the selected evidence by the backend.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
import re
import unicodedata
from typing import Any, Iterable

from . import project_change_summary


VERSION = 1
SCHEMA_VERSION = "1.0"
KIND = "stage_comparison_high_level_project_changes"
PROMPT_VERSION = "stage5_3_high_level_synthesis_v1"
VALIDATOR_VERSION = "stage5_3_high_level_validator_v1"
PRODUCTION_MODEL = project_change_summary.PRODUCTION_MODEL
PRODUCTION_REASONING_EFFORT = project_change_summary.PRODUCTION_REASONING_EFFORT

CONFIRMED = "CONFIRMED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"
MATERIAL_REVIEW = "MATERIAL_REVIEW"
NON_MATERIAL_REVIEW = "NON_MATERIAL_REVIEW"
SOURCE_LINK_UNCERTAIN = "SOURCE_LINK_UNCERTAIN"

HIGH_LEVEL_TYPES = {
    "DESIGN_PRINCIPLE_CHANGED",
    "SYSTEM_OPERATION_CHANGED",
    "SYSTEM_STRUCTURE_CHANGED",
    "SPACE_PROGRAM_CHANGED",
    "CALCULATION_APPROACH_CHANGED",
    "PARAMETER_SET_CHANGED",
    "EQUIPMENT_OR_MATERIAL_CHANGED",
    "QUANTITY_OR_CAPACITY_CHANGED",
    "DETAIL_LEVEL_INCREASED",
    "NO_HIGH_LEVEL_CHANGE",
    "UNRESOLVED_HIGH_LEVEL_CHANGE",
}
AI_DECISIONS = {
    "REAL_CHANGE", "DETAIL_ONLY", "NO_HIGH_LEVEL_CHANGE", "INSUFFICIENT_CONTEXT",
}

_NUMBER_RE = re.compile(r"(?<![a-zа-я0-9])\d+(?:[.,]\d+)*(?!\d)", re.I)
_IDENTIFIER_RE = re.compile(
    r"(?<![a-zа-я0-9])(?=[a-zа-я0-9./-]*\d)[a-zа-я0-9]+"
    r"(?:[./-][a-zа-я0-9]+)+(?![a-zа-я0-9])",
    re.I,
)
_UNSUPPORTED_AUDIT_RE = re.compile(
    r"ошибк[аи]\s+проект|нарушен|норматив|критич|ухудшен|улучшен|"
    r"требу(?:ет|ется)\s+исправ|рекоменд(?:уем|уется|аци)",
    re.I,
)
_SERVICE_RE = re.compile(
    r"адрес|дат[аы]|фамили|подпис|заказчик|\bсро\b|\bгип\b|номер\s+(?:листа|страниц)|"
    r"заголов|пунктуац|форматирован|оформлени|содержание\s+тома|структур\w*\s+(?:тома|комплекта)|"
    r"экспликац|легенд|маркировк|условн\w*\s+обозначени|выноск|"
    r"(?:изменен\w*\s+слово|в\s+ссылке\s+различ)|добавлен\w*\s+комплект\s+[a-zа-я0-9/_-]+",
    re.I,
)
_PARAPHRASE_RE = re.compile(
    r"перефразир|смысл\w*\s+не\s+измен|без\s+изменени[яй]\s+смысл|"
    r"формулировк\w*\s+(?:уточнен|изменен)|совпада|то\s+же\s+решени|"
    r"переставлен\w*\s+(?:слов|предложен)|исправлен\w*\s+(?:опечат|пунктуац)",
    re.I,
)
_DETAIL_RE = re.compile(
    r"детализир|подробн|уточнен\w*\s+описани|описан\w*\s+подроб|"
    r"перечислен\w*\s+(?:устройств|элемент|оборудован)|раскрыт\w*\s+состав",
    re.I,
)
_PARAMETER_RE = re.compile(
    r"площад|мощност|нагруз|размер|ширин|высот|длин|толщин|диаметр|сечени|"
    r"расход|давлени|температур|коэффициент|напряжени|ток\b",
    re.I,
)
_CALCULATION_RE = re.compile(
    r"формул(?:а|ы|е|ой|у)\b|метод\w*\s+расчет|принцип\w*\s+расчет|исходн\w*\s+(?:данн|предпосыл)|"
    r"исходн\w*(?:\s+\w+){0,2}\s+(?:данн|предпосыл)|"
    r"расчет\w*\s+по\s+(?:кратност|вредност)|коэффициент\w*\s+(?:для|в\s+формул)",
    re.I,
)
_OPERATION_RE = re.compile(
    r"режим\w*\s+работ|резервирован|очередност|логик\w*\s+(?:работ|включен|управлен)|"
    r"управлен|переключен|эксплуатац|принцип\w*\s+работ",
    re.I,
)
_STRUCTURE_RE = re.compile(
    r"состав\w*\s+систем|функциональн\w*\s+уз|подсистем|источник\w*\s+(?:питан|тепл)|"
    r"контур|зон[аы]\s+(?:систем|обслуживан)|схем\w*\s+подключен|"
    r"добавлен|удален|исключен|заменен",
    re.I,
)
_SPACE_RE = re.compile(
    r"назначени[ея]\s+помещен|состав\w*\s+помещен|количеств\w*(?:\s+\w+){0,2}\s+помещен|"
    r"помещени\w*\s+(?:добавлен|удален|исключен)|стало\s+\d+\s+помещен",
    re.I,
)
_PURPOSE_RE = re.compile(r"назначени[ея]|переименован\w*\s+помещен|стало\s+(?:техническ|складск|жил)", re.I)
_MATERIAL_RE = re.compile(
    r"материал|минеральн\w*\s+ват|утеплен|марка\s+(?:бетона|стали)|"
    r"тип\s+(?:оборудован|перегород)|оборудован|вентилятор|насос|кабел",
    re.I,
)
_ACTUAL_OBJECT_RE = re.compile(
    r"(?:добавлен|устроен|исключен|удален|перенесен|заменен)\w*\s+"
    r"(?:двер|лестниц|проем|окн|перегород|стен|помещени|оборудован|щит|вру|грщ|"
    r"кабел|трасс|вентилятор|насос)\w*",
    re.I,
)
_NEW_OBJECT_CLAIM_RE = re.compile(r"\b(?:нов\w*\s+объект|добавлен|создан|устроен)\b", re.I)
_DESIGNATION_ONLY_RE = re.compile(
    r"(?:обозначени|маркировк|написани|названи)\w*[^.\n]{0,50}(?:измен|замен|переимен)|"
    r"(?:измен|замен|переимен)\w*[^.\n]{0,50}(?:обозначени|маркировк|написани|названи)",
    re.I,
)
_ROOM_ID_RE = re.compile(r"(?<![a-zа-я0-9])\d+(?:[.]?[а-яa-z]+[.]?\d+)+(?![a-zа-я0-9])", re.I)


RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["groups"],
    "properties": {
        "groups": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["group_id", "decision", "type", "title", "reason", "evidence_ids"],
                "properties": {
                    "group_id": {"type": "string"},
                    "decision": {"type": "string", "enum": sorted(AI_DECISIONS)},
                    "type": {"type": "string", "enum": sorted(HIGH_LEVEL_TYPES)},
                    "title": {"type": "string"},
                    "reason": {"type": "string"},
                    "evidence_ids": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
}

SYSTEM_PROMPT = """Ты выполняешь Stage 5.3: синтез верхнеуровневых изменений П ↔ РД.

На входе уже есть детерминированные semantic groups. Для каждой группы верни ровно одно
решение и дословно скопируй group_id и все evidence_ids. Оцени не качество проекта, а
только подтверждённый текстом смысл:
- REAL_CHANGE — действительно изменилось проектное решение;
- DETAIL_ONLY — РД подробнее, но смена решения не доказана;
- NO_HIGH_LEVEL_CHANGE — перефразировка, оформление, перенос или иное изменение без
  смены проектного смысла;
- INSUFFICIENT_CONTEXT — вопрос может изменить итог, но evidence недостаточно.

Не считай ADDED/REMOVED автоматическим появлением/удалением объекта. Отличай одно новое
значение от изменения формулы или метода. Не склеивай разные смыслы и не придумывай
числа, обозначения, объекты или причинно-следственные связи. Числа в title разрешены
только если буквально есть в evidence; агрегированные counts добавляет backend. Для
DETAIL_ONLY не утверждай, что появился новый объект. Верни только JSON.
"""


class HighLevelValidationError(ValueError):
    """A Stage 5.3 claim is not fully supported by source evidence."""


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", text).strip()


def _evidence_text(evidence: dict[str, Any]) -> str:
    return "\n".join(str(evidence.get(key) or "") for key in (
        "summary", "before", "after", "reason", "stage5_title",
    ))


def _tokens(value: Any) -> set[str]:
    stop = {
        "и", "в", "на", "с", "по", "для", "из", "до", "от", "при", "что", "как",
        "the", "a", "of", "to", "is", "строка", "фрагмент", "справа", "слева",
    }
    return {
        token for token in re.findall(r"[a-zа-я0-9]+", _normalize(value))
        if len(token) > 1 and token not in stop
    }


def _side_counterpart(left: Any, right: Any) -> bool:
    """Conservative text presence proof used only to suppress a strong claim."""
    a, b = _normalize(left), _normalize(right)
    if not a or not b:
        return False
    if a == b:
        return True
    a_numbers, b_numbers = set(_NUMBER_RE.findall(a)), set(_NUMBER_RE.findall(b))
    if a_numbers != b_numbers:
        return False
    a_tokens, b_tokens = _tokens(a), _tokens(b)
    if not a_tokens or not b_tokens:
        return False
    overlap = len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens))
    return overlap >= 0.82


def _stage5_index(project_summary: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for sheet_group in project_summary.get("sheet_groups") or []:
        assignments: dict[str, dict[str, Any]] = {}
        for bucket, stage5_class in (
            ("project_changes", "PROJECT_CHANGE"),
            ("service_structure", "SERVICE_STRUCTURE"),
            ("review", "REVIEW"),
        ):
            for item in sheet_group.get(bucket) or []:
                for evidence_id in item.get("evidence_ids") or []:
                    assignments[str(evidence_id)] = {
                        "stage5_class": stage5_class,
                        "stage5_category": str(item.get("category") or "uncertain"),
                        "stage5_title": str(item.get("title") or ""),
                    }
        for evidence in sheet_group.get("atomic_evidence") or []:
            evidence_id = str(evidence.get("evidence_id") or "")
            if not evidence_id:
                continue
            assigned = assignments.get(evidence_id, {
                "stage5_class": "REVIEW", "stage5_category": "uncertain",
                "stage5_title": "Классификация требует проверки",
            })
            category = assigned["stage5_category"]
            hint_category = str(evidence.get("deterministic_category_hint") or "uncertain")
            if category == "uncertain" and hint_category != "uncertain":
                category = hint_category
            records.append({
                **evidence,
                **assigned,
                "stage5_category": category,
                "group_id": str(sheet_group.get("group_id") or ""),
                "left_pages": list(evidence.get("left_pages") or sheet_group.get("left_pages") or []),
                "right_pages": list(evidence.get("right_pages") or sheet_group.get("right_pages") or []),
                "left_labels": list(sheet_group.get("left_labels") or []),
                "right_labels": list(sheet_group.get("right_labels") or []),
                "pair_status": str((sheet_group.get("pair_precheck") or {}).get("status") or "PAIR_OK"),
                "aggregation_status": str(sheet_group.get("aggregation_status") or ""),
                "evidence_source": "TEXT",
            })
    by_id = {record["evidence_id"]: record for record in records}
    if len(by_id) != len(records):
        raise HighLevelValidationError("duplicate_source_evidence_id")
    return records


def _mark_cross_sheet_counterparts(records: list[dict[str, Any]]) -> None:
    before_records = [record for record in records if record.get("before")]
    after_records = [record for record in records if record.get("after")]
    for record in records:
        candidates: Iterable[dict[str, Any]] = ()
        value: Any = None
        if record.get("source_status") == "REMOVED" and record.get("before"):
            candidates, value = after_records, record.get("before")
        elif record.get("source_status") == "ADDED" and record.get("after"):
            candidates, value = before_records, record.get("after")
        matches = [
            other["evidence_id"] for other in candidates
            if other["evidence_id"] != record["evidence_id"]
            and other.get("group_id") != record.get("group_id")
            and _side_counterpart(value, other.get("after") if record.get("source_status") == "REMOVED" else other.get("before"))
        ]
        if matches:
            record["cross_sheet_counterpart_evidence_ids"] = sorted(set(matches))


def _candidate_type(record: dict[str, Any]) -> str:
    text = _normalize(_evidence_text(record))
    category = str(record.get("stage5_category") or "uncertain")
    if _CALCULATION_RE.search(text) or category == "calculation_method":
        return "CALCULATION_APPROACH_CHANGED"
    if _OPERATION_RE.search(text):
        return "SYSTEM_OPERATION_CHANGED"
    if category == "system_configuration":
        return "SYSTEM_STRUCTURE_CHANGED"
    if category == "floors":
        return "QUANTITY_OR_CAPACITY_CHANGED"
    if category == "room_composition" or _SPACE_RE.search(text):
        return "SPACE_PROGRAM_CHANGED"
    if category in {"materials", "equipment_parameters"} or _MATERIAL_RE.search(text):
        return "EQUIPMENT_OR_MATERIAL_CHANGED"
    if category in {"areas", "electrical_load", "dimensions"} or _PARAMETER_RE.search(text):
        return "PARAMETER_SET_CHANGED"
    if category == "consumer_composition":
        return "SYSTEM_STRUCTURE_CHANGED"
    if _ACTUAL_OBJECT_RE.search(text):
        return "SYSTEM_STRUCTURE_CHANGED"
    return "DESIGN_PRINCIPLE_CHANGED"


def _subject_key(record: dict[str, Any], change_type: str) -> str:
    text = _normalize(_evidence_text(record))
    if change_type == "PARAMETER_SET_CHANGED":
        if "площад" in text:
            return "areas"
        if re.search(r"нагруз|мощност|ток\b|напряжени", text):
            match = re.search(r"\b(?:вру|грщ|щр|що|щао)[-.\s]?[a-zа-я0-9]+", text, re.I)
            return "loads:" + (_normalize(match.group(0)) if match else "general")
        for value in ("толщин", "диаметр", "сечени", "ширин", "высот", "длин", "расход", "давлен"):
            if value in text:
                return value
        return "parameters"
    if change_type == "QUANTITY_OR_CAPACITY_CHANGED":
        return "floors" if "этаж" in text else "quantity_capacity"
    if change_type == "EQUIPMENT_OR_MATERIAL_CHANGED":
        for value in ("утеплен", "минеральн", "бетон", "сталь", "кабел", "насос", "вентилятор", "оборудован"):
            if value in text:
                return value
        return "equipment_material"
    if change_type == "SPACE_PROGRAM_CHANGED":
        return "room_purpose" if _PURPOSE_RE.search(text) else "room_composition"
    if change_type in {"SYSTEM_OPERATION_CHANGED", "SYSTEM_STRUCTURE_CHANGED"}:
        for name, pattern in (
            ("power", r"электроснаб|вру|грщ|питан|щит"),
            ("ventilation", r"вентиляц|дымоудален|воздухообмен"),
            ("water", r"водоснаб|канализац|насос"),
            ("fire", r"пожар|противодым"),
            ("stairs", r"лестниц"),
            ("openings", r"двер|проем|окн"),
        ):
            if re.search(pattern, text, re.I):
                return name
        return "system"
    return "calculation" if change_type == "CALCULATION_APPROACH_CHANGED" else "principle"


def _is_service(record: dict[str, Any]) -> bool:
    hint = str(record.get("deterministic_class_hint") or "")
    return (
        record.get("stage5_class") == "SERVICE_STRUCTURE"
        or (hint == "SERVICE_STRUCTURE" and record.get("stage5_class") != "PROJECT_CHANGE")
    )


def _is_paraphrase_or_same(record: dict[str, Any]) -> bool:
    text = _evidence_text(record)
    if _PARAPHRASE_RE.search(text):
        return True
    before, after = record.get("before"), record.get("after")
    before_tokens, after_tokens = _tokens(before), _tokens(after)
    numbers_equal = set(_NUMBER_RE.findall(str(before or ""))) == set(
        _NUMBER_RE.findall(str(after or ""))
    )
    overlap = len(before_tokens & after_tokens) / max(1, max(len(before_tokens), len(after_tokens)))
    normalized_text = _normalize(text)
    explicit_semantic_change = bool(
        _CALCULATION_RE.search(normalized_text)
        or _OPERATION_RE.search(normalized_text)
        or _SPACE_RE.search(normalized_text)
        or _ACTUAL_OBJECT_RE.search(normalized_text)
        or re.search(
            r"(?:тип\w*\s+(?:оборудован|насос|вентилятор|кабел|материал)|материал\w*|марка\w*\s+(?:бетона|стали))"
            r"[^.\n]{0,80}(?:замен|измен)|(?:замен|измен)[^.\n]{0,80}"
            r"(?:тип\w*\s+(?:оборудован|насос|вентилятор|кабел|материал)|материал\w*|марка\w*\s+(?:бетона|стали))",
            normalized_text,
        )
    )
    if before_tokens and after_tokens and numbers_equal and overlap >= 0.78 and not explicit_semantic_change:
        return True
    return False


def _is_detail_only(record: dict[str, Any]) -> bool:
    text = _evidence_text(record)
    return bool(_DETAIL_RE.search(text) and not _ACTUAL_OBJECT_RE.search(text))


def _has_two_sided_change(record: dict[str, Any]) -> bool:
    before, after = record.get("before"), record.get("after")
    return bool(before and after and _normalize(before) != _normalize(after))


def _numbers_changed(record: dict[str, Any]) -> bool:
    if not _has_two_sided_change(record):
        return False
    return set(_NUMBER_RE.findall(str(record.get("before") or ""))) != set(
        _NUMBER_RE.findall(str(record.get("after") or ""))
    )


def _strong_two_sided(record: dict[str, Any], change_type: str) -> bool:
    text = _normalize(_evidence_text(record))
    if not _has_two_sided_change(record):
        return False
    if change_type == "PARAMETER_SET_CHANGED":
        return _numbers_changed(record) and bool(_PARAMETER_RE.search(text))
    if change_type == "QUANTITY_OR_CAPACITY_CHANGED":
        return _numbers_changed(record) and bool(re.search(r"количеств|этаж|мощност|производительност", text, re.I))
    if change_type == "CALCULATION_APPROACH_CHANGED":
        return bool(_CALCULATION_RE.search(text))
    if change_type == "SYSTEM_OPERATION_CHANGED":
        return bool(_OPERATION_RE.search(text))
    if change_type == "SPACE_PROGRAM_CHANGED":
        return bool(_SPACE_RE.search(text) or _PURPOSE_RE.search(text))
    if change_type == "EQUIPMENT_OR_MATERIAL_CHANGED":
        return bool(_MATERIAL_RE.search(text) and re.search(r"замен|измен|вместо", text, re.I))
    if change_type == "SYSTEM_STRUCTURE_CHANGED":
        return bool(_STRUCTURE_RE.search(text))
    return bool(re.search(r"принцип|подход|способ\w*\s+организац", text, re.I))


def _route(record: dict[str, Any], change_type: str) -> tuple[str, str]:
    if _is_service(record):
        return "SERVICE", "SERVICE_STRUCTURE"
    if _SERVICE_RE.search(_evidence_text(record)) and record.get("stage5_class") != "PROJECT_CHANGE":
        return "NON_MATERIAL", "SERVICE_ONLY_REVIEW"
    if record.get("cross_sheet_counterpart_evidence_ids"):
        return "NON_MATERIAL", "CROSS_SHEET_COUNTERPART"
    if _DESIGNATION_ONLY_RE.search(_evidence_text(record)) and not _numbers_changed(record):
        return "NON_MATERIAL", "DESIGNATION_ONLY"
    if _is_paraphrase_or_same(record):
        return "NON_MATERIAL", "NO_SEMANTIC_CHANGE"
    if record.get("pair_status") == project_change_summary.PAIR_REVIEW_REQUIRED:
        return "MATERIAL_REVIEW", SOURCE_LINK_UNCERTAIN
    if _is_detail_only(record):
        return "DETAIL", "DETAIL_ONLY"
    hint = str(record.get("deterministic_class_hint") or "")
    if _strong_two_sided(record, change_type):
        return "CONFIRMED", "TWO_SIDED_TEXT_PROOF"
    if record.get("source_status") == "UNCERTAIN":
        if hint == "PROJECT_CHANGE" or any(pattern.search(_evidence_text(record)) for pattern in (
            _PARAMETER_RE, _CALCULATION_RE, _OPERATION_RE, _SPACE_RE, _MATERIAL_RE,
        )):
            return "MATERIAL_REVIEW", "INSUFFICIENT_CONTEXT"
        return "NON_MATERIAL", "LOW_VALUE_UNCERTAIN"
    if record.get("stage5_class") == "PROJECT_CHANGE" or hint == "PROJECT_CHANGE":
        return "AI_REVIEW", "DETAIL_VS_REAL_CHANGE"
    if any(pattern.search(_evidence_text(record)) for pattern in (
        _CALCULATION_RE, _OPERATION_RE, _SPACE_RE, _PARAMETER_RE, _MATERIAL_RE,
    )):
        return "MATERIAL_REVIEW", "INSUFFICIENT_CONTEXT"
    return "NON_MATERIAL", "NO_PROJECT_DECISION_SIGNAL"


def _semantic_group_id(route: str, change_type: str, subject: str, records: list[dict[str, Any]]) -> str:
    source = ":".join((route, change_type, subject, *(sorted(record["evidence_id"] for record in records))))
    return "hlg_" + hashlib.sha256(source.encode()).hexdigest()[:16]


def build_semantic_groups(project_summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Deterministically pre-group evidence before any model call."""
    records = _stage5_index(project_summary)
    _mark_cross_sheet_counterparts(records)
    buckets: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        change_type = _candidate_type(record)
        route, reason = _route(record, change_type)
        subject = _subject_key(record, change_type)
        # Uncertain links remain isolated per sheet pair: cross-sheet merging
        # must never hide the source-link guard.
        link_key = record["group_id"] if route == "MATERIAL_REVIEW" and reason == SOURCE_LINK_UNCERTAIN else "cross_sheet"
        buckets[(route, reason, change_type, f"{subject}:{link_key}")].append(record)
    output = []
    for (route, reason, change_type, key), evidence in sorted(buckets.items()):
        evidence.sort(key=lambda item: item["evidence_id"])
        subject = key.rsplit(":", 1)[0]
        output.append({
            "group_id": _semantic_group_id(route, change_type, key, evidence),
            "route": route,
            "route_reason": reason,
            "candidate_type": change_type,
            "subject": subject,
            "source_link_uncertain": reason == SOURCE_LINK_UNCERTAIN,
            "evidence_ids": [item["evidence_id"] for item in evidence],
            "sheet_groups": sorted({item["group_id"] for item in evidence}),
            "atomic_evidence": evidence,
        })
    return output


def _source_text(evidence: Iterable[dict[str, Any]]) -> str:
    return "\n".join(_evidence_text(item) for item in evidence)


def _supported_text(value: str, evidence: list[dict[str, Any]], *, allow_backend_count: bool = False) -> bool:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 320:
        return False
    if _UNSUPPORTED_AUDIT_RE.search(value):
        return False
    source = _source_text(evidence)
    source_numbers = set(_NUMBER_RE.findall(source))
    value_numbers = set(_NUMBER_RE.findall(value))
    if allow_backend_count:
        value_numbers -= {str(len(evidence))}
    if not value_numbers <= source_numbers:
        return False
    source_ids = {item.lower() for item in _IDENTIFIER_RE.findall(source)}
    return {item.lower() for item in _IDENTIFIER_RE.findall(value)} <= source_ids


def _allowed_ai_types(group: dict[str, Any]) -> set[str]:
    candidate = group["candidate_type"]
    adjacent = {
        "PARAMETER_SET_CHANGED": {"QUANTITY_OR_CAPACITY_CHANGED", "SPACE_PROGRAM_CHANGED"},
        "SPACE_PROGRAM_CHANGED": {"PARAMETER_SET_CHANGED", "QUANTITY_OR_CAPACITY_CHANGED"},
        "SYSTEM_OPERATION_CHANGED": {"DESIGN_PRINCIPLE_CHANGED", "SYSTEM_STRUCTURE_CHANGED"},
        "SYSTEM_STRUCTURE_CHANGED": {"SYSTEM_OPERATION_CHANGED", "DESIGN_PRINCIPLE_CHANGED"},
        "EQUIPMENT_OR_MATERIAL_CHANGED": {"SYSTEM_STRUCTURE_CHANGED"},
        "CALCULATION_APPROACH_CHANGED": {"DESIGN_PRINCIPLE_CHANGED"},
        "DESIGN_PRINCIPLE_CHANGED": {"SYSTEM_OPERATION_CHANGED", "SYSTEM_STRUCTURE_CHANGED"},
    }
    return {candidate, *(adjacent.get(candidate) or set())}


def validate_ai_response(payload: Any, semantic_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or set(payload) != {"groups"} or not isinstance(payload.get("groups"), list):
        raise HighLevelValidationError("invalid_response_schema")
    expected = {group["group_id"]: group for group in semantic_groups}
    received: dict[str, dict[str, Any]] = {}
    required_keys = {"group_id", "decision", "type", "title", "reason", "evidence_ids"}
    normalized = []
    for raw in payload["groups"]:
        if not isinstance(raw, dict) or set(raw) != required_keys:
            raise HighLevelValidationError("invalid_group_schema")
        group_id = str(raw.get("group_id") or "")
        if group_id not in expected or group_id in received:
            raise HighLevelValidationError("unexpected_or_duplicate_group")
        group = expected[group_id]
        ids = raw.get("evidence_ids")
        if not isinstance(ids, list) or any(not isinstance(value, str) for value in ids):
            raise HighLevelValidationError("invalid_evidence_ids")
        if len(ids) != len(set(ids)) or set(ids) != set(group["evidence_ids"]):
            raise HighLevelValidationError("incomplete_or_hallucinated_evidence")
        decision, change_type = raw.get("decision"), raw.get("type")
        if decision not in AI_DECISIONS or change_type not in HIGH_LEVEL_TYPES:
            raise HighLevelValidationError("invalid_decision_enum")
        expected_type = {
            "DETAIL_ONLY": "DETAIL_LEVEL_INCREASED",
            "NO_HIGH_LEVEL_CHANGE": "NO_HIGH_LEVEL_CHANGE",
            "INSUFFICIENT_CONTEXT": "UNRESOLVED_HIGH_LEVEL_CHANGE",
        }.get(decision)
        if expected_type and change_type != expected_type:
            raise HighLevelValidationError("decision_type_mismatch")
        if decision == "REAL_CHANGE" and change_type not in _allowed_ai_types(group):
            raise HighLevelValidationError("incompatible_high_level_type")
        if group.get("source_link_uncertain") and decision == "REAL_CHANGE":
            raise HighLevelValidationError("source_link_uncertain_cannot_publish")
        evidence = group["atomic_evidence"]
        if not _supported_text(raw["title"], evidence) or not _supported_text(raw["reason"], evidence):
            raise HighLevelValidationError("unsupported_claim")
        one_sided_statuses = {str(item.get("source_status") or "") for item in evidence}
        if decision == "REAL_CHANGE" and one_sided_statuses in ({"ADDED"}, {"REMOVED"}):
            raise HighLevelValidationError("one_sided_presence_cannot_publish")
        if decision == "DETAIL_ONLY" and _NEW_OBJECT_CLAIM_RE.search(raw["title"]):
            raise HighLevelValidationError("detail_claims_new_object")
        item = {
            "group_id": group_id,
            "decision": decision,
            "type": change_type,
            "title": raw["title"].strip(),
            "reason": raw["reason"].strip(),
            "evidence_ids": list(ids),
            "decision_source": "AI",
        }
        received[group_id] = item
        normalized.append(item)
    if set(received) != set(expected):
        raise HighLevelValidationError("incomplete_group_coverage")
    return normalized


def prompt_for_groups(groups: list[dict[str, Any]]) -> str:
    compact = []
    for group in groups:
        compact.append({
            "group_id": group["group_id"],
            "candidate_type": group["candidate_type"],
            "subject": group["subject"],
            "source_link_uncertain": group["source_link_uncertain"],
            "atomic_evidence": [{
                "evidence_id": item["evidence_id"],
                "source_status": item.get("source_status"),
                "stage5_class": item.get("stage5_class"),
                "stage5_category": item.get("stage5_category"),
                "summary": item.get("summary"),
                "before": item.get("before"),
                "after": item.get("after"),
                "reason": item.get("reason"),
                "sheet_group": item.get("group_id"),
                "left_pages": item.get("left_pages"),
                "right_pages": item.get("right_pages"),
            } for item in group["atomic_evidence"]],
        })
    return (
        SYSTEM_PROMPT + "\nJSON Schema:\n"
        + json.dumps(RESPONSE_SCHEMA, ensure_ascii=False, separators=(",", ":"))
        + "\nINPUT:\n" + json.dumps({"groups": compact}, ensure_ascii=False, separators=(",", ":"))
    )


def _backend_title(change_type: str, evidence: list[dict[str, Any]], proposed: str = "") -> str:
    if change_type == "PARAMETER_SET_CHANGED":
        text = _source_text(evidence)
        room_like = all(
            "площад" in _normalize(item.get("summary"))
            and (_ROOM_ID_RE.search(_evidence_text(item)) or "помещ" in _normalize(_evidence_text(item)))
            and "итог" not in _normalize(item.get("summary"))
            for item in evidence
        )
        if room_like and len(evidence) > 1:
            return f"Скорректированы площади {len(evidence)} помещений."
        if re.search(r"нагруз|мощност", text, re.I):
            return "Скорректированы расчётные нагрузки группы потребителей."
        return "Скорректирован набор проектных параметров."
    defaults = {
        "CALCULATION_APPROACH_CHANGED": "Изменён расчётный подход.",
        "SYSTEM_OPERATION_CHANGED": "Изменён принцип работы системы.",
        "SYSTEM_STRUCTURE_CHANGED": "Изменена структура проектной системы или элемента.",
        "SPACE_PROGRAM_CHANGED": "Изменена программа и состав помещений.",
        "EQUIPMENT_OR_MATERIAL_CHANGED": "Изменён тип оборудования или материала.",
        "QUANTITY_OR_CAPACITY_CHANGED": "Изменено количество или проектная мощность.",
        "DESIGN_PRINCIPLE_CHANGED": "Изменён принцип проектного решения.",
        "DETAIL_LEVEL_INCREASED": "Увеличена детализация РД без подтверждённой смены решения.",
        "NO_HIGH_LEVEL_CHANGE": "Изменение не влияет на проектное решение.",
        "UNRESOLVED_HIGH_LEVEL_CHANGE": "Возможное изменение проектного решения требует проверки.",
    }
    if proposed and len(evidence) == 1 and _supported_text(proposed, evidence):
        return proposed.strip().rstrip(".") + "."
    return defaults[change_type]


def _decision_for_group(group: dict[str, Any]) -> dict[str, Any] | None:
    route = group["route"]
    if route == "AI_REVIEW":
        return None
    decision, change_type = {
        "CONFIRMED": ("REAL_CHANGE", group["candidate_type"]),
        "DETAIL": ("DETAIL_ONLY", "DETAIL_LEVEL_INCREASED"),
        "NON_MATERIAL": ("NO_HIGH_LEVEL_CHANGE", "NO_HIGH_LEVEL_CHANGE"),
        "SERVICE": ("NO_HIGH_LEVEL_CHANGE", "NO_HIGH_LEVEL_CHANGE"),
        "MATERIAL_REVIEW": ("INSUFFICIENT_CONTEXT", "UNRESOLVED_HIGH_LEVEL_CHANGE"),
    }[route]
    return {
        "group_id": group["group_id"], "decision": decision, "type": change_type,
        "title": "", "reason": group["route_reason"],
        "evidence_ids": list(group["evidence_ids"]), "decision_source": "DETERMINISTIC",
    }


def fallback_decision(group: dict[str, Any], error: str) -> dict[str, Any]:
    return {
        "group_id": group["group_id"], "decision": "INSUFFICIENT_CONTEXT",
        "type": "UNRESOLVED_HIGH_LEVEL_CHANGE", "title": "",
        "reason": f"AI_UNAVAILABLE:{error}", "evidence_ids": list(group["evidence_ids"]),
        "decision_source": "FAIL_CLOSED",
    }


def deterministic_decisions(groups: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    resolved, ai_required = [], []
    for group in groups:
        decision = _decision_for_group(group)
        (resolved if decision else ai_required).append(decision or group)
    return resolved, ai_required


def _record_details(group: dict[str, Any]) -> list[dict[str, Any]]:
    return [{key: item.get(key) for key in (
        "evidence_id", "evidence_source", "source_status", "summary", "before", "after", "reason",
        "group_id", "left_pages", "right_pages", "left_labels", "right_labels",
        "left_fragment_ids", "right_fragment_ids", "left_anchors", "right_anchors",
        "stage5_class", "stage5_category", "stage5_title", "pair_status",
        "cross_sheet_counterpart_evidence_ids",
    )} for item in group["atomic_evidence"]]


def _result_id(prefix: str, decision: dict[str, Any]) -> str:
    source = f"{decision['group_id']}:{decision['type']}:{','.join(sorted(decision['evidence_ids']))}"
    return prefix + hashlib.sha256(source.encode()).hexdigest()[:16]


def _result_item(group: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    evidence = group["atomic_evidence"]
    return {
        "change_id": _result_id("hlc_", decision),
        "type": decision["type"],
        "title": _backend_title(decision["type"], evidence, decision.get("title") or ""),
        "status": CONFIRMED if decision["decision"] in {"REAL_CHANGE", "DETAIL_ONLY"} else REVIEW_REQUIRED,
        "confidence": "high" if decision["decision_source"] == "DETERMINISTIC" and decision["decision"] == "REAL_CHANGE" else "medium",
        "reason": decision["reason"],
        "decision_source": decision["decision_source"],
        "evidence_sources": ["TEXT"],
        "evidence_ids": list(decision["evidence_ids"]),
        "sheet_groups": list(group["sheet_groups"]),
        "semantic_subject": group["subject"],
        "count": len(decision["evidence_ids"]),
        "details": _record_details(group),
    }


def _merge_confirmed_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge the same project meaning after AI and deterministic routes converge."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[(item["type"], item.get("semantic_subject") or "")].append(item)
    output = []
    for (_change_type, _subject), related in grouped.items():
        if len(related) == 1:
            output.append(related[0])
            continue
        evidence_by_id = {
            detail["evidence_id"]: detail
            for item in related for detail in item["details"]
        }
        evidence_ids = sorted(evidence_by_id)
        template = related[0]
        merged = {
            **template,
            "change_id": "hlc_" + hashlib.sha256(
                f"{template['type']}:{_subject}:{','.join(evidence_ids)}".encode()
            ).hexdigest()[:16],
            "title": _backend_title(template["type"], list(evidence_by_id.values())),
            "confidence": "medium" if any(item["confidence"] != "high" for item in related) else "high",
            "reason": " | ".join(dict.fromkeys(str(item["reason"]) for item in related)),
            "decision_source": "MIXED" if len({item["decision_source"] for item in related}) > 1 else template["decision_source"],
            "evidence_ids": evidence_ids,
            "sheet_groups": sorted({value for item in related for value in item["sheet_groups"]}),
            "count": len(evidence_ids),
            "details": [evidence_by_id[value] for value in evidence_ids],
        }
        output.append(merged)
    return output


def validate_final_artifact(artifact: dict[str, Any], project_summary: dict[str, Any]) -> None:
    """Strict post-build validation independent of the provider response."""
    evidence = {item["evidence_id"]: item for item in _stage5_index(project_summary)}
    seen_change_evidence: set[str] = set()
    for bucket in ("high_level_changes", "detail_level_increased", "material_review", "non_material_review", "unresolved"):
        for item in artifact.get(bucket) or []:
            ids = item.get("evidence_ids") or []
            if not ids or any(value not in evidence for value in ids):
                raise HighLevelValidationError("artifact_unknown_evidence")
            selected = [evidence[value] for value in ids]
            if not _supported_text(str(item.get("title") or ""), selected, allow_backend_count=True):
                # Backend generic titles intentionally contain no source entity;
                # only their aggregate count needs special allowance.
                generic = _backend_title(str(item.get("type") or ""), selected)
                if item.get("title") != generic:
                    raise HighLevelValidationError("artifact_unsupported_title")
            if bucket == "high_level_changes":
                if seen_change_evidence & set(ids):
                    raise HighLevelValidationError("evidence_published_twice")
                seen_change_evidence.update(ids)
                if any(_is_service(value) for value in selected):
                    raise HighLevelValidationError("service_promoted_to_project_change")
                if any(value.get("pair_status") == project_change_summary.PAIR_REVIEW_REQUIRED for value in selected):
                    raise HighLevelValidationError("uncertain_link_promoted_to_project_change")
            if item.get("type") == "DETAIL_LEVEL_INCREASED" and _NEW_OBJECT_CLAIM_RE.search(str(item.get("title") or "")):
                raise HighLevelValidationError("detail_claims_new_object")
    service_ids = set((artifact.get("service_structure_summary") or {}).get("evidence_ids") or [])
    if any(value not in evidence for value in service_ids):
        raise HighLevelValidationError("service_summary_unknown_evidence")


def build_artifact(
    *, pair_id: str, generated_at: str, source_signature_value: str,
    project_summary: dict[str, Any], semantic_groups: list[dict[str, Any]],
    decisions: list[dict[str, Any]], usage: dict[str, int] | None = None,
    model_calls: int = 0, fresh_model_calls: int = 0,
) -> dict[str, Any]:
    groups = {group["group_id"]: group for group in semantic_groups}
    decision_map = {item["group_id"]: item for item in decisions}
    if set(groups) != set(decision_map):
        raise HighLevelValidationError("decision_coverage_mismatch")
    high_level_changes: list[dict[str, Any]] = []
    detail_level: list[dict[str, Any]] = []
    material: list[dict[str, Any]] = []
    non_material: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    service_groups: list[dict[str, Any]] = []
    debug_groups = []
    for group in semantic_groups:
        decision = decision_map[group["group_id"]]
        item = _result_item(group, decision)
        if group["route"] == "SERVICE":
            service_groups.append(item)
        elif decision["decision"] == "REAL_CHANGE":
            high_level_changes.append(item)
        elif decision["decision"] == "DETAIL_ONLY":
            detail_level.append(item)
        elif decision["decision"] == "NO_HIGH_LEVEL_CHANGE":
            non_material.append(item)
        else:
            material.append(item)
            unresolved.append(item)
        debug_groups.append({
            "group_id": group["group_id"], "route": group["route"],
            "route_reason": group["route_reason"], "candidate_type": group["candidate_type"],
            "subject": group["subject"], "decision": decision["decision"],
            "decision_source": decision["decision_source"],
            "evidence_ids": list(group["evidence_ids"]),
        })
    high_level_changes = _merge_confirmed_items(high_level_changes)
    detail_level = _merge_confirmed_items(detail_level)
    service_ids = sorted({value for item in service_groups for value in item["evidence_ids"]})
    all_source_ids = {item["evidence_id"] for item in _stage5_index(project_summary)}
    represented_ids = {
        value for bucket in (high_level_changes, detail_level, material, non_material, service_groups)
        for item in bucket for value in item["evidence_ids"]
    }
    if represented_ids != all_source_ids:
        raise HighLevelValidationError("source_evidence_not_fully_represented")
    usage = usage or {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "duration_ms": 0}
    fallback_groups = sum(item.get("decision_source") == "FAIL_CLOSED" for item in decisions)
    artifact = {
        "version": VERSION, "schema_version": SCHEMA_VERSION, "kind": KIND,
        "pair_id": pair_id, "generated_at": generated_at,
        "source_signature": source_signature_value,
        "source_artifact": project_change_summary.KIND,
        "prompt_version": PROMPT_VERSION, "validator_version": VALIDATOR_VERSION,
        "model": PRODUCTION_MODEL, "reasoning_effort": PRODUCTION_REASONING_EFFORT,
        "status": "partial" if fallback_groups else "completed", "evidence_sources": ["TEXT"],
        "high_level_changes": high_level_changes,
        "detail_level_increased": detail_level,
        "material_review": material,
        "non_material_review": non_material,
        "unresolved": unresolved,
        "service_structure_summary": {
            "collapsed": True, "groups": len(service_groups),
            "evidence_count": len(service_ids), "evidence_ids": service_ids,
            "items": service_groups,
        },
        "semantic_groups": debug_groups,
        "summary": {
            "atomic_evidence": len(all_source_ids),
            "semantic_groups": len(semantic_groups),
            "high_level_changes": len(high_level_changes),
            "high_level_evidence": sum(item["count"] for item in high_level_changes),
            "detail_level_increased": len(detail_level),
            "material_review": len(material),
            "material_review_evidence": len({value for item in material for value in item["evidence_ids"]}),
            "non_material_review": len(non_material),
            "non_material_review_evidence": len({value for item in non_material for value in item["evidence_ids"]}),
            "service_structure_evidence": len(service_ids),
            "unresolved": len(unresolved),
            "model_calls": model_calls, "fresh_model_calls": fresh_model_calls,
            "fallback_groups": fallback_groups,
            **usage,
        },
        "constraints": {
            "stage4_and_stage5_immutable": True,
            "additive_artifact": True,
            "text_only": True,
            "graphic_evidence_supported_by_contract": True,
            "counts_computed_by_backend": True,
            "source_link_fail_closed": True,
            "service_never_promoted": True,
            "review_triaged_by_materiality": True,
            "ai_only_for_material_semantic_groups": True,
        },
    }
    validate_final_artifact(artifact, project_summary)
    return artifact


def source_signature(project_summary: dict[str, Any], semantic_groups: list[dict[str, Any]]) -> str:
    source = {
        "version": VERSION, "prompt_version": PROMPT_VERSION,
        "validator_version": VALIDATOR_VERSION,
        "model": PRODUCTION_MODEL, "reasoning_effort": PRODUCTION_REASONING_EFFORT,
        "stage5_source_signature": project_summary.get("source_signature"),
        "stage5_version": project_summary.get("version"),
        "groups": [{
            "group_id": group["group_id"], "route": group["route"],
            "candidate_type": group["candidate_type"], "evidence_ids": group["evidence_ids"],
        } for group in semantic_groups],
    }
    return hashlib.sha256(json.dumps(source, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def public_view(payload: dict[str, Any] | None, *, stale: bool = False) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("version") != VERSION or payload.get("kind") != KIND:
        return None
    return {**payload, "stale": bool(stale)}


__all__ = [
    "AI_DECISIONS", "CONFIRMED", "HIGH_LEVEL_TYPES", "HighLevelValidationError",
    "KIND", "MATERIAL_REVIEW", "NON_MATERIAL_REVIEW", "PRODUCTION_MODEL",
    "PRODUCTION_REASONING_EFFORT", "PROMPT_VERSION", "RESPONSE_SCHEMA",
    "REVIEW_REQUIRED", "SCHEMA_VERSION", "SOURCE_LINK_UNCERTAIN", "SYSTEM_PROMPT",
    "VALIDATOR_VERSION", "VERSION", "build_artifact", "build_semantic_groups",
    "deterministic_decisions", "fallback_decision", "prompt_for_groups", "public_view",
    "source_signature", "validate_ai_response", "validate_final_artifact",
]
