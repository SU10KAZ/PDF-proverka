"""Stage 5 engineering aggregation over immutable Stage 4 evidence.

The model may only classify and group existing atomic evidence ids.  Sheet
purpose, counts, source text, pages and anchors are owned by the backend.  A
failed or invalid model response falls back to conservative deterministic
classification; it never changes Stage 2/3/4 artifacts or ``sheet_links``.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Iterable


VERSION = 1
KIND = "stage_comparison_project_change_summary"
PROMPT_VERSION = "stage5_project_change_aggregator_v2"
VALIDATOR_VERSION = "stage5_project_change_validator_v3"
PRODUCTION_MODEL = "gpt-5.6-luna"
PRODUCTION_REASONING_EFFORT = "medium"

PAIR_OK = "PAIR_OK"
PAIR_REVIEW_REQUIRED = "PAIR_REVIEW_REQUIRED"
CLASSES = {"PROJECT_CHANGE", "SERVICE_STRUCTURE", "REVIEW"}
CATEGORIES = {
    "areas", "floors", "electrical_load", "calculation_method",
    "consumer_composition", "room_composition", "dimensions", "materials",
    "equipment_parameters", "system_configuration", "fire_safety",
    "designation_rename", "administrative", "documentation_structure",
    "formatting", "other_project", "other_service", "uncertain",
}

_NUMBER_RE = re.compile(r"(?<![a-zа-я0-9])\d+(?:[.,]\d+)*(?!\d)", re.I)
_IDENTIFIER_RE = re.compile(
    r"(?<![a-zа-я0-9])(?=[a-zа-я0-9./-]*\d)[a-zа-я0-9]+"
    r"(?:[./-][a-zа-я0-9]+)+(?![a-zа-я0-9])",
    re.I,
)
_MODEL_COUNT_RE = re.compile(
    r"\b\d+\s+(?:помещен|нагруз|изменен|изменени|факт|строк|потребител)", re.I
)
_AUDIT_LANGUAGE_RE = re.compile(
    r"ошибк[аи]\s+проект|нарушен|норматив|критич|некритич|ухудшен|улучшен|"
    r"требу(?:ет|ется)\s+исправ|рекоменд(?:уем|уется|аци)|"
    r"влия(?:ет|ние)\s+на\s+(?:стоимост|срок)",
    re.I,
)

_SERVICE_RE = re.compile(
    r"проектн(?:ая|ой)\s+организац|заказчик|генеральн(?:ый|ого)\s+проектиров|"
    r"\bсро\b|саморегулируем|свидетельств|\bгип\b|\bгап\b|главн(?:ый|ого)\s+инженер|"
    r"ответственн(?:ое|ый)\s+лиц|генеральн(?:ый|ого)\s+директор|директор|"
    r"подпис|фамили|инициал|контактн|телефон|e-?mail|www\.|адрес\s+организац|"
    r"логотип|слоган|год\s+(?:выпуска|изменен|добавлен)|дата\s+(?:выпуска|подписи)|"
    r"корректировк|номер\s+(?:изменения|договора|тома|документа)|"
    r"положительн(?:ое|ые)\s+заключен|экспертиз|регистрационн|"
    r"написани[ея]\s+(?:улицы|адреса)|пунктуац|форматирован|регистр\b|"
    r"таблиц[аы]\s+(?:изменений|подписей)|изм\.\s*№\s*док",
    re.I,
)
_STRUCTURE_RE = re.compile(
    r"содержани[ея]\s+тома|номер\s+(?:листа|страницы)|нумерац|"
    r"лист\s+(?:добавлен|исключен|переименован)|добавлен\w*\s+(?:отдельн\w*\s+)?лист|"
    r"схем\w*\s+(?:разделен|разбит|объединен)|разделен\w*\s+на\s+(?:два|2)\s+лист|"
    r"графическ(?:ая|ой)\s+част|состав\s+тома|структур\w*\s+(?:листа|комплекта|документац)|"
    r"перенесен\w*\s+на\s+(?:другой|иной)\s+лист|обозначени[ея]\s+(?:листа|части)|"
    r"справк\w*\s+о\s+(?:внесенных\s+)?изменени",
    re.I,
)
_RENAME_RE = re.compile(r"переимен|изменен\w*\s+(?:названи|обозначени)|сохранен\w*\s+с\s+ин(?:ой|ым)\s+", re.I)
_PROJECT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("areas", re.compile(r"\bплощад[ьи]|\bм[²2]\b", re.I)),
    ("floors", re.compile(r"этажност|количеств\w*\s+этаж|до\s+\d+\s+этаж", re.I)),
    ("electrical_load", re.compile(r"электрическ\w*\s+нагруз|суммарн\w*\s+нагруз|мощност|расчетн\w*\s+ток|напряжени", re.I)),
    ("calculation_method", re.compile(r"метод\w*\s+расчет|принцип\w*\s+расчет|расчет\w*\s+по\s+(?:кратност|вредност)|формул", re.I)),
    ("consumer_composition", re.compile(r"состав\w*\s+потребител|наименовани[ея]\s+потребител|добавлен\w*\s+(?:строк|нагруз|потребител)", re.I)),
    ("room_composition", re.compile(r"состав\w*\s+помещен|назначени[ея]\s+помещен|техническ\w*\s+помещен", re.I)),
    ("dimensions", re.compile(r"толщин|диаметр|сечени|размер|длин|высот|отметк", re.I)),
    ("materials", re.compile(r"материал|марка\s+(?:бетона|стали)|тип\s+перегород", re.I)),
    ("equipment_parameters", re.compile(r"тип\s+оборудован|марка\s+оборудован|количеств\w*\s+оборудован|производительност|расход|давлени|температур", re.I)),
    ("system_configuration", re.compile(r"схем\w*\s+подключен|источник\w*\s+питан|резервирован|секционирован|трасс|принцип\w*\s+работ", re.I)),
    ("fire_safety", re.compile(r"противопожар|эвакуац|противодымн", re.I)),
)

_PURPOSE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("contents", re.compile(r"содержание\s+тома", re.I)),
    ("masonry_plan", re.compile(r"кладочн\w*\s+план", re.I)),
    ("vru_single_line", re.compile(r"однолинейн\w*.*\bвру", re.I)),
    ("panel_single_line", re.compile(r"однолинейн\w*.*(?:щит|щр|що|щао|грщ)", re.I)),
    ("lightning", re.compile(r"молниезащ", re.I)),
    ("grounding", re.compile(r"заземлен", re.I)),
    ("internal_power", re.compile(r"внутренн\w*\s+электроснаб|освещени", re.I)),
    ("ventilation", re.compile(r"вентиляц|воздухообмен", re.I)),
    ("floor_plan", re.compile(r"\bплан\w*\s+(?:-|\d|этажа|подзем)", re.I)),
    ("construction_detail", re.compile(r"\bузел\b|деталь", re.I)),
    ("text_part", re.compile(r"текстов\w*\s+част", re.I)),
)

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
                "required": ["group_id", "items"],
                "properties": {
                    "group_id": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["class", "category", "title", "evidence_ids"],
                            "properties": {
                                "class": {"type": "string", "enum": sorted(CLASSES)},
                                "category": {"type": "string", "enum": sorted(CATEGORIES)},
                                "title": {"type": "string"},
                                "evidence_ids": {
                                    "type": "array", "items": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            },
        },
    },
}

SYSTEM_PROMPT = """Ты — Stage 5 classifier + aggregator текстовых изменений проектной документации.

Работай ТОЛЬКО с переданными atomic evidence. Не проверяй нормы, качество, стоимость,
критичность и не давай рекомендаций. Опиши только то, что фактически изменилось.

Внешний массив groups повторяет INPUT sheet groups один к одному. Для каждого входного
sheet group верни РОВНО один объект, дословно скопируй его group_id. Не создавай свои
group_id вроде g_areas или g_uncertain. Смысловые агрегаты размещай только в items.

Для каждого evidence_id выбери ровно один класс:
- SERVICE_STRUCTURE: оформление, организация/заказчик/СРО/ГИП/подписи/даты/шифры,
  нумерация и структура комплекта, состав/разбиение/переименование листов;
- PROJECT_CHANGE: параметры объекта, систем и решений — площади, этажность, нагрузки,
  размеры, материалы, оборудование, потребители, методы расчёта и принципы работы;
- REVIEW: UNCERTAIN, OCR/неоднозначность, справка без подтверждения, спорное переименование.

Объединяй evidence только по одному инженерному признаку: площади отдельно, нагрузки
отдельно, метод расчёта отдельно. Не создавай общий вывод «изменена часть проекта».
Pure rename без изменения параметров — SERVICE_STRUCTURE; rename вместе с параметрами —
PROJECT_CHANGE. Для страницы «Содержание тома» все изменения структуры документации —
SERVICE_STRUCTURE. Сведения из «Справки об изменениях» без подтверждающего project evidence
относи в REVIEW.

title должен быть кратким factual выводом без Markdown. НЕ пиши в title агрегированное
число вроде «20 помещений» или «11 нагрузок»: backend сам посчитает evidence_ids и добавит
число. Разрешены только числа/обозначения, буквально присутствующие в evidence.
Не придумывай evidence_id, объект или причинно-следственную связь. Верни только JSON.
"""


class SummaryValidationError(ValueError):
    """A Stage 5 model group cannot be trusted."""


def _normalize(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", text).strip()


def _title_body(label: str) -> str:
    value = re.sub(r"^\s*(?:лист|страница)\s+[\w.-]+\s*(?:[-—:]\s*)?", "", str(label or ""), flags=re.I)
    return value.strip()


def _meaningful_titles(labels: Iterable[str]) -> list[str]:
    output = []
    for label in labels:
        title = _title_body(str(label))
        if title and not re.fullmatch(r"(?:лист|страница)?\s*\d+(?:[.]\d+)?", title, re.I):
            output.append(title)
    return output


def _purpose_families(titles: Iterable[str]) -> set[str]:
    joined = "\n".join(titles)
    return {name for name, pattern in _PURPOSE_PATTERNS if pattern.search(joined)}


def _purpose_tokens(titles: Iterable[str]) -> set[str]:
    stop = {
        "архитектурные", "решения", "часть", "лист", "листа", "схема", "расчетная",
        "расчетной", "в", "и", "по", "на", "из", "для", "оси", "осях", "этаж",
    }
    return {
        token for token in re.findall(r"[a-zа-я0-9]+", _normalize(" ".join(titles)))
        if len(token) > 2 and token not in stop
    }


def precheck_sheet_purpose(group: dict[str, Any]) -> dict[str, Any]:
    """Conservatively flag only clear purpose conflicts; never mutate links."""
    titles = {
        "left": _meaningful_titles(group.get("left_labels") or []),
        "right": _meaningful_titles(group.get("right_labels") or []),
    }
    if not titles["left"] or not titles["right"]:
        return {
            "status": PAIR_OK,
            "left_titles": titles["left"], "right_titles": titles["right"],
            "reason": "purpose_title_unavailable_no_conflict_detected",
            "confidence": "low",
        }
    families = {side: _purpose_families(value) for side, value in titles.items()}
    if families["left"] & families["right"]:
        return {
            "status": PAIR_OK,
            "left_titles": titles["left"], "right_titles": titles["right"],
            "reason": "shared_sheet_purpose_family", "confidence": "high",
            "purpose_families": {side: sorted(value) for side, value in families.items()},
        }
    tokens = {side: _purpose_tokens(value) for side, value in titles.items()}
    overlap = len(tokens["left"] & tokens["right"]) / max(1, min(len(tokens["left"]), len(tokens["right"])))
    if overlap >= 0.35:
        return {
            "status": PAIR_OK,
            "left_titles": titles["left"], "right_titles": titles["right"],
            "reason": "sheet_purpose_title_overlap", "confidence": "medium",
            "title_overlap": round(overlap, 4),
        }
    return {
        "status": PAIR_REVIEW_REQUIRED,
        "left_titles": titles["left"], "right_titles": titles["right"],
        "reason": "sheet_purpose_conflict", "confidence": "high",
        "purpose_families": {side: sorted(value) for side, value in families.items()},
        "title_overlap": round(overlap, 4),
    }


def _evidence_text(evidence: dict[str, Any]) -> str:
    return "\n".join(str(evidence.get(key) or "") for key in ("summary", "before", "after", "reason"))


def _is_contents_group(group: dict[str, Any]) -> bool:
    return any("содержание тома" in _normalize(label) for label in [
        *(group.get("left_labels") or []), *(group.get("right_labels") or []),
    ])


def deterministic_class_hint(evidence: dict[str, Any], group: dict[str, Any]) -> tuple[str, str]:
    """Hard safety hints plus a conservative category for provider fallback."""
    text = _normalize(_evidence_text(evidence))
    if evidence.get("source_status") == "UNCERTAIN":
        return "REVIEW", "uncertain"
    if _is_contents_group(group):
        return "SERVICE_STRUCTURE", "documentation_structure"
    if (
        "запись об изменении" in text
        or "в соответствии с изменениями в проектных решениях" in text
    ):
        category = next(
            (value for value, pattern in _PROJECT_PATTERNS if pattern.search(text)),
            "uncertain",
        )
        return "REVIEW", category
    project_category = next((category for category, pattern in _PROJECT_PATTERNS if pattern.search(text)), "")
    if _RENAME_RE.search(text):
        if project_category or len(set(_NUMBER_RE.findall(str(evidence.get("before") or ""))) ^ set(
            _NUMBER_RE.findall(str(evidence.get("after") or ""))
        )):
            return "PROJECT_CHANGE", project_category or "equipment_parameters"
        return "SERVICE_STRUCTURE", "designation_rename"
    if _STRUCTURE_RE.search(text):
        return "SERVICE_STRUCTURE", "documentation_structure"
    if _SERVICE_RE.search(text) or re.search(r"\b(?:19|20)\d{2}\b", text):
        return "SERVICE_STRUCTURE", "administrative"
    if project_category:
        return "PROJECT_CHANGE", project_category
    return "UNCLASSIFIED", "uncertain"


def _evidence_id(group_id: str, bucket: str, index: int, item: dict[str, Any]) -> str:
    source = {
        "group_id": group_id, "bucket": bucket, "index": index,
        "left": item.get("left_fragment_ids") or [],
        "right": item.get("right_fragment_ids") or [],
    }
    digest = hashlib.sha256(json.dumps(source, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return f"ev_{digest[:16]}"


def build_source_groups(final_comparison: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert Stage 4 buckets into stable atomic evidence records."""
    output = []
    for raw_group in final_comparison.get("sheet_groups") or []:
        group = {
            "group_id": str(raw_group.get("id") or ""),
            "left_pages": list(raw_group.get("left_pages") or []),
            "right_pages": list(raw_group.get("right_pages") or []),
            "left_labels": list(raw_group.get("left_labels") or []),
            "right_labels": list(raw_group.get("right_labels") or []),
            "review_status": raw_group.get("review_status"),
        }
        evidence = []
        for bucket, status in (
            ("changed", "CHANGED"), ("removed", "REMOVED"),
            ("added", "ADDED"), ("uncertain", "UNCERTAIN"),
        ):
            for index, item in enumerate(raw_group.get(bucket) or []):
                record = {
                    "evidence_id": _evidence_id(group["group_id"], bucket, index, item),
                    "source_status": str(item.get("final_status") or status),
                    "summary": str(item.get("summary") or item.get("reason") or status),
                    "before": item.get("before"), "after": item.get("after"),
                    "reason": item.get("reason"),
                    "left_fragment_ids": list(item.get("left_fragment_ids") or []),
                    "right_fragment_ids": list(item.get("right_fragment_ids") or []),
                    "left_pages": list(item.get("left_pages") or []),
                    "right_pages": list(item.get("right_pages") or []),
                    "left_anchors": list(item.get("left_anchors") or []),
                    "right_anchors": list(item.get("right_anchors") or []),
                }
                hint, category = deterministic_class_hint(record, {**group, **raw_group})
                record["deterministic_class_hint"] = hint
                record["deterministic_category_hint"] = category
                evidence.append(record)
        if evidence:
            group["pair_precheck"] = precheck_sheet_purpose(group)
            group["atomic_evidence"] = evidence
            encoded = json.dumps(group, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            group["source_group_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
            output.append(group)
    return output


def prompt_for_groups(groups: list[dict[str, Any]]) -> str:
    payload = []
    for group in groups:
        payload.append({
            "group_id": group["group_id"],
            "left_titles": group.get("pair_precheck", {}).get("left_titles") or group.get("left_labels") or [],
            "right_titles": group.get("pair_precheck", {}).get("right_titles") or group.get("right_labels") or [],
            "atomic_evidence": [
                {
                    "evidence_id": item["evidence_id"],
                    "source_status": item["source_status"],
                    "summary": item["summary"], "before": item.get("before"),
                    "after": item.get("after"), "reason": item.get("reason"),
                    "deterministic_class_hint": item["deterministic_class_hint"],
                    "deterministic_category_hint": item["deterministic_category_hint"],
                }
                for item in group.get("atomic_evidence") or []
            ],
        })
    return (
        SYSTEM_PROMPT + "\nJSON Schema:\n"
        + json.dumps(RESPONSE_SCHEMA, ensure_ascii=False, separators=(",", ":"))
        + "\nINPUT:\n" + json.dumps({"groups": payload}, ensure_ascii=False, separators=(",", ":"))
    )


def _supported_title(title: str, evidence: list[dict[str, Any]]) -> bool:
    source = "\n".join(_evidence_text(item) for item in evidence)
    if _AUDIT_LANGUAGE_RE.search(title) or _MODEL_COUNT_RE.search(title):
        return False
    if not set(_NUMBER_RE.findall(title)) <= set(_NUMBER_RE.findall(source)):
        return False
    source_ids = {value.lower() for value in _IDENTIFIER_RE.findall(source)}
    return {value.lower() for value in _IDENTIFIER_RE.findall(title)} <= source_ids


def validate_group_response(response_group: Any, source_group: dict[str, Any]) -> list[dict[str, Any]]:
    expected_group_keys = {"group_id", "items"}
    if not isinstance(response_group, dict) or set(response_group) != expected_group_keys:
        raise SummaryValidationError("invalid_group_schema")
    if str(response_group.get("group_id")) != str(source_group.get("group_id")):
        raise SummaryValidationError("wrong_group_id")
    items = response_group.get("items")
    if not isinstance(items, list):
        raise SummaryValidationError("items_not_list")
    evidence_by_id = {
        str(item["evidence_id"]): item for item in source_group.get("atomic_evidence") or []
    }
    used: set[str] = set()
    normalized = []
    for index, item in enumerate(items):
        prefix = f"item_{index}"
        if not isinstance(item, dict) or set(item) != {"class", "category", "title", "evidence_ids"}:
            raise SummaryValidationError(f"{prefix}_schema")
        item_class, category = item["class"], item["category"]
        if item_class not in CLASSES or category not in CATEGORIES:
            raise SummaryValidationError(f"{prefix}_enum")
        ids = item["evidence_ids"]
        if not isinstance(ids, list) or not ids or any(not isinstance(value, str) for value in ids):
            raise SummaryValidationError(f"{prefix}_evidence_ids")
        if len(ids) != len(set(ids)) or used & set(ids):
            raise SummaryValidationError(f"{prefix}_duplicate_evidence")
        if any(value not in evidence_by_id for value in ids):
            raise SummaryValidationError(f"{prefix}_hallucinated_evidence")
        selected = [evidence_by_id[value] for value in ids]
        if item_class == "PROJECT_CHANGE" and any(
            value["deterministic_class_hint"] == "SERVICE_STRUCTURE" for value in selected
        ):
            raise SummaryValidationError(f"{prefix}_project_from_service_evidence")
        if item_class != "REVIEW" and any(
            value["deterministic_class_hint"] == "REVIEW" for value in selected
        ):
            raise SummaryValidationError(f"{prefix}_uncertain_must_remain_review")
        title = item["title"]
        if not isinstance(title, str) or not title.strip() or len(title.strip()) > 260:
            raise SummaryValidationError(f"{prefix}_title")
        if not _supported_title(title, selected):
            raise SummaryValidationError(f"{prefix}_unsupported_title")
        used.update(ids)
        normalized.append({
            "class": item_class, "category": category,
            "title": title.strip(), "evidence_ids": list(ids),
        })
    if used != set(evidence_by_id):
        raise SummaryValidationError("incomplete_evidence_coverage")
    return normalized


def validate_response(
    payload: Any, source_groups: list[dict[str, Any]], *,
    recover_single_group_id: bool = False,
) -> list[list[dict[str, Any]]]:
    if not isinstance(payload, dict) or set(payload) != {"groups"}:
        raise SummaryValidationError("invalid_response_schema")
    groups = payload.get("groups")
    if not isinstance(groups, list):
        raise SummaryValidationError("groups_not_list")
    expected = {str(group["group_id"]): group for group in source_groups}
    if recover_single_group_id and len(expected) == 1 and groups:
        # Every production call contains exactly one source group.  A model can
        # mistype its opaque id or incorrectly use the outer array for semantic
        # subgroups.  Flattening is safe because validation below still demands
        # exact evidence coverage, unique ids and supported titles/classes.
        if all(
            isinstance(group, dict) and set(group) == {"group_id", "items"}
            and isinstance(group.get("items"), list)
            for group in groups
        ):
            groups = [{
                "group_id": next(iter(expected)),
                "items": [item for group in groups for item in group["items"]],
            }]
    received: dict[str, dict[str, Any]] = {}
    for group in groups:
        group_id = str(group.get("group_id") or "") if isinstance(group, dict) else ""
        if group_id not in expected or group_id in received:
            raise SummaryValidationError("unexpected_or_duplicate_group")
        received[group_id] = group
    if set(received) != set(expected):
        raise SummaryValidationError("incomplete_group_coverage")
    return [validate_group_response(received[str(group["group_id"])], group) for group in source_groups]


def deterministic_fallback_items(source_group: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = list(source_group.get("atomic_evidence") or [])
    return [{
        "class": "REVIEW", "category": "uncertain",
        "title": "Классификация группы требует проверки",
        "evidence_ids": [item["evidence_id"] for item in evidence],
    }] if evidence else []


def _subject_from_evidence(evidence: list[dict[str, Any]]) -> str:
    source = "\n".join(_evidence_text(item) for item in evidence)
    match = re.search(r"\b(?:ВРУ|ГРЩ|ЩР|ЩО|ЩАО)[-.\s]?[А-ЯA-Z0-9]+\b", source, re.I)
    return match.group(0).upper().replace(" ", "-") if match else ""


def _backend_title(item: dict[str, Any], evidence: list[dict[str, Any]]) -> str:
    title = str(item.get("title") or "Изменение").strip().rstrip(".")
    count, category = len(evidence), str(item.get("category") or "")
    if count <= 1:
        return title + "."
    if category == "areas":
        return f"Скорректированы площади {count} помещений."
    if category == "consumer_composition":
        subject = _subject_from_evidence(evidence)
        statuses = {value.get("source_status") for value in evidence}
        if statuses == {"ADDED"}:
            return f"Расширен состав потребителей{(' ' + subject) if subject else ''}: добавлено {count} нагрузок."
        if statuses == {"REMOVED"}:
            return f"Сокращён состав потребителей{(' ' + subject) if subject else ''}: исключено {count} нагрузок."
        return f"Изменён состав потребителей{(' ' + subject) if subject else ''}: {count} изменений."
    if category == "documentation_structure":
        return f"Изменена структура комплекта документации ({count} фактов)."
    if category == "administrative":
        return f"Изменены служебные сведения ({count} фактов)."
    return f"{title} ({count} связанных изменений)."


def _aggregate_id(group_id: str, item: dict[str, Any]) -> str:
    source = f"{group_id}:{item['class']}:{item['category']}:{','.join(item['evidence_ids'])}"
    return "chg_" + hashlib.sha256(source.encode()).hexdigest()[:16]


def build_group_summary(
    source_group: dict[str, Any], items: list[dict[str, Any]], *,
    aggregation_status: str, error: str | None, usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_by_id = {item["evidence_id"]: item for item in source_group["atomic_evidence"]}
    buckets = {"PROJECT_CHANGE": [], "SERVICE_STRUCTURE": [], "REVIEW": []}
    for item in items:
        evidence = [evidence_by_id[value] for value in item["evidence_ids"]]
        buckets[item["class"]].append({
            "id": _aggregate_id(source_group["group_id"], item),
            "title": _backend_title(item, evidence),
            "model_title": item["title"],
            "category": item["category"],
            "evidence_ids": list(item["evidence_ids"]),
            "count": len(item["evidence_ids"]),
            "details": evidence,
        })
    return {
        "group_id": source_group["group_id"],
        "left_pages": source_group["left_pages"], "right_pages": source_group["right_pages"],
        "left_labels": source_group["left_labels"], "right_labels": source_group["right_labels"],
        "pair_precheck": source_group["pair_precheck"],
        "source_group_sha256": source_group["source_group_sha256"],
        "aggregation_status": aggregation_status, "error": error,
        "usage": usage or {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "duration_ms": 0},
        "project_changes": buckets["PROJECT_CHANGE"],
        "service_structure": buckets["SERVICE_STRUCTURE"],
        "review": buckets["REVIEW"],
        "atomic_evidence": source_group["atomic_evidence"],
    }


def build_wrong_pair_summary(source_group: dict[str, Any]) -> dict[str, Any]:
    evidence = source_group["atomic_evidence"]
    item = {
        "class": "REVIEW", "category": "uncertain",
        "title": "Возможно неверно сопоставлены листы",
        "evidence_ids": [value["evidence_id"] for value in evidence],
    }
    return build_group_summary(
        source_group, [item], aggregation_status="pair_review_required",
        error="sheet_purpose_conflict",
    )


def build_artifact(
    *, pair_id: str, generated_at: str, source_signature_value: str,
    sheet_groups: list[dict[str, Any]], fresh_model_calls: int,
) -> dict[str, Any]:
    usage = {key: sum(int(group.get("usage", {}).get(key) or 0) for group in sheet_groups) for key in (
        "input_tokens", "output_tokens", "cached_tokens", "duration_ms",
    )}
    pair_ok = sum(group["pair_precheck"]["status"] == PAIR_OK for group in sheet_groups)
    pair_review = len(sheet_groups) - pair_ok
    project_items = [item for group in sheet_groups for item in group["project_changes"]]
    service_items = [item for group in sheet_groups for item in group["service_structure"]]
    review_items = [item for group in sheet_groups for item in group["review"]]
    failed = sum(group["aggregation_status"] == "deterministic_fallback" for group in sheet_groups)
    represented_model_calls = sum(
        group["aggregation_status"] in {"ai_aggregated", "deterministic_fallback"}
        for group in sheet_groups
    )
    status = "completed" if not failed else "failed" if failed == len(sheet_groups) else "partial"
    return {
        "version": VERSION, "kind": KIND, "pair_id": pair_id,
        "generated_at": generated_at, "source_signature": source_signature_value,
        "prompt_version": PROMPT_VERSION, "validator_version": VALIDATOR_VERSION,
        "model": PRODUCTION_MODEL, "reasoning_effort": PRODUCTION_REASONING_EFFORT,
        "status": status, "sheet_groups": sheet_groups,
        "summary": {
            "groups": len(sheet_groups), "pair_ok": pair_ok,
            "pair_review_required": pair_review,
            "atomic_evidence": sum(len(group["atomic_evidence"]) for group in sheet_groups),
            "project_change_evidence": sum(item["count"] for item in project_items),
            "project_changes": len(project_items),
            "service_structure_evidence": sum(item["count"] for item in service_items),
            "service_structure": len(service_items),
            "review_evidence": sum(item["count"] for item in review_items),
            "review": len(review_items),
            "model_calls": represented_model_calls,
            "represented_model_calls": represented_model_calls,
            "fresh_model_calls": fresh_model_calls,
            "fallback_groups": failed,
            **usage,
        },
        "constraints": {
            "stage4_immutable": True, "atomic_evidence_preserved": True,
            "sheet_links_mutated": False, "images_sent": False, "full_markdown_sent": False,
            "model_role": "classifier_and_aggregator_only",
            "counts_computed_by_backend": True,
            "fallback_policy": "review_only_v1",
        },
    }


def source_signature(final_comparison: dict[str, Any], source_groups: list[dict[str, Any]]) -> str:
    source = {
        "version": VERSION, "prompt_version": PROMPT_VERSION,
        "validator_version": VALIDATOR_VERSION, "model": PRODUCTION_MODEL,
        "reasoning_effort": PRODUCTION_REASONING_EFFORT,
        "stage4_source_signature": final_comparison.get("source_signature"),
        "groups": [group.get("source_group_sha256") for group in source_groups],
    }
    return hashlib.sha256(json.dumps(source, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def public_view(payload: dict[str, Any] | None, *, stale: bool = False) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or payload.get("version") != VERSION or payload.get("kind") != KIND:
        return None
    return {**payload, "stale": bool(stale)}


__all__ = [
    "CATEGORIES", "CLASSES", "KIND", "PAIR_OK", "PAIR_REVIEW_REQUIRED",
    "PRODUCTION_MODEL", "PRODUCTION_REASONING_EFFORT", "PROMPT_VERSION",
    "RESPONSE_SCHEMA", "SYSTEM_PROMPT", "SummaryValidationError", "VALIDATOR_VERSION",
    "VERSION", "build_artifact", "build_group_summary", "build_source_groups",
    "build_wrong_pair_summary", "deterministic_class_hint", "deterministic_fallback_items",
    "precheck_sheet_purpose", "prompt_for_groups", "public_view", "source_signature",
    "validate_group_response", "validate_response",
]
