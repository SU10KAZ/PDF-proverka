"""Предварительный отчёт сравнения — что система нашла, до решений инженера.

Зачем он нужен
--------------
До этого отчёта единственным способом увидеть результат анализа был перечень
внутренних атомов: строки вида «PARAMETER / MATERIAL_CHANGE / HIGH» с
техническими идентификаторами. Чтобы понять смысл прогона, инженеру
приходилось сначала разобрать полсотни таких строк. Отчёт отвечает на вопрос
«что изменилось», а не «какие атомы существуют».

Чем он НЕ является
------------------
Это не итоговый отчёт. Итоговый по-прежнему собирается только из
подтверждённых инженером находок. Предварительный показывает всё, но честно
разделяет найденное, требующее проверки, противоречия самого документа и
недоказанное — четырьмя явными статусами, без внутренних кодов.

Группировка здесь только зрительная: «ХМ1 — холодильная машина» собирает под
одним заголовком и номинал аппарата, и мощность, и ток. Решение инженера
остаётся атомарным — по одному на каждое свойство.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable, Mapping, Optional, Sequence

KIND = "stage_comparison_preliminary_report"
SCHEMA_VERSION = "preliminary-comparison-report.v1"
PRODUCER = "preliminary-comparison-report-v1"

#: Ровно пять статусов. Внутренние коды (MATERIAL_CHANGE, REVIEW_REQUIRED,
#: UNKNOWN_DIMENSION) в отчёт не попадают: они описывают устройство конвейера,
#: а не состояние проекта. Название модели и уровень рассуждения — тем более:
#: инженеру нужно знать, чем находка доказана, а не кем посчитана.
STATUS_AUTOMATIC = "Найдено автоматически"
#: Происхождение, а не степень доверия. Пара строк предложена ИИ, но каждое
#: число сверено правилами по самим строкам, и в отчёт находка попадает только
#: пройдя детерминированный верификатор. Отдельный статус нужен, чтобы
#: инженер видел разницу: «нашлось само» и «подсказано и проверено» — это
#: разные основания, даже когда результат одинаковый.
STATUS_AI_VERIFIED = "Уточнено ИИ и проверено правилами"
STATUS_REVIEW = "Требуется проверка инженера"
STATUS_INCONSISTENCY = "Внутреннее противоречие документа"
STATUS_UNPROVEN = "Недостаточно доказательств"
STATUSES = (
    STATUS_AUTOMATIC,
    STATUS_AI_VERIFIED,
    STATUS_REVIEW,
    STATUS_INCONSISTENCY,
    STATUS_UNPROVEN,
)

SECTION_SUMMARY = "summary"
SECTION_SCHEME = "scheme"
SECTION_EQUIPMENT = "equipment"
SECTION_PARAMETERS = "parameters"
SECTION_AI_VERIFIED = "ai_verified"
SECTION_INCONSISTENCIES = "inconsistencies"
SECTION_REVIEW = "review"
SECTION_UNPROVEN = "unproven"
SECTION_TEXT_REQUIREMENTS = "text_requirements"
SECTION_METADATA = "metadata_changes"

_MODE_REVIEW_REASONS = frozenset(
    {"mode_label_mismatch", "mode_label_unknown", "mode_scope_mismatch"}
)

#: Род названия свойства — чтобы «увеличен» согласовывался с «током», а
#: «увеличена» с «мощностью». Без этого отчёт читается как машинный перевод.
_FACET_GENDER = {
    "rated_current_a": "m",
    "maximum_calculated_current_a": "m",
    "setting_current_a": "m",
    "installed_power_kw": "f",
    "demand_active_power_kw": "f",
    "demand_reactive_power_kvar": "f",
    "installed_reactive_power_kvar": "f",
    "demand_apparent_power_kva": "f",
    "cable_parallel_count": "n",
    "device_status": "n",
}
_VERB = {
    ("INCREASED", "m"): "увеличен",
    ("INCREASED", "f"): "увеличена",
    ("INCREASED", "n"): "увеличено",
    ("DECREASED", "m"): "уменьшен",
    ("DECREASED", "f"): "уменьшена",
    ("DECREASED", "n"): "уменьшено",
    ("ALTERED", "m"): "изменён",
    ("ALTERED", "f"): "изменена",
    ("ALTERED", "n"): "изменено",
    ("REPLACED", "m"): "заменён",
    ("REPLACED", "f"): "заменена",
    ("REPLACED", "n"): "заменено",
}

#: Тип аппарата на языке чертежа, а не перечисления.
_DEVICE_TYPES = {
    "CIRCUIT_BREAKER": "автоматический выключатель",
    "SWITCH_DISCONNECTOR": "разъединитель",
    "SWITCH": "выключатель нагрузки",
    "FUSE": "предохранитель",
    "CONTACTOR": "контактор",
    "UNKNOWN_DEVICE": "аппарат неопределённого типа",
}

#: Названия узлов, которые на чертеже подписаны иначе, чем в графе.
_SUBJECT_TITLES = {
    "SECTION-TIE#BUS1-BUS2": "Секционный аппарат между секциями 1 и 2",
    "INPUT#BUS1": "Вводной выключатель секции 1",
    "INPUT#BUS2": "Вводной выключатель секции 2",
}
_SUBJECT_HINTS = {
    "ХМ1": "холодильная машина",
    "ХМ2": "холодильная машина",
    "ДР1-ХМ1": "охладитель холодильной машины",
    "ДР2-ХМ2": "охладитель холодильной машины",
    "АУКРМ-1": "установка компенсации реактивной мощности",
    "АУКРМ-2": "установка компенсации реактивной мощности",
    "ВРУ-ИТП": "вводно-распределительное устройство ИТП",
    "ВРУ-АПТ": "вводно-распределительное устройство насосной АПТ",
    "ВРУ-НСТ": "вводно-распределительное устройство хозпитьевого водоснабжения",
    "ВРУ-ХЦ": "вводно-распределительное устройство холодильного центра",
    "ВРУ-А": "вводно-распределительное устройство автостоянки",
    "ШНО": "щит наружного освещения",
    "ЯСН-ТП": "ящик собственных нужд ТП",
    "ЭБ-ГВС": "резервные баки ГВС",
}

_TECHNICAL_SUBJECT_REF = re.compile(
    r"^(?:graphic(?:[._]subject)?[.:_]|text_entity:|project_(?:text_)?entity_|"
    r"u(?:review|chg)_|hquestion_)",
    re.IGNORECASE,
)


#: Названия семейств щитов, которые граф снимает с подписи при опознании.
#: Свернуть укороченный ключ можно ТОЛЬКО через них: «ДР1» — обозначение
#: охладителя, а не семейство, и «ХМ1» ни при каких условиях не должен
#: оказаться внутри группы «ДР1-ХМ1».
_PANEL_FAMILIES = frozenset({"ВРУ", "ШУ", "ЩУ", "ШР", "ЩР", "ЭБ", "ЯСН"})


def _stable_id(prefix: str, *parts: Any) -> str:
    payload = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def format_number(value: Any) -> str:
    """Число по-русски: запятая как разделитель, без пустого хвоста.

    «335.0» в отчёте выглядит как недоделанная выгрузка, «335» — как значение
    с чертежа.
    """
    if isinstance(value, bool) or value is None:
        return str(value)
    if isinstance(value, (int, float)):
        if float(value).is_integer():
            return str(int(value))
        return f"{value}".replace(".", ",")
    if isinstance(value, (list, tuple)):
        return "/".join(format_number(item) for item in value)
    return str(value)


def _atom_provenance(change: Mapping[str, Any]) -> dict[str, Any]:
    atoms = (change.get("provenance") or {}).get("source_atoms") or []
    if not atoms:
        return {}
    return dict((atoms[0] or {}).get("provenance") or {})


def _structured(change: Mapping[str, Any]) -> dict[str, Any]:
    return dict(_atom_provenance(change).get("structured") or {})


def subject_identity(change: Mapping[str, Any]) -> Optional[str]:
    """Обозначение объекта, к которому относится изменение."""
    subject = _structured(change).get("subject") or {}
    identity = subject.get("identity")
    if isinstance(identity, list) and identity:
        return str(identity[0])
    return None


def subject_title(identity: Optional[str]) -> str:
    """Заголовок группы на языке чертежа."""
    if not identity:
        return "Схема в целом"
    if identity in _SUBJECT_TITLES:
        return _SUBJECT_TITLES[identity]
    hint = _SUBJECT_HINTS.get(identity)
    return f"{identity} — {hint}" if hint else identity


def subject_name(identity: Optional[str]) -> str:
    """Имя объекта внутри фразы.

    Внутренние идентификаторы («SECTION-TIE#BUS1-BUS2», «INPUT#BUS1») в текст
    отчёта не попадают: инженер читает чертёж, а не устройство графа.
    Обозначения самого чертежа («ХМ1», «ВРУ4») остаются как есть — они и есть
    язык документа.
    """
    if not identity:
        return "объект схемы"
    if identity in _SUBJECT_TITLES or "#" in identity:
        return _SUBJECT_TITLES.get(identity, "объект схемы")
    return identity


def _facet_title(change: Mapping[str, Any]) -> Optional[str]:
    relation = _structured(change).get("relation") or {}
    title = relation.get("facet_title")
    return str(title) if title else None


def _unit(change: Mapping[str, Any]) -> str:
    relation = _structured(change).get("relation") or {}
    unit = relation.get("unit") or _atom_provenance(change).get("unit")
    return f" {unit}" if unit else ""


def _display_values(change: Mapping[str, Any]) -> tuple[Any, Any]:
    """Values as an engineer reads them, using only persisted fact metadata."""
    relation = _structured(change).get("relation") or {}
    dimension = str(change.get("dimension") or "")
    if dimension == "TYPE":
        before = relation.get("left_effective_type", change.get("before_value"))
        after = relation.get("right_effective_type", change.get("after_value"))
        return (
            _DEVICE_TYPES.get(str(before), before),
            _DEVICE_TYPES.get(str(after), after),
        )
    before = change.get("before_value")
    after = change.get("after_value")
    if before is None:
        before = relation.get("left_value", relation.get("left_count"))
    if after is None:
        after = relation.get("right_value", relation.get("right_count"))
    return before, after


def change_property_label(change: Mapping[str, Any]) -> str:
    """Concrete Russian property label backed by the finding's metadata."""
    relation = _structured(change).get("relation") or {}
    facet_title = _facet_title(change)
    direction = str(change.get("direction") or "ALTERED")
    base_facet = (
        relation.get("base_facet_ref")
        or _atom_provenance(change).get("base_facet_ref")
        or change.get("facet_ref")
    )
    if facet_title:
        gender = _FACET_GENDER.get(str(base_facet or ""), "m")
        verb = _VERB.get((direction, gender), _VERB[("ALTERED", gender)])
        return f"{facet_title} {verb}"
    dimension = str(change.get("dimension") or "")
    if dimension == "TYPE":
        return "Тип аппарата изменён"
    if dimension == "QUANTITY":
        return "Число отходящих линий изменено"
    subject = _structured(change).get("subject") or {}
    if subject.get("kind") == "reserve_function":
        return "Число резервных линий изменено"
    if subject.get("kind") == "unresolved_correspondence":
        return "Соответствие элементов требует уточнения"
    return "Свойство не удалось однозначно определить"


def change_review_presentation(change: Mapping[str, Any]) -> dict[str, Any]:
    """Human fields shared by Preliminary Report and Engineer Review.

    This is a read-model only.  It does not create an identity, infer a facet,
    or alter the atomic finding; every label and unit comes from the same
    persisted structured metadata that :func:`describe_change` already uses.
    """
    identity = subject_identity(change)
    human_identity = (
        identity
        if identity and not _TECHNICAL_SUBJECT_REF.search(identity)
        else None
    )
    subject = _structured(change).get("subject") or {}
    scheme_level = subject.get("kind") in _SCHEME_SUBJECT_KINDS
    before, after = _display_values(change)
    unit = _unit(change).strip()

    def display(value: Any) -> Optional[str]:
        if value is None:
            return None
        rendered = format_number(value)
        if unit and unit.casefold() not in rendered.casefold():
            return f"{rendered} {unit}"
        return rendered

    provenance = _atom_provenance(change)
    detail = []
    if provenance.get("section_ref"):
        detail.append(f"секция {provenance['section_ref']}")
    if provenance.get("input_number"):
        detail.append(f"ввод {provenance['input_number']}")
    if provenance.get("mode_label"):
        detail.append(f"режим: {provenance['mode_label']}")
    return {
        "version": "engineer-review-presentation.v1",
        "object_label": (
            subject_title(human_identity)
            if human_identity
            else "Схема в целом" if scheme_level else None
        ),
        "object_known": bool(human_identity or scheme_level),
        "property_label": change_property_label(change),
        "before_display": display(before),
        "after_display": display(after),
        "unit": unit or None,
        "detail": ", ".join(detail) or None,
        "summary": describe_change(change),
    }


def describe_change(change: Mapping[str, Any]) -> str:
    """Одно изменение человеческой фразой.

    Инженер должен прочитать «номинальный ток увеличен с 2500 до 3200 А», а не
    «PARAMETER / MATERIAL_CHANGE / HIGH».
    """
    dimension = str(change.get("dimension") or "")
    direction = str(change.get("direction") or "ALTERED")
    identity = subject_identity(change)
    name = subject_name(identity)
    relation = _structured(change).get("relation") or {}

    if dimension == "TYPE":
        before = _DEVICE_TYPES.get(
            str(relation.get("left_effective_type")), relation.get("left_effective_type")
        )
        after = _DEVICE_TYPES.get(
            str(relation.get("right_effective_type")),
            relation.get("right_effective_type"),
        )
        return f"{name}: {before} заменён на {after}."

    if dimension == "QUANTITY":
        before = relation.get("left_count", change.get("before_value"))
        after = relation.get("right_count", change.get("after_value"))
        return (
            f"Число отходящих линий на схеме изменено с {format_number(before)} "
            f"на {format_number(after)}."
        )

    facet_title = _facet_title(change)
    before = change.get("before_value")
    after = change.get("after_value")
    if before is None and after is None:
        before = relation.get("left_value")
        after = relation.get("right_value")

    if facet_title and before is not None and after is not None:
        base_facet = (
            relation.get("base_facet_ref")
            or _atom_provenance(change).get("base_facet_ref")
            or change.get("facet_ref")
        )
        gender = _FACET_GENDER.get(str(base_facet or ""), "m")
        verb = _VERB.get((direction, gender), _VERB[("ALTERED", gender)])
        unit = _unit(change)
        return (
            f"{name}: {facet_title.lower()} {verb} "
            f"с {format_number(before)} до {format_number(after)}{unit}."
        )

    subject = _structured(change).get("subject") or {}
    if subject.get("kind") == "reserve_function":
        before = relation.get("left_count", 0)
        after = relation.get("right_count", 0)
        return (
            f"Число резервных линий изменено с {format_number(before)} "
            f"на {format_number(after)}."
        )
    if subject.get("kind") == "unresolved_correspondence":
        return (
            "Часть узлов схемы не удалось сопоставить между редакциями "
            "однозначно."
        )
    if facet_title:
        return f"{name}: {facet_title.lower()} изменено."
    return f"{name}: изменение структуры схемы."


def _evidence_of(change: Mapping[str, Any]) -> dict[str, Any]:
    """Ссылки на места в обоих документах для кнопки «Открыть доказательство»."""
    provenance = _atom_provenance(change)
    evidence = provenance.get("evidence")
    if isinstance(evidence, Mapping):
        return {
            side: {
                key: value
                for key, value in (evidence.get(side) or {}).items()
                if key in {"page_index", "bbox", "raw", "raw_run", "consumer_label", "row_id"}
            }
            for side in ("LEFT", "RIGHT")
            if isinstance(evidence.get(side), Mapping)
        }
    return {}


def _is_review(change: Mapping[str, Any]) -> bool:
    if str(change.get("review_status") or "") == "REVIEW_REQUIRED":
        return True
    if str(change.get("outcome") or "") == "REVIEW_REQUIRED":
        return True
    return str((change.get("confidence") or {}).get("level") or "") in {"LOW", "UNKNOWN"}


def _atom_provenances(change: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Провенанс ВСЕХ исходных атомов находки, а не только первого.

    У кросс-источникового изменения атомов два, и метка ИИ может стоять на
    втором: чтение только нулевого молча теряло бы происхождение половины
    уточнённых находок.
    """
    atoms = (change.get("provenance") or {}).get("source_atoms") or []
    return [
        dict(atom.get("provenance") or {})
        for atom in atoms if isinstance(atom, Mapping)
    ]


def _is_ai_verified(change: Mapping[str, Any]) -> bool:
    """Находка получена из разрешения ИИ, прошедшего детерминированные проверки."""
    return any(
        provenance.get("ai_change_resolution")
        or provenance.get("ai_verified_relation")
        for provenance in _atom_provenances(change)
    )


def change_is_review(change: Mapping[str, Any]) -> bool:
    """Публичное имя для «эта находка уедет в раздел проверки инженера».

    Инвентаризация маршрутизации обязана считать «нерешённым» ровно то же,
    что показывает отчёт. Своя копия правила разошлась бы с отчётом при
    первой же правке — и картина маршрутов начала бы описывать не тот прогон,
    к которому приложена.
    """
    return _is_review(change)


def _change_item(change: Mapping[str, Any]) -> dict[str, Any]:
    provenance = _atom_provenance(change)
    notes = [str(note) for note in (provenance.get("notes") or []) if note]
    # Ветка ИИ стоит ПЕРВОЙ намеренно: применение разрешения не трогает
    # уверенность атома, она остаётся UNKNOWN, и проверка на review увела бы
    # уточнённую находку обратно в раздел проверки инженера — туда, откуда её
    # только что вынули.
    if _is_ai_verified(change):
        status = STATUS_AI_VERIFIED
    elif _is_review(change):
        status = STATUS_REVIEW
    else:
        status = STATUS_AUTOMATIC
    section = provenance.get("section_ref")
    detail_parts = []
    if section:
        detail_parts.append(f"секция {section}")
    if provenance.get("input_number"):
        detail_parts.append(f"ввод {provenance['input_number']}")
    if provenance.get("mode_label"):
        detail_parts.append(f"режим: {provenance['mode_label']}")
    return {
        "item_id": _stable_id("pritem", change.get("change_id")),
        "status": status,
        "text": describe_change(change),
        "detail": ", ".join(detail_parts) or None,
        "notes": notes,
        "subject": subject_identity(change),
        "change_ids": [str(change.get("change_id"))],
        "evidence": _evidence_of(change),
        "navigation": {"kind": "CHANGE", "target_id": str(change.get("change_id"))},
        # Инженерное содержимое не должно тонуть в тексте штампа и примечаний.
        "engineering": True,
    }


def _merge_duplicates(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Схлопывает одинаковые фразы, сохраняя все идентификаторы изменений.

    Сравнение графа выдаёт одно и то же различие дважды, когда узел опознан по
    двум путям. Для инженера это одна строка; но решение остаётся атомарным по
    каждому изменению, поэтому идентификаторы не теряются.
    """
    merged: dict[tuple, dict[str, Any]] = {}
    order: list[tuple] = []
    for item in items:
        key = (item["text"], item["status"], item.get("detail"))
        if key not in merged:
            merged[key] = {**item, "change_ids": list(item["change_ids"])}
            order.append(key)
            continue
        for change_id in item["change_ids"]:
            if change_id not in merged[key]["change_ids"]:
                merged[key]["change_ids"].append(change_id)
    return [merged[key] for key in order]


def group_key(subject: Optional[str]) -> Optional[str]:
    """Ключ зрительной группы.

    Граф щита называет щит автостоянки «ВРУА», таблица нагрузок — «ВРУ-А».
    Это один объект, и показывать его двумя заголовками значит заставить
    инженера самого догадаться, что номинал и мощность относятся к одному
    щиту. Ключ снимает только разделитель и взаимозаменяемые Щ/Ш — разные
    семейства («ШУ-ХЦ» и «ВРУ-ХЦ») он НЕ сливает.
    """
    if not subject:
        return None
    key = str(subject).upper().replace("Ё", "Е")
    key = re.sub(r"[\s\-_.]", "", key)
    if key.startswith("Щ"):
        key = "Ш" + key[1:]
    return key


def _group_by_subject(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Собирает строки под заголовком объекта.

    Группа только зрительная: она отвечает на «что стало с ХМ1», не создавая
    ни одного нового факта и не объединяя решения инженера.
    """
    groups: dict[Optional[str], list[dict[str, Any]]] = {}
    names: dict[Optional[str], str] = {}
    order: list[Optional[str]] = []
    for item in items:
        subject = item.get("subject")
        key = group_key(subject)
        if key not in groups:
            groups[key] = []
            order.append(key)
        # Заголовок берём у того написания, для которого известна расшифровка,
        # иначе у более полного — «ВРУ-А» понятнее, чем «ВРУА».
        if subject and (
            key not in names
            or (subject in _SUBJECT_HINTS and names[key] not in _SUBJECT_HINTS)
            or (len(subject) > len(names[key]) and names[key] not in _SUBJECT_HINTS)
        ):
            names[key] = subject
        groups[key].append(dict(item))
    # Граф щита снимает у подписи название семейства: «1ГРЩ-ВРУ.ИТП» даёт
    # «ИТП», тогда как таблица нагрузок сохраняет «ВРУ-ИТП». Два заголовка об
    # одном щите заставляют инженера самого догадываться, что номинал и
    # мощность относятся к одному объекту. Свернуть можно только когда
    # укороченному ключу отвечает РОВНО ОДИН полный: два семейства с общим
    # хвостом («ШУ-ИТП» и «ВРУ-ИТП») сливать нечем.
    for short in [key for key in list(order) if key]:
        longer = [
            key
            for key in order
            if key
            and key != short
            and key.endswith(short)
            and key[: -len(short)] in _PANEL_FAMILIES
        ]
        if len(longer) != 1 or short not in groups:
            continue
        target = longer[0]
        groups[target].extend(groups.pop(short))
        order.remove(short)
        names.pop(short, None)

    result = []
    for key in order:
        subject = names.get(key)
        # Порядок внутри группы задаётся секцией и названием свойства, а не
        # порядком обхода: инженер читает «сначала первая секция, потом
        # вторая», а не «как легло в файл».
        ordered = sorted(
            groups[key],
            key=lambda line: (str(line.get("detail") or ""), line["text"]),
        )
        lines = _merge_duplicates(ordered)
        result.append(
            {
                "group_id": _stable_id("prgroup", subject),
                "subject": subject,
                "title": subject_title(subject),
                "items": lines,
                "counts": {
                    "items": len(lines),
                    "review": sum(1 for line in lines if line["status"] == STATUS_REVIEW),
                    "ai_verified": sum(
                        1 for line in lines if line["status"] == STATUS_AI_VERIFIED
                    ),
                },
                "creates_engineering_fact": False,
            }
        )
    result.sort(key=lambda group: (group["subject"] is None, str(group["subject"])))
    return result


_SCHEME_SUBJECT_KINDS = {"repeated_node_group", "reserve_function", "unresolved_correspondence"}


def _is_scheme_level(change: Mapping[str, Any]) -> bool:
    """Изменение уровня схемы, а не отдельного потребителя."""
    subject = _structured(change).get("subject") or {}
    if subject.get("kind") in _SCHEME_SUBJECT_KINDS:
        return True
    identity = subject_identity(change)
    return bool(identity) and (
        identity in _SUBJECT_TITLES or str(identity).startswith(("INPUT#", "SECTION-TIE#"))
    )


#: Вердикт находки, при котором она не утверждает ошибку чертежа, а просит
#: проверить. Такие находки идут в раздел «Что требует проверки инженера»:
#: назвать противоречием доказанным то, что доказано лишь статистикой, значило
#: бы выдать правдоподобие за факт.
VERDICT_REVIEW = "REVIEW"


def _inconsistency_items(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    items = list((payload or {}).get("items") or ())
    result = []
    for item in items:
        review = item.get("verdict") == VERDICT_REVIEW
        result.append(
            {
                "item_id": _stable_id("pritem", item.get("inconsistency_id") or item.get("row_id")),
                "status": STATUS_REVIEW if review else STATUS_INCONSISTENCY,
                # Находка по чертежу, а не текстовое различие штампа: в разделе
                # проверки она обязана стоять выше, иначе тонет под ними.
                "engineering": True,
                "text": str(item.get("summary") or ""),
                "detail": f"лист: {'левый' if item.get('side') == 'LEFT' else 'правый'}",
                "notes": [],
                "subject": item.get("subject"),
                "change_ids": [],
                "evidence": {
                    str(item.get("side") or "RIGHT"): {
                        "bbox": (item.get("evidence") or {}).get("bbox"),
                    }
                },
                "navigation": {
                    "kind": "DOCUMENT_INCONSISTENCY",
                    "target_id": str(
                        item.get("inconsistency_id") or item.get("row_id") or ""
                    ),
                },
            }
        )
    return _merge_duplicates(result)


def _proposal_notes(payload: Mapping[str, Any] | None) -> dict[str, list[str]]:
    """Подсказки ИИ к строкам, чьё сравнение не состоялось: row_id → примечания.

    Тождество доказано, а сравнить значения не удалось — например, режим
    величин распознан не у обеих строк. Заводить ради этого новую строку
    отчёта значит добавить инженеру работы там, где её собирались снять.
    Подсказка приписывается к ЕГО строке: он и так должен её разобрать, но
    теперь знает, с чем именно сравнивать.
    """
    notes: dict[str, list[str]] = {}
    for record in (payload or {}).get("derived_blocked") or ():
        if not isinstance(record, Mapping):
            continue
        summary = str(record.get("summary") or "").strip()
        if not summary:
            continue
        for key in ("left_row_id", "right_row_id"):
            row_id = str(record.get(key) or "")
            if row_id:
                notes.setdefault(row_id, []).append(summary)
    return notes


def _unproven_items(
    table_changes: Mapping[str, Any] | None,
    resolved_row_ids: Iterable[Any] = (),
    proposals: Mapping[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    payload = table_changes or {}
    hints = dict(proposals or {})
    # Строка, для которой пара доказана и проверена, больше не «не смогли
    # сравнить»: оставить её здесь значило бы показать инженеру одну и ту же
    # строку дважды — как сравнённую и как несравнимую.
    resolved = {str(value) for value in resolved_row_ids}
    result: list[dict[str, Any]] = []
    for record in payload.get("blocked") or ():
        if record.get("reason") in _MODE_REVIEW_REASONS:
            continue
        result.append(
            {
                "item_id": _stable_id("pritem", "blocked", record.get("summary")),
                "status": STATUS_UNPROVEN,
                "text": str(record.get("summary") or ""),
                "detail": None,
                "notes": [],
                "subject": record.get("subject") or (record.get("key") or [None])[0],
                "change_ids": [],
                "evidence": {},
                "navigation": {"kind": "NOT_COMPARABLE", "target_id": ""},
            }
        )
    seen_subjects: set[tuple] = set()
    for record in payload.get("unproven") or ():
        if str(record.get("row_id") or "") in resolved:
            continue
        key = (record.get("side"), record.get("subject"), record.get("section_ref"))
        if key in seen_subjects:
            continue
        seen_subjects.add(key)
        result.append(
            {
                "item_id": _stable_id("pritem", "unproven", *(str(part) for part in key)),
                "status": STATUS_UNPROVEN,
                "text": str(record.get("summary") or ""),
                "detail": None,
                "notes": hints.get(str(record.get("row_id") or ""), []),
                "subject": record.get("subject"),
                "change_ids": [],
                "evidence": {},
                "navigation": {"kind": "NOT_COMPARABLE", "target_id": ""},
            }
        )
    return _merge_duplicates(result)


def _mode_review_items(
    table_changes: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Несопоставимые режимные величины — человеку, с числами и причиной."""
    result: list[dict[str, Any]] = []
    for record in (table_changes or {}).get("blocked") or ():
        if record.get("reason") not in _MODE_REVIEW_REASONS:
            continue
        result.append(
            {
                "item_id": _stable_id(
                    "pritem", "mode", record.get("match_id"), record.get("facet_ref")
                ),
                "status": STATUS_REVIEW,
                "text": str(record.get("summary") or ""),
                "detail": None,
                "notes": [],
                "subject": record.get("subject"),
                "change_ids": [],
                "evidence": dict(record.get("evidence") or {}),
                "navigation": {
                    "kind": "NOT_COMPARABLE",
                    "target_id": _mode_target_id(record),
                },
                "engineering": True,
            }
        )
    return _merge_duplicates(result)


def _mode_target_id(record: Mapping[str, Any]) -> str:
    # Local import avoids making the base report depend on the orchestrator at
    # module import time while keeping one canonical target identity.
    from .human_review_orchestrator import mode_target_id

    return mode_target_id(record)


def _plan_item(
    value: Mapping[str, Any],
    *,
    status: str,
    navigation_kind: str,
) -> dict[str, Any]:
    target_id = str(value.get("target_id") or "")
    return {
        "item_id": _stable_id("pritem", target_id),
        "status": status,
        "text": str(value.get("text") or value.get("reason") or ""),
        "detail": None,
        "notes": [],
        "subject": value.get("subject"),
        "change_ids": [],
        "evidence": {},
        "navigation": {"kind": navigation_kind, "target_id": target_id},
        "engineering": navigation_kind == "TEXT_REQUIREMENT_CHANGE",
        "classification": value.get("classification"),
        "subtype": value.get("subtype"),
        "source_region": value.get("source_region"),
        "bounded_absence": value.get("bounded_absence"),
    }


def _plan_group_item(value: Mapping[str, Any]) -> dict[str, Any]:
    modes = value.get("mode_sets") or {}
    subjects = ", ".join(str(item) for item in value.get("affected_subjects") or ())
    return {
        "item_id": _stable_id("pritem", value.get("group_id")),
        "status": STATUS_REVIEW,
        "text": str(value.get("question") or ""),
        "detail": (
            f"Слева: {', '.join(modes.get('LEFT') or ())}; "
            f"справа: {', '.join(modes.get('RIGHT') or ())}. "
            f"Объекты: {subjects}."
        ),
        "notes": [],
        "subject": None,
        "change_ids": [],
        "evidence": {},
        "navigation": {
            "kind": "HUMAN_REVIEW_GROUP",
            "target_id": str(value.get("group_id") or ""),
        },
        "engineering": True,
        "affected_target_ids": list(value.get("affected_target_ids") or ()),
        "allowed_answers": list(value.get("allowed_answers") or ()),
    }


def _ai_identity_items(
    payload: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Изменения, посчитанные по парам строк, тождество которых доказал ИИ.

    Текст строки собирается тем же описателем величин, что и у обычных
    находок таблиц: инженер читает «Расчётный ток: 116,5 → 223,3 А», а не
    «ИИ сопоставил etrow_… с etrow_…».
    """
    result: list[dict[str, Any]] = []
    for record in (payload or {}).get("derived_changes") or ():
        if not isinstance(record, Mapping):
            continue
        title = str(record.get("facet_title") or record.get("facet_ref") or "")
        unit = str(record.get("unit") or "")
        before = format_number(record.get("before_value"))
        after = format_number(record.get("after_value"))
        tail = f" {unit}" if unit else ""
        subject = str(record.get("subject") or "")
        # Род и глагол берутся теми же таблицами, что и у обычных находок:
        # своя формулировка выдавала «расчётный ток изменена».
        gender = _FACET_GENDER.get(
            str(record.get("base_facet_ref") or record.get("facet_ref") or ""), "m"
        )
        verb = _VERB.get(
            (str(record.get("direction") or "ALTERED"), gender),
            _VERB[("ALTERED", gender)],
        )
        detail_parts = []
        if record.get("section_ref"):
            detail_parts.append(f"секция {record['section_ref']}")
        if record.get("input_number"):
            detail_parts.append(f"ввод {record['input_number']}")
        result.append({
            "item_id": _stable_id("pritem", "aiid", record.get("change_id")),
            "status": STATUS_AI_VERIFIED,
            "engineering": True,
            "text": (
                f"{subject}: {title.lower()} {verb} с {before} до {after}{tail}."
                if title
                else f"{subject}: значение изменено с {before} до {after}{tail}."
            ),
            "detail": ", ".join(detail_parts) or None,
            "notes": [str(note) for note in record.get("notes") or () if note],
            "subject": subject,
            "change_ids": [str(record.get("change_id") or "")],
            "evidence": dict(record.get("evidence") or {}),
            "navigation": {
                "kind": "AI_IDENTITY_CHANGE",
                "target_id": str(record.get("change_id") or ""),
            },
        })
    return _merge_duplicates(result)


def _review_items(review_items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Свидетельства, которые конвейер не смог довести до факта.

    Формулировка здесь принципиальна. Конвейер объявил такое свидетельство
    «требующим разбора», а не «добавленным»: на левом листе Markdown нет, его
    текст прочитан из вектор-слоя и имеет право лишь подтверждать совпадение.
    Написать «на правом листе появилось» значило бы выдать нехватку
    распознавания за изменение проекта.
    """
    result = []
    for item in review_items:
        before, after = item.get("before_value"), item.get("after_value")
        if after is not None and before is None:
            text = (
                f"Текст правого листа не сопоставлен с левым: «{after}». "
                "Требуется решение инженера."
            )
        elif before is not None and after is None:
            text = (
                f"Текст левого листа не сопоставлен с правым: «{before}». "
                "Требуется решение инженера."
            )
        else:
            text = (
                f"Текстовое различие: «{before}» → «{after}». "
                "Требуется решение инженера."
            )
        result.append(
            {
                "item_id": _stable_id("pritem", item.get("review_evidence_id")),
                "status": STATUS_REVIEW,
                "text": text,
                "detail": None,
                "notes": [],
                "subject": None,
                "change_ids": [],
                "evidence": {},
                "navigation": {
                    "kind": "REVIEW_EVIDENCE",
                    "target_id": str(item.get("review_evidence_id") or ""),
                },
                "engineering": False,
            }
        )
    return _merge_duplicates(result)


_EVIDENCE_NAVIGATION_KINDS = frozenset(
    {"CHANGE", "REVIEW_EVIDENCE", "AI_IDENTITY_CHANGE"}
)


def _inline_evidence_is_available(item: Mapping[str, Any]) -> bool:
    evidence = item.get("evidence")
    return isinstance(evidence, Mapping) and any(
        isinstance(side, Mapping) and bool(side) for side in evidence.values()
    )


def _apply_evidence_availability(
    sections: Iterable[Mapping[str, Any]],
    availability: Mapping[str, bool] | None,
) -> None:
    """Attach the backend-owned evidence contract to every report row.

    When the production orchestrator supplies the canonical resolver index,
    its answer is authoritative for synthesis targets.  Inline evidence is a
    compatibility fallback for projections (such as AI table identity) that
    do not belong to that resolver yet and for standalone report builders.
    """
    for section in sections:
        collections = [section.get("items") or []]
        collections.extend(
            group.get("items") or []
            for group in section.get("groups") or []
            if isinstance(group, Mapping)
        )
        for items in collections:
            for item in items:
                if not isinstance(item, dict):
                    continue
                navigation = item.get("navigation") or {}
                kind = str(navigation.get("kind") or "")
                target_id = str(navigation.get("target_id") or "")
                navigable = bool(target_id) and kind in _EVIDENCE_NAVIGATION_KINDS
                if (
                    navigable
                    and availability is not None
                    and target_id in availability
                ):
                    item["has_evidence"] = bool(availability[target_id])
                else:
                    item["has_evidence"] = (
                        navigable and _inline_evidence_is_available(item)
                    )


def plural(count: int, one: str, few: str, many: str) -> str:
    """Русское склонение числительного: 1 изменение, 2 изменения, 5 изменений.

    Без него сводка выдаёт «24 позиций» и читается как машинный перевод —
    ровно то впечатление, ради устранения которого отчёт и делался.
    """
    tail_100 = abs(count) % 100
    tail_10 = abs(count) % 10
    if 11 <= tail_100 <= 14:
        return many
    if tail_10 == 1:
        return one
    if 2 <= tail_10 <= 4:
        return few
    return many


def _summary_sentences(counts: Mapping[str, int]) -> list[str]:
    sentences = []
    if counts["automatic"]:
        word = plural(counts["automatic"], "изменение", "изменения", "изменений")
        proved = plural(
            counts["automatic"],
            "подтверждённое доказательствами",
            "подтверждённых доказательствами",
            "подтверждённых доказательствами",
        )
        sentences.append(
            f"Система нашла {counts['automatic']} {word}, {proved} с обоих листов."
        )
    else:
        sentences.append("Доказанных изменений между редакциями не найдено.")
    if counts["ai_verified"]:
        word = plural(counts["ai_verified"], "изменение", "изменения", "изменений")
        checked = plural(
            counts["ai_verified"], "уточнено", "уточнены", "уточнены",
        )
        sentences.append(
            f"Ещё {counts['ai_verified']} {word} {checked} ИИ и проверено "
            "правилами: сопоставление подсказано, значения сверены по самим "
            "строкам листов."
        )
    if counts["review"]:
        verb = plural(counts["review"], "требует", "требуют", "требуют")
        sentences.append(
            f"Ещё {counts['review']} {verb} проверки инженера: доказательство "
            "есть, но оно не полное."
        )
    if counts["inconsistency"]:
        word = plural(
            counts["inconsistency"],
            "внутреннее противоречие",
            "внутренних противоречия",
            "внутренних противоречий",
        )
        sentences.append(
            f"Отдельно отмечено {counts['inconsistency']} {word} самих листов — "
            "это ошибки чертежа, а не расхождения редакций."
        )
    if counts["unproven"]:
        word = plural(counts["unproven"], "позицию", "позиции", "позиций")
        sentences.append(
            f"{counts['unproven']} {word} система сравнить не смогла и об этом "
            "сообщает прямо, а не умалчивает."
        )
    if counts.get("text_requirements"):
        sentences.append(
            f"Отдельно показано {counts['text_requirements']} новых технических "
            "требований с ограниченно доказанным отсутствием в другой редакции."
        )
    if counts.get("metadata"):
        sentences.append(
            f"Изменения оформления и штампа ({counts['metadata']}) вынесены из "
            "инженерной очереди в отдельный сворачиваемый раздел."
        )
    return sentences


def build_preliminary_report(
    *,
    pair_id: str,
    synthesis: Mapping[str, Any] | None,
    document_inconsistencies: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
    ai_table_identity: Mapping[str, Any] | None = None,
    human_review_plan: Mapping[str, Any] | None = None,
    evidence_availability: Mapping[str, bool] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Собирает предварительный отчёт из уже готовых находок.

    Отчёт ничего не вычисляет заново и не обращается к модели: он только
    переводит структурированные находки на язык, на котором о них говорит
    инженер, и раскладывает по разделам.
    """
    changes = list((synthesis or {}).get("changes") or ())
    review_evidence = list((synthesis or {}).get("review_items") or ())

    scheme_items: list[dict[str, Any]] = []
    equipment_items: list[dict[str, Any]] = []
    for change in changes:
        item = _change_item(change)
        (scheme_items if _is_scheme_level(change) else equipment_items).append(item)

    scheme_lines = _merge_duplicates(scheme_items)
    equipment_groups = _group_by_subject(equipment_items)
    inconsistency_lines = _inconsistency_items(document_inconsistencies)
    # Находки, доказанные не до конца, живут в разделе проверки, а не в разделе
    # противоречий: разделы отличаются не источником находки, а тем, что о ней
    # утверждается.
    inconsistency_review_lines = [
        line for line in inconsistency_lines if line["status"] == STATUS_REVIEW
    ]
    inconsistency_lines = [
        line for line in inconsistency_lines if line["status"] != STATUS_REVIEW
    ]
    ai_lines = _ai_identity_items(ai_table_identity)
    unproven_lines = _unproven_items(
        electrical_table_changes,
        (ai_table_identity or {}).get("resolved_row_ids") or (),
        _proposal_notes(ai_table_identity),
    )
    review_lines = (
        _review_items(review_evidence)
        + inconsistency_review_lines
        + _mode_review_items(electrical_table_changes)
    )
    review_lines += [
        line
        for group in equipment_groups
        for line in group["items"]
        if line["status"] == STATUS_REVIEW
    ]
    review_lines += [line for line in scheme_lines if line["status"] == STATUS_REVIEW]
    # Инженерные строки идут первыми. Текстовые различия штампа и примечаний
    # не скрываются — их порождает нехватка распознавания левого листа, и
    # спрятать их значило бы соврать о полноте, — но и хоронить под ними шесть
    # находок по оборудованию нельзя.
    review_lines = _merge_duplicates(review_lines)
    review_lines.sort(key=lambda line: (not line.get("engineering", False), line["text"]))
    metadata_lines: list[dict[str, Any]] = []
    requirement_lines: list[dict[str, Any]] = []
    if human_review_plan:
        standalone_targets = {
            str(target_id)
            for question in human_review_plan.get("standalone_questions") or ()
            for target_id in question.get("affected_target_ids") or ()
        }
        review_lines = [
            line for line in review_lines
            if str((line.get("navigation") or {}).get("target_id") or "")
            in standalone_targets
        ]
        review_lines.extend(
            _plan_group_item(group)
            for group in human_review_plan.get("groups") or ()
        )
        existing_review_targets = {
            str((line.get("navigation") or {}).get("target_id") or "")
            for line in review_lines
        }
        review_lines.extend(
            {
                "item_id": _stable_id("pritem", question.get("question_id")),
                "status": STATUS_REVIEW,
                "text": str(question.get("question") or ""),
                "detail": None,
                "notes": [],
                "subject": None,
                "change_ids": [],
                "evidence": {},
                "navigation": {
                    "kind": "HUMAN_REVIEW_QUESTION",
                    "target_id": str((question.get("affected_target_ids") or [""])[0]),
                },
                "engineering": True,
                "allowed_answers": list(question.get("allowed_answers") or ()),
            }
            for question in human_review_plan.get("standalone_questions") or ()
            if str((question.get("affected_target_ids") or [""])[0])
            not in existing_review_targets
        )
        metadata_lines = [
            _plan_item(
                value,
                status=STATUS_AUTOMATIC,
                navigation_kind="DOCUMENT_METADATA_CHANGE",
            )
            for value in human_review_plan.get("metadata_changes") or ()
        ]
        requirement_lines = [
            _plan_item(
                value,
                status=STATUS_AUTOMATIC,
                navigation_kind="TEXT_REQUIREMENT_CHANGE",
            )
            for value in human_review_plan.get("text_requirement_changes") or ()
        ]
        unproven_lines = [
            _plan_item(
                value,
                status=STATUS_UNPROVEN,
                navigation_kind="MISSING_EVIDENCE",
            )
            for value in human_review_plan.get("missing_evidence") or ()
        ]

    published = (
        scheme_lines
        + [item for group in equipment_groups for item in group["items"]]
        + ai_lines
    )
    counts = {
        "automatic": sum(
            1 for line in published if line["status"] == STATUS_AUTOMATIC
        ),
        "ai_verified": sum(
            1 for line in published if line["status"] == STATUS_AI_VERIFIED
        ),
        "review": len(review_lines),
        "inconsistency": len(inconsistency_lines),
        "unproven": len(unproven_lines),
        "changes": len(changes),
        "equipment_groups": len(equipment_groups),
        "text_requirements": len(requirement_lines),
        "metadata": len(metadata_lines),
    }

    sections = [
        {
            "section_id": SECTION_SCHEME,
            "title": "Основные изменения схемы",
            "items": scheme_lines,
        },
        {
            "section_id": SECTION_EQUIPMENT,
            "title": "Изменения по оборудованию и фидерам",
            "groups": equipment_groups,
        },
        {
            "section_id": SECTION_AI_VERIFIED,
            "title": "Уточнено ИИ и проверено правилами",
            "items": ai_lines,
        },
        {
            "section_id": SECTION_INCONSISTENCIES,
            "title": "Внутренние противоречия документа",
            "items": inconsistency_lines,
        },
        {
            "section_id": SECTION_TEXT_REQUIREMENTS,
            "title": "Изменения технических требований",
            "items": requirement_lines,
        },
        {
            "section_id": SECTION_REVIEW,
            "title": "Что требует проверки инженера",
            "items": review_lines,
        },
        {
            "section_id": SECTION_UNPROVEN,
            "title": "Не удалось сравнить",
            "items": unproven_lines,
        },
        {
            "section_id": SECTION_METADATA,
            "title": "Изменения оформления и штампа",
            "items": metadata_lines,
            "collapsed": True,
        },
    ]
    _apply_evidence_availability(sections, evidence_availability)

    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "summary": {
            "title": "Предварительный отчёт анализа",
            "sentences": _summary_sentences(counts),
            "counts": counts,
        },
        "sections": sections,
        "statuses": list(STATUSES),
        "constraints": {
            # Предварительный отчёт НЕ итоговый: он показывает всё найденное,
            # тогда как итоговый собирается только из подтверждённого инженером.
            "is_final_report": False,
            "requires_engineer_review": True,
            "read_only": True,
            "uses_model": False,
        },
        "provenance": {
            "producer": PRODUCER,
            "sources": [
                name
                for name, payload in (
                    ("unified_synthesis", synthesis),
                    ("document_inconsistencies", document_inconsistencies),
                    ("electrical_table_changes", electrical_table_changes),
                    ("ai_table_identity", ai_table_identity),
                    ("human_review_plan", human_review_plan),
                )
                if payload
            ],
        },
    }


def render_markdown(report: Mapping[str, Any]) -> str:
    """Отчёт в виде текста — для чтения в консоли и в приёмке."""
    lines: list[str] = []
    summary = report.get("summary") or {}
    lines.append(f"# {summary.get('title') or 'Предварительный отчёт анализа'}")
    lines.append("")
    for sentence in summary.get("sentences") or ():
        lines.append(sentence)
    lines.append("")
    for section in report.get("sections") or ():
        items = list(section.get("items") or ())
        groups = list(section.get("groups") or ())
        if not items and not groups:
            continue
        lines.append(f"## {section['title']}")
        lines.append("")
        for item in items:
            lines.append(f"- [{item['status']}] {item['text']}")
            if item.get("detail"):
                lines.append(f"      {item['detail']}")
            for note in item.get("notes") or ():
                lines.append(f"      Оговорка: {note}")
        for group in groups:
            lines.append(f"### {group['title']}")
            for item in group["items"]:
                lines.append(f"- [{item['status']}] {item['text']}")
                if item.get("detail"):
                    lines.append(f"      {item['detail']}")
                for note in item.get("notes") or ():
                    lines.append(f"      Оговорка: {note}")
            lines.append("")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


__all__ = [
    "KIND",
    "PRODUCER",
    "SCHEMA_VERSION",
    "STATUSES",
    "STATUS_AI_VERIFIED",
    "STATUS_AUTOMATIC",
    "STATUS_INCONSISTENCY",
    "STATUS_REVIEW",
    "STATUS_UNPROVEN",
    "build_preliminary_report",
    "change_is_review",
    "describe_change",
    "format_number",
    "group_key",
    "plural",
    "render_markdown",
    "subject_identity",
    "subject_name",
    "subject_title",
]
