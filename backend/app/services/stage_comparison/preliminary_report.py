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
from typing import Any, Iterable, Mapping, Optional, Sequence

KIND = "stage_comparison_preliminary_report"
SCHEMA_VERSION = "preliminary-comparison-report.v1"
PRODUCER = "preliminary-comparison-report-v1"

#: Ровно четыре статуса. Внутренние коды (MATERIAL_CHANGE, REVIEW_REQUIRED,
#: UNKNOWN_DIMENSION) в отчёт не попадают: они описывают устройство конвейера,
#: а не состояние проекта.
STATUS_AUTOMATIC = "Найдено автоматически"
STATUS_REVIEW = "Требуется проверка инженера"
STATUS_INCONSISTENCY = "Внутреннее противоречие документа"
STATUS_UNPROVEN = "Недостаточно доказательств"
STATUSES = (STATUS_AUTOMATIC, STATUS_REVIEW, STATUS_INCONSISTENCY, STATUS_UNPROVEN)

SECTION_SUMMARY = "summary"
SECTION_SCHEME = "scheme"
SECTION_EQUIPMENT = "equipment"
SECTION_PARAMETERS = "parameters"
SECTION_INCONSISTENCIES = "inconsistencies"
SECTION_REVIEW = "review"
SECTION_UNPROVEN = "unproven"

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
        gender = _FACET_GENDER.get(str(change.get("facet_ref") or ""), "m")
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


def _change_item(change: Mapping[str, Any]) -> dict[str, Any]:
    provenance = _atom_provenance(change)
    notes = [str(note) for note in (provenance.get("notes") or []) if note]
    status = STATUS_REVIEW if _is_review(change) else STATUS_AUTOMATIC
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


def _group_by_subject(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Собирает строки под заголовком объекта.

    Группа только зрительная: она отвечает на «что стало с ХМ1», не создавая
    ни одного нового факта и не объединяя решения инженера.
    """
    groups: dict[Optional[str], list[dict[str, Any]]] = {}
    order: list[Optional[str]] = []
    for item in items:
        subject = item.get("subject")
        if subject not in groups:
            groups[subject] = []
            order.append(subject)
        groups[subject].append(dict(item))
    result = []
    for subject in order:
        # Порядок внутри группы задаётся секцией и названием свойства, а не
        # порядком обхода: инженер читает «сначала первая секция, потом
        # вторая», а не «как легло в файл».
        ordered = sorted(
            groups[subject],
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


def _inconsistency_items(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    items = list((payload or {}).get("items") or ())
    result = []
    for item in items:
        result.append(
            {
                "item_id": _stable_id("pritem", item.get("inconsistency_id") or item.get("row_id")),
                "status": STATUS_INCONSISTENCY,
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


def _unproven_items(table_changes: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    payload = table_changes or {}
    result: list[dict[str, Any]] = []
    for record in payload.get("blocked") or ():
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
                "notes": [],
                "subject": record.get("subject"),
                "change_ids": [],
                "evidence": {},
                "navigation": {"kind": "NOT_COMPARABLE", "target_id": ""},
            }
        )
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


def _summary_sentences(counts: Mapping[str, int]) -> list[str]:
    sentences = []
    if counts["automatic"]:
        sentences.append(
            f"Система нашла {counts['automatic']} изменен"
            f"{'ие' if counts['automatic'] % 10 == 1 and counts['automatic'] % 100 != 11 else 'ий'}, "
            "подтверждённых доказательствами с обоих листов."
        )
    else:
        sentences.append("Доказанных изменений между редакциями не найдено.")
    if counts["review"]:
        sentences.append(
            f"Ещё {counts['review']} требует проверки инженера: доказательство "
            "есть, но оно не полное."
        )
    if counts["inconsistency"]:
        sentences.append(
            f"Отдельно отмечено {counts['inconsistency']} внутренних противоречий "
            "самих листов — это ошибки чертежа, а не расхождения редакций."
        )
    if counts["unproven"]:
        sentences.append(
            f"{counts['unproven']} позиц"
            f"{'ия' if counts['unproven'] % 10 == 1 and counts['unproven'] % 100 != 11 else 'ий'} "
            "система сравнить не смогла и об этом сообщает прямо, а не умалчивает."
        )
    return sentences


def build_preliminary_report(
    *,
    pair_id: str,
    synthesis: Mapping[str, Any] | None,
    document_inconsistencies: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
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
    unproven_lines = _unproven_items(electrical_table_changes)
    review_lines = _review_items(review_evidence)
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

    counts = {
        "automatic": sum(
            1
            for line in scheme_lines + [i for g in equipment_groups for i in g["items"]]
            if line["status"] == STATUS_AUTOMATIC
        ),
        "review": len(review_lines),
        "inconsistency": len(inconsistency_lines),
        "unproven": len(unproven_lines),
        "changes": len(changes),
        "equipment_groups": len(equipment_groups),
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
            "section_id": SECTION_INCONSISTENCIES,
            "title": "Внутренние противоречия документа",
            "items": inconsistency_lines,
        },
        {
            "section_id": SECTION_REVIEW,
            "title": "Что требует проверки инженера",
            "items": review_lines,
        },
        {
            "section_id": SECTION_UNPROVEN,
            "title": "Что система не смогла доказать",
            "items": unproven_lines,
        },
    ]

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
    "STATUS_AUTOMATIC",
    "STATUS_INCONSISTENCY",
    "STATUS_REVIEW",
    "STATUS_UNPROVEN",
    "build_preliminary_report",
    "describe_change",
    "format_number",
    "render_markdown",
    "subject_identity",
    "subject_name",
    "subject_title",
]
