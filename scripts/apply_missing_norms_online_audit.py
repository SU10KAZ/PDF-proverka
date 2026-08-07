#!/usr/bin/env python3
"""Применить подтвержденные решения интернет-аудита missing norms.

Рабочий реестр остаётся JSON-массивом строк. Подробности проверки,
источники и причины решений записываются только в отдельный отчёт.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from norms.external_provider import resolve_norm_status

VAULT = Path("backend/app/data/missing_norms_vault.json")
FIRST_AUDIT = Path("backend/app/data/missing_norms_online_audit.json")
CANDIDATES = Path("backend/app/data/missing_norms_online_candidates.json")
REPORT = Path("backend/app/data/missing_norms_online_decisions.json")
BACKUP = Path("backend/app/data/missing_norms_vault.before_online_audit_20260724.json")
ACTIVE = {"действует", "действует только в рф", "принят"}


def _body(value: str) -> str:
    return re.sub(
        r"^\s*(?:ГОСТ(?:\s+Р)?(?:\s+(?:МЭК|IEC))?|СП)\s+",
        "",
        value,
        flags=re.I,
    )


def _key(value: str) -> str:
    return re.sub(r"[._\s-]", "", value).upper()


def _strip_year(value: str) -> str:
    return re.sub(r"[-.](?:(?:19|20)\d{2}|\d{2})$", "", value)


def _year(value: str) -> int:
    match = re.search(r"[-.]((?:19|20)\d{2}|\d{2})$", value)
    if not match:
        return -1
    year = int(match.group(1))
    return year + 1900 if year < 100 else year


def _best_active(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    active = [item for item in items if item.get("status") in ACTIVE]
    if not active:
        return None
    return max(
        active,
        key=lambda item: (
            item.get("status") in {"действует", "действует только в рф"},
            _year(_body(item["designation"])),
        ),
    )


def _catalog_resolution(
    original: str,
    item: dict[str, Any],
) -> tuple[str, dict[str, Any] | None, str]:
    """Вернуть (решение, карточка, причина) для неточного ГОСТ/СП."""
    official = item.get("official_candidates") or []
    input_body = _body(original)

    exact = [
        row
        for row in official
        if _key(_body(row["designation"])) == _key(input_body)
    ]
    if exact:
        selected = _best_active(exact)
        if selected:
            return "rewrite", selected, "Исправлен префикс или разделитель."
        return (
            "remove",
            exact[0],
            "После исправления обозначения документ оказался недействующим.",
        )

    input_keys = {_key(input_body), _key(_strip_year(input_body))}
    series = [
        row
        for row in official
        if _key(_strip_year(_body(row["designation"]))) in input_keys
    ]
    if series:
        selected = _best_active(series)
        if selected:
            return (
                "rewrite",
                selected,
                "Исправлен отсутствующий или ошибочный год при том же номере.",
            )
        return (
            "remove",
            series[0],
            "Для этого номера найдены только недействующие редакции.",
        )

    return "unresolved", None, "Точного безопасного исправления не найдено."


MANUAL_CATALOG: dict[str, dict[str, str]] = {
    "ГОСТ 70.13330.2012": {
        "designation": "СП 70.13330.2012",
        "status": "действует",
        "url": "https://protect.gost.ru/sp?month=0&year=0&search=" + quote_plus("СП 70.13330.2012"),
        "reason": "Ошибочно указан тип ГОСТ вместо СП.",
    },
    "ГОСТ IEC 61386-1-2014": {
        "designation": "ГОСТ Р МЭК 61386.1-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost?month=0&year=0&search=" + quote_plus("ГОСТ Р МЭК 61386.1-2014"),
        "reason": "Исправлены национальный префикс и номер части.",
    },
    "ГОСТ IEC 61386-22-2014": {
        "designation": "ГОСТ Р МЭК 61386.22-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/92dd488e-f5c2-43a5-816c-4da66a7602b2",
        "reason": "Исправлены национальный префикс и номер части.",
    },
    "ГОСТ Р МЭК 62305-3-2010": {
        "designation": "ГОСТ Р 59789-2021",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/1d21baf2-ecc8-4434-81c0-fff014792783",
        "reason": "Российское обозначение стандарта МЭК 62305-3:2010 — ГОСТ Р 59789-2021.",
    },
    "СП 131.1330.2020": {
        "designation": "СП 131.13330.2020",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 15.1330.2012": {
        "designation": "СП 15.13330.2012",
        "status": "заменен",
        "reason": "Исправлен шифр; исправленная редакция заменена.",
    },
    "СП 15.1330.2020": {
        "designation": "СП 15.13330.2020",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 26.1325800.2016": {
        "designation": "СП 256.1325800.2016",
        "status": "действует",
        "reason": "В номере СП была пропущена цифра 5.",
    },
    "СП 28.1330.2017": {
        "designation": "СП 28.13330.2017",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 48.1330.2011": {
        "designation": "СП 48.13330.2011",
        "status": "заменен",
        "reason": "Исправлен шифр; исправленная редакция заменена.",
    },
    "СП 48.1330.2019": {
        "designation": "СП 48.13330.2019",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 484.131500.2020": {
        "designation": "СП 484.1311500.2020",
        "status": "действует",
        "reason": "В шифре 1311500 была пропущена цифра 1.",
    },
    "СП 485.131500.2020": {
        "designation": "СП 485.1311500.2020",
        "status": "действует",
        "reason": "В шифре 1311500 была пропущена цифра 1.",
    },
    "СП 486.131500.2020": {
        "designation": "СП 486.1311500.2020",
        "status": "действует",
        "reason": "В шифре 1311500 была пропущена цифра 1.",
    },
    "СП 50.1330.2024": {
        "designation": "СП 50.13330.2024",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 506.131500.2021": {
        "designation": "СП 506.1311500.2021",
        "status": "отменен",
        "reason": "Исправлен шифр; исправленный документ отменён.",
    },
    "СП 518.131500.2022": {
        "designation": "СП 518.1311500.2022",
        "status": "действует",
        "reason": "В шифре 1311500 была пропущена цифра 1.",
    },
    "СП 52.1330.2016": {
        "designation": "СП 52.13330.2016",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 60.1330.2020": {
        "designation": "СП 60.13330.2020",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
    "СП 70.1330.2012": {
        "designation": "СП 70.13330.2012",
        "status": "действует",
        "reason": "В шифре 13330 была пропущена цифра 3.",
    },
}


EXTERNAL_REWRITES: dict[str, dict[str, str]] = {
    "ПП РФ № 2130": {
        "designation": "Постановление Правительства РФ от 30.11.2021 № 2130",
        "status": "действует",
        "url": "https://government.ru/docs/all/137829/",
    },
    "ПП РФ № 354": {
        "designation": "Постановление Правительства РФ от 06.05.2011 № 354",
        "status": "действует",
        "url": "https://government.ru/docs/all/77428/",
    },
    "ПП РФ № 87": {
        "designation": "Постановление Правительства РФ от 16.02.2008 № 87",
        "status": "действует",
        "url": "https://government.ru/docs/all/63014/",
    },
    "ПП РФ №1034": {
        "designation": "Постановление Правительства РФ от 18.11.2013 № 1034",
        "status": "действует",
        "url": "https://www.consultant.ru/document/cons_doc_LAW_154646/",
    },
    "ПП РФ №145": {
        "designation": "Постановление Правительства РФ от 05.03.2007 № 145",
        "status": "действует",
        "url": "https://government.ru/docs/all/59094/",
    },
    "ПП РФ №1479": {
        "designation": "Постановление Правительства РФ от 16.09.2020 № 1479",
        "status": "действует",
        "url": "https://government.ru/docs/all/130094/",
    },
    "ПП РФ №442": {
        "designation": "Постановление Правительства РФ от 04.05.2012 № 442",
        "status": "действует",
        "url": "https://government.ru/docs/all/151843/",
    },
    "ПП РФ №776": {
        "designation": "Постановление Правительства РФ от 04.09.2013 № 776",
        "status": "действует",
        "url": "https://government.ru/docs/all/108939/?page=5",
    },
    "ПУЭ-7": {
        "designation": "ПУЭ-7",
        "status": "действует добровольно",
        "url": "https://www.consultant.ru/document/cons_doc_LAW_98464/",
    },
    "СО 2507-1-2015": {
        "designation": "ГОСТ Р ИСО 2507-1-2015",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/6b35c47f-637f-4c1c-9f65-44641f3c8766",
    },
    "СО 3506-1": {
        "designation": "ГОСТ ISO 3506-1-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/540247a5-abd0-4030-8a34-f248684f5aab",
    },
    "СО 4032": {
        "designation": "ГОСТ ISO 4032-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/80aa51e0-84b3-4740-8421-722e560077ad",
    },
    "СО 7040-2013": {
        "designation": "ГОСТ ISO 7040-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/52125cab-9123-4e9a-86d7-fd90a09c19b1",
    },
    "СО 898-1-2014": {
        "designation": "ГОСТ ISO 898-1-2014",
        "status": "действует",
        "url": "https://protect.gost.ru/gost/details/422d8f11-ac2e-413a-a152-705dc4061385",
    },
}


EXTERNAL_REMOVALS: dict[str, dict[str, str]] = {
    "ПП РФ № 1521": {
        "reason": "Строительное постановление № 1521 от 26.12.2014 утратило силу; номер без даты также неоднозначен.",
        "url": "https://government.ru/docs/all/128760/",
    },
    "ПП РФ № 956": {
        "reason": "Номер без даты неоднозначен: официально существуют разные постановления № 956, контекст не позволяет выбрать одно.",
        "url": "https://government.ru/docs/all/",
    },
    "ПП РФ №390": {
        "reason": "Постановление от 25.04.2012 № 390 утратило силу и заменено постановлением № 1479.",
        "url": "https://government.ru/docs/all/130094/",
    },
    "ПП РФ №985": {
        "reason": "Постановление от 04.07.2020 № 985 утратило силу 01.09.2021.",
        "url": "https://government.ru/docs/all/128760/",
    },
    "ВСН 60-89": {
        "reason": "Старая норма заменена актуальным СП 134.13330.2022.",
        "url": "https://protect.gost.ru/sp/details/b8acbe38-84cc-4813-8b47-5807ef276d6c",
    },
    "СНиП 12-01-2004": {
        "reason": "Используется актуализированная редакция СП 48.13330.2019.",
        "url": "https://protect.gost.ru/sp/details/278f4ffa-b29b-4733-b8ac-d9a5ff3775cc",
    },
    "СНиП 12.03-2001": {
        "reason": "Обозначение записано неверно (12.03 вместо 12-03) и не является текущим СП.",
        "url": "https://minstroyrf.gov.ru/docs/",
    },
    "СНиП 2.03.11-85": {
        "reason": "Используется актуализированная редакция СП 28.13330.2017.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=26&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 2.07.01-89": {
        "reason": "Используется актуализированная редакция СП 42.13330.2016.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=13&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 21-02-99": {
        "reason": "Используется актуализированный свод правил по стоянкам автомобилей, а не старое обозначение СНиП.",
        "url": "https://protect.gost.ru/sp?month=0&year=0&search=" + quote_plus("СП 113.13330.2023"),
    },
    "СНиП 23-02-2003": {
        "reason": "Используется актуализированная редакция СП 50.13330.2012.",
        "url": "https://minstroyrf.gov.ru/docs/141540/",
    },
    "СНиП 23-05-95": {
        "reason": "Используется актуализированная редакция СП 52.13330.2016.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=26&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 3.01.04-87": {
        "reason": "Используется актуализированная редакция СП 68.13330.2017.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=37&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 3.02.01-87": {
        "reason": "Используется актуализированная редакция СП 45.13330.2017.",
        "url": "https://minstroyrf.gov.ru/press/novye-pravila-proektirovaniya/",
    },
    "СНиП 3.04.01": {
        "reason": "Неполное обозначение без года; однозначной действующей нормы с таким шифром нет.",
        "url": "https://minstroyrf.gov.ru/docs/",
    },
    "СНиП 3.04.01-87": {
        "reason": "Используется актуализированная редакция СП 71.13330.2017.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=8",
    },
    "СНиП 3.05.04-85": {
        "reason": "Используется актуализированная редакция СП 129.13330.2019.",
        "url": "https://protect.gost.ru/sp?month=0&year=0&search=" + quote_plus("СП 129.13330.2019"),
    },
    "СНиП 3.05.05-84": {
        "reason": "Используется актуализированная редакция СП 75.13330.2011.",
        "url": "https://protect.gost.ru/sp?month=0&year=0&search=" + quote_plus("СП 75.13330.2011"),
    },
    "СНиП 3.05.06-85": {
        "reason": "Используется актуализированная редакция СП 76.13330.2016.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=41&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 31-06-2009": {
        "reason": "Используется актуализированная редакция СП 118.13330.2022.",
        "url": "https://minstroyrf.gov.ru/docs/?PAGEN_1=13&active%5B0%5D=65&d%5B0%5D=169",
    },
    "СНиП 35-01-2001": {
        "reason": "Используется актуализированная редакция СП 59.13330.2020.",
        "url": "https://protect.gost.ru/sp?month=0&year=0&search=" + quote_plus("СП 59.13330.2020"),
    },
    "СО 01": {"reason": "Обрезанное и неоднозначное обозначение; официальная норма не идентифицирована."},
    "со 125": {"reason": "Обрезанное и неоднозначное обозначение; официальная норма не идентифицирована."},
    "СО 16890": {"reason": "Обрезанное обозначение ISO; российский ГОСТ с таким обозначением не подтверждён."},
    "СО 16890-1-2016": {"reason": "Обозначение ISO ошибочно принято за российскую норму; ГОСТ не подтверждён."},
    "со 2-": {"reason": "Обрезанное и неоднозначное обозначение."},
    "СО 2507": {"reason": "Неполный номер серии ISO 2507 без части; существует несколько документов."},
    "СО 2531-2012": {
        "reason": "Исправленная форма ГОСТ ISO 2531-2012 заменена редакцией 2022 года.",
        "url": "https://protect.gost.ru/gost/details/401d2f34-26c1-490c-a428-a03a267ec86a",
    },
    "СО 2560": {"reason": "Неполное обозначение; найденная российская редакция ГОСТ Р ИСО 2560-2009 заменена."},
}


FORCE_AMBIGUOUS_REMOVALS = {
    "ГОСТ 15589-2014",
    "ГОСТ 17074-91",
    "ГОСТ 2.312-2024",
    "ГОСТ 2.755-2014",
    "ГОСТ 21.601-2021",
    "ГОСТ 25809-2020",
    "ГОСТ 31569-2024",
    "ГОСТ 32396-2020",
    "ГОСТ 3282-75",
    "ГОСТ 34600-2017",
    "ГОСТ 380-2015",
    "ГОСТ 5945-70",
    "ГОСТ 6485-76",
    "ГОСТ 6727-2018",
    "ГОСТ Р 21.101-2013",
    "ГОСТ Р 21.201-2020",
    "ГОСТ Р 21.301-2018",
    "ГОСТ Р 53229-2013",
    "СП 132.13330.2020",
    "СП 132.13330.2022",
    "СП 15.13330.2019",
    "СП 3.13130.2020",
    "СП 49.13330.2012",
    "СП 58.13330.2020",
    "СП 66.13330.2020",
    "СП 73.13330.2022",
}


def _source_for_designation(
    designation: str,
    candidates: dict[str, dict[str, Any]],
) -> str | None:
    for item in candidates.values():
        for row in item.get("official_candidates") or []:
            if row.get("designation") == designation:
                return row.get("url")
    return None


def _decision_for(
    original: str,
    first: dict[str, Any],
    candidate_items: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if original in EXTERNAL_REWRITES:
        data = EXTERNAL_REWRITES[original]
        return {
            "proposed": data["designation"],
            "official_status": data["status"],
            "source_url": data.get("url"),
            "reason": "Уточнено полное официальное обозначение.",
        }
    if original in EXTERNAL_REMOVALS:
        data = EXTERNAL_REMOVALS[original]
        return {
            "proposed": None,
            "official_status": "invalid_or_non_current",
            "source_url": data.get("url") or (
                "https://protect.gost.ru/gost?month=0&year=0&search="
                + quote_plus(original)
            ),
            "reason": data["reason"],
        }

    if original in FORCE_AMBIGUOUS_REMOVALS:
        return {
            "proposed": None,
            "official_status": "not_found_or_ambiguous",
            "source_url": first.get("query_url"),
            "reason": "Найдено несколько правдоподобных исправлений; без контекста выбор был бы догадкой.",
        }

    verdict = first.get("verdict")
    if verdict == "verified_active_exact":
        selected = first.get("selected") or {}
        return {
            "proposed": original,
            "official_status": selected.get("status"),
            "source_url": selected.get("url"),
            "reason": "Точное обозначение и действующий статус подтверждены Росстандартом.",
        }
    if verdict == "verified_non_active_exact":
        selected = first.get("selected") or {}
        return {
            "proposed": None,
            "official_status": selected.get("status"),
            "source_url": selected.get("url"),
            "reason": "Точная официальная карточка подтверждает недействующий статус.",
        }

    if original in MANUAL_CATALOG:
        data = MANUAL_CATALOG[original]
        source = data.get("url") or _source_for_designation(
            data["designation"], candidate_items
        )
        return {
            "proposed": data["designation"] if data["status"] in ACTIVE else None,
            "corrected_designation": data["designation"],
            "official_status": data["status"],
            "source_url": source,
            "reason": data["reason"],
        }

    item = candidate_items.get(original)
    if item:
        action, selected, reason = _catalog_resolution(original, item)
        if action == "rewrite" and selected:
            return {
                "proposed": selected["designation"],
                "official_status": selected.get("status"),
                "source_url": selected.get("url"),
                "reason": reason,
            }
        if action == "remove" and selected:
            return {
                "proposed": None,
                "corrected_designation": selected.get("designation"),
                "official_status": selected.get("status"),
                "source_url": selected.get("url"),
                "reason": reason,
            }
        top = (item.get("official_candidates") or [])[:5]
        return {
            "proposed": None,
            "official_status": "not_found_or_ambiguous",
            "source_url": first.get("query_url"),
            "reason": "Официальное точное совпадение отсутствует, а варианты неоднозначны.",
            "alternatives": [
                {
                    "designation": row.get("designation"),
                    "status": row.get("status"),
                    "url": row.get("url"),
                }
                for row in top
            ],
        }

    return {
        "proposed": None,
        "official_status": "not_found_or_ambiguous",
        "source_url": first.get("query_url"),
        "reason": "Документ не удалось однозначно подтвердить по официальному источнику.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    original_docs = json.loads(VAULT.read_text(encoding="utf-8"))
    first_payload = json.loads(FIRST_AUDIT.read_text(encoding="utf-8"))
    candidate_payload = json.loads(CANDIDATES.read_text(encoding="utf-8"))
    first_items = {item["input"]: item for item in first_payload["items"]}
    candidate_items = {item["input"]: item for item in candidate_payload["items"]}

    report_items: list[dict[str, Any]] = []
    retained: dict[str, str] = {}
    for original in original_docs:
        base = _decision_for(original, first_items[original], candidate_items)
        proposed = base.pop("proposed")
        local = None
        if proposed:
            local = resolve_norm_status(proposed)
            if local.get("found"):
                outcome = "removed_after_correction_already_in_base"
            else:
                key = re.sub(r"\s+", "", proposed).replace("_", ".").casefold()
                outcome = "kept_unchanged" if proposed == original else "rewritten_and_kept"
                if key in retained:
                    outcome = "removed_duplicate_after_correction"
                else:
                    retained[key] = proposed
        else:
            status = str(base.get("official_status") or "")
            outcome = (
                "removed_non_active"
                if status in {"заменен", "отменен", "срок действия истек", "утратил силу в рф", "invalid_or_non_current"}
                else "removed_invalid_or_ambiguous"
            )

        report_items.append(
            {
                "original": original,
                "outcome": outcome,
                "final_designation": proposed if outcome in {"kept_unchanged", "rewritten_and_kept"} else None,
                **base,
                "local_resolution": {
                    "found": local.get("found"),
                    "matched_code": local.get("matched_code"),
                    "status": local.get("status"),
                    "resolution_reason": local.get("resolution_reason"),
                } if local else None,
            }
        )

    final_docs = sorted(retained.values(), key=str.casefold)
    summary = Counter(item["outcome"] for item in report_items)
    payload = {
        "audit_date": "2026-07-24",
        "initial_count": len(original_docs),
        "final_count": len(final_docs),
        "summary": dict(sorted(summary.items())),
        "items": report_items,
    }
    REPORT.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    if args.apply:
        if not BACKUP.exists():
            shutil.copy2(VAULT, BACKUP)
        VAULT.write_text(
            json.dumps(final_docs, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))
    print(f"initial={len(original_docs)} final={len(final_docs)} apply={args.apply}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
