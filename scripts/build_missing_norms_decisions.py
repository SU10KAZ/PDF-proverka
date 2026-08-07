#!/usr/bin/env python3
"""Второй проход: найти официальные кандидаты для неточно записанных норм.

Скрипт ничего не меняет в рабочем реестре. Он расширяет поиск для записей,
которые не дали точного совпадения на первом проходе, и сохраняет найденные
официальные карточки для последующего принятия решения.
"""
from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from audit_missing_norms_online import (
    BASE_URL,
    _compact,
    _get,
    _parse_results,
    _section,
)

DEFAULT_AUDIT = Path("backend/app/data/missing_norms_online_audit.json")
DEFAULT_INDEX = Path("norms/tools/status_index.json")
DEFAULT_OUTPUT = Path("backend/app/data/missing_norms_online_candidates.json")
UNRESOLVED = {"not_found", "search_results_without_exact_match"}


def _body(value: str) -> str:
    return re.sub(
        r"^\s*(?:ГОСТ(?:\s+Р)?(?:\s+(?:МЭК|IEC))?|СП)\s+",
        "",
        value,
        flags=re.I,
    )


def _without_year(value: str) -> str:
    return re.sub(
        r"(?:(?<=-)|(?<=\.))(?:(?:19|20)\d{2}|\d{2})-?$",
        "",
        value,
    ).rstrip(".- ")


def _similarity(source: str, candidate: str) -> float:
    """Сходство обозначений с меньшим штрафом за ошибочный префикс."""
    full = SequenceMatcher(None, _compact(source), _compact(candidate)).ratio()
    bodies = SequenceMatcher(
        None,
        _compact(_body(source)),
        _compact(_body(candidate)),
    ).ratio()
    return round(max(full, bodies), 5)


def _local_candidates(doc: str, index: dict[str, Any], limit: int = 6) -> list[str]:
    ranked: list[tuple[float, str]] = []
    source_body = _compact(_body(doc))
    for item in index.get("norms", []):
        for value in [item.get("code"), *(item.get("aliases") or [])]:
            if not value:
                continue
            candidate_body = _compact(_body(value))
            score = SequenceMatcher(None, source_body, candidate_body).ratio()
            ranked.append((score, value))
    result: list[str] = []
    for score, value in sorted(ranked, reverse=True):
        if score < 0.72:
            break
        if value not in result:
            result.append(value)
        if len(result) >= limit:
            break
    return result


def _heuristic_candidates(doc: str) -> list[str]:
    variants: list[str] = []

    def add(value: str) -> None:
        value = re.sub(r"\s+", " ", value).strip(" .-")
        if value and value != doc and value not in variants:
            variants.append(value)

    add(doc.replace("131500", "1311500"))
    add(doc.replace(".1330.", ".13330."))
    add(doc.replace(".1330-", ".13330-"))
    add(doc.replace("ГОСТ Р ", "ГОСТ ", 1))
    if doc.startswith("ГОСТ ") and not doc.startswith(
        ("ГОСТ Р ", "ГОСТ IEC ", "ГОСТ МЭК ")
    ):
        add("ГОСТ Р " + doc[len("ГОСТ "):])
    add(doc.replace("ГОСТ IEC ", "ГОСТ Р МЭК ", 1))
    add(doc.replace("ГОСТ Р МЭК ", "ГОСТ IEC ", 1))
    add(doc.replace("ГОСТ Р IEC ", "ГОСТ IEC ", 1))
    return variants


def _queries(doc: str, local: list[str]) -> list[str]:
    candidates = [
        _body(doc),
        _without_year(doc),
        _without_year(_body(doc)),
        *_heuristic_candidates(doc),
        *local,
    ]
    result: list[str] = []
    for value in candidates:
        value = value.strip()
        if len(value) < 3 or value in result:
            continue
        result.append(value)
    return result


def collect_one(item: dict[str, Any], index: dict[str, Any]) -> dict[str, Any]:
    doc = item["input"]
    section = _section(doc)
    assert section
    local = _local_candidates(doc, index)
    found: dict[str, dict[str, str]] = {
        row["designation"]: row for row in item.get("results") or []
    }
    queries = _queries(doc, local)
    errors: list[str] = []
    for query in queries:
        try:
            response = _get(
                f"{BASE_URL}/{section}",
                params={"month": "0", "year": "0", "search": query},
            )
            for row in _parse_results(response.content, section):
                found.setdefault(row["designation"], row)
        except Exception as exc:  # pragma: no cover - сеть
            errors.append(f"{query}: {type(exc).__name__}: {exc}")

    ranked = sorted(
        found.values(),
        key=lambda row: (_similarity(doc, row["designation"]), row["designation"]),
        reverse=True,
    )
    return {
        "input": doc,
        "first_verdict": item["verdict"],
        "queries": queries,
        "local_candidates": local,
        "official_candidates": [
            {**row, "similarity": _similarity(doc, row["designation"])}
            for row in ranked[:20]
        ],
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit", type=Path, default=DEFAULT_AUDIT)
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    audit = json.loads(args.audit.read_text(encoding="utf-8"))
    index = json.loads(args.index.read_text(encoding="utf-8"))
    pending = [
        item
        for item in audit["items"]
        if item.get("verdict") in UNRESOLVED and _section(item["input"])
    ]
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(collect_one, item, index): item["input"] for item in pending
        }
        for number, future in enumerate(as_completed(futures), 1):
            row = future.result()
            results[row["input"]] = row
            if number % 20 == 0 or number == len(pending):
                print(f"completed={number}/{len(pending)}", flush=True)

    payload = {
        "source": BASE_URL,
        "total": len(pending),
        "items": [results[item["input"]] for item in pending],
    }
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
