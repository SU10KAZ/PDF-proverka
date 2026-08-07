#!/usr/bin/env python3
"""Пакетная проверка missing_norms_vault по официальному protect.gost.ru."""
from __future__ import annotations

import argparse
import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import urlencode, urljoin

import requests
from lxml import html

BASE_URL = "https://protect.gost.ru"
DEFAULT_INPUT = Path("backend/app/data/missing_norms_vault.json")
DEFAULT_OUTPUT = Path("backend/app/data/missing_norms_online_audit.json")
ACTIVE_STATUSES = {"действует", "действует только в рф", "принят"}
NON_ACTIVE_STATUSES = {
    "заменен", "отменен", "срок действия истек", "утратил силу в рф"
}
_thread_local = threading.local()


def _compact(value: str) -> str:
    value = value.upper().replace("Ё", "Е").replace("_", ".")
    value = value.replace("–", "-").replace("—", "-")
    value = re.sub(r"\s+", "", value)
    return value.rstrip(".,;:")


def _clean_status(value: str) -> str:
    value = re.sub(r"^[^А-Яа-я]+", "", value).strip().lower()
    return re.sub(r"\s+", " ", value)


def _section(doc: str) -> str | None:
    upper = doc.upper()
    if upper.startswith("СП "):
        return "sp"
    if upper.startswith("ГОСТ"):
        return "gost"
    return None


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "PDF-proverka missing norms audit/1.0 (official registry verification)",
            "Accept-Language": "ru-RU,ru;q=0.9",
        })
        _thread_local.session = session
    return session


def _get(url: str, *, params: dict[str, str] | None = None) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            response = _session().get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except Exception as exc:  # pragma: no cover - зависит от сети
            last_error = exc
            time.sleep(0.8 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _parse_results(page: bytes, section: str) -> list[dict[str, str]]:
    tree = html.fromstring(page)
    results: list[dict[str, str]] = []
    marker = f"/{section}/details/"
    for row in tree.xpath("//tbody/tr"):
        links = row.xpath(f'.//a[contains(@href, "{marker}")]')
        cells = row.xpath("./td")
        if not links or len(cells) < 4:
            continue
        values = [" ".join(cell.text_content().split()) for cell in cells]
        designation, title, pages, raw_status = values[:4]
        results.append({
            "designation": designation,
            "title": title,
            "pages": pages,
            "status": _clean_status(raw_status),
            "url": urljoin(BASE_URL, links[0].get("href")),
        })
    return results


def _verdict(doc: str, results: list[dict[str, str]]) -> tuple[str, dict[str, str] | None]:
    key = _compact(doc)
    exact = [item for item in results if _compact(item["designation"]) == key]
    if exact:
        selected = exact[0]
        status = selected["status"]
        if status in ACTIVE_STATUSES:
            return "verified_active_exact", selected
        if status in NON_ACTIVE_STATUSES:
            return "verified_non_active_exact", selected
        return "verified_exact_status_other", selected

    if not results:
        return "not_found", None

    ranked = sorted(
        results,
        key=lambda item: SequenceMatcher(None, key, _compact(item["designation"])).ratio(),
        reverse=True,
    )
    selected = ranked[0]
    score = SequenceMatcher(None, key, _compact(selected["designation"])).ratio()
    if score >= 0.94:
        return "probable_typo_candidate", selected
    return "search_results_without_exact_match", selected


def audit_one(doc: str) -> dict[str, Any]:
    section = _section(doc)
    if section is None:
        return {
            "input": doc,
            "section": None,
            "verdict": "needs_other_official_source",
            "selected": None,
            "results": [],
            "query_url": None,
        }

    params = {"month": "0", "year": "0", "search": doc}
    query_url = f"{BASE_URL}/{section}?{urlencode(params)}"
    try:
        response = _get(f"{BASE_URL}/{section}", params=params)
        results = _parse_results(response.content, section)
        verdict, selected = _verdict(doc, results)
        return {
            "input": doc,
            "section": section,
            "verdict": verdict,
            "selected": selected,
            "results": results,
            "query_url": query_url,
        }
    except Exception as exc:
        return {
            "input": doc,
            "section": section,
            "verdict": "request_error",
            "selected": None,
            "results": [],
            "query_url": query_url,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    docs = json.loads(args.input.read_text(encoding="utf-8"))
    if not isinstance(docs, list) or not all(isinstance(item, str) for item in docs):
        raise SystemExit("input должен быть JSON-массивом строк")

    existing: dict[str, dict[str, Any]] = {}
    if args.output.exists():
        try:
            prior = json.loads(args.output.read_text(encoding="utf-8"))
            existing = {item["input"]: item for item in prior.get("items", [])}
        except Exception:
            existing = {}

    pending = [doc for doc in docs if doc not in existing]
    print(f"total={len(docs)} cached={len(existing)} pending={len(pending)}", flush=True)

    completed = 0
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(audit_one, doc): doc for doc in pending}
        for future in as_completed(futures):
            item = future.result()
            with lock:
                existing[item["input"]] = item
                completed += 1
                if completed % 20 == 0 or completed == len(pending):
                    ordered = [existing[doc] for doc in docs if doc in existing]
                    counts: dict[str, int] = {}
                    for row in ordered:
                        verdict = row["verdict"]
                        counts[verdict] = counts.get(verdict, 0) + 1
                    _write_atomic(args.output, {
                        "source": BASE_URL,
                        "input": str(args.input),
                        "total": len(docs),
                        "completed": len(ordered),
                        "verdict_counts": counts,
                        "items": ordered,
                    })
                    print(f"completed={len(ordered)}/{len(docs)} {counts}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
