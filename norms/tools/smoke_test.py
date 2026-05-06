"""Smoke-тест для authoritative Norms API.

Проверяет минимум:
  1. active из vault (exact + dirty input)
  2. outdated_edition через manual override
  3. replaced через manual override (vault + override_only)
  4. cancelled через manual override
  5. supported family, но не в index (not_in_index)
  6. unsupported family
  7. get_paragraph для override-only/no-text записи
  8. get_paragraph для not_in_index записи
  9. intake_missing_norms.py заводит очередь для unresolved
  10. semantic_search возвращает list[dict]

Запуск:
    python3 tools/smoke_test.py
    python3 tools/smoke_test.py --skip-semantic
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

OVERRIDES = HERE / "status_overrides.yaml"
STATUS_INDEX = HERE / "status_index.json"


def _banner(title: str) -> None:
    print(f"\n=== {title} ===")


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        print(f"  FAIL: {msg}")
        raise SystemExit(1)
    print(f"  ok:   {msg}")


def _rebuild() -> None:
    r = subprocess.run(
        [sys.executable, str(HERE / "build_status_index.py"), "--quiet"],
        check=False,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(r.stderr)
        raise SystemExit(1)


def _reset_api_cache() -> None:
    import norms_api

    norms_api._reset_cache()


def test_active_lookup() -> None:
    _banner("active (vault) — exact + dirty input")
    import norms_api

    r1 = norms_api.get_norm_status("ГОСТ 10180-2012")
    _assert(r1["found"] is True, "exact code found")
    _assert(r1["status"] == "active", f"status={r1['status']}")
    _assert(r1["doc_status"] == "active", f"doc_status={r1['doc_status']}")
    _assert(r1["authoritative"] is True, "authoritative=true")
    _assert(r1["resolution_reason"] == "exact", f"reason={r1['resolution_reason']}")
    _assert(r1["source"] == "vault", f"source={r1['source']}")
    _assert(r1["supported_family"] is True, "supported_family=true")
    _assert(r1["needs_manual_addition"] is False, "resolved → no manual addition")

    r2 = norms_api.get_norm_status("  гост  10180-2012  ")
    _assert(r2["found"] is True, "case-insensitive + extra whitespace")

    r3 = norms_api.get_norm_status("СП 256_1325800_2016")
    _assert(r3["found"] is True, "underscore instead of dot")
    _assert(r3["matched_code"] == "СП 256.1325800.2016", "resolves to canonical dotted code")


def test_overrides_flow() -> None:
    _banner("manual overrides — outdated_edition, replaced, cancelled, alias")
    backup = OVERRIDES.read_text(encoding="utf-8")
    try:
        OVERRIDES.write_text(
            "overrides:\n"
            "  \"СП 256.1325800.2016\":\n"
            "    doc_status: active\n"
            "    edition_status: outdated\n"
            "    current_version: \"СП 256.1325800.2026\"\n"
            "    aliases:\n"
            "      - \"СП 256/1325800/2016\"\n"
            "    details: \"Тестовый override: помечаем как устаревшую редакцию\"\n"
            "  \"ГОСТ 10180-2012\":\n"
            "    doc_status: cancelled\n"
            "    details: \"Тестовый override: cancelled\"\n"
            "  \"СНиП 2.04.01-85\":\n"
            "    doc_status: replaced\n"
            "    replacement_doc: \"СП 30.13330.2020\"\n"
            "    last_verified: \"2026-04-17\"\n"
            "  \"ВСН 353-86\":\n"
            "    replaced_by: \"СП 60.13330.2020\"\n",
            encoding="utf-8",
        )
        _rebuild()
        _reset_api_cache()
        import norms_api

        # outdated_edition
        ro = norms_api.get_norm_status("СП 256.1325800.2016")
        _assert(ro["status"] == "outdated_edition", f"outdated_edition: {ro['status']}")
        _assert(ro["doc_status"] == "active", f"doc_status={ro['doc_status']}")
        _assert(ro["edition_status"] == "outdated", f"edition_status={ro['edition_status']}")
        _assert(ro["current_version"] == "СП 256.1325800.2026", "current_version set")
        _assert(ro["authoritative"] is True, "authoritative")
        _assert(ro["resolution_reason"] == "exact", f"reason={ro['resolution_reason']}")

        # alias resolution (override-added alias)
        ra = norms_api.get_norm_status("сп 256/1325800/2016")
        _assert(ra["found"] is True, "custom alias matches")

        # cancelled
        rc = norms_api.get_norm_status("ГОСТ 10180-2012")
        _assert(rc["status"] == "cancelled", f"cancelled: {rc['status']}")
        _assert(rc["current_version"] is None, "cancelled → current_version=None")

        # replaced via vault doc
        rv = norms_api.get_norm_status("ВСН 353-86")
        _assert(rv["status"] == "replaced", f"replaced (vault): {rv['status']}")
        _assert(rv["replacement_doc"] == "СП 60.13330.2020", "replacement_doc set")

        # replaced via override_only
        ro2 = norms_api.get_norm_status("СНиП 2.04.01-85")
        _assert(ro2["found"] is True, "override_only entry resolvable")
        _assert(ro2["source"] == "override_only", f"source={ro2['source']}")
        _assert(ro2["has_text"] if ro2.get("file") else True, "override-only → file=None")
        _assert(ro2["resolution_reason"] == "manual_override", f"reason={ro2['resolution_reason']}")
        _assert(ro2["current_version"] == "СП 30.13330.2020", "current_version = replacement")

        # dirty input works on override_only too
        rd = norms_api.get_norm_status("снип 2_04_01-85")
        _assert(rd["resolution_reason"] == "manual_override", "dirty → manual_override")

        # meta stats
        idx = norms_api.load_status_index(force_reload=True)
        eff = idx["meta"]["totals_by_effective_status"]
        _assert(eff.get("outdated_edition", 0) >= 1, f"meta outdated_edition≥1: {eff}")
        _assert(eff.get("replaced", 0) >= 2, f"meta replaced≥2: {eff}")
        _assert(eff.get("cancelled", 0) >= 1, f"meta cancelled≥1: {eff}")
    finally:
        OVERRIDES.write_text(backup, encoding="utf-8")
        _rebuild()
        _reset_api_cache()


def test_not_in_index() -> None:
    _banner("supported family, not in index")
    import norms_api

    r = norms_api.get_norm_status("ГОСТ 00000-9999")
    _assert(r["found"] is False, "found=False")
    _assert(r["authoritative"] is False, "authoritative=False")
    _assert(r["detected_family"] == "ГОСТ", f"family={r['detected_family']}")
    _assert(r["supported_family"] is True, "supported_family=True")
    _assert(r["needs_manual_addition"] is True, "needs_manual_addition=True")
    _assert(r["resolution_reason"] == "not_in_index", f"reason={r['resolution_reason']}")
    _assert(r["status"] == "unknown", "status=unknown")


def test_unsupported_family() -> None:
    _banner("unsupported family")
    import norms_api

    r = norms_api.get_norm_status("какая-то произвольная строка без семейства")
    _assert(r["found"] is False, "found=False")
    _assert(r["supported_family"] is False, "supported_family=False")
    _assert(r["needs_manual_addition"] is False, "needs_manual_addition=False")
    _assert(r["resolution_reason"] == "unsupported_family", f"reason={r['resolution_reason']}")


def test_get_paragraph_shapes() -> None:
    _banner("get_paragraph — happy path + override-only + not_in_index")
    import norms_api

    # happy path (vault)
    r = norms_api.get_paragraph("ГОСТ 10180-2012", "4", max_lines=5)
    required = {
        "query_code", "matched_code", "paragraph", "found", "text", "file",
        "line", "status", "doc_status", "edition_status", "authoritative",
        "has_text", "resolution_reason", "replacement_doc", "truncated",
    }
    _assert(required.issubset(r.keys()), f"has required keys, missing={required - r.keys()}")
    _assert(r["has_text"] is True, "has_text=True for vault doc")

    # override-only entry — no text → honest fail
    backup = OVERRIDES.read_text(encoding="utf-8")
    try:
        OVERRIDES.write_text(
            "overrides:\n"
            "  \"СНиП 2.04.01-85\":\n"
            "    doc_status: replaced\n"
            "    replacement_doc: \"СП 30.13330.2020\"\n",
            encoding="utf-8",
        )
        _rebuild()
        _reset_api_cache()
        import norms_api as na

        ro = na.get_paragraph("СНиП 2.04.01-85", "1")
        _assert(ro["found"] is False, "override_only: found=False")
        _assert(ro["has_text"] is False, "override_only: has_text=False")
        _assert(ro["authoritative"] is True, "override_only: authoritative=True")
        _assert(ro["resolution_reason"] == "no_document_text", f"reason={ro['resolution_reason']}")

        # not_in_index
        rn = na.get_paragraph("ГОСТ 00000-9999", "1")
        _assert(rn["found"] is False, "not_in_index: found=False")
        _assert(rn["has_text"] is False, "not_in_index: has_text=False")
        _assert(rn["authoritative"] is False, "not_in_index: authoritative=False")
        _assert(rn["resolution_reason"] == "not_in_index", f"reason={rn['resolution_reason']}")
    finally:
        OVERRIDES.write_text(backup, encoding="utf-8")
        _rebuild()
        _reset_api_cache()


def test_intake_queue() -> None:
    _banner("intake_missing_norms: queue + report")
    with tempfile.TemporaryDirectory() as td:
        findings_path = Path(td) / "03_findings.json"
        findings_path.write_text(
            json.dumps(
                {
                    "findings": [
                        {"id": "F-001", "norm": "ГОСТ 10180-2012",
                         "finding": "см. пункт 5.1 ГОСТ 10180-2012", "recommendation": ""},
                        {"id": "F-002", "norm": "ГОСТ 99999-0000",
                         "finding": "выдуманный стандарт ГОСТ 99999-0000", "recommendation": ""},
                        {"id": "F-003", "norm": "",
                         "finding": "какая-то левая фраза без кода нормы", "recommendation": ""},
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        r = subprocess.run(
            [sys.executable, str(HERE / "intake_missing_norms.py"),
             "--findings", str(findings_path)],
            capture_output=True, text=True,
        )
        _assert(r.returncode == 0, f"intake exited 0 (stderr: {r.stderr.strip()})")
    report = json.loads((HERE / "missing_norms_report.json").read_text(encoding="utf-8"))
    queue = json.loads((HERE / "missing_norms_queue.json").read_text(encoding="utf-8"))
    _assert(report["meta"]["total_resolved"] >= 1, "≥1 resolved (ГОСТ 10180-2012)")
    _assert(report["meta"]["total_unresolved"] >= 1, "≥1 unresolved (ГОСТ 99999-0000)")
    found_missing = any(
        it["raw_norm"].startswith("ГОСТ 99999") for it in queue["items"]
    )
    _assert(found_missing, "unresolved list contains ГОСТ 99999-0000")
    actions = {it["suggested_action"] for it in queue["items"]}
    _assert(
        "add_document_to_vault" in actions or "review_family_support" in actions,
        f"queue actions present: {actions}",
    )


def test_semantic_search_shape() -> None:
    _banner("semantic_search — list[dict], empty-safe")
    import norms_api

    empty = norms_api.semantic_search("", top=3)
    _assert(empty == [], "empty query → []")

    res = norms_api.semantic_search("прочность бетона", top=3)
    _assert(isinstance(res, list), "returns list")
    if res:
        for k in ("score", "code", "paragraph", "file", "line", "text"):
            _assert(k in res[0], f"result has '{k}'")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--skip-semantic", action="store_true")
    args = ap.parse_args()

    if not STATUS_INDEX.exists():
        _rebuild()

    test_active_lookup()
    test_overrides_flow()
    test_not_in_index()
    test_unsupported_family()
    test_get_paragraph_shapes()
    test_intake_queue()
    if not args.skip_semantic:
        test_semantic_search_shape()
    else:
        print("\n=== semantic_search SKIPPED (--skip-semantic) ===")

    print("\nall smoke tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
