"""Бэкфилл номеров листов (sheet_no) для проектов нового 3-файлового формата.

Причина: синтезатор result.json писал штамп в block["ocr_json"], а
graph_builder читает block["stamp_data"] → sheet_no_raw страниц графа
оставался None → столбец «Лист/Раздел» в Excel пустой. Код-фикс уже внесён
(blocks_json.py эмитит stamp_data), этот скрипт лечит УЖЕ посчитанные версии.

Что делает для каждой затронутой версии projects_v2:
  1. Находит source MD (*_results.md / 02_work/document.md) → sheet_map().
  2. Заполняет sheet_no_raw/normalized/sheet_name в document_graph.json
     (в 03_analysis/latest И во всех run-копиях с пустыми листами).
  3. Пере-деривит sheet в 03_findings.json штатной функцией
     backfill_text_evidence_in_findings (тот же формат «Лист X»).

Идемпотентно, fail-soft, детерминированно (без сети/LLM). Dry-run по умолчанию.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.common.results_md import parse_results_md_file  # noqa: E402
from backend.app.pipeline.stages.prepare.graph_builder import _normalize_sheet_no  # noqa: E402


def _find_md(version_dir: Path) -> Path | None:
    """Source MD той же генерации: 02_work/document.md либо 01_input/*_results.md."""
    cands = [version_dir / "02_work" / "document.md"]
    inp = version_dir / "01_input"
    if inp.is_dir():
        cands.extend(sorted(inp.glob("*_results.md")))
    for c in cands:
        if c.is_file():
            return c
    return None


def _load_sheet_map(md_path: Path) -> dict[int, dict]:
    """{PDF-страница (1-based) → {sheet, name}} только с непустым sheet/name."""
    try:
        doc = parse_results_md_file(md_path)
    except Exception:
        return {}
    out = {}
    for pg, info in (doc.sheet_map() or {}).items():
        if info.get("sheet") or info.get("name"):
            out[int(pg)] = info
    return out


def _patch_graph(graph_path: Path, sheet_map: dict[int, dict]) -> int:
    """Заполнить пустые sheet_no_raw/normalized/sheet_name. Возвращает # правок."""
    try:
        g = json.loads(graph_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0
    changed = 0
    for p in g.get("pages", []):
        has = p.get("sheet_no_raw") or p.get("sheet_no_normalized") or p.get("sheet_no")
        if has:
            continue
        # ключ листа = страница PDF своего документа (портал = один PDF)
        key = p.get("source_page_number") or p.get("page")
        info = sheet_map.get(int(key)) if key is not None else None
        if not info:
            continue
        sheet = info.get("sheet")
        name = info.get("name")
        if sheet:
            p["sheet_no_raw"] = str(sheet)
            p["sheet_no_normalized"] = _normalize_sheet_no(str(sheet))
            p["sheet_confidence"] = "high"
        if name and not p.get("sheet_name"):
            p["sheet_name"] = name
        if sheet or name:
            changed += 1
    if changed:
        graph_path.write_text(
            json.dumps(g, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return changed


def process_version(version_dir: Path, apply: bool) -> dict | None:
    latest = version_dir / "03_analysis" / "latest"
    gpath = latest / "document_graph.json"
    fpath = latest / "03_findings.json"
    if not gpath.is_file() or not fpath.is_file():
        return None

    # затронута ли: 0 листов в графе + есть findings с пустым sheet
    try:
        g = json.loads(gpath.read_text(encoding="utf-8"))
        fd = json.loads(fpath.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    pages = g.get("pages", [])
    with_sheet = sum(
        1 for p in pages
        if p.get("sheet_no_raw") or p.get("sheet_no_normalized") or p.get("sheet_no")
    )
    findings = fd.get("findings", [])
    empty = sum(1 for f in findings if not (f.get("sheet") or None))
    if not (pages and with_sheet == 0 and findings and empty):
        return None

    md = _find_md(version_dir)
    sheet_map = _load_sheet_map(md) if md else {}
    if not sheet_map:
        return {"version": str(version_dir), "status": "no_sheet_source",
                "empty": empty, "md": str(md) if md else None}

    if not apply:
        # dry-run: посчитать сколько страниц реально резолвятся
        resolvable = 0
        for p in pages:
            key = p.get("source_page_number") or p.get("page")
            if key is not None and sheet_map.get(int(key), {}).get("sheet"):
                resolvable += 1
        return {"version": str(version_dir), "status": "would_fix",
                "empty": empty, "pages": len(pages),
                "resolvable_pages": resolvable, "md": str(md)}

    # APPLY: патчим latest + все run-копии графа с пустыми листами
    graph_paths = [gpath]
    graph_paths.extend(
        Path(p) for p in glob.glob(
            str(version_dir / "03_analysis" / "runs" / "*" / "document_graph.json")
        )
    )
    patched_pages = 0
    for gp in graph_paths:
        patched_pages += _patch_graph(gp, sheet_map)

    # пере-деривим sheet в findings штатной функцией (тот же формат)
    from backend.app.pipeline.stages.findings_merge.runner import (
        backfill_text_evidence_in_findings,
    )
    os.environ["AUDIT_OUTPUT_DIR"] = str(latest)
    try:
        backfill_text_evidence_in_findings("_backfill", output_dir=latest)
    finally:
        os.environ.pop("AUDIT_OUTPUT_DIR", None)

    fd2 = json.loads(fpath.read_text(encoding="utf-8"))
    still_empty = sum(1 for f in fd2.get("findings", []) if not (f.get("sheet") or None))
    return {"version": str(version_dir), "status": "fixed",
            "patched_pages": patched_pages,
            "empty_before": empty, "empty_after": still_empty}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="применить (иначе dry-run)")
    ap.add_argument("--root", default=str(ROOT / "projects_v2" / "objects"))
    ap.add_argument("--filter", default="", help="подстрока пути версии")
    args = ap.parse_args()

    pattern = f"{args.root}/**/versions/*/03_analysis/latest/document_graph.json"
    version_dirs = sorted({
        Path(gp).parents[2] for gp in glob.glob(pattern, recursive=True)
    })
    results = []
    for vd in version_dirs:
        if args.filter and args.filter not in str(vd):
            continue
        r = process_version(vd, apply=args.apply)
        if r:
            results.append(r)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== {mode}: {len(results)} затронутых версий ===")
    from collections import Counter
    print("статусы:", dict(Counter(r["status"] for r in results)))
    for r in results:
        short = r["version"].split("/objects/")[-1]
        if r["status"] == "fixed":
            print(f"  [fixed]  {short}: pages+{r['patched_pages']} "
                  f"empty {r['empty_before']}→{r['empty_after']}")
        elif r["status"] == "would_fix":
            print(f"  [would]  {short}: empty={r['empty']} "
                  f"resolvable_pages={r['resolvable_pages']}/{r['pages']}")
        else:
            print(f"  [{r['status']}] {short}: empty={r.get('empty')} md={r.get('md')}")


if __name__ == "__main__":
    main()
