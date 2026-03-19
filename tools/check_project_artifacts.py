#!/usr/bin/env python3
"""
check_project_artifacts.py — Диагностика артефактов проекта.

Проверяет, в каком состоянии артефакты проекта (legacy vs v2),
и выводит предупреждения если проект требует перегенерации.

Использование:
    python tools/check_project_artifacts.py projects/EM/087-РД-ГП5
    python tools/check_project_artifacts.py --all
"""

import argparse
import json
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def check_project(project_dir: str | Path) -> dict:
    """Проверить артефакты проекта и выявить legacy-состояние.

    Returns:
        dict с результатами проверки и списком warnings.
    """
    project_dir = Path(project_dir)
    output_dir = project_dir / "_output"
    warnings = []
    result = {
        "project_dir": str(project_dir),
        "project_id": "",
        "warnings": warnings,
        "is_legacy": False,
    }

    # project_info
    info_path = project_dir / "project_info.json"
    if info_path.exists():
        info = json.loads(info_path.read_text(encoding="utf-8"))
        result["project_id"] = info.get("project_id", project_dir.name)
    else:
        result["project_id"] = project_dir.name
        warnings.append("project_info.json отсутствует")

    # document_graph version
    graph_path = output_dir / "document_graph.json"
    if graph_path.exists():
        graph = json.loads(graph_path.read_text(encoding="utf-8"))
        result["graph_version"] = graph.get("version", 1)
        if graph.get("version", 1) < 2:
            warnings.append("document_graph.json v1 — нет coords, locality, stamp_data")
            result["is_legacy"] = True
    else:
        result["graph_version"] = None
        warnings.append("document_graph.json отсутствует")
        result["is_legacy"] = True

    # page_sheet_map в compact
    compact_path = output_dir / "_findings_compact.json"
    if compact_path.exists():
        compact = json.loads(compact_path.read_text(encoding="utf-8"))
        psm = compact.get("page_sheet_map", {})
        result["page_sheet_map_size"] = len(psm)
        if len(psm) == 0:
            warnings.append("page_sheet_map пуст — merge LLM будет угадывать sheet")
            result["is_legacy"] = True

        # blocks_compact locality fields
        bcs = compact.get("blocks_compact", [])
        with_stbi = sum(1 for bc in bcs if bc.get("selected_text_block_ids"))
        with_etr = sum(1 for bc in bcs if bc.get("evidence_text_refs"))
        result["blocks_compact_total"] = len(bcs)
        result["blocks_with_selected_text_block_ids"] = with_stbi
        result["blocks_with_evidence_text_refs"] = with_etr
        if len(bcs) > 0 and with_stbi == 0:
            warnings.append(f"0/{len(bcs)} blocks_compact имеют selected_text_block_ids")
            result["is_legacy"] = True

        # preliminary_findings block_evidence format
        pfs = compact.get("preliminary_findings", [])
        filename_form = sum(
            1 for pf in pfs
            if pf.get("block_evidence", "").startswith("block_")
            or pf.get("block_evidence", "").endswith(".png")
        )
        bare_form = sum(
            1 for pf in pfs
            if pf.get("block_evidence") and not (
                pf["block_evidence"].startswith("block_")
                or pf["block_evidence"].endswith(".png")
            )
        )
        result["findings_block_evidence_filename"] = filename_form
        result["findings_block_evidence_bare"] = bare_form
        if filename_form > 0:
            warnings.append(f"{filename_form} findings с block_evidence в filename-form (block_X.png)")
    else:
        result["page_sheet_map_size"] = None

    # 02_blocks_analysis: selected_text_block_ids
    ba_path = output_dir / "02_blocks_analysis.json"
    if ba_path.exists():
        ba_data = json.loads(ba_path.read_text(encoding="utf-8"))
        analyses = ba_data.get("block_analyses", [])
        with_stbi_ba = sum(1 for ba in analyses if ba.get("selected_text_block_ids"))
        with_etr_ba = sum(1 for ba in analyses if ba.get("evidence_text_refs"))
        result["block_analyses_total"] = len(analyses)
        result["block_analyses_with_selected_text"] = with_stbi_ba
        result["block_analyses_with_evidence_refs"] = with_etr_ba
        if len(analyses) > 0 and with_stbi_ba == 0:
            warnings.append(f"0/{len(analyses)} block_analyses имеют selected_text_block_ids")

    # 03_findings
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        fdata = json.loads(findings_path.read_text(encoding="utf-8"))
        findings = fdata.get("findings", [])
        with_source = sum(1 for f in findings if f.get("source_block_ids"))
        with_merge_g = sum(1 for f in findings if f.get("merge_source_g_ids"))
        result["findings_total"] = len(findings)
        result["findings_with_source_block_ids"] = with_source
        result["findings_with_merge_source_g_ids"] = with_merge_g

    return result


def print_report(result: dict):
    """Вывести отчёт."""
    pid = result["project_id"]
    status = "LEGACY" if result["is_legacy"] else "OK"
    print(f"\n{'='*60}")
    print(f"  {pid}  [{status}]")
    print(f"{'='*60}")
    print(f"  graph version:        {result.get('graph_version', '?')}")
    print(f"  page_sheet_map size:  {result.get('page_sheet_map_size', '?')}")
    print(f"  blocks_compact:       {result.get('blocks_compact_total', '?')}")
    print(f"    with selected_text: {result.get('blocks_with_selected_text_block_ids', '?')}")
    print(f"    with evidence_refs: {result.get('blocks_with_evidence_text_refs', '?')}")
    be_fn = result.get('findings_block_evidence_filename', 0)
    be_bare = result.get('findings_block_evidence_bare', 0)
    print(f"  block_evidence:       {be_bare} bare / {be_fn} filename-form")

    if result.get("findings_total"):
        print(f"  findings total:       {result['findings_total']}")
        print(f"    with source_block:  {result.get('findings_with_source_block_ids', 0)}")
        print(f"    with merge_g_ids:   {result.get('findings_with_merge_source_g_ids', 0)}")

    if result["warnings"]:
        print(f"\n  WARNINGS:")
        for w in result["warnings"]:
            print(f"    ⚠ {w}")


def main():
    parser = argparse.ArgumentParser(description="Проверка артефактов проекта")
    parser.add_argument("project_dir", nargs="?", help="Путь к папке проекта")
    parser.add_argument("--all", action="store_true", help="Проверить все проекты")
    args = parser.parse_args()

    if args.all:
        projects_root = BASE_DIR / "projects"
        total = 0
        legacy = 0
        for disc_dir in sorted(projects_root.iterdir()):
            if not disc_dir.is_dir() or disc_dir.name == "DOC":
                continue
            for proj_dir in sorted(disc_dir.iterdir()):
                if not proj_dir.is_dir():
                    continue
                if not (proj_dir / "project_info.json").exists():
                    continue
                result = check_project(proj_dir)
                print_report(result)
                total += 1
                if result["is_legacy"]:
                    legacy += 1
        print(f"\n{'='*60}")
        print(f"  ИТОГО: {total} проектов, {legacy} legacy")
    elif args.project_dir:
        project_dir = args.project_dir
        if not os.path.isabs(project_dir):
            project_dir = BASE_DIR / project_dir
        result = check_project(project_dir)
        print_report(result)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
