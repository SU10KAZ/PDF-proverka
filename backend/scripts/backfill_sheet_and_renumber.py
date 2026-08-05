#!/usr/bin/env python3
"""Бэкфилл номера листа и сплошной нумерации F-ID в уже посчитанных аудитах.

Чинит два дефекта прошлых прогонов (код-фиксы см. в gemma_findings_only.
sheet_for_page, findings_merge.runner.renumber_findings_sequentially):

  1. sheet = НАЗВАНИЕ листа из штампа («Корпус 14.6. Маркировочные планы
     1 этажа») вместо номера «Лист 2» — stage 02 читал v2-граф по v1-ключу
     sheet_no. Название переносится в sheet_title, sheet получает номер из
     document_graph.json (page → sheet_no_raw).
  2. Разрывы нумерации («F-016, F-035, F-036, F-017») — atomicity_guard
     вставлял расщеплённые замечания в середину списка с хвостовыми номерами.

ОПАСНО для версий с вердиктами: expert_review.json / decisions_log ключуются
на F-ID, перенумерация задним числом делает их сиротами. Такие версии
перенумеровываются только с --force-renumber; sheet чинится всегда (он ни на
что не ссылается).

Примеры:
    # что изменится в одной версии
    python backend/scripts/backfill_sheet_and_renumber.py --version <version_dir>

    # применить
    python backend/scripts/backfill_sheet_and_renumber.py --version <dir> --apply

    # только лист, без нумерации, по всем версиям объекта
    python backend/scripts/backfill_sheet_and_renumber.py --root projects_v2/objects/X --no-renumber
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.prepare.graph_builder import (  # noqa: E402
    build_block_page_index, build_md_line_page_index, build_page_sheet_map,
    looks_like_sheet_ref, resolve_document_markdown, resolve_finding_sheet_label,
)


def _load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _graph_indexes(output_dir: Path) -> tuple[dict, dict, list]:
    graph = _load(output_dir / "document_graph.json")
    md_index: list = []
    md_path = resolve_document_markdown(output_dir)
    if md_path is not None:
        try:
            md_index = build_md_line_page_index(md_path.read_text(encoding="utf-8"))
        except OSError:
            pass
    if not isinstance(graph, dict):
        return {}, {}, md_index
    return build_page_sheet_map(graph), build_block_page_index(graph), md_index


def _has_verdicts(version_dir: Path, output_dir: Path) -> bool:
    """Есть ли вердикты эксперта, ключованные на F-ID.

    Реальный формат expert_review.json — {"decisions": [{"item_id": "F-001",
    …}]}; проверка только по "findings" молча пропускала бы такие версии в
    перенумерацию и осиротила вердикты.
    """
    for candidate in (
        version_dir / "04_review" / "expert_review.json",
        output_dir / "expert_review.json",
        version_dir / "_output" / "expert_review.json",
    ):
        data = _load(candidate)
        if isinstance(data, list) and data:
            return True
        if isinstance(data, dict):
            for key in ("decisions", "findings", "items", "reviews"):
                if data.get(key):
                    return True
    return False


def fix_sheets(findings: list, psm: dict, bpi: dict, mdi: list) -> int:
    fixed = 0
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        sheet = finding.get("sheet")
        if sheet and looks_like_sheet_ref(sheet):
            continue
        label = resolve_finding_sheet_label(finding, psm, bpi, mdi)
        if not label:
            continue
        if sheet:
            finding.setdefault("sheet_title", str(sheet).strip())
        finding["sheet"] = label
        finding.pop("sheet_unavailable", None)
        finding.pop("sheet_unavailable_reason", None)
        fixed += 1
    return fixed


def renumber(findings: list) -> int:
    id_map = {}
    for index, finding in enumerate(findings, start=1):
        if not isinstance(finding, dict):
            continue
        old = str(finding.get("id") or "")
        new = f"F-{index:03d}"
        if old and old != new:
            id_map[old] = new
    if not id_map:
        return 0
    for index, finding in enumerate(findings, start=1):
        if not isinstance(finding, dict):
            continue
        finding["id"] = f"F-{index:03d}"
        guard = finding.get("atomicity_guard")
        if isinstance(guard, dict) and str(guard.get("split_from") or "") in id_map:
            guard["split_from"] = id_map[str(guard["split_from"])]
        dup = str(finding.get("duplicate_of") or "")
        if dup in id_map:
            finding["duplicate_of"] = id_map[dup]
    return len(id_map)


# Порядок как в projects_v2_adapter._FINDINGS_PRIORITY: сайт читает
# 03a_norms_verified.json РАНЬШЕ 03_findings.json, поэтому чинить надо оба —
# иначе бэкфилл виден в Excel, но не в UI.
FINDINGS_FILES = ("03_findings.json", "03a_norms_verified.json")


def process_version(
    version_dir: Path, *, apply: bool, do_renumber: bool, force_renumber: bool,
) -> dict | None:
    output_dir = version_dir / "03_analysis" / "latest"
    targets = [output_dir / name for name in FINDINGS_FILES]
    targets = [p for p in targets if p.exists()]
    if not targets:
        return None

    verdicts = _has_verdicts(version_dir, output_dir)
    psm, bpi, mdi = _graph_indexes(output_dir)
    allow_renumber = do_renumber and (force_renumber or not verdicts)
    skipped_renumber = do_renumber and not allow_renumber

    sheets_fixed = 0
    ids_fixed = 0
    total_findings = 0
    backups: list = []
    for findings_path in targets:
        data = _load(findings_path)
        if not isinstance(data, dict) or not isinstance(data.get("findings"), list):
            continue
        findings = data["findings"]
        total_findings = max(total_findings, len(findings))
        file_sheets = fix_sheets(findings, psm, bpi, mdi)
        file_ids = renumber(findings) if allow_renumber else 0
        sheets_fixed += file_sheets
        ids_fixed += file_ids
        if not (file_sheets or file_ids):
            continue
        if apply:
            stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
            backup = findings_path.with_suffix(f".json.bak_{stamp}")
            shutil.copy2(findings_path, backup)
            meta = data.get("meta")
            if isinstance(meta, dict):
                if file_sheets:
                    meta["sheet_backfilled"] = file_sheets
                if file_ids:
                    meta["findings_renumbered"] = file_ids
            findings_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            backups.append(str(backup))

    return {
        "version": str(version_dir),
        "findings": total_findings,
        "sheets_fixed": sheets_fixed,
        "ids_renumbered": ids_fixed,
        "has_verdicts": verdicts,
        "renumber_skipped_due_to_verdicts": skipped_renumber,
        "backups": backups,
    }


def iter_versions(root: Path):
    for findings_path in sorted(root.glob("**/versions/*/03_analysis/latest/03_findings.json")):
        yield findings_path.parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", action="append", default=[],
                        help="путь к папке версии (…/versions/v001)")
    parser.add_argument("--root", help="обойти все версии внутри пути")
    parser.add_argument("--apply", action="store_true", help="записать изменения (иначе dry-run)")
    parser.add_argument("--no-renumber", action="store_true", help="чинить только номер листа")
    parser.add_argument("--force-renumber", action="store_true",
                        help="перенумеровать даже при наличии вердиктов эксперта (осиротит их)")
    args = parser.parse_args()

    versions = [Path(v).resolve() for v in args.version]
    if args.root:
        versions.extend(iter_versions(Path(args.root).resolve()))
    if not versions:
        parser.error("нужен --version или --root")

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] версий к обработке: {len(versions)}")
    totals = {"sheets": 0, "ids": 0, "touched": 0, "skipped": 0}
    for version_dir in versions:
        report = process_version(
            version_dir, apply=args.apply,
            do_renumber=not args.no_renumber,
            force_renumber=args.force_renumber,
        )
        if not report:
            continue
        if report["renumber_skipped_due_to_verdicts"]:
            totals["skipped"] += 1
        if not (report["sheets_fixed"] or report["ids_renumbered"]):
            continue
        totals["sheets"] += report["sheets_fixed"]
        totals["ids"] += report["ids_renumbered"]
        totals["touched"] += 1
        name = Path(report["version"]).parents[1].name
        print(f"  {name:44s} лист: {report['sheets_fixed']:3d}  "
              f"ID: {report['ids_renumbered']:3d}"
              + ("  [вердикты — нумерация пропущена]"
                 if report["renumber_skipped_due_to_verdicts"] else ""))
    print(f"[{mode}] версий изменено: {totals['touched']}, "
          f"листов: {totals['sheets']}, ID: {totals['ids']}, "
          f"пропущено из-за вердиктов: {totals['skipped']}")
    if not args.apply:
        print("Ничего не записано. Повторите с --apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
