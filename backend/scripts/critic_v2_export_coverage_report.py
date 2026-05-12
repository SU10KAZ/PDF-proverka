"""
Diagnostic: почему «Этот проект отсутствует в Critic v2 UI export» появляется
для части проектов в UI.

Назначение:
  - read-only сверка между системными проектами (projects/) и текущим UI
    export'ом (CRITIC_V2_UI_EXPORT_PATH или default-путь);
  - построение coverage-таблицы: matched / missing-in-export /
    matching-failed-but-in-export / no-expert-review / no-findings;
  - запись JSON+MD отчётов в /tmp.

Запрещено:
  - не запускает LLM;
  - не пишет в _output проектов;
  - не меняет artifact;
  - не трогает production pipeline.

Запуск:
  python backend/scripts/critic_v2_export_coverage_report.py \
    [--export-path <path>] [--projects-dir projects] \
    [--out-json /tmp/critic_v2_export_coverage_report.json] \
    [--out-md   /tmp/critic_v2_export_coverage_report.md]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

DEFAULT_EXPORT_PATH = Path(
    "/tmp/critic_v2_ui_export_for_manual_review/llm_no_context/critic_v2_triage_ui.json"
)
DEFAULT_PROJECTS_DIR = Path("projects")
DEFAULT_OUT_JSON = Path("/tmp/critic_v2_export_coverage_report.json")
DEFAULT_OUT_MD = Path("/tmp/critic_v2_export_coverage_report.md")
REPLAY_HINT = (
    "python backend/scripts/replay_critic_v2_triage_policy.py "
    "--matrix-output-dir /tmp/critic_v2_matrix_real_expanded_full "
    "--experiment llm_no_context --profile conservative "
    "--output-dir /tmp/critic_v2_ui_export_for_manual_review --ui-export"
)


def _normalize(name: str) -> str:
    """lower + strip + drop trailing .pdf + collapse whitespace."""
    if not name:
        return ""
    s = str(name).strip().lower()
    if s.endswith(".pdf"):
        s = s[:-4].rstrip()
    return " ".join(s.split())


def _resolve_export_path(arg: str | None) -> Path:
    if arg:
        return Path(arg)
    env = os.environ.get("CRITIC_V2_UI_EXPORT_PATH", "").strip()
    return Path(env) if env else DEFAULT_EXPORT_PATH


def _load_export(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: не удалось прочитать {path}: {exc}", file=sys.stderr)
        return None


def _scan_projects(projects_dir: Path) -> list[dict[str, Any]]:
    """
    Build a flat list of project entries by walking projects/<object>/<SECTION>/<name>/.

    Each entry has:
      project_id      — relative path under projects/ (excluding the object root)
      name            — folder name as-is (may include .pdf suffix)
      section         — section folder (AR/EOM/KJ/SS/...)
      object          — top-level object folder
      output_dir      — Path to _output if exists
      has_findings    — bool (03_findings.json exists)
      has_expert_rev  — bool (expert_review.json exists)
      has_blocks      — bool (02_blocks_analysis.json exists)
      has_doc_graph   — bool (document_graph.json exists)
    """
    out: list[dict[str, Any]] = []
    if not projects_dir.exists():
        return out
    for obj_root in sorted(projects_dir.iterdir()):
        if not obj_root.is_dir():
            continue
        for section_dir in sorted(obj_root.iterdir()):
            if not section_dir.is_dir():
                continue
            section = section_dir.name
            # skip non-section helper dirs like __BATCH__, projects, DOC
            if section.startswith("__") or section.startswith("."):
                continue
            for proj_dir in sorted(section_dir.iterdir()):
                if not proj_dir.is_dir():
                    continue
                out_dir = proj_dir / "_output"
                entry = {
                    "project_id": proj_dir.name,
                    "name": proj_dir.name,
                    "section": section,
                    "object": obj_root.name,
                    "folder_path": str(proj_dir.relative_to(projects_dir)),
                    "output_dir": str(out_dir) if out_dir.exists() else None,
                    "has_findings": (out_dir / "03_findings.json").exists(),
                    "has_expert_review": (out_dir / "expert_review.json").exists(),
                    "has_blocks": (out_dir / "02_blocks_analysis.json").exists(),
                    "has_document_graph": (out_dir / "document_graph.json").exists(),
                }
                out.append(entry)
    return out


def match_project(project: dict[str, Any], export_names: set[str],
                  export_names_normalized: dict[str, str]) -> tuple[str | None, str | None]:
    """
    Returns (matched_export_name, matched_by) or (None, None).

    Matching стратегия — должна точно совпадать с backend/app/api/routers/critic_v2_ui.py
    и webapp/routers/critic_v2_ui.py:
      1. exact project_name (folder name as-is)
      2. exact project_name без trailing .pdf
      3. project_id == export project_name (на случай, если pid отличается от name)
      4. normalized: lowercase + no .pdf + single spaces
    """
    nm = project.get("name") or ""
    pid = project.get("project_id") or ""

    if nm in export_names:
        return nm, "project_name"

    if nm.lower().endswith(".pdf"):
        nm_no = nm[:-4].rstrip()
        if nm_no in export_names:
            return nm_no, "project_name_no_pdf"

    if pid and pid in export_names:
        return pid, "project_id"

    if pid and pid.lower().endswith(".pdf"):
        pid_no = pid[:-4].rstrip()
        if pid_no in export_names:
            return pid_no, "project_id_no_pdf"

    n = _normalize(nm)
    if n and n in export_names_normalized:
        return export_names_normalized[n], "normalized"

    n2 = _normalize(pid)
    if n2 and n2 in export_names_normalized:
        return export_names_normalized[n2], "normalized_project_id"

    return None, None


def build_report(export: dict[str, Any] | None, projects: list[dict[str, Any]],
                 export_path: Path) -> dict[str, Any]:
    if export is None:
        return {
            "ok": False,
            "error": "export_missing",
            "export_path": str(export_path),
            "hint_command": REPLAY_HINT,
            "projects_total": len(projects),
            "projects": [
                {"project_id": p["project_id"], "section": p["section"],
                 "has_findings": p["has_findings"],
                 "has_expert_review": p["has_expert_review"]}
                for p in projects
            ],
        }

    items = export.get("items") or []
    pn_counter: Counter[str] = Counter(it.get("project_name") for it in items)
    export_names_raw = set(pn_counter.keys())
    export_names_normalized = {_normalize(pn): pn for pn in export_names_raw if pn}

    rows: list[dict[str, Any]] = []
    for p in projects:
        matched_name, matched_by = match_project(
            p, export_names_raw, export_names_normalized
        )
        in_export = matched_name is not None
        export_item_count = pn_counter.get(matched_name, 0) if matched_name else 0
        reason: str | None = None
        if not in_export:
            if not p["has_findings"]:
                reason = "no_findings_no_export"
            elif not p["has_expert_review"]:
                reason = "no_expert_review_likely_excluded_from_matrix"
            else:
                reason = "not_in_current_matrix_export"
        rows.append({
            **p,
            "in_export": in_export,
            "matched_export_name": matched_name,
            "matched_by": matched_by,
            "export_item_count": export_item_count,
            "reason_if_missing": reason,
        })

    matched_rows = [r for r in rows if r["in_export"]]
    missing_rows = [r for r in rows if not r["in_export"]]

    return {
        "ok": True,
        "export_path": str(export_path),
        "export_items_total": len(items),
        "export_unique_projects": len(export_names_raw),
        "export_project_names": sorted(export_names_raw),
        "system_projects_total": len(projects),
        "matched_count": len(matched_rows),
        "missing_count": len(missing_rows),
        "matched_by_breakdown": dict(Counter(r["matched_by"] for r in matched_rows)),
        "missing_reasons_breakdown": dict(
            Counter(r["reason_if_missing"] for r in missing_rows)
        ),
        "missing_have_findings": sum(1 for r in missing_rows if r["has_findings"]),
        "missing_have_expert_review": sum(
            1 for r in missing_rows if r["has_expert_review"]
        ),
        "projects": rows,
        "hint_command": REPLAY_HINT,
    }


def write_markdown(report: dict[str, Any], path: Path) -> None:
    out: list[str] = []
    out.append("# Critic v2 — Export Coverage Report")
    out.append("")
    if not report.get("ok"):
        out.append(f"**Export файл отсутствует:** `{report['export_path']}`")
        out.append("")
        out.append("Команда для генерации:")
        out.append("```")
        out.append(report["hint_command"])
        out.append("```")
        out.append("")
        out.append(f"Системных проектов: **{report['projects_total']}**")
        path.write_text("\n".join(out), encoding="utf-8")
        return

    out.append(f"**Export:** `{report['export_path']}`")
    out.append("")
    out.append("## Сводка")
    out.append("")
    out.append(f"- Всего системных проектов: **{report['system_projects_total']}**")
    out.append(f"- Уникальных проектов в export: **{report['export_unique_projects']}**")
    out.append(f"- Items в export: **{report['export_items_total']}**")
    out.append(f"- Совпало (matched): **{report['matched_count']}**")
    out.append(f"- Отсутствует в export: **{report['missing_count']}**")
    out.append("")
    if report["matched_by_breakdown"]:
        out.append("### Как сматчилось")
        for k, v in sorted(report["matched_by_breakdown"].items(),
                           key=lambda x: -x[1]):
            out.append(f"- `{k}`: {v}")
        out.append("")
    if report["missing_reasons_breakdown"]:
        out.append("### Причины отсутствия")
        for k, v in sorted(report["missing_reasons_breakdown"].items(),
                           key=lambda x: -x[1]):
            out.append(f"- `{k}`: {v}")
        out.append("")
    out.append(
        f"- Из отсутствующих имеют `03_findings.json`: "
        f"**{report['missing_have_findings']}**"
    )
    out.append(
        f"- Из отсутствующих имеют `expert_review.json`: "
        f"**{report['missing_have_expert_review']}**"
    )
    out.append("")

    out.append("## Доступные сейчас (в export)")
    out.append("")
    out.append("| project_id | section | matched_by | items |")
    out.append("|---|---|---|---|")
    for r in report["projects"]:
        if r["in_export"]:
            out.append(
                f"| {r['project_id']} | {r['section']} | "
                f"{r['matched_by']} | {r['export_item_count']} |"
            )
    out.append("")

    out.append("## Отсутствуют в текущем export")
    out.append("")
    out.append("| project_id | section | findings | expert_review | reason |")
    out.append("|---|---|---|---|---|")
    for r in report["projects"]:
        if not r["in_export"]:
            out.append(
                f"| {r['project_id']} | {r['section']} | "
                f"{'✓' if r['has_findings'] else '—'} | "
                f"{'✓' if r['has_expert_review'] else '—'} | "
                f"{r['reason_if_missing']} |"
            )
    out.append("")

    out.append("## Как пересобрать export")
    out.append("")
    out.append("```")
    out.append(report["hint_command"])
    out.append("```")
    out.append("")
    out.append(
        "Этот replay-скрипт читает уже посчитанный matrix output и не запускает LLM. "
        "Если для нужных проектов нет данных в matrix output, придётся отдельно "
        "запустить benchmark/matrix — это требует ручного подтверждения, "
        "потому что включает LLM."
    )

    path.write_text("\n".join(out), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--export-path", default=None,
                    help="Путь к critic_v2_triage_ui.json")
    ap.add_argument("--projects-dir", default=str(DEFAULT_PROJECTS_DIR))
    ap.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    export_path = _resolve_export_path(args.export_path)
    projects_dir = Path(args.projects_dir)
    out_json = Path(args.out_json)
    out_md = Path(args.out_md)

    export = _load_export(export_path)
    projects = _scan_projects(projects_dir)
    report = build_report(export, projects, export_path)

    out_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_markdown(report, out_md)

    if not args.quiet:
        if report.get("ok"):
            print(
                f"matched={report['matched_count']}/"
                f"{report['system_projects_total']}  "
                f"missing={report['missing_count']}  "
                f"export_items={report['export_items_total']}"
            )
        else:
            print(f"export missing: {export_path}")
        print(f"wrote: {out_json}")
        print(f"wrote: {out_md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
