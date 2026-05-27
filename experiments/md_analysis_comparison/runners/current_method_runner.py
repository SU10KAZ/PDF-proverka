"""Current-method runner — single-pass Opus on the full MD.

Mirrors AuditManager Stage 01 conceptually: one Claude Opus call with one
big prompt produces all findings. Done as a subprocess of `claude -p`,
exactly the production pattern. No imports from production code.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from configs import config as cfg  # noqa: E402
from runners._common import run_claude, read_md  # noqa: E402
from runners.unified_output_schema import RunResult, coerce_finding  # noqa: E402

DISCIPLINE_FULL_NAMES = {
    "EOM": "Электроснабжение и силовое электрооборудование",
    "OV":  "Отопление, вентиляция, кондиционирование",
    "VK":  "Внутреннее водоснабжение и канализация",
    "AR":  "Архитектурные решения",
    "KJ":  "Конструкции железобетонные",
    "KM":  "Конструкции металлические",
    "SS":  "Сети связи / слаботочные системы",
    "APS": "Автоматическая пожарная сигнализация",
    "SOUE":"Система оповещения и управления эвакуацией",
    "TX":  "Технологические решения",
    "POS": "Проект организации строительства",
    "GP":  "Генеральный план",
    "FACADE":"Фасады",
    "ROOF":"Кровля",
    "FINISH":"Отделка",
    "LANDSCAPE":"Благоустройство",
    "VT":  "Вертикальный транспорт",
    "TZ_RD":"ТЗ vs РД",
    "CONTRACT":"Договорные условия",
    "MULTI":"Междисциплинарный",
}

PROMPT_PATH = cfg.PROMPTS_DIR / "current_method" / "text_analysis_task.md"


def build_prompt(md_content: str, discipline: str) -> str:
    template = PROMPT_PATH.read_text(encoding="utf-8")
    return (template
            .replace("{DISCIPLINE}", discipline)
            .replace("{DISCIPLINE_FULL_NAME}", DISCIPLINE_FULL_NAMES.get(discipline, discipline))
            .replace("{MD_CONTENT}", md_content))


def run(case_dir: Path, output_path: Path) -> RunResult:
    info_path = case_dir / "case.json"
    info = json.loads(info_path.read_text(encoding="utf-8"))
    discipline = info.get("discipline", "MULTI")
    md_path = case_dir / info.get("md_file", "input.md")
    md = read_md(md_path)

    prompt = build_prompt(md, discipline)
    started = time.time()
    res = run_claude(
        prompt=prompt,
        model=cfg.MODEL_OPUS,
        timeout=cfg.DEFAULT_TIMEOUT_SEC,
        label=f"current_method/{info.get('id', case_dir.name)}",
    )
    duration = time.time() - started

    findings = []
    errors: list[str] = []
    if res.parsed_json and isinstance(res.parsed_json, dict):
        raw_list = res.parsed_json.get("findings") or []
        for i, f in enumerate(raw_list, start=1):
            try:
                findings.append(coerce_finding(f, i, source_agent="current_method"))
            except Exception as exc:
                errors.append(f"coerce_finding[{i}] failed: {exc}")
    else:
        errors.append("No parseable JSON in Claude response")
        errors.append(f"stderr: {res.raw_stderr[:300]}")

    result = RunResult(
        method="current_method",
        case_id=info.get("id", case_dir.name),
        discipline=discipline,
        model_main=cfg.MODEL_OPUS,
        duration_sec=duration,
        findings=findings,
        meta={
            "prompt_chars": len(prompt),
            "md_chars": len(md),
            "exit_code": res.exit_code,
            "ok": res.ok,
        },
        errors=errors,
    )
    result.save(output_path)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", required=True, help="Case ID (folder under datasets/)")
    ap.add_argument("--out", default=None, help="Output path (defaults to results/<case>/current.json)")
    args = ap.parse_args()

    case = cfg.DATASETS_DIR / args.case
    if not case.exists():
        sys.exit(f"Case not found: {case}")
    out = Path(args.out) if args.out else cfg.RESULTS_DIR / args.case / "current.json"

    result = run(case, out)
    print(f"[current_method] {args.case}: {len(result.findings)} findings in {result.duration_sec:.1f}s")
    if result.errors:
        print(f"  errors: {result.errors[:3]}", file=sys.stderr)
    print(f"  saved: {out}")


if __name__ == "__main__":
    main()
