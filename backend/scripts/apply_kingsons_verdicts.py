#!/usr/bin/env python3
"""Импорт ответов заказчика (реестр СУ-10, King&Sons) в замечания проектов.

Пайплайн (флаги выполняются в этом порядке за один вызов):

    --import   распарсить .xlsx → сохранить реестр (без мутации findings)
    --match    LLM-сопоставление записей реестра с findings (claude -p, МЕДЛЕННО;
               пишет external_register-бейджи на matched findings)
    --apply    создать недостающие findings (REG-*) + проставить экспертные
               вердикты (accepted/rejected) по решению заказчика — в V1

Без --apply печатается DRY-RUN план по текущему состоянию реестра
(ничего не пишется). Реестр должен быть предварительно импортирован.

Примеры:

    # 1. Импорт + просмотр плана (что сматчится, что создастся):
    python backend/scripts/apply_kingsons_verdicts.py --import

    # 2. LLM-матчинг (долго), затем review в /external-register:
    python backend/scripts/apply_kingsons_verdicts.py --match

    # 3. Применить (создать findings + вердикты), отчёт в файл:
    python backend/scripts/apply_kingsons_verdicts.py --apply --report /tmp/kingsons_apply.md

Решения (зафиксированы):
  Отклонено→rejected; Требует внесения/Внесено/По согласованию→accepted.
  Версии: пишем только в V1. Источник — лист «Замечания к отправ.».
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DEFAULT_OBJECT_ID = "0b540226"  # 213. Мосфильмовская 31А "King&Sons"
DEFAULT_REGISTER_ID = "su10_2026-05-27"
DEFAULT_XLSX = (
    REPO_ROOT
    / 'projects/213. Мосфильмовская 31А "King&Sons"/1_feedback'
    / "KingSons реестр от СУ-10 - ответы для СУ-10 от 27.05.2026.xlsx"
)


def _render_report(report) -> str:
    """Markdown-отчёт по ApplyReport."""
    from backend.app.services.external_register.apply_verdicts import VERDICT_MAP  # noqa

    t = report.totals()
    lines: list[str] = []
    mode = "DRY-RUN (ничего не записано)" if report.dry_run else "ПРИМЕНЕНО"
    lines.append(f"# Реестр СУ-10 → вердикты — {mode}")
    lines.append("")
    lines.append(f"- object_id: `{report.object_id}`  register_id: `{report.register_id}`")
    lines.append(f"- проектов затронуто: **{t['projects']}**")
    lines.append(f"- пометить существующие findings: **{t['mark_existing']}**")
    lines.append(f"- создать новые findings (REG-*): **{t['create_new']}**"
                 f" (из них из needs_review: {t['needs_review_as_new']})")
    lines.append(f"- пропущено без вердикта (Не определено): {t['skipped_no_verdict']}")
    if t["skipped_unmapped_sections"]:
        lines.append(f"- ⚠ unmapped-разделы: {t['skipped_unmapped_sections']}")
    lines.append("")
    lines.append("| Проект | разделы | findings? | пометить | создать | needs_rev→new | accept | reject |")
    lines.append("|---|---|:-:|--:|--:|--:|--:|--:|")
    for p in report.projects:
        s = p.as_summary()
        lines.append(
            f"| `{s['project_id']}` | {len(s['sections'])} | "
            f"{'да' if s['had_findings_file'] else '—'} | "
            f"{s['mark_existing']} | {s['create_new']} | {s['needs_review_as_new']} | "
            f"{s['accepted']} | {s['rejected']} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--object", default=DEFAULT_OBJECT_ID, help="object_id (default King&Sons)")
    ap.add_argument("--register-id", default=DEFAULT_REGISTER_ID)
    ap.add_argument("--xlsx", default=str(DEFAULT_XLSX), help="путь к .xlsx реестру (для --import)")
    ap.add_argument("--import", dest="do_import", action="store_true", help="распарсить xlsx и сохранить реестр")
    ap.add_argument("--match", action="store_true", help="LLM-матчинг (медленно, пишет бейджи)")
    ap.add_argument("--apply", action="store_true", help="создать findings + проставить вердикты (иначе dry-run)")
    ap.add_argument("--report", default=None, help="сохранить markdown-отчёт в файл")
    args = ap.parse_args()

    from backend.app.services.external_register import apply_verdicts, matcher, parser, service

    # 1. Import
    if args.do_import:
        xlsx = Path(args.xlsx)
        if not xlsx.exists():
            print(f"❌ xlsx не найден: {xlsx}", file=sys.stderr)
            return 2
        entries = parser.parse_kingsons_xlsx(xlsx)
        reg = service.import_register_entries(args.object, args.register_id, entries, source=str(xlsx))
        print(f"✔ импортировано записей: {len(reg.entries)}; unmapped-разделы: {reg.unmapped_sections or '—'}")

    # 2. Match (опционально, медленно)
    if args.match:
        register = service.load_register(args.object, args.register_id)
        if register is None:
            print(f"❌ реестр {args.register_id} не найден — сначала --import", file=sys.stderr)
            return 2
        print("⏳ LLM-матчинг (claude -p)…")
        stats = matcher.match_register_sync(args.object, args.register_id)
        print(f"✔ матчинг: {stats}")

    # 3. Plan / Apply
    register = service.load_register(args.object, args.register_id)
    if register is None:
        print(f"❌ реестр {args.register_id} не найден — сначала --import", file=sys.stderr)
        return 2

    report = apply_verdicts.apply_register(register, args.object, dry_run=not args.apply)
    md = _render_report(report)
    print("\n" + md + "\n")
    if args.report:
        Path(args.report).write_text(md, encoding="utf-8")
        print(f"📝 отчёт сохранён: {args.report}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
