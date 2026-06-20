#!/usr/bin/env python3
"""reserc.md #96 — единый eval-харнесс качества аудита против экспертного эталона.

Эталон (ground-truth) — это накопленная база решений `knowledge_base/decisions_log.json`:
каждая запись = замечание, которое аудит ПРОИЗВЁЛ, с вердиктом эксперта
(`expert_decision` = accepted | rejected) и действием заказчика
(`customer_response`, `customer_confirmed`).

Что считаем:
  precision (acceptance-rate) = accepted / (accepted + rejected)
    — доля произведённых замечаний, которые эксперт ПРИНЯЛ. Это объективное
      «число до/после» для всей темы качества аудита, не требующее новой разметки.

Разрезы: по типу (finding/optimization), разделу (AR/KJ/EOM…), severity, категории,
объекту и проекту (топ/антитоп исполнители при достаточной выборке).

Честная оговорка про RECALL: полный recall (сколько РЕАЛЬНЫХ дефектов аудит
ПРОПУСТИЛ) из decisions_log не вычисляется — датасет содержит только то, что аудит
выдал и эксперт оценил; множества «должен был найти, но не нашёл» здесь нет.
Для recall нужна отдельная адверсариальная экспертная разметка (см. reserc.md
open_questions). Этот харнесс честно меряет precision/acceptance, а не выдаёт
несуществующий recall.

READ-ONLY: ничего не мутирует. Пригоден как CI-метрика (стабильные числа на
фиксированном эталоне).

Использование:
    python scripts/eval_harness.py                      # отчёт по live decisions_log
    python scripts/eval_harness.py --json out.json      # + дамп метрик в JSON
    python scripts/eval_harness.py --min-sample 30      # порог выборки для топ/антитоп
    python scripts/eval_harness.py --input path.json    # альтернативный эталон
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_LOG = _REPO_ROOT / "knowledge_base" / "decisions_log.json"

_ACCEPTED = "accepted"
_REJECTED = "rejected"


def _norm_decision(rec: dict) -> str:
    return str(rec.get("expert_decision") or "").strip().lower()


def _breakdown(entries: list[dict], key: str) -> dict:
    """Группировка по полю key → {total, accepted, rejected, precision}."""
    groups: dict[str, dict] = {}
    for r in entries:
        g = str(r.get(key) or "—")
        d = _norm_decision(r)
        b = groups.setdefault(g, {"total": 0, "accepted": 0, "rejected": 0})
        b["total"] += 1
        if d == _ACCEPTED:
            b["accepted"] += 1
        elif d == _REJECTED:
            b["rejected"] += 1
    for b in groups.values():
        rev = b["accepted"] + b["rejected"]
        b["precision"] = round(b["accepted"] / rev, 4) if rev else None
    return dict(sorted(groups.items(), key=lambda kv: -kv[1]["total"]))


def compute_eval_metrics(entries: list[dict], *, min_sample: int = 20) -> dict:
    """Чистая функция: precision-метрики аудита против экспертного эталона.

    min_sample — минимальный размер выборки проекта, чтобы попасть в топ/антитоп
    (иначе один-два замечания дают шумные 0%/100%).
    """
    total = len(entries)
    accepted = sum(1 for r in entries if _norm_decision(r) == _ACCEPTED)
    rejected = sum(1 for r in entries if _norm_decision(r) == _REJECTED)
    reviewed = accepted + rejected
    other = total - reviewed
    confirmed = sum(1 for r in entries if r.get("customer_confirmed") is True)

    by_project = _breakdown(entries, "source_project")
    ranked = [
        {"source_project": k, **v}
        for k, v in by_project.items()
        if (v["accepted"] + v["rejected"]) >= min_sample and v["precision"] is not None
    ]
    ranked.sort(key=lambda x: x["precision"], reverse=True)

    # распределение действий заказчика
    cust = {}
    for r in entries:
        cr = str(r.get("customer_response") or "").strip() or "—"
        cust[cr] = cust.get(cr, 0) + 1

    return {
        "total": total,
        "accepted": accepted,
        "rejected": rejected,
        "reviewed": reviewed,
        "other_no_verdict": other,
        "customer_confirmed": confirmed,
        "precision": round(accepted / reviewed, 4) if reviewed else None,
        "customer_confirm_rate": round(confirmed / total, 4) if total else None,
        "by_item_type": _breakdown(entries, "item_type"),
        "by_section": _breakdown(entries, "section"),
        "by_severity": _breakdown(entries, "severity"),
        "by_category": _breakdown(entries, "category"),
        "by_object": _breakdown(entries, "object_id"),
        "customer_response_distribution": dict(
            sorted(cust.items(), key=lambda kv: -kv[1])
        ),
        "min_sample": min_sample,
        "top_projects": ranked[:10],
        "bottom_projects": ranked[-10:][::-1] if len(ranked) > 10 else [],
        "recall_note": (
            "recall (пропущенные дефекты) НЕ вычисляется из decisions_log: датасет "
            "содержит только произведённые аудитом замечания с вердиктом эксперта; "
            "множества «должен был найти, но пропустил» здесь нет. Для recall нужна "
            "отдельная адверсариальная экспертная разметка по эталонному объекту."
        ),
    }


def load_entries(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data.get("entries") or data.get("decisions") or data.get("log") or []
    return data if isinstance(data, list) else []


def _fmt_breakdown(title: str, bd: dict, limit: int = 12) -> str:
    lines = [f"\n{title}:"]
    lines.append(f"  {'группа':<28} {'всего':>6} {'прин':>5} {'откл':>5} {'precision':>9}")
    for k, v in list(bd.items())[:limit]:
        p = "—" if v["precision"] is None else f"{v['precision']*100:.1f}%"
        lines.append(f"  {k[:28]:<28} {v['total']:>6} {v['accepted']:>5} {v['rejected']:>5} {p:>9}")
    return "\n".join(lines)


def format_report(m: dict) -> str:
    out = ["=== EVAL-ХАРНЕСС: качество аудита против экспертного эталона ==="]
    pr = "—" if m["precision"] is None else f"{m['precision']*100:.1f}%"
    out.append(
        f"всего замечаний: {m['total']} | принято: {m['accepted']} | "
        f"отклонено: {m['rejected']} | precision (acceptance): {pr}"
    )
    ccr = "—" if m["customer_confirm_rate"] is None else f"{m['customer_confirm_rate']*100:.1f}%"
    out.append(f"подтверждено заказчиком: {m['customer_confirmed']} ({ccr})")
    out.append(_fmt_breakdown("По типу", m["by_item_type"]))
    out.append(_fmt_breakdown("По разделу", m["by_section"]))
    out.append(_fmt_breakdown("По severity", m["by_severity"]))
    if m["top_projects"]:
        out.append(f"\nТоп проектов по precision (выборка ≥ {m['min_sample']}):")
        for p in m["top_projects"]:
            out.append(f"  {p['precision']*100:5.1f}%  {p['source_project'][:40]} (n={p['accepted']+p['rejected']})")
    if m["bottom_projects"]:
        out.append("\nАнтитоп проектов по precision:")
        for p in m["bottom_projects"]:
            out.append(f"  {p['precision']*100:5.1f}%  {p['source_project'][:40]} (n={p['accepted']+p['rejected']})")
    out.append("\n" + m["recall_note"])
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Eval-харнесс качества аудита (#96)")
    ap.add_argument("--input", type=Path, default=_DEFAULT_LOG, help="путь к decisions_log.json")
    ap.add_argument("--json", type=Path, default=None, help="дамп метрик в JSON")
    ap.add_argument("--min-sample", type=int, default=20, help="порог выборки для топ/антитоп")
    args = ap.parse_args(argv)

    if not args.input.exists():
        print(f"[eval] эталон не найден: {args.input}", file=sys.stderr)
        return 2
    entries = load_entries(args.input)
    metrics = compute_eval_metrics(entries, min_sample=args.min_sample)
    print(format_report(metrics))
    if args.json:
        args.json.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n[eval] метрики записаны: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
