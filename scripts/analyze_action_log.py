#!/usr/bin/env python3
"""
Анализ журнала действий (logs/actions/actions-YYYY-MM-DD.jsonl).

Примеры:
  python scripts/analyze_action_log.py                    # сводка за 7 дней
  python scripts/analyze_action_log.py --days 30          # сводка за 30 дней
  python scripts/analyze_action_log.py --errors           # последние ошибки
  python scripts/analyze_action_log.py --errors --days 3 --limit 50
  python scripts/analyze_action_log.py --user andrey      # действия инженера
  python scripts/analyze_action_log.py --kind pipeline --q "block_analysis"
"""
import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.app.core import action_log  # noqa: E402

# Управляющие символы из недоверенных полей (path/query/message приходят из
# HTTP-запросов) экранируем: иначе злонамеренный %0A в path подделывает строки
# журнала, а ANSI-эскейпы исполняются терминалом администратора.
_CTRL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def _safe(value) -> str:
    return _CTRL_RE.sub(lambda m: f"\\x{ord(m.group()):02x}", str(value))


def _fmt_event(e: dict) -> str:
    ts = _safe(e.get("ts", ""))[:19].replace("T", " ")
    kind = _safe(e.get("kind", "?"))
    if kind == "api":
        base = (
            f"{ts}  [api]  {_safe(e.get('actor') or '-'):<12} "
            f"{_safe(e.get('method', '')):<7} {_safe(e.get('path', ''))}  "
            f"→ {e.get('status')}  {e.get('dur_ms', 0)}мс"
        )
    elif kind == "pipeline":
        base = (
            f"{ts}  [pipeline]  {_safe(e.get('project_id', ''))}  "
            f"{_safe(e.get('stage', ''))} → {_safe(e.get('status', ''))}"
            + (f"  ({e.get('duration_sec')}с)" if e.get("duration_sec") else "")
        )
    elif kind == "app_log":
        base = (f"{ts}  [{_safe(e.get('level', ''))}]  "
                f"{_safe(e.get('logger', ''))}: {_safe(e.get('message', ''))[:160]}")
    else:
        rest = {k: v for k, v in e.items() if k not in ("ts", "kind")}
        base = f"{ts}  [{kind}]  {_safe(rest)}"
    err = e.get("error")
    if err:
        base += f"\n{'':>21}ОШИБКА: {_safe(err)[:300]}"
    return base


def main() -> int:
    parser = argparse.ArgumentParser(description="Анализ журнала действий")
    parser.add_argument("--days", type=int, default=7, help="глубина в днях (default 7)")
    parser.add_argument("--errors", action="store_true", help="показать только ошибки")
    parser.add_argument("--user", help="фильтр по логину портала")
    parser.add_argument("--kind", help="api | pipeline | app_log | system")
    parser.add_argument("--q", help="подстрока по событию")
    parser.add_argument("--limit", type=int, default=30, help="сколько событий показать")
    args = parser.parse_args()

    date_from = (date.today() - timedelta(days=args.days - 1)).isoformat()

    if args.errors or args.user or args.kind or args.q:
        result = action_log.read_events(
            date_from=date_from,
            kind=args.kind,
            actor=args.user,
            q=args.q,
            errors_only=args.errors,
            limit=args.limit,
        )
        items = result["items"]
        title = "ОШИБКИ" if args.errors else "СОБЫТИЯ"
        print(f"═══ {title} за {args.days} дн. "
              f"(показано {len(items)}{', есть ещё' if result['truncated'] else ''}) ═══\n")
        if not items:
            print("Ничего не найдено.")
        for e in items:
            print(_fmt_event(e))
        return 0

    # Режим сводки
    summary = action_log.stats(days=args.days)
    totals = summary["totals"]
    print(f"═══ СВОДКА ЖУРНАЛА ДЕЙСТВИЙ за {args.days} дн. ═══")
    print(f"Всего событий: {totals['events']}, из них ошибок: {totals['errors']}")
    print(f"По типам: {totals['by_kind']}\n")
    for row in summary["days"]:
        print(f"── {row['day']}: {row['total']} событий, ошибок {row['errors']} ──")
        if row["actors"]:
            actors = ", ".join(f"{a} ({n})" for a, n in row["actors"].items())
            print(f"   Инженеры: {actors}")
        if row["pipeline_errors"]:
            print(f"   Падения конвейера: {row['pipeline_errors']}")
        for p, n in row["top_paths"][:5]:
            print(f"   {n:>5}  {p}")
    if totals["errors"]:
        print("\nПодробности ошибок: python scripts/analyze_action_log.py --errors")
    return 0


if __name__ == "__main__":
    sys.exit(main())
