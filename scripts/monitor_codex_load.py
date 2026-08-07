#!/usr/bin/env python3
"""Монитор нагрузки на Codex при параллельном прогоне проектов.

Зачем
─────
BUDGET_CODEX_CLI поднят с 6 до 20 (06.08.2026), чтобы пять проектов не стояли
в очереди за шестью слотами. Риск ровно один: подписка Codex может ответить
на двадцать одновременных сессий отказом или деградацией. Беда в том, что
у codex-пути НЕТ retry на usage limit (в отличие от claude-пути с
`_wait_for_rate_limit`): исчерпание лимита приходит как exit!=0, нога ансамбля
молча выпадает из `detectors_ok`, и стадия завершается «успешно», просто с
меньшим числом находок. Глазами это не видно — нужен монитор.

Что меряем
──────────
1. Фактическая одновременность `codex exec` — подтверждает, что новый бюджет
   реально используется (если пик так и держится на 6, правка не доехала).
2. Выпавшие ноги ансамбля — `detectors_failed` в пер-блочных отчётах.
   Это прямой индикатор «штрафа» от провайдера.
3. `reasoning_tokens` на ногу — прямая мера «уровня мышления». Если провайдер
   под нагрузкой начнёт думать меньше, медиана просядет относительно базы.
4. `elapsed_ms` на ногу — троттлинг проявляется ростом задержки раньше,
   чем отказами.

Использование
─────────────
    # 1) ДО прогона: снять базу по историческим отчётам
    python scripts/monitor_codex_load.py baseline

    # 2) ВО ВРЕМЯ прогона: живое наблюдение, сравнение с базой
    python scripts/monitor_codex_load.py watch

    # 3) ПОСЛЕ прогона: разовый отчёт по всему, что появилось после базы
    python scripts/monitor_codex_load.py report

Скрипт только читает. Ничего не пишет в данные проектов — свой снимок кладёт
в logs/codex_load_baseline.json.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
PROJECTS_DIR = ROOT / "projects_v2"
BASELINE_PATH = ROOT / "logs" / "codex_load_baseline.json"

# Пер-блочные отчёты этапа 01 (см. gemma_findings_only.py: run_dir / block_<id>.json)
BLOCK_REPORT_GLOB = "**/_stage01_findings_only_runs/*/block_*.json"

# Признаки отказа провайдера в тексте ошибки/логах.
PROVIDER_LIMIT_RE = re.compile(
    r"usage limit|rate.?limit|429|too many requests|quota|overloaded|capacity",
    re.IGNORECASE,
)

# Насколько медиана reasoning_tokens может просесть, прежде чем это тревога.
REASONING_DROP_ALERT_PCT = 30.0
# Насколько может вырасти медианная задержка, прежде чем это тревога.
LATENCY_GROWTH_ALERT_PCT = 60.0


# ─── Живая одновременность codex ──────────────────────────────────────


def live_codex_processes() -> int:
    """Сколько процессов `codex exec` крутится прямо сейчас."""
    try:
        out = subprocess.run(
            ["ps", "-eo", "args"], capture_output=True, text=True, timeout=10
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return -1
    count = 0
    for line in out.splitlines():
        # Ищем именно исполнение стадии, а не наш ps и не редактор.
        if "/codex" in line and " exec" in line and "monitor_codex_load" not in line:
            count += 1
    return count


# ─── Разбор пер-блочных отчётов ───────────────────────────────────────


@dataclass
class LegStats:
    """Накопитель по одной ноге ансамбля (одной модели)."""

    model: str
    ok: int = 0
    failed: int = 0
    reasoning: list[int] = field(default_factory=list)
    elapsed: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.ok + self.failed

    def summary(self) -> dict:
        return {
            "model": self.model,
            "blocks": self.total,
            "ok": self.ok,
            "failed": self.failed,
            "fail_pct": round(100.0 * self.failed / self.total, 1) if self.total else 0.0,
            "reasoning_median": int(statistics.median(self.reasoning)) if self.reasoning else 0,
            "reasoning_mean": int(statistics.fmean(self.reasoning)) if self.reasoning else 0,
            "elapsed_median_ms": int(statistics.median(self.elapsed)) if self.elapsed else 0,
            "provider_limit_hits": sum(1 for e in self.errors if PROVIDER_LIMIT_RE.search(e)),
            "sample_errors": self.errors[:5],
        }


def iter_block_reports(since_mtime: float = 0.0) -> Iterable[tuple[Path, dict]]:
    """Пер-блочные отчёты этапа 01, изменённые позже since_mtime."""
    if not PROJECTS_DIR.exists():
        return
    for path in PROJECTS_DIR.glob(BLOCK_REPORT_GLOB):
        try:
            if path.stat().st_mtime <= since_mtime:
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict):
            yield path, data


def collect(since_mtime: float = 0.0) -> dict:
    """Свести статистику по ногам ансамбля из отчётов новее since_mtime."""
    legs: dict[str, LegStats] = {}
    blocks = 0
    blocks_partial = 0
    newest = since_mtime
    for path, data in iter_block_reports(since_mtime):
        result = data.get("result") or {}
        if not isinstance(result, dict):
            continue
        blocks += 1
        newest = max(newest, path.stat().st_mtime)
        if result.get("partial"):
            blocks_partial += 1

        # Ноги, о которых стадия знает, что они не ответили, могут вообще
        # отсутствовать в detector_results — считаем их отдельно по имени.
        for model in result.get("detectors_failed") or []:
            leg = legs.setdefault(str(model), LegStats(str(model)))
            leg.failed += 1
            err = str(result.get("error") or result.get("parse_error") or "")
            if err:
                leg.errors.append(err[:300])

        for entry in result.get("detector_results") or []:
            if not isinstance(entry, dict):
                continue
            model = str(entry.get("model") or "?")
            res = entry.get("result") or {}
            if not isinstance(res, dict):
                continue
            leg = legs.setdefault(model, LegStats(model))
            if res.get("ok"):
                leg.ok += 1
            else:
                leg.failed += 1
                err = str(res.get("error") or res.get("parse_error") or "")
                if err:
                    leg.errors.append(err[:300])
            rt = res.get("reasoning_tokens")
            if isinstance(rt, int) and rt > 0:
                leg.reasoning.append(rt)
            el = res.get("elapsed_ms")
            if isinstance(el, int) and el > 0:
                leg.elapsed.append(el)

    return {
        "blocks": blocks,
        "blocks_partial": blocks_partial,
        "newest_mtime": newest,
        "legs": {name: leg.summary() for name, leg in sorted(legs.items())},
    }


# ─── Логи сервера ─────────────────────────────────────────────────────


def scan_server_log(tail_bytes: int = 2_000_000) -> list[str]:
    """Строки хвоста server.log, похожие на отказ провайдера."""
    log = ROOT / "logs" / "server.log"
    if not log.exists():
        return []
    try:
        size = log.stat().st_size
        with log.open("rb") as fh:
            if size > tail_bytes:
                fh.seek(size - tail_bytes)
            chunk = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    hits = [ln.strip() for ln in chunk.splitlines() if PROVIDER_LIMIT_RE.search(ln)]
    return hits[-20:]


# ─── Вывод ────────────────────────────────────────────────────────────


def print_legs(stats: dict, baseline: dict | None) -> list[str]:
    """Печать таблицы по ногам. Возвращает список тревог."""
    alerts: list[str] = []
    base_legs = (baseline or {}).get("legs") or {}
    legs = stats.get("legs") or {}
    if not legs:
        print("  (пер-блочных отчётов пока нет)")
        return alerts

    header = f"  {'модель':<22} {'блоков':>7} {'сбоев':>7} {'думал':>9} {'задержка':>10}  {'к базе':<28}"
    print(header)
    print("  " + "─" * (len(header) - 2))
    for name, s in legs.items():
        base = base_legs.get(name)
        note = "—"
        if base and base.get("reasoning_median") and s.get("reasoning_median"):
            d_reason = 100.0 * (s["reasoning_median"] - base["reasoning_median"]) / base["reasoning_median"]
            d_lat = 0.0
            if base.get("elapsed_median_ms") and s.get("elapsed_median_ms"):
                d_lat = 100.0 * (s["elapsed_median_ms"] - base["elapsed_median_ms"]) / base["elapsed_median_ms"]
            note = f"думал {d_reason:+.0f}%, задержка {d_lat:+.0f}%"
            if d_reason <= -REASONING_DROP_ALERT_PCT:
                alerts.append(
                    f"{name}: медиана reasoning_tokens просела на {abs(d_reason):.0f}% "
                    f"({base['reasoning_median']} → {s['reasoning_median']}) — похоже на понижение уровня мышления"
                )
            if d_lat >= LATENCY_GROWTH_ALERT_PCT:
                alerts.append(
                    f"{name}: медианная задержка выросла на {d_lat:.0f}% "
                    f"({base['elapsed_median_ms']} → {s['elapsed_median_ms']} мс) — похоже на троттлинг"
                )
        print(
            f"  {name:<22} {s['blocks']:>7} {s['failed']:>7} "
            f"{s['reasoning_median']:>9} {s['elapsed_median_ms']:>9}мс  {note:<28}"
        )
        if s["failed"]:
            alerts.append(f"{name}: {s['failed']} блоков без ответа ({s['fail_pct']}%) — нога выпадала из ансамбля")
        if s["provider_limit_hits"]:
            alerts.append(f"{name}: {s['provider_limit_hits']} ошибок с признаком лимита провайдера")
    return alerts


def cmd_baseline() -> int:
    print("Снимаю базу по историческим отчётам этапа 01 (это «до» изменения)...")
    stats = collect(0.0)
    stats["captured_at"] = time.time()
    stats["note"] = "база снята до подъёма BUDGET_CODEX_CLI"
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    BASELINE_PATH.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Блоков в базе: {stats['blocks']}")
    print_legs(stats, None)
    print(f"\nСохранено: {BASELINE_PATH.relative_to(ROOT)}")
    return 0


def load_baseline() -> dict | None:
    if not BASELINE_PATH.exists():
        return None
    try:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def cmd_report() -> int:
    baseline = load_baseline()
    if baseline is None:
        print("Базы нет. Сначала: python scripts/monitor_codex_load.py baseline")
        return 1
    since = float(baseline.get("newest_mtime") or 0.0)
    stats = collect(since)
    print(f"Блоков после снятия базы: {stats['blocks']} (частичных: {stats['blocks_partial']})")
    alerts = print_legs(stats, baseline)
    hits = scan_server_log()
    if hits:
        alerts.append(f"в server.log {len(hits)} строк с признаком лимита провайдера")
        print("\nПоследние подозрительные строки server.log:")
        for h in hits[-5:]:
            print("   ", h[:200])
    print()
    if alerts:
        print("ТРЕВОГИ:")
        for a in alerts:
            print("  ⚠ ", a)
    else:
        print("Тревог нет: сбоев ног, просадки reasoning и следов лимита не обнаружено.")
    return 0


def cmd_watch(interval: int) -> int:
    baseline = load_baseline()
    since = float((baseline or {}).get("newest_mtime") or 0.0)
    if baseline is None:
        print("Базы нет — сравнивать не с чем, показываю только абсолютные значения.")
        print("Рекомендую прервать и снять базу: python scripts/monitor_codex_load.py baseline\n")
    peak = 0
    samples: list[int] = []
    print(f"Наблюдение каждые {interval} с. Ctrl+C — выход.\n")
    try:
        while True:
            live = live_codex_processes()
            if live >= 0:
                samples.append(live)
                peak = max(peak, live)
            stats = collect(since)
            print("\033[2J\033[H", end="")  # очистить экран
            print(f"=== Монитор Codex — {time.strftime('%H:%M:%S')} ===\n")
            line = f"  codex exec сейчас: {live}   пик за сеанс: {peak}"
            if samples:
                line += f"   среднее: {statistics.fmean(samples):.1f}"
            print(line)
            if peak <= 6 and len(samples) > 10:
                print("  ⚠  пик так и не превысил 6 — возможно, правка BUDGET_CODEX_CLI не доехала "
                      "(нужен рестарт бэкенда) либо просто нет нагрузки")
            print(f"\n  Новых блоков с момента базы: {stats['blocks']}\n")
            alerts = print_legs(stats, baseline)
            hits = scan_server_log()
            if hits:
                alerts.append(f"в server.log {len(hits)} строк с признаком лимита провайдера")
            print()
            if alerts:
                print("  ТРЕВОГИ:")
                for a in alerts:
                    print("   ⚠ ", a)
            else:
                print("  Тревог нет.")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n\nИтог сеанса наблюдения:")
        print(f"  пик одновременных codex exec: {peak}")
        return cmd_report()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["baseline", "watch", "report"])
    ap.add_argument("--interval", type=int, default=20, help="период опроса в режиме watch, с")
    args = ap.parse_args()
    if args.mode == "baseline":
        return cmd_baseline()
    if args.mode == "report":
        return cmd_report()
    return cmd_watch(args.interval)


if __name__ == "__main__":
    sys.exit(main())
