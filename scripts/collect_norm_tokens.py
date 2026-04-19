"""Собирает token usage по norm_verify для 3 benchmark-проектов:
   - новые прогоны (2026-04-18, benchmark lite)
   - старые historical прогоны (по таймингам из pipeline_log.json)

Источник: ~/.claude/projects/-home-coder-projects-PDF-proverka/*.jsonl
Отдельно не разделяет стадии, если проект был в одной и той же Claude session с
другими этапами — такой проект помечается comparison_unreliable.
"""
from __future__ import annotations
import json, os, sys, statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path

SESSIONS_DIR = Path("/home/coder/.claude/projects/-home-coder-projects-PDF-proverka")
REPORTS = Path("/home/coder/projects/PDF-proverka/reports")

# ─── Конфигурация: 6 прогонов ────────────────────────────────────────────
# Все временные окна — в локальной MSK-зоне. JSONL timestamps — в UTC (Z).
# Для сравнения приводим локальное к UTC через offset(3h).
LOCAL_OFFSET = timedelta(hours=5)  # машина в UTC+5 (проверено date +%z)


def _to_utc(local_iso: str) -> datetime:
    dt = datetime.fromisoformat(local_iso)
    return (dt - LOCAL_OFFSET).replace(tzinfo=timezone.utc)


RUNS = [
    # ─── NEW (2026-04-18 benchmark lite) ───────────────────────────────
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/213. Мосфильмовская 31А "King&Sons"/AI/133-23-ГК-АИ2',
        "project_id": "AI/133-23-ГК-АИ2",
        "run_type": "new",
        "start_local": "2026-04-18T12:31:10",
        "end_local":   "2026-04-18T12:46:37",
        "prompt_signature": "NORMATIVE QUOTE VERIFICATION",
    },
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)',
        "project_id": "AR/13АВ-РД-АР1.1-К4 (Изм.2)",
        "run_type": "new",
        "start_local": "2026-04-18T12:46:37",
        "end_local":   "2026-04-18T12:50:31",
        "prompt_signature": "NORMATIVE QUOTE VERIFICATION",
    },
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/213. Мосфильмовская 31А "King&Sons"/OV/133_23-ГК-ОВ1.2',
        "project_id": "OV/133_23-ГК-ОВ1.2",
        "run_type": "new",
        "start_local": "2026-04-18T12:50:31",
        "end_local":   "2026-04-18T12:59:36",
        "prompt_signature": "NORMATIVE QUOTE VERIFICATION",
    },
    # ─── OLD (historical из pipeline_log.json) ────────────────────────
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/213. Мосфильмовская 31А "King&Sons"/AI/133-23-ГК-АИ2',
        "project_id": "AI/133-23-ГК-АИ2",
        "run_type": "old",
        "start_local": "2026-03-16T13:10:18",
        "end_local":   "2026-03-16T13:23:41",
        "prompt_signature": "NORMATIVE REFERENCE VERIFICATION",
    },
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/AR/13АВ-РД-АР1.1-К4 (Изм.2)',
        "project_id": "AR/13АВ-РД-АР1.1-К4 (Изм.2)",
        "run_type": "old",
        "start_local": "2026-04-16T05:34:11",
        "end_local":   "2026-04-16T06:00:01",
        "prompt_signature": "NORMATIVE REFERENCE VERIFICATION",
    },
    {
        "project_path": '/home/coder/projects/PDF-proverka/projects/213. Мосфильмовская 31А "King&Sons"/OV/133_23-ГК-ОВ1.2',
        "project_id": "OV/133_23-ГК-ОВ1.2",
        "run_type": "old",
        "start_local": "2026-03-21T23:17:08",
        "end_local":   "2026-03-21T23:29:04",
        "prompt_signature": "NORMATIVE REFERENCE VERIFICATION",
    },
]


def _parse_ts(s):
    """Parse Claude JSONL timestamp (UTC, ISO with trailing Z)."""
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def find_session_candidates(run, margin_min=3):
    """Возвращает список JSONL-кандидатов для этого прогона:
    файлы с timestamps в расширенном окне [start-margin, end+margin]."""
    start_utc = _to_utc(run["start_local"]) - timedelta(minutes=margin_min)
    end_utc   = _to_utc(run["end_local"])   + timedelta(minutes=margin_min)
    out = []
    for p in SESSIONS_DIR.glob("*.jsonl"):
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        # быстрый prefilter по mtime
        if mtime < start_utc - timedelta(hours=2) or mtime > end_utc + timedelta(hours=2):
            continue
        out.append(p)
    return out


def match_session(path, run):
    """Читает JSONL и возвращает dict с метриками если сессия соответствует
    этому прогону (project_id, окно, сигнатура), иначе None."""
    pid_marker_candidates = [run["project_id"]]
    # раньше project_id мог записываться как "АИ2" вместо "АИ2" — все варианты уже в строке
    # добавим базовый short-id без подпапки дисциплины:
    if "/" in run["project_id"]:
        pid_marker_candidates.append(run["project_id"].split("/", 1)[1])

    first_ts = None
    last_ts  = None
    found_project = False
    found_signature = False if run["prompt_signature"] else True
    model = None
    in_tot = out_tot = cache_creation = cache_read = 0
    tool_counts: dict[str, int] = {}
    assistant_turns = 0
    all_tool_names: set[str] = set()

    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                ts = _parse_ts(rec.get("timestamp"))
                if ts:
                    if first_ts is None or ts < first_ts:
                        first_ts = ts
                    if last_ts is None or ts > last_ts:
                        last_ts = ts

                # Поиск project_id и signature в content
                content = rec.get("content")
                if isinstance(content, str):
                    if not found_project:
                        for pm in pid_marker_candidates:
                            if pm in content:
                                found_project = True
                                break
                    if not found_signature:
                        if run["prompt_signature"] and run["prompt_signature"] in content:
                            found_signature = True

                # content может быть внутри message.content (list of blocks)
                msg = rec.get("message") or {}
                if isinstance(msg, dict):
                    mc = msg.get("content")
                    if isinstance(mc, list):
                        for block in mc:
                            if isinstance(block, dict):
                                txt = block.get("text") or ""
                                if isinstance(txt, str):
                                    if not found_project:
                                        for pm in pid_marker_candidates:
                                            if pm in txt:
                                                found_project = True
                                                break
                                    if not found_signature and run["prompt_signature"]:
                                        if run["prompt_signature"] in txt:
                                            found_signature = True
                                # tool_use
                                if block.get("type") == "tool_use":
                                    name = block.get("name")
                                    if name:
                                        all_tool_names.add(name)
                                        tool_counts[name] = tool_counts.get(name, 0) + 1

                # usage
                if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
                    u = msg["usage"]
                    in_tot += int(u.get("input_tokens") or 0)
                    out_tot += int(u.get("output_tokens") or 0)
                    cache_creation += int(u.get("cache_creation_input_tokens") or 0)
                    cache_read += int(u.get("cache_read_input_tokens") or 0)
                    if msg.get("role") == "assistant":
                        assistant_turns += 1
                    if not model:
                        model = msg.get("model")
    except Exception as e:
        return {"error": f"read failed: {e}"}

    return {
        "path": str(path),
        "first_ts": first_ts.isoformat() if first_ts else None,
        "last_ts":  last_ts.isoformat() if last_ts else None,
        "found_project": found_project,
        "found_signature": found_signature,
        "model": model,
        "input_tokens": in_tot,
        "output_tokens": out_tot,
        "total_tokens": in_tot + out_tot,
        "cache_creation_input_tokens": cache_creation,
        "cache_read_input_tokens": cache_read,
        "tool_counts": tool_counts,
        "assistant_turns": assistant_turns,
        "all_tool_names": sorted(all_tool_names),
    }


def overlaps_window(run, sess_first, sess_last):
    if not sess_first or not sess_last:
        return False
    start_utc = _to_utc(run["start_local"])
    end_utc   = _to_utc(run["end_local"])
    first = datetime.fromisoformat(sess_first)
    last  = datetime.fromisoformat(sess_last)
    # Сессия хотя бы частично пересекается с окном
    return not (last < start_utc or first > end_utc)


def analyze_run(run):
    candidates = find_session_candidates(run, margin_min=5)
    matches = []
    for p in candidates:
        m = match_session(p, run)
        if not m or "error" in m:
            continue
        if not m["found_project"]:
            continue
        if not overlaps_window(run, m["first_ts"], m["last_ts"]):
            continue
        if run["prompt_signature"] and not m["found_signature"]:
            continue
        matches.append(m)
    return matches


def main():
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sessions_dir": str(SESSIONS_DIR),
        "runs": [],
    }
    for run in RUNS:
        matches = analyze_run(run)
        run_out = {
            "project_id": run["project_id"],
            "project_path": run["project_path"],
            "run_type": run["run_type"],
            "start_local": run["start_local"],
            "end_local": run["end_local"],
            "candidate_count": len(matches),
            "matches": matches,
        }
        # агрегат
        if matches:
            run_out["aggregate"] = {
                "input_tokens":  sum(m["input_tokens"]  for m in matches),
                "output_tokens": sum(m["output_tokens"] for m in matches),
                "total_tokens":  sum(m["total_tokens"]  for m in matches),
                "cache_creation_input_tokens": sum(m["cache_creation_input_tokens"] for m in matches),
                "cache_read_input_tokens":     sum(m["cache_read_input_tokens"] for m in matches),
                "assistant_turns":             sum(m["assistant_turns"] for m in matches),
                "tool_counts": {
                    name: sum(m["tool_counts"].get(name, 0) for m in matches)
                    for name in sorted({n for m in matches for n in m["tool_counts"]})
                },
                "models": sorted({m["model"] for m in matches if m["model"]}),
            }
        result["runs"].append(run_out)

    REPORTS.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS / "_tokens_raw_scan.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}")
    # краткий summary
    for r in result["runs"]:
        agg = r.get("aggregate") or {}
        print(f"[{r['run_type'].upper()}] {r['project_id']}: candidates={r['candidate_count']} total={agg.get('total_tokens')}")


if __name__ == "__main__":
    main()
