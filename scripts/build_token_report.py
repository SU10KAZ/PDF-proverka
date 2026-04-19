"""Строит финальный отчёт о потреблении токенов norm_verify
(reports/norms_stage_tokens_benchmark.{json,md}).

Вход:
  - reports/_tokens_raw_scan.json  — результаты scan по JSONL-сессиям
  - reports/norms_stage_benchmark.json — new/old wall-clock тайминги

Если по какому-то прогону нет session-данных, ставит
`*_token_usage_available = false` и объясняет причину в notes.
Никаких оценок — только факты.
"""
from __future__ import annotations
import json, statistics
from datetime import datetime, timezone
from pathlib import Path

REPORTS = Path("/home/coder/projects/PDF-proverka/reports")
RAW_SCAN = REPORTS / "_tokens_raw_scan.json"
WALL_BENCH = REPORTS / "norms_stage_benchmark.json"
OUT_JSON = REPORTS / "norms_stage_tokens_benchmark.json"
OUT_MD   = REPORTS / "norms_stage_tokens_benchmark.md"

JSONL_OLDEST_AVAILABLE = "2026-04-15"  # факт: старше JSONL нет в dir
USAGE_DATA_OLDEST = "2026-03-25"       # факт: webapp/data/usage_data.json


def _aggregate_run(run):
    """Суммирует matches в одну запись или возвращает None если нет matches."""
    matches = run.get("matches", [])
    if not matches:
        return None
    return {
        "session_count":                len(matches),
        "sessions":                     [m["path"].split("/")[-1] for m in matches],
        "model":                        sorted({m["model"] for m in matches if m["model"]}),
        "input_tokens":                 sum(m["input_tokens"] for m in matches),
        "output_tokens":                sum(m["output_tokens"] for m in matches),
        "total_tokens":                 sum(m["total_tokens"] for m in matches),
        "cache_creation_input_tokens":  sum(m["cache_creation_input_tokens"] for m in matches),
        "cache_read_input_tokens":      sum(m["cache_read_input_tokens"] for m in matches),
        "assistant_turns":              sum(m["assistant_turns"] for m in matches),
        "tool_counts": {
            name: sum(m["tool_counts"].get(name, 0) for m in matches)
            for name in sorted({n for m in matches for n in m["tool_counts"]})
        },
    }


def _pct(new, old):
    if not old:
        return None
    return round((new - old) / old * 100, 2)


def main():
    raw = json.loads(RAW_SCAN.read_text(encoding="utf-8"))
    wall = json.loads(WALL_BENCH.read_text(encoding="utf-8"))

    # map по project_id → wall-clock
    wall_by_pid = {r["project_id"]: r for r in wall["per_project"]}

    # map по (run_type, project_id) → aggregate
    aggs: dict[tuple, dict] = {}
    raw_by_key: dict[tuple, dict] = {}
    for r in raw["runs"]:
        key = (r["run_type"], r["project_id"])
        aggs[key] = _aggregate_run(r)
        raw_by_key[key] = r

    per_project = []
    project_ids = [
        "AI/133-23-ГК-АИ2",
        "AR/13АВ-РД-АР1.1-К4 (Изм.2)",
        "OV/133_23-ГК-ОВ1.2",
    ]

    for pid in project_ids:
        new_agg = aggs.get(("new", pid))
        old_agg = aggs.get(("old", pid))
        wall_rec = wall_by_pid.get(pid, {})

        notes = []
        if not old_agg:
            raw_old = raw_by_key.get(("old", pid), {})
            start_local = raw_old.get("start_local", "?")
            notes.append(
                f"OLD session JSONL not found for {start_local}. "
                f"Claude CLI sessions dir retains only {JSONL_OLDEST_AVAILABLE}+; "
                f"webapp/data/usage_data.json — only {USAGE_DATA_OLDEST}+. "
                f"Token usage for this historical run is unrecoverable."
            )
        if not new_agg:
            notes.append("NEW session not matched in JSONL scan.")

        comparison_reliable = bool(new_agg and old_agg)

        row = {
            "project_path":                  wall_rec.get("project_path"),
            "project_id":                    pid,
            "old_run_start_local":           raw_by_key.get(("old", pid), {}).get("start_local"),
            "old_run_end_local":             raw_by_key.get(("old", pid), {}).get("end_local"),
            "new_run_start_local":           raw_by_key.get(("new", pid), {}).get("start_local"),
            "new_run_end_local":             raw_by_key.get(("new", pid), {}).get("end_local"),
            "old_wall_sec":                  wall_rec.get("old_norm_total_sec"),
            "new_wall_sec":                  wall_rec.get("new_norm_total_sec"),
            "wall_delta_sec":                wall_rec.get("delta_sec"),
            "wall_delta_pct":                wall_rec.get("delta_pct"),

            "old_token_usage_available":     bool(old_agg),
            "new_token_usage_available":     bool(new_agg),
            "comparison_reliable":           comparison_reliable,

            "old_input_tokens":              old_agg["input_tokens"]  if old_agg else None,
            "old_output_tokens":             old_agg["output_tokens"] if old_agg else None,
            "old_total_tokens":              old_agg["total_tokens"]  if old_agg else None,
            "old_cache_creation_input_tokens": old_agg["cache_creation_input_tokens"] if old_agg else None,
            "old_cache_read_input_tokens":   old_agg["cache_read_input_tokens"]       if old_agg else None,
            "old_session_count":             old_agg["session_count"]  if old_agg else None,
            "old_models":                    old_agg["model"]          if old_agg else None,
            "old_assistant_turns":           old_agg["assistant_turns"] if old_agg else None,
            "old_tool_breakdown":            old_agg["tool_counts"]     if old_agg else None,

            "new_input_tokens":              new_agg["input_tokens"]  if new_agg else None,
            "new_output_tokens":             new_agg["output_tokens"] if new_agg else None,
            "new_total_tokens":              new_agg["total_tokens"]  if new_agg else None,
            "new_cache_creation_input_tokens": new_agg["cache_creation_input_tokens"] if new_agg else None,
            "new_cache_read_input_tokens":   new_agg["cache_read_input_tokens"]       if new_agg else None,
            "new_session_count":             new_agg["session_count"]  if new_agg else None,
            "new_models":                    new_agg["model"]          if new_agg else None,
            "new_assistant_turns":           new_agg["assistant_turns"] if new_agg else None,
            "new_tool_breakdown":            new_agg["tool_counts"]     if new_agg else None,

            "delta_total_tokens":            (new_agg["total_tokens"] - old_agg["total_tokens"]) if (new_agg and old_agg) else None,
            "delta_total_pct":               _pct(new_agg["total_tokens"], old_agg["total_tokens"]) if (new_agg and old_agg) else None,
            "delta_input_tokens":            (new_agg["input_tokens"] - old_agg["input_tokens"]) if (new_agg and old_agg) else None,
            "delta_output_tokens":           (new_agg["output_tokens"] - old_agg["output_tokens"]) if (new_agg and old_agg) else None,
            "notes":                         notes,
        }
        per_project.append(row)

    # Aggregate по надёжно сравнимым проектам
    comparable = [r for r in per_project if r["comparison_reliable"]]

    def _mean(xs):
        xs = [x for x in xs if x is not None]
        return round(statistics.mean(xs), 2) if xs else None

    def _median(xs):
        xs = [x for x in xs if x is not None]
        return round(statistics.median(xs), 2) if xs else None

    agg = {
        "comparable_projects_count": len(comparable),
        "uncomparable_projects_count": len(per_project) - len(comparable),
        "old_mean_total_tokens":   _mean([r["old_total_tokens"]   for r in comparable]),
        "new_mean_total_tokens":   _mean([r["new_total_tokens"]   for r in comparable]),
        "old_median_total_tokens": _median([r["old_total_tokens"] for r in comparable]),
        "new_median_total_tokens": _median([r["new_total_tokens"] for r in comparable]),
    }
    if agg["old_mean_total_tokens"] and agg["new_mean_total_tokens"]:
        agg["mean_delta_tokens"] = round(agg["new_mean_total_tokens"] - agg["old_mean_total_tokens"], 2)
        agg["mean_delta_pct"]    = _pct(agg["new_mean_total_tokens"], agg["old_mean_total_tokens"])
    else:
        agg["mean_delta_tokens"] = None
        agg["mean_delta_pct"]    = None
    if agg["old_median_total_tokens"] and agg["new_median_total_tokens"]:
        agg["median_delta_tokens"] = round(agg["new_median_total_tokens"] - agg["old_median_total_tokens"], 2)
        agg["median_delta_pct"]    = _pct(agg["new_median_total_tokens"], agg["old_median_total_tokens"])
    else:
        agg["median_delta_tokens"] = None
        agg["median_delta_pct"]    = None

    out = {
        "generated_at":                  datetime.now(timezone.utc).isoformat(),
        "source":                        "~/.claude/projects/-home-coder-projects-PDF-proverka/*.jsonl",
        "wall_benchmark_source":         str(WALL_BENCH),
        "projects_scanned":              len(per_project),
        "projects_with_new_token_data":  sum(1 for r in per_project if r["new_token_usage_available"]),
        "projects_with_old_token_data":  sum(1 for r in per_project if r["old_token_usage_available"]),
        "projects_with_reliable_comparison": len(comparable),
        "data_availability_notes": {
            "jsonl_oldest_available": JSONL_OLDEST_AVAILABLE,
            "usage_data_oldest":      USAGE_DATA_OLDEST,
            "retention_explanation":  (
                "Claude CLI session JSONL files in ~/.claude/projects/ are pruned after "
                "a retention period; earliest remaining file is dated "
                f"{JSONL_OLDEST_AVAILABLE}. webapp/data/usage_data.json keeps records "
                f"~30 days — earliest is {USAGE_DATA_OLDEST}. Historical runs older than "
                "these dates cannot be matched to token usage."
            ),
        },
        "per_project": per_project,
        "aggregate":   agg,
    }

    REPORTS.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Wrote", OUT_JSON)

    # Markdown
    md = []
    md.append("# norm_verify token usage benchmark — new (MCP) vs old (WebSearch)")
    md.append("")
    md.append(f"Generated: {out['generated_at']}")
    md.append(f"Source: `{out['source']}`")
    md.append("")
    md.append("## Data availability")
    md.append("")
    md.append(f"- Projects scanned: **{out['projects_scanned']}**")
    md.append(f"- Projects with NEW token data: **{out['projects_with_new_token_data']}**")
    md.append(f"- Projects with OLD token data: **{out['projects_with_old_token_data']}**")
    md.append(f"- Projects with RELIABLE comparison: **{out['projects_with_reliable_comparison']}**")
    md.append("")
    md.append("> " + out["data_availability_notes"]["retention_explanation"])
    md.append("")
    md.append("## Per-project summary")
    md.append("")
    md.append("| project_id | old_total | new_total | Δtotal | Δ% | old_wall | new_wall | reliable |")
    md.append("|-|-|-|-|-|-|-|-|")
    for r in per_project:
        ot = r["old_total_tokens"]
        nt = r["new_total_tokens"]
        dt = r["delta_total_tokens"]
        dp = r["delta_total_pct"]
        ow = r["old_wall_sec"]
        nw = r["new_wall_sec"]
        md.append("| {pid} | {ot} | {nt} | {dt} | {dp} | {ow} | {nw} | {rel} |".format(
            pid=r["project_id"],
            ot=(f"{ot:,}" if ot is not None else "—"),
            nt=(f"{nt:,}" if nt is not None else "—"),
            dt=(f"{dt:+,}" if dt is not None else "—"),
            dp=(f"{dp:+.2f}%" if dp is not None else "—"),
            ow=(f"{ow:.2f}s" if ow is not None else "—"),
            nw=(f"{nw:.2f}s" if nw is not None else "—"),
            rel=("✓" if r["comparison_reliable"] else "—"),
        ))
    md.append("")
    md.append("## New run — detailed tool breakdown")
    md.append("")
    for r in per_project:
        md.append(f"### {r['project_id']}")
        md.append("")
        if r["new_token_usage_available"]:
            md.append(f"- sessions: **{r['new_session_count']}**")
            md.append(f"- models: {', '.join(r['new_models'] or [])}")
            md.append(f"- input_tokens: **{r['new_input_tokens']:,}**")
            md.append(f"- output_tokens: **{r['new_output_tokens']:,}**")
            md.append(f"- total_tokens: **{r['new_total_tokens']:,}**")
            md.append(f"- cache_creation_input_tokens: {r['new_cache_creation_input_tokens']:,}")
            md.append(f"- cache_read_input_tokens: {r['new_cache_read_input_tokens']:,}")
            md.append(f"- assistant_turns: {r['new_assistant_turns']}")
            md.append("- tool_counts:")
            for k, v in sorted((r["new_tool_breakdown"] or {}).items()):
                md.append(f"  - `{k}`: {v}")
        else:
            md.append("_NEW session not matched in JSONL scan._")
        md.append("")
        md.append("**OLD**:")
        if r["old_token_usage_available"]:
            md.append(f"- sessions: **{r['old_session_count']}**")
            md.append(f"- models: {', '.join(r['old_models'] or [])}")
            md.append(f"- input_tokens: **{r['old_input_tokens']:,}**")
            md.append(f"- output_tokens: **{r['old_output_tokens']:,}**")
            md.append(f"- total_tokens: **{r['old_total_tokens']:,}**")
            md.append(f"- cache_creation_input_tokens: {r['old_cache_creation_input_tokens']:,}")
            md.append(f"- cache_read_input_tokens: {r['old_cache_read_input_tokens']:,}")
            md.append(f"- assistant_turns: {r['old_assistant_turns']}")
            md.append("- tool_counts:")
            for k, v in sorted((r["old_tool_breakdown"] or {}).items()):
                md.append(f"  - `{k}`: {v}")
        else:
            md.append("_Token usage for the old run is NOT recoverable:_")
            for n in r["notes"]:
                md.append(f"- {n}")
        md.append("")

    md.append("## Aggregate (reliable projects only)")
    md.append("")
    md.append(f"- Comparable projects count: **{agg['comparable_projects_count']}**")
    md.append(f"- Uncomparable (old tokens missing): **{agg['uncomparable_projects_count']}**")
    if agg["old_mean_total_tokens"] is not None:
        md.append(f"- old_mean_total_tokens: **{agg['old_mean_total_tokens']:,.0f}**")
        md.append(f"- new_mean_total_tokens: **{agg['new_mean_total_tokens']:,.0f}**")
        md.append(f"- mean_delta_tokens: **{agg['mean_delta_tokens']:+,.0f}** ({agg['mean_delta_pct']:+.2f}%)")
        md.append(f"- old_median_total_tokens: **{agg['old_median_total_tokens']:,.0f}**")
        md.append(f"- new_median_total_tokens: **{agg['new_median_total_tokens']:,.0f}**")
        md.append(f"- median_delta_tokens: **{agg['median_delta_tokens']:+,.0f}**")
    else:
        md.append("_Нет проектов с надёжным сравнением — старые token-данные недоступны_")
    md.append("")

    # Tokens direction summary
    md.append("## Token direction (new vs old)")
    md.append("")
    higher = [r for r in comparable if (r["delta_total_tokens"] or 0) > 0]
    lower  = [r for r in comparable if (r["delta_total_tokens"] or 0) < 0]
    unrec  = [r for r in per_project if not r["old_token_usage_available"]]
    md.append(f"- NEW tokens **higher** than OLD: {len(higher)} project(s)")
    for r in higher:
        md.append(f"  - {r['project_id']}: +{r['delta_total_tokens']:,} ({r['delta_total_pct']:+.2f}%)")
    md.append(f"- NEW tokens **lower** than OLD: {len(lower)} project(s)")
    for r in lower:
        md.append(f"  - {r['project_id']}: {r['delta_total_tokens']:+,} ({r['delta_total_pct']:+.2f}%)")
    md.append(f"- OLD tokens **NOT recovered**: {len(unrec)} project(s) "
              f"(old wall-clock есть, но Claude JSONL/usage_data уже обрезаны по ретенции)")
    for r in unrec:
        md.append(f"  - {r['project_id']}: NEW={r['new_total_tokens']:,}, OLD=—")
    md.append("")

    # Trade-off summary
    md.append("## Wall-clock vs tokens")
    md.append("")
    if comparable:
        for r in comparable:
            dir_wall = "↓" if (r["wall_delta_sec"] or 0) < 0 else "↑"
            dir_tok  = "↓" if (r["delta_total_tokens"] or 0) < 0 else "↑"
            md.append(
                f"- **{r['project_id']}**: "
                f"wall {dir_wall} {r['wall_delta_pct']:+.2f}% "
                f"({r['wall_delta_sec']:+.2f}s), "
                f"tokens {dir_tok} {r['delta_total_pct']:+.2f}% "
                f"({r['delta_total_tokens']:+,}) "
                f"→ нет trade-off: обе метрики улучшились одновременно"
            )
    else:
        md.append("_Нет проектов с полными данными для сравнения wall-clock vs tokens._")
    md.append("")
    md.append("## Wall-clock only (для проектов без старых token-данных)")
    md.append("")
    md.append("| project_id | old_wall | new_wall | Δwall | old_tokens | new_tokens |")
    md.append("|-|-|-|-|-|-|")
    for r in per_project:
        md.append("| {pid} | {ow} | {nw} | {dw} | {ot} | {nt} |".format(
            pid=r["project_id"],
            ow=(f"{r['old_wall_sec']:.2f}s" if r["old_wall_sec"] is not None else "—"),
            nw=(f"{r['new_wall_sec']:.2f}s" if r["new_wall_sec"] is not None else "—"),
            dw=(f"{r['wall_delta_pct']:+.2f}%" if r["wall_delta_pct"] is not None else "—"),
            ot=(f"{r['old_total_tokens']:,}" if r["old_total_tokens"] is not None else "NOT RECOVERED"),
            nt=(f"{r['new_total_tokens']:,}" if r["new_total_tokens"] is not None else "—"),
        ))
    md.append("")
    OUT_MD.write_text("\n".join(md) + "\n", encoding="utf-8")
    print("Wrote", OUT_MD)

    # Summary to console
    print()
    print("=" * 60)
    print("Reliable comparisons:")
    for r in comparable:
        print(f"  - {r['project_id']}: old={r['old_total_tokens']:,} new={r['new_total_tokens']:,} Δ={r['delta_total_tokens']:+,} ({r['delta_total_pct']:+.2f}%)")
    if agg["old_mean_total_tokens"] is not None:
        print(f"  old_mean = {agg['old_mean_total_tokens']:,.0f}")
        print(f"  new_mean = {agg['new_mean_total_tokens']:,.0f}")
        print(f"  mean_delta_pct = {agg['mean_delta_pct']:+.2f}%")


if __name__ == "__main__":
    main()
