"""
LLM-judge: семантическое сравнение findings от 3+ источников.

Берёт per-block findings от нескольких моделей (Opus 4.6, GPT-5.4, Gemini),
вызывает claude-opus-4-7 через Claude CLI с promptом «найди семантические совпадения,
оцени уникальные findings как valuable/questionable/noise».

Аккумулирует:
  - precision/recall каждого источника относительно объединения
  - сколько уникальных valuable findings у каждого
  - общая оценка качества per-source

Артефакты в `<project>/_experiments/findings_judge/<ts>/`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_ROOT / ".env")

DEFAULT_PROJECT_DIR = _ROOT / "projects" / "214. Alia (ASTERUS)" / "KJ" / "13АВ-РД-КЖ5.1-К1К2 (2).pdf"
JUDGE_MODEL = "claude-opus-4-7"
CLAUDE_CLI_BIN = os.environ.get("CLAUDE_CLI_BIN", str(Path.home() / ".local" / "bin" / "claude"))


JUDGE_SYSTEM_PROMPT = """Ты — старший инженер КЖ (железобетонные конструкции) с экспертизой в нормах РФ (СП 63.13330, СП 28.13330, СП 70.13330, ГОСТ 21.501, ГОСТ 34028 и др.).

Тебе дают findings (замечания) от нескольких моделей по одному и тому же блоку чертежа КЖ. Твоя задача — провести **семантическое сравнение**:

1. Найди пары findings, которые семантически про **одно и то же** (даже если формулировка разная). Пример: "защитный слой не указан" в одной модели и "z.s. не задан числом" в другой — одно и то же.
2. Для каждого finding, у которого НЕТ пары в других источниках, оцени реальную ценность:
   - **valuable** — настоящая нормативная или конструктивная проблема, инженер должен это знать
   - **questionable** — спорно: либо мелкое формальное замечание, либо вывод сделан с натяжкой, либо не критично
   - **noise** — ложное срабатывание, бессодержательное «не указано X», или дублирование того, что уже сказано в общих указаниях

3. В конце дай суммарную оценку — какой источник дал лучший набор findings для этого блока.

Output: запиши результат через Write tool в файл, путь которого указан в задаче.
Формат — строго JSON по схеме (никаких markdown-обёрток в файле).
"""


def _build_judge_task(
    block_id: str,
    page: int,
    sheet_no: str,
    enrichment: dict | None,
    sources: dict[str, list[dict]],
    output_path: Path,
) -> str:
    src_blocks = []
    for label, items in sources.items():
        if not items:
            src_blocks.append(f"## Источник {label} — НЕТ findings (пустой массив)\n")
            continue
        lines = [f"## Источник {label} ({len(items)} findings)"]
        for i, f in enumerate(items, start=1):
            sev = f.get("severity", "")
            cat = f.get("category", "")
            text = f.get("finding", "")
            norm = f.get("norm_quote") or ""
            rec = f.get("recommendation", "")
            lines.append(f"\n**{label}{i}** [{sev}] `{cat}`")
            lines.append(f"  finding: {text}")
            if norm:
                lines.append(f"  norm: {norm}")
            if rec:
                lines.append(f"  rec: {rec}")
        src_blocks.append("\n".join(lines))
    sources_section = "\n\n".join(src_blocks)

    enrichment_str = json.dumps(enrichment, ensure_ascii=False, indent=2) if enrichment else "(не доступен)"

    return f"""{JUDGE_SYSTEM_PROMPT}

# ЗАДАЧА

Блок: `{block_id}`, страница PDF {page}, лист {sheet_no or "(не определён)"}

## Контекст блока (Qwen enrichment, для понимания о чём блок):
```json
{enrichment_str}
```

## Findings от разных моделей:

{sources_section}

## Что от тебя нужно

Запиши результат через Write tool в файл: `{output_path}`

Формат файла — JSON:
```json
{{
  "matches": [
    {{
      "members": ["A1", "B3"],
      "match_strength": "strong | partial | weak",
      "rationale": "почему они про одно и то же"
    }}
  ],
  "uniques": [
    {{
      "source_ref": "A2",
      "value_judgment": "valuable | questionable | noise",
      "rationale": "обоснование оценки"
    }}
  ],
  "verdict": {{
    "best_overall": "A | B | C | tie",
    "by_source": {{
      "A": {{"valuable_unique": N, "matched": N, "noise": N, "comment": "..."}},
      "B": {{...}},
      "C": {{...}}
    }},
    "rationale": "1-2 предложения почему такая оценка"
  }}
}}
```

Никаких других файлов. JSON в файле без markdown-обёрток.
"""


def _parse_cli_stdout(stdout: str) -> dict:
    try:
        return json.loads(stdout)
    except Exception:
        m = re.search(r"\{[\s\S]*\}\s*$", stdout)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
        return {}


def _latest_qwen_enrichment(project_dir: Path, block_id: str) -> dict | None:
    root = project_dir / "_experiments" / "qwen_enrichment"
    if not root.exists():
        return None
    for run_dir in sorted(root.iterdir(), reverse=True):
        path = run_dir / f"block_{block_id}.json"
        if path.exists():
            rec = json.loads(path.read_text(encoding="utf-8"))
            if rec.get("ok") and rec.get("enrichment"):
                return rec["enrichment"]
    return None


def _findings_from_pilot_record(rec: dict) -> list[dict]:
    pr = rec.get("pro_with_enrichment") or {}
    parsed = pr.get("parsed") or {}
    if isinstance(parsed, dict):
        return parsed.get("findings") or []
    return []


def _findings_from_opus_baseline_record(rec: dict) -> list[dict]:
    return rec.get("findings") or []


def _load_source_findings_for_block(
    project_dir: Path, block_id: str, source_spec: dict
) -> list[dict]:
    """Загрузить findings из указанного запуска для конкретного блока.

    source_spec = {"path": "<exp_dir>", "kind": "pilot|opus_baseline"}
    """
    path = Path(source_spec["path"])
    block_file = path / f"block_{block_id}.json"
    if not block_file.exists():
        return []
    rec = json.loads(block_file.read_text(encoding="utf-8"))
    kind = source_spec.get("kind", "pilot")
    if kind == "pilot":
        return _findings_from_pilot_record(rec)
    elif kind == "opus_baseline":
        return _findings_from_opus_baseline_record(rec)
    else:
        raise ValueError(f"unknown source kind: {kind}")


async def judge_one_block(
    block: dict,
    sources_map: dict[str, dict],  # label -> {"path": ..., "kind": ...}
    project_dir: Path,
    out_dir: Path,
    *,
    timeout_sec: int,
    sem: asyncio.Semaphore,
) -> dict:
    block_id = block["block_id"]
    page = block["page"]

    # collect findings per source
    sources: dict[str, list[dict]] = {}
    for label, spec in sources_map.items():
        sources[label] = _load_source_findings_for_block(project_dir, block_id, spec)

    enrichment = _latest_qwen_enrichment(project_dir, block_id)

    # if all sources empty — skip judging, mark trivial
    if all(len(v) == 0 for v in sources.values()):
        record = {
            "block_id": block_id,
            "page": page,
            "ok": True,
            "skipped_reason": "all sources empty",
            "sources": sources,
            "judgment": None,
        }
        (out_dir / f"block_{block_id}.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return record

    # find sheet_no from graph
    graph_path = project_dir / "_output" / "document_graph.json"
    graph = json.loads(graph_path.read_text(encoding="utf-8"))
    sheet_no = ""
    for p in graph.get("pages", []):
        if p.get("page") == page:
            sheet_no = str(p.get("sheet_no_normalized") or p.get("sheet_no_raw") or "")
            break

    output_json = (out_dir / f"block_{block_id}.judgment.json").resolve()
    task_text = _build_judge_task(block_id, page, sheet_no, enrichment, sources, output_json)

    cmd = [
        CLAUDE_CLI_BIN, "-p",
        "--model", JUDGE_MODEL,
        "--allowedTools", "Write",
        "--output-format", "json",
    ]

    async with sem:
        started = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, **{k: "" for k in os.environ if k.startswith("CLAUDE_CODE")}},
            )
        except FileNotFoundError as exc:
            return {
                "block_id": block_id, "page": page, "ok": False,
                "error": f"CLI not found: {exc}",
                "sources": sources,
            }

        try:
            stdout_b, stderr_b = await asyncio.wait_for(
                proc.communicate(task_text.encode("utf-8")),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return {
                "block_id": block_id, "page": page, "ok": False,
                "error": f"CLI timeout {timeout_sec}s",
                "sources": sources,
            }

        elapsed_ms = int((time.monotonic() - started) * 1000)
        stdout_text = stdout_b.decode("utf-8", errors="replace")
        stderr_text = stderr_b.decode("utf-8", errors="replace")
        exit_code = proc.returncode or 0

    cli_meta = _parse_cli_stdout(stdout_text)
    usage = cli_meta.get("usage", {}) or {}

    judgment = None
    parse_error = None
    if output_json.exists():
        try:
            judgment = json.loads(output_json.read_text(encoding="utf-8"))
        except Exception as e:
            parse_error = f"judgment parse failed: {e}"

    record = {
        "block_id": block_id,
        "page": page,
        "ok": exit_code == 0 and judgment is not None,
        "exit_code": exit_code,
        "elapsed_ms": elapsed_ms,
        "input_tokens": usage.get("input_tokens"),
        "output_tokens": usage.get("output_tokens"),
        "cache_read_tokens": usage.get("cache_read_input_tokens"),
        "cli_reported_cost_usd": cli_meta.get("total_cost_usd") or cli_meta.get("cost_usd"),
        "sources": sources,
        "judgment": judgment,
        "parse_error": parse_error,
        "stderr_tail": stderr_text[-300:] if stderr_text else None,
    }
    (out_dir / f"block_{block_id}.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return record


def _aggregate(records: list[dict], source_labels: list[str]) -> dict:
    per_source = {label: {"matched": 0, "valuable_unique": 0, "questionable_unique": 0, "noise_unique": 0, "total_findings": 0, "wins": 0} for label in source_labels}
    total_matches = 0
    total_uniques = 0

    for r in records:
        if not r.get("ok") or not r.get("judgment"):
            continue
        j = r["judgment"]
        # count total findings per source
        for label in source_labels:
            per_source[label]["total_findings"] += len(r.get("sources", {}).get(label, []))
        # matches: each member counts as "matched" for its source
        for m in j.get("matches", []) or []:
            total_matches += 1
            for member in m.get("members", []) or []:
                # member like "A1", "B3"
                mlabel = (member or "")[:1]
                if mlabel in per_source:
                    per_source[mlabel]["matched"] += 1
        # uniques
        for u in j.get("uniques", []) or []:
            total_uniques += 1
            ref = u.get("source_ref") or ""
            ulabel = ref[:1] if ref else ""
            v = u.get("value_judgment") or ""
            if ulabel in per_source:
                if v == "valuable":
                    per_source[ulabel]["valuable_unique"] += 1
                elif v == "questionable":
                    per_source[ulabel]["questionable_unique"] += 1
                elif v == "noise":
                    per_source[ulabel]["noise_unique"] += 1
        # winner
        verdict = j.get("verdict") or {}
        best = verdict.get("best_overall") or ""
        if best in per_source:
            per_source[best]["wins"] += 1

    return {
        "total_blocks": len(records),
        "judged_blocks": sum(1 for r in records if r.get("ok") and r.get("judgment")),
        "skipped_blocks": sum(1 for r in records if r.get("skipped_reason")),
        "total_matches": total_matches,
        "total_uniques": total_uniques,
        "per_source": per_source,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=str, default=str(DEFAULT_PROJECT_DIR))
    parser.add_argument(
        "--source",
        action="append",
        required=True,
        help="формат: LABEL=KIND:PATH  (пример: A=opus_baseline:/path/to/run)",
    )
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--blocks", type=str, default="")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    project_dir = Path(args.project).resolve()

    sources_map: dict[str, dict] = {}
    for s in args.source:
        if "=" not in s or ":" not in s:
            print(f"[error] bad --source spec: {s}", file=sys.stderr)
            return 2
        label, rhs = s.split("=", 1)
        kind, path = rhs.split(":", 1)
        sources_map[label.strip()] = {"kind": kind.strip(), "path": path.strip()}
    print(f"[plan] sources: {list(sources_map.keys())}")
    for label, spec in sources_map.items():
        print(f"  {label}: kind={spec['kind']} path={spec['path']}")

    blocks_dir = project_dir / "_output" / "blocks"
    index = json.loads((blocks_dir / "index.json").read_text(encoding="utf-8"))
    blocks_all = index["blocks"]

    if args.all:
        wanted = blocks_all
    elif args.blocks:
        wanted_ids = {s.strip() for s in args.blocks.split(",") if s.strip()}
        wanted = [b for b in blocks_all if b["block_id"] in wanted_ids]
    else:
        wanted = blocks_all
    if args.limit:
        wanted = wanted[: args.limit]

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = project_dir / "_experiments" / "findings_judge" / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[out] {out_dir}")
    print(f"[blocks] {len(wanted)}, parallelism={args.parallelism}")

    sem = asyncio.Semaphore(args.parallelism)
    overall_start = time.monotonic()

    async def _runner(b):
        print(f"  -> judging {b['block_id']}", flush=True)
        rec = await judge_one_block(b, sources_map, project_dir, out_dir, timeout_sec=args.timeout, sem=sem)
        if rec.get("skipped_reason"):
            print(f"  -- {b['block_id']}: SKIP ({rec['skipped_reason']})", flush=True)
        elif rec.get("ok"):
            j = rec["judgment"] or {}
            verdict = (j.get("verdict") or {}).get("best_overall", "?")
            n_match = len(j.get("matches", []) or [])
            n_unique = len(j.get("uniques", []) or [])
            print(f"  <- {b['block_id']} t={(rec.get('elapsed_ms') or 0)/1000:.1f}s matches={n_match} uniques={n_unique} winner={verdict}", flush=True)
        else:
            print(f"  <- FAIL {b['block_id']} err={rec.get('error') or rec.get('parse_error')}", flush=True)
        return rec

    results = await asyncio.gather(*(_runner(b) for b in wanted))
    overall_elapsed = time.monotonic() - overall_start

    agg = _aggregate(results, list(sources_map.keys()))

    summary = {
        "timestamp": ts,
        "judge_model": JUDGE_MODEL,
        "sources_map": sources_map,
        "total_blocks": len(results),
        "wall_clock_s": round(overall_elapsed, 1),
        "aggregate": agg,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # markdown
    lines = [
        f"# Findings judge — {ts}",
        "",
        f"Judge: `{JUDGE_MODEL}`  Project: `{project_dir.name}`  Wall: {overall_elapsed:.1f}s",
        "",
        "## Source map",
        "",
    ]
    for label, spec in sources_map.items():
        lines.append(f"- **{label}** — kind={spec['kind']}, path=`{spec['path']}`")
    lines.append("")
    lines.append("## Aggregate")
    lines.append("")
    lines.append(f"- Blocks judged: {agg['judged_blocks']}/{agg['total_blocks']}  (skipped {agg['skipped_blocks']})")
    lines.append(f"- Total semantic matches: {agg['total_matches']}")
    lines.append(f"- Total unique findings: {agg['total_uniques']}")
    lines.append("")
    lines.append("## Per source")
    lines.append("")
    lines.append("| source | total | matched | valuable_unique | questionable | noise | wins |")
    lines.append("|---|---|---|---|---|---|---|")
    for label, ps in agg["per_source"].items():
        lines.append(
            f"| {label} | {ps['total_findings']} | {ps['matched']} | {ps['valuable_unique']} | {ps['questionable_unique']} | {ps['noise_unique']} | {ps['wins']} |"
        )
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"\n[done] judged {agg['judged_blocks']}/{agg['total_blocks']} in {overall_elapsed:.1f}s")
    print(f"[out]  {out_dir / 'summary.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
