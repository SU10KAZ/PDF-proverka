#!/usr/bin/env python3
"""Probe HYBRID-10: run the SAME model on the MINIMAL payload.

Same model (gpt-5.6-sol), same output schema and same base prompt as Track A's
ai_experiment, same five pairs — only the payload is the minimal one built by
HYBRID-2. Records the reported token bill for a like-for-like comparison with
Track A's 70,631 (vector) and 38,069 (vision).

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p10_minimal_ai_run
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from experiments.stage_comparison_vector_blocks import run_ai_experiment as rae
from experiments.stage_comparison_vector_architecture_opus.probes import hybrid_p2_minimal_payload as p2

OUT = Path(__file__).resolve().parents[1] / "artifacts"
CODEX = Path(
    "/home/coder/.vscode-server/extensions/openai.chatgpt-26.818.41705-linux-x64/bin/linux-x86_64/codex"
)
SCHEMA = Path(__file__).resolve().parents[3] / "experiments" / "stage_comparison_vector_blocks" / "ai_output_schema.json"
PAIRS = rae.PAIR_IDS


def build_prompt(page_context: bool) -> str:
    rows = [
        rae._base_prompt(),
        "Ниже — минимальное детерминированное описание изменений по каждой паре; картинок нет.",
        "Поля: context — структурный контекст; changes — сгруппированные изменения "
        "(at = нормализованная позиция, context = ближайшие неизменные подписи, values = [было, стало]); "
        "geometry_only_in_* — локализованные несовпавшие участки геометрии; uncertainty — что проверить нельзя.",
        "Ничего, кроме приведённых данных, использовать нельзя.",
    ]
    for pid in PAIRS:
        rows.append(json.dumps(p2.build(pid, "span", page_context), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return "\n\n".join(rows) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--page-context", action="store_true",
                    help="add the page-context crop-artefact suppression (probe HYBRID-8)")
    args = ap.parse_args()
    tag = "pagectx" if args.page_context else "base"
    prompt = build_prompt(args.page_context)
    (OUT / f"hybrid_minimal_prompt_{tag}.txt").write_text(prompt, encoding="utf-8")
    cmd = [
        str(CODEX), "exec", "--ephemeral", "--skip-git-repo-check", "--ignore-rules",
        "--sandbox", "read-only", "--model", "gpt-5.6-sol",
        "-c", 'model_reasoning_effort="xhigh"',
        "--output-schema", str(SCHEMA), "-C", "/tmp", "--json", "-",
    ]
    r = subprocess.run(cmd, input=prompt, text=True, capture_output=True, timeout=1800)
    usage, message = None, None
    for line in r.stdout.splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("type") == "turn.completed":
            usage = ev["usage"]
        if ev.get("type") == "item.completed" and ev["item"].get("type") == "agent_message":
            message = ev["item"]["text"]
    res = {
        "variant": tag,
        "prompt_characters": len(prompt),
        "prompt_bytes_utf8": len(prompt.encode("utf-8")),
        "usage": usage,
        "reported_total": (usage["input_tokens"] + usage["output_tokens"]) if usage else None,
        "returncode": r.returncode,
        "output": json.loads(message) if message else None,
        "stderr_tail": r.stderr[-1500:],
    }
    (OUT / f"hybrid_minimal_ai_run_{tag}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(json.dumps({k: v for k, v in res.items() if k not in ("output", "stderr_tail")}, ensure_ascii=False, indent=2))
    print(json.dumps(res["output"], ensure_ascii=False, indent=1)[:4000])


if __name__ == "__main__":
    main()
