#!/usr/bin/env python3
"""Fair old-Level-3 versus L3_CHANGE_ONLY size and real-token probe."""
from __future__ import annotations

import json
import re
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from .l3_change_only import payload_metrics


EXPERIMENT_DIR = Path(__file__).resolve().parent
BASE = EXPERIMENT_DIR.parent / "stage_comparison_vector_blocks/artifacts"
ARTIFACT = EXPERIMENT_DIR / "artifacts/prompt_size_results.json"
ROUTING = EXPERIMENT_DIR / "artifacts/routing_results.json"
SCHEMA = EXPERIMENT_DIR / "token_probe_schema.json"
MODEL = "gpt-5.6-sol"
PAIR_IDS = (
    "ss_scheme_text_changed", "ss_plan_dense", "ss_simple_node", "ss_table_graphic",
    "ar_plan", "ar_wall_sections", "vk_plan", "vk_nodes", "vk_node_plan", "eom_singleline_changed",
)
PROBE_IDS = ("ss_scheme_text_changed", "ar_plan", "vk_nodes")


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _old_payload(pair_id: str) -> dict[str, Any]:
    left = _load(BASE / "descriptions" / pair_id / "left/vector_block.json")
    right = _load(BASE / "descriptions" / pair_id / "right/vector_block.json")
    comparison = _load(BASE / "comparisons" / pair_id / "comparison.json")
    geometry = comparison["geometry"]; text = comparison["text"]
    return {
        "pair_id": pair_id,
        "left_level_3": left["size_metrics"]["compact_payload"],
        "right_level_3": right["size_metrics"]["compact_payload"],
        "deterministic_diff": {
            "status": comparison["status"],
            "geometry": {
                key: geometry[key] for key in ("similarity", "selected_tolerance", "left_coverage", "right_coverage", "encoding_rewrite_suspected")
            },
            "geometry_tolerance_experiment": [
                {key: run[key] for key in ("tolerance", "similarity", "left_coverage", "right_coverage", "left_used", "right_used", "capped")}
                for run in geometry["tolerance_experiment"]
            ],
            "text": {key: text[key] for key in ("effective_similarity", "reliable", "left_layer_quality", "right_layer_quality", "removed", "added", "value_changes", "truncated")},
            "topology": comparison["topology"], "patterns": comparison["repeated_patterns"],
            "differences": comparison["differences"], "caveats": comparison["caveats"],
        },
    }


def _usage(events: list[dict[str, Any]], stderr: str) -> dict[str, Any]:
    found: dict[str, int] = {}
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key,item in value.items():
                if key.lower() in {"input_tokens","output_tokens","cached_input_tokens","total_tokens"} and isinstance(item,int): found[key.lower()]=max(found.get(key.lower(),0),item)
                visit(item)
        elif isinstance(value,list):
            for item in value: visit(item)
    visit(events); match=re.search(r"tokens used\s+([\d\s\u00a0]+)",stderr)
    return {"input_tokens":found.get("input_tokens"),"cached_input_tokens":found.get("cached_input_tokens"),"output_tokens":found.get("output_tokens"),"total_tokens":found.get("total_tokens") or (int(re.sub(r"\D","",match.group(1))) if match else None)}


def _invoke(payloads: list[dict[str, Any]], label: str, work: Path) -> dict[str, Any]:
    prompt = "Token probe only. Read the supplied JSON payloads without tools and return their pair_id values in received.\n" + "\n".join(json.dumps(item,ensure_ascii=False,sort_keys=True,separators=(",",":")) for item in payloads)
    output=work/f"{label}.json";command=["codex","exec","--ephemeral","--skip-git-repo-check","--ignore-rules","--sandbox","read-only","--model",MODEL,"--output-schema",str(SCHEMA),"--json","--output-last-message",str(output),"-C",str(work),"-"]
    started=time.perf_counter();completed=subprocess.run(command,input=prompt,text=True,capture_output=True,timeout=1200,check=False);latency=time.perf_counter()-started
    if completed.returncode: raise RuntimeError(completed.stderr[-5000:])
    events=[]
    for line in completed.stdout.splitlines():
        try: events.append(json.loads(line))
        except json.JSONDecodeError: pass
    return {"payload":payload_metrics(prompt),"usage":_usage(events,completed.stderr),"latency_seconds":round(latency,6),"pairs":len(payloads),"model":MODEL}


def run() -> dict[str, Any]:
    routing=_load(ROUTING);new_by_id={row["pair_id"]:row["l3_change_only"] for row in routing["pairs"]}
    rows=[]
    for pair_id in PAIR_IDS:
        old,new=_old_payload(pair_id),new_by_id[pair_id];om,nm=payload_metrics(old),payload_metrics(new)
        rows.append({"pair_id":pair_id,"old_l3":om,"l3_change_only":nm,"estimated_reduction_percent":round((1-nm["estimated_tokens"]/max(om["estimated_tokens"],1))*100,3)})
    with tempfile.TemporaryDirectory(prefix="vector-token-probe-") as directory:
        work=Path(directory);old_probe=_invoke([_old_payload(pair_id) for pair_id in PROBE_IDS],"old",work);new_probe=_invoke([{"pair_id":pair_id,"l3_change_only":new_by_id[pair_id]} for pair_id in PROBE_IDS],"new",work)
    result={
        "schema_version":"vector-prompt-size-v0.2-codex","comparison":"Baseline v0.1 full compact Level 3 + filtered diff versus v0.2 change-only evidence.",
        "pairs":rows,"aggregate":{
            "sample_pairs":len(rows),"old_l3_median_bytes":statistics.median(row["old_l3"]["bytes"] for row in rows),"change_only_median_bytes":statistics.median(row["l3_change_only"]["bytes"] for row in rows),
            "old_l3_median_estimated_tokens":statistics.median(row["old_l3"]["estimated_tokens"] for row in rows),"change_only_median_estimated_tokens":statistics.median(row["l3_change_only"]["estimated_tokens"] for row in rows),"median_reduction_percent":round(statistics.median(row["estimated_reduction_percent"] for row in rows),3),
            "real_model_token_probe":{"pair_ids":list(PROBE_IDS),"old_l3":old_probe,"l3_change_only":new_probe,"input_token_reduction_percent":round((1-new_probe["usage"]["input_tokens"]/old_probe["usage"]["input_tokens"])*100,3)},
        },
    }
    ARTIFACT.write_text(json.dumps(result,ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8");return result


if __name__=="__main__":
    print(json.dumps(run()["aggregate"],ensure_ascii=False,indent=2))
