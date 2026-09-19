#!/usr/bin/env python3
"""Source-truth leakage + runtime research-dependency audit (zero model calls).

Static:  AST imports and string constants of the V3 / Human Mapping runtime.
Dynamic: a subprocess runs the synthetic generic V3 flow through the real
         production routers under a Python audit hook that records every file
         opened below experiments/, scripts/ or corpus-audits/, then scans the
         produced contracts for research-truth tokens.

Usage:
    python scripts/audit_projectchange_v3_generic_e2e.py --out-dir <dir>
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RUNTIME = [
    *sorted((REPO / "backend/app/services/project_change_v3").glob("*.py")),
    *sorted((REPO / "backend/app/services/project_change_v3/prompts").glob("*.txt")),
    *sorted((REPO / "backend/app/services/human_mapping_production").glob("*.py")),
    REPO / "backend/app/api/routers/human_mapping.py",
    REPO / "backend/app/api/routers/project_change_preview.py",
]
TRUTH = re.compile(r"REAL15|PROVEN10|\bF13\b|GOOD_HUMAN_LEVEL|TOO_ATOMIC|OVER_MERGED|EVALUATION|GROUPING_QUALITY|"
                   r"SPLIT\.json|validation/|holdout", re.I)
RESEARCH_PATH = re.compile(r"experiments[/.]|corpus-audits|20260914_project_change_272|scripts/")
RESEARCH_ROOTS = [str(REPO / "experiments"), str(REPO / "scripts"), "/home/coder/auditmanager/corpus-audits"]

DYNAMIC = r'''
import json, sys, tempfile, os
roots = json.loads(sys.argv[1])
opened = []
def hook(event, args):
    if event == "open" and args:
        p = args[0]
        if isinstance(p, (str, bytes)) or hasattr(p, "__fspath__"):
            p = os.fsdecode(os.fspath(p))
            if any(p.startswith(r) for r in roots):
                opened.append(p)
sys.addaudithook(hook)
tmp = tempfile.mkdtemp(prefix="pc_v3_audit_")
os.environ["COMPARISON_ROOT"] = os.path.join(tmp, "comparison")
os.makedirs(os.environ["COMPARISON_ROOT"])
os.environ.update(PROJECT_COMPARISON_ENGINE="v3", PROJECT_COMPARISON_V3_ALLOW_INFERENCE="0",
                  PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE="1")
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.app.services.project_change_v3 import scope
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.app.api.routers import human_mapping, project_change_preview, stage_comparison
built = gf.build_comparison(Path(tmp))
scope._object_stage_paths = lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])}
fake = FakeProvider(handlers=gf.fake_handlers()); set_test_provider(fake)
app = FastAPI()
for r in (stage_comparison.router, project_change_preview.router, project_change_preview.availability_router,
          human_mapping.router, human_mapping.api_router):
    app.include_router(r)
c = TestClient(app)
sid = built["session_id"]
outputs = {}
outputs["state"] = c.post(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/run",
                          json={"input_mode": "DOCUMENT"}).json()
outputs["changes"] = c.get(f"/api/stage-comparison/sessions/{sid}/pairs/{gf.PAIR_ID}/production/changes").json()
outputs["view"] = c.get(f"/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes").json()
for e in outputs["view"]["items"][0]["evidence"]:
    assert c.get(e["image_url"]).status_code == 200
outputs["hm_ui"] = c.get(f"/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/{gf.PAIR_ID}/ui-data").json()
outputs["hm_page"] = c.get(f"/human-mapping/?object={gf.OBJECT_ID}&comparison={gf.PAIR_ID}").text[-4000:]
print(json.dumps({
    "opened_research_files": sorted(set(opened)),
    "experiments_modules": sorted(m for m in sys.modules if m == "experiments" or m.startswith("experiments.")),
    "scripts_modules": sorted(m for m in sys.modules if m == "scripts" or m.startswith("scripts.")),
    "outputs": outputs,
    "provider_calls": [x["stage"] for x in fake.calls],
}, ensure_ascii=False))
'''


def static_scan() -> dict:
    imports, research_strings, truth_hits, docs = [], [], [], []
    for path in RUNTIME:
        rel = str(path.relative_to(REPO))
        text = path.read_text(encoding="utf-8")
        if path.suffix == ".txt":
            for i, line in enumerate(text.splitlines(), 1):
                if TRUTH.search(line) or re.search(r"\btruth\b", line, re.I):
                    truth_hits.append({"file": rel, "line": i, "kind": "frozen_prompt_instruction",
                                       "text": line.strip()[:160]})
            continue
        tree = ast.parse(text)
        docstrings = {
            id(owner.body[0].value)
            for owner in ast.walk(tree)
            if isinstance(owner, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
            and owner.body and isinstance(owner.body[0], ast.Expr)
            and isinstance(owner.body[0].value, ast.Constant) and isinstance(owner.body[0].value.value, str)
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                names = []
            for name in names:
                if name.split(".")[0] in {"experiments", "scripts"}:
                    imports.append({"file": rel, "line": node.lineno, "module": name})
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) in docstrings:
                if RESEARCH_PATH.search(node.value) or TRUTH.search(node.value):
                    docs.append({"file": rel, "line": node.lineno, "text": node.value.splitlines()[0][:160]})
                continue
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if RESEARCH_PATH.search(node.value):
                    research_strings.append({"file": rel, "line": node.lineno, "text": node.value[:160]})
                if TRUTH.search(node.value):
                    truth_hits.append({"file": rel, "line": node.lineno, "kind": "code_string",
                                       "text": node.value[:160]})
    return {"research_imports": imports, "research_path_strings": research_strings, "truth_hits": truth_hits,
            "docstring_mentions": docs}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    out = Path(parser.parse_args().out_dir)
    out.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    static = static_scan()
    proc = subprocess.run([sys.executable, "-c", DYNAMIC, json.dumps(RESEARCH_ROOTS)], cwd=REPO,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr[-4000:], file=sys.stderr)
        return 1
    dynamic = json.loads(proc.stdout.strip().splitlines()[-1])
    outputs_text = json.dumps(dynamic.pop("outputs"), ensure_ascii=False)
    output_truth = sorted(set(m.group(0) for m in TRUTH.finditer(outputs_text)))
    output_truth_word = bool(re.search(r"\btruth\b", outputs_text, re.I))
    code_truth = [h for h in static["truth_hits"] if h["kind"] == "code_string"]
    leakage = {
        "schema": "projectchange-v3-source-truth-leakage/2",
        "checked_at": now,
        "runtime_files": [str(p.relative_to(REPO)) for p in RUNTIME],
        "code_truth_references": code_truth,
        "frozen_prompt_instructions_mentioning_truth": [h for h in static["truth_hits"]
                                                        if h["kind"] == "frozen_prompt_instruction"],
        "generic_outputs_truth_tokens": output_truth,
        "generic_outputs_truth_word": output_truth_word,
        "SOURCE_TRUTH_LEAKAGE": len(code_truth) + len(output_truth) + int(output_truth_word),
        "pass": not code_truth and not output_truth and not output_truth_word,
        "model_calls": 0,
        "notes": "The frozen Mapper prompt instructs the model NOT to use truth; that instruction is not leakage.",
    }
    research = {
        "schema": "projectchange-v3-runtime-research-dependencies/2",
        "checked_at": now,
        "static_research_imports": static["research_imports"],
        "static_research_path_strings": static["research_path_strings"],
        "docstring_mentions_not_runtime": static["docstring_mentions"],
        "dynamic_opened_research_files": dynamic["opened_research_files"],
        "dynamic_experiments_modules": dynamic["experiments_modules"],
        "dynamic_scripts_modules": dynamic["scripts_modules"],
        "provider_calls_fake": dynamic["provider_calls"],
        "RUNTIME_RESEARCH_DEPENDENCIES": (len(static["research_imports"]) + len(static["research_path_strings"])
                                          + len(dynamic["opened_research_files"])
                                          + len(dynamic["experiments_modules"]) + len(dynamic["scripts_modules"])),
        "model_calls": 0,
    }
    research["pass"] = research["RUNTIME_RESEARCH_DEPENDENCIES"] == 0
    (out / "SOURCE_TRUTH_LEAKAGE.json").write_text(json.dumps(leakage, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "RUNTIME_RESEARCH_DEPENDENCIES.json").write_text(json.dumps(research, ensure_ascii=False, indent=2),
                                                          encoding="utf-8")
    print(json.dumps({"SOURCE_TRUTH_LEAKAGE": leakage["SOURCE_TRUTH_LEAKAGE"],
                      "RUNTIME_RESEARCH_DEPENDENCIES": research["RUNTIME_RESEARCH_DEPENDENCIES"],
                      "opened": dynamic["opened_research_files"][:5],
                      "strings": static["research_path_strings"][:5], "code_truth": code_truth[:5]},
                     ensure_ascii=False))
    return 0 if leakage["pass"] and research["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
