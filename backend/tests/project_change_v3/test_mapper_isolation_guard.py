"""Mapper isolation guard — pre-analysis human prelinks must never reach a model.

The PRELINK Mapper experiment (25–26.09.2026) proved that a wrong human link
shown to the Mapper can pull blocks into one region (PRELINK_MAPPER_UNSAFE).
The safe design keeps the V3 engine blind to prelinks by construction; these
static checks make that impossible to undo silently.  Zero model calls.

* G1 — nothing in the V3 engine package imports or names prelink modules or
  the experimental model-visible sections.
* G2 — the orchestrator calls the V3 engine with exactly the keywords of
  production d38bf907: no new channel into the engine.
* G6 — prelink modules (drafts, run snapshot, reconciliation, router) cannot
  reach a model transport.
* G7 — the experimental Mapper-context flags are read nowhere.
"""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
APP = ROOT / "backend" / "app"
ENGINE_PACKAGE = APP / "services" / "project_change_v3"
ORCHESTRATOR = APP / "services" / "stage_comparison" / "production_orchestrator.py"

ENGINE_KEYWORDS = {"input_mode", "left_pages", "right_pages", "left_block_ids", "right_block_ids",
                   "ai_mode", "run_id", "cancel_token"}
MODEL_VISIBLE_MARKERS = ("HUMAN_PRELINKS", "HUMAN_SHEET_MAP", "prelink_outcomes", "MAPPER_CONTEXT")
FORBIDDEN_FOR_PRELINK_MODULES = ("provider", "transport", "claude", "openrouter", "codex", "subprocess",
                                 "project_change_v3.engine")
EXPERIMENT_FLAGS = ("PROJECT_COMPARISON_V3_MAPPER_CONTEXT", "PROJECT_COMPARISON_V3_MAPPER_PRELINKS",
                    "PROJECT_COMPARISON_V3_MAPPER_SHEET_MAP")


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imported_names(tree: ast.AST) -> list[str]:
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            names += [base] + [f"{base}.{alias.name}" for alias in node.names]
    return names


def _string_constants(tree: ast.AST) -> list[str]:
    return [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]


def test_engine_package_has_no_prelink_imports_or_markers():
    problems = []
    for path in sorted(ENGINE_PACKAGE.rglob("*.py")):
        tree = _tree(path)
        for name in _imported_names(tree):
            if "prelink" in name.lower() or "mapping_context" in name.lower():
                problems.append(f"{path.relative_to(ROOT)}: import {name}")
        for text in _string_constants(tree):
            if "prelink" in text.lower() or any(marker in text for marker in MODEL_VISIBLE_MARKERS):
                problems.append(f"{path.relative_to(ROOT)}: literal {text[:60]!r}")
    for path in sorted(ENGINE_PACKAGE.rglob("*.txt")):
        text = path.read_text(encoding="utf-8")
        if "prelink" in text.lower() or any(marker in text for marker in MODEL_VISIBLE_MARKERS):
            problems.append(f"{path.relative_to(ROOT)}: prompt mentions human prelinks")
    assert problems == []


def test_engine_call_signature_unchanged():
    tree = _tree(ORCHESTRATOR)
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef) and node.name == "run_production_comparison")
    calls = [node for node in ast.walk(function) if isinstance(node, ast.Call)
             and getattr(node.func, "id", getattr(node.func, "attr", None)) == "run_v3_production_comparison"]
    assert len(calls) == 1
    [call] = calls
    assert len(call.args) == 2  # session_id, pair_id
    assert all(keyword.arg is not None for keyword in call.keywords)  # no **kwargs channel
    assert {keyword.arg for keyword in call.keywords} == ENGINE_KEYWORDS


def test_prelink_modules_cannot_call_models():
    modules = sorted(path for path in APP.rglob("*.py") if "prelink" in path.name)
    problems = []
    for path in modules:
        for name in _imported_names(_tree(path)):
            if any(bad in name.lower() for bad in FORBIDDEN_FOR_PRELINK_MODULES):
                problems.append(f"{path.relative_to(ROOT)}: import {name}")
    assert problems == []


def test_experimental_mapper_context_flags_are_read_nowhere():
    hits = [f"{path.relative_to(ROOT)}: {flag}"
            for path in sorted(APP.rglob("*.py"))
            for flag in EXPERIMENT_FLAGS
            if flag in path.read_text(encoding="utf-8")]
    assert hits == []
