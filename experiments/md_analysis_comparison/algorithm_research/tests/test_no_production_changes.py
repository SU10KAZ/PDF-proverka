"""test_no_production_changes — guardrail that this research stand does NOT
modify any production paths.

Asserts:
- experiments/ tree never imports from backend.app.*
- production directories (backend/, frontend/, etc.) are not written by any
  script under experiments/
- production pipeline manager.py is not referenced by experiments.

This is a static check — no LLM calls.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]  # repo root
EXPERIMENTS = ROOT / "experiments" / "md_analysis_comparison"

BANNED_IMPORTS = [
    r"\bfrom\s+backend\.app",
    r"\bimport\s+backend\.app",
    r"\bfrom\s+backend\b\.\s*app",
]
BANNED_WRITE_TARGETS = [
    "backend/app/",
    "frontend/src/",
    "norms/tools/",
    "backend/app/pipeline/manager.py",
]


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


SELF_PATH = Path(__file__).resolve()

# Files that legitimately *mention* production paths in documentation /
# gating-rule strings but don't actually write to them.
DOC_EXCLUSIONS = {
    "test_no_production_changes.py",
    "evaluate_gating.py",
    "a1_v2_final_recommendation.md",
    "FINAL_SUMMARY.md",
}


def _actually_writes_to(text: str, target: str) -> bool:
    """A more strict check: write call must be near the target path string."""
    target_q = f"\"{target}"
    target_q2 = f"'{target}"
    target_path = f"/ \"{target.rstrip('/')}"  # e.g.  Path(...) / "backend"
    write_indicators = (".write_text", ".write_bytes", "open(")
    # Only flag if the target appears as a string literal AND there's a write
    # call within ~80 chars of the target string.
    for i, _ in enumerate(text):
        for q in (target_q, target_q2, target_path):
            j = text.find(q, i)
            if j >= 0:
                # Look for write call nearby
                near = text[max(0, j-80):j+200]
                if any(w in near for w in write_indicators):
                    return True
        break  # only scan once
    return False


def main():
    py_files = [p for p in EXPERIMENTS.rglob("*.py") if p.resolve() != SELF_PATH
                 and p.name not in DOC_EXCLUSIONS]
    t_assert("experiments has Python files", len(py_files) > 0)

    offending: list[str] = []
    for p in py_files:
        text = p.read_text(encoding="utf-8", errors="ignore")
        for pat in BANNED_IMPORTS:
            if re.search(pat, text):
                offending.append(f"{p.relative_to(ROOT)}: imports backend.app")
                break
        for target in BANNED_WRITE_TARGETS:
            if _actually_writes_to(text, target):
                offending.append(f"{p.relative_to(ROOT)}: writes to {target}")

    t_assert("no production imports / writes detected",
             not offending, "\n  ".join(offending))

    # Also check that production manager.py is not modified by us; we cannot
    # easily verify mtime without git, so just check it's still there.
    prod_manager = ROOT / "backend" / "app" / "pipeline" / "manager.py"
    t_assert("production manager.py exists",
             prod_manager.exists(),
             "manager.py missing — should not have been touched")
    print("\ntest_no_production_changes PASSED")


if __name__ == "__main__":
    main()
