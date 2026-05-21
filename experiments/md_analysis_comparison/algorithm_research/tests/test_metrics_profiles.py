"""Smoke tests for the score profiles."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from metrics.score_algorithms import SCORE_PROFILES


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def main():
    # Perfect run: recall=1, fp=0, dupes=0, missed=0
    perfect = {name: fn(1.0, 0, 0, 0, 0, 100.0) for name, fn in SCORE_PROFILES.items()}
    for name, score in perfect.items():
        t_assert(f"{name} perfect ~ 100", 95 <= score <= 110, f"{name}={score}")

    # Noisy run with high FP
    noisy = SCORE_PROFILES["strict_production"](0.8, 20, 5, 0, 0, 500)
    t_assert("strict heavily penalises FP", noisy < 0, f"got {noisy}")

    # Recall-only profile
    r = SCORE_PROFILES["recall_priority"](0.9, 30, 0, 0, 0, 500)
    t_assert("recall_priority tolerates FP at 0.9 recall", r > 50, f"got {r}")

    # Balanced rewards beyond_gt
    a = SCORE_PROFILES["balanced_engineering"](0.8, 10, 0, 0, 0, 200)
    b = SCORE_PROFILES["balanced_engineering"](0.8, 10, 0, 0, 5, 200)
    t_assert("balanced rewards beyond_gt", b > a, f"a={a} b={b}")

    # Cost-aware penalises wall-clock
    c1 = SCORE_PROFILES["cost_aware"](0.8, 5, 0, 0, 0, 200)
    c2 = SCORE_PROFILES["cost_aware"](0.8, 5, 0, 0, 0, 800)
    t_assert("cost_aware penalises slow", c1 > c2, f"c1={c1} c2={c2}")

    print("\nAll score profile tests passed.")


if __name__ == "__main__":
    main()
