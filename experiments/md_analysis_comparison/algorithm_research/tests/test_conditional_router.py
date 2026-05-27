"""Smoke tests for the conditional_router."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runners.conditional_router import (
    should_run_cross_discipline, reviewer_trigger,
)


def t_assert(name, cond, detail=""):
    if cond:
        print(f"OK  {name}")
    else:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)


def main():
    cross_md = """## А.1 Состав нагрузки
По заданию ОВ: 18,4 кВт. Тепловая нагрузка передана через ИТП.
В ЭОМ не учтены тепловые завесы."""
    plain_md = """## КЖ Армирование плиты
Класс арматуры A500C, диаметр 12 мм, шаг 200 мм."""

    d_cross = should_run_cross_discipline(cross_md, "EOM")
    t_assert("EOM with triggers fires", d_cross.cross_discipline_triggered)
    t_assert("EOM hits multiple triggers", len(d_cross.triggers_hit) >= 2)

    d_intra = should_run_cross_discipline(plain_md, "KJ")
    t_assert("KJ without triggers skips", not d_intra.cross_discipline_triggered)

    # KJ with 2+ triggers should fire
    kj_with_triggers = plain_md + "\nЗакладные согласовать с КМ и ЭОМ."
    d_kj_yes = should_run_cross_discipline(kj_with_triggers, "KJ")
    t_assert("KJ with 2+ triggers fires", d_kj_yes.cross_discipline_triggered)

    # MULTI always fires
    d_multi = should_run_cross_discipline("anything", "MULTI")
    t_assert("MULTI always fires", d_multi.cross_discipline_triggered)

    # Reviewer trigger
    r1 = reviewer_trigger(8, ["m1", "m2", "m3"], "EOM")
    t_assert("reviewer fires when conditions met", r1["reviewer_triggered"])
    r2 = reviewer_trigger(15, ["m1", "m2", "m3"], "EOM")
    t_assert("reviewer skips when post-critic full", not r2["reviewer_triggered"])
    r3 = reviewer_trigger(8, ["m1"], "EOM")
    t_assert("reviewer skips with too few warnings", not r3["reviewer_triggered"])
    r4 = reviewer_trigger(8, ["m1", "m2"], "AR")
    t_assert("reviewer skips for AR", not r4["reviewer_triggered"])

    print("\nAll conditional_router tests passed.")


if __name__ == "__main__":
    main()
