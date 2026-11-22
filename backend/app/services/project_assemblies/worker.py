"""Isolated process entry point for one project assembly build (with duplicate scan)."""
from __future__ import annotations

import os
import signal
import sys

from .service import run_version


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        return 2
    try:
        os.nice(10)
    except OSError:
        pass
    def timed_out(_signum, _frame):
        raise TimeoutError("Сборка превысила общий лимит 45 минут")

    signal.signal(signal.SIGALRM, timed_out)
    # Поиск дублей (~0,1 с на страницу) идёт в том же задании перед склейкой.
    signal.alarm(45 * 60)
    run_version(argv[0], argv[1], argv[2])
    signal.alarm(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
