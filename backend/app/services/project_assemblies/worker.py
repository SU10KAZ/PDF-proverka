"""Isolated process entry point for one project assembly build."""
from __future__ import annotations

import os
import signal
import sys

from .service import build_version


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        return 2
    try:
        os.nice(10)
    except OSError:
        pass
    def timed_out(_signum, _frame):
        raise TimeoutError("Сборка превысила общий лимит 30 минут")

    signal.signal(signal.SIGALRM, timed_out)
    signal.alarm(30 * 60)
    build_version(argv[0], argv[1], argv[2])
    signal.alarm(0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
