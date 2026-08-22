#!/usr/bin/env python3
"""FMC shared IO — read a JSON artifact whether it is stored plain or gzipped.

Large FMC artifacts are committed gzipped (`*.json.gz`) to keep the repository small.
Every FMC probe reads through this helper, so both forms work without any manual step.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any


def read_json(path: str | Path) -> Any:
    p = Path(path)
    if p.is_file():
        return json.loads(p.read_text(encoding="utf-8"))
    gz = Path(str(p) + ".gz")
    if gz.is_file():
        with gzip.open(gz, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    raise FileNotFoundError(f"neither {p} nor {gz}")


def exists(path: str | Path) -> bool:
    p = Path(path)
    return p.is_file() or Path(str(p) + ".gz").is_file()
