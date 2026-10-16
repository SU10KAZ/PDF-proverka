"""Admission control for local computations, including every ancestor memory limit."""
from __future__ import annotations

import math
import os
from pathlib import Path

GIB = 1024 ** 3
MIB = 1024 ** 2


def number(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
        return max(minimum, value) if math.isfinite(value) else default
    except ValueError:
        return default


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def pressure(text: str) -> float:
    for line in text.splitlines():
        if line.startswith("some "):
            return float(dict(x.split("=", 1) for x in line.split()[1:]).get("avg10", 0))
    return 0.0


def snapshot(proc: Path = Path("/proc"), cgroups: Path = Path("/sys/fs/cgroup")) -> dict:
    """Available allocation headroom, not host MemFree alone. Fail closed on missing data."""
    mem = {}
    for line in _read(proc / "meminfo").splitlines():
        key, value = line.split(":", 1)
        mem[key] = int(value.split()[0]) * 1024
    reserve = int(number("VERSION_COMPARISON_HOST_RESERVE_GB", 4) * GIB)
    available = max(0, mem.get("MemAvailable", 0) - reserve)
    psi = pressure(_read(proc / "pressure/memory"))
    limits = []
    for line in _read(proc / "self/cgroup").splitlines():
        if not line.startswith("0::"):
            continue
        current = cgroups / line[3:].lstrip("/")
        for directory in [current, *current.parents]:
            if directory == cgroups.parent or cgroups not in (directory, *directory.parents):
                break
            used = _read(directory / "memory.current")
            for name in ("memory.high", "memory.max"):
                raw = _read(directory / name)
                if raw.isdigit() and used.isdigit():
                    headroom = max(0, int(raw) - int(used) - 512 * MIB)
                    available = min(available, headroom)
                    limits.append({"path": str(directory), "limit": name,
                                   "bytes": int(raw), "used_bytes": int(used)})
            psi = max(psi, pressure(_read(directory / "memory.pressure")))
    return {"headroom_bytes": available, "pressure_avg10": psi,
            "host_available_bytes": mem.get("MemAvailable", 0), "limits": limits}


def estimate(operation: str, args: list) -> int:
    """Conservative reservation; runtime RSS enforcement catches underestimated tasks."""
    if operation == "probe":
        return 256 * MIB
    if operation == "sheet":
        task = args[0]
        dpi = float(task["params"]["dpi"])
        pages = [task[side]["profile"] for side in ("a", "b")]
        # The identical-content branch never allocates full-resolution rasters.
        if pages[0].get("content_hash") and pages[0]["content_hash"] == pages[1].get("content_hash"):
            return 384 * MIB
        # Cover both orientations, including portrait -> landscape pairs.
        pixels = (max(float(p["width"]) for p in pages)
                  * max(float(p["height"]) for p in pages) * (dpi / 72) ** 2)
        source_bytes = 0
        for side in ("a", "b"):
            try:
                source_bytes += Path(task[side]["pdf"]).stat().st_size
            except (OSError, KeyError):
                pass
        # RGB/gray, masks, labels and Lab temporaries are not all live at once.
        # 96 B/px plus 384 MiB covers the measured synthetic A4/A1/A0 paths;
        # PDF bytes and MuPDF document copies have a separate allowance.
        # This is an estimate: unusual alignment/content still uses the RSS
        # guard and one retry with the measured reservation, under the unchanged service limit.
        return int(384 * MIB + pixels * 96 + source_bytes * 4)
    return 2 * GIB  # document profiling / specification parsing


def rss_bytes(pid: int) -> int:
    try:
        return int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return 0


def can_start(cost: int, active: list[tuple[int, int]], budget: int, state: dict) -> bool:
    """Reserve expected future growth as well as the RSS already charged by the kernel."""
    if sum(reservation for reservation, _ in active) + cost > budget:
        return False
    growth = sum(max(0, reservation - rss) for reservation, rss in active)
    return (state["pressure_avg10"] < number("VERSION_COMPARISON_PRESSURE_MAX", 10, 1)
            and cost + growth <= state["headroom_bytes"])
