"""
Тесты симулятора write-cutover + регрессионные гарантии (Step 8/10).

Покрывает:
  * scripts/projects_v2/simulate_write_cutover.py — dry-run проходит без
    нарушений инвариантов и с exit 0, НЕ трогая production projects/ и
    projects_v2/;
  * фасад записи НЕ подключён ни к одному production endpoint/router (write
    cutover ещё НЕ включён) — read-путь не может быть задет;
  * дефолты backend хранилища/режима записи не изменились (read endpoints не
    регрессируют): AUDIT_STORAGE_BACKEND default legacy, write mode default legacy.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SIM_PATH = _REPO_ROOT / "scripts" / "projects_v2" / "simulate_write_cutover.py"


def _load_sim_module():
    spec = importlib.util.spec_from_file_location("simulate_write_cutover", _SIM_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["simulate_write_cutover"] = mod  # нужно для dataclass-резолва
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------
# симулятор
# --------------------------------------------------------------------------

def test_simulation_script_exists():
    assert _SIM_PATH.is_file(), f"simulator missing at {_SIM_PATH}"


def test_simulation_all_invariants_hold(monkeypatch, tmp_path):
    # дефолтный режим записи (env не выставлен) — симулятор сам локально
    # переключает режимы через monkeypatch get_write_mode.
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)
    sim = _load_sim_module()

    v2root = tmp_path / "v2"
    report = [
        sim._scenario("legacy", v2root / "legacy"),
        sim._scenario("dual_write_shadow", v2root / "shadow_ok"),
        sim._scenario("dual_write_shadow", v2root / "shadow_fail", fail_v2=True),
        sim._scenario("projects_v2_primary", v2root / "v2_primary"),
    ]
    violations = sim._check(report)
    assert violations == [], f"invariant violations: {violations}"

    # legacy сценарий не создал ни одного v2-файла
    assert not (v2root / "legacy" / "objects").exists()
    # shadow_ok создал v2-дерево
    assert (v2root / "shadow_ok" / "objects").exists()


def test_simulation_main_exit_zero(monkeypatch):
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)
    sim = _load_sim_module()
    # main() создаёт собственный tempdir и сам его чистит; должен вернуть 0
    monkeypatch.setattr(sys, "argv", ["simulate_write_cutover.py"])
    assert sim.main() == 0


def test_simulation_does_not_touch_production(monkeypatch):
    """Симулятор пишет только в системный tmp, не в репозиторий/прод."""
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)
    sim = _load_sim_module()
    monkeypatch.setattr(sys, "argv", ["simulate_write_cutover.py", "--keep"])

    captured = {}
    real_print = print

    def _cap(*a, **k):
        # перехватываем человекочитаемый вывод, чтобы найти temp-путь
        line = " ".join(str(x) for x in a)
        if "temp=" in line:
            captured["line"] = line
        real_print(*a, **k)

    monkeypatch.setattr("builtins.print", _cap)
    rc = sim.main()
    assert rc == 0
    # temp-каталог обязан быть вне репозитория
    assert "temp=" in captured.get("line", "")
    temp_str = captured["line"].split("temp=")[1].rstrip(") ")
    assert _REPO_ROOT not in Path(temp_str).resolve().parents


# --------------------------------------------------------------------------
# регрессия: фасад НЕ подключён к production (read endpoints не задеты)
# --------------------------------------------------------------------------

def test_write_facade_not_wired_to_routers():
    """Ни один router/endpoint не должен импортировать storage_write_facade на
    этом этапе — write cutover ещё не включён, read-путь не может регрессировать."""
    routers_dir = _REPO_ROOT / "backend" / "app" / "api" / "routers"
    offenders = []
    for py in routers_dir.glob("*.py"):
        txt = py.read_text(encoding="utf-8", errors="ignore")
        if "storage_write_facade" in txt:
            offenders.append(py.name)
    assert offenders == [], f"storage_write_facade wired into routers: {offenders}"


def test_write_facade_not_wired_to_pipeline_or_services():
    """Pipeline manager / common services тоже не вызывают фасад (не включён)."""
    targets = [
        _REPO_ROOT / "backend" / "app" / "pipeline" / "manager.py",
        _REPO_ROOT / "backend" / "app" / "services" / "common" / "project_service.py",
        _REPO_ROOT / "backend" / "app" / "services" / "common" / "version_service.py",
    ]
    offenders = []
    for t in targets:
        if t.is_file() and "storage_write_facade" in t.read_text(encoding="utf-8", errors="ignore"):
            offenders.append(t.name)
    assert offenders == [], f"storage_write_facade wired into: {offenders}"


def test_storage_backend_default_unchanged(monkeypatch):
    """Read-backend default остаётся legacy (importing write facade ничего не меняет)."""
    from backend.app.services.storage import projects_v2_adapter as adp
    from backend.app.services.storage import storage_write_facade as swf

    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)
    assert adp.get_storage_backend() == "legacy"
    assert adp.is_v2_backend_enabled() is False
    assert swf.get_write_mode() == "legacy"
    assert swf.v2_writes_enabled() is False
