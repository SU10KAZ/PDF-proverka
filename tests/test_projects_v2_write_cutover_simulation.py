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
# Step 9/10: фасад ПОДКЛЮЧЁН к write-chokepoints (но default legacy → no-op)
# --------------------------------------------------------------------------

def test_write_facade_wired_to_expected_chokepoints():
    """Step 9/10: write-facade подключён к ожидаемым write-chokepoints через
    safe-обёртки (shadow_mirror_project_*_safe)."""
    expected = {
        "backend/app/services/common/project_service.py":
            ["shadow_mirror_project_path_safe"],
        "backend/app/services/common/version_service.py":
            ["shadow_mirror_project_id_safe"],
        "backend/app/services/knowledge_base/knowledge_base_service.py":
            ["shadow_mirror_project_id_safe"],
        "backend/app/pipeline/manager.py":
            ["shadow_mirror_project_id_safe"],
    }
    missing = []
    for rel, needles in expected.items():
        txt = (_REPO_ROOT / rel).read_text(encoding="utf-8", errors="ignore")
        if "storage_write_facade" not in txt:
            missing.append(f"{rel}: facade not imported")
        for n in needles:
            if n not in txt:
                missing.append(f"{rel}: missing {n}")
    assert missing == [], f"chokepoints not wired: {missing}"


def test_routers_do_not_directly_wire_write_facade():
    """Запись идёт через сервисы, не напрямую из routers — фасад не должен
    вызываться прямо в HTTP-слое (GET read-эндпоинты гарантированно не задеты)."""
    routers_dir = _REPO_ROOT / "backend" / "app" / "api" / "routers"
    offenders = [py.name for py in routers_dir.glob("*.py")
                 if "storage_write_facade" in py.read_text(encoding="utf-8", errors="ignore")]
    assert offenders == [], f"facade wired directly into routers: {offenders}"


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


def test_wired_chokepoint_legacy_mode_no_v2_write(monkeypatch, tmp_path):
    """Интеграция: реальный wired chokepoint (save_project_info) в режиме legacy
    работает как раньше и НЕ создаёт projects_v2 (read/write не регрессируют)."""
    import json as _json
    from backend.app.services.common import project_service as ps

    projects_dir = tmp_path / "projects"
    proj = projects_dir / "PRJ1"           # resolve_project_dir → projects_dir/<id>
    proj.mkdir(parents=True)
    (proj / "project_info.json").write_text(_json.dumps({"project_id": "PRJ1"}), encoding="utf-8")

    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "projects_v2"))
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)  # legacy

    ok = ps.save_project_info("PRJ1", {"project_id": "PRJ1", "name": "PRJ1", "x": 1})
    assert ok is True
    # legacy-файл записан
    assert _json.loads((proj / "project_info.json").read_text(encoding="utf-8"))["x"] == 1
    # v2 НЕ создан (legacy mode → hook no-op)
    assert not (tmp_path / "projects_v2").exists()
