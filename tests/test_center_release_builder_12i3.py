"""12I.3 — сборщик релиза принадлежит репозиторию, а не каталогу /tmp.

До этого этапа боевые релизы собирал скрипт из `/tmp` (`build_release_v2…v5`).
Процесс не воспроизводился из коммита: инварианты релиза жили в файле, которого
нет ни в одной истории, и исчезли бы вместе с tmpfs при первой перезагрузке.

Здесь проверяется, что канонический вход существует, соблюдает инварианты и —
главное — что временное дерево исчезает при ЛЮБОМ исходе. Именно неудаляемое
запечатанное дерево на 1 ГБ забивало tmpfs на 32 ГБ и останавливало на машине
всё, чему нужен временный файл.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts import build_center_release as builder  # noqa: E402
from scripts.release_staging import STAGING_PREFIX, staging_workspace  # noqa: E402


def test_canonical_builder_is_repository_owned():
    path = _ROOT / "scripts" / "build_center_release.py"
    assert path.is_file()
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "scripts/build_center_release.py"],
        cwd=_ROOT, capture_output=True, text=True,
    )
    assert tracked.returncode == 0, "канонический сборщик обязан быть в индексе git"


def test_builder_does_not_depend_on_tmp_scripts():
    """Проверяется КОД, а не пояснения: в docstring про /tmp сказано намеренно."""
    import ast

    path = _ROOT / "scripts" / "build_center_release.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    tree.body = [node for node in tree.body
                 if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))]
    code = ast.unparse(tree)
    assert "/tmp/" not in code, "боевая сборка не имеет права зависеть от /tmp"
    assert "build_release_v" not in code
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert imported <= {
        "argparse", "datetime", "hashlib", "json", "os", "shutil", "stat",
        "subprocess", "sys", "pathlib", "typing", "scripts", "__future__",
    }, f"неожиданная зависимость сборщика: {imported}"


def test_builder_takes_the_deploy_lock_before_touching_releases():
    source = (_ROOT / "scripts" / "build_center_release.py").read_text(encoding="utf-8")
    assert "deploy_lock(" in source, "две сессии не должны собирать один release_id"


def test_required_paths_include_the_release_machinery():
    assert "scripts/release_staging.py" in builder.REQUIRED_PATHS
    assert "scripts/deploy_lock.py" in builder.REQUIRED_PATHS
    assert "scripts/build_center_release.py" in builder.REQUIRED_PATHS


def test_fileset_digest_reflects_content_not_names(tmp_path):
    root = tmp_path / "app"
    (root / "pkg").mkdir(parents=True)
    (root / "pkg" / "a.py").write_text("x = 1\n", encoding="utf-8")
    first = builder.fileset_digest(root)
    assert first == builder.fileset_digest(root), "отпечаток обязан быть устойчив"
    (root / "pkg" / "a.py").write_text("x = 2\n", encoding="utf-8")
    assert builder.fileset_digest(root) != first


def test_fileset_digest_catches_a_repointed_symlink(tmp_path):
    """Подмена кода без единого изменённого байта в обычных файлах."""
    root = tmp_path / "app"
    root.mkdir()
    (root / "real").write_text("data", encoding="utf-8")
    (root / "other").write_text("evil", encoding="utf-8")
    (root / "link").symlink_to(root / "real")
    before = builder.fileset_digest(root)
    (root / "link").unlink()
    (root / "link").symlink_to(root / "other")
    assert builder.fileset_digest(root) != before


def test_fileset_digest_catches_a_mode_change(tmp_path):
    """Снятый бит чтения для прочих останавливает шлюз, не тронув содержимое."""
    root = tmp_path / "app"
    root.mkdir()
    target = root / "mod.py"
    target.write_text("x = 1\n", encoding="utf-8")
    target.chmod(0o444)
    before = builder.fileset_digest(root)
    target.chmod(0o440)
    assert builder.fileset_digest(root) != before


# ═════════════ Жизненный цикл staging через настоящий контекст ═══════════════
def _sealed_tree(root: Path) -> None:
    from scripts.release_staging import seal_tree

    (root / "app").mkdir(parents=True, exist_ok=True)
    (root / "app" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    seal_tree(root)


def test_success_removes_staging(tmp_path):
    with staging_workspace(parent=tmp_path) as staging:
        _sealed_tree(staging / "release")
        captured = staging
    assert not captured.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("seal", [False, True])
def test_failure_before_and_after_sealing_removes_staging(tmp_path, seal):
    captured = None
    with pytest.raises(RuntimeError):
        with staging_workspace(parent=tmp_path) as staging:
            captured = staging
            target = staging / "release"
            target.mkdir()
            if seal:
                _sealed_tree(target)
            raise RuntimeError("сборка отказала")
    assert captured is not None and not captured.exists()
    assert list(tmp_path.iterdir()) == []


def test_multiple_consecutive_failures_leak_nothing(tmp_path):
    for attempt in range(5):
        with pytest.raises(RuntimeError):
            with staging_workspace(parent=tmp_path) as staging:
                _sealed_tree(staging / "release")
                raise RuntimeError(f"отказ {attempt}")
    assert [p for p in tmp_path.iterdir() if p.name.startswith(STAGING_PREFIX)] == []
    assert sum(p.stat().st_size for p in tmp_path.rglob("*") if p.is_file()) == 0


def test_builder_fails_loudly_if_cleanup_could_not_finish(tmp_path, monkeypatch):
    """`ignore_errors=True` молча объявляет успех — это обязано быть замечено."""
    import shutil

    from scripts import release_staging

    monkeypatch.setattr(shutil, "rmtree", lambda *a, **k: None)
    staging = tmp_path / f"{STAGING_PREFIX}stuck"
    staging.mkdir()
    (staging / "file").write_text("x", encoding="utf-8")
    with pytest.raises(RuntimeError, match="не удалён"):
        release_staging.cleanup_staging(staging)


def test_existing_production_release_is_never_removed(tmp_path):
    from scripts.release_staging import cleanup_staging

    production = tmp_path / "ui-real-a5c47dc6"
    production.mkdir()
    (production / "release-manifest.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError):
        cleanup_staging(production)
    assert (production / "release-manifest.json").is_file()


def test_release_identity_reuse_with_different_commit_is_refused(tmp_path, monkeypatch):
    """Один идентификатор с другим содержимым — подмена уже выданного имени."""
    releases = tmp_path / "releases"
    base = releases / "base"
    (base).mkdir(parents=True)
    (base / "release-manifest.json").write_text(
        json.dumps({"git_tree_sha1": "t", "database_schema": {"target": 13}}),
        encoding="utf-8")
    monkeypatch.setattr(builder, "run", lambda *a, **k: {
        ("git", "rev-parse", "HEAD"): "a" * 40,
        ("git", "status", "--porcelain"): "",
        ("git", "rev-parse", "HEAD^"): "b" * 40,
        ("git", "rev-parse", "HEAD^{tree}"): "c" * 40,
    }.get(a, ""))
    existing = releases / "ui-real-aaaaaaaa"
    existing.mkdir()
    (existing / "release-manifest.json").write_text(
        json.dumps({"commit": "d" * 40}), encoding="utf-8")
    with pytest.raises(SystemExit, match="переиспользование"):
        builder.build(base_release="base", kind="k", notes="",
                      releases_dir=releases, tests=())


def test_dirty_worktree_is_refused(tmp_path, monkeypatch):
    releases = tmp_path / "releases"
    (releases / "base").mkdir(parents=True)
    (releases / "base" / "release-manifest.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(builder, "run", lambda *a, **k: (
        "a" * 40 if a == ("git", "rev-parse", "HEAD") else " M file"
    ))
    with pytest.raises(SystemExit, match="грязн"):
        builder.build(base_release="base", kind="k", notes="", releases_dir=releases,
                      tests=())


# ═════ Переключатель боевого релиза ═════════════════════════════════════════
import scripts.deploy_center_release as deployer  # noqa: E402


def _release(root: Path, release_id: str, *, digest_ok: bool = True) -> Path:
    """Каталог релиза, достаточный для предпроверок (без venv-исполнения)."""
    release = root / "releases" / release_id
    app = release / "app"
    (app / "contracts/agent_stream/v1").mkdir(parents=True)
    for name in ("agent_stream_v1.desc", "agent_stream.proto", "common.proto"):
        (app / "contracts/agent_stream/v1" / name).write_text(name, encoding="utf-8")
    (app / "mod.py").write_text("x = 1\n", encoding="utf-8")
    manifest = {
        "release_id": release_id,
        "database_schema": {"target": 13},
        "fileset_sha256": builder.fileset_digest(app),
    }
    if not digest_ok:
        manifest["fileset_sha256"] = "0" * 64
    (release / "release-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    (release / "venv" / "bin").mkdir(parents=True)
    (release / "venv/bin/python").symlink_to(sys.executable)
    return release


def _gateway(root: Path, source: Path) -> Path:
    gateway = root / "gateway"
    (gateway / "app/contracts/agent_stream/v1").mkdir(parents=True)
    for name in ("agent_stream_v1.desc", "agent_stream.proto", "common.proto"):
        shutil.copy2(source / "app/contracts/agent_stream/v1" / name,
                     gateway / "app/contracts/agent_stream/v1" / name)
    return gateway


def test_deploy_precheck_catches_a_release_edited_after_the_build(tmp_path):
    """Между сборкой и выкаткой каталог могли поправить «на живую».

    Ни импорт, ни HTTP 200 такого не заметят: 200 отдаёт любой процесс на
    порту. Ловит только пересчёт отпечатка.
    """
    release = _release(tmp_path, "ui-real-deadbeef")
    gateway = _gateway(tmp_path, release)
    clean = deployer.prechecks(release, gateway)
    assert not any("не совпадает с манифестом" in item for item in clean), clean
    (release / "app" / "mod.py").chmod(0o644)
    (release / "app" / "mod.py").write_text("x = 666\n", encoding="utf-8")
    problems = deployer.prechecks(release, gateway)
    assert any("не совпадает с манифестом" in item for item in problems), problems


def test_deploy_precheck_refuses_a_schema_migration(tmp_path):
    release = _release(tmp_path, "ui-real-cafe0001")
    manifest = json.loads((release / "release-manifest.json").read_text(encoding="utf-8"))
    manifest["database_schema"] = {"target": 14}
    (release / "release-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    problems = deployer.prechecks(release, _gateway(tmp_path, release))
    assert any("миграция запрещена" in item for item in problems), problems


def test_deploy_precheck_catches_a_wire_split_with_the_running_gateway(tmp_path):
    release = _release(tmp_path, "ui-real-cafe0002")
    gateway = _gateway(tmp_path, release)
    (gateway / "app/contracts/agent_stream/v1/common.proto").write_text(
        "изменённый контракт", encoding="utf-8")
    problems = deployer.prechecks(release, gateway)
    assert any("провод разошёлся со шлюзом" in item for item in problems), problems


def test_gateway_directory_cannot_be_overridden_away_from_the_running_unit(monkeypatch,
                                                                          tmp_path):
    """Сверка со шлюзом НЕ отключаема параметром командной строки."""
    running = tmp_path / "running-gateway"
    running.mkdir()
    monkeypatch.setattr(deployer, "running_gateway_release_dir", lambda: running)
    with pytest.raises(SystemExit) as caught:
        deployer.deploy("ui-real-cafe0003", gateway_release_dir=str(tmp_path / "elsewhere"))
    assert "не совпадает с работающим шлюзом" in str(caught.value)


def test_deploy_touches_nothing_when_prechecks_fail(monkeypatch, tmp_path):
    """Отказ предпроверки не имеет права стоить простоя."""
    monkeypatch.setattr(deployer, "ROOT", tmp_path)
    (tmp_path / "releases").mkdir()
    (tmp_path / "current").symlink_to(tmp_path / "releases")
    monkeypatch.setattr(deployer, "running_gateway_release_dir", lambda: tmp_path)
    monkeypatch.setattr(deployer, "prechecks", lambda *a, **k: ["выдуманная беда"])
    switched = []
    monkeypatch.setattr(deployer, "_switch", lambda target: switched.append(target))
    monkeypatch.setattr(deployer, "_systemctl_user",
                        lambda *a: pytest.fail("рестарт до прохождения предпроверок"))
    monkeypatch.setenv("AUDITMANAGER_DEPLOY_LOCK_DIR", str(tmp_path / "locks"))
    with pytest.raises(SystemExit):
        deployer.deploy("ui-real-cafe0004")
    assert switched == [], "боевой указатель тронут при неудачной предпроверке"


def test_failed_restart_rolls_back_before_releasing_the_lock(monkeypatch, tmp_path):
    """Отказ САМОГО restart раньше пролетал мимо отката.

    Новый указатель оставался активным, backend мог быть уже остановлен, а
    замок снимался — то есть чужая выкатка входила в незастабилизированное
    состояние.
    """
    monkeypatch.setattr(deployer, "ROOT", tmp_path)
    releases = tmp_path / "releases"
    (releases / "old").mkdir(parents=True)
    (releases / "new").mkdir(parents=True)
    (tmp_path / "current").symlink_to(releases / "old")
    monkeypatch.setattr(deployer, "running_gateway_release_dir", lambda: tmp_path)
    monkeypatch.setattr(deployer, "prechecks", lambda *a, **k: [])
    monkeypatch.setattr(deployer, "_health", lambda *a, **k: 200)
    monkeypatch.setattr(deployer, "running_release_dir", lambda: None)
    monkeypatch.setenv("AUDITMANAGER_DEPLOY_LOCK_DIR", str(tmp_path / "locks"))

    switches = []
    real_switch = deployer._switch

    def _switch(target):
        switches.append(Path(target).name)
        real_switch(target)

    monkeypatch.setattr(deployer, "_switch", _switch)

    calls = {"n": 0}

    def _systemctl(*args):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("systemctl restart failed")
        return ""

    monkeypatch.setattr(deployer, "_systemctl_user", _systemctl)
    with pytest.raises(SystemExit):
        deployer.deploy("new")
    assert switches == ["new", "old"], "откат обязан вернуть прежний релиз"
    assert os.readlink(tmp_path / "current").endswith("old")
    # Замок снят только ПОСЛЕ отката — иначе он бы не освободился к этому месту.
    with deploy_lock_ctx(tmp_path / "locks"):
        pass


def test_health_200_alone_does_not_prove_the_new_release_is_running(monkeypatch, tmp_path):
    """200 отдаёт любой процесс на порту, в том числе переживший рестарт старый."""
    monkeypatch.setattr(deployer, "ROOT", tmp_path)
    releases = tmp_path / "releases"
    (releases / "old").mkdir(parents=True)
    (releases / "new").mkdir(parents=True)
    (tmp_path / "current").symlink_to(releases / "old")
    monkeypatch.setattr(deployer, "running_gateway_release_dir", lambda: tmp_path)
    monkeypatch.setattr(deployer, "prechecks", lambda *a, **k: [])
    monkeypatch.setattr(deployer, "_health", lambda *a, **k: 200)
    monkeypatch.setattr(deployer, "_systemctl_user", lambda *a: "")
    monkeypatch.setattr(deployer, "running_release_dir", lambda: releases / "old")
    monkeypatch.setenv("AUDITMANAGER_DEPLOY_LOCK_DIR", str(tmp_path / "locks"))
    with pytest.raises(SystemExit):
        deployer.deploy("new")
    assert os.readlink(tmp_path / "current").endswith("old"), "должен быть откат"


from contextlib import contextmanager  # noqa: E402


@contextmanager
def deploy_lock_ctx(lock_dir: Path):
    from scripts.deploy_lock import COMPONENT_CENTER, deploy_lock

    with deploy_lock(COMPONENT_CENTER, operation="probe", lock_dir=lock_dir) as path:
        yield path
