"""Поведенческие проверки стража происхождения прод-кода (этап 12J.1).

Страж существует из-за двух одинаковых инцидентов 18.08.2026: релиз центра
собирался и переключался из локального коммита, которого не было на origin
(78199ef7, затем 08666e4d), и исходник боевого кода оставался только в клоне
под /tmp. Здесь проверяется именно ПОВЕДЕНИЕ на этих сценариях, а не наличие
функций.

Все проверки офлайн: каноническим remote выступает bare-репозиторий в tmp,
подставленный через AUDITMANAGER_CANONICAL_PRODUCTION_REMOTE/BRANCH. Сеть не
нужна, а логика проверяется та же самая.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.production_source_guard import (  # noqa: E402
    ProductionSourceNotCanonical,
    verify_production_source,
)

#: Каноническая ветка прод-истины. С 19.08.2026 — `main`; фикстуры создают
#: ветку именно с этим именем, чтобы тест ломался, если умолчание стража
#: разъедется с реальностью.
CANONICAL_BRANCH = "main"
OLD_CANONICAL_BRANCH = "feature/block-vector-graphs"


def _git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)
    assert proc.returncode == 0, f"git {' '.join(args)}: {proc.stderr}"
    return proc.stdout.strip()


@pytest.fixture()
def fixture_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Клон с каноническим bare-remote: один опубликованный коммит на ветке."""
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-q", str(origin)], check=True)

    work = tmp_path / "work"
    subprocess.run(["git", "init", "-q", "-b", CANONICAL_BRANCH, str(work)], check=True)
    _git(work, "config", "user.email", "guard@test")
    _git(work, "config", "user.name", "guard")
    _git(work, "remote", "add", "origin", str(origin))

    (work / "backend").mkdir()
    (work / "backend" / "app.py").write_text("print('v1')\n", encoding="utf-8")
    _git(work, "add", "-A")
    _git(work, "commit", "-qm", "первый опубликованный коммит")
    _git(work, "push", "-q", "origin", CANONICAL_BRANCH)

    monkeypatch.setenv("AUDITMANAGER_CANONICAL_PRODUCTION_REMOTE", "origin")
    monkeypatch.setenv("AUDITMANAGER_CANONICAL_PRODUCTION_BRANCH", CANONICAL_BRANCH)
    return work


def _verify(work: Path, **kw):
    return verify_production_source(
        work, remote="origin", branch=CANONICAL_BRANCH, **kw
    )


# ─────────────────────────── A. опубликованный коммит ───────────────────────

def test_a_published_canonical_commit_passes(fixture_repo: Path) -> None:
    receipt = _verify(fixture_repo)
    assert receipt["reachable_from_canonical_remote"] is True
    assert receipt["source_commit"] == _git(fixture_repo, "rev-parse", "HEAD")
    assert receipt["canonical_branch"] == CANONICAL_BRANCH
    assert receipt["clean_tree"]["checked"] is True


def test_a_production_release_lagging_behind_branch_head_still_passes(
    fixture_repo: Path,
) -> None:
    """Прод часто отстаёт от головы ветки — это норма, а не отказ."""
    old = _git(fixture_repo, "rev-parse", "HEAD")
    (fixture_repo / "backend" / "app.py").write_text("print('v2')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "второй коммит")
    _git(fixture_repo, "push", "-q", "origin", CANONICAL_BRANCH)

    receipt = _verify(fixture_repo, commit=old)
    assert receipt["source_commit"] == old
    assert receipt["reachable_from_canonical_remote"] is True


# ────────────────────── B. неопубликованный локальный коммит ────────────────

def test_b_unpublished_local_commit_is_refused(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "app.py").write_text("print('local')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "локальный коммит без push")
    local = _git(fixture_repo, "rev-parse", "HEAD")

    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "commit_not_published"
    assert "PRODUCTION_SOURCE_NOT_CANONICAL" in str(exc.value)
    assert local[:12] in str(exc.value)
    # Текст обязан подсказывать ПОРЯДОК, иначе оператор в инциденте будет
    # угадывать, что от него хотят.
    assert "PUSH" in str(exc.value)


def test_b_unpublished_main_head_is_allowed_only_as_build_candidate(
    fixture_repo: Path,
) -> None:
    (fixture_repo / "backend" / "app.py").write_text(
        "print('local build')\n", encoding="utf-8"
    )
    _git(fixture_repo, "commit", "-aqm", "локальный кандидат сборки")

    receipt = _verify(fixture_repo, allow_local_ahead_build=True)

    assert receipt["reachable_from_canonical_remote"] is False
    assert receipt["local_build_candidate"] is True
    assert receipt["publication_required_before_deploy"] is True
    assert receipt["local_ahead_remote"] == 1


def test_b_build_candidate_rejects_unpublished_feature_head(
    fixture_repo: Path,
) -> None:
    _git(fixture_repo, "checkout", "-q", "-b", "feat/local-build")
    (fixture_repo / "backend" / "app.py").write_text(
        "print('feature')\n", encoding="utf-8"
    )
    _git(fixture_repo, "commit", "-aqm", "кандидат не из main")

    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo, allow_local_ahead_build=True)
    assert exc.value.reason == "commit_not_published"


def test_b_build_candidate_rejects_diverged_main(
    fixture_repo: Path, tmp_path: Path,
) -> None:
    clone = tmp_path / "publisher"
    origin = _git(fixture_repo, "remote", "get-url", "origin")
    subprocess.run(
        ["git", "clone", "-q", "-b", CANONICAL_BRANCH, origin, str(clone)],
        check=True,
    )
    _git(clone, "config", "user.email", "publisher@test")
    _git(clone, "config", "user.name", "publisher")
    (clone / "backend" / "remote.py").write_text("remote = 1\n", encoding="utf-8")
    _git(clone, "add", "backend/remote.py")
    _git(clone, "commit", "-qm", "удалённое изменение")
    _git(clone, "push", "-q", "origin", CANONICAL_BRANCH)

    (fixture_repo / "backend" / "local.py").write_text("local = 1\n", encoding="utf-8")
    _git(fixture_repo, "add", "backend/local.py")
    _git(fixture_repo, "commit", "-qm", "локальное изменение")

    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo, allow_local_ahead_build=True)
    assert exc.value.reason == "commit_not_published"


# ─────────────────── C. коммит на посторонней feature-ветке ─────────────────

def test_c_commit_on_unrelated_branch_is_refused(fixture_repo: Path) -> None:
    _git(fixture_repo, "checkout", "-q", "-b", "feat/unrelated")
    (fixture_repo / "backend" / "app.py").write_text("print('side')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "работа на посторонней ветке")
    # Ветка ОПУБЛИКОВАНА, но не каноническая: публикация сама по себе не делает
    # коммит прод-истиной.
    _git(fixture_repo, "push", "-q", "origin", "feat/unrelated")

    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "commit_not_published"


# ──────────── D. коммит из отдельного клона, не влитый в канонику ───────────

def test_d_commit_from_separate_clone_is_refused(
    fixture_repo: Path, tmp_path: Path
) -> None:
    """Ровно инцидент 18.08: релиз из клона /tmp, коммита нет в канонической ветке."""
    clone = tmp_path / "tmp-clone"
    subprocess.run(["git", "clone", "-q", str(fixture_repo), str(clone)], check=True)
    _git(clone, "config", "user.email", "clone@test")
    _git(clone, "config", "user.name", "clone")
    _git(clone, "checkout", "-q", "-b", "ui/sidebar-all-sections-counter")
    (clone / "backend" / "app.py").write_text("print('clone-only')\n", encoding="utf-8")
    _git(clone, "commit", "-aqm", "правка, из которой собран боевой релиз")
    clone_commit = _git(clone, "rev-parse", "HEAD")

    # Объект физически доставлен в дерево-источник (как я сделал через remote
    # liveprod), но в каноническую ветку не влит — этого недостаточно.
    _git(fixture_repo, "fetch", "-q", str(clone),
         "+refs/heads/*:refs/remotes/liveclone/*")
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo, commit=clone_commit)
    assert exc.value.reason == "commit_not_published"

    # А после слияния в каноническую ветку и push — тот же коммит проходит.
    _git(fixture_repo, "merge", "-q", "--no-edit", clone_commit)
    _git(fixture_repo, "push", "-q", "origin", CANONICAL_BRANCH)
    receipt = _verify(fixture_repo, commit=clone_commit)
    assert receipt["reachable_from_canonical_remote"] is True


# ──────────────────────── E. грязное дерево-источник ───────────────────────

def test_e_dirty_tracked_file_is_refused(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "app.py").write_text("print('uncommitted')\n",
                                                     encoding="utf-8")
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "dirty_worktree"
    assert "backend/app.py" in str(exc.value)


def test_e_staged_but_uncommitted_is_refused(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "new.py").write_text("x = 1\n", encoding="utf-8")
    _git(fixture_repo, "add", "backend/new.py")
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "staged_changes"


def test_e_untracked_file_in_code_dir_is_refused(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "orphan.py").write_text("y = 2\n", encoding="utf-8")
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "untracked_build_files"
    assert "backend/orphan.py" in str(exc.value)


def test_e_harmless_untracked_work_files_do_not_block(fixture_repo: Path) -> None:
    """Грязная РАЗРАБОТКА разрешена: заметки и отчёты в прод не едут."""
    (fixture_repo / ".tmp_audit_tracker.md").write_text("рабочие заметки\n",
                                                        encoding="utf-8")
    (fixture_repo / "ПД-00542664-СКД оптимизация.md").write_text("отчёт\n",
                                                                 encoding="utf-8")
    (fixture_repo / "docs").mkdir(exist_ok=True)
    (fixture_repo / "docs" / "evidence.json").write_text("{}\n", encoding="utf-8")
    (fixture_repo / "deliverables").mkdir(exist_ok=True)
    (fixture_repo / "deliverables" / "report.xlsx").write_text("x", encoding="utf-8")

    receipt = _verify(fixture_repo)
    assert receipt["clean_tree"]["untracked_blocking"] == 0
    assert receipt["clean_tree"]["untracked_total"] >= 4


def test_e_explicit_allowlist_can_release_one_path(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "generated.py").write_text("z = 3\n", encoding="utf-8")
    with pytest.raises(ProductionSourceNotCanonical):
        _verify(fixture_repo)
    receipt = _verify(fixture_repo, allow_untracked=["backend/generated.py"])
    assert receipt["clean_tree"]["untracked_blocking"] == 0


# ───────────────────── свежесть канонической ссылки ────────────────────────

def test_stale_remote_tracking_ref_does_not_authorise(
    fixture_repo: Path, tmp_path: Path
) -> None:
    """Отказ по УСТАРЕВШЕМУ снимку remote — без fetch страж бесполезен."""
    (fixture_repo / "backend" / "app.py").write_text("print('v2')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "второй коммит")
    _git(fixture_repo, "push", "-q", "origin", CANONICAL_BRANCH)
    head = _git(fixture_repo, "rev-parse", "HEAD")

    # Ссылка искусственно отброшена назад: с --no-fetch страж поверил бы ей.
    _git(fixture_repo, "update-ref",
         f"refs/remotes/origin/{CANONICAL_BRANCH}", f"{head}^")
    with pytest.raises(ProductionSourceNotCanonical):
        _verify(fixture_repo, fetch=False)
    # С fetch — тот же коммит проходит, потому что он действительно опубликован.
    assert _verify(fixture_repo)["reachable_from_canonical_remote"] is True


def test_unreachable_remote_fails_closed(fixture_repo: Path, tmp_path: Path) -> None:
    _git(fixture_repo, "remote", "set-url", "origin", str(tmp_path / "нет-такого.git"))
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo)
    assert exc.value.reason == "remote_unreachable"


def test_missing_canonical_branch_fails_closed(fixture_repo: Path) -> None:
    with pytest.raises(ProductionSourceNotCanonical) as exc:
        verify_production_source(fixture_repo, remote="origin", branch="нет/ветки")
    assert exc.value.reason in ("remote_unreachable", "canonical_ref_missing")


# ──────────────── F. страж прошёл, но замок держит другой ──────────────────

def test_f_canonical_source_plus_held_lock_still_refuses(
    fixture_repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Страж и замок — независимые условия, обязательны ОБА."""
    from scripts import deploy_lock as lock_module

    monkeypatch.setattr(lock_module, "DEFAULT_LOCK_DIR", tmp_path / "locks")
    assert _verify(fixture_repo)["reachable_from_canonical_remote"] is True

    with lock_module.deploy_lock(lock_module.COMPONENT_CENTER, operation="deploy",
                                 release="held-by-other"):
        with pytest.raises(lock_module.DeployLockHeld) as exc:
            child = subprocess.run(
                [sys.executable, "-c",
                 "import sys; sys.path.insert(0, %r);"
                 "from scripts.deploy_lock import deploy_lock, COMPONENT_CENTER;"
                 "import os;"
                 "os.environ['AUDITMANAGER_DEPLOY_LOCK_DIR']=%r;"
                 "from scripts import deploy_lock as m;"
                 "m.DEFAULT_LOCK_DIR=__import__('pathlib').Path(%r);"
                 "ctx=m.deploy_lock(COMPONENT_CENTER, operation='deploy');"
                 "ctx.__enter__()"
                 % (str(REPO_ROOT), str(tmp_path / "locks"), str(tmp_path / "locks"))],
                capture_output=True, text=True,
            )
            if child.returncode != 0:
                raise lock_module.DeployLockHeld(
                    "DEPLOY_LOCK_HELD: " + (child.stderr or "").strip()[-200:]
                )
        assert "DEPLOY_LOCK_HELD" in str(exc.value)


# ───────── G. после обычного push в каноническую ветку — проходит ──────────

def test_g_after_push_to_canonical_remote_it_passes(fixture_repo: Path) -> None:
    (fixture_repo / "backend" / "app.py").write_text("print('fix')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "исправление")
    with pytest.raises(ProductionSourceNotCanonical):
        _verify(fixture_repo)
    _git(fixture_repo, "push", "-q", "origin", CANONICAL_BRANCH)
    assert _verify(fixture_repo)["reachable_from_canonical_remote"] is True


# ───── H. отказ стража = ни переключения симлинка, ни рестарта сервиса ─────

def test_h_guard_failure_switches_nothing_and_restarts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import deploy_center_release as deployer

    root = tmp_path / "auditmanager"
    (root / "releases" / "ui-real-deadbeef").mkdir(parents=True)
    (root / "deploy-receipts").mkdir(parents=True)
    previous = root / "releases" / "ui-real-previous"
    previous.mkdir()
    (root / "current").symlink_to(previous)
    (root / "releases" / "ui-real-deadbeef" / "release-manifest.json").write_text(
        json.dumps({"commit": "dead" * 10, "release_id": "ui-real-deadbeef"}),
        encoding="utf-8",
    )

    calls: list[str] = []
    monkeypatch.setattr(deployer, "ROOT", root)
    monkeypatch.setattr(deployer, "running_gateway_release_dir", lambda: tmp_path)
    monkeypatch.setattr(deployer, "_switch",
                        lambda target: calls.append(f"switch:{target}"))
    monkeypatch.setattr(deployer, "_systemctl_user",
                        lambda *a: calls.append(f"systemctl:{a}") or "")
    monkeypatch.setattr(deployer, "prechecks",
                        lambda *a, **k: pytest.fail("страж обязан отказать ДО предпроверок"))
    monkeypatch.setattr(
        deployer, "verify_production_source",
        lambda *a, **k: (_ for _ in ()).throw(
            ProductionSourceNotCanonical("commit_not_published", "тестовый отказ")
        ),
    )

    with pytest.raises(ProductionSourceNotCanonical):
        deployer.deploy("ui-real-deadbeef", milestone="test")

    assert calls == [], f"прод тронут при отказе стража: {calls}"
    assert os.readlink(root / "current") == str(previous)
    assert list((root / "deploy-receipts").iterdir()) == []


# ───────────────────── обязательность обвязки в инструментах ───────────────

@pytest.mark.parametrize("tool", [
    "scripts/build_center_release.py",
    "scripts/deploy_center_release.py",
    "scripts/deploy_audit_worker.py",
])
def test_every_production_tool_calls_the_guard(tool: str) -> None:
    src = (REPO_ROOT / tool).read_text(encoding="utf-8")
    assert "production_source_guard" in src, f"{tool} не подключает стража"
    assert ("verify_production_source" in src
            or "_guard_production_source" in src), f"{tool} не ВЫЗЫВАЕТ стража"


def test_guard_runs_before_the_deploy_lock_in_center_deploy() -> None:
    """Порядок: страж → замок. Падать после захвата замка — держать чужую выкатку."""
    src = (REPO_ROOT / "scripts" / "deploy_center_release.py").read_text(encoding="utf-8")
    body = src[src.index("def deploy("):]
    assert body.index("_verify_release_source(") < body.index("deploy_lock(")


def test_guard_runs_before_the_deploy_lock_in_center_build() -> None:
    src = (REPO_ROOT / "scripts" / "build_center_release.py").read_text(encoding="utf-8")
    body = src[src.index("def build("):]
    assert body.index("verify_production_source(") < body.index("deploy_lock(")
    assert "allow_local_ahead_build=True" in body


def test_guard_module_has_no_environment_bypass() -> None:
    """Обхода «выключить стража» быть не должно — иначе он ритуал, а не запрет."""
    src = (REPO_ROOT / "scripts" / "production_source_guard.py").read_text(encoding="utf-8")
    for hole in ("GUARD_DISABLED", "SKIP_GUARD", "GUARD_SKIP", "FORCE_DEPLOY",
                 "ALLOW_UNPUBLISHED"):
        assert hole not in src, f"в страже найден обход {hole}"


# ───────── обёртка переключателя: коммит берётся из манифеста релиза ────────

def test_release_without_manifest_cannot_be_switched(tmp_path: Path) -> None:
    from scripts import deploy_center_release as deployer

    empty = tmp_path / "ui-real-nomanifest"
    empty.mkdir()
    with pytest.raises(SystemExit, match="нет манифеста релиза"):
        deployer._verify_release_source(empty)


def test_release_manifest_without_commit_cannot_be_switched(tmp_path: Path) -> None:
    """Релиз без поля commit недоказуем — переключение запрещено."""
    from scripts import deploy_center_release as deployer

    rel = tmp_path / "ui-real-nocommit"
    rel.mkdir()
    (rel / "release-manifest.json").write_text(json.dumps({"release_id": "x"}),
                                               encoding="utf-8")
    with pytest.raises(SystemExit, match="нет поля commit"):
        deployer._verify_release_source(rel)


def test_release_source_wrapper_forwards_manifest_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Проверяется КОММИТ РЕЛИЗА, а не текущий HEAD дерева выкатки."""
    from scripts import deploy_center_release as deployer

    rel = tmp_path / "ui-real-08666e4d"
    rel.mkdir()
    (rel / "release-manifest.json").write_text(
        json.dumps({"commit": "08666e4d" + "0" * 32, "release_id": "ui-real-08666e4d"}),
        encoding="utf-8")
    seen: dict[str, object] = {}
    monkeypatch.setattr(deployer, "verify_production_source",
                        lambda repo, **kw: seen.update(kw) or {"ok": True})
    deployer._verify_release_source(rel)
    assert seen["commit"] == "08666e4d" + "0" * 32
    assert seen.get("allow_local_ahead_build", False) is False


# ═════ Смена канонической ветки на main (19.08.2026) ═══════════════════════

def test_default_canonical_branch_is_main() -> None:
    """Умолчание стража — `main`, а не старая production feature-ветка.

    Проверяется исходник, а не импортированная константа: модуль читает
    окружение при импорте, и в сеансе с выставленной переменной тест был бы
    зелёным при любом умолчании.
    """
    src = (REPO_ROOT / "scripts" / "production_source_guard.py").read_text(encoding="utf-8")
    assert '"AUDITMANAGER_CANONICAL_PRODUCTION_BRANCH", "main"' in src
    assert '"AUDITMANAGER_CANONICAL_PRODUCTION_BRANCH", "feature/block-vector-graphs"' not in src


def test_commit_only_in_old_feature_branch_is_refused(fixture_repo: Path) -> None:
    """Старая прод-ветка больше НЕ даёт права на выкатку.

    Это главный смысл миграции: пока `feature/block-vector-graphs` считалась
    авторитетом, туда можно было запушить и сразу собрать релиз. Теперь такой
    коммит опубликован, но не канонический — отказ.
    """
    _git(fixture_repo, "checkout", "-q", "-b", OLD_CANONICAL_BRANCH)
    (fixture_repo / "backend" / "app.py").write_text("print('old-branch')\n", encoding="utf-8")
    _git(fixture_repo, "commit", "-aqm", "правка на старой прод-ветке")
    _git(fixture_repo, "push", "-q", "origin", OLD_CANONICAL_BRANCH)
    old_commit = _git(fixture_repo, "rev-parse", "HEAD")

    with pytest.raises(ProductionSourceNotCanonical) as exc:
        _verify(fixture_repo, commit=old_commit)
    assert exc.value.reason == "commit_not_published"

    # А после слияния в main и push — тот же коммит проходит.
    _git(fixture_repo, "checkout", "-q", CANONICAL_BRANCH)
    _git(fixture_repo, "merge", "-q", "--no-edit", old_commit)
    _git(fixture_repo, "push", "-q", "origin", CANONICAL_BRANCH)
    assert _verify(fixture_repo, commit=old_commit)["reachable_from_canonical_remote"] is True


def test_no_production_tooling_still_treats_the_old_branch_as_authority() -> None:
    """Ни один боевой инструмент не должен считать старую ветку авторитетом."""
    tools = [
        REPO_ROOT / "scripts" / "production_source_guard.py",
        REPO_ROOT / "scripts" / "build_center_release.py",
        REPO_ROOT / "scripts" / "deploy_center_release.py",
        REPO_ROOT / "scripts" / "deploy_audit_worker.py",
        REPO_ROOT / "scripts" / "deploy_lock.py",
    ]
    offenders = []
    for tool in tools:
        src = tool.read_text(encoding="utf-8")
        for line in src.splitlines():
            stripped = line.strip()
            if "feature/block-vector-graphs" not in stripped:
                continue
            # Упоминание в комментарии-истории допустимо; исполняемая строка — нет.
            if stripped.startswith("#") or stripped.startswith("#:"):
                continue
            offenders.append(f"{tool.name}: {stripped[:120]}")
    assert not offenders, "старая ветка осталась авторитетом: " + "; ".join(offenders)


def test_main_authority_is_documented() -> None:
    doc = (REPO_ROOT / "docs" / "production_source_guard.md").read_text(encoding="utf-8")
    assert "origin/main" in doc
    assert "умолчание `main`" in doc
