#!/usr/bin/env python3
"""Канонический сборщик иммутабельного релиза центра.

До 12I.3 боевые релизы собирались скриптами из `/tmp` (`build_release_v2…v5`).
Это работало ровно до первого исчезновения `/tmp`: процесс выкатки не
воспроизводился из коммита, а инварианты релиза жили в файле, которого нет ни
в одной истории. Здесь тот же процесс, но принадлежащий репозиторию.

Раскладка релиза (не меняется):

    releases/<release_id>/
        app/                    ← дерево из `git archive` нужного коммита
        venv/                   ← клон venv базового релиза (симлинки целы)
        release-manifest.json
        patch-<sha8>.diff

Инварианты, каждый из которых уже ронял выкатку:

  * `venv/bin/python` обязан остаться СИМЛИНКОМ. `shutil.copytree` по
    умолчанию разыменовывает симлинки, и прекчек установщика падал на
    «missing venv in source release»;
  * дерево обязано читаться ПОСТОРОННИМ: шлюз работает от другой учётной
    записи, и каталог 0700 обнаружился бы только после остановки шлюза;
  * релиз обязан импортировать шлюз из СВОЕГО WorkingDirectory: `python-dotenv`
    в `backend/app/core/config.py` смотрит на `os.getcwd()`;
  * временное дерево обязано исчезать при любом исходе (см.
    `scripts/release_staging.py`).

Использование:

    python scripts/build_center_release.py --base ui-real-43ee9769 \\
        --kind operational_cleanup_12i3 --notes "..."
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from typing import Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.deploy_lock import COMPONENT_CENTER, deploy_lock  # noqa: E402
from scripts.release_staging import (  # noqa: E402
    make_writable,
    seal_tree,
    staging_workspace,
)

DEFAULT_RELEASES = Path("/home/coder/auditmanager/releases")

#: Пути, ради которых релиз и собирается. Их отсутствие означает, что сборка
#: не состоялась, — молча выкатывать такое нельзя.
REQUIRED_PATHS = (
    "contracts/agent_stream/v1/agent_stream_v1.desc",
    "contracts/agent_stream/v1/common.proto",
    "contracts/agent_stream/v1/adapters.py",
    "audit_worker/diagnostics.py",
    "audit_worker/uploader.py",
    "backend/app/agent_gateway/domain.py",
    "backend/app/agent_gateway/__main__.py",
    "backend/app/services/distributed_workers/distributed_ui.py",
    "backend/app/services/distributed_workers/worker_registry.py",
    "backend/app/services/distributed_workers/repositories.py",
    "frontend/static/js/distributed-feature.js",
    "frontend/static/js/distributed-page.js",
    "frontend/static/js/distributed-service.js",
    "frontend/static/js/app.js",
    "frontend/index.html",
    "backend/app/main.py",
    "backend/app/api/routers/audit_worker_agent.py",
    "backend/app/services/distributed_workers/database.py",
    "backend/app/services/distributed_workers/schema.py",
    "scripts/release_staging.py",
    "scripts/deploy_lock.py",
    "scripts/build_center_release.py",
)

#: Тесты, которые обязаны пройти НА ДЕРЕВЕ, которое поедет в прод.
DEFAULT_RELEASE_TESTS = (
    "tests/test_shared_sqlite_sidecar_permissions.py",
    "tests/test_permission_boundary_12f.py",
    "tests/test_agent_stream_protocol_v1.py",
    "tests/test_distributed_ui_real_backend.py",
    "tests/test_production_candidate_12f1b.py",
    "tests/test_result_delivery_terminal_12i2.py",
    "tests/test_worker_runtime_diagnostics_12i2.py",
    "tests/test_release_staging_cleanup_12i2.py",
    "tests/test_deploy_lock_12i3.py",
    "tests/test_provider_startup_state_12i3.py",
)


def run(*args: str, cwd: Path = REPO_ROOT) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fileset_digest(root: Path) -> str:
    """Отпечаток дерева приложения: содержимое, права и цели симлинков.

    Отдельно от `git_tree_sha1`: тот описывает коммит, а этот — то, что
    реально легло на диск. Расхождение между ними означает, что в релиз
    попало (или не попало) что-то помимо коммита.

    Права и симлинки входят в отпечаток намеренно. Подмена, которую он обязан
    ловить, — это не только «переписали файл»: снятый бит чтения для прочих
    останавливает шлюз, а переброшенный симлинк подменяет код, не тронув ни
    одного байта в обычных файлах.
    """
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        rel = str(path.relative_to(root)).encode("utf-8")
        digest.update(rel)
        digest.update(b"\0")
        if path.is_symlink():
            digest.update(b"L")
            digest.update(os.readlink(path).encode("utf-8"))
        elif path.is_dir():
            digest.update(b"D")
            digest.update(oct(stat.S_IMODE(path.lstat().st_mode)).encode("ascii"))
        else:
            digest.update(b"F")
            digest.update(oct(stat.S_IMODE(path.lstat().st_mode)).encode("ascii"))
            digest.update(b"\0")
            digest.update(sha256_file(path).encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


def seal_venv(venv: Path) -> None:
    """venv до вида готового релиза: читаем посторонним, исполняемое живо."""
    for path in sorted(venv.rglob("*"), reverse=True):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o555)
        else:
            current = stat.S_IMODE(path.stat().st_mode)
            path.chmod(0o555 if current & 0o100 else 0o444)
    venv.chmod(0o555)


def symlink_parity(base: Path, built: Path) -> list[str]:
    problems: list[str] = []
    base_venv, built_venv = base / "venv", built / "venv"
    for path in base_venv.rglob("*"):
        if not path.is_symlink():
            continue
        rel = path.relative_to(base_venv)
        mirror = built_venv / rel
        if not mirror.is_symlink():
            kind = "отсутствует" if not mirror.exists() else "стал обычным файлом"
            problems.append(f"симлинк venv/{rel} {kind} (дефект разыменования)")
        elif os.readlink(mirror) != os.readlink(path):
            problems.append(f"симлинк venv/{rel} указывает не туда")
    return problems


def verify_release(root: Path, *, base: Path) -> list[str]:
    problems: list[str] = []
    app = root / "app"
    venv_python = root / "venv/bin/python"

    if not app.is_dir():
        problems.append("нет каталога app")
    if not (root / "release-manifest.json").is_file():
        problems.append("нет release-manifest.json")
    if not venv_python.is_symlink():
        problems.append(f"venv/bin/python не симлинк: {venv_python}")
    else:
        target = os.readlink(venv_python)
        if not Path(target).is_absolute():
            problems.append(f"venv/bin/python указывает относительно: {target}")
        elif not os.access(target, os.X_OK):
            problems.append(f"цель venv/bin/python не исполняема: {target}")
    if not os.access(venv_python, os.X_OK):
        problems.append("venv/bin/python не проходит -x")
    problems.extend(symlink_parity(base, root))

    if app.is_dir() and os.access(venv_python, os.X_OK):
        env = dict(os.environ, PYTHONPATH=str(app), PYTHONDONTWRITEBYTECODE="1")
        probe = subprocess.run(
            [str(venv_python), "-c",
             "import backend.app.main; import backend.app.agent_gateway"],
            cwd=str(app), env=env, capture_output=True, text=True,
        )
        if probe.returncode != 0:
            tail = (probe.stdout + probe.stderr).strip().splitlines()[-1:] or [""]
            problems.append(f"релиз не импортирует шлюз из своего каталога: {tail[0]}")

    for probe_dir in (root, app, root / "venv", root / "venv/bin"):
        # Нужны ОБА бита: без `x` посторонний не войдёт в каталог, даже видя его.
        if probe_dir.exists() and (stat.S_IMODE(probe_dir.stat().st_mode) & 0o005) != 0o005:
            problems.append(f"каталог закрыт для прочих: {probe_dir}")
    for rel in REQUIRED_PATHS:
        if not (app / rel).exists():
            problems.append(f"нет обязательного пути: {rel}")
    for junk in (".pytest_cache", "__pycache__"):
        if (root / junk).exists():
            problems.append(f"мусор сборки в корне релиза: {junk}")
    return problems


def build(
    *,
    base_release: str,
    kind: str,
    notes: str,
    releases_dir: Path = DEFAULT_RELEASES,
    tests: Sequence[str] = DEFAULT_RELEASE_TESTS,
    milestone: str = "",
) -> dict[str, object]:
    commit = run("git", "rev-parse", "HEAD")
    if run("git", "status", "--porcelain"):
        raise SystemExit("рабочее дерево грязное: релиз собирается только из коммита")
    parent = run("git", "rev-parse", "HEAD^")
    tree = run("git", "rev-parse", "HEAD^{tree}")
    release_id = f"ui-real-{commit[:8]}"
    base = releases_dir / base_release
    final = releases_dir / release_id
    if not (base / "release-manifest.json").is_file():
        raise SystemExit(f"базовый релиз не найден: {base}")
    base_manifest = json.loads((base / "release-manifest.json").read_text(encoding="utf-8"))
    base_commit = str(base_manifest.get("commit") or "")
    if base_commit and base_commit != parent:
        raise SystemExit(
            f"релиз-донор собран из {base_commit[:8]}, а родитель HEAD — "
            f"{parent[:8]}: происхождение в манифесте вышло бы ложным"
        )
    # Производные поля прошлого релиза не наследуются: они описывают ЧУЖУЮ
    # сборку и в новом манифесте были бы неправдой.
    for derived in ("changed_paths", "patch_bundle_file", "patch_bundle_sha256",
                    "fileset_sha256", "notes", "kind", "milestone",
                    "builder", "builder_fix", "supersedes_candidate"):
        base_manifest.pop(derived, None)

    # Замок берётся ДО первой записи в каталог релизов: одновременная сборка
    # того же release_id двумя сессиями означала бы гонку за один каталог.
    with deploy_lock(
        COMPONENT_CENTER, operation="build", release=release_id, milestone=milestone
    ):
        if final.exists():
            # Существующий каталог релиза НЕ ТРОГАЕМ никогда. На него может
            # указывать боевой `current`, и пересборка того же коммита сносила
            # бы работающий прод — снимала бы read-only и удаляла дерево под
            # запущенным процессом. Совпал коммит — считаем сборку уже
            # выполненной; не совпал — это подмена уже выданного имени.
            existing = json.loads(
                (final / "release-manifest.json").read_text(encoding="utf-8")
            )
            if existing.get("commit") != commit:
                raise SystemExit(
                    f"релиз {release_id} уже существует и собран из другого "
                    f"коммита ({existing.get('commit')}) — переиспользование "
                    f"идентификатора запрещено"
                )
            return {"RELEASE_ID": release_id, "PATH": str(final), "COMMIT": commit,
                    "FILESET_SHA256": existing.get("fileset_sha256"),
                    "VERIFY": "ALREADY_BUILT"}
        with staging_workspace() as tmp:
            return _build(tmp, base=base, final=final, release_id=release_id,
                          commit=commit, parent=parent, tree=tree,
                          base_manifest=base_manifest, kind=kind, notes=notes,
                          tests=tests)


def _build(tmp, *, base, final, release_id, commit, parent, tree,
           base_manifest, kind, notes, tests) -> dict[str, object]:
    staging = tmp / release_id
    subprocess.run(["cp", "-a", str(base), str(staging)], check=True)
    make_writable(staging)

    app = staging / "app"
    previous = staging / "app.previous"
    app.rename(previous)
    archive = tmp / "app.tar"
    run("git", "archive", "--format=tar", f"--output={archive}", commit)
    app.mkdir()
    subprocess.run(["tar", "-xf", str(archive), "-C", str(app)], check=True)
    make_writable(previous)
    shutil.rmtree(previous)

    for junk in (".pytest_cache", "__pycache__"):
        path = staging / junk
        if path.exists():
            make_writable(path)
            shutil.rmtree(path, ignore_errors=True)

    patch = staging / f"patch-{commit[:8]}.diff"
    patch.write_bytes(subprocess.check_output(
        ["git", "diff", f"{parent}..{commit}"], cwd=REPO_ROOT))

    env = dict(os.environ, PYTHONPATH=str(app))
    subprocess.run(
        [str(staging / "venv/bin/python"), "-m", "pytest", *tests, "-q",
         "-p", "no:cacheprovider"],
        check=True, env=env, cwd=str(app),
    )
    for cache in list(app.rglob("__pycache__")) + list(app.rglob(".pytest_cache")):
        if cache.is_dir():
            make_writable(cache)
            shutil.rmtree(cache, ignore_errors=True)

    # Целевая версия схемы берётся ИЗ КОДА собираемого дерева, а не из
    # манифеста донора. Унаследованная «13» при коде с миграцией 14 означала бы,
    # что предпроверка выкатки доверяет числу, которое никто не сверял, — а
    # общую базу читает ещё и шлюз более старого релиза.
    schema_probe = subprocess.run(
        [str(staging / "venv/bin/python"), "-c",
         "from backend.app.services.distributed_workers import schema;"
         "print(schema.SCHEMA_VERSION)"],
        cwd=str(app), env=env, capture_output=True, text=True,
    )
    if schema_probe.returncode != 0:
        raise SystemExit("не удалось прочитать SCHEMA_VERSION собираемого дерева")
    code_schema = int(schema_probe.stdout.strip())
    inherited = (base_manifest.get("database_schema") or {}).get("target")
    if inherited is not None and int(inherited) != code_schema:
        raise SystemExit(
            f"схема кода {code_schema} не совпадает с унаследованной "
            f"{inherited}: миграция общей базы требует отдельного решения"
        )

    files_digest = fileset_digest(app)
    manifest = dict(base_manifest)
    manifest.update({
        "release_id": release_id,
        "kind": kind,
        "built_at": dt.datetime.now().astimezone().isoformat(timespec="microseconds"),
        "commit": commit,
        "git_tree_sha1": tree,
        "immediate_git_parent": parent,
        "parent_commit": parent,
        "parent_tree_sha1": base_manifest.get("git_tree_sha1"),
        "fileset_sha256": files_digest,
        "database_schema": {"baseline": code_schema, "target": code_schema,
                            "migration": "none"},
        "patch_bundle_file": patch.name,
        "patch_bundle_sha256": sha256_file(patch),
        "venv_cloned_from_release": base.name,
        "builder": "scripts/build_center_release.py",
        "builder_repository_owned": True,
        "staging_lifecycle_module": "scripts/release_staging.py",
        "deploy_lock_module": "scripts/deploy_lock.py",
        "release_tests": list(tests),
        "production_current_pointer_changed": False,
        "production_restart_performed": False,
        "notes": notes,
    })
    (staging / "release-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    seal_tree(app)
    seal_venv(staging / "venv")
    for entry in staging.iterdir():
        if entry.is_symlink() or entry.is_dir():
            continue
        entry.chmod(0o444)
    staging.chmod(0o755)

    problems = verify_release(staging, base=base)
    if problems:
        for item in problems:
            print(f"НАРУШЕН ИНВАРИАНТ: {item}", file=sys.stderr)
        raise SystemExit("релиз не прошёл самопроверку — в каталог релизов не ставим")

    if final.exists():
        # Сюда попасть уже нельзя (проверка выше возвращает раньше), но
        # оставлять рядом код, способный удалить боевой релиз, нельзя тем более.
        raise SystemExit(f"каталог релиза уже существует, удаление запрещено: {final}")
    subprocess.run(["cp", "-a", str(staging), str(final)], check=True)
    after = verify_release(final, base=base)
    if after:
        for item in after:
            print(f"НАРУШЕН ИНВАРИАНТ ПОСЛЕ УСТАНОВКИ: {item}", file=sys.stderr)
        raise SystemExit("установленный релиз не прошёл самопроверку")

    return {"RELEASE_ID": release_id, "PATH": str(final), "COMMIT": commit,
            "FILESET_SHA256": files_digest, "VERIFY": "PASS"}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="релиз-донор venv")
    parser.add_argument("--kind", required=True)
    parser.add_argument("--notes", default="")
    parser.add_argument("--milestone", default="")
    parser.add_argument("--releases-dir", default=str(DEFAULT_RELEASES))
    args = parser.parse_args(argv)
    result = build(base_release=args.base, kind=args.kind, notes=args.notes,
                   releases_dir=Path(args.releases_dir), milestone=args.milestone)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
