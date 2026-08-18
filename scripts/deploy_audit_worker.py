#!/usr/bin/env python3
"""Воспроизводимая установка audit-worker на сторонний VPS.

Зачем скрипт, если можно `scp -r .`
───────────────────────────────────
Слепое копирование рабочего дерева увозит на чужую машину `.env` центра,
`workers.db`, `~/.claude`, корпус проектов и 60 МБ `norms/tools`. Здесь
доставляется ЯВНЫЙ allowlist, а перед отправкой артефакт проверяется
denylist'ом — не «мы вроде ничего лишнего не клали», а «в архиве этого нет».

Модель установки
────────────────
    <root>/app/<release>/      ← неизменяемый релиз (код)
    <root>/current -> app/<release>
    <root>/venv/               ← окружение, переживает смену релиза
    <root>/data/               ← worker.db, token, jobs, tmp, trash
    <root>/config/worker.env   ← 0600, читается systemd-юнитами
    <root>/logs/

Код и данные разведены намеренно: обновление кода не трогает `data/`, а
откат — это переключение симлинка, а не восстановление из бэкапа. Уже идущий
процесс переключения не замечает: `ROOT_DIR` конвейера выводится из
`Path(__file__).resolve()`, то есть симлинк уже развёрнут в `app/<release>`.

Чего скрипт НЕ делает
─────────────────────
Не менеджер парка: один хост за вызов. Не хранит и не печатает секретов —
bootstrap-секрет и worker-токен проходят мимо него. Не трогает firewall, sshd
и системный systemd: юниты ставятся пользовательские.

Примеры
───────
    python scripts/deploy_audit_worker.py build --out /tmp/art
    python scripts/deploy_audit_worker.py deploy --host 10.0.0.5 --user coder
    python scripts/deploy_audit_worker.py releases --host 10.0.0.5 --user coder
    python scripts/deploy_audit_worker.py rollback --host 10.0.0.5 --user coder
"""

from __future__ import annotations

import argparse
import hashlib
import gzip
import json
import os
import shlex
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Iterable, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
# Скрипт документирован для прямого запуска (`python scripts/deploy_audit_worker.py`).
# При нём `sys.path[0]` — это каталог `scripts/`, и пакет `scripts` не находится:
# импорт замка падал ДО его взятия, то есть выкатка вообще не стартовала.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

#: Версия формата пакета доставки. Меняется, когда меняется РАСКЛАДКА архива
#: или набор обязательных полей манифеста, — чтобы старый воркер не пытался
#: развернуть то, чего не понимает.
PACKAGE_FORMAT_VERSION = 1

#: Профили исполнения, с которыми совместим этот релиз.
COMPATIBLE_EXECUTION_PROFILES = ("remote_audit_pilot_v1",)

#: Что едет на воркер. Пути относительно корня репозитория; завершающий «/»
#: означает каталог целиком (с учётом PRUNE_* ниже).
#:
#: Каждая строка — обоснование, а не привычка:
#:   audit_worker/            сам агент и исполнитель;
#:   backend/app/             конвейер, который запускает дочерний процесс;
#:   backend/__init__.py      без него `backend.app...` не импортируется;
#:   blocks.py и три соседа   config.py ссылается на них как на файлы-скрипты;
#:   norms/*.py + 2 json      импортируются по цепочке, хотя norm_verify
#:                            выполняется только на центре;
#:   prompts/pipeline/        шаблоны этапов конвейера;
#:   prompts/disciplines/_registry.json  реестр код→папка для discipline_identity.
#:                            ПРОФИЛИ дисциплин НЕ везём: role.md/checklist.md
#:                            приезжают в пакете задания, и если код попробует
#:                            прочитать их из дерева кода — он обязан упасть
#:                            громко, а не подобрать чужую дисциплину молча;
#:   tests/distributed_audit_e2e/  фикстура и изоляция для офлайн-smoke на VPS.
BUNDLE_INCLUDE: tuple[str, ...] = (
    "audit_worker/",
    "contracts/__init__.py",
    "contracts/agent_stream/",
    # Продление сертификата mTLS: agent.py безусловно импортирует
    # certificate_renewal, а тот — contracts.worker_certificate. Пакета здесь
    # не было, и агент падал с ModuleNotFoundError на КАЖДОМ старте, уходя в
    # вечный цикл перезапусков. Заметить это по выкатке было нельзя: сборка,
    # хэш и selftest проходили — selftest импортирует audit_worker.agent как
    # модуль, а до конструктора WorkerAgent, где и происходит импорт, не
    # доходит. Поэтому пакет перечислен здесь, а тест ниже сверяет список
    # включений с фактическими импортами.
    "contracts/worker_certificate/",
    "backend/__init__.py",
    "backend/app/",
    "blocks.py",
    "process_project.py",
    "generate_excel_report.py",
    "norms/__init__.py",
    "norms/_core.py",
    "norms/_native_verify.py",
    "norms/external_provider.py",
    "norms/norms_db.json",
    "norms/norms_paragraphs.json",
    "prompts/pipeline/",
    "prompts/disciplines/_registry.json",
    "requirements-worker.txt",
    "requirements-worker-grpc.txt",
    "requirements-worker-pipeline.txt",
    "tests/distributed_audit_e2e/",
)

#: Мусор, вырезаемый ВНУТРИ включённых деревьев.
PRUNE_DIR_NAMES = frozenset({"__pycache__", ".git", ".pytest_cache", ".mypy_cache", ".ruff_cache"})
PRUNE_SUFFIXES = (".pyc", ".pyo", ".log", ".sqlite", ".sqlite3", ".db", ".db-wal", ".db-shm")

#: Рантайм-состояние, которое ЖИВАЯ платформа пишет прямо в каталоги кода.
#: Главный пример — `backend/app/data/usage_data.json`: учёт токенов, файл в
#: .gitignore, появляется у любого работающего центра. Каталог
#: `backend/app/data/` при этом везти надо целиком (чек-листы дисциплин,
#: `model_prices.json`), поэтому вырезается по именам.
#:
#: Основной барьер — не этот список, а фильтр «только то, что под контролем
#: git» (см. `collect_bundle_files`): он снимает весь класс утечек рантайма
#: сразу. Список остаётся страховкой для сборки из дерева без git.
PRUNE_FILE_NAMES = frozenset(
    {
        "usage_data.json", "batch_queue.json", "decisions_log.json",
        "stage_models.json", "workers.db", "worker.db", "norms_paragraphs_cache.json",
    }
)

#: Чего в артефакте не должно быть НИКОГДА, на любой глубине. Проверяется по
#: собранному архиву, а не по намерениям: имя файла, имя каталога, суффикс.
DENY_NAMES = frozenset(
    {
        ".env", ".git", ".venv", "venv", "node_modules",
        ".claude", ".codex", ".ssh", ".aws", ".npmrc",
        "token", "claim_secret", "worker.db", "workers.db",
        "batch_queue.json", "usage_data.json", "decisions_log.json",
        "stage_models.json",
    }
)

#: Каталоги ДАННЫХ, запрещённые только в КОРНЕ репозитория. Отдельный список,
#: потому что те же слова совершенно законны как имена пакетов кода:
#: `backend/app/services/knowledge_base/` — это сервис, а `/knowledge_base/` —
#: корпус решений, которому на чужой машине делать нечего. Проверять их
#: «на любой глубине» значило бы запретить половину backend.
DENY_ROOT_DIRS = frozenset(
    {
        "projects", "projects_v2", "knowledge_base",
        "comparison", "comparison_sources",
        "frontend", "experiments", "logs", "webapp",
    }
)
DENY_SUFFIXES = (".pem", ".key", ".crt", ".p12", ".pfx", ".pid", ".sock", ".lock")
DENY_SUBSTRINGS = ("id_rsa", "id_ed25519", "credentials", "secret_key")

#: Читается только чтобы посчитать хэш требований — содержимое не изменяется.
REQUIREMENT_FILES = (
    "requirements-worker.txt",
    "requirements-worker-grpc.txt",
    "requirements-worker-pipeline.txt",
)


# ─── общее ───────────────────────────────────────────────────────────────────


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _commit_timestamp(repo_root: Path, commit: str) -> str:
    """Stable timestamp of the immutable source commit, not build wall-clock."""
    raw = _git(repo_root, "show", "-s", "--format=%ct", commit)
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (TypeError, ValueError, OSError):
        # Non-git source exports may not have commit metadata. Their caller
        # must pin created_at explicitly to obtain byte reproducibility.
        return _utc_now()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(                                   # noqa: S603
        ["git", *args], cwd=str(repo_root),
        capture_output=True, text=True, timeout=120,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def _should_prune(rel: Path) -> bool:
    if any(part in PRUNE_DIR_NAMES for part in rel.parts):
        return True
    if rel.name in PRUNE_FILE_NAMES:
        return True
    return rel.suffix in PRUNE_SUFFIXES


def tracked_files(repo_root: Path) -> Optional[set[str]]:
    """Файлы под контролем git, или None если git недоступен.

    Артефакт обязан быть «слепком коммита», а не «слепком каталога». Разница
    не теоретическая: живой центр пишет рантайм-состояние прямо в дерево
    кода, и сборка из каталога увезла бы на чужую машину учёт токенов,
    очередь батчей и прочее, чего в репозитории нет и быть не должно.
    """
    result = subprocess.run(                                   # noqa: S603
        ["git", "ls-files", "-z"], cwd=str(repo_root),
        capture_output=True, timeout=120,
    )
    if result.returncode != 0:
        return None
    names = {name for name in result.stdout.decode("utf-8").split("\0") if name}
    return names or None


def _denied_reason(rel: Path) -> Optional[str]:
    """Почему этот путь не имеет права оказаться в артефакте (или None)."""
    if rel.parts and rel.parts[0] in DENY_ROOT_DIRS:
        return f"каталог данных «{rel.parts[0]}/» в корне репозитория"
    for part in rel.parts:
        if part in DENY_NAMES:
            return f"запрещённое имя «{part}»"
        low = part.lower()
        for needle in DENY_SUBSTRINGS:
            if needle in low:
                return f"подозрительное имя «{part}» (совпадение «{needle}»)"
    if rel.suffix in DENY_SUFFIXES:
        return f"запрещённый суффикс «{rel.suffix}»"
    return None


# ─── сборка артефакта ────────────────────────────────────────────────────────


@dataclass
class BuildResult:
    archive: Path
    manifest_path: Path
    manifest: dict
    files: list[Path] = field(default_factory=list)


def collect_bundle_files(
    repo_root: Path,
    include: Sequence[str] = BUNDLE_INCLUDE,
    *,
    tracked_only: bool = True,
) -> list[Path]:
    """Развернуть allowlist в отсортированный список путей относительно корня.

    `tracked_only=True` оставляет только файлы под контролем git. Это не
    придирка к чистоте: `backend/app/data/` едет целиком ради чек-листов
    дисциплин, а живая платформа пишет в тот же каталог `usage_data.json` —
    учёт токенов, который в .gitignore. Без фильтра сборка на работающем
    центре либо увезла бы его на чужую машину, либо (как и вышло на первом
    же прогоне тестов) падала бы на denylist каждый раз.
    """
    tracked = tracked_files(repo_root) if tracked_only else None
    found: list[Path] = []
    missing: list[str] = []
    for entry in include:
        target = repo_root / entry.rstrip("/")
        if entry.endswith("/"):
            if not target.is_dir():
                missing.append(entry)
                continue
            for path in sorted(target.rglob("*")):
                if not path.is_file():
                    continue
                rel = path.relative_to(repo_root)
                if _should_prune(rel):
                    continue
                if tracked is not None and str(rel) not in tracked:
                    continue
                found.append(rel)
        else:
            if not target.is_file():
                missing.append(entry)
                continue
            found.append(target.relative_to(repo_root))
    if missing:
        raise SystemExit(
            "в дереве нет объявленных в allowlist путей: " + ", ".join(sorted(missing))
        )
    return sorted(set(found))


def audit_bundle_files(files: Iterable[Path]) -> list[str]:
    """Пройтись denylist'ом. Пустой список = артефакт чист."""
    problems: list[str] = []
    for rel in files:
        reason = _denied_reason(rel)
        if reason:
            problems.append(f"{rel}: {reason}")
    return problems


def tree_hash(repo_root: Path, files: Sequence[Path]) -> str:
    """Хэш содержимого бандла: устойчив к порядку и к времени сборки.

    Считается по строкам «<путь> <sha256>», отсортированным по пути, — то есть
    два одинаковых по содержанию артефакта дают одинаковый хэш даже при разном
    mtime, а перестановка файлов его не меняет.
    """
    digest = hashlib.sha256()
    for rel in sorted(files):
        digest.update(str(rel).encode("utf-8"))
        digest.update(b" ")
        digest.update(_sha256_file(repo_root / rel).encode("ascii"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def requirements_hash(repo_root: Path) -> str:
    digest = hashlib.sha256()
    for name in REQUIREMENT_FILES:
        path = repo_root / name
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes() if path.is_file() else b"")
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def worker_version(repo_root: Path) -> str:
    """Версия пакета из audit_worker/__init__.py без импорта пакета."""
    text = (repo_root / "audit_worker" / "__init__.py").read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("__version__"):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return "0.0.0"


def protocol_version(repo_root: Path) -> int:
    text = (repo_root / "audit_worker" / "__init__.py").read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith("PROTOCOL_VERSION"):
            return int(line.split("=", 1)[1].strip())
    return 0


def build_manifest(
    repo_root: Path,
    files: Sequence[Path],
    *,
    pipeline_revision: str,
    source_commit: str = "",
    created_at: Optional[str] = None,
) -> dict:
    commit = source_commit or _git(repo_root, "rev-parse", "HEAD")
    return {
        "package_format_version": PACKAGE_FORMAT_VERSION,
        "worker_version": worker_version(repo_root),
        "protocol_version": protocol_version(repo_root),
        "pipeline_revision": pipeline_revision,
        "source_commit": commit,
        "source_branch": _git(repo_root, "rev-parse", "--abbrev-ref", "HEAD"),
        "created_at": created_at or _commit_timestamp(repo_root, commit),
        "tree_hash": tree_hash(repo_root, files),
        "requirements_hash": requirements_hash(repo_root),
        "compatible_execution_profiles": list(COMPATIBLE_EXECUTION_PROFILES),
        "file_count": len(files),
        "total_bytes": sum((repo_root / rel).stat().st_size for rel in files),
        "files": [str(rel) for rel in files],
    }


def release_name(manifest: dict) -> str:
    """Имя каталога релиза: время + короткий хэш дерева.

    Время делает порядок релизов очевидным глазом, хэш — гарантией, что два
    разных дерева не займут один каталог.
    """
    stamp = manifest["created_at"].replace("-", "").replace(":", "").replace("Z", "")
    short = manifest["tree_hash"].split(":", 1)[1][:12]
    return f"{stamp}-{short}"


def build_artifact(
    repo_root: Path,
    out_dir: Path,
    *,
    pipeline_revision: str,
    source_commit: str = "",
    created_at: Optional[str] = None,
) -> BuildResult:
    files = collect_bundle_files(repo_root)
    problems = audit_bundle_files(files)
    if problems:
        raise SystemExit("artifact denylist: " + "; ".join(problems[:20]))

    manifest = build_manifest(
        repo_root, files,
        pipeline_revision=pipeline_revision,
        source_commit=source_commit,
        created_at=created_at,
    )
    rel_name = release_name(manifest)
    manifest["release"] = rel_name

    out_dir.mkdir(parents=True, exist_ok=True)
    archive = out_dir / f"audit-worker-{rel_name}.tar.gz"
    manifest_path = out_dir / f"audit-worker-{rel_name}.manifest.json"

    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

    write_bundle_archive(repo_root, files, archive, manifest_bytes)

    manifest["archive_sha256"] = _sha256_file(archive)
    manifest["archive_bytes"] = archive.stat().st_size
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    return BuildResult(archive=archive, manifest_path=manifest_path, manifest=manifest, files=list(files))


def write_bundle_archive(
    repo_root: Path, files: Sequence[Path], archive: Path, manifest_bytes: bytes
) -> None:
    """Записать byte-for-byte deterministic tar.gz.

    Одного `TarInfo.mtime=0` недостаточно: `tarfile.open(..., "w:gz")`
    оставляет текущее время в GZIP header. Явный `GzipFile(mtime=0)` закрывает
    последнюю зависимость от времени; пустой filename убирает имя output-файла.
    """
    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as tar:
                for rel in sorted(files):
                    info = tar.gettarinfo(str(repo_root / rel), arcname=str(rel))
                    info.uid, info.gid = 0, 0
                    info.uname, info.gname = "", ""
                    info.mtime = 0
                    with (repo_root / rel).open("rb") as handle:
                        tar.addfile(info, handle)
                meta = tarfile.TarInfo("MANIFEST.json")
                meta.size = len(manifest_bytes)
                meta.mtime = 0
                meta.mode = 0o644
                import io

                tar.addfile(meta, io.BytesIO(manifest_bytes))


def verify_artifact(archive: Path, manifest_path: Path) -> list[str]:
    """Сверить архив с манифестом. Пустой список = всё сошлось."""
    problems: list[str] = []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual = _sha256_file(archive)
    if actual != manifest.get("archive_sha256"):
        problems.append(f"sha256 архива {actual} ≠ манифест {manifest.get('archive_sha256')}")
    with tarfile.open(archive, "r:gz") as tar:
        names = [m.name for m in tar.getmembers() if m.isfile()]
    inside = {n for n in names if n != "MANIFEST.json"}
    declared = set(manifest.get("files", []))
    if inside != declared:
        only_arc = sorted(inside - declared)[:5]
        only_man = sorted(declared - inside)[:5]
        problems.append(f"состав расходится: только в архиве {only_arc}, только в манифесте {only_man}")
    for name in sorted(inside):
        reason = _denied_reason(Path(name))
        if reason:
            problems.append(f"{name}: {reason}")
    return problems


# ─── удалённая сторона ───────────────────────────────────────────────────────


@contextmanager
def _worker_deploy_lock(remote: "Remote", *, operation: str, release: str):
    """Обязательный замок ВНУТРИ мутирующего примитива.

    Замок только у CLI-разбора команд оставлял дыру: `smoke_distributed_audit_
    real_vps.py` дёргает `remote_install_release`/`remote_switch_current`
    напрямую, минуя `main()`. Одновременный smoke и штатная выкатка одного
    воркера конкурировали бы за один симлинк и один рестарт.

    Повторный вход из уже заблокировавшей команды разрешён (см.
    `scripts/deploy_lock.py`), поэтому обычная выкатка не блокирует сама себя.
    """
    from scripts.deploy_lock import COMPONENT_WORKER, deploy_lock

    # Имя экземпляра собирается ИЗ АТРИБУТОВ, а не требует метода: сюда
    # приходят и настоящий `Remote`, и подменённые объекты из тестов. Чего-то
    # не хватило — получится общий замок, то есть перестраховка, а не дыра.
    instance = worker_lock_instance(
        host=str(getattr(remote, "host", "") or ""),
        user=str(getattr(remote, "user", "") or ""),
        remote_root=str(getattr(remote, "root", "") or ""),
    )
    with deploy_lock(
        COMPONENT_WORKER, operation=operation, release=release,
        instance=instance,
        milestone=os.environ.get("AUDITMANAGER_DEPLOY_MILESTONE", ""),
        reentrant=True,
    ) as path:
        yield path


@dataclass
class Remote:
    host: str
    user: str
    root: str
    ssh_opts: tuple[str, ...] = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=15")
    dry_run: bool = False

    @property
    def target(self) -> str:
        return f"{self.user}@{self.host}"

    @property
    def lock_instance(self) -> str:
        return worker_lock_instance(host=self.host, user=self.user,
                                    remote_root=self.root)

    def run(self, script: str, *, timeout: int = 600, check: bool = True) -> subprocess.CompletedProcess:
        cmd = ["ssh", *self.ssh_opts, self.target, "bash -s"]
        if self.dry_run:
            print(f"[dry-run] ssh {self.target} <<'EOF'\n{script}\nEOF")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        result = subprocess.run(                               # noqa: S603
            cmd, input=script, capture_output=True, text=True, timeout=timeout,
        )
        if check and result.returncode != 0:
            raise SystemExit(
                f"удалённая команда завершилась кодом {result.returncode}\n"
                f"stdout: {result.stdout[-4000:]}\nstderr: {result.stderr[-4000:]}"
            )
        return result

    def copy(self, local: Path, remote_rel: str, *, timeout: int = 1800) -> None:
        dest = f"{self.target}:{self.root}/{remote_rel}"
        if self.dry_run:
            print(f"[dry-run] scp {local} {dest}")
            return
        result = subprocess.run(                               # noqa: S603
            ["scp", *self.ssh_opts, str(local), dest],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            raise SystemExit(f"scp не удался: {result.stderr[-2000:]}")


def remote_from_args(args: argparse.Namespace) -> Remote:
    """Remote с опциональным явным ssh config.

    Пустое значение сохраняет прежнее поведение. ``-F /dev/null`` полезен на
    изолированном центре, где системный include SSH повреждён; это не должно
    заставлять deploy править глобальную конфигурацию машины.
    """
    opts = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=15")
    ssh_config = str(getattr(args, "ssh_config", "") or "").strip()
    if ssh_config:
        opts = ("-F", ssh_config, *opts)
    return Remote(
        host=args.host,
        user=args.user,
        root=args.remote_root,
        ssh_opts=opts,
        dry_run=args.dry_run,
    )


def remote_bootstrap_layout(remote: Remote) -> None:
    """Создать раскладку. Идемпотентно; существующие data/ не трогает."""
    remote.run(
        f"""set -euo pipefail
root={shlex.quote(remote.root)}
mkdir -p "$root"/{{app,data,config,logs,incoming}}
mkdir -p "$root"/data/{{jobs,tmp,trash,credentials}}
chmod 750 "$root" "$root/data" "$root/config"
echo LAYOUT_OK
"""
    )


def remote_install_release(
    remote: Remote, archive_name: str, manifest_name: str, release: str, expected_sha: str
) -> str:
    """Развернуть архив в app/<release>. Симлинк НЕ трогается."""
    with _worker_deploy_lock(remote, operation="install", release=release):
        return _remote_install_release(remote, archive_name, manifest_name,
                                       release, expected_sha)


def _remote_install_release(
    remote: Remote, archive_name: str, manifest_name: str, release: str, expected_sha: str
) -> str:
    result = remote.run(
        f"""set -euo pipefail
root={shlex.quote(remote.root)}
rel={shlex.quote(release)}
arc="$root/incoming/{shlex.quote(archive_name)}"
actual=$(sha256sum "$arc" | awk '{{print $1}}')
if [ "$actual" != {shlex.quote(expected_sha)} ]; then
  echo "SHA_MISMATCH actual=$actual expected={expected_sha}" >&2
  exit 3
fi
echo "SHA_OK $actual"
target="$root/app/$rel"
if [ -d "$target" ]; then
  echo "RELEASE_EXISTS $rel"
else
  tmp="$root/app/.unpack.$rel.$$"
  rm -rf "$tmp"; mkdir -p "$tmp"
  tar -xzf "$arc" -C "$tmp"
  mv -T "$tmp" "$target"
  echo "RELEASE_UNPACKED $rel"
fi
cp -f "$root/incoming/{shlex.quote(manifest_name)}" "$target/MANIFEST.deploy.json"
chmod -R go-w "$target"
python3 - "$target/MANIFEST.deploy.json" "$target" <<'TREE_VERIFY_PY'
import hashlib, json, sys
from pathlib import Path, PurePosixPath

manifest_path, root_path = Path(sys.argv[1]), Path(sys.argv[2])
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
digest = hashlib.sha256()
for raw in sorted(manifest.get("files") or []):
    rel = PurePosixPath(raw)
    if rel.is_absolute() or ".." in rel.parts:
        raise SystemExit("TREE_PATH_INVALID")
    path = root_path.joinpath(*rel.parts)
    if not path.is_file() or path.is_symlink():
        raise SystemExit("TREE_FILE_MISSING " + raw)
    file_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    digest.update(raw.encode("utf-8"))
    digest.update(b" ")
    digest.update(file_hash.encode("ascii"))
    digest.update(b"\\n")
actual = "sha256:" + digest.hexdigest()
if actual != manifest.get("tree_hash"):
    raise SystemExit("TREE_HASH_MISMATCH")
print("TREE_OK " + actual)
TREE_VERIFY_PY
echo "INSTALL_OK $rel"
""",
        timeout=900,
    )
    return result.stdout


def remote_sync_venv(
    remote: Remote, release: str, *, python: str = "python3", grpc: bool = False
) -> str:
    """Создать/дообновить venv. Живёт вне релиза — переживает откат."""
    grpc_install = (
        '"$root/venv/bin/python" -m pip install --quiet '
        '-r "$root/app/$rel/requirements-worker-grpc.txt"'
        if grpc else ":"
    )
    result = remote.run(
        f"""set -euo pipefail
root={shlex.quote(remote.root)}
rel={shlex.quote(release)}
if [ ! -x "$root/venv/bin/python" ]; then
  {shlex.quote(python)} -m venv "$root/venv"
fi
"$root/venv/bin/python" -m pip install --quiet --upgrade pip >/dev/null
"$root/venv/bin/python" -m pip install --quiet -r "$root/app/$rel/requirements-worker.txt"
"$root/venv/bin/python" -m pip install --quiet -r "$root/app/$rel/requirements-worker-pipeline.txt"
{grpc_install}
echo "VENV_OK $("$root/venv/bin/python" -V 2>&1)"
""",
        timeout=1800,
    )
    return result.stdout


def remote_selftest(remote: Remote, release: str) -> str:
    """Проверки БЕЗ центра: импорт агента, импорт конвейера, тестовый процесс."""
    result = remote.run(
        f"""set -euo pipefail
root={shlex.quote(remote.root)}
rel={shlex.quote(release)}
app="$root/app/$rel"
py="$root/venv/bin/python"
cd "$app"
export AUDIT_DISABLE_DOTENV=1
export PYTHONPATH="$app"
# Импортируем ИМЕННО то, что агент грузит на старте, а не только пакет.
# `import audit_worker` трогает лишь __init__ и потому пропустил подряд два
# отказа, каждый из которых оставил воркер в вечном цикле перезапусков:
# синтаксическую ошибку в resource_monitor и отсутствие пакета
# contracts.worker_certificate в артефакте. Оба видны здесь — до переключения
# `current`, то есть до того, как выкатка станет необратимой.
# certificate_renewal перечислен отдельно: agent.py импортирует его внутри
# конструктора, и до него импорт самого agent не доходит.
"$py" -c 'import audit_worker, audit_worker.agent, audit_worker.certificate_renewal, audit_worker.executor, audit_worker.resource_monitor, audit_worker.grpc_transport, audit_worker.heartbeat; print("AGENT_IMPORT_OK", audit_worker.__version__, audit_worker.PROTOCOL_VERSION)'
"$py" -c 'import backend.app.pipeline.remote_audit_runner as r; print("PIPELINE_IMPORT_OK")'
"$py" -c 'import fitz; print("FITZ_OK", fitz.__doc__.strip()[:40] if fitz.__doc__ else "")'
"$py" -m audit_worker selftest --root "$root/data" --steps 3 2>&1 | tail -4
echo SELFTEST_OK
""",
        timeout=900,
    )
    return result.stdout


def remote_current_release(remote: Remote) -> str:
    result = remote.run(
        f"""root={shlex.quote(remote.root)}
if [ -L "$root/current" ]; then basename "$(readlink -f "$root/current")"; fi
""",
        check=False,
    )
    return result.stdout.strip()


def remote_list_releases(remote: Remote) -> list[str]:
    result = remote.run(
        f"""root={shlex.quote(remote.root)}
ls -1 "$root/app" 2>/dev/null | grep -v '^\\.' || true
""",
        check=False,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def remote_switch_current(remote: Remote, release: str) -> str:
    """Атомарное переключение симлинка: ln -sfn во временный + mv -T."""
    with _worker_deploy_lock(remote, operation="switch", release=release):
        return _remote_switch_current(remote, release)


def _remote_switch_current(remote: Remote, release: str) -> str:
    result = remote.run(
        f"""set -euo pipefail
root={shlex.quote(remote.root)}
rel={shlex.quote(release)}
test -d "$root/app/$rel" || {{ echo "NO_SUCH_RELEASE $rel" >&2; exit 4; }}
prev=""
if [ -L "$root/current" ]; then prev=$(basename "$(readlink -f "$root/current")"); fi
ln -sfn "$root/app/$rel" "$root/.current.new"
mv -T "$root/.current.new" "$root/current"
echo "SWITCH_OK prev=${{prev:-none}} now=$rel"
"""
    )
    return result.stdout.strip()


def remote_restart_units(remote: Remote, units: Sequence[str]) -> str:
    """Перезапуск пользовательских юнитов. Агент и исполнитель независимы."""
    joined = " ".join(shlex.quote(u) for u in units)
    result = remote.run(
        f"""set -euo pipefail
export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user daemon-reload
for u in {joined}; do
  if systemctl --user list-unit-files "$u" >/dev/null 2>&1; then
    systemctl --user restart "$u" || echo "RESTART_FAILED $u" >&2
    echo "RESTARTED $u"
  else
    echo "UNIT_ABSENT $u"
  fi
done
""",
        check=False,
    )
    return result.stdout


def assert_units_healthy(health: dict, *, release: str, stage: str) -> None:
    """Юниты обязаны быть ЖИВЫ и работать из ожидаемого релиза.

    Прежде отказ `systemctl restart` печатался строкой `RESTART_FAILED` и на
    этом всё: команда завершалась успехом, замок снимался, а воркер оставался
    выключенным — при уже переключённом `current`. Снаружи это выглядит как
    «выкатка прошла», и обнаруживается только когда задание некому взять.
    """
    problems = []
    for unit in health.get("units") or []:
        if str(unit.get("STATE")) != "active":
            problems.append(f"{unit.get('UNIT')}: состояние {unit.get('STATE')}")
        elif not str(unit.get("PID") or "").strip() or str(unit.get("PID")) == "0":
            problems.append(f"{unit.get('UNIT')}: нет главного процесса")
    if not (health.get("units") or []):
        problems.append("юнитов не найдено вовсе")
    actual = str(health.get("release") or "")
    if release and actual and actual != release:
        problems.append(f"работает релиз {actual}, а ожидался {release}")
    if problems:
        raise SystemExit(
            f"{stage}: воркер не подтвердил работоспособность — "
            + "; ".join(problems)
        )


def remote_health(remote: Remote, units: Sequence[str]) -> dict:
    joined = " ".join(shlex.quote(u) for u in units)
    result = remote.run(
        f"""set +e
export XDG_RUNTIME_DIR=/run/user/$(id -u)
root={shlex.quote(remote.root)}
echo "RELEASE=$( [ -L "$root/current" ] && basename "$(readlink -f "$root/current")" || echo none )"
for u in {joined}; do
  echo "UNIT=$u STATE=$(systemctl --user is-active "$u" 2>/dev/null) PID=$(systemctl --user show -p MainPID --value "$u" 2>/dev/null)"
done
echo "DISK_FREE_MB=$(df -Pm "$root" | awk 'NR==2{{print $4}}')"
echo "TOKEN=$( [ -f "$root/data/token" ] && echo present || echo absent )"
echo "TOKEN_MODE=$( [ -f "$root/data/token" ] && stat -c '%a' "$root/data/token" || echo - )"
echo "BOOTSTRAP=$( [ -f "$root/data/claim_secret" ] && echo present || echo absent )"
""",
        check=False,
    )
    parsed: dict = {"units": []}
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("UNIT="):
            parts = dict(p.split("=", 1) for p in line.split() if "=" in p)
            parsed["units"].append(parts)
        elif "=" in line:
            key, value = line.split("=", 1)
            parsed[key.lower()] = value
    return parsed


# ─── команды ─────────────────────────────────────────────────────────────────


def cmd_build(args: argparse.Namespace) -> int:
    result = build_artifact(
        REPO_ROOT, Path(args.out).resolve(),
        pipeline_revision=args.pipeline_revision,
        source_commit=args.source_commit,
    )
    problems = verify_artifact(result.archive, result.manifest_path)
    if problems:
        raise SystemExit("самопроверка артефакта не прошла: " + "; ".join(problems[:10]))
    m = result.manifest
    print(f"артефакт:  {result.archive}")
    print(f"манифест:  {result.manifest_path}")
    print(f"релиз:     {m['release']}")
    print(f"файлов:    {m['file_count']}, {m['total_bytes'] / 1048576:.1f} МБ сырьём")
    print(f"архив:     {m['archive_bytes'] / 1048576:.1f} МБ, sha256 {m['archive_sha256'][:16]}…")
    print(f"tree_hash: {m['tree_hash']}")
    print(f"revision:  {m['pipeline_revision']}")
    print(f"commit:    {m['source_commit'][:12]}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    problems = verify_artifact(Path(args.artifact), Path(args.manifest))
    if problems:
        for line in problems:
            print("  ✗", line)
        return 1
    print("артефакт соответствует манифесту, запрещённых путей нет")
    return 0


def cmd_deploy(args: argparse.Namespace) -> int:
    remote = remote_from_args(args)

    if args.artifact:
        archive = Path(args.artifact).resolve()
        manifest_path = Path(args.manifest or str(archive).replace(".tar.gz", ".manifest.json"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        out_dir = Path(args.out or tempfile.mkdtemp(prefix="audit-worker-artifact-"))
        built = build_artifact(
            REPO_ROOT, out_dir,
            pipeline_revision=args.pipeline_revision,
            source_commit=args.source_commit,
        )
        archive, manifest_path, manifest = built.archive, built.manifest_path, built.manifest

    problems = verify_artifact(archive, manifest_path)
    if problems:
        raise SystemExit("артефакт не прошёл проверку перед отправкой: " + "; ".join(problems[:10]))

    release = manifest["release"]
    print(f"[1/7] раскладка на {remote.target}:{remote.root}")
    remote_bootstrap_layout(remote)

    previous = remote_current_release(remote)
    print(f"      текущий релиз: {previous or 'нет'}")

    print(f"[2/7] доставка {archive.name} ({manifest['archive_bytes'] / 1048576:.1f} МБ)")
    remote.copy(archive, f"incoming/{archive.name}")
    remote.copy(manifest_path, f"incoming/{manifest_path.name}")

    print(f"[3/7] проверка хэша и распаковка релиза {release}")
    print("      " + remote_install_release(
        remote, archive.name, manifest_path.name, release, manifest["archive_sha256"]
    ).strip().replace("\n", "\n      "))

    print("[4/7] синхронизация venv")
    print("      " + remote_sync_venv(remote, release, python=args.remote_python).strip())

    if args.selftest:
        print("[5/7] selftest нового релиза (ещё до переключения current)")
        print("      " + remote_selftest(remote, release).strip().replace("\n", "\n      "))
    else:
        print("[5/7] selftest пропущен (--no-selftest)")

    print(f"[6/7] переключение current → {release}")
    print("      " + remote_switch_current(remote, release))

    if args.restart:
        print("[7/7] перезапуск юнитов")
        restart_out = remote_restart_units(remote, args.units)
        print("      " + restart_out.strip().replace("\n", "\n      "))
    else:
        print("[7/7] перезапуск пропущен (--no-restart)")

    health = remote_health(remote, args.units)
    print("health:", json.dumps(health, ensure_ascii=False))
    if args.restart:
        try:
            assert_units_healthy(health, release=release, stage="выкатка")
        except SystemExit:
            # Указатель уже переключён, а воркер не поднялся. Возвращаем его
            # сами и ДО снятия замка: иначе следующая выкатка входит в машину,
            # где current указывает на нерабочий релиз.
            if previous and previous != release:
                print(f"ВЫКАТКА НЕ УДАЛАСЬ — откат на {previous}", file=sys.stderr)
                print("  " + remote_switch_current(remote, previous), file=sys.stderr)
                print("  " + remote_restart_units(remote, args.units).strip(),
                      file=sys.stderr)
                back = remote_health(remote, args.units)
                print("health после отката:", json.dumps(back, ensure_ascii=False),
                      file=sys.stderr)
            raise
    if previous and previous != release:
        print(f"откат: python {Path(__file__).name} rollback --host {args.host} --user {args.user} --to {previous}")
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    remote = remote_from_args(args)
    releases = remote_list_releases(remote)
    current = remote_current_release(remote)
    target = args.to
    if not target:
        candidates = [r for r in sorted(releases) if r != current]
        if not candidates:
            raise SystemExit("откатываться некуда: другого релиза на хосте нет")
        target = candidates[-1]
    if target not in releases:
        raise SystemExit(f"релиза {target} на хосте нет; есть: {', '.join(sorted(releases))}")
    print(f"откат {current or 'нет'} → {target}")
    print("  " + remote_switch_current(remote, target))
    if args.restart:
        print("  " + remote_restart_units(remote, args.units).strip().replace("\n", "\n  "))
    health = remote_health(remote, args.units)
    print("health:", json.dumps(health, ensure_ascii=False))
    if args.restart:
        # Откат обязан доказывать успех так же строго, как выкатка: молчаливо
        # «откатившийся» в выключенное состояние воркер — это тот же простой.
        assert_units_healthy(health, release=target, stage="откат")
    return 0


def cmd_releases(args: argparse.Namespace) -> int:
    remote = remote_from_args(args)
    current = remote_current_release(remote)
    for name in sorted(remote_list_releases(remote)):
        print(("* " if name == current else "  ") + name)
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    remote = remote_from_args(args)
    print(json.dumps(remote_health(remote, args.units), ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="deploy_audit_worker",
        description="Сборка и установка audit-worker на сторонний VPS",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_remote(p: argparse.ArgumentParser) -> None:
        p.add_argument("--host", required=True, help="адрес worker VPS (в коде не зашит)")
        p.add_argument("--user", required=True, help="SSH-пользователь")
        p.add_argument(
            "--ssh-config", default="",
            help="явный файл ssh_config (например /dev/null для изолированного стенда)",
        )
        p.add_argument("--remote-root", default="/home/coder/audit-worker",
                       help="корень установки на воркере")
        # Пустой список = «вывести из --remote-root» (см. `units_for_root`).
        # Фиксированное умолчание разворачивало ВТОРУЮ установку и
        # перезапускало юниты ПЕРВОЙ: имена были константами, а корень —
        # параметром. Инцидент воспроизведён на 11G, поэтому умолчание теперь
        # производное, а явный список остаётся для нестандартных установок.
        p.add_argument("--units", nargs="*", default=[])
        p.add_argument("--dry-run", action="store_true")

    def add_build_opts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--pipeline-revision", default=os.environ.get("AUDIT_PIPELINE_REVISION", ""),
                       help="строка ревизии; она же обязана стоять на центре")
        p.add_argument("--source-commit", default="")

    p_build = sub.add_parser("build", help="собрать артефакт и манифест")
    p_build.add_argument("--out", required=True)
    add_build_opts(p_build)
    p_build.set_defaults(func=cmd_build)

    p_verify = sub.add_parser("verify", help="сверить артефакт с манифестом")
    p_verify.add_argument("--artifact", required=True)
    p_verify.add_argument("--manifest", required=True)
    p_verify.set_defaults(func=cmd_verify)

    p_deploy = sub.add_parser("deploy", help="собрать, доставить, установить, переключить")
    add_remote(p_deploy)
    add_build_opts(p_deploy)
    p_deploy.add_argument("--artifact", default="", help="готовый артефакт вместо сборки")
    p_deploy.add_argument("--manifest", default="")
    p_deploy.add_argument("--out", default="", help="куда положить собранный артефакт")
    p_deploy.add_argument("--remote-python", default="python3")
    p_deploy.add_argument("--no-selftest", dest="selftest", action="store_false")
    p_deploy.add_argument("--no-restart", dest="restart", action="store_false")
    p_deploy.set_defaults(func=cmd_deploy, selftest=True, restart=True)

    p_rollback = sub.add_parser("rollback", help="вернуть предыдущий релиз")
    add_remote(p_rollback)
    p_rollback.add_argument("--to", default="", help="конкретный релиз (по умолчанию — предыдущий)")
    p_rollback.add_argument("--no-restart", dest="restart", action="store_false")
    p_rollback.set_defaults(func=cmd_rollback, restart=True)

    p_rel = sub.add_parser("releases", help="список релизов на хосте")
    add_remote(p_rel)
    p_rel.set_defaults(func=cmd_releases)

    p_status = sub.add_parser("status", help="состояние установки на хосте")
    add_remote(p_status)
    p_status.set_defaults(func=cmd_status)

    return parser


def units_for_root(root: str) -> list[str]:
    """Юниты ЭТОЙ установки. Умолчание выводится из корня, а не зашито.

    Корень `…/audit-worker` сохраняет прежние имена дословно — уже стоящие
    юниты и их журналы никуда не переезжают. Любой другой корень получает
    собственную пару имён, и развёртывание второго экземпляра перестаёт
    трогать первый.
    """
    name = Path(root).name
    if name == "audit-worker":
        return ["audit-worker-agent.service", "audit-worker-executor.service"]
    suffix = name.removeprefix("audit-worker-") or name
    return [
        f"audit-worker-{suffix}-agent.service",
        f"audit-worker-{suffix}-executor.service",
    ]


def discover_units(remote: Remote, root: str) -> list[str]:
    """Спросить у systemd, какие юниты ОБСЛУЖИВАЮТ этот корень.

    Имя выводить из корня недостаточно: боевые юниты 11l называются
    `audit-worker-audit-worker-11l-<хэш>-agent.service`, и хэш из пути не
    получить никак. Прежде расхождение проходило молча — `UNIT_ABSENT`,
    рестарта нет, выкатка «успешна», а воркер продолжает работать на старом
    коде при уже переключённом `current`. Именно так эта выкатка и выглядела бы
    без проверки здоровья.

    Возвращает пустой список, если спросить не удалось: тогда остаётся
    умолчание из корня, и несоответствие поймает проверка здоровья.
    """
    try:
        result = remote.run(
            """set +e
export XDG_RUNTIME_DIR=/run/user/$(id -u)
systemctl --user list-units --type=service --all --no-legend 'audit-worker-*' \
  | awk '{print $1}'
""",
            check=False,
        )
    except SystemExit:
        return []
    names = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    root_name = Path(root).name
    matched = [n for n in names if root_name in n and n.endswith(".service")]
    agent = [n for n in matched if "-agent.service" in n]
    executor = [n for n in matched if "-executor.service" in n]
    if agent and executor:
        return sorted(set(agent)) + sorted(set(executor))
    return []


def _ssh_canonical_host(host: str, ssh_config: str = "") -> str:
    """Что SSH на самом деле считает адресом этого имени.

    Алиас из `~/.ssh/config` — не то же самое, что имя хоста: обращение по
    алиасу и по адресу идёт на одну машину, но как строки не совпадает. Про
    это знает только сам ssh, поэтому спрашиваем его (`-G` печатает итоговую
    конфигурацию и ничего не подключает).
    """
    cmd = ["ssh", "-G"]
    if ssh_config:
        cmd += ["-F", ssh_config]
    cmd.append(host)
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except (OSError, subprocess.SubprocessError):
        return host
    if out.returncode != 0:
        return host
    for line in out.stdout.splitlines():
        key, _, value = line.strip().partition(" ")
        if key.lower() == "hostname" and value.strip():
            return value.strip()
    return host


def worker_lock_instance(*, host: str, user: str, remote_root: str,
                         ssh_config: str = "") -> str:
    """Имя замка = ОДНА установка воркера, а не одно написание её адреса.

    Две ошибки, каждая из которых возможна на этом стенде:

      * `11l` и `11g` живут на одной машине разными пользователями и в разных
        корнях — общий замок запрещал бы обслуживать их одновременно без
        всякой причины;
      * одну и ту же машину зовут то по IP, то по алиасу из `~/.ssh/config`, и
        разные написания давали бы РАЗНЫЕ замки: две выкатки в один корень
        пошли бы параллельно, а именно это замок и обязан запретить.

    Поэтому имя хоста сначала раскрывается тем же ssh, который будет
    подключаться, затем резолвится в ПОЛНЫЙ отсортированный набор адресов
    (набор, а не первый адрес: порядок в ответе DNS меняется сам по себе).
    Корень участвует целиком, через отпечаток: совпадение последнего сегмента
    пути у двух разных пользователей — не совпадение установок.

    Честные границы, которые этим не закрываются:
      * замок ЛОКАЛЬНЫЙ — выкатку с другой управляющей машины он не увидит;
      * DNS с раздачей разных подмножеств адресов (round-robin с усечением)
        может дать двум процессам разные имена. Против этого помогает только
        явный стабильный идентификатор установки; здесь он не введён.
    """
    import hashlib
    import socket

    canonical = _ssh_canonical_host(host.strip(), ssh_config).strip().lower()
    try:
        addresses = sorted({str(item[4][0]) for item in socket.getaddrinfo(canonical, None)})
    except OSError:
        addresses = []
    identity = ",".join(addresses) if addresses else canonical
    fingerprint = hashlib.sha256(
        f"{user.strip()}@{identity}:{PurePosixPath(remote_root.strip() or '/')}".encode("utf-8")
    ).hexdigest()[:12]
    readable = "".join(ch for ch in (addresses[0] if addresses else canonical)
                       if ch.isalnum() or ch in "-_.")
    return f"{readable or 'worker'}-{fingerprint}"


#: Команды, которые МЕНЯЮТ боевое состояние воркера. `build` и `verify`
#: работают с артефактом и замка не требуют — блокировать их значило бы
#: запрещать безобидную параллельную сборку.
_MUTATING_COMMANDS = frozenset({"deploy", "rollback"})


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if hasattr(args, "units") and not args.units:
        args.units = units_for_root(args.remote_root)
        # Умолчание из корня — только запасной вариант. Спрашиваем systemd на
        # самой машине: настоящие имена содержат хэш установки, которого в пути
        # нет, и промах означал бы выкатку без единого рестарта.
        if getattr(args, "host", "") and getattr(args, "user", ""):
            try:
                found = discover_units(remote_from_args(args), args.remote_root)
            except Exception:  # noqa: BLE001 — разведка не вправе ронять выкатку
                found = []
            if found and set(found) != set(args.units):
                print(f"юниты по факту: {', '.join(found)}")
                args.units = found
    if args.command not in _MUTATING_COMMANDS:
        return int(args.func(args))
    from scripts.deploy_lock import COMPONENT_WORKER, deploy_lock

    instance = worker_lock_instance(
        host=str(getattr(args, "host", "")),
        user=str(getattr(args, "user", "")),
        remote_root=str(getattr(args, "remote_root", "")),
    )
    with deploy_lock(
        COMPONENT_WORKER,
        operation=args.command,
        release=str(getattr(args, "to", "") or ""),
        instance=instance,
        milestone=os.environ.get("AUDITMANAGER_DEPLOY_MILESTONE", ""),
    ):
        return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
