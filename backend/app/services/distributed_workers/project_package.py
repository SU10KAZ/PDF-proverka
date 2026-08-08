"""Сборка переносимого пакета РЕАЛЬНОГО проекта для удалённого аудита.

Три решения, каждое подтверждено первым аудитом архитектуры:

**TAR, а не ZIP.** 18 % файлов корпуса (36 673 из 199 016) — хардлинки, из них
34 932 — кропы блоков после дедупликации. ZIP не имеет типа записи «жёсткая
ссылка» вовсе, и пакет раздувается на 40 %. Здесь хардлинки сохраняются явной
картой инодов, а не надеждой на библиотеку.

**Сканирование дерева, а не список путей.** Раскладка версий НЕОДНОРОДНА: у
одной версии `pipeline_log.json` лежит в `99_service/`, у другой каталога
`99_service/` нет вовсе и всё в `03_analysis/latest/`. Фиксированный список
путей на таком корпусе молча теряет артефакты, а resume-детектор на воркере
после этого начинает конвейер не с того этапа.

**Снимок конфигурации внутри пакета.** `prompts/` редактируются из UI, а
`stage_models.json` вообще вне git: воркер, взявший их из своего клона,
прогонит аудит другими моделями и другими промптами — молча и дороже. Снимок
делается на КОНКРЕТНУЮ попытку, его хэш едет в манифесте, и изменение
конфигурации центра после старта на текущую попытку уже не влияет.

Чего в пакете нет и быть не может: `.env`, секретов, токенов, авторизации
Claude/Codex, PID-файлов, WAL, каталога `.git`, исходного кода приложения,
нормативной базы, чужих проектов.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from backend.app.services.distributed_workers import package_service

#: Версия раскладки проекта внутри пакета. Растёт при несовместимом изменении.
PROJECT_LAYOUT_VERSION = 1

#: Корень дерева проекта внутри архива (под общим `payload/`).
PROJECT_ROOT = "project/"

#: Каталог снимков конфигурации внутри архива.
SNAPSHOT_ROOT = "snapshot/"

#: Что НИКОГДА не попадает в пакет. Проверяется по каждому сегменту пути.
FORBIDDEN_NAMES: frozenset[str] = frozenset(
    {
        ".git", ".env", ".venv", "venv", "__pycache__", "node_modules",
        ".claude", ".codex", ".ssh", ".aws", ".config",
    }
)

#: Расширения и имена, которые не переносятся никогда.
FORBIDDEN_SUFFIXES: tuple[str, ...] = (
    ".pid", ".lock", ".sock", "-wal", "-shm", ".db-wal", ".db-shm",
    ".pem", ".key", ".crt", ".p12", ".pfx",
)

FORBIDDEN_FILENAMES: frozenset[str] = frozenset(
    {
        ".env", ".env.local", "token", "claim_secret", "credentials",
        ".credentials.json", "auth.json", "workers.db", "worker.db",
        "batch_queue.json", "paid_cost.json", "paid_cost_events.jsonl",
        "usage_data.json", "decisions_log.json", "norms_paragraphs.json",
    }
)

#: Восстановимое: кропы блоков ре-рендерятся из `02_work/document.pdf` офлайн.
#: Исключаются ТОЛЬКО если PDF в пакете есть — иначе воркер молча ушёл бы в
#: сеть на портал, а 15 % ссылок `crop_url` в корпусе мертвы.
REGENERABLE_DIR_PATTERNS: tuple[str, ...] = (
    "_stage02_paid_response_cache",
    ".evicted",
)

#: Максимальный размер одного файла в пакете. Больше — почти наверняка мусор.
MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024


class ProjectPackageError(RuntimeError):
    """Пакет собрать нельзя. Сообщение показывается оператору."""


@dataclass
class PackageLimits:
    max_total_bytes: int = 8 * 1024 * 1024 * 1024
    max_files: int = 200_000


@dataclass
class ScanResult:
    """Что нашлось в дереве версии."""

    files: list[tuple[Path, str]] = field(default_factory=list)   # (абсолютный, относительный)
    excluded: list[str] = field(default_factory=list)
    total_bytes: int = 0


# ─── Классификация путей ─────────────────────────────────────────────────────
def _is_forbidden(rel_parts: tuple[str, ...], name: str) -> Optional[str]:
    for part in rel_parts:
        if part in FORBIDDEN_NAMES:
            return f"запрещённый каталог {part!r}"
    if name in FORBIDDEN_FILENAMES:
        return f"запрещённое имя {name!r}"
    lowered = name.lower()
    for suffix in FORBIDDEN_SUFFIXES:
        if lowered.endswith(suffix):
            return f"запрещённое расширение {suffix!r}"
    if lowered.startswith(".env"):
        return "файл окружения"
    return None


def _is_regenerable(rel_parts: tuple[str, ...]) -> bool:
    return any(part in REGENERABLE_DIR_PATTERNS for part in rel_parts)


def scan_version_tree(
    version_dir: Path, *, limits: Optional[PackageLimits] = None
) -> ScanResult:
    """Обойти дерево версии и отобрать то, что едет на воркер.

    Симлинки не переносятся и не разыменовываются: пакет должен быть
    самодостаточным, а ссылка наружу дерева — это либо ошибка, либо попытка
    вынести чужие данные.
    """
    lim = limits or PackageLimits()
    version_dir = Path(version_dir).resolve()
    if not version_dir.is_dir():
        raise ProjectPackageError(f"Каталог версии не найден: {version_dir}")

    result = ScanResult()
    for root, dirnames, filenames in os.walk(version_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(version_dir)
        rel_parts = tuple(p for p in rel_root.parts if p not in (".",))
        # Обрезаем ветки целиком: так дешевле и так виднее в отчёте.
        keep_dirs = []
        for dirname in dirnames:
            if dirname in FORBIDDEN_NAMES:
                result.excluded.append(str(rel_root / dirname) + "/ (запрещённый каталог)")
                continue
            if dirname in REGENERABLE_DIR_PATTERNS:
                result.excluded.append(str(rel_root / dirname) + "/ (восстановимо)")
                continue
            keep_dirs.append(dirname)
        dirnames[:] = sorted(keep_dirs)

        for filename in sorted(filenames):
            abs_path = root_path / filename
            rel_path = (rel_root / filename).as_posix().lstrip("./")
            if abs_path.is_symlink():
                result.excluded.append(f"{rel_path} (симлинк)")
                continue
            reason = _is_forbidden(rel_parts + (filename,), filename)
            if reason:
                result.excluded.append(f"{rel_path} ({reason})")
                continue
            if _is_regenerable(rel_parts):
                result.excluded.append(f"{rel_path} (восстановимо)")
                continue
            try:
                size = abs_path.stat().st_size
            except OSError:
                result.excluded.append(f"{rel_path} (недоступен)")
                continue
            if size > MAX_FILE_BYTES:
                raise ProjectPackageError(
                    f"Файл {rel_path} больше потолка ({size} байт)"
                )
            result.files.append((abs_path, rel_path))
            result.total_bytes += size
            if len(result.files) > lim.max_files:
                raise ProjectPackageError(
                    f"В версии больше {lim.max_files} файлов — пакет не собирается"
                )
            if result.total_bytes > lim.max_total_bytes:
                raise ProjectPackageError(
                    f"Версия больше потолка пакета ({lim.max_total_bytes} байт)"
                )
    result.files.sort(key=lambda pair: pair[1])
    return result


# ─── Снимки конфигурации ─────────────────────────────────────────────────────
_SECRET_KEY_RE = re.compile(
    r"(secret|token|password|passwd|api[_-]?key|credential|cookie|bootstrap)",
    re.IGNORECASE,
)


def collect_prompt_snapshot(prompts_dir: Path) -> dict[str, bytes]:
    """Снимок промптов. Только текстовые шаблоны, только относительные пути."""
    out: dict[str, bytes] = {}
    prompts_dir = Path(prompts_dir)
    if not prompts_dir.is_dir():
        return out
    for path in sorted(prompts_dir.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.suffix.lower() not in (".md", ".txt", ".json"):
            continue
        rel = path.relative_to(prompts_dir).as_posix()
        if any(part in FORBIDDEN_NAMES for part in rel.split("/")):
            continue
        out[f"prompts/{rel}"] = path.read_bytes()
    return out


def collect_model_config_snapshot(stage_models_file: Path) -> dict[str, bytes]:
    """Снимок моделей этапов. Файл вне git, и без него прогон пойдёт не на тех."""
    path = Path(stage_models_file)
    if not path.is_file():
        return {}
    return {"stage_models.json": path.read_bytes()}


def collect_feature_flags_snapshot(env: Optional[dict[str, str]] = None) -> dict[str, Any]:
    """Профиль флагов БЕЗ секретов.

    Берутся только переменные из известных префиксов, и каждая проходит через
    фильтр по имени: ключ, похожий на секрет, не попадает в снимок ни при
    каких обстоятельствах (E-25).
    """
    source = env if env is not None else os.environ
    prefixes = (
        "AUDIT_", "PIPELINE_", "STAGE01_", "STAGE02_", "FINDINGS_", "BLOCK_",
        "BUDGET_", "PAID_API_", "CRITIC_", "NORMS_",
    )
    flags: dict[str, str] = {}
    for key, value in sorted(source.items()):
        if not key.startswith(prefixes):
            continue
        if _SECRET_KEY_RE.search(key):
            continue
        flags[key] = str(value)
    return flags


def hash_files(files: dict[str, bytes]) -> str:
    """Хэш набора файлов: стабильный, зависит от имён и содержимого."""
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(files[name]).digest())
    return "sha256:" + digest.hexdigest()


def hash_json(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(blob).hexdigest()


# ─── Сборка архива ───────────────────────────────────────────────────────────
def _tar_add_bytes(tar: tarfile.TarFile, name: str, data: bytes, mtime: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mtime = mtime
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(data))


def build_project_source_package(
    *,
    dest_path: Path,
    version_dir: Path,
    manifest_base: dict[str, Any],
    snapshot_files: dict[str, bytes],
    feature_flags: dict[str, Any],
    compression: str = "gzip",
    limits: Optional[PackageLimits] = None,
) -> dict[str, Any]:
    """Собрать пакет из ФАКТИЧЕСКОГО дерева версии.

    Хардлинки сохраняются: первый файл каждого инода кладётся как обычный, все
    последующие — записью типа `link`. Карта групп уезжает в манифест, чтобы
    приёмная сторона могла проверить, что связи не потерялись.
    """
    scan = scan_version_tree(version_dir, limits=limits)
    if not scan.files:
        raise ProjectPackageError(f"В версии {version_dir} нет ни одного файла")

    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    mtime = int(time.time())

    file_entries: list[dict[str, Any]] = []
    hardlink_groups: dict[str, list[str]] = {}
    inode_first: dict[tuple[int, int], str] = {}
    uncompressed = 0

    manifest = dict(manifest_base)
    manifest.update(
        {
            "manifest_version": manifest_base.get("manifest_version", 1),
            "package_type": "source",
            "project_layout_version": PROJECT_LAYOUT_VERSION,
            "path_root": package_service.PAYLOAD_ROOT,
            "project_root": PROJECT_ROOT,
            "snapshot_root": SNAPSHOT_ROOT,
            "compression": compression,
            "created_at": time.time(),
        }
    )

    tar = package_service._open_write(tmp_path, compression)   # noqa: SLF001
    try:
        for abs_path, rel_path in scan.files:
            arc_name = package_service.PAYLOAD_ROOT + PROJECT_ROOT + rel_path
            stat = abs_path.stat()
            key = (stat.st_dev, stat.st_ino)
            if stat.st_nlink > 1 and key in inode_first:
                info = tarfile.TarInfo(arc_name)
                info.type = tarfile.LNKTYPE
                info.linkname = inode_first[key]
                info.mtime = mtime
                info.mode = 0o644
                tar.addfile(info)
                hardlink_groups.setdefault(inode_first[key], []).append(arc_name)
                file_entries.append(
                    {"path": arc_name, "bytes": 0, "hardlink_to": inode_first[key]}
                )
                continue
            data_hash = package_service.sha256_file(abs_path)
            info = tar.gettarinfo(str(abs_path), arcname=arc_name)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with abs_path.open("rb") as fh:
                tar.addfile(info, fh)
            if stat.st_nlink > 1:
                inode_first[key] = arc_name
                hardlink_groups.setdefault(arc_name, [])
            file_entries.append(
                {"path": arc_name, "bytes": stat.st_size, "sha256": data_hash}
            )
            uncompressed += stat.st_size

        for name, data in sorted(snapshot_files.items()):
            arc_name = package_service.PAYLOAD_ROOT + SNAPSHOT_ROOT + name
            _tar_add_bytes(tar, arc_name, data, mtime)
            file_entries.append(
                {
                    "path": arc_name,
                    "bytes": len(data),
                    "sha256": package_service.sha256_bytes(data),
                }
            )
            uncompressed += len(data)

        flags_blob = json.dumps(feature_flags, ensure_ascii=False, indent=2).encode("utf-8")
        flags_name = package_service.PAYLOAD_ROOT + SNAPSHOT_ROOT + "feature_flags.json"
        _tar_add_bytes(tar, flags_name, flags_blob, mtime)
        file_entries.append(
            {
                "path": flags_name,
                "bytes": len(flags_blob),
                "sha256": package_service.sha256_bytes(flags_blob),
            }
        )
        uncompressed += len(flags_blob)

        tree_source = "\n".join(
            f"{e['path']}:{e.get('sha256') or e.get('hardlink_to')}"
            for e in file_entries
        )
        manifest.update(
            {
                "files": file_entries,
                "hardlinks": {k: sorted(v) for k, v in hardlink_groups.items() if v},
                "hardlink_groups": len([v for v in hardlink_groups.values() if v]),
                "excluded_regenerable_paths": scan.excluded[:2000],
                "excluded_count": len(scan.excluded),
                "total_size": scan.total_bytes,
                "uncompressed_size": uncompressed,
                "source_tree_hash": "sha256:"
                + hashlib.sha256(tree_source.encode("utf-8")).hexdigest(),
                "feature_flags_hash": hash_json(feature_flags),
            }
        )
        manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        _tar_add_bytes(tar, package_service.MANIFEST_NAME, manifest_bytes, mtime)
    except Exception:
        tar_streams = getattr(tar, "_dw_streams", None)
        try:
            tar.close()
        except Exception:                                # noqa: BLE001
            pass
        if tar_streams:
            for stream in tar_streams:
                try:
                    stream.close()
                except Exception:                        # noqa: BLE001
                    pass
        tmp_path.unlink(missing_ok=True)
        raise
    else:
        package_service._close_write(tar)                # noqa: SLF001

    os.replace(tmp_path, dest_path)
    manifest["archive"] = {
        "sha256": package_service.sha256_file(dest_path),
        "compressed_bytes": dest_path.stat().st_size,
        "uncompressed_bytes": uncompressed + len(manifest_bytes),
        "entries": len(file_entries) + 1,
        "hardlink_entries": sum(len(v) for v in hardlink_groups.values()),
    }
    sidecar = dest_path.parent / package_service.MANIFEST_NAME
    sidecar.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest


def find_secrets_in_files(files: Iterable[tuple[str, bytes]]) -> list[str]:
    """Грубый сканер секретов для проверки собранного пакета.

    Не «на всякий случай», а рубеж: если сюда что-то попало, дальше оно уедет
    на чужой VPS. Ищутся конкретные формы, встречающиеся в этом репозитории.
    """
    patterns = (
        (re.compile(rb"wtk_[A-Za-z0-9_\-]{20,}"), "worker token"),
        (re.compile(rb"etk_[A-Za-z0-9_\-]{20,}"), "execution token"),
        (re.compile(rb"clm_[A-Za-z0-9_\-]{20,}"), "claim secret"),
        (re.compile(rb"sk-[A-Za-z0-9]{20,}"), "api key"),
        (re.compile(rb"PORTAL_SESSION_SECRET\s*="), "portal session secret"),
        (re.compile(rb"PORTAL_AUTH_USERS\s*="), "portal users"),
        (re.compile(rb"OPENROUTER_API_KEY\s*="), "openrouter key"),
        (re.compile(rb"pbkdf2_sha256\$"), "password hash"),
    )
    hits: list[str] = []
    for name, blob in files:
        for pattern, label in patterns:
            if pattern.search(blob):
                hits.append(f"{name}: {label}")
    return hits
