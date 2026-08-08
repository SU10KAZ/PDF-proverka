"""Чтение исходного пакета и сборка результирующего на стороне воркера.

Безопасность распаковки — та же лестница, что на центре (§20.9 техпроекта),
но реализована здесь отдельно намеренно: агент самодостаточен и не импортирует
backend.app.*, иначе его нельзя было бы поставить на голый VPS.

  1. sha256 архива до всего остального;
  2. потолки: распакованный объём и число записей — ДО распаковки;
  3. запрет ссылок, спецфайлов, абсолютных путей и `..`;
  4. всё лежит под payload/, итоговый путь проверяется на принадлежность staging;
  5. атомарная публикация: os.replace, «наполовину распакованного» нет.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tarfile
import time
from pathlib import Path
from typing import Any, Optional

PAYLOAD_ROOT = "payload/"
MANIFEST_NAME = "package_manifest.json"
_CHUNK = 1024 * 1024

MAX_UNPACKED_BYTES = 8 * 1024 * 1024 * 1024
MAX_ENTRIES = 200_000
# Потолок степени сжатия: архив, распаковывающийся в сотни раз больше своего
# размера, — классическая «бомба». Легитимный tar.gz из JSON даёт ~10×, запас
# до 200× оставлен намеренно широким, чтобы не отвергать нормальные пакеты.
MAX_COMPRESSION_RATIO = 200

#: Раскладки пакета проекта, которые воркер умеет исполнять. ДВОЙНИК константы
#: `project_package.SUPPORTED_PROJECT_LAYOUT_VERSIONS` — намеренный: пакет
#: `audit_worker` ставится на чужой VPS отдельно и `backend.app` не импортирует.
#: Версия 1 (плоское `payload/project/<содержимое версии>`) отвергается: на ней
#: `resolve_v2_job_paths` возвращает None, а `resolve_project_dir` доходит до
#: fallback'а по `.pdf` и возвращает ФАЙЛ вместо каталога проекта.
SUPPORTED_PROJECT_LAYOUT_VERSIONS: frozenset[int] = frozenset({2})

#: Секции архива реального аудита → каталоги попытки. `projects_v2` становится
#: `project/`: имя каталога историческое, содержимое — переносимый корень
#: `projects_v2`, и именно на него указывает `AUDIT_PROJECTS_V2_DIR`.
AUDIT_PACKAGE_SECTIONS: tuple[tuple[str, str], ...] = (
    ("projects_v2", "project"),
    ("snapshot", "snapshot"),
    ("runtime", "runtime"),
    # Профиль дисциплины — отдельный раздел со своим `tree_hash`. Раскладывает
    # его в рабочие каталоги код платформы (`remote_audit_runner`), а не агент:
    # агент не знает ни о `PROMPTS_DIR`, ни о `APP_DATA_DIR`.
    ("discipline_profile", "discipline_profile"),
)

#: Минимальная форма переносимого дерева. Проверяется ПОСЛЕ распаковки: манифест
#: может обещать что угодно, а исполняется то, что лежит на диске.
_REQUIRED_TREE_MARKERS: tuple[str, ...] = ("objects",)


class BundleError(RuntimeError):
    """Пакет не прошёл проверку — задание принимать нельзя."""


def require_portable_layout(manifest: dict[str, Any], unpacked_root: Path) -> dict[str, Any]:
    """Отвергнуть пакет, чью раскладку воркер исполнить не сможет.

    Отказ здесь стоит одну строку в логе. Отсутствие отказа стоит многочасового
    прогона, который упадёт на резолве путей уже после приёма задания, — ровно
    то, что случилось с плоской раскладкой версии 1 (Б-3 отчёта 07).
    """
    layout = manifest.get("project_layout_version")
    try:
        layout_no = int(layout)
    except (TypeError, ValueError):
        raise BundleError(
            f"В манифесте нет project_layout_version (получено {layout!r})"
        ) from None
    if layout_no not in SUPPORTED_PROJECT_LAYOUT_VERSIONS:
        raise BundleError(
            f"Раскладка пакета {layout_no} не поддерживается воркером "
            f"(поддерживаются {sorted(SUPPORTED_PROJECT_LAYOUT_VERSIONS)})"
        )

    root = Path(unpacked_root) / "projects_v2"
    if not root.is_dir():
        raise BundleError(
            "В пакете нет переносимого корня projects_v2/ — проект резолвиться "
            "не будет"
        )
    for marker in _REQUIRED_TREE_MARKERS:
        if not (root / marker).is_dir():
            raise BundleError(f"В переносимом корне нет каталога {marker}/")

    version_rel = str(manifest.get("version_relative_path") or "").strip()
    if not version_rel:
        raise BundleError("В манифесте нет version_relative_path")
    if version_rel.startswith("/") or ".." in version_rel.split("/"):
        raise BundleError(f"Небезопасный version_relative_path: {version_rel!r}")
    version_dir = root / version_rel
    if not version_dir.is_dir():
        raise BundleError(
            f"Каталог версии {version_rel} в распакованном дереве отсутствует"
        )
    project_rel = str(manifest.get("project_relative_path") or "").strip()
    if not project_rel or not (root / project_rel / "document.json").is_file():
        raise BundleError(
            "В переносимом дереве нет document.json — адаптер projects_v2 "
            "пропустит документ молча"
        )
    # Снимок профиля дисциплины обязателен для реального аудита: без него
    # процесс конвейера выбрал бы профиль сам, из дерева установленного кода.
    if str(manifest.get("job_type") or "") == "audit_pipeline_v1":
        profile_manifest = Path(unpacked_root) / "discipline_profile" / "profile_manifest.json"
        if not profile_manifest.is_file():
            raise BundleError(
                "В пакете нет снимка профиля дисциплины "
                "(payload/discipline_profile/profile_manifest.json)"
            )
        if not str(manifest.get("discipline_id") or "").strip():
            raise BundleError("В манифесте пакета нет discipline_id")
    return {
        "project_layout_version": layout_no,
        "version_dir": str(version_dir),
        "project_dir": str(root / project_rel),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_hash(value: str) -> str:
    v = (value or "").strip().lower()
    return v.split(":", 1)[1] if v.startswith("sha256:") else v


def _open_read(path: Path, compression: str) -> tarfile.TarFile:
    if compression == "gzip":
        return tarfile.open(path, "r:gz")
    if compression == "none":
        return tarfile.open(path, "r:")
    if compression == "zstd":
        import zstandard

        raw = path.open("rb")
        stream = zstandard.ZstdDecompressor().stream_reader(raw)
        tar = tarfile.open(fileobj=stream, mode="r|")
        tar._aw_streams = (stream, raw)  # type: ignore[attr-defined]
        return tar
    raise BundleError(f"Неизвестная компрессия: {compression!r}")


def _close(tar: tarfile.TarFile) -> None:
    streams = getattr(tar, "_aw_streams", None)
    tar.close()
    if streams:
        for item in streams:
            item.close()


def detect_compression(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".tar.zst"):
        return "zstd"
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "gzip"
    return "none"


def read_manifest(archive: Path, compression: Optional[str] = None) -> dict[str, Any]:
    tar = _open_read(archive, compression or detect_compression(archive))
    try:
        for member in tar:
            if member.name == MANIFEST_NAME:
                fh = tar.extractfile(member)
                if fh is not None:
                    return json.loads(fh.read().decode("utf-8"))
    finally:
        _close(tar)
    raise BundleError(f"{MANIFEST_NAME} отсутствует в архиве")


def _safe_name(name: str) -> str:
    clean = name.replace("\\", "/")
    if clean.startswith("/") or (len(clean) > 1 and clean[1] == ":"):
        raise BundleError(f"Абсолютный путь в архиве: {name!r}")
    parts = [p for p in clean.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise BundleError(f"Выход за пределы архива: {name!r}")
    return "/".join(parts)


def verify_and_unpack(
    *,
    archive: Path,
    expected_sha256: str,
    work_dir: Path,
    compression: Optional[str] = None,
) -> dict[str, Any]:
    """Проверить архив и распаковать payload/ в work_dir.

    Возвращает {manifest, files, bytes}. Любая ошибка → BundleError и
    нетронутый work_dir.
    """
    if not archive.is_file():
        raise BundleError("Архив не найден")
    actual = sha256_file(archive)
    if actual != normalize_hash(expected_sha256):
        raise BundleError(
            f"SHA-256 архива не совпал: ожидался {normalize_hash(expected_sha256)[:16]}…, "
            f"получен {actual[:16]}…"
        )

    comp = compression or detect_compression(archive)
    manifest = read_manifest(archive, comp)
    declared = int((manifest.get("archive") or {}).get("uncompressed_bytes") or 0)
    if declared > MAX_UNPACKED_BYTES:
        raise BundleError(f"Распакованный объём {declared} превышает потолок")
    declared_entries = int((manifest.get("archive") or {}).get("entries") or 0)
    if declared_entries > MAX_ENTRIES:
        raise BundleError(f"Число записей {declared_entries} превышает потолок")
    compressed = archive.stat().st_size
    if compressed and declared and declared / compressed > MAX_COMPRESSION_RATIO:
        raise BundleError(
            f"Подозрительная степень сжатия {declared / compressed:.0f}× "
            f"(потолок {MAX_COMPRESSION_RATIO}×) — архив отклонён"
        )

    # Ожидаемые хэши по файлам: расхождение = подмена содержимого при
    # совпавшем хэше архива невозможна, но манифест может лгать сам о себе —
    # сверяем и его.
    expected_hashes = {
        str(item.get("path", "")): str(item.get("sha256", ""))
        for item in (manifest.get("files") or [])
        if item.get("sha256")
    }
    required_files = [str(x) for x in (manifest.get("required_files") or [])]
    seen_names: set[str] = set()

    staging = work_dir.parent / f".{work_dir.name}.staging-{os.getpid()}-{int(time.time()*1000)}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    count = 0
    tar = _open_read(archive, comp)
    try:
        for member in tar:
            count += 1
            if count > MAX_ENTRIES:
                raise BundleError("Слишком много записей в архиве")
            if member.issym():
                # СИМВОЛИЧЕСКИЕ ссылки запрещены полностью: их цель
                # разыменовывается при чтении и может указывать куда угодно.
                raise BundleError(f"Символические ссылки запрещены: {member.name!r}")
            if member.ischr() or member.isblk() or member.isfifo() or member.isdev():
                raise BundleError(f"Спецфайл запрещён: {member.name!r}")
            if member.islnk():
                # ЖЁСТКИЕ ссылки разрешены и обязаны быть: 18 % файлов корпуса
                # (34 932 кропа блоков) — хардлинки, и запрет на них раздувал бы
                # пакет проекта на 40 %. Опасность у них другая, чем у symlink:
                # цель обязана быть УЖЕ распакованной записью внутри payload/.
                # Ссылку «вперёд» или «наружу» tar создать не сможет.
                safe = _safe_name(member.name)
                link_target = _safe_name(member.linkname or "")
                if link_target not in seen_names:
                    raise BundleError(
                        f"Жёсткая ссылка на неизвестную запись: {member.name!r} → "
                        f"{member.linkname!r}"
                    )
                if not link_target.startswith(PAYLOAD_ROOT):
                    raise BundleError(
                        f"Жёсткая ссылка ведёт вне payload/: {member.linkname!r}"
                    )
                if safe in seen_names:
                    raise BundleError(f"Повторяющийся путь в архиве: {safe!r}")
                seen_names.add(safe)
                rel = safe[len(PAYLOAD_ROOT):]
                target = staging / rel
                if not str(target.resolve()).startswith(str(staging.resolve())):
                    raise BundleError(f"Путь выходит за staging: {member.name!r}")
                source = staging / link_target[len(PAYLOAD_ROOT):]
                target.parent.mkdir(parents=True, exist_ok=True)
                try:
                    os.link(source, target)
                except OSError:
                    # Разные файловые системы или лимит nlink — копируем.
                    # Данные важнее экономии места.
                    shutil.copy2(source, target)
                continue
            safe = _safe_name(member.name)
            if safe in seen_names:
                # Повторяющийся путь: последняя запись «перекрывает» первую —
                # классический способ протащить содержимое мимо проверок.
                raise BundleError(f"Повторяющийся путь в архиве: {safe!r}")
            seen_names.add(safe)
            if safe == MANIFEST_NAME:
                continue
            if not safe.startswith(PAYLOAD_ROOT):
                raise BundleError(f"Запись вне payload/: {member.name!r}")
            rel = safe[len(PAYLOAD_ROOT):]
            total_bytes += max(0, member.size)
            if total_bytes > MAX_UNPACKED_BYTES:
                raise BundleError("Распакованный объём превысил потолок")
            target = staging / rel
            if not str(target.resolve()).startswith(str(staging.resolve())):
                raise BundleError(f"Путь выходит за staging: {member.name!r}")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise BundleError(f"Неподдерживаемый тип записи: {member.name!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            src = tar.extractfile(member)
            if src is None:
                raise BundleError(f"Не удалось прочитать запись: {member.name!r}")
            digest = hashlib.sha256()
            with target.open("wb") as out:
                while True:
                    block = src.read(_CHUNK)
                    if not block:
                        break
                    digest.update(block)
                    out.write(block)
            os.chmod(target, 0o644)
            expected = expected_hashes.get(safe)
            if expected and digest.hexdigest() != normalize_hash(expected):
                raise BundleError(
                    f"SHA-256 файла не совпал с манифестом: {rel!r}"
                )
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        _close(tar)

    # Обязательные файлы источника: их отсутствие означает, что пакет собран
    # не по контракту, и запускаться по нему нельзя.
    missing = [
        name for name in required_files
        if not (staging / name[len(PAYLOAD_ROOT):]).is_file()
        and not (staging / name).is_file()
    ]
    if missing:
        shutil.rmtree(staging, ignore_errors=True)
        raise BundleError(f"В пакете нет обязательных файлов: {missing}")

    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, work_dir)
    return {"manifest": manifest, "files": count, "bytes": total_bytes}


# Разделы результирующего пакета. `input/` — что получили, `work/` — как
# считали, `result/` — что получилось. Без первых двух разбор инцидента
# сводится к гаданию.
RESULT_SECTIONS = ("input", "work", "result")
# Что из рабочего каталога наружу не уходит: в параметрах нет секретов, но
# принцип «в результат попадает только явно перечисленное» дешевле поддерживать,
# чем каждый раз доказывать безопасность нового файла.
_WORK_ALLOWLIST = {"test_params.json", "completed.marker", "pipeline_log.json",
                   "process_exit.json"}

#: Что из дерева проекта возвращается центру. Всё остальное центр уже имеет:
#: он сам это отправлял, и обратная перезапись исходников заказчика недопустима.
#: Пути ОТНОСИТЕЛЬНЫ каталога версии — именно так их читает
#: `result_import.classify_path`, и любой другой корень отправил бы весь пакет
#: в `unknown`, то есть под отказ целиком.
_PROJECT_RETURN_PREFIXES = ("03_analysis/", "99_service/")


class PortableTreeError(BundleError):
    """Переносимое дерево не имеет единственного каталога версии."""


def portable_version_dir(project_root: Path, *, hint: Optional[str] = None) -> Path:
    """Найти каталог версии внутри переносимого корня `projects_v2`.

    Пакет по контракту содержит РОВНО ОДИН object/document/version. Поэтому
    поиск детерминирован, а неоднозначность — ошибка, а не повод «взять
    первый»: взяв не тот, воркер вернул бы центру артефакты чужой версии.
    """
    root = Path(project_root)
    if hint:
        clean = str(hint).replace("\\", "/").strip("/")
        if clean and ".." not in clean.split("/"):
            candidate = root / clean
            if candidate.is_dir():
                return candidate
    matches = sorted(root.glob("objects/*/disciplines/*/documents/*/versions/*"))
    matches = [p for p in matches if p.is_dir()]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise PortableTreeError(
            f"В переносимом корне {root} нет каталога версии "
            "objects/*/disciplines/*/documents/*/versions/*"
        )
    raise PortableTreeError(
        f"В переносимом корне {root} несколько каталогов версии: "
        + ", ".join(str(p.relative_to(root)) for p in matches[:5])
    )

#: Разделы, которые собираются для реального аудита.
_AUDIT_SECTIONS = ("project", "work", "result", "usage", "logs")

#: Потолок объёма логов в пакете: полный stdout многочасового аудита — сотни
#: мегабайт, и центру он нужен как диагностика, а не как архив.
_MAX_LOG_BYTES = 8 * 1024 * 1024


def build_result_package(
    *,
    dest_path: Path,
    job_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    worker_id: str,
    worker_version: str,
    protocol_version: int,
    manifest_version: int,
    source_package_hash: Optional[str] = None,
    exit_code: int = 0,
    compression: str = "gzip",
    job_type: str = "test_pipeline_v1",
    required_artifacts: Optional[list[str]] = None,
    pipeline_revision: Optional[str] = None,
    stage_completion: Optional[dict[str, Any]] = None,
    resume_hint: Optional[str] = None,
    cancellation_state: Optional[str] = None,
    project_version_rel: Optional[str] = None,
    runtime_snapshot_hash: Optional[str] = None,
    applied_write_mode: Optional[str] = None,
    execution_profile: Optional[str] = None,
    worker_stage_plan: Optional[list[str]] = None,
    completed_stages: Optional[list[str]] = None,
    forbidden_stages_not_run: Optional[list[str]] = None,
    provider_mode: Optional[str] = None,
    external_network_attempts: Optional[int] = None,
    source_integrity: Optional[dict[str, Any]] = None,
    discipline_id: Optional[str] = None,
    discipline_profile_hash: Optional[str] = None,
) -> dict[str, Any]:
    """Собрать TAR результата: input/ + work/ + result/ (+ project/usage/logs).

    Архив материализуется на диск ДО уведомления центра — это и есть защита
    «готовый пакет не должен потеряться» (§11.8 техпроекта).
    """
    result_dir = job_dir / "result"
    files: dict[str, bytes] = {}
    is_audit = job_type == "audit_pipeline_v1"

    if is_audit:
        # project/: ТОЛЬКО то, что произвёл конвейер, и ТОЛЬКО относительно
        # каталога версии. Исходники заказчика обратно не едут — центр их и
        # отправлял, а перезапись PDF необратима.
        project_root = job_dir / "project"
        if project_root.is_dir():
            version_dir = portable_version_dir(
                project_root, hint=project_version_rel
            )
            for path in sorted(version_dir.rglob("*")):
                if not path.is_file() or path.is_symlink():
                    continue
                rel = path.relative_to(version_dir).as_posix()
                if not rel.startswith(_PROJECT_RETURN_PREFIXES):
                    continue
                files[f"project/{rel}"] = path.read_bytes()
        usage_dir = job_dir / "usage"
        if usage_dir.is_dir():
            for path in sorted(usage_dir.rglob("*")):
                # Симлинк пропускается ВЕЗДЕ, а не только в дереве проекта:
                # этап или CLI модели с правом записи в каталог попытки мог бы
                # положить ссылку и отправить центру байты её цели внутри
                # пакета, который манифест объявляет чистым.
                if path.is_file() and not path.is_symlink():
                    files["usage/" + path.relative_to(usage_dir).as_posix()] = (
                        path.read_bytes()
                    )
        logs_dir = job_dir / "logs"
        if logs_dir.is_dir():
            for path in sorted(logs_dir.rglob("*")):
                if not path.is_file() or path.is_symlink():
                    continue
                blob = path.read_bytes()
                if len(blob) > _MAX_LOG_BYTES:
                    # Обрезка ВИДНА: молчаливо укороченный лог хуже отсутствующего.
                    blob = (
                        b"[...journal truncated by worker: keeping the tail...]\n"
                        + blob[-_MAX_LOG_BYTES:]
                    )
                files["logs/" + path.relative_to(logs_dir).as_posix()] = blob

    # input/: описание задания из исходного пакета — что именно нам выдали.
    source_job = job_dir / "work" / "job.json"
    if source_job.is_file():
        files["input/job.json"] = source_job.read_bytes()

    # work/: только явно разрешённое.
    work_dir = job_dir / "work"
    if work_dir.is_dir():
        for name in sorted(_WORK_ALLOWLIST):
            candidate = work_dir / name
            if candidate.is_file():
                files[f"work/{name}"] = candidate.read_bytes()

    # result/: всё, что произвёл процесс.
    for path in sorted(result_dir.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.name.endswith(".tar.gz") or path.name.endswith(".tar.gz.tmp"):
            continue          # сам архив внутрь себя не кладём
        files["result/" + path.relative_to(result_dir).as_posix()] = path.read_bytes()

    if not any(k.startswith("result/") for k in files):
        raise BundleError("Каталог результата пуст — собирать нечего")

    # Сканирование СОДЕРЖИМОГО на абсолютные пути хоста. Дёшево (артефакты —
    # JSON), и превращает поле манифеста из обещания в измерение.
    # Маркеры выводятся из ФАКТИЧЕСКОГО каталога попытки, а не из фиксированного
    # списка `/home,/var,/opt,…`: воркер с корнем `/data` или `/mnt` проходил бы
    # проверку, продолжая утекать. Список остаётся как дополнение — он ловит
    # чужие пути, а не только свои.
    attempt_prefix = str(Path(job_dir).resolve())
    markers = [attempt_prefix]
    for parent in Path(attempt_prefix).parents:
        text_parent = str(parent)
        if text_parent in ("/", ""):
            break
        markers.append(text_parent)
        if len(markers) >= 4:
            break
    markers += ["/home/", "/var/", "/opt/", "/tmp/", "/root/", "/srv/", "/mnt/", "/data/"]

    absolute_path_hits: list[str] = []
    for rel, data in sorted(files.items()):
        if not rel.endswith((".json", ".jsonl", ".txt", ".md")):
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for marker in markers:
            if marker and marker in text:
                absolute_path_hits.append(rel)
                break

    entries = []
    uncompressed = 0
    for rel, data in sorted(files.items()):
        entries.append(
            {
                "path": PAYLOAD_ROOT + rel,
                "bytes": len(data),
                "sha256": sha256_bytes(data),
                "mode": "0644",
            }
        )
        uncompressed += len(data)

    manifest: dict[str, Any] = {
        "manifest_version": manifest_version,
        "package_id": f"pkg_{attempt_id}",
        "package_type": "result",
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": project_id,
        "version_id": version_id,
        "created_at": time.time(),
        "created_by": {"role": "worker"},
        "worker_id": worker_id,
        "worker_version": worker_version,
        "protocol_version": protocol_version,
        "project_layout_version": (
            max(SUPPORTED_PROJECT_LAYOUT_VERSIONS) if is_audit else 0
        ),
        "job_type": job_type,
        "pipeline_revision": pipeline_revision,
        "compression": compression,
        # Что ФАКТИЧЕСКИ применялось на воркере. Не повтор задания: центр
        # обязан иметь возможность проверить, что попытка шла по той
        # конфигурации, которую он отправлял, а не по локальной воркера.
        "runtime_snapshot_hash": runtime_snapshot_hash,
        "applied_write_mode": applied_write_mode,
        "execution_profile": execution_profile,
        "worker_stage_plan": list(worker_stage_plan or []),
        "completed_stages": list(completed_stages or []),
        "forbidden_stages_not_run": list(forbidden_stages_not_run or []),
        "provider_mode": provider_mode,
        # Дисциплина и хэш ПРИМЕНЁННОГО профиля. Центр сверяет их с тем, что
        # отправлял: расхождение означает аудит чужим профилем.
        "discipline_id": discipline_id,
        "discipline_profile_hash": discipline_profile_hash,
        # None означает «не измерялось», а не «ноль». Ноль по умолчанию был
        # аттестацией, которую не производит ни одна строка кода: читающий
        # манифест на центре принимал её за измерение.
        "external_network_attempts": (
            None if external_network_attempts is None
            else int(external_network_attempts)
        ),
        "source_integrity": dict(source_integrity or {}),
        # Хэш исходного пакета: связывает результат с тем, из чего он получен.
        "source_package_hash": normalize_hash(source_package_hash or "") or None,
        "exit_code": exit_code,
        "cancellation_state": cancellation_state,
        "sections": list(_AUDIT_SECTIONS if is_audit else RESULT_SECTIONS),
        "path_root": PAYLOAD_ROOT,
        "files": entries,
        "hardlinks": {},
        "hardlink_groups": {},
        "required_artifacts": list(
            required_artifacts
            if required_artifacts is not None
            else ["result/summary.json", "result/run_log.txt"]
        ),
        "generated_artifacts": sorted(files),
        "stage_completion": dict(stage_completion or {}),
        "resume_hint": resume_hint,
        "excluded_artifacts": [],
        "excluded_recoverable": [],
        # Раньше здесь стояло безусловное False. Это было УТВЕРЖДЕНИЕ, которого
        # никто не проверял: имена записей действительно относительны, а вот
        # СОДЕРЖИМОЕ артефактов несёт абсолютные пути каталога попытки
        # (`pipeline_log.artifacts_dir`, `stage01_meta.runtime_plan_path`), и
        # центр, построивший логику на этом поле, получал бы их в дерево
        # проекта. Теперь поле вычисляется, а нарушители перечисляются.
        "path_rules": {
            "absolute_paths_present": bool(absolute_path_hits),
            "absolute_path_files": absolute_path_hits[:200],
            "rewrite_on_unpack": [],
        },
        "tree_hash": "sha256:"
        + sha256_bytes(
            "\n".join(f"{e['path']}:{e['sha256']}" for e in entries).encode("utf-8")
        ),
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_suffix(dest_path.suffix + ".tmp")
    mode = {"gzip": "w:gz", "none": "w"}.get(compression, "w:gz")
    with tarfile.open(tmp, mode) as tar:
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(manifest_bytes)
        info.mtime = int(time.time())
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(manifest_bytes))
        for rel, data in sorted(files.items()):
            item = tarfile.TarInfo(PAYLOAD_ROOT + rel)
            item.size = len(data)
            item.mtime = int(time.time())
            item.mode = 0o644
            tar.addfile(item, io.BytesIO(data))
    os.replace(tmp, dest_path)

    manifest["archive"] = {
        "sha256": sha256_file(dest_path),
        "compressed_bytes": dest_path.stat().st_size,
        "uncompressed_bytes": uncompressed + len(manifest_bytes),
        "entries": len(entries) + 1,
        "hardlink_entries": 0,
    }
    # Манифест внутри архива уже без блока archive (он самореферентен) —
    # центр читает его оттуда, а размеры сверяет по факту.
    return manifest
