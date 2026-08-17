"""Безопасная загрузка ZIP-архивов stage_1/stage_2 для выбранного объекта.

Выбранный объект берётся из общего platform object registry, а не из
собственного selector'а stage-comparison. Архив распаковывается во временную
папку, полностью проверяется и импортируется в versioned layout стадии.
Повторная загрузка документа создаёт следующую ``vNNN``; предыдущее состояние
также сохраняется recoverable backup'ом.
"""
from __future__ import annotations

import os
import json
import shutil
import stat
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from backend.app.services.common import object_service
from backend.app.services.storage.storage_write_facade import get_write_facade

from . import stage_storage


VALID_STAGES = {"stage_1", "stage_2"}


class StageUploadError(ValueError):
    """Архив или выбранный объект непригодны для загрузки стадии."""


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int((os.environ.get(name) or "").strip() or default))
    except ValueError:
        return default


def _max_archive_bytes() -> int:
    return _env_int("STAGE_COMPARISON_UPLOAD_MAX_ARCHIVE_BYTES", 2 * 1024**3)


def _max_unpacked_bytes() -> int:
    return _env_int("STAGE_COMPARISON_UPLOAD_MAX_UNPACKED_BYTES", 10 * 1024**3)


def _max_members() -> int:
    return _env_int("STAGE_COMPARISON_UPLOAD_MAX_MEMBERS", 20_000)


def _safe_object_dir_name(value: str) -> str:
    name = str(value or "").strip()
    if (
        not name
        or name in {".", ".."}
        or Path(name).name != name
        or "/" in name
        or "\\" in name
        or "\x00" in name
    ):
        raise StageUploadError("Некорректное имя выбранного объекта")
    return name


def _projects_v2_root() -> Path:
    root = get_write_facade().v2_root()
    if root is None:
        raise StageUploadError("Не удалось определить каталог projects_v2")
    return Path(root).resolve()


def _ensure_v2_object_metadata(obj: dict, object_dir: Path) -> None:
    """Создать минимальный стандартный каркас объекта projects_v2.

    Comparison не относится к дисциплине, поэтому живёт рядом с ``DOC`` и
    ``disciplines``, а не внутри одного из них.
    """
    object_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = object_dir / "object.json"
    if metadata_path.is_file():
        try:
            current = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StageUploadError(f"Повреждён object.json: {object_dir}") from exc
        if str(current.get("object_id") or "") != str(obj.get("id") or ""):
            raise StageUploadError(f"Папка объекта уже занята: {object_dir}")
    else:
        metadata_path.write_text(json.dumps({
            "schema_version": 1,
            "object_id": obj.get("id"),
            "legacy_name": obj.get("name"),
            "legacy_path": obj.get("projects_dir"),
            "created_at": obj.get("created_at"),
            "display_name": obj.get("name"),
            "folder_name": object_dir.name,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
    (object_dir / "DOC").mkdir(exist_ok=True)
    (object_dir / "disciplines").mkdir(exist_ok=True)


def resolve_object_dir(object_id: str, *, create: bool = False) -> tuple[dict, Path]:
    """Вернуть ``projects_v2/objects/<object>/comparison`` для объекта UI."""
    obj = object_service.get_object_by_id(object_id)
    if obj is None:
        raise StageUploadError("Выбранный объект не найден")

    v2_root = _projects_v2_root()
    v2lib = get_write_facade()._load_v2lib()
    if create:
        project_object_dir = v2lib.allocate_object_folder(
            v2_root, str(obj.get("id") or object_id), str(obj.get("name") or object_id),
        )
    else:
        project_object_dir = v2lib.resolve_object_folder(
            v2_root, str(obj.get("id") or object_id), str(obj.get("name") or object_id),
        )
    project_object_dir = Path(project_object_dir).resolve()
    try:
        project_object_dir.relative_to((v2_root / "objects").resolve())
    except ValueError as exc:
        raise StageUploadError("Путь объекта выходит за пределы projects_v2/objects") from exc

    if create:
        _ensure_v2_object_metadata(obj, project_object_dir)
    comparison_dir = project_object_dir / "comparison"
    if comparison_dir.is_symlink():
        raise StageUploadError("Символическая ссылка вместо comparison запрещена")
    if create:
        comparison_dir.mkdir(parents=True, exist_ok=True)
    return obj, comparison_dir


def _safe_member_path(info: zipfile.ZipInfo) -> PurePosixPath | None:
    raw = str(info.filename or "")
    path = _safe_relative_path(raw)
    if path is None:
        return None
    mode = info.external_attr >> 16
    if mode and stat.S_ISLNK(mode):
        raise StageUploadError(f"Символические ссылки в архиве запрещены: {raw}")
    if info.flag_bits & 0x1:
        raise StageUploadError(f"Зашифрованные файлы не поддерживаются: {raw}")
    return path


def _safe_relative_path(value: str) -> PurePosixPath | None:
    """Проверить относительный путь из ZIP или browser folder upload."""
    raw = str(value or "").replace("\\", "/")
    if not raw or "\x00" in raw:
        raise StageUploadError("Передан файл с некорректным именем")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise StageUploadError(f"Небезопасный путь файла: {raw}")
    if any(part.endswith(":") for part in path.parts):
        raise StageUploadError(f"Небезопасный путь файла: {raw}")
    if "__MACOSX" in path.parts or path.name in {".DS_Store", "Thumbs.db"}:
        return None
    return path


def _copy_archive(upload: BinaryIO, destination: Path) -> int:
    try:
        upload.seek(0)
    except (AttributeError, OSError):
        pass
    total = 0
    limit = _max_archive_bytes()
    with destination.open("wb") as dst:
        while True:
            chunk = upload.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise StageUploadError(
                    f"Архив превышает допустимый размер ({limit // 1024**2} МБ)"
                )
            dst.write(chunk)
    return total


def _extract_checked(zip_path: Path, destination: Path) -> dict:
    try:
        archive = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile as exc:
        raise StageUploadError("Файл повреждён или не является ZIP-архивом") from exc

    files = 0
    pdfs = 0
    unpacked = 0
    seen: set[str] = set()
    try:
        infos = archive.infolist()
        if len(infos) > _max_members():
            raise StageUploadError(f"В архиве слишком много элементов: {len(infos)}")
        declared_total = sum(max(0, int(info.file_size or 0)) for info in infos)
        if declared_total > _max_unpacked_bytes():
            raise StageUploadError("Архив слишком большой в распакованном виде")

        for info in infos:
            rel = _safe_member_path(info)
            if rel is None:
                continue
            key = rel.as_posix().casefold()
            if key in seen:
                raise StageUploadError(f"Дублирующийся путь в архиве: {rel.as_posix()}")
            seen.add(key)
            out = destination.joinpath(*rel.parts)
            try:
                out.resolve().relative_to(destination.resolve())
            except ValueError as exc:
                raise StageUploadError(f"Небезопасный путь в архиве: {rel.as_posix()}") from exc
            if info.is_dir():
                out.mkdir(parents=True, exist_ok=True)
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            try:
                src = archive.open(info)
            except (RuntimeError, NotImplementedError) as exc:
                raise StageUploadError(f"Не удалось прочитать {rel.as_posix()}: {exc}") from exc
            with src, out.open("wb") as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    unpacked += len(chunk)
                    if unpacked > _max_unpacked_bytes():
                        raise StageUploadError("Архив слишком большой в распакованном виде")
                    dst.write(chunk)
            files += 1
            if out.suffix.lower() == ".pdf":
                pdfs += 1
    finally:
        archive.close()

    if pdfs == 0:
        raise StageUploadError("Архив не содержит PDF-файлов")
    return {"files_count": files, "pdf_count": pdfs, "unpacked_bytes": unpacked}


def _copy_selected_folder(
    uploads: list[tuple[BinaryIO, str]],
    destination: Path,
) -> dict:
    """Скопировать единым деревом папку, выбранную в браузере."""
    if not uploads:
        raise StageUploadError("Папка не содержит файлов")
    if len(uploads) > _max_members():
        raise StageUploadError(f"В папке слишком много файлов: {len(uploads)}")

    files = 0
    pdfs = 0
    unpacked = 0
    seen: set[str] = set()
    uploaded_zips: list[Path] = []
    for stream, raw_path in uploads:
        rel = _safe_relative_path(raw_path)
        if rel is None:
            continue
        key = rel.as_posix().casefold()
        if key in seen:
            raise StageUploadError(f"Дублирующийся путь в папке: {rel.as_posix()}")
        seen.add(key)
        out = destination.joinpath(*rel.parts)
        try:
            out.resolve().relative_to(destination.resolve())
        except ValueError as exc:
            raise StageUploadError(f"Небезопасный путь файла: {rel.as_posix()}") from exc
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            stream.seek(0)
        except (AttributeError, OSError):
            pass
        with out.open("wb") as dst:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                unpacked += len(chunk)
                if unpacked > _max_unpacked_bytes():
                    raise StageUploadError("Папка слишком большая для загрузки")
                dst.write(chunk)
        files += 1
        if out.suffix.lower() == ".pdf":
            pdfs += 1
        elif out.suffix.lower() == ".zip":
            uploaded_zips.append(out)

    # Папка инженера часто содержит по одному ZIP-комплекту на проект.
    # Распаковываем выбранные комплекты в изолированные подпапки; сам временный
    # ZIP в stage не переносится.
    for zip_path in uploaded_zips:
        bundle_dir = zip_path.parent / _safe_object_dir_name(zip_path.stem)
        if bundle_dir.exists():
            bundle_dir = zip_path.parent / f"{_safe_object_dir_name(zip_path.stem)}_bundle"
        bundle_dir.mkdir(parents=True)
        zip_stats = _extract_checked(zip_path, bundle_dir)
        unpacked += int(zip_stats["unpacked_bytes"])
        if unpacked > _max_unpacked_bytes():
            raise StageUploadError("Папка слишком большая для загрузки")
        files += int(zip_stats["files_count"]) - 1
        pdfs += int(zip_stats["pdf_count"])
        zip_path.unlink()
    if pdfs == 0:
        raise StageUploadError("В выбранной папке не найдено PDF-файлов")
    return {"files_count": files, "pdf_count": pdfs, "unpacked_bytes": unpacked}


def _stage_pdf_count(path: Path) -> int:
    if not path.is_dir():
        return 0
    if stage_storage.is_versioned_stage(path):
        return stage_storage.current_document_count(path)
    try:
        return sum(1 for item in path.rglob("*.pdf") if item.is_file())
    except OSError:
        return 0


def _prepare_stage_target(object_id: str, stage_name: str):
    if stage_name not in VALID_STAGES:
        raise StageUploadError("Разрешены только stage_1 и stage_2")
    obj, object_dir = resolve_object_dir(object_id, create=True)
    target = object_dir / stage_name
    # Проверяем формат до создания stage.json: иначе legacy-папка выглядела бы
    # уже переведённой в новый профиль и её файлы не мигрировали бы.
    target_was_versioned = stage_storage.is_versioned_stage(target)
    previous_pdf_count = _stage_pdf_count(target)
    # Обе папки создаются сразу в versioned storage profile.
    for name in sorted(VALID_STAGES):
        stage_dir = object_dir / name
        if stage_dir.is_symlink():
            raise StageUploadError(f"Символическая ссылка вместо {name} запрещена")
    stage_storage.ensure_comparison_object_scaffold(
        object_dir,
        object_id=str(obj.get("id") or object_id),
        object_name=str(obj.get("name") or object_dir.name),
    )
    return obj, object_dir, target, target_was_versioned, previous_pdf_count


def _commit_extracted_stage(
    *,
    obj: dict,
    object_id: str,
    object_dir: Path,
    target: Path,
    target_was_versioned: bool,
    previous_pdf_count: int,
    stage_name: str,
    extracted: Path,
    structured: Path,
    work_root: Path,
    upload_filename: str | None,
    transfer_stats: dict,
) -> dict:
    """Импортировать проверенное дерево и атомарно переключить stage."""
    backup_path: Path | None = None
    previous_scaffold: Path | None = None
    if target_was_versioned:
        shutil.copytree(target, structured)
    else:
        structured.mkdir()
        stage_storage.ensure_stage_scaffold(
            structured,
            stage_name=stage_name,
            object_id=str(obj.get("id") or object_id),
            object_name=str(obj.get("name") or object_dir.name),
        )
        if target.is_dir() and previous_pdf_count > 0:
            stage_storage.import_extracted_tree(
                target,
                structured,
                stage_name=stage_name,
                object_id=str(obj.get("id") or object_id),
                object_name=str(obj.get("name") or object_dir.name),
                upload_filename="legacy_stage_migration",
            )

    import_stats = stage_storage.import_extracted_tree(
        extracted,
        structured,
        stage_name=stage_name,
        object_id=str(obj.get("id") or object_id),
        object_name=str(obj.get("name") or object_dir.name),
        upload_filename=upload_filename,
    )

    has_previous = target.is_dir() and previous_pdf_count > 0
    if has_previous:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_root = object_dir / "_stage_upload_backups"
        if backup_root.is_symlink():
            raise StageUploadError("Символическая ссылка вместо backup-папки запрещена")
        backup_path = backup_root / f"{stage_name}_{stamp}"
        backup_root.mkdir(parents=True, exist_ok=True)
        os.replace(target, backup_path)
    elif target.exists():
        previous_scaffold = work_root / "previous_scaffold"
        os.replace(target, previous_scaffold)

    try:
        os.replace(structured, target)
    except Exception:
        if backup_path is not None and backup_path.exists() and not target.exists():
            os.replace(backup_path, target)
        elif previous_scaffold is not None and previous_scaffold.exists() and not target.exists():
            os.replace(previous_scaffold, target)
        raise

    stage_paths = {name: str(object_dir / name) for name in sorted(VALID_STAGES)}
    stage_pdf_counts = {
        name: _stage_pdf_count(object_dir / name) for name in sorted(VALID_STAGES)
    }
    return {
        "status": "ok",
        "object_id": obj.get("id"),
        "object_name": obj.get("name"),
        "object_path": str(object_dir),
        "stage": stage_name,
        "stage_path": str(target),
        "stage_paths": stage_paths,
        "stage_pdf_counts": stage_pdf_counts,
        "ready_for_comparison": all(stage_pdf_counts.get(name, 0) > 0 for name in VALID_STAGES),
        "backup_path": str(backup_path) if backup_path is not None else None,
        **transfer_stats,
        **import_stats,
    }


def replace_stage_from_zip(
    object_id: str,
    stage_name: str,
    upload: BinaryIO,
    filename: str | None,
) -> dict:
    """Проверить ZIP и импортировать его как версии документов stage."""
    if not str(filename or "").lower().endswith(".zip"):
        raise StageUploadError("Загрузите ZIP-архив")

    obj, object_dir, target, target_was_versioned, previous_pdf_count = (
        _prepare_stage_target(object_id, stage_name)
    )

    temp_root = Path(tempfile.mkdtemp(prefix=f".{stage_name}_upload_", dir=object_dir))
    zip_path = temp_root / "upload.zip"
    extracted = temp_root / "extracted"
    extracted.mkdir()
    structured = temp_root / "structured"
    try:
        archive_bytes = _copy_archive(upload, zip_path)
        stats = _extract_checked(zip_path, extracted)
        return _commit_extracted_stage(
            obj=obj, object_id=object_id, object_dir=object_dir, target=target,
            target_was_versioned=target_was_versioned,
            previous_pdf_count=previous_pdf_count, stage_name=stage_name,
            extracted=extracted, structured=structured, work_root=temp_root,
            upload_filename=filename,
            transfer_stats={"upload_type": "zip", "archive_bytes": archive_bytes, **stats},
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def replace_stage_from_folder(
    object_id: str,
    stage_name: str,
    uploads: list[tuple[BinaryIO, str]],
    folder_name: str | None = None,
) -> dict:
    """Импортировать целиком папку, выбранную в браузере."""
    obj, object_dir, target, target_was_versioned, previous_pdf_count = (
        _prepare_stage_target(object_id, stage_name)
    )
    temp_root = Path(tempfile.mkdtemp(prefix=f".{stage_name}_folder_", dir=object_dir))
    extracted = temp_root / "extracted"
    extracted.mkdir()
    structured = temp_root / "structured"
    try:
        stats = _copy_selected_folder(uploads, extracted)
        return _commit_extracted_stage(
            obj=obj, object_id=object_id, object_dir=object_dir, target=target,
            target_was_versioned=target_was_versioned,
            previous_pdf_count=previous_pdf_count, stage_name=stage_name,
            extracted=extracted, structured=structured, work_root=temp_root,
            upload_filename=folder_name or "browser_folder_upload",
            transfer_stats={"upload_type": "folder", **stats},
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


__all__ = [
    "StageUploadError",
    "VALID_STAGES",
    "resolve_object_dir",
    "replace_stage_from_folder",
    "replace_stage_from_zip",
]
