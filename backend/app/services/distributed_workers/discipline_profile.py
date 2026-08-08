"""Immutable-снимок профиля дисциплины для удалённой попытки.

**Почему профиль едет отдельным разделом, а не «вместе с промптами».**
До этого этапа в пакет уезжало ВСЁ дерево `prompts/`, включая профили всех
четырнадцати дисциплин. Три следствия, и каждое реальное:

  1. на воркер уезжали чужие профили — в том числе EOM, — и «аудит пошёл не тем
     профилем» нельзя было отличить от «аудит пошёл своим»: оба файла лежали
     рядом;
  2. хэш промптов менялся от правки ЛЮБОГО профиля, то есть попытка становилась
     несовместимой из-за раздела, который к ней не относится;
  3. проверить «применён профиль ВК» было нечем: отдельного хэша не было.

Здесь снимок собирается ровно на одну дисциплину, имеет собственный
`tree_hash`, и этот хэш проверяется трижды: воркером после распаковки, самим
процессом конвейера перед запуском и центром при приёме результата.

**Состав снимка** — два логических корня, и оба обязательны, потому что
конвейер читает профиль из ДВУХ мест:

  `prompts/`  → `AUDIT_PROMPTS_DIR` (`prompts/disciplines/<dir>/…`) —
                ролевой профиль, чек-лист, триаж, типы чертежей, нормативный
                справочник дисциплины; их читает `discipline_service`;
  `app_data/` → `AUDIT_APP_DATA_DIR` (`discipline_checklists/<КОД>.md` и
                `discipline_checklists_metadata/<КОД>.json`) — каталог, о
                котором отчёт 08 (§24 п. 5) написал «на воркере его нет».

Пути внутри снимка — только относительные POSIX. Обхода каталога нет и быть не
может: сегменты выводятся из ПРОВЕРЕННОГО `profile_dir` реестра, а не из
пользовательской строки.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from backend.app.services.common import discipline_identity

#: Версия схемы `profile_manifest.json`. Растёт при несовместимом изменении.
PROFILE_MANIFEST_VERSION = 1

#: Каталог раздела внутри архива (под общим `payload/`).
PROFILE_ROOT = "discipline_profile/"

#: Подкаталог с самими файлами. Отдельный уровень нужен, чтобы манифест не
#: мог быть перекрыт файлом профиля с тем же именем.
FILES_PREFIX = "files/"

#: Логические корни снимка → куда их разворачивает воркер.
PROMPTS_ROOT = "prompts/"
APP_DATA_ROOT = "app_data/"
LOGICAL_ROOTS: tuple[str, ...] = (PROMPTS_ROOT, APP_DATA_ROOT)

#: Расширения файлов профиля. Профиль — текст и JSON; исполняемого в нём быть
#: не может, и проверять это дешевле, чем доказывать.
ALLOWED_SUFFIXES: frozenset[str] = frozenset({".md", ".txt", ".json"})

#: Без чего профиль не является профилем. Отсутствие любого — отказ СБОРКИ
#: пакета, то есть до отправки задания на воркер (CH-04).
REQUIRED_PROFILE_FILES: tuple[str, ...] = ("role.md", "checklist.md")

#: Каталоги чек-листов в `APP_DATA_DIR`. Именуются по КАНОНИЧЕСКОМУ коду.
CHECKLIST_DIRS: tuple[str, ...] = (
    "discipline_checklists",
    "discipline_checklists_metadata",
)


class DisciplineProfileSnapshotError(RuntimeError):
    """Снимок профиля собрать или применить нельзя."""


@dataclass(frozen=True)
class DisciplineProfileSnapshot:
    """Готовый снимок: файлы, манифест и его хэш."""

    discipline_id: str
    profile_dir: str
    files: dict[str, bytes] = field(default_factory=dict)
    manifest: dict[str, Any] = field(default_factory=dict)

    @property
    def tree_hash(self) -> str:
        return str(self.manifest.get("tree_hash") or "")

    def manifest_bytes(self) -> bytes:
        return json.dumps(
            self.manifest, ensure_ascii=False, indent=2, sort_keys=True
        ).encode("utf-8")

    def package_entries(self) -> dict[str, bytes]:
        """Записи для архива: манифест + файлы, все под `discipline_profile/`."""
        out = {PROFILE_ROOT + "profile_manifest.json": self.manifest_bytes()}
        for rel, blob in self.files.items():
            out[PROFILE_ROOT + FILES_PREFIX + rel] = blob
        return out


def _sha256(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def tree_hash(files: dict[str, bytes]) -> str:
    """Хэш набора: зависит и от имён, и от содержимого, и от состава."""
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(files[name]).digest())
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _safe_relative(rel: str) -> str:
    text = str(rel or "").replace("\\", "/").strip()
    if not text:
        raise DisciplineProfileSnapshotError("пустой путь внутри снимка профиля")
    if text.startswith("/"):
        raise DisciplineProfileSnapshotError(f"абсолютный путь в снимке: {text!r}")
    parts = [p for p in text.split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        raise DisciplineProfileSnapshotError(f"обход каталога в снимке: {text!r}")
    if not text.startswith(LOGICAL_ROOTS):
        raise DisciplineProfileSnapshotError(
            f"путь {text!r} вне логических корней снимка {LOGICAL_ROOTS}"
        )
    return "/".join(parts)


def collect_profile_snapshot(
    discipline: discipline_identity.DisciplineId,
    *,
    prompts_dir: Path,
    app_data_dir: Path,
    source_revision: str = "",
    created_at: Optional[float] = None,
) -> DisciplineProfileSnapshot:
    """Собрать снимок профиля ОДНОЙ дисциплины.

    Отсутствие обязательного файла — исключение здесь, а не предупреждение на
    воркере: задание, отправленное без ролевого профиля, отработает многочасовой
    прогон и вернёт замечания, найденные не тем экспертом.
    """
    profile_dir_name = discipline.profile_dir
    if not discipline_identity.safe_profile_segment(profile_dir_name):
        raise DisciplineProfileSnapshotError(
            f"Небезопасное имя каталога профиля: {profile_dir_name!r}"
        )
    source_dir = Path(prompts_dir) / "disciplines" / profile_dir_name
    if not source_dir.is_dir():
        raise DisciplineProfileSnapshotError(
            f"Профиль дисциплины {discipline.code!r} не найден: {source_dir}. "
            "Задание не отправляется: удалённый прогон пошёл бы профилем EOM."
        )

    files: dict[str, bytes] = {}
    entries: list[dict[str, Any]] = []

    def take(path: Path, rel: str) -> None:
        if path.is_symlink():
            # Симлинк внутри профиля — либо ошибка, либо попытка вынести чужой
            # файл: снимок обязан быть самодостаточным.
            raise DisciplineProfileSnapshotError(
                f"Символическая ссылка в профиле запрещена: {path}"
            )
        blob = path.read_bytes()
        key = _safe_relative(rel)
        if key in files:
            raise DisciplineProfileSnapshotError(f"Повтор пути в снимке: {key!r}")
        files[key] = blob
        entries.append(
            {"path": key, "bytes": len(blob), "sha256": _sha256(blob)}
        )

    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in ALLOWED_SUFFIXES:
            continue
        rel = path.relative_to(source_dir).as_posix()
        take(path, f"{PROMPTS_ROOT}disciplines/{profile_dir_name}/{rel}")

    required = [
        f"{PROMPTS_ROOT}disciplines/{profile_dir_name}/{name}"
        for name in REQUIRED_PROFILE_FILES
    ]
    missing = [name for name in required if name not in files]
    if missing:
        raise DisciplineProfileSnapshotError(
            f"В профиле {discipline.code!r} нет обязательных файлов: "
            + ", ".join(missing)
        )

    # Чек-листы дисциплины. Их отсутствие не блокирует прогон (в Stage 01 они
    # ещё не подключены — см. `checklist_loader`), но и молчать о нём нельзя:
    # факт пишется в манифест отдельным полем.
    checklists_present: list[str] = []
    for sub in CHECKLIST_DIRS:
        base = Path(app_data_dir) / sub
        for suffix in (".md", ".json"):
            candidate = base / f"{discipline.code}{suffix}"
            if candidate.is_file():
                take(candidate, f"{APP_DATA_ROOT}{sub}/{candidate.name}")
                checklists_present.append(f"{sub}/{candidate.name}")

    manifest = {
        "profile_manifest_version": PROFILE_MANIFEST_VERSION,
        "discipline_id": discipline.code,
        "profile_dir": profile_dir_name,
        "discipline_source": discipline.source,
        "display_name": discipline.display_name,
        "profile_version": _profile_version(files, profile_dir_name),
        "source_revision": str(source_revision or ""),
        "files": sorted(entries, key=lambda item: item["path"]),
        "required_files": required,
        "checklists": sorted(checklists_present),
        "tree_hash": tree_hash(files),
        "created_at": float(created_at if created_at is not None else time.time()),
    }
    return DisciplineProfileSnapshot(
        discipline_id=discipline.code,
        profile_dir=profile_dir_name,
        files=files,
        manifest=manifest,
    )


def _profile_version(files: dict[str, bytes], profile_dir_name: str) -> str:
    """Версия профиля из его `config.json`, иначе «1».

    Отдельно от `tree_hash`: хэш отвечает «то же ли это байтово», версия — «то
    же ли это по смыслу», и второе задаёт человек.
    """
    key = f"{PROMPTS_ROOT}disciplines/{profile_dir_name}/config.json"
    blob = files.get(key)
    if not blob:
        return "1"
    try:
        data = json.loads(blob.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return "1"
    if isinstance(data, dict) and data.get("profile_version"):
        return str(data["profile_version"])
    return "1"


# ─── Сторона воркера ─────────────────────────────────────────────────────────
def load_profile_manifest(profile_root: Path) -> dict[str, Any]:
    path = Path(profile_root) / "profile_manifest.json"
    if not path.is_file():
        raise DisciplineProfileSnapshotError(
            f"В пакете нет снимка профиля дисциплины: {path}"
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DisciplineProfileSnapshotError(
            f"profile_manifest.json нечитаем: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise DisciplineProfileSnapshotError("profile_manifest.json не объект JSON")
    if int(data.get("profile_manifest_version") or 0) != PROFILE_MANIFEST_VERSION:
        raise DisciplineProfileSnapshotError(
            "Версия profile_manifest не поддерживается: "
            f"{data.get('profile_manifest_version')!r}"
        )
    return data


def verify_profile_snapshot(
    profile_root: Path,
    *,
    expected_discipline: Optional[str] = None,
    expected_tree_hash: Optional[str] = None,
) -> dict[str, Any]:
    """Проверить распакованный снимок: состав, хэши каждого файла и дерева.

    Порядок «структура → файлы → дерево → ожидания задания» обязателен:
    сверять tree_hash первым значило бы принять как «совпало» набор, часть
    которого мы прочитать не смогли.
    """
    manifest = load_profile_manifest(profile_root)
    files_root = Path(profile_root) / FILES_PREFIX.rstrip("/")
    declared = manifest.get("files") or []
    if not isinstance(declared, list) or not declared:
        raise DisciplineProfileSnapshotError("В снимке профиля нет ни одного файла")

    blobs: dict[str, bytes] = {}
    for item in declared:
        if not isinstance(item, dict):
            raise DisciplineProfileSnapshotError("Битая запись в списке файлов")
        rel = _safe_relative(str(item.get("path") or ""))
        path = files_root / rel
        if not path.is_file() or path.is_symlink():
            raise DisciplineProfileSnapshotError(
                f"Файл профиля отсутствует в пакете: {rel}"
            )
        blob = path.read_bytes()
        expected = str(item.get("sha256") or "")
        if expected and _sha256(blob) != expected:
            raise DisciplineProfileSnapshotError(
                f"SHA-256 файла профиля не совпал: {rel}"
            )
        blobs[rel] = blob

    # На диске не должно быть НИЧЕГО сверх манифеста: лишний файл — это либо
    # подмена, либо остаток чужой попытки, и оба варианта меняют профиль.
    on_disk = {
        p.relative_to(files_root).as_posix()
        for p in files_root.rglob("*")
        if p.is_file()
    }
    extra = sorted(on_disk - set(blobs))
    if extra:
        raise DisciplineProfileSnapshotError(
            "В снимке профиля файлы вне манифеста: " + ", ".join(extra[:5])
        )

    actual_tree = tree_hash(blobs)
    if str(manifest.get("tree_hash") or "") != actual_tree:
        raise DisciplineProfileSnapshotError(
            f"tree_hash профиля не совпал: заявлен "
            f"{str(manifest.get('tree_hash'))[:23]}…, вычислен {actual_tree[:23]}…"
        )
    if expected_tree_hash and str(expected_tree_hash) != actual_tree:
        raise DisciplineProfileSnapshotError(
            f"Профиль в пакете не тот, что заявлен в задании: ожидался "
            f"{str(expected_tree_hash)[:23]}…, получен {actual_tree[:23]}…"
        )
    if expected_discipline and str(manifest.get("discipline_id")) != str(
        expected_discipline
    ):
        raise DisciplineProfileSnapshotError(
            f"Дисциплина снимка {manifest.get('discipline_id')!r} не совпадает с "
            f"заявленной в задании {expected_discipline!r}"
        )
    for name in manifest.get("required_files") or []:
        if str(name) not in blobs:
            raise DisciplineProfileSnapshotError(
                f"В снимке нет обязательного файла профиля: {name}"
            )
    return manifest


def materialize_profile(
    profile_root: Path,
    *,
    prompts_dir: Path,
    app_data_dir: Path,
    expected_discipline: Optional[str] = None,
    expected_tree_hash: Optional[str] = None,
) -> dict[str, Any]:
    """Разложить проверенный снимок туда, где конвейер читает профиль.

    Возвращает описание применённого — оно уезжает в манифест результата, и
    именно по нему центр видит, каким профилем шёл прогон.
    """
    manifest = verify_profile_snapshot(
        profile_root,
        expected_discipline=expected_discipline,
        expected_tree_hash=expected_tree_hash,
    )
    files_root = Path(profile_root) / FILES_PREFIX.rstrip("/")
    targets = {
        PROMPTS_ROOT: Path(prompts_dir),
        APP_DATA_ROOT: Path(app_data_dir),
    }
    applied: list[str] = []
    for item in manifest.get("files") or []:
        rel = _safe_relative(str(item.get("path") or ""))
        root = next(prefix for prefix in LOGICAL_ROOTS if rel.startswith(prefix))
        destination = targets[root] / rel[len(root):]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((files_root / rel).read_bytes())
        applied.append(str(destination))
    return {
        "discipline_id": manifest.get("discipline_id"),
        "profile_dir": manifest.get("profile_dir"),
        "profile_version": manifest.get("profile_version"),
        "discipline_profile_hash": manifest.get("tree_hash"),
        "files_applied": len(applied),
        "checklists": list(manifest.get("checklists") or []),
        "source_revision": manifest.get("source_revision"),
    }
