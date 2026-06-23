"""
storage_write_facade.py — подготовительный фасад ЗАПИСИ данных проекта в
`projects_v2` (Step 8/10 «prepare write/upload cutover»).

⚠️ ВАЖНО: на этом этапе фасад **НЕ подключён** ни к одному production-endpoint'у
и по умолчанию **ничего не пишет в projects_v2**. Это скелет + 1-2 безопасных
метода записи, чтобы подготовить (но не включить) write-cutover. Production-режим
по умолчанию `legacy` — legacy `projects/` остаётся единственным авторитетным
хранилищем записи.

------------------------------------------------------------------------------
РЕЖИМЫ ЗАПИСИ (env `AUDIT_PROJECTS_V2_WRITE_MODE`, default `legacy`)
------------------------------------------------------------------------------

* `legacy` (default, production):
    фасад — прозрачный no-op для v2. Авторитетна ТОЛЬКО legacy-запись (callable
    от вызывающего кода). В projects_v2 ничего не пишется. Поведение системы
    идентично сборке без фасада.

* `dual_write_shadow` (контролируемый rollout, НЕ на проде):
    1) сначала выполняется legacy-запись (АВТОРИТЕТНА, исключение пробрасывается);
    2) ТОЛЬКО ПОСЛЕ успешной legacy-записи фасад зеркалит данные в projects_v2;
    3) ошибка v2-записи логируется и НЕ ломает legacy (fail-soft). Никакой
       «тихой» потери данных: legacy всегда записан, v2 — best-effort тень.

* `projects_v2_primary` (будущее, НЕ на проде):
    1) сначала v2-запись (primary, исключение пробрасывается);
    2) затем legacy как архив (fail-soft, ошибка логируется).
    Симметрично shadow, меняется только порядок и какая сторона авторитетна.

ИНВАРИАНТЫ:
  * production default = `legacy`;
  * в `dual_write_shadow` v2-запись происходит ТОЛЬКО после успешной legacy;
  * сбой v2-записи в shadow НЕ ломает legacy (логируется);
  * деструктивные операции в projects_v2 ЗАПРЕЩЕНЫ на этом этапе
    (`DestructiveWriteBlocked`), независимо от режима;
  * никакой silent data loss: фасад либо пишет, либо явно сообщает об ошибке.

Раскладка projects_v2 (см. docs/projects_v2_storage_standard.md):
  objects/<folder>/disciplines/<disc>/documents/<code>/document.json
  objects/<folder>/disciplines/<disc>/documents/<code>/current_version.txt
  .../versions/<vid>/version.json
  .../versions/<vid>/01_input/                  (исходные PDF/MD)
  .../versions/<vid>/03_analysis/latest/        (01/02/03 .json)
  .../versions/<vid>/03_analysis/runs/<run>/    (исторические прогоны)
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# режимы записи
# ---------------------------------------------------------------------------

WRITE_MODE_LEGACY = "legacy"
WRITE_MODE_DUAL_SHADOW = "dual_write_shadow"
WRITE_MODE_V2_PRIMARY = "projects_v2_primary"

_VALID_WRITE_MODES = {WRITE_MODE_LEGACY, WRITE_MODE_DUAL_SHADOW, WRITE_MODE_V2_PRIMARY}

_WRITE_MODE_ENV = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2_DIR_ENV = "AUDIT_PROJECTS_V2_DIR"


def get_write_mode() -> str:
    """Текущий режим записи. Default `legacy`. Читается из env на КАЖДЫЙ вызов.

    Любое неизвестное / пустое значение → `legacy` (fail-safe: непонятный конфиг
    НИКОГДА не включает запись в v2).
    """
    val = (os.environ.get(_WRITE_MODE_ENV) or "").strip().lower()
    return val if val in _VALID_WRITE_MODES else WRITE_MODE_LEGACY


def v2_writes_enabled() -> bool:
    """True только если режим явно разрешает писать в projects_v2."""
    return get_write_mode() in (WRITE_MODE_DUAL_SHADOW, WRITE_MODE_V2_PRIMARY)


def v2_is_primary() -> bool:
    return get_write_mode() == WRITE_MODE_V2_PRIMARY


# ---------------------------------------------------------------------------
# исключения
# ---------------------------------------------------------------------------

class StorageWriteError(Exception):
    """Базовая ошибка записи фасада."""


class DestructiveWriteBlocked(StorageWriteError):
    """Деструктивная операция в projects_v2 запрещена на этом этапе."""


# ---------------------------------------------------------------------------
# целевой адрес документа/версии в projects_v2
# ---------------------------------------------------------------------------

_VID_RE = re.compile(r"^v0*(\d+)$", re.IGNORECASE)


def normalize_vid_for_disk(version_id: str) -> str:
    """`v1`/`V1`/`v001` → `v001` (формат каталога версии на диске).

    Любой нераспознанный формат возвращается как есть (с lower), чтобы фасад не
    «угадывал» и не создавал кривые каталоги молча.
    """
    s = (version_id or "").strip()
    m = _VID_RE.match(s)
    if m:
        return f"v{int(m.group(1)):03d}"
    return s.lower()


def _version_no_for_disk_id(version_id: str) -> int:
    m = _VID_RE.match((version_id or "").strip())
    return int(m.group(1)) if m else 1


@dataclass(frozen=True)
class V2Target:
    """Куда писать в projects_v2 (нормализованный адрес одной версии документа)."""

    object_folder: str
    discipline: str
    document_code: str
    version_id: str  # любой ввод; на диск пойдёт normalize_vid_for_disk()

    def vid_disk(self) -> str:
        return normalize_vid_for_disk(self.version_id)

    def doc_dir(self, v2_root: Path) -> Path:
        return (Path(v2_root) / "objects" / self.object_folder / "disciplines"
                / self.discipline / "documents" / self.document_code)

    def version_dir(self, v2_root: Path) -> Path:
        return self.doc_dir(v2_root) / "versions" / self.vid_disk()


# ---------------------------------------------------------------------------
# результат одной операции записи (для диагностики / симуляции / тестов)
# ---------------------------------------------------------------------------

@dataclass
class WriteResult:
    op: str
    mode: str
    legacy_ok: Optional[bool] = None      # None = legacy-writer не передан
    legacy_authoritative: bool = True
    v2_ok: Optional[bool] = None          # None = v2-запись не выполнялась (legacy mode)
    v2_attempted: bool = False
    v2_paths: list[str] = field(default_factory=list)
    v2_error: Optional[str] = None
    legacy_result: Any = None

    def to_dict(self) -> dict:
        return {
            "op": self.op,
            "mode": self.mode,
            "legacy_ok": self.legacy_ok,
            "legacy_authoritative": self.legacy_authoritative,
            "v2_ok": self.v2_ok,
            "v2_attempted": self.v2_attempted,
            "v2_paths": list(self.v2_paths),
            "v2_error": self.v2_error,
        }


# ---------------------------------------------------------------------------
# атомарная запись (write tmp → os.replace), как в остальном коде проекта
# ---------------------------------------------------------------------------

def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".wtmp_")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _atomic_write_json(path: Path, payload: Any) -> None:
    _atomic_write_bytes(path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))


def _as_bytes(payload: Union[dict, list, str, bytes]) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


# ---------------------------------------------------------------------------
# фасад
# ---------------------------------------------------------------------------

class StorageWriteFacade:
    """Подготовительный фасад записи. По умолчанию (`legacy`) НИЧЕГО не пишет в v2.

    На этом этапе НЕ подключён к production endpoint'ам. Используется тестами и
    `scripts/projects_v2/simulate_write_cutover.py` (dry-run на temp-фикстурах).
    """

    def __init__(self, v2_root: Optional[Union[str, Path]] = None):
        self._v2_root_override = Path(v2_root).resolve() if v2_root else None

    # -- v2 root ----------------------------------------------------------
    def v2_root(self) -> Optional[Path]:
        """Корень projects_v2. Override (тесты/симуляция) > env > config.DATA_DIR."""
        if self._v2_root_override is not None:
            return self._v2_root_override
        env = os.environ.get(_V2_DIR_ENV)
        if env:
            return Path(env).resolve()
        try:
            from backend.app.core.config import DATA_DIR
            return Path(DATA_DIR) / "projects_v2"
        except Exception:
            return None

    # -- деструктив (только через явный backup+confirmation contract) -------
    def block_destructive(self, op: str) -> None:
        """Блокировать destructive-op без контекста backup+confirmation.

        Низкоуровневый фасад не знает target/backup_id, поэтому direct-вызовы
        clean/rename/delete остаются запрещены. Разрешающий путь живёт выше, в
        v2_primary_wiring/project_service: сначала copytree backup версии, затем
        append-only confirmation log, затем конкретная операция.
        """
        raise DestructiveWriteBlocked(
            f"destructive v2 op '{op}' blocked "
            f"(missing backup/confirmation context)"
        )

    # -- ядро: диспетчер режимов -----------------------------------------
    def _execute(
        self,
        op: str,
        *,
        legacy_write: Optional[Callable[[], Any]],
        v2_write: Callable[[], list[Path]],
    ) -> WriteResult:
        """Выполнить запись согласно режиму.

        legacy_write — callable существующего legacy-кода (может быть None в
        тестах/симуляции). v2_write — callable, возвращающий список записанных
        v2-путей (вызывается только когда v2 разрешена режимом).
        """
        mode = get_write_mode()
        res = WriteResult(op=op, mode=mode)

        if mode == WRITE_MODE_LEGACY:
            # production default: только legacy, v2 не трогаем.
            if legacy_write is not None:
                res.legacy_result = legacy_write()
                res.legacy_ok = True
            res.legacy_authoritative = True
            res.v2_ok = None
            return res

        if mode == WRITE_MODE_DUAL_SHADOW:
            # 1) legacy авторитетна и идёт ПЕРВОЙ (исключение пробрасывается).
            if legacy_write is not None:
                res.legacy_result = legacy_write()
                res.legacy_ok = True
            res.legacy_authoritative = True
            # 2) v2 — тень, ТОЛЬКО после успешной legacy, fail-soft.
            res.v2_attempted = True
            try:
                paths = v2_write() or []
                res.v2_paths = [str(p) for p in paths]
                res.v2_ok = True
            except Exception as exc:  # noqa: BLE001 — намеренный fail-soft
                res.v2_ok = False
                res.v2_error = f"{type(exc).__name__}: {exc}"
                logger.warning("[storage_write_facade] shadow v2 write failed op=%s: %s",
                               op, res.v2_error)
            return res

        if mode == WRITE_MODE_V2_PRIMARY:
            # 1) v2 primary (исключение пробрасывается).
            res.v2_attempted = True
            paths = v2_write() or []
            res.v2_paths = [str(p) for p in paths]
            res.v2_ok = True
            res.legacy_authoritative = False
            # 2) legacy как архив, fail-soft.
            if legacy_write is not None:
                try:
                    res.legacy_result = legacy_write()
                    res.legacy_ok = True
                except Exception as exc:  # noqa: BLE001
                    res.legacy_ok = False
                    logger.warning("[storage_write_facade] v2_primary legacy archive failed op=%s: %s",
                                   op, exc)
            return res

        # недостижимо (get_write_mode нормализует), но fail-safe → legacy
        if legacy_write is not None:
            res.legacy_result = legacy_write()
            res.legacy_ok = True
        return res

    # -- v2-writers (вызываются только при разрешённой записи) ------------
    def _ensure_document_scaffold(self, v2_root: Path, target: V2Target) -> list[Path]:
        """Создать минимальный каркас документа/версии в v2 (idempotent).

        Новый v2-документ должен быть виден read-адаптеру сразу после первой
        записи. Поэтому, кроме каталогов, поддерживаем `document.json` с
        `versions`/`version_ids` и `current_version.txt`. Старые минимальные
        scaffold-файлы дозаполняются без потери посторонних полей.
        """
        written: list[Path] = []
        doc_dir = target.doc_dir(v2_root)
        vid = target.vid_disk()
        version_no = _version_no_for_disk_id(vid)
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        version_dir = target.version_dir(v2_root)
        for subdir in (
            version_dir / "01_input",
            version_dir / "02_work",
            version_dir / "03_analysis" / "latest",
            version_dir / "04_review",
            version_dir / "05_export",
        ):
            subdir.mkdir(parents=True, exist_ok=True)

        doc_json = doc_dir / "document.json"
        if doc_json.exists():
            try:
                payload = json.loads(doc_json.read_text(encoding="utf-8"))
                if not isinstance(payload, dict):
                    payload = {}
            except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                payload = {}
        else:
            payload = {}

        changed = not doc_json.exists()
        defaults = {
            "schema_version": 1,
            "document_code": target.document_code,
            "object_folder": target.object_folder,
            "discipline": target.discipline,
        }
        for key, value in defaults.items():
            if not payload.get(key):
                payload[key] = value
                changed = True

        version_entry = {
            "version_id": vid,
            "version_no": version_no,
            "label": f"V{version_no}",
            "status": "source_only",
            "source": "upload",
            "created_at": now,
        }
        versions = payload.get("versions")
        if not isinstance(versions, list):
            versions = []
            changed = True
        if not any(isinstance(v, dict) and v.get("version_id") == vid for v in versions):
            versions.append(version_entry)
            versions.sort(key=lambda v: _version_no_for_disk_id(str(v.get("version_id", "v001"))) if isinstance(v, dict) else 0)
            changed = True
        payload["versions"] = versions

        version_ids = payload.get("version_ids")
        if not isinstance(version_ids, list):
            version_ids = []
            changed = True
        if vid not in version_ids:
            version_ids.append(vid)
            version_ids.sort(key=_version_no_for_disk_id)
            changed = True
        payload["version_ids"] = version_ids

        if not payload.get("current_version"):
            payload["current_version"] = vid
            changed = True

        if changed:
            _atomic_write_json(doc_json, payload)
            written.append(doc_json)

        cur = doc_dir / "current_version.txt"
        if not cur.exists():
            _atomic_write_bytes(cur, vid.encode("utf-8"))
            written.append(cur)
        return written

    # -- безопасный метод #1: метаданные версии --------------------------
    def save_version_metadata(
        self,
        target: V2Target,
        version_json: dict,
        *,
        legacy_write: Optional[Callable[[], Any]] = None,
    ) -> WriteResult:
        """Записать version.json версии в projects_v2 (+ каркас документа).

        В legacy-режиме v2 не трогается; авторитетна legacy_write.
        """
        def _v2() -> list[Path]:
            root = self.v2_root()
            if root is None:
                raise StorageWriteError("projects_v2 root not resolvable")
            written = self._ensure_document_scaffold(root, target)
            vj = dict(version_json)
            vj.setdefault("version_id", target.vid_disk())
            vpath = target.version_dir(root) / "version.json"
            _atomic_write_json(vpath, vj)
            written.append(vpath)
            return written

        return self._execute("save_version_metadata", legacy_write=legacy_write, v2_write=_v2)

    # -- безопасный метод #2: входной бандл (PDF/MD) ---------------------
    def save_input_bundle(
        self,
        target: V2Target,
        files: list[tuple[str, bytes]],
        *,
        legacy_write: Optional[Callable[[], Any]] = None,
    ) -> WriteResult:
        """Записать исходные файлы версии в `versions/<vid>/01_input/`.

        files — список (filename, bytes). Имена файлов санируются до basename,
        чтобы исключить выход за пределы 01_input.
        """
        def _v2() -> list[Path]:
            root = self.v2_root()
            if root is None:
                raise StorageWriteError("projects_v2 root not resolvable")
            self._ensure_document_scaffold(root, target)
            inp = target.version_dir(root) / "01_input"
            written: list[Path] = []
            for name, data in files:
                safe = os.path.basename((name or "").strip())
                if not safe or safe in (".", ".."):
                    raise StorageWriteError(f"unsafe input filename: {name!r}")
                dst = inp / safe
                _atomic_write_bytes(dst, data if isinstance(data, bytes) else _as_bytes(data))
                written.append(dst)
            return written

        return self._execute("save_input_bundle", legacy_write=legacy_write, v2_write=_v2)

    # -- безопасный метод #3: analysis-артефакт --------------------------
    def save_analysis_artifact(
        self,
        target: V2Target,
        artifact_name: str,
        payload: Union[dict, list, str, bytes],
        *,
        run_id: Optional[str] = None,
        legacy_write: Optional[Callable[[], Any]] = None,
    ) -> WriteResult:
        """Записать analysis-артефакт (03_findings.json, 02_blocks_analysis.json, …).

        Пишется в `03_analysis/latest/<artifact_name>`. Если задан run_id —
        дополнительно в `03_analysis/runs/<run_id>/<artifact_name>` (история).
        """
        safe = os.path.basename((artifact_name or "").strip())
        if not safe or safe in (".", ".."):
            raise StorageWriteError(f"unsafe artifact name: {artifact_name!r}")

        def _v2() -> list[Path]:
            root = self.v2_root()
            if root is None:
                raise StorageWriteError("projects_v2 root not resolvable")
            self._ensure_document_scaffold(root, target)
            vdir = target.version_dir(root)
            data = _as_bytes(payload)
            written: list[Path] = []
            latest = vdir / "03_analysis" / "latest" / safe
            _atomic_write_bytes(latest, data)
            written.append(latest)
            if run_id:
                rsafe = os.path.basename(run_id.strip())
                if rsafe and rsafe not in (".", ".."):
                    rpath = vdir / "03_analysis" / "runs" / rsafe / safe
                    _atomic_write_bytes(rpath, data)
                    written.append(rpath)
            return written

        return self._execute("save_analysis_artifact", legacy_write=legacy_write, v2_write=_v2)


    # -- production mirror: целый проект через проверенную миграцию ----------
    def _load_v2lib(self):
        """Lazy-import scripts/projects_v2/v2lib.py (только при разрешённой v2-записи).

        В legacy-режиме НИКОГДА не вызывается, поэтому prod-путь полностью
        развязан от scripts/. Импорт кэшируется на инстанс.
        """
        cached = getattr(self, "_v2lib", None)
        if cached is not None:
            return cached
        import importlib.util
        # storage_write_facade.py → parents[4] = корень репозитория (код)
        repo_root = Path(__file__).resolve().parents[4]
        v2lib_path = repo_root / "scripts" / "projects_v2" / "v2lib.py"
        if not v2lib_path.is_file():
            raise StorageWriteError(f"v2lib not found at {v2lib_path}")
        spec = importlib.util.spec_from_file_location("projects_v2_v2lib", v2lib_path)
        mod = importlib.util.module_from_spec(spec)
        import sys as _sys
        _sys.modules.setdefault("projects_v2_v2lib", mod)
        spec.loader.exec_module(mod)
        self._v2lib = mod
        return mod

    def _project_root_entry(self, legacy_project_dir: Path, v2lib) -> Path:
        """Нормализовать путь к ВЕРХНЕУРОВНЕВОЙ записи проекта (контейнер `(main)`
        или plain-проект) — именно её ожидает migrate_project."""
        p = Path(legacy_project_dir).resolve()
        try:
            from backend.app.services.common.version_service import container_dir_for
            c = container_dir_for(p)
            if c is not None:
                return Path(c).resolve()
        except Exception:
            pass
        # ручной фолбэк (без version_service): родитель — контейнер (main)?
        parent = p.parent
        if parent.name.endswith("(main)") and (parent / "version_group.json").exists():
            return parent
        return p

    def shadow_mirror_project(self, legacy_project_dir, *, run_id: Optional[str] = None) -> WriteResult:
        """Зеркалировать ВЕСЬ legacy-проект в projects_v2 через проверенную
        миграцию (parity-faithful, идемпотентно, обновляет old_to_new_map).

        В режиме legacy (default) — ничего не делает (no-op для v2).
        В shadow — legacy уже записан вызывающим (legacy-first); сбой v2 fail-soft.
        """
        legacy_project_dir = Path(legacy_project_dir)

        def _v2() -> list[Path]:
            v2root = self.v2_root()
            if v2root is None:
                raise StorageWriteError("projects_v2 root not resolvable")
            v2lib = self._load_v2lib()
            root_entry = self._project_root_entry(legacy_project_dir, v2lib)
            # objects.json лежит в <DATA>/backend/app/data; <DATA> = parent от projects_v2
            objects_map = v2lib.load_objects_map(root=v2root.parent)
            rec = v2lib.migrate_project(root_entry, v2root, objects_map=objects_map, run_id=run_id)
            map_path = v2root / "_system" / "old_to_new_map.json"
            mp = v2lib.load_old_to_new_map(map_path)
            for vrec in rec["versions"]:
                v2lib.upsert_migration(mp, {
                    "object_id": rec["object_id"],
                    "object_name": rec["object_name"],
                    "discipline": rec["discipline"],
                    "document_code": rec["document_code"],
                    "kind": rec["kind"],
                    "version_id": vrec["version_id"],
                    "version_no": vrec["version_no"],
                    "legacy_folder_name": vrec["legacy_folder_name"],
                    "legacy_folder_path": vrec["legacy_folder_path"],
                    "analysis_run_id": vrec["analysis_run_id"],
                    "v2_document_dir": rec["v2_document_dir"],
                    "files": vrec["files"],
                })
            v2lib.save_old_to_new_map(mp, map_path)
            return [Path(rec["v2_document_dir"])]

        return self._execute("shadow_mirror_project", legacy_write=None, v2_write=_v2)

    def shadow_mirror_project_by_id(self, project_id: str, *, run_id: Optional[str] = None) -> WriteResult:
        """Как shadow_mirror_project, но резолвит legacy-путь по project_id.

        Вызывается только когда v2-запись разрешена (через safe-обёртку), поэтому
        резолв здесь не нарушает legacy no-op."""
        from backend.app.services.common.project_service import resolve_project_dir
        d = resolve_project_dir(project_id, must_exist=True)
        return self.shadow_mirror_project(d, run_id=run_id)

    def remove_project_from_v2(self, legacy_root_entry, *, run_id: Optional[str] = None) -> WriteResult:
        """Удалить v2-документ(ы), относящиеся к legacy-проекту (контейнер или
        plain), и их записи из old_to_new_map.

        `legacy_root_entry` — путь к ВЕРХНЕУРОВНЕВОЙ записи проекта (контейнер
        `(main)` или plain-папка). Сопоставление с map идёт по `legacy_folder_path`
        строго по границе сегмента (==root или startswith root+os.sep), чтобы
        `…/X` не зацепил `…/X V1`. Несколько версий контейнера обычно делят один
        `v2_document_dir` — он удаляется один раз.

        В режиме legacy (default) — no-op (см. safe-обёртку). Идемпотентно:
        отсутствие совпадений = пустой результат.
        """
        legacy_root_entry = Path(legacy_root_entry)

        def _v2() -> list[Path]:
            v2root = self.v2_root()
            if v2root is None:
                raise StorageWriteError("projects_v2 root not resolvable")
            v2lib = self._load_v2lib()
            root = str(legacy_root_entry.resolve())
            map_path = v2root / "_system" / "old_to_new_map.json"
            mp = v2lib.load_old_to_new_map(map_path)
            migs = mp.get("migrations", [])

            def _match(e) -> bool:
                lp = e.get("legacy_folder_path") or ""
                try:
                    lp = str(Path(lp).resolve())
                except Exception:
                    lp = str(lp)
                return lp == root or lp.startswith(root + os.sep)

            matched = [e for e in migs if _match(e)]
            doc_dirs: list[str] = []
            for e in matched:
                dd = e.get("v2_document_dir")
                if dd and dd not in doc_dirs:
                    doc_dirs.append(dd)
            removed: list[Path] = []
            for dd in doc_dirs:
                p = Path(dd)
                # защита: удаляем только внутри objects/ этого v2root
                try:
                    inside = str(p.resolve()).startswith(str((v2root / "objects").resolve()) + os.sep)
                except Exception:
                    inside = False
                if inside and p.is_dir():
                    shutil.rmtree(p)
                    removed.append(p)
            mp["migrations"] = [e for e in migs if not _match(e)]
            v2lib.save_old_to_new_map(mp, map_path)
            return removed

        return self._execute("remove_project_from_v2", legacy_write=None, v2_write=_v2)


# ---------------------------------------------------------------------------
# module-level singleton helper (для будущего подключения к endpoint'ам)
# ---------------------------------------------------------------------------

_default_facade: Optional[StorageWriteFacade] = None


def get_write_facade() -> StorageWriteFacade:
    """Глобальный фасад (lazy). Production-режим читается из env на каждый вызов
    метода, поэтому singleton безопасен."""
    global _default_facade
    if _default_facade is None:
        _default_facade = StorageWriteFacade()
    return _default_facade


# ---------------------------------------------------------------------------
# safe-обёртки для подключения к production write-chokepoints
#
# ГЛАВНАЯ ГАРАНТИЯ: в режиме legacy (default) обе функции выходят НЕМЕДЛЕННО,
# не импортируя v2lib, не резолвя пути, не трогая projects_v2 — поведение
# chokepoint'а байт-в-байт прежнее. Любое исключение в v2-плече ловится и
# логируется (никогда не пробрасывается в legacy-путь вызывающего).
# ---------------------------------------------------------------------------

def _record_shadow_error(op: str, target: str, exc: BaseException) -> None:
    """Записать ошибку shadow-записи в отдельный JSONL-отчёт (best-effort)."""
    try:
        facade = get_write_facade()
        root = facade.v2_root()
        if root is None:
            return
        rep = root / "_system" / "dual_write_shadow_errors.jsonl"
        rep.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({
            "op": op, "target": str(target),
            "error": f"{type(exc).__name__}: {exc}",
        }, ensure_ascii=False)
        with open(rep, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def shadow_mirror_project_path_safe(legacy_project_dir, *, run_id: Optional[str] = None):
    """Хук после legacy-записи: зеркалит проект в v2 (no-op в legacy, fail-soft).

    Передаётся явный legacy-путь (для register_*). НИКОГДА не бросает наружу.
    """
    if not v2_writes_enabled():
        return None
    try:
        return get_write_facade().shadow_mirror_project(legacy_project_dir, run_id=run_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[storage_write_facade] shadow mirror (path) failed: %s", exc)
        _record_shadow_error("shadow_mirror_project_path", legacy_project_dir, exc)
        return None


def shadow_mirror_project_id_safe(project_id: str, *, run_id: Optional[str] = None):
    """Хук после legacy-записи: зеркалит проект по project_id (no-op в legacy, fail-soft)."""
    if not v2_writes_enabled():
        return None
    try:
        return get_write_facade().shadow_mirror_project_by_id(project_id, run_id=run_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[storage_write_facade] shadow mirror (id=%s) failed: %s", project_id, exc)
        _record_shadow_error("shadow_mirror_project_id", project_id, exc)
        return None


def remove_project_from_v2_safe(legacy_root_entry, *, run_id: Optional[str] = None):
    """Хук удаления: убрать v2-документ(ы) проекта (no-op в legacy, fail-soft).

    Вызывается из delete_project и merge_project_as_version (удаление source).
    В режиме legacy (default) — немедленный no-op (v2 не трогаем). Любая ошибка
    логируется и не пробрасывается — удаление legacy остаётся авторитетным.
    """
    if not v2_writes_enabled():
        return None
    try:
        return get_write_facade().remove_project_from_v2(legacy_root_entry, run_id=run_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[storage_write_facade] remove from v2 (%s) failed: %s", legacy_root_entry, exc)
        _record_shadow_error("remove_project_from_v2", str(legacy_root_entry), exc)
        return None
