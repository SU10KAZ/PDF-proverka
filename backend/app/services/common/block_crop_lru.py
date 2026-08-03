"""LRU-кэш восстановленных кропов блоков с жёстким потолком размера.

Отличать от ``crop_cache.py``: тот кэширует ИСХОДНЫЕ вектор-PDF портала в
``01_input/crops/``. Здесь — готовые PNG, восстановленные после эвакуации
локальных кропов (см. ``block_crop_store.py``).

Ключевые решения
----------------
* **Корень вне деревьев проектов.** Один общий потолок можно удержать только
  над одним пулом; кэш внутри версии заново создал бы ту же проблему (12.2 ГБ
  кропов) и уезжал бы в ``destructive_backups``.
* **Ключ = sha256(realpath(blocks_dir) + имя файла).** Путь папки уже кодирует
  объект/дисциплину/документ/версию/прогон/профиль, поэтому ключ версие- и
  профиле-зависим по построению: ``blocks_gemma_300`` и ``blocks_stage02_100``
  никогда не столкнутся.
* **Вытеснение по mtime, а не по atime.** Корень смонтирован ``relatime``:
  atime переписывается лишь раз в сутки, то есть LRU выродился бы в FIFO.
  Поэтому ``get()`` сам делает ``os.utime`` — mtime и есть «последнее чтение».
* **Свежие записи не вытесняются** (``BLOCK_CROP_CACHE_MIN_AGE_S``): иначе
  агент, восстановивший 40 кропов, потеряет первые до того, как их прочитает
  ``codex_runner`` (который молча выбрасывает недоступные пути).
* Все ошибки ФС проглатываются: кэш — ускорение, а не источник истины.
"""

from __future__ import annotations

import fcntl
import hashlib
import logging
import os
import shutil
import threading
import time
import weakref
from pathlib import Path

from backend.app.core.config import (
    BLOCK_CROP_CACHE_DIR,
    BLOCK_CROP_CACHE_MAX_BYTES,
    BLOCK_CROP_CACHE_MAX_FILE_BYTES,
    BLOCK_CROP_CACHE_MIN_AGE_S,
    BLOCK_CROP_CACHE_MIN_FREE_BYTES,
    BLOCK_CROP_CACHE_SWEEP_EVERY,
)

logger = logging.getLogger(__name__)

LOW_WATERMARK = 0.9  # вытесняем до 90% потолка, чтобы не молотить на каждой вставке

_key_locks: "weakref.WeakValueDictionary[str, threading.Lock]" = weakref.WeakValueDictionary()
_key_locks_guard = threading.Lock()
_insert_counter = 0
_counter_guard = threading.Lock()


def cache_root() -> Path:
    return Path(BLOCK_CROP_CACHE_DIR)


def _lock_for(key: str) -> threading.Lock:
    with _key_locks_guard:
        lock = _key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _key_locks[key] = lock
        return lock


def cache_key(blocks_dir: Path | str, file_name: str) -> str:
    try:
        origin = os.path.realpath(str(blocks_dir))
    except OSError:
        origin = str(blocks_dir)
    raw = f"{origin}\n{file_name}".encode("utf-8", "surrogatepass")
    return hashlib.sha256(raw).hexdigest()[:32]


def _paths_for(key: str, suffix: str) -> tuple[Path, Path]:
    root = cache_root()
    shard = root / key[:2]
    return shard / f"{key}{suffix}", shard


def get(blocks_dir: Path | str, file_name: str) -> Path | None:
    """Вернуть путь к закэшированному файлу и обновить его «время доступа»."""
    key = cache_key(blocks_dir, file_name)
    suffix = Path(file_name).suffix or ".png"
    path, _shard = _paths_for(key, suffix)
    try:
        if not path.is_file() or path.stat().st_size <= 0:
            return None
        os.utime(path, None)  # mtime = последнее чтение (atime под relatime не годится)
        return path
    except OSError:
        return None


def _free_bytes() -> int:
    """Свободное место на ФС кэша.

    Корень кэша может ещё не существовать — поднимаемся к ближайшему
    существующему предку, иначе disk_usage бросит и мы решим, что места нет.
    """
    for candidate in [cache_root(), *cache_root().parents]:
        if candidate.exists():
            try:
                return shutil.disk_usage(str(candidate)).free
            except OSError:
                return 0
    return 0


def put(blocks_dir: Path | str, file_name: str, source: Path) -> Path | None:
    """Положить готовый файл в кэш. Возвращает путь в кэше либо None."""
    try:
        size = Path(source).stat().st_size
    except OSError:
        return None
    if size <= 0 or size > BLOCK_CROP_CACHE_MAX_FILE_BYTES:
        return None
    # Диск живёт на пределе: ниже пола свободного места не пишем вовсе.
    if _free_bytes() < BLOCK_CROP_CACHE_MIN_FREE_BYTES:
        logger.warning("block_crop_lru: пропуск записи, мало свободного места")
        return None

    key = cache_key(blocks_dir, file_name)
    suffix = Path(file_name).suffix or ".png"
    path, shard = _paths_for(key, suffix)
    with _lock_for(key):
        try:
            shard.mkdir(parents=True, exist_ok=True)
            tmp = shard / f".{key}{suffix}.{os.getpid()}.tmp"
            shutil.copyfile(source, tmp)
            os.replace(tmp, path)
        except OSError as exc:
            logger.warning("block_crop_lru: запись не удалась (%s): %s", file_name, exc)
            return None

    _maybe_sweep()
    return path


def _maybe_sweep() -> None:
    global _insert_counter
    with _counter_guard:
        _insert_counter += 1
        due = _insert_counter % max(1, BLOCK_CROP_CACHE_SWEEP_EVERY) == 0
    if due:
        sweep()


def _iter_entries() -> list[tuple[Path, float, int]]:
    root = cache_root()
    out: list[tuple[Path, float, int]] = []
    if not root.is_dir():
        return out
    for shard in root.iterdir():
        if not shard.is_dir():
            continue
        for f in shard.iterdir():
            if not f.is_file() or f.name.endswith(".tmp"):
                continue
            try:
                st = f.stat()
            except OSError:
                continue
            out.append((f, st.st_mtime, st.st_size))
    return out


def stats() -> dict:
    entries = _iter_entries()
    total = sum(size for _p, _m, size in entries)
    return {
        "entries": len(entries),
        "total_bytes": total,
        "max_bytes": BLOCK_CROP_CACHE_MAX_BYTES,
        "free_bytes": _free_bytes(),
        "root": str(cache_root()),
    }


def sweep() -> dict:
    """Вытеснить самые давно не читанные записи до нижней ватерлинии.

    Межпроцессная блокировка: параллельный проход (второй воркер, cron) не
    должен считать освобождённые байты дважды. Занятый lock — просто выходим.
    """
    root = cache_root()
    try:
        root.mkdir(parents=True, exist_ok=True)
        lock_path = root / ".sweep.lock"
        lock_fh = lock_path.open("a+")
    except OSError:
        return {"skipped": "no_root"}

    try:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return {"skipped": "locked"}

        now = time.time()
        entries = _iter_entries()
        total = sum(size for _p, _m, size in entries)
        target = int(BLOCK_CROP_CACHE_MAX_BYTES * LOW_WATERMARK)

        # Заодно подчищаем осиротевшие .tmp старше часа.
        for shard in root.iterdir() if root.is_dir() else []:
            if not shard.is_dir():
                continue
            for f in shard.glob("*.tmp"):
                try:
                    if now - f.stat().st_mtime > 3600:
                        f.unlink()
                except OSError:
                    pass

        if total <= BLOCK_CROP_CACHE_MAX_BYTES:
            return {"total_bytes": total, "freed_bytes": 0, "evicted": 0}

        entries.sort(key=lambda item: item[1])  # старые по времени чтения — первыми
        freed = 0
        evicted = 0
        blocked_fresh = 0
        for path, mtime, size in entries:
            if total - freed <= target:
                break
            if now - mtime < BLOCK_CROP_CACHE_MIN_AGE_S:
                blocked_fresh += 1
                continue
            try:
                path.unlink()
            except OSError:
                continue
            freed += size
            evicted += 1

        if total - freed > BLOCK_CROP_CACHE_MAX_BYTES:
            logger.warning(
                "block_crop_lru: не удалось уложиться в потолок (%d байт, свежих %d)",
                total - freed,
                blocked_fresh,
            )
        return {
            "total_bytes": total - freed,
            "freed_bytes": freed,
            "evicted": evicted,
            "blocked_fresh": blocked_fresh,
        }
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_fh.close()
