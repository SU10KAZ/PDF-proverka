"""Разрешение на РАБОЧИЙ вызов модели из конвейера — отдельно от probe.

Почему не переиспользуется `probe_grant` (§9 задания).

`probe_grant` разрешает контрольный запрос: фиксированную фразу без документов
и без задания. Его единица — «ещё один запрос этому провайдеру», и большего
ему знать не нужно. Разрешение на работу конвейера обязано быть у́же по трём
осям сразу, и ни одну из них `probe_grant` не выражает:

  * ЗАДАНИЕ. Разрешение выдаётся под конкретный `task_id`. Иначе «разрешил один
    прогон» означает «разрешил любой следующий прогон», а следующий может
    прийти с другим документом;
  * СРОК. У разрешения есть `expires_at`. Забытое разрешение без срока — это
    открытая дверь, о которой никто не помнит: файл лежит месяцами, и первый же
    случайный запуск тратит подписку;
  * ЧИСЛО. `max_uses` списывается АТОМАРНО под межпроцессной блокировкой.
    `probe_grant` переписывает файл без блокировки — для ручной команды
    оператора этого хватало, но здесь списывает автомат, и два исполнителя на
    одной машине обязаны получить разные ответы.

Формат — JSON, а не строки `<провайдер>=<число>`: у записи пять полей, и
плоский формат превратился бы в позиционный, где порядок значений — контракт.

    {
      "schema_version": 1,
      "grants": [
        {
          "grant_id":   "g-11c-claude-0001",
          "provider":   "claude",
          "task_id":    "job-...",
          "max_uses":   1,
          "used":       0,
          "expires_at": 1786000000.0,
          "note":       "этап 11C, synthetic pipeline inference"
        }
      ]
    }

Файл создаёт ЧЕЛОВЕК с доступом к машине (`0600`, владелец — пользователь
воркера). Воркер его только читает и списывает: разрешение, которое воркер
выписывает себе сам, разрешением не является.

Порядок такой же, как у `probe_grant`, и по той же причине: сначала запись на
диск, потом вызов модели. Аварийное завершение работает в безопасную сторону —
попытка засчитана.
"""
from __future__ import annotations

import fcntl
import json
import os
import stat
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

#: Имя файла внутри `<worker_root>/config`. Намеренно НЕ совпадает с
#: `allow_real_provider_probe`: два разных разрешения в одном файле означали бы,
#: что оператор, выдавая одно, случайно продлевает второе.
GRANT_FILENAME = "allow_synthetic_inference"

#: Права, шире которых файл быть не должен.
MAX_GRANT_MODE = 0o600

#: Версия схемы файла. Неизвестная версия — отказ, а не «разберём как выйдет».
SCHEMA_VERSION = 1


class InferenceGrantError(RuntimeError):
    """Разрешения нет, оно повреждено, просрочено или исчерпано."""


@dataclass(frozen=True)
class GrantRecord:
    """Одна запись разрешения."""

    grant_id: str
    provider: str
    task_id: str
    max_uses: int
    used: int
    expires_at: float
    note: str = ""

    @property
    def remaining(self) -> int:
        return max(0, int(self.max_uses) - int(self.used))

    def expired(self, *, now: Optional[float] = None) -> bool:
        return float(self.expires_at) <= (now if now is not None else time.time())

    def as_dict(self) -> dict[str, Any]:
        return {
            "grant_id": self.grant_id,
            "provider": self.provider,
            "task_id": self.task_id,
            "max_uses": int(self.max_uses),
            "used": int(self.used),
            "expires_at": float(self.expires_at),
            "note": self.note,
        }

    def as_public_dict(self, *, now: Optional[float] = None) -> dict[str, Any]:
        """Вид ДЛЯ ЦЕНТРА: без `task_id` и без заметки оператора."""
        return {
            "grant_id": self.grant_id,
            "provider": self.provider,
            "remaining": self.remaining,
            "expired": self.expired(now=now),
        }


def grant_path(worker_root: Path) -> Path:
    return Path(worker_root) / "config" / GRANT_FILENAME


def _lock_path(worker_root: Path) -> Path:
    return grant_path(worker_root).with_suffix(".lock")


def _check_file_safety(path: Path) -> Optional[str]:
    """Обычный файл, свой, узкий по правам. Ссылка отвергается отдельно."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        return f"файл разрешения недоступен: {exc}"
    if stat.S_ISLNK(info.st_mode):
        return "файл разрешения — символьная ссылка; ожидается обычный файл"
    if not stat.S_ISREG(info.st_mode):
        return "файл разрешения не является обычным файлом"
    if info.st_uid != os.getuid():
        return (
            f"файл разрешения принадлежит uid={info.st_uid}, "
            f"а воркер работает под uid={os.getuid()}"
        )
    extra = stat.S_IMODE(info.st_mode) & ~MAX_GRANT_MODE
    if extra:
        return (
            f"права файла разрешения {stat.S_IMODE(info.st_mode):04o} шире "
            f"допустимых {MAX_GRANT_MODE:04o}"
        )
    return None


def _parse(text: str) -> list[GrantRecord]:
    """Разбор файла. Мусор — ошибка, а не «разрешений нет»."""
    try:
        data = json.loads(text or "{}")
    except ValueError as exc:
        raise InferenceGrantError(f"файл разрешения не является JSON: {exc}") from None
    if not isinstance(data, dict):
        raise InferenceGrantError("файл разрешения: ожидается объект верхнего уровня")
    version = data.get("schema_version")
    if version != SCHEMA_VERSION:
        raise InferenceGrantError(
            f"schema_version={version!r}, поддерживается {SCHEMA_VERSION}"
        )
    raw = data.get("grants")
    if not isinstance(raw, list):
        raise InferenceGrantError("файл разрешения: поле grants должно быть списком")
    records: list[GrantRecord] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise InferenceGrantError(f"grants[{index}] не является объектом")
        try:
            record = GrantRecord(
                grant_id=str(item["grant_id"]),
                provider=str(item["provider"]).strip().lower(),
                task_id=str(item["task_id"]),
                max_uses=int(item["max_uses"]),
                used=int(item.get("used", 0)),
                expires_at=float(item["expires_at"]),
                note=str(item.get("note") or ""),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InferenceGrantError(f"grants[{index}]: {exc}") from None
        if record.max_uses < 0 or record.used < 0:
            raise InferenceGrantError(f"grants[{index}]: отрицательные значения")
        records.append(record)
    return records


def _serialize(records: list[GrantRecord]) -> str:
    return json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "_comment": (
                "Разрешение воркера на РАБОЧИЙ вызов модели из конвейера. "
                "Файл создаёт человек с доступом к машине; воркер только "
                "списывает."
            ),
            "grants": [record.as_dict() for record in records],
        },
        ensure_ascii=False,
        indent=2,
    ) + "\n"


def _write_atomically(path: Path, records: list[GrantRecord]) -> None:
    """Переписать файл целиком через временный + `rename` (см. probe_grant)."""
    handle, temp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".sgrant.")
    temp_path = Path(temp_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(_serialize(records))
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp_path, MAX_GRANT_MODE)
        os.replace(temp_path, path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise


class _FileLock:
    """Межпроцессная блокировка на время «прочитал → изменил → записал».

    `probe_grant` обходится без неё: там списывает человек одной командой. Здесь
    списывает исполнитель, а исполнителей на машине может оказаться два (штатный
    и запущенный руками) — и без блокировки оба прочитали бы `used=0`.
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._fd: Optional[int] = None

    def __enter__(self) -> "_FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *_exc: Any) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


def read_records(worker_root: Path) -> list[GrantRecord]:
    """Прочитать все записи, ничего не расходуя."""
    path = grant_path(worker_root)
    problem = _check_file_safety(path)
    if problem:
        raise InferenceGrantError(problem)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise InferenceGrantError(f"файл разрешения не читается: {exc}") from None
    return _parse(text)


def describe(worker_root: Path, *, now: Optional[float] = None) -> dict[str, Any]:
    """Безопасная сводка для отчёта и heartbeat. Никогда не бросает."""
    try:
        records = read_records(worker_root)
    except InferenceGrantError as exc:
        return {"grant_file_present": grant_path(worker_root).exists(),
                "error": str(exc), "grants": [], "remaining_total": 0}
    moment = now if now is not None else time.time()
    usable = [r for r in records if r.remaining > 0 and not r.expired(now=moment)]
    return {
        "grant_file_present": grant_path(worker_root).exists(),
        "error": None,
        "grants": [record.as_public_dict(now=moment) for record in records],
        "remaining_total": sum(record.remaining for record in usable),
    }


def find(
    worker_root: Path, *, provider: str, task_id: str, now: Optional[float] = None
) -> Optional[GrantRecord]:
    """Найти пригодную запись под пару (провайдер, задание). Ничего не тратит."""
    moment = now if now is not None else time.time()
    name = str(provider).strip().lower()
    for record in read_records(worker_root):
        if record.provider != name or record.task_id != str(task_id):
            continue
        if record.remaining <= 0 or record.expired(now=moment):
            continue
        return record
    return None


def consume(
    worker_root: Path, *, provider: str, task_id: str, now: Optional[float] = None
) -> GrantRecord:
    """Списать одну попытку под (провайдер, задание). Возвращает запись ПОСЛЕ.

    Все проверки и запись — под одной блокировкой: между «нашли пригодную» и
    «списали» не должно быть окна, в котором вторая попытка увидит ту же
    единицу.
    """
    path = grant_path(worker_root)
    name = str(provider).strip().lower()
    moment = now if now is not None else time.time()
    with _FileLock(_lock_path(worker_root)):
        problem = _check_file_safety(path)
        if problem:
            raise InferenceGrantError(problem)
        if not path.exists():
            raise InferenceGrantError(
                f"нет разрешения воркера на рабочий вызов модели: файл {path} "
                "отсутствует. Его создаёт человек с доступом к машине; воркер "
                "разрешения себе не выписывает."
            )
        records = _parse(path.read_text(encoding="utf-8"))
        index = next(
            (
                i for i, record in enumerate(records)
                if record.provider == name and record.task_id == str(task_id)
            ),
            None,
        )
        if index is None:
            raise InferenceGrantError(
                f"разрешения под провайдера {name!r} и задание {task_id!r} нет. "
                "Разрешение привязано к заданию намеренно: иначе один выданный "
                "прогон разрешал бы любой следующий."
            )
        record = records[index]
        if record.expired(now=moment):
            raise InferenceGrantError(
                f"разрешение {record.grant_id!r} просрочено "
                f"(истекло {int(moment - record.expires_at)} с назад)"
            )
        if record.remaining <= 0:
            raise InferenceGrantError(
                f"разрешение {record.grant_id!r} исчерпано "
                f"({record.used}/{record.max_uses})"
            )
        updated = GrantRecord(
            grant_id=record.grant_id,
            provider=record.provider,
            task_id=record.task_id,
            max_uses=record.max_uses,
            used=record.used + 1,
            expires_at=record.expires_at,
            note=record.note,
        )
        records[index] = updated
        _write_atomically(path, records)
        return updated


def issue(
    worker_root: Path,
    *,
    grant_id: str,
    provider: str,
    task_id: str,
    ttl_sec: float,
    max_uses: int = 1,
    note: str = "",
    now: Optional[float] = None,
) -> GrantRecord:
    """Выписать разрешение. ТОЛЬКО для оператора и тестов, не для воркера.

    Функция живёт здесь, а не в скрипте, ради одного: формат файла имеет ровно
    одного автора. Вызывать её из кода воркера нельзя — это проверяется тестом
    (`test_worker_runtime_never_issues_its_own_grant`).
    """
    moment = now if now is not None else time.time()
    path = grant_path(worker_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FileLock(_lock_path(worker_root)):
        records = _parse(path.read_text(encoding="utf-8")) if path.exists() else []
        record = GrantRecord(
            grant_id=str(grant_id),
            provider=str(provider).strip().lower(),
            task_id=str(task_id),
            max_uses=int(max_uses),
            used=0,
            expires_at=moment + float(ttl_sec),
            note=str(note or ""),
        )
        records = [r for r in records if r.grant_id != record.grant_id]
        records.append(record)
        _write_atomically(path, records)
        return record
