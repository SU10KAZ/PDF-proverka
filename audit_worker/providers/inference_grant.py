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

#: Сколько РАЗ одна попытка вправе войти в оплачиваемый канал (этап 11G).
#:
#: Это не число вызовов модели и не бюджет: сколько обращений допускает
#: задание, знает `binding.max_inferences`, и считает их журнал вызовов
#: (`inference_ledger`, инвариант I-P9 — повторный вход отдаёт СОХРАНЁННЫЙ
#: ответ, не оплачивая заново). Здесь считается другое: сколько раз исполнитель
#: может ВОЙТИ в этот канал по одной попытке.
#:
#: Единица была бы неверна: перезапуск исполнителя посреди попытки — штатное
#: событие (`reconciliation`), и после него привязка выписывается заново.
#: С `max_uses=1` такая попытка получала бы отказ «разрешение исчерпано» и
#: становилась невосстановимой, хотя ни одного лишнего вызова не произошло бы.
#: Три = один обычный вход плюс два технических восстановления.
GRANT_MAX_ENTRIES_PER_ATTEMPT = 3


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
        matching = [
            i for i, record in enumerate(records)
            if record.provider == name and record.task_id == str(task_id)
        ]
        if not matching:
            raise InferenceGrantError(
                f"разрешения под провайдера {name!r} и задание {task_id!r} нет. "
                "Разрешение привязано к заданию намеренно: иначе один выданный "
                "прогон разрешал бы любой следующий."
            )
        # ПРИГОДНАЯ запись, а не просто первая совпавшая. Пока разрешение
        # выписывал человек, запись под задание была ровно одна, и разница не
        # проявлялась. С автоматической выпиской (11G) под одним заданием
        # накапливаются записи РАЗНЫХ попыток, и «первая совпавшая» означало бы
        # вечный отказ «разрешение исчерпано» по записи прошлой попытки, при
        # живом разрешении текущей. Порядок просмотра — от последней к первой:
        # свежая запись относится к текущей попытке.
        index = next(
            (
                i for i in reversed(matching)
                if records[i].remaining > 0 and not records[i].expired(now=moment)
            ),
            None,
        )
        if index is None:
            latest = records[matching[-1]]
            if latest.expired(now=moment):
                raise InferenceGrantError(
                    f"разрешение {latest.grant_id!r} просрочено "
                    f"(истекло {int(moment - latest.expires_at)} с назад)"
                )
            raise InferenceGrantError(
                f"разрешение {latest.grant_id!r} исчерпано "
                f"({latest.used}/{latest.max_uses})"
            )
        record = records[index]
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


def issue_for_job(
    worker_root: Path,
    *,
    provider: str,
    job_id: str,
    attempt_id: str,
    capability: str,
    requested_max_inferences: int,
    machine_ceiling: int,
    ttl_sec: float,
    now: Optional[float] = None,
) -> GrantRecord:
    """Разрешение, выписанное ШТАТНЫМ КОДОМ по заданию центра (этап 11G).

    Почему это не отмена рубежа, а перенос подписи.

    Прежний порядок требовал, чтобы человек с доступом к машине создал файл
    ПОСЛЕ появления задания. Для этапов 11C–11F это было единственно верно:
    центр не умел сказать, чего он хочет, и файл был единственным местом, где
    расход чужой подписки кем-то подписан. С 11G центр присылает ограниченное
    требование, и подпись переезжает на два решения ВЛАДЕЛЬЦА МАШИНЫ, принятые
    заранее и не зависящие от задания:

      * воркер зарегистрирован и одобрен у этого центра;
      * администратор VPS включил автоматические разрешения и задал потолок
        обращений на одно задание (`machine_ceiling`).

    Что при этом сохраняется дословно: запись ложится на диск ДО вызова модели,
    привязана к ЗАДАНИЮ, имеет СРОК и списывается АТОМАРНО. Ни одно свойство
    разрешения не ослаблено — сменился только автор записи.

    Идемпотентность — не удобство, а инвариант. `grant_id` детерминирован по
    попытке, и повторный вход (перезапуск исполнителя, повторная доставка
    задания) НЕ создаёт вторую запись и НЕ обнуляет `used`: уже потраченная
    единица остаётся потраченной. Именно этим автоматическая выписка
    отличается от «выписать заново», которое `issue` запрещает.
    """
    ceiling = int(machine_ceiling)
    if ceiling <= 0:
        raise InferenceGrantError(
            "автоматические разрешения на этой машине не включены "
            "(AUDIT_WORKER_PIPELINE_PROVIDER_MAX_INFERENCES=0). Потолок задаёт "
            "владелец VPS, и задание его не переопределяет"
        )
    requested = int(requested_max_inferences)
    if requested <= 0:
        raise InferenceGrantError(
            f"задание не запрашивает обращений к модели (max_inferences={requested})"
        )
    if requested > ceiling:
        # Отказ, а не молчаливое усечение. Урезанный потолок означал бы аудит,
        # оборвавшийся в середине уже оплаченным, — и никто не смог бы сказать,
        # что произошло: журнал показал бы «упёрлись в лимит», не назвав чей.
        raise InferenceGrantError(
            f"задание просит {requested} обращений к модели, а машина разрешает "
            f"не более {ceiling} на задание. Урезать требование молча нельзя: "
            "аудит оборвался бы в середине, уже потратив часть вызовов"
        )
    if not str(capability or "").strip():
        raise InferenceGrantError(
            "разрешение не выписывается без логической способности: без неё "
            "точную модель выберет CLI, а не политика машины"
        )
    provider_name = str(provider).strip().lower()
    if not provider_name:
        raise InferenceGrantError("разрешение не выписывается без провайдера")
    legacy_grant_id = f"auto-{attempt_id}"
    moment = now if now is not None else time.time()
    path = grant_path(worker_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FileLock(_lock_path(worker_root)):
        problem = _check_file_safety(path)
        if problem:
            raise InferenceGrantError(problem)
        records = _parse(path.read_text(encoding="utf-8")) if path.exists() else []
        # Single-provider попытки сохраняют исторический идентификатор
        # ``auto-<attempt>``. У multi-provider попытки тот же attempt_id, но
        # разрешения принадлежат РАЗНЫМ подпискам. Прежний поиск только по
        # attempt_id возвращал Claude-запись для Codex/OpenRouter, после чего
        # их ``consume(provider=...)`` закономерно не находил разрешения.
        # Первый провайдер оставляет legacy ID, следующие получают безопасный
        # суффикс провайдера. Повторный вход находит именно свою запись и не
        # обнуляет её счётчик.
        legacy = next(
            (r for r in records if r.grant_id == legacy_grant_id), None
        )
        grant_id = (
            legacy_grant_id
            if legacy is None or legacy.provider == provider_name
            else f"{legacy_grant_id}-{provider_name}"
        )
        existing = next((r for r in records if r.grant_id == grant_id), None)
        if existing is not None:
            # Повторный вход. Возвращаем запись КАК ЕСТЬ — со всем, что уже
            # потрачено. Перевыписать значило бы вернуть израсходованной
            # попытке новый оплаченный прогон.
            return existing
        record = GrantRecord(
            grant_id=grant_id,
            provider=provider_name,
            task_id=str(job_id),
            # ВХОДЫ в канал, а не обращения к модели: см. комментарий к
            # `GRANT_MAX_ENTRIES_PER_ATTEMPT`. Потолок обращений едет в привязке
            # (`binding.max_inferences`) и проверяется на каждом вызове.
            max_uses=GRANT_MAX_ENTRIES_PER_ATTEMPT,
            used=0,
            expires_at=moment + float(ttl_sec),
            note=(
                f"auto:11G job={job_id} attempt={attempt_id} "
                f"capability={capability} inferences={requested}/{ceiling}"
            ),
        )
        records.append(record)
        _write_atomically(path, records)
        return record


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
    """Выписать разрешение ВРУЧНУЮ. Оператор и тесты, не рантайм воркера.

    Отличие от `issue_for_job` — не в правах, а в ИСТОЧНИКЕ: здесь параметры
    задаёт человек произвольно, там они выводятся из задания центра и
    зажимаются потолком машины. Поэтому вызывать `issue` из кода воркера
    по-прежнему нельзя, и это проверяется тестом.
    """
    moment = now if now is not None else time.time()
    path = grant_path(worker_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _FileLock(_lock_path(worker_root)):
        records = _parse(path.read_text(encoding="utf-8")) if path.exists() else []
        # Перевыписать УЖЕ ИСПОЛЬЗОВАННОЕ разрешение под тем же идентификатором
        # нельзя. Раньше строка ниже просто выбрасывала прежнюю запись, и
        # `used` обнулялся — то есть повторный `issue` с тем же `grant_id`
        # возвращал израсходованному заданию новую оплаченную попытку, минуя
        # оба замка сразу (разрешение и, при новом каталоге попытки, журнал).
        # Разрешение — единица расхода чужой подписки; «выписать заново» обязано
        # означать НОВЫЙ идентификатор, а не сброс счётчика у старого.
        previous = next((r for r in records if r.grant_id == str(grant_id)), None)
        if previous is not None and previous.used > 0:
            raise InferenceGrantError(
                f"разрешение {grant_id!r} уже использовано "
                f"({previous.used}/{previous.max_uses}) и не может быть выписано "
                "повторно: для новой попытки нужен новый идентификатор"
            )
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
