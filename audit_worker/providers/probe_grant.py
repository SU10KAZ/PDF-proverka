"""Разрешение на реальный контрольный запрос СО СТОРОНЫ ВОРКЕРА.

Зачем отдельный модуль вместо переменной окружения (находка 9 дока 11b).

Этап 11 объявлял два независимых разрешения на вызов модели:

  1. `AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=true` — «решение администратора VPS»;
  2. `--i-confirm-single-real-request` — «решение оператора здесь и сейчас».

Независимость оказалась мнимой. Смоук этапа 11 формировал одну строку

    ssh worker 'AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=true … provider-probe …
                --i-confirm-single-real-request'

и подавал ОБА разрешения из одной команды одного вызывающего. Переменная не
читалась с воркера — она приписывалась к команде снаружи, а значит второе
разрешение ничего не добавляло к первому: кто мог написать флаг, тот же мог
написать и переменную.

Здесь разрешение со стороны воркера — ФАЙЛ на воркере:

    <worker_root>/config/allow_real_provider_probe      (0600, владелец — воркер)

Его нельзя приписать к SSH-команде: он либо лежит на машине, либо нет. Создать
его может только тот, у кого есть доступ к машине, а прочитать — только процесс
воркера. Это и есть тот второй голос, которого не хватало.

Формат намеренно примитивный — строки `<провайдер>=<число>`:

    # выдано оператором 2026-08-09 на этап 11b
    claude=1
    codex=1

Число — БЮДЖЕТ оставшихся контрольных запросов, а не булево «можно». Разница
принципиальная и стоит отдельного слова.

«Можно» не ограничивает количество: разрешив один запрос, оператор разрешил бы
и сотый — дальше всё держится на дисциплине вызывающего. Ровно этого рода
обещание («я выполню только один») подписка проверить не может, а счёт
расходуется настоящий. Бюджет же расходуется машиной: `consume()` списывает
единицу ДО обращения к модели и переписывает файл на диск, поэтому второй запуск
той же команды упирается в `остаток 0` независимо от намерений и от того, чем
кончился первый. Аварийное завершение здесь работает в безопасную сторону —
попытка засчитана.

Чего модуль НЕ делает: не читает учётные данные, не создаёт файл сам (иначе
воркер выписывал бы разрешение себе) и не считает переменную окружения
равноценной заменой файла.
"""
from __future__ import annotations

import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

#: Имя файла разрешения внутри `<worker_root>/config`. Совпадает с именем
#: переменной этапа 11 не случайно: оператор, который искал переменную, найдёт
#: файл, а не будет гадать, куда делось разрешение.
GRANT_FILENAME = "allow_real_provider_probe"

#: Права, шире которых файл разрешения быть не должен. Группе и «остальным»
#: нельзя ничего: разрешение на расход чужой подписки — не общедоступный факт.
MAX_GRANT_MODE = 0o600


class ProbeGrantError(RuntimeError):
    """Разрешение отсутствует, повреждено или исчерпано."""


@dataclass(frozen=True)
class GrantState:
    """Состояние разрешения для одного провайдера."""

    path: Path
    exists: bool
    remaining: int
    error: Optional[str] = None

    @property
    def usable(self) -> bool:
        return self.exists and self.error is None and self.remaining > 0

    def as_dict(self) -> dict[str, object]:
        # Абсолютный путь описывает раскладку чужой машины — центру он не нужен.
        # Оператору на самой машине путь и так известен из сообщения об ошибке.
        return {
            "grant_file_present": self.exists,
            "remaining": self.remaining,
            "error": self.error,
        }


def grant_path(worker_root: Path) -> Path:
    return Path(worker_root) / "config" / GRANT_FILENAME


def _check_file_safety(path: Path) -> Optional[str]:
    """Файл разрешения обязан быть обычным, своим и узким по правам.

    Символьная ссылка отвергается отдельно от прав: `lstat` описывает саму
    ссылку, и «0600 на ссылке» не говорит ничего о том, куда она ведёт.
    """
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


def _parse(text: str) -> dict[str, int]:
    """Разбор `<провайдер>=<число>`. Мусорная строка — ошибка, не «ноль».

    Молча пропустить нечитаемую строку значит превратить опечатку
    (`claude:1` вместо `claude=1`) в «разрешения нет», а искать оператор будет
    в подписке.
    """
    budgets: dict[str, int] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ProbeGrantError(
                f"строка {lineno} файла разрешения не вида <провайдер>=<число>: {line!r}"
            )
        name, _, value = line.partition("=")
        name = name.strip().lower()
        try:
            budget = int(value.strip())
        except ValueError:
            raise ProbeGrantError(
                f"строка {lineno}: остаток {value.strip()!r} не целое число"
            ) from None
        if budget < 0:
            raise ProbeGrantError(f"строка {lineno}: отрицательный остаток {budget}")
        budgets[name] = budget
    return budgets


def read_state(worker_root: Path, provider: str) -> GrantState:
    """Прочитать остаток, ничего не расходуя. Для отчётов и предупреждений."""
    path = grant_path(worker_root)
    problem = _check_file_safety(path)
    if problem:
        return GrantState(path=path, exists=True, remaining=0, error=problem)
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return GrantState(path=path, exists=False, remaining=0)
    except OSError as exc:
        return GrantState(path=path, exists=True, remaining=0,
                          error=f"файл разрешения не читается: {exc}")
    try:
        budgets = _parse(text)
    except ProbeGrantError as exc:
        return GrantState(path=path, exists=True, remaining=0, error=str(exc))
    return GrantState(
        path=path, exists=True, remaining=int(budgets.get(str(provider).lower(), 0))
    )


def _write_atomically(path: Path, budgets: dict[str, int]) -> None:
    """Переписать файл целиком через временный + `rename`.

    Правка «на месте» оставила бы окно, в котором файл усечён: падение внутри
    него стёрло бы остатки ВСЕХ провайдеров, а не одного.
    """
    lines = [
        "# Разрешение воркера на реальные контрольные запросы к моделям.",
        "# Число — остаток запросов; списывается автоматически перед вызовом.",
        "# Файл создаёт человек с доступом к машине, а не воркер.",
    ]
    lines.extend(f"{name}={value}" for name, value in sorted(budgets.items()))
    payload = "\n".join(lines) + "\n"
    directory = path.parent
    handle, temp_name = tempfile.mkstemp(dir=str(directory), prefix=".grant.")
    temp_path = Path(temp_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp_path, MAX_GRANT_MODE)
        os.replace(temp_path, path)
    except BaseException:
        temp_path.unlink(missing_ok=True)
        raise


def consume(worker_root: Path, provider: str) -> int:
    """Списать одну попытку. Возвращает остаток ПОСЛЕ списания.

    Порядок именно такой: сначала запись на диск, потом вызов модели. Обратный
    («вызвали, потом списали») дарил бы бесплатную попытку при любом падении в
    середине — а падение в середине как раз и означает, что запрос ушёл, а
    результат потерян.
    """
    name = str(provider).lower()
    path = grant_path(worker_root)
    state = read_state(worker_root, name)
    if state.error:
        raise ProbeGrantError(state.error)
    if not state.exists:
        raise ProbeGrantError(
            f"нет разрешения воркера на контрольный запрос: файл {path} отсутствует.\n"
            f"Его создаёт человек с доступом к машине:\n"
            f"    install -m 600 /dev/null {path}\n"
            f"    printf '%s=1\\n' {name} > {path}\n"
            "Переменная окружения этого разрешения больше не заменяет "
            "(находка 9 дока 11b): разрешение, приписанное к той же SSH-команде, "
            "не является независимым."
        )
    if state.remaining <= 0:
        raise ProbeGrantError(
            f"разрешение на контрольные запросы {name} исчерпано (остаток 0). "
            f"Новую попытку выдаёт человек с доступом к машине правкой {path}."
        )
    budgets = _parse(path.read_text(encoding="utf-8"))
    budgets[name] = int(budgets.get(name, 0)) - 1
    _write_atomically(path, budgets)
    return budgets[name]
