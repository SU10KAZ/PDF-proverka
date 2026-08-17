"""Жизненный цикл временного дерева сборки иммутабельного релиза.

Зачем это отдельный модуль, а не пара строк внутри сборщика.

Релиз запечатывается: каталоги доводятся до 0555, файлы до 0444/0555. Так и
должно быть — работающий релиз никто не правит на месте. Но ровно это делает
временное дерево НЕУДАЛЯЕМЫМ: `rm -rf` не может снять запись в каталоге, у
которого нет бита `w`. Пока сборка удавалась, разницы не было: дерево
переезжало в `releases/` целиком. Отказ ПОСЛЕ запечатывания оставлял в `/tmp`
почти гигабайт, который не убирался ничем.

`/tmp` здесь — tmpfs на 32 ГБ, общая для всей машины. Несколько неудачных
сборок подряд забили её, и перестал работать вообще любой процесс, которому
нужен временный файл. То есть цена невынесенной уборки — не мусор на диске, а
остановка машины.

Отсюда правила, которые проверяются тестами:

  * уборка живёт в `finally` — она обязана случиться и на исключении, а не
    только на удачном пути;
  * перед удалением снимаются режимы «только чтение»; бит исполнения при этом
    НЕ снимается, а симлинки не трогаются вовсе (иначе `chmod` пошёл бы по
    ссылке в venv и испортил бы цель);
  * убирается ТОЛЬКО собственное staging-дерево. Готовый релиз, чужие каталоги
    и `/tmp` соседей не трогаются ни при каких условиях.
"""
from __future__ import annotations

import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

#: Префикс временных каталогов сборки. Опознаваемый префикс — часть защиты:
#: по нему видно, что каталог наш, и его можно удалять.
STAGING_PREFIX = "auditmanager-relbuild-"


def make_writable(root: Path) -> None:
    """Снять «только чтение» со всего дерева, не ломая его содержимое.

    Обход снизу вверх: сначала вложенное, потом родитель. Иначе после
    `chmod` родителя обход мог бы не найти путь к детям.
    """
    if not root.exists():
        return
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            # Симлинк не имеет собственных прав в Linux, а `chmod` по нему
            # применился бы к ЦЕЛИ — то есть к файлу вне staging.
            continue
        try:
            mode = stat.S_IMODE(path.stat().st_mode)
            path.chmod(mode | (0o700 if path.is_dir() else 0o600))
        except OSError:
            continue
    try:
        root.chmod(stat.S_IMODE(root.stat().st_mode) | 0o700)
    except OSError:
        pass


def cleanup_staging(tmp: Path) -> None:
    """Снести временное дерево, даже если оно уже запечатано.

    Безопасность здесь строится на одном условии: удаляется только каталог с
    нашим префиксом. Всё остальное — чужое, и молчаливое удаление чужого хуже,
    чем оставленный мусор.
    """
    if not tmp or not tmp.exists():
        return
    if not tmp.name.startswith(STAGING_PREFIX):
        raise ValueError(
            f"отказ удалять чужой каталог: {tmp} (ожидался префикс {STAGING_PREFIX!r})"
        )
    make_writable(tmp)
    shutil.rmtree(tmp, ignore_errors=True)
    if tmp.exists():
        # `ignore_errors=True` нужен, чтобы уборка не падала на гонке с чужим
        # процессом, — но он же молча объявляет успех, когда каталог остался.
        # Именно так утечка и была бы не замечена во второй раз: в логах чисто,
        # в tmpfs гигабайт. Проверяем факт, а не намерение.
        raise RuntimeError(
            f"каталог сборки не удалён и продолжает занимать место: {tmp}"
        )


@contextmanager
def staging_workspace(*, parent: Path | None = None) -> Iterator[Path]:
    """Временное дерево сборки, которое исчезает при ЛЮБОМ исходе.

    Успех, отказ до запечатывания, отказ после запечатывания, исключение,
    прерывание — все пять исходов сходятся в `finally`.
    """
    tmp = Path(tempfile.mkdtemp(prefix=STAGING_PREFIX, dir=str(parent) if parent else None))
    try:
        yield tmp
    finally:
        cleanup_staging(tmp)


def seal_tree(root: Path, *, directory_mode: int = 0o555) -> None:
    """Довести дерево до вида готового релиза: каталоги 0555, файлы 0444/0555.

    Выделено сюда ради тестов уборки: они обязаны проверять снос ИМЕННО
    запечатанного дерева, а не приблизительной имитации.
    """
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(directory_mode)
        else:
            current = stat.S_IMODE(path.stat().st_mode)
            path.chmod(0o555 if current & 0o111 else 0o444)
    root.chmod(directory_mode)
