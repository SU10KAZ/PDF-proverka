"""ProviderAuthMode — ОТКУДА CLI провайдера берёт свою авторизацию.

Этап 11 знал ровно одну схему: изолированный provider home внутри каталога
данных воркера, куда оператор входит отдельно. Этап 11b добавляет вторую —
`ambient_user`, когда CLI пользуется штатной авторизацией того самого
пользователя Unix, под которым работает воркер.

Почему это отдельная ось, а не флаг «использовать /home/coder»:

  * у режима РАЗНАЯ семантика владения. В изолированном режиме каталогом
    владеет воркер: он его создаёт, выставляет 0700 и вправе пересоздать. В
    ambient каталог принадлежит ЧЕЛОВЕКУ, и единственно верное поведение —
    не трогать его вовсе (см. `ProviderHome.ensure_dirs`);
  * режим виден оператору и центру как значение, а не выводится из путей.
    «Почему воркер вдруг авторизован» — вопрос, на который отчёт обязан
    отвечать словом, а не сравнением двух абсолютных путей;
  * третье значение `unavailable` нужно, чтобы «мы сознательно не даём этому
    провайдеру учётных данных на этом воркере» отличалось от «что-то
    сломалось». Первое — решение, второе — авария.

Режим задаётся ПОПРОВАЙДЕРНО и только явно:
`AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE`, `AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE`.
Глобальной переменной нет намеренно: `ambient_user` — это разрешение CLI
дотянуться до личного каталога человека, и включаться оно обязано на одном
конкретном воркере для одного конкретного провайдера, а не «везде по
умолчанию» (§13 задания).
"""
from __future__ import annotations

import os
import pwd
from pathlib import Path
from typing import Optional

#: CLI пользуется штатной авторизацией пользователя Unix, под которым работает
#: воркер: `HOME=/home/<user>`, `~/.claude` / `~/.codex` — его личные.
AUTH_MODE_AMBIENT_USER = "ambient_user"

#: Схема этапа 11: свой HOME внутри каталога данных воркера. Значение по
#: умолчанию — новый режим не включается сам собой нигде.
AUTH_MODE_ISOLATED_PROVIDER_HOME = "isolated_provider_home"

#: Учётных данных для этого провайдера на этом воркере нет и не предполагается.
#: Это РЕШЕНИЕ оператора, а не сбой: адаптер в этом режиме не запускает CLI для
#: проверки авторизации вовсе.
AUTH_MODE_UNAVAILABLE = "unavailable"

#: Порядок фиксирован: он же порядок перечисления в отчётах.
AUTH_MODES: tuple[str, ...] = (
    AUTH_MODE_AMBIENT_USER,
    AUTH_MODE_ISOLATED_PROVIDER_HOME,
    AUTH_MODE_UNAVAILABLE,
)

#: Умолчание = поведение этапа 11. Менять его значит менять поведение всех уже
#: развёрнутых воркеров одной правкой кода — ровно то, что §13 запрещает.
DEFAULT_AUTH_MODE = AUTH_MODE_ISOLATED_PROVIDER_HOME


class UnknownAuthMode(ValueError):
    """Значение режима не из закрытого списка."""


class AmbientHomeUnresolved(RuntimeError):
    """Режим ambient_user выбран, но домашний каталог не определён."""


def require_auth_mode(value: str) -> str:
    """Привести значение к каноническому виду или упасть.

    Тихий фолбэк на умолчание здесь недопустим: опечатка в
    `AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE=ambient-user` (через дефис) означала
    бы, что оператор думает про ambient, а воркер молча работает в изоляции и
    рапортует «вход не выполнен». Ошибка обязана быть громкой.
    """
    text = str(value or "").strip().lower()
    if text not in AUTH_MODES:
        raise UnknownAuthMode(
            f"неизвестный режим авторизации {value!r}; допустимы {AUTH_MODES}"
        )
    return text


def normalize_auth_mode(value: Optional[str]) -> str:
    """`None`/пусто → умолчание; всё остальное проходит строгую проверку."""
    if value is None or not str(value).strip():
        return DEFAULT_AUTH_MODE
    return require_auth_mode(value)


def resolve_ambient_home(override: Optional[Path] = None) -> Path:
    """Домашний каталог пользователя, под которым работает воркер.

    Источник истины — база пользователей (`pwd`), а НЕ `os.environ["HOME"]`.
    Разница принципиальная: `HOME` — обычная переменная окружения, и тот, кто
    сумеет её подменить, увёл бы CLI читать учётные данные из подставного
    каталога, а воркер отрапортовал бы центру `logged_in`. `pwd` описывает
    учётную запись, а не окружение процесса, и подменяется только правкой
    системной базы.

    `override` существует ради тестов и нештатных раскладок и приходит из кода
    (конструктор `ProviderHome`), а не из окружения: переменная-override здесь
    вернула бы ровно ту дыру, ради закрытия которой взят `pwd`.
    """
    if override is not None:
        return Path(override).expanduser()
    try:
        entry = pwd.getpwuid(os.getuid())
    except KeyError as exc:                                # pragma: no cover
        raise AmbientHomeUnresolved(
            "пользователь процесса отсутствует в базе учётных записей"
        ) from exc
    home = str(entry.pw_dir or "").strip()
    if not home or home == "/":
        raise AmbientHomeUnresolved(
            "у пользователя процесса нет пригодного домашнего каталога"
        )
    return Path(home)


def ambient_user_name(override: Optional[str] = None) -> str:
    """Имя пользователя Unix для `USER`/`LOGNAME` подпроцесса.

    Берётся оттуда же, откуда и домашний каталог, и по той же причине.
    """
    if override is not None:
        return str(override)
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except KeyError:                                       # pragma: no cover
        raise AmbientHomeUnresolved(
            "пользователь процесса отсутствует в базе учётных записей"
        ) from None
