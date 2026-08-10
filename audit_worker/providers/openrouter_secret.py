"""Ключ OpenRouter как ЛОКАЛЬНЫЙ секрет воркера (этап 11J, §5 и §6 задания).

Почему отдельный модуль, а не поле привязки и не переменная окружения.

Claude и Codex авторизуются входом оператора на самой машине: центр не знает
их учётных данных и не может их передать даже случайно — их просто нет ни в
одном объекте, который едет по проводу. С OpenRouter так не получается само
собой: ключ — это строка, а строку слишком легко положить в задание. Один раз
положив, её потом не вынуть: она уедет в `logical_jobs.payload`, в пакет
источника, в EventOutbox, в отчёт о прогоне и в резервную копию БД центра — и
всё это на машинах, которыми владелец ключа не управляет.

Поэтому канал ровно один и он однонаправленный:

    оператор VPS  ──(ssh, один раз)──>  файл 0600 в каталоге данных воркера
                                             │
                                             ▼
                            adapter читает в момент запроса
                                             │
                                             ▼
                                  заголовок Authorization

Центр в этой схеме не участвует НИГДЕ. Он присылает `provider=openrouter` и
способность; чем именно воркер расплатится — его собственное дело, ровно как с
подпиской Claude.

Что здесь запрещено конструкцией, а не соглашением:

  * **ключ не хранится на объекте.** `read_secret()` возвращает строку
    вызывающему и ничего не запоминает: ни в модуле, ни в адаптере нет поля,
    в котором ключ пережил бы вызов. Кеш здесь сэкономил бы микросекунды и
    добавил бы значение, которое попадёт в дамп памяти и в `repr` объекта;
  * **ключ не берётся из окружения процесса.** `OPENROUTER_API_KEY` входит в
    `FORBIDDEN_ENV_NAMES` слоя провайдеров и отсутствует в белом списке
    окружения процесса конвейера — то есть его там физически нет. Читать его
    оттуда значило бы завести второй, неконтролируемый путь провижининга;
  * **путь к файлу — не секрет, но и не данные задания.** Его задаёт
    администратор VPS (`AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL`) либо он
    вычисляется из раскладки `ProviderHome`. Из задания путь не приходит
    никогда — это тот же рубеж I-P5, что и у argv;
  * **факт «ключ настроен» отделён от значения.** `probe()` отвечает
    `configured=True/False`, читая ТОЛЬКО `os.stat`; heartbeat пользуется
    именно им и содержимого файла не касается.

Формат файла. Принимаются две формы, обе — обычный текст:

    {"api_key": "sk-or-…"}          ← предпочтительная (JSON, расширяемая)
    sk-or-…                          ← одна строка, без кавычек

Вторая нужна затем, чтобы провижининг сводился к одной команде и не требовал
от оператора собирать JSON руками в ssh-сессии. Оболочка обоих вариантов
одинакова: файл 0600, владелец — пользователь воркера.
"""
from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

#: Переменная, которой АДМИНИСТРАТОР VPS может указать другой путь к файлу
#: ключа. Значение — путь, не ключ. Ни центр, ни задание её не задают: она
#: попадает в окружение процесса конвейера через белый список
#: `audit_runner._ENV_WHITELIST`, куда данные задания не доходят.
CREDENTIAL_PATH_ENV = "AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL"

#: Максимальный размер файла ключа. Ключ OpenRouter — десятки байт; всё, что
#: больше килобайта, — это не ключ, а чужой файл, случайно указанный путём.
#: Читать такое целиком в память процесса, который пишет логи, незачем.
MAX_CREDENTIAL_BYTES = 4096

#: Минимальная длина, при которой строка вообще считается ключом. Пустой файл
#: и файл с переводом строки — это «не настроен», а не «настроен пустым».
MIN_KEY_LEN = 8


class OpenRouterSecretError(RuntimeError):
    """Ключ недоступен. Текст сообщения НИКОГДА не содержит значения."""


@dataclass(frozen=True)
class SecretStatus:
    """Наблюдаемое состояние ключа. Значения в нём нет ни в каком виде."""

    #: Ключ настроен и пригоден к использованию.
    configured: bool
    #: Файл существует (может быть непригодным — например, пустым).
    present: bool
    #: Права файла в восьмеричном виде, как их видит `os.stat`.
    mode: Optional[str] = None
    owner_is_current_user: Optional[bool] = None
    group_readable: Optional[bool] = None
    world_readable: Optional[bool] = None
    #: Откуда взят путь: `admin_env` либо `provider_home`.
    source: str = ""
    #: Почему `configured=False`. Диагностика для оператора, не для центра.
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "configured": bool(self.configured),
            "present": bool(self.present),
            "mode": self.mode,
            "owner_is_current_user": self.owner_is_current_user,
            "group_readable": self.group_readable,
            "world_readable": self.world_readable,
            "source": self.source,
            "reason": self.reason,
        }


def credential_path(default_path: Path, *, env: Optional[dict[str, str]] = None) -> tuple[Path, str]:
    """Путь к файлу ключа и то, откуда он взялся.

    `default_path` приходит из `ProviderHome.credential_path`, то есть вычислен
    от корня данных воркера. Переопределение администратора обязано быть
    АБСОЛЮТНЫМ: относительный путь означал бы «взять что-нибудь из cwd», а cwd
    процесса конвейера — каталог попытки, куда пишет задание.
    """
    source = env if env is not None else os.environ
    raw = str(source.get(CREDENTIAL_PATH_ENV, "") or "").strip()
    if not raw:
        return Path(default_path), "provider_home"
    if not raw.startswith("/"):
        raise OpenRouterSecretError(
            f"{CREDENTIAL_PATH_ENV} задан относительным путём: ожидается "
            "абсолютный путь к файлу ключа"
        )
    return Path(raw), "admin_env"


#: Максимальные права файла ключа. Проверяется ВСЯ маска, а не только биты
#: чтения: файл 0622 «не читается посторонним», но записывается им — и тогда в
#: заголовок `Authorization` уедет чужая строка. Значение и способ проверки
#: взяты у `inference_grant.MAX_GRANT_MODE` дословно: два файла с одинаковыми
#: требованиями обязаны проверяться одинаково, иначе они разъедутся.
MAX_CREDENTIAL_MODE = 0o600


def _safety_reason(info: os.stat_result) -> str:
    """Почему файл с такими фактами непригоден. Пустая строка — пригоден.

    Правила совпадают с `inference_grant._check_file_safety`, и это не
    копирование от лени: два соседних файла с одинаковыми требованиями,
    проверяемые по-разному, — это гарантия, что однажды они разойдутся, и
    разойдутся в сторону послабления.
    """
    if stat.S_ISLNK(info.st_mode):
        return (
            "файл ключа — символьная ссылка; ожидается обычный файл. Ссылка "
            "означает, что содержимое выбирает не тот, кто владеет каталогом"
        )
    if not stat.S_ISREG(info.st_mode):
        return "файл ключа не является обычным файлом"
    if info.st_uid != os.getuid():
        return (
            f"файл ключа принадлежит uid={info.st_uid}, а воркер работает "
            f"под uid={os.getuid()}"
        )
    extra = stat.S_IMODE(info.st_mode) & ~MAX_CREDENTIAL_MODE
    if extra:
        return (
            f"права файла ключа {stat.S_IMODE(info.st_mode):04o} шире "
            f"допустимых {MAX_CREDENTIAL_MODE:04o}: чтение или запись "
            "разрешены не только владельцу"
        )
    if info.st_size <= 0:
        return "файл ключа пуст"
    if info.st_size > MAX_CREDENTIAL_BYTES:
        return (
            f"файл ключа больше {MAX_CREDENTIAL_BYTES} байт — это не ключ, "
            "а чужой файл, указанный путём по ошибке"
        )
    return ""


def _facts(info: os.stat_result, source: str) -> dict[str, Any]:
    mode = stat.S_IMODE(info.st_mode)
    return {
        "present": True,
        "mode": f"{mode:04o}",
        "owner_is_current_user": info.st_uid == os.getuid(),
        "group_readable": bool(mode & stat.S_IRGRP),
        "world_readable": bool(mode & stat.S_IROTH),
        "source": source,
    }


def probe(default_path: Path, *, env: Optional[dict[str, str]] = None) -> SecretStatus:
    """Настроен ли ключ. Файл НЕ открывается — только `lstat`.

    Это тот самый «безопасный zero-cost auth check» §7 задания: он не тратит ни
    одного запроса к провайдеру и не может стоить денег. Больше он ничего и не
    утверждает: `configured` означает «ключ на месте и права узкие», а не
    «ключ действителен». Выдавать второе за первое — ровно то, что §7
    запрещает.

    `lstat`, а не `stat`: `stat` идёт ПО ССЫЛКЕ, и файл ключа, подменённый
    ссылкой на чужой файл, прошёл бы проверку прав по цели ссылки. Для секрета,
    который уезжает в заголовок HTTP-запроса, это означало бы отправку чужого
    содержимого на внешний хост.
    """
    try:
        path, source = credential_path(default_path, env=env)
    except OpenRouterSecretError as exc:
        return SecretStatus(configured=False, present=False, reason=str(exc))
    try:
        info = os.lstat(path)
    except OSError:
        return SecretStatus(
            configured=False, present=False, source=source,
            reason="файл ключа не найден: провайдер на этом воркере не настроен",
        )
    facts = _facts(info, source)
    reason = _safety_reason(info)
    if reason:
        return SecretStatus(configured=False, reason=reason, **facts)
    return SecretStatus(configured=True, reason="", **facts)


def read_secret(default_path: Path, *, env: Optional[dict[str, str]] = None) -> str:
    """Прочитать ключ. Вызывается ТОЛЬКО в момент запроса к провайдеру.

    Проверки не берутся из снимка heartbeat и не повторяются «примерно»: файл
    открывается ОДИН раз с `O_NOFOLLOW`, и решение о пригодности принимается по
    `fstat` УЖЕ ОТКРЫТОГО дескриптора. Это принципиально: между `lstat` и
    `open` файл можно подменить, и тогда проверенные права описывали бы один
    inode, а прочитанные байты приходили бы из другого. Здесь проверяется
    ровно тот inode, из которого читаем.

    Это же закрывает §24 задания: пропавший между preflight и действием ключ
    обязан провалить КОНКРЕТНОЕ действие.
    """
    try:
        path, _source = credential_path(default_path, env=env)
    except OpenRouterSecretError:
        raise
    try:
        # `O_NOFOLLOW` отвергает символьную ссылку на уровне ядра: проверка
        # «а не ссылка ли это» и открытие становятся одной операцией.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise OpenRouterSecretError(
            f"файл ключа не открывается ({type(exc).__name__}): "
            "провайдер на этом воркере не настроен либо путь ведёт на ссылку"
        ) from None
    try:
        info = os.fstat(fd)
        reason = _safety_reason(info)
        if reason:
            raise OpenRouterSecretError(reason)
        # Читаем не больше потолка + 1 байт: превышение уже отвергнуто по
        # `fstat`, но между ним и чтением файл мог вырасти, и брать в память
        # процесса, который пишет логи, произвольный объём незачем.
        raw = os.read(fd, MAX_CREDENTIAL_BYTES + 1)
    finally:
        os.close(fd)
    if len(raw) > MAX_CREDENTIAL_BYTES:
        raise OpenRouterSecretError(
            f"файл ключа больше {MAX_CREDENTIAL_BYTES} байт"
        )
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise OpenRouterSecretError(
            "файл ключа не является текстом UTF-8"
        ) from None
    key = _extract_key(text)
    if len(key) < MIN_KEY_LEN:
        raise OpenRouterSecretError(
            "файл ключа не содержит пригодного значения "
            f"(нужно не меньше {MIN_KEY_LEN} символов)"
        )
    if not key.isascii():
        # Заголовок HTTP кодируется latin-1. Не-ASCII значение уронило бы
        # клиент исключением, в текст которого попал бы сам ключ.
        raise OpenRouterSecretError(
            "ключ содержит символы вне ASCII: такое значение невозможно "
            "передать заголовком HTTP"
        )
    return key


def _extract_key(raw: str) -> str:
    """Разобрать содержимое файла. Ни одна ветка не логирует и не возвращает сырьё."""
    text = (raw or "").strip()
    if not text:
        return ""
    if text.startswith("{"):
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            raise OpenRouterSecretError(
                "файл ключа похож на JSON, но не разбирается"
            ) from None
        if not isinstance(data, dict):
            raise OpenRouterSecretError("файл ключа: ожидается объект JSON")
        value = data.get("api_key")
        if not isinstance(value, str):
            raise OpenRouterSecretError(
                "в файле ключа нет строкового поля 'api_key'"
            )
        return value.strip()
    # Однострочная форма. Берётся ПЕРВАЯ непустая строка: файл, дописанный
    # вторым ключом, — это ошибка провижининга, и молча брать из него что-то
    # одно наугад нельзя, но и падать на завершающем переводе строки глупо.
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != 1:
        raise OpenRouterSecretError(
            "файл ключа содержит несколько строк: ожидается один ключ либо "
            "объект JSON с полем 'api_key'"
        )
    return lines[0]


def write_secret_for_tests(path: Path, key: str) -> Path:
    """Разложить ТЕСТОВОЕ значение ключа с правильными правами.

    Существует ради §25 задания: доказать жизненный цикл секрета можно только
    на настоящем файле, а настоящий ключ в автоматических тестах недопустим.
    Имя функции намеренно длинное и говорящее — вызов из боевого кода обязан
    выглядеть неуместно при чтении диффа.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except OSError:
        pass
    with open(path, "w", encoding="utf-8",
              opener=lambda p, f: os.open(p, f, 0o600)) as fh:
        fh.write(json.dumps({"api_key": str(key)}, ensure_ascii=False))
    os.chmod(path, 0o600)
    return path
