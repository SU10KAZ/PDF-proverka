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


def probe(default_path: Path, *, env: Optional[dict[str, str]] = None) -> SecretStatus:
    """Настроен ли ключ. Файл НЕ открывается — только `os.stat`.

    Это тот самый «безопасный zero-cost auth check» §7 задания: он не тратит ни
    одного запроса к провайдеру и не может стоить денег. Больше он ничего и не
    утверждает: `configured` означает «ключ на месте и права узкие», а не
    «ключ действителен». Выдавать второе за первое — ровно то, что §7
    запрещает.
    """
    try:
        path, source = credential_path(default_path, env=env)
    except OpenRouterSecretError as exc:
        return SecretStatus(configured=False, present=False, reason=str(exc))
    try:
        info = os.stat(path)
    except OSError:
        return SecretStatus(
            configured=False, present=False, source=source,
            reason="файл ключа не найден: провайдер на этом воркере не настроен",
        )
    mode = stat.S_IMODE(info.st_mode)
    group_readable = bool(mode & stat.S_IRGRP)
    world_readable = bool(mode & stat.S_IROTH)
    owner_ok = info.st_uid == os.getuid()
    common = {
        "present": True,
        "mode": f"{mode:04o}",
        "owner_is_current_user": owner_ok,
        "group_readable": group_readable,
        "world_readable": world_readable,
        "source": source,
    }
    if info.st_size <= 0:
        return SecretStatus(configured=False, reason="файл ключа пуст", **common)
    if info.st_size > MAX_CREDENTIAL_BYTES:
        return SecretStatus(
            configured=False,
            reason=(
                f"файл ключа больше {MAX_CREDENTIAL_BYTES} байт — это не ключ, "
                "а чужой файл, указанный путём по ошибке"
            ),
            **common,
        )
    if world_readable or group_readable:
        # Отказ, а не предупреждение. Ключ, доступный на чтение соседнему
        # сервису этого VPS, — это ключ, который уже утёк; продолжать работу
        # с ним значило бы платить за чужие запросы и не знать об этом.
        return SecretStatus(
            configured=False,
            reason=(
                f"права файла ключа {mode:04o} слишком широкие: чтение "
                "разрешено не только владельцу. Требуется 0600"
            ),
            **common,
        )
    if not owner_ok:
        return SecretStatus(
            configured=False,
            reason="файл ключа принадлежит другому пользователю",
            **common,
        )
    return SecretStatus(configured=True, reason="", **common)


def read_secret(default_path: Path, *, env: Optional[dict[str, str]] = None) -> str:
    """Прочитать ключ. Вызывается ТОЛЬКО в момент запроса к провайдеру.

    Все проверки `probe()` повторяются здесь заново, а не берутся из снимка:
    между heartbeat и вызовом файл мог смениться, и «когда-то было 0600» — не
    утверждение о настоящем моменте. Это же закрывает §24 задания: пропавший
    между preflight и действием ключ обязан провалить КОНКРЕТНОЕ действие.
    """
    status = probe(default_path, env=env)
    if not status.configured:
        raise OpenRouterSecretError(
            status.reason or "ключ OpenRouter на этом воркере не настроен"
        )
    path, _source = credential_path(default_path, env=env)
    try:
        raw = path.read_text(encoding="utf-8", errors="strict")
    except (OSError, UnicodeDecodeError) as exc:
        # В текст ошибки уходит ТИП проблемы, а не содержимое файла: сообщение
        # попадёт в лог попытки и в отчёт о прогоне.
        raise OpenRouterSecretError(
            f"файл ключа не читается ({type(exc).__name__})"
        ) from None
    key = _extract_key(raw)
    if len(key) < MIN_KEY_LEN:
        raise OpenRouterSecretError(
            "файл ключа не содержит пригодного значения "
            f"(нужно не меньше {MIN_KEY_LEN} символов)"
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
