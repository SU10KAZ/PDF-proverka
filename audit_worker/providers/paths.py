"""Раскладка provider home и её инварианты.

Почему отдельный слой, а не «вернуть воркеру настоящий /home/coder».

Конвейер аудита намеренно работает с ИЗОЛИРОВАННЫМ HOME внутри каталога
попытки (`audit_runner.isolated_roots`): наследованный HOME означал бы и запись
мимо каталога попытки, и ambient-авторизацию настоящих CLI из личного каталога
человека. Вернуть настоящий HOME значит потерять оба свойства разом.

Поэтому авторизация провайдеров живёт в ТРЕТЬЕМ месте — ни личный каталог
пользователя VPS, ни каталог попытки:

    <AUDIT_WORKER_ROOT>/providers/
    ├── claude/
    │   ├── home/        ← HOME процесса CLI (внутри: .claude/.credentials.json)
    │   ├── runtime/     ← cwd подпроцессов: ПУСТОЙ каталог, не git-репозиторий
    │   └── metadata/    ← наши собственные заметки (соль отпечатка, кеш)
    └── codex/
        ├── home/        ← HOME процесса CLI (внутри: .codex/auth.json)
        ├── runtime/
        └── metadata/

Пути внутри `home/` заданы не нами, а официальным поведением CLI:

  * Claude Code на Linux хранит учётные данные в `~/.claude/.credentials.json`
    с режимом 0600, а `CLAUDE_CONFIG_DIR` переносит туда всё, что иначе жило бы
    в `~/.claude` (официальная документация «Authentication → Credential
    management» и «Explore the .claude directory»);
  * Codex CLI хранит их в `$CODEX_HOME/auth.json`, где `CODEX_HOME` по
    умолчанию `~/.codex` и официально описан как «root for Codex state,
    including config, auth, logs, sessions» (Codex → Environment variables).
    Каталог обязан существовать заранее — это тоже документированное поведение.

Три свойства раскладки, ради которых она такая:

  1. `providers/` лежит в КАТАЛОГЕ ДАННЫХ воркера, а не в каталоге кода
     (`app/<релиз>/` со ссылкой `current`). Обновление и откат кода не трогают
     авторизацию.
  2. `runtime/` пустой и не является git-репозиторием: у CLI, запущенного в нём,
     нет ни проектных настроек, ни файлов для чтения. Это не косметика — при
     запуске `codex app-server` с cwd=/home/coder он обнаружил личный
     `/home/coder/.codex` как project-local конфигурацию и предупредил об этом.
  3. Каталоги создаются с режимом 0700. Соседние сервисы на этом VPS работают
     под другими пользователями, и «читать нельзя» должно обеспечиваться
     файловой системой, а не договорённостью.

Этап 11b добавил второй режим — `ambient_user` (см. `auth_mode.py`), в котором
HOME процесса CLI = личный каталог пользователя VPS. Сказанное выше при этом
НЕ отменяется, потому что относится к другому процессу: HOME КОНВЕЙЕРА как был,
так и остаётся внутри каталога попытки (`audit_runner.isolated_roots`), и
ambient-режим его не касается. Различие ролей — единственная причина, по которой
два «домашних каталога» вообще могут сосуществовать:

    процесс конвейера   HOME=<job_dir>/work/home   (изоляция данных задания)
    процесс CLI         HOME=<по режиму>           (авторизация провайдера)

В ambient-режиме личный каталог считается ЧУЖИМ: он читается (`os.stat`), но
не создаётся и не перенастраивается по правам — см. `ensure_dirs`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from audit_worker.providers.auth_mode import (
    AUTH_MODE_AMBIENT_USER,
    DEFAULT_AUTH_MODE,
    normalize_auth_mode,
    resolve_ambient_home,
)

PROVIDER_CLAUDE = "claude"
PROVIDER_CODEX = "codex"
#: Внешний платный шлюз (этап 11J). Провайдер БЕЗ CLI: обращение к модели —
#: HTTPS-запрос из процесса конвейера, а не подпроцесс.
#:
#: Раскладка ему всё равно нужна, и по трём причинам сразу. Во-первых, учётные
#: данные обязаны лежать там же, где у остальных, — под 0700 в каталоге ДАННЫХ
#: воркера, а не в каталоге кода, чтобы обновление и откат их не трогали.
#: Во-вторых, `credential_facts` (режим файла, владелец, доступность группе и
#: миру) считается общим кодом по `credential_path`, и провайдер без раскладки
#: выпал бы из этой проверки целиком. В-третьих, `metadata/` держит соль
#: отпечатка учётной записи — единственный способ заметить, что ключ на этой
#: машине подменили.
#:
#: Чего у него нет: `runtime`-каталога в роли cwd подпроцесса (подпроцесса нет
#: вовсе) и `default_executable` (см. `OpenRouterProviderAdapter.installed`).
PROVIDER_OPENROUTER = "openrouter"

#: Порядок фиксирован: он же порядок вывода в heartbeat и в интерфейсе.
SUPPORTED_PROVIDERS: tuple[str, ...] = (
    PROVIDER_CLAUDE, PROVIDER_CODEX, PROVIDER_OPENROUTER,
)

#: Провайдеры, обращение к которым идёт по HTTP из процесса конвейера, а не
#: запуском CLI. Различие не косметическое: у них нет argv, нет окружения
#: подпроцесса и нет кода возврата — то есть инварианты I-P1…I-P8 к ним
#: неприменимы ДОСЛОВНО и заменяются собственными (см. `openrouter_adapter`).
HTTP_PROVIDERS: tuple[str, ...] = (PROVIDER_OPENROUTER,)


def is_http_provider(name: str) -> bool:
    return str(name or "").strip().lower() in HTTP_PROVIDERS

#: Режим каталогов provider home. 0700 — не «на всякий случай»: на этом VPS
#: живут посторонние сервисы, и единственная надёжная граница здесь — права ФС.
PROVIDER_DIR_MODE = 0o700


class UnknownProvider(ValueError):
    """Имя провайдера не из закрытого списка."""


def require_provider(name: str) -> str:
    text = str(name or "").strip().lower()
    if text not in SUPPORTED_PROVIDERS:
        raise UnknownProvider(
            f"неизвестный провайдер {name!r}; допустимы {SUPPORTED_PROVIDERS}"
        )
    return text


def providers_root(worker_root: Path) -> Path:
    return Path(worker_root) / "providers"


@dataclass(frozen=True)
class ProviderHome:
    """Пути одного провайдера. Значения вычислены, а не приняты извне.

    `auth_mode` меняет ровно одно: ЧЕЙ каталог служит HOME процессу CLI.
    `runtime` и `metadata` остаются во владении воркера в любом режиме —
    первый потому, что пустой cwd вне репозиториев нужен одинаково (см.
    шапку модуля про `codex app-server`), второй потому, что соль отпечатка
    учётной записи не имеет права лежать в личном каталоге человека.
    """

    provider: str
    root: Path
    #: Одно из `auth_mode.AUTH_MODES`. Умолчание = поведение этапа 11.
    auth_mode: str = DEFAULT_AUTH_MODE
    #: Личный каталог пользователя VPS. Заполняется ТОЛЬКО в ambient-режиме и
    #: только фабрикой `provider_home`; в остальных режимах обязан быть `None`,
    #: иначе «изолированный» режим тихо перестал бы быть изолированным.
    ambient_home: Optional[Path] = None

    def __post_init__(self) -> None:
        # Инвариант проверяется в конструкторе, а не в свойствах: свойство,
        # которое бросает, невозможно безопасно использовать в отчётах, а
        # именно там пути и читаются чаще всего.
        mode = normalize_auth_mode(self.auth_mode)
        object.__setattr__(self, "auth_mode", mode)
        if mode == AUTH_MODE_AMBIENT_USER:
            if self.ambient_home is None:
                raise ValueError(
                    "режим ambient_user требует ambient_home; "
                    "используйте provider_home(..., auth_mode=...)"
                )
            object.__setattr__(self, "ambient_home", Path(self.ambient_home))
        elif self.ambient_home is not None:
            raise ValueError(
                f"ambient_home задан при auth_mode={mode!r}: в этом режиме "
                "личный каталог пользователя не участвует"
            )

    @property
    def ambient(self) -> bool:
        """Работает ли CLI под личной авторизацией пользователя VPS."""
        return self.auth_mode == AUTH_MODE_AMBIENT_USER

    @property
    def home(self) -> Path:
        """HOME процесса CLI.

        В ambient-режиме это личный каталог человека. Всё, что ниже по коду
        обращается к `home`, обязано считать его ЧУЖИМ: читать `os.stat` можно,
        создавать и менять права — нельзя.
        """
        if self.ambient:
            if self.ambient_home is None:             # pragma: no cover
                # Не `assert`: под `python -O` он исчезает, и вместо громкой
                # ошибки получился бы `None` в пути к HOME процесса CLI.
                raise ValueError("ambient_user без ambient_home")
            return self.ambient_home
        return self.root / "home"

    @property
    def runtime(self) -> Path:
        """cwd подпроцессов: пустой каталог вне репозиториев и проектов."""
        return self.root / "runtime"

    @property
    def metadata(self) -> Path:
        """Наши собственные файлы: соль отпечатка, последний снимок квоты."""
        return self.root / "metadata"

    @property
    def credential_path(self) -> Path:
        """Файл учётных данных по ОФИЦИАЛЬНОЙ раскладке провайдера.

        Читается только `os.stat`: существование, режим, владелец. Содержимое
        не открывается ни разу — ни для проверки, ни для диагностики.

        Исключение ровно одно и оно осознанное: у OpenRouter ключ нужен САМОМУ
        вызову (он уходит в заголовок `Authorization`), и прочитать файл всё же
        придётся. Но делает это не этот модуль и не `identity`, а отдельный
        `openrouter_secret`, и только в момент запроса. Здесь по-прежнему
        только путь.
        """
        if self.provider == PROVIDER_CLAUDE:
            return self.config_dir / ".credentials.json"
        if self.provider == PROVIDER_OPENROUTER:
            return self.config_dir / "credentials.json"
        return self.config_dir / "auth.json"

    @property
    def config_dir(self) -> Path:
        """`CLAUDE_CONFIG_DIR` либо `CODEX_HOME` — то, что передаётся CLI.

        У OpenRouter передавать нечего: каталог существует только затем, чтобы
        ключ лежал в предсказуемом месте под теми же 0700, что и у остальных.
        """
        if self.provider == PROVIDER_CLAUDE:
            return self.home / ".claude"
        if self.provider == PROVIDER_OPENROUTER:
            return self.home / ".openrouter"
        return self.home / ".codex"

    @property
    def default_executable(self) -> Path:
        """Путь, по которому официальный установщик кладёт лаунчер при HOME=home.

        Claude Code: `$HOME/.local/bin/claude` → символьная ссылка в
        `$HOME/.local/share/claude/versions/<версия>`.
        Codex CLI: `$CODEX_INSTALL_DIR/codex` (по умолчанию `$HOME/.local/bin`)
        → ссылка в `$CODEX_HOME/packages/standalone/current/bin/codex`.
        """
        return self.home / ".local" / "bin" / self.provider

    def ensure_dirs(self) -> None:
        """Создать раскладку с узкими правами. Идемпотентно.

        `config_dir` входит в список НЕ для симметрии: документация Codex
        прямо требует «If you set it, the directory must already exist» про
        `CODEX_HOME`. Без этого на чистом воркере первый же `codex login`
        или `app-server` упал бы — притом что цитата этого требования стоит
        в шапке модуля. Для Claude создание `~/.claude` заранее безвредно.

        В ambient-режиме список СОКРАЩЁН до каталогов воркера, и это главный
        предохранитель всего режима. Оставь здесь `home` и `config_dir` —
        и `os.chmod(path, 0o700)` уехал бы на `/home/coder` и `~/.claude`
        живого человека: сначала закрыл бы его домашний каталог от группы
        (на этом VPS под соседними пользователями работает почтово-веб стек),
        а `mkdir` вдобавок создал бы `~/.codex` там, где оператор его,
        возможно, не заводил. Каталог, которым воркер не владеет, воркер не
        трогает — ни правами, ни созданием.
        """
        if self.ambient:
            managed: tuple[Path, ...] = (self.root, self.runtime, self.metadata)
        else:
            managed = (self.root, self.home, self.runtime, self.metadata,
                       self.config_dir)
        for path in managed:
            path.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(path, PROVIDER_DIR_MODE)
            except OSError:
                # Права могли быть выставлены заранее администратором VPS —
                # не повод падать. Фактический режим всё равно попадёт в
                # capability_snapshot и будет виден оператору.
                pass

    def as_public_dict(self) -> dict[str, object]:
        """Безопасное представление ДЛЯ ЦЕНТРА: без абсолютных путей.

        Абсолютный путь — не секрет, но он описывает раскладку чужой машины и
        имя её пользователя. Центру достаточно знать, что каталоги на месте.
        """
        return {
            "layout": (
                "ambient_user_home/{.claude|.codex}"
                if self.ambient
                else "worker_data_dir/providers/<provider>/{home,runtime,metadata}"
            ),
            "auth_mode": self.auth_mode,
            # В ambient-режиме это факт про ЧУЖОЙ каталог — «есть ли у CLI
            # куда смотреть». Абсолютного пути здесь нет ни в одном режиме.
            "home_exists": self.home.is_dir(),
            "home_owned_by_worker": not self.ambient,
            "runtime_exists": self.runtime.is_dir(),
            "metadata_exists": self.metadata.is_dir(),
        }


def provider_home(
    worker_root: Path,
    provider: str,
    *,
    auth_mode: Optional[str] = None,
    ambient_home: Optional[Path] = None,
) -> ProviderHome:
    """Собрать раскладку провайдера для заданного режима авторизации.

    Домашний каталог для ambient-режима вычисляется ЗДЕСЬ и один раз, чтобы
    ниже по коду не было ни одной ветки, которая берёт его из окружения.
    """
    name = require_provider(provider)
    mode = normalize_auth_mode(auth_mode)
    if mode == AUTH_MODE_AMBIENT_USER and is_http_provider(name):
        # `ambient_user` означает «CLI входит из личного каталога человека».
        # У провайдера без CLI входа нет, а раскладка при этом уехала бы в
        # `~/.openrouter/credentials.json` живого пользователя: воркер начал бы
        # тратить ключ, о котором его никто не просил, и который оператор
        # воркера не выдавал. Режим отвергается, а не игнорируется.
        raise ValueError(
            f"провайдер {name!r} не имеет CLI и не поддерживает режим "
            f"{AUTH_MODE_AMBIENT_USER!r}: ключ выдаётся воркеру отдельно, "
            "а не наследуется из личного каталога пользователя VPS"
        )
    if mode != AUTH_MODE_AMBIENT_USER and ambient_home is not None:
        # Тихо проигнорировать было бы худшим из вариантов: вызывающий явно
        # передал личный каталог и вправе считать, что CLI пойдёт туда. Молча
        # оставить его в изоляции значит расписаться в том, что аргумент
        # ничего не значит.
        raise ValueError(
            f"ambient_home передан при auth_mode={mode!r}: личный каталог "
            "участвует только в режиме ambient_user"
        )
    resolved = (
        resolve_ambient_home(ambient_home)
        if mode == AUTH_MODE_AMBIENT_USER
        else None
    )
    return ProviderHome(
        provider=name,
        root=providers_root(worker_root) / name,
        auth_mode=mode,
        ambient_home=resolved,
    )
