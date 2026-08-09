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
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROVIDER_CLAUDE = "claude"
PROVIDER_CODEX = "codex"

#: Порядок фиксирован: он же порядок вывода в heartbeat и в интерфейсе.
SUPPORTED_PROVIDERS: tuple[str, ...] = (PROVIDER_CLAUDE, PROVIDER_CODEX)

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
    """Пути одного провайдера. Значения вычислены, а не приняты извне."""

    provider: str
    root: Path

    @property
    def home(self) -> Path:
        """HOME процесса CLI."""
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
        """
        if self.provider == PROVIDER_CLAUDE:
            return self.config_dir / ".credentials.json"
        return self.config_dir / "auth.json"

    @property
    def config_dir(self) -> Path:
        """`CLAUDE_CONFIG_DIR` либо `CODEX_HOME` — то, что передаётся CLI."""
        if self.provider == PROVIDER_CLAUDE:
            return self.home / ".claude"
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
        """
        for path in (self.root, self.home, self.runtime, self.metadata,
                     self.config_dir):
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
            "layout": "worker_data_dir/providers/<provider>/{home,runtime,metadata}",
            "home_exists": self.home.is_dir(),
            "runtime_exists": self.runtime.is_dir(),
            "metadata_exists": self.metadata.is_dir(),
        }


def provider_home(worker_root: Path, provider: str) -> ProviderHome:
    name = require_provider(provider)
    return ProviderHome(provider=name, root=providers_root(worker_root) / name)
