"""Аварийный запуск иммутабельного релиза БЕЗ systemd --user.

Применяется ТОЛЬКО когда user@<uid>.service мёртв (убит OOM-killer'ом) и поднять
его некому: из-под coder это невозможно, polkit требует интерактивной аутентификации.
Инцидент 23.08.2026 — портал лежал 12 ч 40 мин именно поэтому.

Повторяет ExecStart / WorkingDirectory / EnvironmentFile юнита
auditmanager-backend.service. Отличия от юнита (цена аварийного режима):
нет Restart=on-failure и нет sandbox-хардненинга (ProtectSystem/ProtectHome).

PID сохраняется в pid-файл ДО execv — execv не меняет PID, поэтому вотчдог
потом опознаёт аварийный процесс именно по этому файлу (после execv cmdline
становится неотличимой от systemd-запуска).

Вызывается через emergency-backend.sh, вручную запускать не нужно.
"""

import os
import sys

RELEASE = "/home/coder/auditmanager/current"
SECRETS_ENV = "/home/coder/.config/auditmanager/backend.secrets.env"
PHASEB_ENV = "/home/coder/.config/auditmanager/backend.phaseb.env"
PIDFILE = os.path.expanduser("~/.cloudflared/backend-fallback.pid")

# Переменные, которые НЕЛЬЗЯ протащить в бэкенд: аудит порождает дочерние
# раннеры `claude -p`, они наследуют окружение целиком. Если запуск случился
# из-под сессии Claude Code / VS Code, раннер сочтёт себя вложенной сессией,
# а CLAUDE_EFFORT перебьёт боевой режим рассуждения (напоролись 24.08).
CONTAMINATING_PREFIXES = ("CLAUDE", "CLAUDECODE", "VSCODE_", "ANTHROPIC_")


def scrub_environment():
    """Убирает следы интерактивной сессии, из которой мог быть вызван запуск."""
    dropped = []
    for key in list(os.environ):
        if key.startswith(CONTAMINATING_PREFIXES):
            del os.environ[key]
            dropped.append(key)
    if dropped:
        print(f"[emergency] вычищено переменных сессии: {len(dropped)}", flush=True)


def load_env(path):
    """Загружает systemd EnvironmentFile в os.environ. Значения не печатаются."""
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                val = val[1:-1]
            if key:
                os.environ[key] = val


def main():
    scrub_environment()

    # те же ExecStartPre-проверки, что и в юните
    if not os.path.islink(RELEASE):
        sys.exit(f"[emergency] {RELEASE} не симлинк — запуск отменён")
    if not os.path.isfile(os.path.join(RELEASE, "release-manifest.json")):
        sys.exit("[emergency] нет release-manifest.json — запуск отменён")

    load_env(SECRETS_ENV)
    load_env(PHASEB_ENV)
    os.environ["PYTHONUNBUFFERED"] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

    os.chdir(os.path.join(RELEASE, "app"))

    # execv сохраняет PID — записываем его до подмены образа процесса
    with open(PIDFILE, "w", encoding="utf-8") as fh:
        fh.write(f"{os.getpid()}\n")

    print(f"[emergency] АВАРИЙНЫЙ старт релиза мимо systemd, pid={os.getpid()}", flush=True)

    py = os.path.join(RELEASE, "venv/bin/python")
    os.execv(py, [py, "-m", "uvicorn", "backend.app.main:app",
                  "--host", "127.0.0.1", "--port", "8081"])


if __name__ == "__main__":
    main()
