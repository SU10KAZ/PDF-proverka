"""Поддельные CLI-провайдеры для автоматических прогонов.

Зачем они существуют. Проверять транспорт, изоляцию и приём результата на
НАСТОЯЩИХ Claude/Codex нельзя: это часы времени, деньги подписки и
недетерминированный ответ, по которому нечего сравнивать. Поэтому в тестовом
режиме воркер подставляет фиксированные исполняемые файлы с тем же минимальным
контрактом, что и настоящие CLI.

Чего они НЕ подменяют — оркестрацию. Этапы, их порядок, запись артефактов и
сборка результата идут настоящие: подделан только последний метр, где процесс
обращается к внешней модели. Подмена оркестрации превратила бы E2E в проверку
самой подделки.

Выбор режима — свойство КОНФИГУРАЦИИ ВОРКЕРА, а не поле задания (§17 задания):
центр не может попросить «а запусти-ка настоящий Claude» и наоборот.
"""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

#: Имена, под которыми конвейер ищет исполняемые файлы.
FAKE_BINARIES: tuple[str, ...] = ("claude", "codex")

#: Поведение подделки задаётся переменной окружения, которую ставит ТЕСТ, а не
#: задание: ok | rate_limit | auth_error | timeout | broken_json.
BEHAVIOUR_ENV = "AUDIT_WORKER_FAKE_BEHAVIOUR"

_SCRIPT = '''#!/usr/bin/env python3
"""Поддельный CLI-провайдер audit-worker. Сети не касается."""
import json
import os
import sys
import time

BEHAVIOUR = os.environ.get("{behaviour_env}", "ok").strip().lower()
NAME = os.path.basename(sys.argv[0])


def _read_prompt() -> str:
    if sys.stdin.isatty():
        return ""
    try:
        return sys.stdin.read()
    except Exception:
        return ""


def main() -> int:
    prompt = _read_prompt()
    if BEHAVIOUR == "rate_limit":
        sys.stderr.write("Claude usage limit reached. resets 11pm (Europe/Moscow)\\n")
        return 1
    if BEHAVIOUR == "auth_error":
        sys.stderr.write("Invalid API key / not logged in\\n")
        return 1
    if BEHAVIOUR == "timeout":
        time.sleep(float(os.environ.get("AUDIT_WORKER_FAKE_SLEEP_SEC", "3600")))
        return 0
    if BEHAVIOUR == "broken_json":
        sys.stdout.write('{{"result": "not a valid json')
        return 0
    payload = {{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "provider": NAME,
        "duration_ms": 5,
        "num_turns": 1,
        "total_cost_usd": 0.0,
        "usage": {{"input_tokens": len(prompt), "output_tokens": 16}},
        "result": json.dumps(
            {{
                "findings": [],
                "note": "поддельный провайдер: реальная модель не вызывалась",
                "prompt_sha_prefix": str(abs(hash(prompt)) % 10_000_000),
            }},
            ensure_ascii=False,
        ),
    }}
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''


def materialize(target_dir: Path) -> Path:
    """Создать каталог с поддельными CLI. Возвращает путь каталога.

    Файлы создаются каждый раз заново: подделка не должна «протухать» между
    версиями пакета воркера.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    body = _SCRIPT.format(behaviour_env=BEHAVIOUR_ENV)
    for name in FAKE_BINARIES:
        path = target_dir / name
        path.write_text(body, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    (target_dir / "PROVIDERS.json").write_text(
        json.dumps(
            {
                "mode": "fake",
                "binaries": list(FAKE_BINARIES),
                "python": sys.executable,
                "note": "Настоящие Claude/Codex этими файлами не вызываются.",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return target_dir


def looks_like_fake_dir(path: Path) -> bool:
    """Проверить, что каталог действительно содержит подделки, а не настоящие CLI."""
    path = Path(path)
    marker = path / "PROVIDERS.json"
    if not marker.is_file():
        return False
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    if data.get("mode") != "fake":
        return False
    return all((path / name).is_file() for name in FAKE_BINARIES)
