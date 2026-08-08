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

**Контракт соблюдается фактический, а не воображаемый.** Первый живой прогон
показал, чего стоит «примерно тот же формат»: подделка отдавала claude-конверт
на stdout для обоих бинарей, а `codex exec` пишет финальный ответ в файл из
`-o` и на stdout выдаёт JSONL. Stage 01 честно падал с `codex_json_not_found` на
каждом блоке. Поэтому здесь разбирается argv и воспроизводится ровно то, что
читают `claude_runner`/`codex_runner`.
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

#: Куда подделка пишет журнал своих вызовов. Нужен для доказательства «модель
#: вызывалась столько-то раз и вот с какими этапами», а также для сверки
#: расхода: usage-отчёт обязан сойтись с числом вызовов.
CALL_LOG_ENV = "AUDIT_WORKER_FAKE_CALL_LOG"

_SCRIPT = r'''#!/usr/bin/env python3
"""Поддельный CLI-провайдер audit-worker. Сети не касается.

Воспроизводит ФАКТИЧЕСКИЙ контракт двух настоящих CLI:

  claude -p … --output-format json   → JSON-конверт на stdout
  codex exec … -o <file> -           → финальный ответ в файл, JSONL на stdout

Полезная нагрузка выбирается по содержимому промпта: у этапов разные схемы
ответа, и один payload на всех давал бы «успех», который следующий этап не
может прочитать.
"""
import json
import os
import re
import sys
import time

BEHAVIOUR = os.environ.get("{behaviour_env}", "ok").strip().lower()
CALL_LOG = os.environ.get("{call_log_env}", "").strip()
NAME = os.path.basename(sys.argv[0])

#: Фиксированный расход. Детерминизм здесь принципиален: usage-отчёт воркера
#: сверяется центром, и «примерно столько-то токенов» сверить нельзя.
INPUT_TOKENS_PER_CALL = 1000
OUTPUT_TOKENS_PER_CALL = 16


def _read_prompt():
    if sys.stdin.isatty():
        return ""
    try:
        return sys.stdin.read()
    except Exception:
        return ""


def _argv_value(flag):
    argv = sys.argv[1:]
    for i, item in enumerate(argv):
        if item == flag and i + 1 < len(argv):
            return argv[i + 1]
        if item.startswith(flag + "="):
            return item.split("=", 1)[1]
    return None


#: Куда этапы просят записать результат. Настоящий CLI делает это инструментом
#: Write; подделка обязана уметь то же самое, иначе этап честно падает с
#: «<файл> не создан» — что и показал второй живой прогон.
#:
#: Ищем не по одной фразе, а по строкам, где есть слово Write и путь в обратных
#: кавычках: формулировка живёт в промптах, промпты редактируются из UI, а часть
#: из них уходит в модель ПО-АНГЛИЙСКИ («WRITE via Write tool: `…`») — привязка
#: к русской фразе ловила ровно ноль вызовов.
_PATH_RE = re.compile(r"`([^`\n]+\.json)`")


def _write_targets(prompt):
    out = []
    for line in (prompt or "").splitlines():
        if "write" not in line.lower():
            continue
        for match in _PATH_RE.finditer(line):
            candidate = match.group(1).strip()
            if not candidate.startswith("/"):
                continue                      # относительный путь — не адрес записи
            if candidate not in out:
                out.append(candidate)
    return out


def _payload_for(name):
    """Минимальный, но СХЕМНО ВАЛИДНЫЙ ответ этапа — по ИМЕНИ артефакта.

    По имени файла, а не по ключевым словам промпта: имя однозначно, а
    формулировки промптов правятся из UI и разъезжаются молча.

    Пустые коллекции выбраны намеренно: E2E проверяет транспорт, изоляцию и
    приём результата, а не качество аудита. Выдуманные замечания сделали бы
    семантическое сравнение local/remote проверкой генератора случайных чисел.
    """
    base = os.path.basename(name or "")
    if base == "02_text_analysis.json":
        return {{
            "text_findings": [],
            "project_params": {{}},
            "normative_refs_found": [],
            "items_verified_from_blocks": [],
        }}
    if base == "03_findings.json":
        return {{"findings": []}}
    if base == "03_findings_review.json":
        return {{"verdicts": [], "findings": []}}
    if base == "optimization.json":
        return {{"optimizations": []}}
    if base == "optimization_review.json":
        return {{"verdicts": [], "optimizations": []}}
    if base.startswith("block_batch_"):
        return {{"findings": []}}
    return {{"findings": []}}


def _classify(prompt):
    """Что именно у нас просят — для журнала вызовов."""
    targets = _write_targets(prompt)
    if targets:
        return os.path.basename(targets[0])
    return "inline_json"


def _payload(kind):
    return _payload_for(kind)


def _log_call(kind, prompt):
    if not CALL_LOG:
        return
    # Полный промпт рядом с журналом: диагностика «почему этап не получил то,
    # что просил» без него превращается в гадание.
    try:
        dump_dir = CALL_LOG + ".prompts"
        os.makedirs(dump_dir, exist_ok=True)
        with open(os.path.join(dump_dir, "%s-%d.txt" % (NAME, os.getpid())), "w",
                  encoding="utf-8") as fh:
            fh.write(prompt or "")
    except OSError:
        pass
    try:
        with open(CALL_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({{
                "provider": NAME,
                "kind": kind,
                "argv": sys.argv[1:],
                "prompt_bytes": len(prompt or ""),
                "prompt_head": (prompt or "")[:600],
                "prompt_tail": (prompt or "")[-600:],
                "write_targets": _write_targets(prompt),
                "input_tokens": INPUT_TOKENS_PER_CALL,
                "output_tokens": OUTPUT_TOKENS_PER_CALL,
                "at": 0,
            }}, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _fail_rate_limit():
    # Формат, который распознаёт cli_utils.is_rate_limited + parse_rate_limit_reset.
    sys.stderr.write("Claude usage limit reached. resets 11pm (Europe/Moscow)\n")
    return 1


def _fail_auth():
    sys.stderr.write("Invalid API key / not logged in\n")
    return 1


def main():
    prompt = _read_prompt()
    kind = _classify(prompt)

    if BEHAVIOUR == "rate_limit":
        _log_call(kind, prompt)
        return _fail_rate_limit()
    if BEHAVIOUR == "auth_error":
        _log_call(kind, prompt)
        return _fail_auth()
    if BEHAVIOUR == "timeout":
        time.sleep(float(os.environ.get("AUDIT_WORKER_FAKE_SLEEP_SEC", "3600")))
        return 0

    targets = _write_targets(prompt)
    body = _payload_for(targets[0] if targets else "")
    body_text = json.dumps(body, ensure_ascii=False)
    if BEHAVIOUR == "broken_json":
        body_text = '{{"findings": [ '        # намеренно оборванный JSON

    # Эмуляция инструмента Write настоящего CLI. Пишем ровно те файлы, которые
    # промпт назвал явно, и ничего сверх: подделка не имеет права создавать
    # артефакты, о которых её не просили.
    for target in targets:
        payload_text = body_text if target == targets[0] else json.dumps(
            _payload_for(target), ensure_ascii=False
        )
        try:
            directory = os.path.dirname(target)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(target, "w", encoding="utf-8") as fh:
                fh.write(payload_text)
        except OSError as exc:
            sys.stderr.write("fake %s: не смог записать %s: %s\n" % (NAME, target, exc))
            return 1

    _log_call(kind, prompt)

    out_file = _argv_value("-o") or _argv_value("--output-last-message")
    if out_file:
        # Путь codex: финальный ответ уходит В ФАЙЛ, на stdout — JSONL событий.
        try:
            with open(out_file, "w", encoding="utf-8") as fh:
                fh.write(body_text)
        except OSError as exc:
            sys.stderr.write("fake codex: не смог записать %s: %s\n" % (out_file, exc))
            return 1
        sys.stdout.write(json.dumps({{
            "type": "token_count",
            "info": {{
                "total_token_usage": {{
                    "input_tokens": INPUT_TOKENS_PER_CALL,
                    "output_tokens": OUTPUT_TOKENS_PER_CALL,
                    "cached_input_tokens": 0,
                }}
            }},
        }}, ensure_ascii=False) + "\n")
        sys.stdout.write(json.dumps({{
            "type": "item.completed",
            "item": {{"type": "agent_message", "text": body_text}},
        }}, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        return 0

    # Путь claude: JSON-конверт целиком на stdout.
    sys.stdout.write(json.dumps({{
        "type": "result",
        "subtype": "success",
        "is_error": False,
        "provider": NAME,
        "duration_ms": 5,
        "duration_api_ms": 5,
        "num_turns": 1,
        "total_cost_usd": 0.0,
        "usage": {{
            "input_tokens": INPUT_TOKENS_PER_CALL,
            "output_tokens": OUTPUT_TOKENS_PER_CALL,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 0,
        }},
        "result": body_text,
    }}, ensure_ascii=False))
    sys.stdout.flush()
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
    body = _SCRIPT.format(behaviour_env=BEHAVIOUR_ENV, call_log_env=CALL_LOG_ENV)
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


def read_call_log(path: Path) -> list[dict]:
    """Журнал вызовов подделок. Пустой список = модель не звали ни разу."""
    path = Path(path)
    if not path.is_file():
        return []
    entries: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except ValueError:
            continue
    return entries
