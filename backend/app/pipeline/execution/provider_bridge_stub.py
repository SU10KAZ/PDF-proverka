"""Заглушка Claude CLI для сетевых прогонов ЧЕРЕЗ МОСТ провайдера (этап 11G).

Чем она отличается от `fake_providers` и почему их две.

`fake_providers` подменяет ПУТЬ К МОДЕЛИ ЦЕЛИКОМ: воркер объявляет центру
`provider_mode="fake"`, требования к провайдеру не получает, привязку не
выписывает, мост не активирует. Это правильный инструмент для проверки
транспорта — и совершенно негодный для 11G, где доказывать надо ровно то, что
он выключает: что центр прислал требование, что воркер разрешил способность в
модель СВОЕЙ политикой, что привязка и разрешение возникли штатно.

Заглушка решает обратную задачу. Весь механизм остаётся боевым: `provider_mode
= "real"`, `ProviderResolver`, `inference_grant`, `ProviderBinding`,
`inference_ledger`, `ProviderAdapter`, argv с `--model`. Подделан РОВНО
последний метр — сам бинарь, который вместо обращения к модели отвечает
фиксированной структурой. Обращений к сети ноль, расхода подписки ноль.

Отсюда требование к контракту: заглушка обязана воспроизводить не «примерно
такой JSON», а ФАКТИЧЕСКИЕ ответы четырёх разных вызовов настоящего бинаря,
потому что все четыре делает боевой код:

    claude --version                       → строка версии
    claude auth status                     → JSON с `loggedIn`
    claude -p … --output-format json       → один JSON-конверт
    claude -p … --input-format stream-json → NDJSON, последним `type: result`
                --output-format stream-json

Полезная нагрузка выбирается по МАРКЕРУ ЭТАПА в промпте: у этапов разные схемы
ответа, и один payload на всех дал бы «успех», который следующий этап прочитать
не может. Логика разбора маркеров перенесена из диагностического харнеса 11F
без изменений — она уже проведена через весь worker-участок.

Заглушка инертна: пока администратор VPS не укажет на неё
`AUDIT_WORKER_PROVIDER_CLAUDE_EXECUTABLE`, её никто не вызывает. Центр
подменить путь к бинарю не может — это свойство машины (I-P5).
"""
from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

#: Куда заглушка пишет журнал своих вызовов. Доказательство «модель не звали»
#: и одновременно счётчик: число записей обязано сойтись с журналом вызовов
#: попытки (`inference_ledger`).
CALL_LOG_ENV = "AUDIT_PROVIDER_STUB_CALL_LOG"

#: Какой моделью заглушка ПРЕДСТАВЛЯЕТСЯ. Значение обязано входить в
#: `accepted_reported_models` локальной политики, иначе адаптер честно
#: отвергнет ответ как несовпадение модели — и это правильное поведение,
#: которое 11G тоже проверяет отдельным сценарием.
MODEL_ENV = "AUDIT_PROVIDER_STUB_MODEL"

#: Маркер каталога. Без него «мы подставили заглушку» — утверждение о
#: намерении, а не о факте: точно так же, как `PROVIDERS.json` у подделок.
MARKER_NAME = "PROVIDER_STUB.json"

STUB_NAME = "claude"

_SCRIPT = r'''#!/usr/bin/env python3
"""Заглушка Claude CLI для сетевого прогона через мост. Сети не касается."""
import json
import os
import re
import sys
import time

CALL_LOG = os.environ.get("{call_log_env}", "")
MODEL = os.environ.get("{model_env}", "claude-opus-5")

argv = sys.argv[1:]


def _log(kind, extra=None):
    if not CALL_LOG:
        return
    row = {{"ts": time.time(), "kind": kind, "argv": argv}}
    row.update(extra or {{}})
    try:
        os.makedirs(os.path.dirname(CALL_LOG) or ".", exist_ok=True)
        with open(CALL_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ─── 1. Версия ───────────────────────────────────────────────────────────────
if "--version" in argv:
    _log("version")
    sys.stdout.write("2.1.220 (Claude Code)\n")
    sys.exit(0)

# ─── 2. Состояние авторизации ────────────────────────────────────────────────
if argv[:2] == ["auth", "status"] or (len(argv) >= 2 and argv[-2:] == ["auth", "status"]):
    _log("auth")
    sys.stdout.write(json.dumps({{
        "loggedIn": True,
        "authMethod": "claude.ai",
        "apiProvider": "firstParty",
        "account": {{"emailAddress": "stub@example.invalid"}},
    }}, ensure_ascii=False))
    sys.exit(0)

# ─── 3. Обращение «к модели» ─────────────────────────────────────────────────
stream = "stream-json" in argv
prompt = sys.stdin.read()

# Модальность определяется форматом ввода, а не догадкой: изображение приходит
# ТОЛЬКО строкой stream-json с content-блоком type=image.
images = 0
text = prompt
if stream:
    try:
        msg = json.loads(prompt.strip().splitlines()[0])
        content = (msg.get("message") or {{}}).get("content") or []
        images = sum(1 for c in content if c.get("type") == "image")
        text = "\n".join(c.get("text", "") for c in content if c.get("type") == "text")
    except Exception:
        pass


def payload_for(body):
    """Схема ответа выбирается по МАРКЕРУ ЭТАПА, а не по угадыванию.

    Порядок проверок содержателен: в промпт текстового анализа вкладывается
    блочный контекст, и он содержит слово "findings". Заглушка, решавшая по
    подстроке, отдавала бы текстовому этапу схему блока — то есть ломала бы
    прогон там, где конвейер исправен.
    """
    if "SOURCE DOCUMENT (inlined by the pipeline)" in body:
        return {{
            "stage": "text_analysis",
            "text_source": "md",
            "project_params": {{}},
            "text_findings": [{{
                "id": "T-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "заглушка",
                "finding": "Ответ заглушки CLI: обращения к модели не было",
                "sheet": None, "page": None,
            }}],
            "normative_refs_found": [],
            "items_verified_from_blocks": [],
        }}
    if "findings_merge" in body or "СВОД ЗАМЕЧАНИЙ" in body.upper() or "MERGE" in body.upper():
        return {{
            "meta": {{"total_findings": 1}},
            "findings": [{{
                "id": "F-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "заглушка",
                "sheet": None, "page": None,
                "problem": "Отсутствует тестовая маркировка на синтетическом листе",
                "description": (
                    "Детерминированный absence-кандидат заглушки; "
                    "обращения к реальной модели не было"
                ),
                "norm": None, "norm_quote": None,
                "solution": "—", "risk": "—",
                "source_finding_ids": ["T-001", "G-001"],
                "source_block_ids": [], "related_block_ids": [],
                "evidence_text_refs": [], "evidence": [], "highlight_regions": [],
            }}],
        }}
    # Остальные этапы объявляют корневой ключ ПРЯМО В КОНТРАКТЕ ответа.
    # Читаем его оттуда: угадывание уже один раз обмануло репетицию 11F.
    match = re.search(r'Объект обязан содержать ключ "([A-Za-z_]+)"', body)
    if match:
        return {{match.group(1): [{{
            "id": "OPT-001",
            "title": "Ответ заглушки CLI: обращения к модели не было",
            "description": "Сетевой прогон 11G через мост провайдера",
            "category": "заглушка",
            "verdict": "accepted",
        }}]}}
    return {{"findings": []}}


answer = payload_for(text)
if images:
    answer = {{"findings": [{{
        "id": "G-001",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "заглушка",
        "finding": "Ответ заглушки CLI по приложенному изображению (%d шт.)" % images,
        "value_found": "",
        "norm_quote": None,
    }}]}}
answer_text = json.dumps(answer, ensure_ascii=False)
_log("inference", {{"stream": stream, "images": images, "prompt_chars": len(prompt)}})

usage = {{
    "input_tokens": 1000, "output_tokens": 16,
    "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0,
}}
envelope = {{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer_text, "usage": usage, "total_cost_usd": 0.0,
    "modelUsage": {{MODEL: {{"outputTokens": 16}}}},
}}
if stream:
    sys.stdout.write(json.dumps({{
        "type": "assistant",
        "message": {{
            "model": MODEL,
            "content": [{{"type": "text", "text": answer_text}}],
            "usage": usage,
        }},
    }}, ensure_ascii=False) + "\n")
    sys.stdout.write(json.dumps(envelope, ensure_ascii=False) + "\n")
else:
    sys.stdout.write(json.dumps(envelope, ensure_ascii=False))
sys.exit(0)
'''


#: Имя бинаря Codex-заглушки. Отдельный файл, а не ветка внутри claude-скрипта:
#: адаптеры запускают их по РАЗНЫМ путям (у каждого свой
#: `AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE`), и «один файл на двоих» означало бы,
#: что подстановка одного провайдера молча меняет поведение другого.
CODEX_STUB_NAME = "codex"

_CODEX_SCRIPT = r'''#!/usr/bin/env python3
"""Заглушка Codex CLI для сетевого прогона через мост. Сети не касается.

Воспроизводит ЧЕТЫРЕ фактических контракта настоящего `codex` 0.147.0, потому
что все четыре делает боевой код `CodexProviderAdapter`:

    codex --version                    → "codex-cli 0.147.0"
    codex login status                 → строка + код возврата 0
    codex app-server                   → JSON-RPC по stdio: account/read,
                                         account/rateLimits/read
    codex exec --json … -              → JSONL: thread.started (МОДЕЛЬ),
                                         item.completed (ответ), turn.completed
                                         (usage)

Модальность здесь определяется НЕ форматом ввода (как у Claude), а наличием
флагов `--image=…`: у `codex exec` изображение приходит файлом. Заглушка эти
файлы ЧИТАЕТ и считает их размер — иначе «мультимодальный вызов прошёл» не
означало бы, что вложение вообще существовало и было доступно процессу CLI.
"""
import json
import os
import re
import sys
import time

CALL_LOG = os.environ.get("{call_log_env}", "")
MODEL = os.environ.get("{model_env}", "gpt-5.6-sol")

argv = sys.argv[1:]


def _log(kind, extra=None):
    if not CALL_LOG:
        return
    row = {{"ts": time.time(), "kind": kind, "argv": argv}}
    row.update(extra or {{}})
    try:
        os.makedirs(os.path.dirname(CALL_LOG) or ".", exist_ok=True)
        with open(CALL_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError:
        pass


# ─── 1. Версия ───────────────────────────────────────────────────────────────
if "--version" in argv or "-V" in argv:
    _log("version")
    sys.stdout.write("codex-cli 0.147.0\n")
    sys.exit(0)

# ─── 2. Состояние авторизации ────────────────────────────────────────────────
if argv[:2] == ["login", "status"]:
    _log("auth")
    sys.stdout.write("Logged in using ChatGPT\n")
    sys.exit(0)

# ─── 3. app-server: JSON-RPC по stdio (0 обращений к модели) ─────────────────
if argv[:1] == ["app-server"]:
    _log("app_server")
    now = int(time.time())
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            continue
        request_id = message.get("id")
        method = message.get("method")
        if request_id is None:
            continue
        if method == "initialize":
            result = {{"userAgent": "codex-stub/0.147.0"}}
        elif method == "account/read":
            result = {{
                "account": {{"type": "chatgpt", "planType": "pro",
                             "email": "stub@example.invalid"}},
                "requiresOpenaiAuth": True,
            }}
        elif method == "account/rateLimits/read":
            result = {{"rateLimits": {{
                "limitId": "codex-stub",
                "primary": {{"usedPercent": 1.0, "windowDurationMins": 10080,
                             "resetsAt": now + 604800}},
            }}}}
        else:
            continue
        sys.stdout.write(json.dumps({{"id": request_id, "result": result}},
                                    ensure_ascii=False) + "\n")
        sys.stdout.flush()
    sys.exit(0)

# ─── 4. Обращение «к модели» ─────────────────────────────────────────────────
images = []
for arg in argv:
    if arg.startswith("--image="):
        images.append(arg[len("--image="):])
    elif arg.startswith("-i="):
        images.append(arg[len("-i="):])

# Вложения ЧИТАЮТСЯ: сам факт, что путь дошёл до процесса CLI и файл открылся,
# и есть предмет проверки. Недоступное вложение — отказ с кодом 1, а не тихий
# текстовый ответ: молчаливая деградация здесь означала бы «анализ чертежа без
# чертежа», ровно то, что мост запрещает.
image_bytes = 0
for path in images:
    try:
        with open(path, "rb") as fh:
            image_bytes += len(fh.read())
    except OSError as exc:
        _log("inference_error", {{"reason": "attachment_unreadable", "detail": str(exc)}})
        sys.stderr.write("stub: attachment unreadable: %s\n" % exc)
        sys.exit(1)

# Заглушка ПРЕДСТАВЛЯЕТСЯ моделью из своего окружения, а не той, которую
# попросили. Эхо запрошенного значения сделало бы сверку модели тождественно
# истинной: мы проверяли бы, что наша же строка вернулась обратно, и сценарий
# «CLI молча ответил другой моделью» стал бы непроверяемым.
model = MODEL
requested_model = ""
for arg in argv:
    if arg.startswith("--model="):
        requested_model = arg[len("--model="):]

prompt = sys.stdin.read()


def payload_for(body):
    """Схема ответа выбирается по МАРКЕРУ ЭТАПА — как в claude-заглушке."""
    if "SOURCE DOCUMENT (inlined by the pipeline)" in body:
        return {{
            "stage": "text_analysis",
            "text_source": "md",
            "project_params": {{}},
            "text_findings": [{{
                "id": "T-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "заглушка",
                "finding": "Ответ заглушки Codex CLI: обращения к модели не было",
                "sheet": None, "page": None,
            }}],
            "normative_refs_found": [],
            "items_verified_from_blocks": [],
        }}
    if "findings_merge" in body or "СВОД ЗАМЕЧАНИЙ" in body.upper() or "MERGE" in body.upper():
        return {{
            "meta": {{"total_findings": 1}},
            "findings": [{{
                "id": "F-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "заглушка",
                "sheet": None, "page": None,
                "problem": "Отсутствует тестовая маркировка на синтетическом листе",
                "description": (
                    "Детерминированный absence-кандидат Codex-заглушки; "
                    "обращения к реальной модели не было"
                ),
                "norm": None, "norm_quote": None,
                "solution": "—", "risk": "—",
                "source_finding_ids": ["T-001", "G-001"],
                "source_block_ids": [], "related_block_ids": [],
                "evidence_text_refs": [], "evidence": [], "highlight_regions": [],
            }}],
        }}
    match = re.search(r'Объект обязан содержать ключ "([A-Za-z_]+)"', body)
    if match:
        return {{match.group(1): [{{
            "id": "OPT-001",
            "title": "Ответ заглушки Codex CLI: обращения к модели не было",
            "description": "Сетевой прогон 11H через мост провайдера",
            "category": "заглушка",
            "verdict": "accepted",
        }}]}}
    return {{"findings": []}}


answer = payload_for(prompt)
if images:
    answer = {{"findings": [{{
        "id": "G-001",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "заглушка",
        "finding": "Ответ заглушки Codex по вложению (%d шт., %d байт)" % (
            len(images), image_bytes),
        "value_found": "",
        "norm_quote": None,
    }}]}}
answer_text = json.dumps(answer, ensure_ascii=False)
_log("inference", {{"images": len(images), "image_bytes": image_bytes,
                   "prompt_chars": len(prompt), "model": model,
                   "requested_model": requested_model}})

# Порядок событий — как у настоящего `codex exec --json`: модель объявляется в
# служебном событии начала нити, ответ приходит элементом, расход — в итоге.
sys.stdout.write(json.dumps({{
    "type": "codex.thread.started",
    "thread": {{"thread_id": "stub-thread", "model": model}},
}}, ensure_ascii=False) + "\n")
sys.stdout.write(json.dumps({{
    "type": "item.completed",
    "item": {{"type": "agent_message", "text": answer_text}},
}}, ensure_ascii=False) + "\n")
sys.stdout.write(json.dumps({{
    "type": "turn.completed",
    "usage": {{"input_tokens": 1000, "output_tokens": 16,
              "cached_input_tokens": 0, "reasoning_output_tokens": 0}},
}}, ensure_ascii=False) + "\n")
sys.exit(0)
'''


def materialize(target_dir: Path, *, provider: str = "claude") -> Path:
    """Положить заглушку в каталог и вернуть путь к самому файлу.

    Возвращается путь БИНАРЯ, а не каталога: заглушка подставляется явным
    указанием администратора (`AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE`), а не
    префиксом PATH. Разница существенна — поиска по PATH в адаптере нет
    намеренно, и «положили в каталог» ничего бы не изменило.

    `provider` появился на 11H вместе с Codex-заглушкой. Умолчание `claude`
    сохраняет прежние вызовы дословно.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    if provider == "codex":
        name, script = CODEX_STUB_NAME, _CODEX_SCRIPT
    elif provider == "claude":
        name, script = STUB_NAME, _SCRIPT
    else:
        raise ValueError(f"нет заглушки для провайдера {provider!r}")
    path = target_dir / name
    path.write_text(
        script.format(call_log_env=CALL_LOG_ENV, model_env=MODEL_ENV),
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    (target_dir / MARKER_NAME).write_text(
        json.dumps(
            {
                "mode": "provider_bridge_stub",
                "binary": name,
                "python": sys.executable,
                "note": (
                    "Мост провайдера боевой; подделан только сам бинарь. "
                    "Обращений к модели и к сети не происходит."
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def looks_like_stub(path: Path) -> bool:
    """Это действительно заглушка, а не настоящий CLI.

    Проверяется КАТАЛОГ бинаря по маркеру, а не имя файла: `claude` называется
    и настоящий бинарь, и «заглушка, которую мы вроде бы туда положили» — а
    прогон, объявивший ноль обращений к модели, обязан это доказать, а не
    предположить.
    """
    path = Path(path)
    marker = path.parent / MARKER_NAME
    if not (path.is_file() and marker.is_file()):
        return False
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return data.get("mode") == "provider_bridge_stub"


def read_call_log(path: Path) -> list[dict]:
    """Журнал вызовов заглушки. Пустой список — заглушку не звали ни разу."""
    path = Path(path)
    if not path.is_file():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows
