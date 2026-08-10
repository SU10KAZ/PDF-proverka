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
                "problem": "Ответ заглушки CLI: обращения к модели не было",
                "description": "Сетевой прогон 11G через мост провайдера",
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


def materialize(target_dir: Path) -> Path:
    """Положить заглушку в каталог и вернуть путь к самому файлу.

    Возвращается путь БИНАРЯ, а не каталога: заглушка подставляется явным
    указанием администратора (`AUDIT_WORKER_PROVIDER_CLAUDE_EXECUTABLE`), а не
    префиксом PATH. Разница существенна — поиска по PATH в адаптере нет
    намеренно, и «положили в каталог» ничего бы не изменило.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / STUB_NAME
    path.write_text(
        _SCRIPT.format(call_log_env=CALL_LOG_ENV, model_env=MODEL_ENV),
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    (target_dir / MARKER_NAME).write_text(
        json.dumps(
            {
                "mode": "provider_bridge_stub",
                "binary": STUB_NAME,
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
