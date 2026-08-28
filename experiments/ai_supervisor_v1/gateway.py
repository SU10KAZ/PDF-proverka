"""AI Session Gateway — единственная точка запуска моделей.

Проект НЕ ходит в HTTP-API моделей. Разрешены ровно два транспорта:
  CLAUDE_SESSION — `claude -p` по подписке Claude Code
  CODEX_SESSION  — `codex exec` по подписке ChatGPT

Шлюз отвечает за: выбор модели, уровень рассуждения, изоляцию (модель не должна
иметь права трогать файлы и shell), таймаут, разбор структурированного вывода,
журнал попыток. Оркестратор конвейера сам CLI не запускает.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path

CODEX_BIN = os.environ.get(
    "AI_SUPERVISOR_CODEX_BIN",
    "/home/coder/.vscode-server/extensions/openai.chatgpt-26.825.32147-linux-x64/bin/linux-x86_64/codex",
)
CLAUDE_BIN = os.environ.get("AI_SUPERVISOR_CLAUDE_BIN", "claude")

# Рабочий каталог сессии: пустой временный каталог, не репозиторий.
# Так модель не подхватывает CLAUDE.md/AGENTS.md проекта и не видит исходники.
SESSION_CWD = os.environ.get("AI_SUPERVISOR_SESSION_CWD", "/tmp")


@dataclass
class CallResult:
    provider: str
    model: str
    effort: str | None
    ok: bool
    parsed: dict | None = None
    raw: str = ""
    error: str = ""
    duration_s: float = 0.0
    exit_code: int | None = None
    session_id: str | None = None
    usage: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["raw"] = self.raw[:4000]
        return d


class GatewayError(RuntimeError):
    pass


# Переменные окружения, которые не должны утечь в сессию модели.
_STRIP_ENV_PREFIXES = ("ANTHROPIC_", "OPENAI_", "OPENROUTER_", "AWS_", "GOOGLE_", "GEMINI_")
_STRIP_ENV_EXACT = ("CLAUDE_CODE_SSE_PORT", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT")


def _clean_env() -> dict:
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith(_STRIP_ENV_PREFIXES) and k not in _STRIP_ENV_EXACT
    }
    env.setdefault("LANG", "ru_RU.UTF-8")
    return env


def _extract_json(text: str) -> dict | None:
    """Достать объект JSON из вывода. Сначала честный разбор, затем скобки.

    Регулярные выражения по свободному тексту как основной контракт запрещены:
    сюда попадаем, только если нативный структурированный вывод не сработал.
    """
    text = (text or "").strip()
    if not text:
        return None
    try:
        val = json.loads(text)
        return val if isinstance(val, dict) else None
    except json.JSONDecodeError:
        pass
    # ограждение ```json ... ```
    if "```" in text:
        for chunk in text.split("```"):
            chunk = chunk.strip()
            if chunk.startswith("json"):
                chunk = chunk[4:].strip()
            if chunk.startswith("{"):
                try:
                    val = json.loads(chunk)
                    if isinstance(val, dict):
                        return val
                except json.JSONDecodeError:
                    continue
    # последний сбалансированный объект
    start = text.find("{")
    while start != -1:
        depth, in_str, esc = 0, False, False
        for i in range(start, len(text)):
            c = text[i]
            if in_str:
                if esc:
                    esc = False
                elif c == "\\":
                    esc = True
                elif c == '"':
                    in_str = False
                continue
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        val = json.loads(text[start:i + 1])
                        if isinstance(val, dict):
                            return val
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)
    return None


# ── Claude ──────────────────────────────────────────────────────────────────

def call_claude(
    prompt: str,
    *,
    model: str,
    schema: dict | None = None,
    effort: str | None = None,
    timeout_s: int = 300,
    system_prompt: str | None = None,
) -> CallResult:
    """Один изолированный вызов Claude без права трогать файловую систему."""
    # Изоляция и цена — одно и то же решение. Замер: обычный вызов стоит 33 802
    # входных токена (описания инструментов + дефолтный системный промпт), тот же
    # вызов с `--tools ""` + собственным `--system-prompt` + `--setting-sources ""`
    # стоит 240. Разница ×140. Убирать надо ОБА источника: по отдельности каждый
    # оставляет 6,5-26 тыс. токенов. На 1720 элементов это 58 млн токенов против
    # 0,4 млн — то есть без этих флагов слой просто нежизнеспособен.
    cmd = [
        CLAUDE_BIN, "-p", prompt,
        "--model", model,
        "--output-format", "json",
        "--tools", "",                 # инструментов физически нет: init отдаёт tools: []
        "--setting-sources", "",       # без ~/.claude/settings.json и его allow-правил
        "--strict-mcp-config",         # без внешних MCP-серверов
        "--system-prompt", system_prompt or "Ты — точный классификатор. Отвечай строго по схеме.",
        "--no-session-persistence",
        "--disable-slash-commands",
    ]
    if schema is not None:
        cmd += ["--json-schema", json.dumps(schema, ensure_ascii=False)]
    if effort:
        cmd += ["--effort", effort]

    t0 = time.time()
    try:
        # stdin обязателен: без него CLI ждёт ввод 3 секунды и печатает Warning
        # прямо в поток. stderr держим отдельно — предупреждения ломают разбор JSON.
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout_s,
            cwd=SESSION_CWD, env=_clean_env(), input="",
        )
    except subprocess.TimeoutExpired:
        return CallResult("CLAUDE_SESSION", model, effort, False,
                          error=f"timeout {timeout_s}s", duration_s=time.time() - t0)
    dt = time.time() - t0

    out = proc.stdout or ""
    envelope = _extract_json(out)
    usage, session_id, payload = {}, None, None
    if envelope and "result" in envelope:
        usage = envelope.get("usage") or {}
        session_id = envelope.get("session_id")
        if envelope.get("is_error"):
            return CallResult("CLAUDE_SESSION", model, effort, False, raw=out,
                              error=str(envelope.get("result"))[:500], duration_s=dt,
                              exit_code=proc.returncode, session_id=session_id, usage=usage)
        # structured_output — уже распарсенный объект; result — та же строка текстом.
        payload = envelope.get("structured_output") or _extract_json(str(envelope.get("result")))
        usage["total_input_tokens"] = sum(
            usage.get(k) or 0 for k in
            ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
        )
    else:
        payload = envelope

    if payload is None:
        return CallResult("CLAUDE_SESSION", model, effort, False, raw=out or proc.stderr,
                          error="не удалось получить JSON из ответа", duration_s=dt,
                          exit_code=proc.returncode, session_id=session_id, usage=usage)

    return CallResult("CLAUDE_SESSION", model, effort, True, parsed=payload, raw=out,
                      duration_s=dt, exit_code=proc.returncode, session_id=session_id, usage=usage)


# ── Codex ───────────────────────────────────────────────────────────────────

_CODEX_TRANSIENT = ("at capacity", "overloaded", "rate limit", "too many requests",
                    "temporarily unavailable", "503", "502")


def call_codex(
    prompt: str,
    *,
    model: str,
    schema: dict | None = None,
    effort: str | None = None,
    timeout_s: int = 420,
    images: list[str] | None = None,
    retries: int = 1,
) -> CallResult:
    """Один изолированный вызов Codex. Песочница только для чтения, без сети правок."""
    workdir = Path(tempfile.mkdtemp(prefix="ai_sup_codex_"))
    try:
        out_file = workdir / "last_message.txt"
        cmd = [
            CODEX_BIN, "exec",
            "-m", model,
            "-s", "read-only",
            "--skip-git-repo-check",
            "--ephemeral",
            "-C", str(workdir),
            "-o", str(out_file),
        ]
        if effort:
            cmd += ["-c", f"model_reasoning_effort={effort}"]
        if schema is not None:
            sf = workdir / "schema.json"
            sf.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
            cmd += ["--output-schema", str(sf)]

        # `-i/--image` объявлен как <FILE>..., то есть переменной длины: позиционный
        # промпт, поставленный после него, будет проглочен как ещё один файл.
        # Поэтому при картинках промпт всегда идёт через stdin, а вместо него "-".
        stdin_text: str | None = None
        if images:
            cmd += ["-i", *images, "-"]
            stdin_text = prompt
        else:
            cmd.append(prompt)

        last: CallResult | None = None
        for attempt in range(retries + 1):
            t0 = time.time()
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True,
                                      timeout=timeout_s, cwd=str(workdir), env=_clean_env(),
                                      input=stdin_text)
            except subprocess.TimeoutExpired:
                return CallResult("CODEX_SESSION", model, effort, False,
                                  error=f"timeout {timeout_s}s", duration_s=time.time() - t0)
            dt = time.time() - t0
            combined = (proc.stdout or "") + "\n" + (proc.stderr or "")

            payload = None
            if out_file.exists():
                payload = _extract_json(out_file.read_text(encoding="utf-8"))
            if payload is None:
                payload = _extract_json(proc.stdout or "")

            if payload is not None:
                return CallResult("CODEX_SESSION", model, effort, True, parsed=payload,
                                  raw=combined, duration_s=dt, exit_code=proc.returncode)

            last = CallResult("CODEX_SESSION", model, effort, False, raw=combined,
                              error=(combined.strip()[-500:] or "пустой ответ"),
                              duration_s=dt, exit_code=proc.returncode)
            low = combined.lower()
            if attempt < retries and any(m in low for m in _CODEX_TRANSIENT):
                time.sleep(3 * (attempt + 1))
                continue
            break
        return last  # type: ignore[return-value]
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def call(provider: str, **kw) -> CallResult:
    if provider == "CLAUDE_SESSION":
        kw.pop("images", None)
        kw.pop("retries", None)
        return call_claude(**kw)
    if provider == "CODEX_SESSION":
        kw.pop("system_prompt", None)
        return call_codex(**kw)
    raise GatewayError(f"неизвестный провайдер: {provider}")
