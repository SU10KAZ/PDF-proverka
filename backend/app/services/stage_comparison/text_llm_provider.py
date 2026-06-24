"""Provider-абстракция для текстового LLM-анализа (Claude Sonnet / Claude Code).

Цель: дать чистую границу между сервисом text_llm.py и конкретным способом
вызова модели. Не смешиваем с llm_runner.py (тот ходит в OpenRouter/Gemini).

Логика поведения:
  • STAGE_COMPARISON_TEXT_LLM_ENABLED=false (default) → resolve_provider()
    возвращает None и сервис должен ответить status="disabled".
  • provider=claude_code → CLI ищется в стандартных местах. Если не найден →
    ProviderResult(status="provider_not_available", reason="..."). Сервис в
    этом случае сохраняет prompt в text_llm_prompt.md для ручного запуска.
  • Реальный вызов — subprocess.run([CLI, "-p", "--model", "sonnet", ...],
    input=prompt, ...). Без shell=True. Безопасные аргументы.

ВАЖНО: provider НЕ парсит структуру changes/summary — он возвращает только
сырое тело ответа. JSON-парсинг живёт в text_llm.py. Это позволяет менять
провайдеров (mock в тестах) не трогая остальную логику.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _is_rate_limited(rc: int, stdout: str, stderr: str) -> bool:
    """#84: единый детектор rate-limit (переиспользуем cli_utils аудита)."""
    try:
        from backend.app.services.common.cli_utils import is_rate_limited
        return bool(is_rate_limited(rc, stdout, stderr))
    except Exception:  # noqa: BLE001 — детектор не должен валить вызов
        return False


def _rate_limit_max_retries() -> int:
    """#84: единый лимит ретраев (из config аудита, fallback 3)."""
    try:
        from backend.app.core.config import RATE_LIMIT_MAX_RETRIES
        return max(0, int(RATE_LIMIT_MAX_RETRIES))
    except Exception:  # noqa: BLE001
        return 3


@dataclass
class ProviderResult:
    """Результат вызова провайдера.

    status:
      • "done"                    — есть raw_response, можно парсить JSON
      • "provider_not_available"  — CLI/SDK не найден или недоступен
      • "error"                   — вызов сделан, но провайдер вернул ошибку
      • "timeout"                 — провайдер не уложился в TIMEOUT_SEC
    """
    status: str
    raw_response: str = ""
    error: Optional[str] = None
    duration_sec: float = 0.0
    provider: str = ""
    model: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class ProviderConfig:
    enabled: bool
    provider: str          # "claude_code" | ...
    model: str             # "sonnet" | "opus" | ...
    timeout_sec: int
    max_chars: int


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def load_config() -> ProviderConfig:
    return ProviderConfig(
        enabled=_env_bool("STAGE_COMPARISON_TEXT_LLM_ENABLED", False),
        provider=os.environ.get("STAGE_COMPARISON_TEXT_LLM_PROVIDER", "claude_code").strip().lower() or "claude_code",
        model=os.environ.get("STAGE_COMPARISON_TEXT_LLM_MODEL", "sonnet").strip() or "sonnet",
        timeout_sec=_env_int("STAGE_COMPARISON_TEXT_LLM_TIMEOUT_SEC", 300),
        max_chars=_env_int("STAGE_COMPARISON_TEXT_LLM_MAX_CHARS", 350_000),
    )


class BaseTextLLMProvider:
    """Общий контракт. Подклассы реализуют check_availability() + invoke()."""

    name: str = "base"

    def check_availability(self) -> tuple[bool, Optional[str]]:
        """Возвращает (доступен, причина_недоступности)."""
        return False, "not_implemented"

    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_sec: int,
        work_dir: Optional[Path] = None,
    ) -> ProviderResult:
        raise NotImplementedError


class ClaudeCodeProvider(BaseTextLLMProvider):
    """Subprocess-провайдер: вызывает `claude -p --model sonnet ...`.

    Авторизация Claude Code (subscription) подхватывается из той же
    пользовательской сессии, в которой запущен backend (uvicorn).
    Per CLAUDE.md memory: «runner'ы через `claude -p` subprocess».

    Безопасность: список аргументов, без shell=True. prompt передаётся через
    stdin. CWD = work_dir (или TempDir) — чтобы Claude не подхватил соседние
    CLAUDE.md/skills проекта во время text-сравнения.
    """

    name = "claude_code"

    # Кандидаты на расположение бинаря, в порядке приоритета.
    _CLI_CANDIDATES_ENV = "STAGE_COMPARISON_TEXT_LLM_CLI_PATH"

    def _find_cli(self) -> Optional[str]:
        env_path = os.environ.get(self._CLI_CANDIDATES_ENV, "").strip()
        if env_path:
            p = Path(env_path).expanduser()
            if p.exists() and os.access(p, os.X_OK):
                return str(p.resolve())
        # Стандартные пути
        candidates = [
            shutil.which("claude"),
            os.path.expanduser("~/.local/bin/claude"),
            os.path.expanduser("~/.npm-global/lib/node_modules/@anthropic-ai/claude-code/node_modules/@anthropic-ai/claude-code-linux-x64/claude"),
        ]
        # Авто-поиск свежей VSCode-extension версии (берём lexicographically last)
        ext_root = Path.home() / ".vscode-server" / "extensions"
        if ext_root.exists():
            ext_candidates = sorted(ext_root.glob("anthropic.claude-code-*-linux-x64/resources/native-binary/claude"))
            if ext_candidates:
                candidates.append(str(ext_candidates[-1]))
        for c in candidates:
            if not c:
                continue
            p = Path(c).expanduser()
            try:
                if p.exists() and os.access(p, os.X_OK) and not p.is_symlink():
                    return str(p.resolve())
                # symlink — резолвим и проверяем target
                if p.is_symlink():
                    target = p.resolve()
                    if target.exists() and os.access(target, os.X_OK):
                        return str(target)
            except OSError:
                continue
        return None

    def check_availability(self) -> tuple[bool, Optional[str]]:
        cli = self._find_cli()
        if not cli:
            return False, "claude_cli_not_found"
        # Быстрая проверка через --version (~50ms). Не вызываем -p (чтобы не
        # триггерить OAuth).
        try:
            r = subprocess.run(
                [cli, "--version"],
                capture_output=True, text=True, timeout=5,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            return False, f"version_check_failed: {exc}"
        if r.returncode != 0:
            return False, f"version_check_rc={r.returncode}"
        return True, None

    def invoke(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        model: str,
        timeout_sec: int,
        work_dir: Optional[Path] = None,
    ) -> ProviderResult:
        cli = self._find_cli()
        if not cli:
            return ProviderResult(
                status="provider_not_available",
                error="claude_cli_not_found",
                provider=self.name,
                model=model,
            )

        # Аргументы. --bare выключен — нам нужен subscription auth. Поэтому
        # передаём system prompt через --append-system-prompt-file, чтобы
        # не вешать его на argv (он большой).
        # --output-format json — claude отдаёт {"result": "...", ...}.
        sys_file: Optional[Path] = None
        try:
            if work_dir:
                # ОБЯЗАТЕЛЬНО абсолютный путь: ниже subprocess запускается с
                # cwd=work_dir, и относительный --append-system-prompt-file
                # резолвился бы Claude CLI от нового CWD → задвоение пути
                # (work_dir/work_dir/_text_llm_system_prompt.tmp.md) и
                # «Append system prompt file not found».
                work_dir = Path(work_dir).resolve()
                work_dir.mkdir(parents=True, exist_ok=True)
                # УНИКАЛЬНОЕ имя на каждый вызов. При chunk_concurrency>1
                # (evidence_first_fallback) несколько потоков делят ОДИН work_dir;
                # фиксированное имя приводило к гонке — finally.unlink() одного
                # потока удалял файл, нужный subprocess'у другого → ENOENT на
                # --append-system-prompt-file → rc!=0 → тихая потеря changes
                # чанка (pre-deploy review #54). tempfile.mkstemp атомарно даёт
                # неконфликтующее имя.
                _fd, _sys_path = tempfile.mkstemp(
                    prefix="_text_llm_sys_", suffix=".tmp.md", dir=str(work_dir),
                )
                os.close(_fd)
                sys_file = Path(_sys_path)
                sys_file.write_text(system_prompt, encoding="utf-8")

            args = [
                cli, "-p",
                "--model", model,
                "--output-format", "json",
                "--no-session-persistence",
                "--permission-mode", "default",
                "--disable-slash-commands",
            ]
            if sys_file is not None:
                args.extend(["--append-system-prompt-file", str(sys_file)])
            else:
                # fallback: inline system prompt
                args.extend(["--append-system-prompt", system_prompt[:4000]])

            # #84: единый rate-limit-aware retry. Распознаём 'usage limit reached'/
            # 'overloaded'/429 тем же cli_utils.is_rate_limited, что и аудит, и
            # применяем bounded backoff (как with_rate_limit_retry в аудите) —
            # чтобы политика была одна на оба пайплайна на одной подписке.
            max_retries = _rate_limit_max_retries()
            attempt = 0
            while True:
                t0 = time.monotonic()
                try:
                    proc = subprocess.run(
                        args,
                        input=user_prompt,
                        capture_output=True,
                        text=True,
                        timeout=timeout_sec,
                        cwd=str(work_dir) if work_dir else None,
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    return ProviderResult(
                        status="timeout",
                        error=f"timed_out_after_{timeout_sec}s",
                        duration_sec=time.monotonic() - t0,
                        provider=self.name,
                        model=model,
                    )
                duration = time.monotonic() - t0

                if proc.returncode != 0:
                    stderr_tail = (proc.stderr or "")[-2000:]
                    if (attempt < max_retries
                            and _is_rate_limited(proc.returncode, proc.stdout or "", proc.stderr or "")):
                        wait = min(60, 2 ** attempt * 5)
                        logger.warning(
                            "text_llm rate-limited (attempt %d/%d), waiting %ds",
                            attempt + 1, max_retries, wait,
                        )
                        time.sleep(wait)
                        attempt += 1
                        continue
                    return ProviderResult(
                        status="error",
                        error=f"claude_rc={proc.returncode}: {stderr_tail}",
                        duration_sec=duration,
                        provider=self.name,
                        model=model,
                    )
                stdout = proc.stdout or ""
                return ProviderResult(
                    status="done",
                    raw_response=stdout,
                    duration_sec=duration,
                    provider=self.name,
                    model=model,
                )
        finally:
            if sys_file is not None:
                try:
                    sys_file.unlink()
                except OSError:
                    pass


# ── Registry ─────────────────────────────────────────────────────────────

_REGISTRY: dict[str, type[BaseTextLLMProvider]] = {
    "claude_code": ClaudeCodeProvider,
}


def resolve_provider(config: Optional[ProviderConfig] = None) -> tuple[Optional[BaseTextLLMProvider], ProviderConfig]:
    """Вернёт (provider или None, config).

    None означает что provider запрещён ENV-флагом — сервис должен
    отрапортовать status="disabled".
    """
    cfg = config or load_config()
    if not cfg.enabled:
        return None, cfg
    cls = _REGISTRY.get(cfg.provider)
    if cls is None:
        logger.warning("text_llm: unknown provider '%s'", cfg.provider)
        return None, cfg
    return cls(), cfg


__all__ = [
    "ProviderResult",
    "ProviderConfig",
    "BaseTextLLMProvider",
    "ClaudeCodeProvider",
    "load_config",
    "resolve_provider",
]
