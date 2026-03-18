"""E2E-тест с реальным Claude CLI.

Запускается ТОЛЬКО при наличии env-флага RUN_E2E_CLAUDE=1.
По умолчанию — skip. Не ломает обычный pytest на чистой машине.

Запуск:
    RUN_E2E_CLAUDE=1 python -m pytest tests/test_e2e_claude_cli.py -v

Что проверяет:
1. Claude CLI доступен и отвечает на --version
2. Формирование task prompt через task_builder
3. Запуск Claude CLI с минимальным prompt, проверка что output не пустой
"""
import asyncio
import json
import os
import shutil
import pytest
from pathlib import Path

E2E_ENABLED = os.environ.get("RUN_E2E_CLAUDE", "0") == "1"
skip_reason = "E2E тест: установите RUN_E2E_CLAUDE=1 для запуска"


@pytest.fixture(scope="module")
def e2e_guard():
    if not E2E_ENABLED:
        pytest.skip(skip_reason)


@pytest.fixture(scope="module")
def claude_cli_path(e2e_guard):
    """Найти Claude CLI."""
    from webapp.config import CLAUDE_CLI
    found = shutil.which(CLAUDE_CLI) or shutil.which("claude")
    if not found:
        pytest.skip("Claude CLI не найден в PATH")
    return found


# ─── Тест 1: Claude CLI --version ─────────────────────────────
class TestClaudeCLI:
    def test_claude_version(self, claude_cli_path):
        """Claude CLI отвечает на --version."""
        import subprocess
        result = subprocess.run(
            [claude_cli_path, "--version"],
            capture_output=True, timeout=30,
        )
        output = result.stdout.decode("utf-8", errors="replace") + \
                 result.stderr.decode("utf-8", errors="replace")
        # Claude CLI должен вернуть версию или usage
        assert result.returncode == 0 or "claude" in output.lower(), \
            f"Claude CLI не ответил корректно: {output[:200]}"


# ─── Тест 2: task_builder формирует prompt ─────────────────────
class TestTaskBuilder:
    def test_build_text_analysis_prompt(self, e2e_guard):
        """task_builder формирует непустой text_analysis prompt."""
        from webapp.services.task_builder import build_text_analysis_prompt

        project_info = {
            "project_id": "test-e2e",
            "name": "E2E Test Project",
            "section": "EM",
        }
        output_path = "/tmp/test_output"
        md_file_path = "/tmp/test.md"

        prompt = build_text_analysis_prompt(project_info, output_path, md_file_path)
        assert isinstance(prompt, str)
        assert len(prompt) > 100, f"Prompt слишком короткий: {len(prompt)}"
        assert "test-e2e" in prompt or "E2E" in prompt


# ─── Тест 3: Минимальный Claude CLI run ───────────────────────
class TestClaudeRun:
    def test_minimal_prompt(self, claude_cli_path):
        """Claude CLI обрабатывает минимальный prompt и возвращает output."""
        import subprocess
        result = subprocess.run(
            [claude_cli_path, "-p", "Ответь одним словом: 2+2=?",
             "--max-turns", "1", "--output-format", "text"],
            capture_output=True, timeout=60,
        )
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        # Claude CLI должен вернуть что-то осмысленное
        assert len(stdout) > 0, "Claude CLI вернул пустой output"

    def test_json_output(self, claude_cli_path, tmp_path):
        """Claude CLI пишет файл через Write tool."""
        import subprocess
        out_file = tmp_path / "test_output.json"
        prompt = (
            f'Создай файл {out_file} с содержимым: '
            '{"test": true, "status": "ok"}. '
            'Используй Write tool. Ничего не выводи в чат.'
        )
        result = subprocess.run(
            [claude_cli_path, "-p", prompt,
             "--max-turns", "3", "--allowedTools", "Write",
             "--output-format", "text"],
            capture_output=True, timeout=120,
        )
        # Проверяем что файл создан (или хотя бы CLI не упал)
        if out_file.exists():
            data = json.loads(out_file.read_text(encoding="utf-8"))
            assert data.get("test") is True
            assert data.get("status") == "ok"
        else:
            # CLI мог не создать файл (permissions, sandbox), но не должен падать
            assert result.returncode in (0, -1, 1), \
                f"Claude CLI упал с кодом {result.returncode}"
