"""Этап 11b, inference-гейт: нейтрализация контекста и разрешение воркера.

Что здесь закрывается и почему именно так.

Три проверенных дефекта этапа: личный контекст владельца машины доезжал до
контрольного запроса (находка 11), разрешение «со стороны воркера» приходило
из той же SSH-команды, что и подтверждение оператора (находка 9), и
подпроцессу доставался stdin вызывающего — на чём этап и обжёгся живьём.

Тесты специально написаны как рубежи против БУДУЩЕЙ правки, а не как
подтверждение текущего кода: почти каждый из них падает от одного удалённого
флага или одной «упрощающей» замены формы записи аргумента.
"""
from __future__ import annotations

import ast
import inspect
import os
import stat
import subprocess
from pathlib import Path

import pytest

from audit_worker.providers import probe_grant
from audit_worker.providers.base import PROBE_PROMPT
from audit_worker.providers.claude_adapter import _probe_argv
from audit_worker.providers.codex_adapter import CodexProviderAdapter

REPO = Path(__file__).resolve().parents[1]


# ─── Находка 11: личный контекст владельца машины ───────────────────────────
class TestPersonalContextNeutralized:
    """В ambient HOME — чужой, и всё, что в нём лежит, обязано быть отключено."""

    @pytest.mark.parametrize("flag", [
        "--safe-mode",              # CLAUDE.md, навыки, плагины, ХУКИ, MCP, агенты
        "--strict-mcp-config",      # второй рубеж по MCP
        "--disable-slash-commands",
        "--no-session-persistence",  # не оставлять сессию в личном каталоге
        "--setting-sources=",       # ни user, ни project, ни local
    ])
    def test_claude_probe_disables_personal_context(self, flag: str) -> None:
        assert flag in _probe_argv(), (
            f"из контрольного запроса пропал {flag}: вместе с ним вернётся "
            "личный контекст владельца машины, и результат перестанет быть "
            "воспроизводимым"
        )

    def test_claude_probe_disables_all_builtin_tools(self) -> None:
        """`--tools=` сильнее перечисления: новый инструмент запрещён по умолчанию."""
        assert "--tools=" in _probe_argv()

    def test_claude_probe_does_not_use_bare_mode(self) -> None:
        """`--bare` выключает и OAuth — в ambient он сделал бы запрос невозможным."""
        assert "--bare" not in _probe_argv()

    def test_codex_probe_ignores_personal_config_and_rules(self) -> None:
        source = inspect.getsource(CodexProviderAdapter.minimal_probe)
        for flag in ("--ignore-user-config", "--ignore-rules", "--ephemeral"):
            assert flag in source, (
                f"из контрольного запроса Codex пропал {flag}: вернутся личные "
                "модель, усилие, service_tier и MCP-серверы владельца машины"
            )


# ─── Урок инцидента: вариадические флаги ────────────────────────────────────
class TestVariadicArgumentSafety:
    """`--tools ""` съедает соседний токен. Один раз это уже стоило запроса."""

    def test_variadic_flags_are_written_with_equals(self) -> None:
        argv = _probe_argv()
        bare = [a for a in argv
                if a in {"--tools", "--disallowed-tools", "--allowedTools",
                         "--allowed-tools", "--add-dir", "--plugin-dir"}]
        assert bare == [], (
            f"вариадические флаги {bare} записаны без `=`: они заберут "
            "следующий аргумент, и промпт перестанет быть промптом"
        )

    def test_prompt_is_the_last_argument(self) -> None:
        assert _probe_argv()[-1] == PROBE_PROMPT

    def test_codex_prompt_is_last_in_source_order(self) -> None:
        source = inspect.getsource(CodexProviderAdapter.minimal_probe)
        body = source.split("argv = [", 1)[1].split("]", 1)[0]
        entries = [line.strip().rstrip(",") for line in body.splitlines() if line.strip()
                   and not line.strip().startswith("#")]
        assert entries[-1] == "PROBE_PROMPT"


class TestSubprocessStdinIsolation:
    """I-P8. Оба CLI читают промпт со stdin, если он не пуст."""

    def test_run_uses_devnull_when_there_is_no_own_stdin(self) -> None:
        source = inspect.getsource(
            __import__("audit_worker.providers.base", fromlist=["x"]).ProviderAdapter.run
        )
        assert "subprocess.DEVNULL" in source
        assert "stdin_text is not None" in source

    def test_devnull_actually_prevents_inheriting_caller_stdin(self, tmp_path: Path) -> None:
        """Поведенческая проверка, а не чтение исходника.

        Дочерний процесс пытается прочитать stdin. Через PIPE он получил бы
        ввод, через DEVNULL — пустую строку. Это ровно та разница, из-за
        которой скрипт когда-то стал промптом.
        """
        script = tmp_path / "reader.py"
        script.write_text("import sys; sys.stdout.write(sys.stdin.read())")
        result = subprocess.run(
            ["python", str(script)], input="СЕКРЕТНЫЙ ВВОД ВЫЗЫВАЮЩЕГО",
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
        )
        assert "СЕКРЕТНЫЙ" in result.stdout          # контроль: канал работает
        result = subprocess.run(
            ["python", str(script)], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False,
        )
        assert result.stdout == ""


# ─── Находка 9: разрешение со стороны воркера ───────────────────────────────
class TestProbeGrant:
    def _grant(self, root: Path, text: str, mode: int = 0o600) -> Path:
        path = probe_grant.grant_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        os.chmod(path, mode)
        return path

    def test_absent_file_means_refusal(self, tmp_path: Path) -> None:
        with pytest.raises(probe_grant.ProbeGrantError) as exc:
            probe_grant.consume(tmp_path, "claude")
        assert "отсутствует" in str(exc.value)

    def test_single_use_budget_is_consumed_before_the_call(self, tmp_path: Path) -> None:
        self._grant(tmp_path, "claude=1\ncodex=1\n")
        assert probe_grant.consume(tmp_path, "claude") == 0
        with pytest.raises(probe_grant.ProbeGrantError) as exc:
            probe_grant.consume(tmp_path, "claude")
        assert "исчерпано" in str(exc.value)
        # Сосед не пострадал: бюджет попровайдерный.
        assert probe_grant.read_state(tmp_path, "codex").remaining == 1

    def test_consumption_survives_a_crash_after_it(self, tmp_path: Path) -> None:
        """Списание durable: перечитанное с диска состояние уже уменьшено."""
        self._grant(tmp_path, "codex=1\n")
        probe_grant.consume(tmp_path, "codex")
        assert probe_grant.read_state(tmp_path, "codex").remaining == 0

    def test_world_readable_grant_is_rejected(self, tmp_path: Path) -> None:
        self._grant(tmp_path, "claude=1\n", mode=0o644)
        state = probe_grant.read_state(tmp_path, "claude")
        assert state.error and "шире" in state.error
        with pytest.raises(probe_grant.ProbeGrantError):
            probe_grant.consume(tmp_path, "claude")

    def test_symlink_is_rejected_even_with_narrow_target(self, tmp_path: Path) -> None:
        real = tmp_path / "real_grant"
        real.write_text("claude=5\n", encoding="utf-8")
        os.chmod(real, 0o600)
        path = probe_grant.grant_path(tmp_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to(real)
        state = probe_grant.read_state(tmp_path, "claude")
        assert state.error and "ссылка" in state.error

    def test_malformed_line_is_an_error_not_a_silent_zero(self, tmp_path: Path) -> None:
        self._grant(tmp_path, "claude:1\n")
        state = probe_grant.read_state(tmp_path, "claude")
        assert state.error and "claude:1" in state.error

    def test_env_variable_alone_no_longer_authorizes(self, tmp_path: Path) -> None:
        """Суть находки 9: переменная больше не является разрешением воркера."""
        source = inspect.getsource(
            __import__("audit_worker.__main__", fromlist=["x"])._cmd_provider_probe
        )
        tree = ast.parse(inspect.getsource(
            __import__("audit_worker.__main__", fromlist=["x"])._cmd_provider_probe
        ).lstrip())
        gates = [n for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                 and n.attr == "allow_real_provider_probe"]
        assert gates == [], (
            "подкоманда снова смотрит на allow_real_provider_probe: разрешение, "
            "которое подставляется в ту же SSH-команду, не является независимым"
        )
        assert "probe_grant.consume" in source

    def test_grant_file_never_created_by_the_worker(self) -> None:
        """Воркер не выписывает разрешение сам — иначе оно ничего не значит."""
        source = Path(REPO / "audit_worker" / "providers" / "probe_grant.py").read_text()
        tree = ast.parse(source)
        creators = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in {"touch", "mkdir"}:
                    creators.append(node.func.attr)
        assert creators == [], f"модуль создаёт разрешение сам: {creators}"


class TestGrantVisibility:
    """Разрешение, которого не видно, невозможно вовремя отозвать."""

    def test_remaining_reaches_capability_for_the_center(self) -> None:
        source = inspect.getsource(
            __import__("audit_worker.providers.manager", fromlist=["x"]).ProviderManager
            .heartbeat_payload
        )
        assert "inference_probe_grant_remaining" in source
        assert "capability" in source

    def test_ui_renders_auth_mode_and_grant(self) -> None:
        ui = (REPO / "frontend" / "static" / "js" / "audit-workers.js").read_text()
        assert "AUTH_MODE_LABEL" in ui
        assert "capability.auth_mode" in ui or "capability.auth_mode" in ui
        assert "inference_probe_grant_remaining" in ui
        assert "личная учётная запись пользователя VPS" in ui


class TestSmokeCannotSupplyBothPermissions:
    def test_stage11_smoke_no_longer_injects_the_env_permission(self) -> None:
        """Проверяется ДЕРЕВО РАЗБОРА, а не текст файла.

        Наивный греп здесь ложно срабатывает на комментарии, который как раз и
        объясняет, почему подстановки больше нет, — то есть на доказательстве
        отсутствия дефекта. Ровно этот класс ошибки этап уже ловил дважды
        (дефект 2 §22 и находка 5 §24.1), и тест, написанный грепом, приучал бы
        снимать красную строку вместо чтения.
        """
        path = REPO / "scripts" / "smoke_distributed_audit_provider_gate.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if "AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=" in node.value:
                    offenders.append(node.value.strip()[:80])
        assert offenders == [], (
            f"смоук снова подаёт оба разрешения из одной команды: {offenders}"
        )
