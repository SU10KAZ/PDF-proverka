"""Этап 11 — provider auth & quota gate.

Тесты поведенческие: везде, где можно, проверяется НАБЛЮДАЕМЫЙ результат
(что вернул адаптер, что записалось в базу, что увидел бы оператор), а не
наличие строки в исходнике. Исключения — там, где инвариант структурный
(«колонки для токена не существует»), и его нельзя выразить иначе.

Поддельные CLI здесь — не «моки ради моков»: адаптер обязан работать с
настоящим подпроцессом, его кодом возврата, его stdout и его таймаутом.
Подменить subprocess значило бы не проверить ровно то, что ломается в жизни.
"""
from __future__ import annotations

import json
import os
import stat
import sys
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from audit_worker.providers import errors, quota  # noqa: E402
from audit_worker.providers.base import ProviderAdapter  # noqa: E402
from audit_worker.providers.claude_adapter import ClaudeProviderAdapter  # noqa: E402
from audit_worker.providers.codex_adapter import (  # noqa: E402
    CodexProviderAdapter,
    _snapshot_from_rate_limits,
)
from audit_worker.providers.identity import (  # noqa: E402
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    INSTALL_INSTALLED,
    INSTALL_MISSING,
    credential_file_facts,
)
from audit_worker.providers.manager import ProviderManager  # noqa: E402
from audit_worker.providers.paths import provider_home  # noqa: E402


# ─── Инфраструктура: поддельные CLI ──────────────────────────────────────────
def _write_exe(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = body if body.startswith("#!") else "#!/bin/bash\n" + body
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)
    return path


@pytest.fixture()
def worker_root(tmp_path: Path) -> Path:
    root = tmp_path / "worker-data"
    root.mkdir()
    return root


def _claude(worker_root: Path, body: str) -> ClaudeProviderAdapter:
    home = provider_home(worker_root, "claude")
    home.ensure_dirs()
    exe = _write_exe(home.home / ".local" / "bin" / "claude", body)
    return ClaudeProviderAdapter(home, executable=exe, timeout_sec=15.0)


def _codex(worker_root: Path, body: str) -> CodexProviderAdapter:
    home = provider_home(worker_root, "codex")
    home.ensure_dirs()
    exe = _write_exe(home.home / ".local" / "bin" / "codex", body)
    return CodexProviderAdapter(home, executable=exe, timeout_sec=15.0)


#: Поддельный Claude: отвечает на --version и auth status, как настоящий.
_CLAUDE_LOGGED_OUT = """
case "$1" in
  --version) echo "2.1.226 (Claude Code)"; exit 0 ;;
  auth) if [ "$2" = "status" ]; then
          echo '{"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"}'
          exit 1
        fi ;;
esac
exit 2
"""

_CLAUDE_LOGGED_IN = """
case "$1" in
  --version) echo "2.1.226 (Claude Code)"; exit 0 ;;
  auth) if [ "$2" = "status" ]; then
          echo '{"loggedIn": true, "authMethod": "claudeai", "apiProvider": "firstParty"}'
          exit 0
        fi ;;
esac
exit 2
"""

#: Поддельный Codex целиком на Python, а не на bash.
#:
#: Причина конкретная и стоила отладки: `python3 - <<'PYEOF'` внутри bash
#: подменяет stdin процесса на heredoc, и JSON-RPC-диалог до программы просто
#: не доходит — сервер молча не отвечает ни на один запрос. Здесь stdin
#: остаётся тем самым каналом, по которому адаптер и говорит.
_CODEX_PY_TEMPLATE = '''#!/usr/bin/env python3
import json, sys

ACCOUNT = @@ACCOUNT@@
RATE = @@RATE@@
LOGIN_EXIT = @@LOGIN_EXIT@@
LOGIN_TEXT = @@LOGIN_TEXT@@

argv = sys.argv[1:]
if argv[:1] == ["--version"]:
    print("codex-cli 0.147.0")
    raise SystemExit(0)
if argv[:2] == ["login", "status"]:
    print(LOGIN_TEXT)
    raise SystemExit(LOGIN_EXIT)
if argv[:1] == ["app-server"]:
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        mid = msg.get("id")
        method = msg.get("method")
        if method == "initialize":
            print(json.dumps({"id": mid, "result": {"userAgent": "fake"}}), flush=True)
        elif method == "account/read":
            print(json.dumps({"id": mid, **ACCOUNT}), flush=True)
        elif method == "account/rateLimits/read":
            print(json.dumps({"id": mid, **RATE}), flush=True)
    raise SystemExit(0)
raise SystemExit(2)
'''


def _codex_script(*, account: dict, rate: dict, login_exit: int = 0,
                  login_status_text: str = "Logged in") -> str:
    return (
        _CODEX_PY_TEMPLATE
        .replace("@@ACCOUNT@@", repr(account))
        .replace("@@RATE@@", repr(rate))
        .replace("@@LOGIN_EXIT@@", str(login_exit))
        .replace("@@LOGIN_TEXT@@", repr(login_status_text))
    )


_RATE_OK = {
    "result": {
        "rateLimits": {
            "limitId": "codex",
            "limitName": None,
            "primary": {"usedPercent": 25.0, "windowDurationMins": 300,
                        "resetsAt": 4_000_000_000},
            "secondary": {"usedPercent": 60.0, "windowDurationMins": 10080,
                          "resetsAt": 4_000_500_000},
            "rateLimitReachedType": None,
        }
    }
}

_ACCOUNT_OK = {
    "result": {
        "account": {"type": "chatgpt", "email": "person@example.com", "planType": "pro"},
        "requiresOpenaiAuth": True,
    }
}


# ═════════════════════════════════════════════════════════════════════════════
# 31.1 Адаптеры провайдеров
# ═════════════════════════════════════════════════════════════════════════════
class TestProviderAdapters:
    def test_installed_and_version(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_OUT)
        assert adapter.installed() is True
        assert adapter.version() == "2.1.226"

    def test_missing_executable_is_not_an_exception(self, worker_root):
        home = provider_home(worker_root, "claude")
        home.ensure_dirs()
        adapter = ClaudeProviderAdapter(home, timeout_sec=5.0)
        assert adapter.installed() is False
        assert adapter.version() is None
        identity = adapter.identity()
        assert identity.installation_status == INSTALL_MISSING
        assert identity.error_code == errors.ERR_CLI_MISSING

    def test_auth_logged_out(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_OUT)
        status = adapter.auth_status()
        assert status.auth_state == AUTH_LOGGED_OUT
        assert status.auth_method == "none"

    def test_auth_logged_in(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        status = adapter.auth_status()
        assert status.auth_state == AUTH_LOGGED_IN
        assert status.auth_method == "claudeai"
        identity = adapter.identity()
        assert identity.installation_status == INSTALL_INSTALLED
        assert identity.auth_state == AUTH_LOGGED_IN

    def test_timeout_does_not_hang_and_is_classified(self, worker_root):
        adapter = _claude(worker_root, "sleep 30\n")
        adapter.timeout_sec = 1.0
        started = time.monotonic()
        result = adapter.run(["--version"], timeout_sec=1.0)
        assert time.monotonic() - started < 15.0
        assert result.timed_out is True
        assert result.error_code() == errors.ERR_TIMEOUT

    def test_malformed_status_is_reported_as_such(self, worker_root):
        adapter = _claude(worker_root, "echo 'это не json'\nexit 0\n")
        status = adapter.auth_status()
        assert status.error_code == errors.ERR_MALFORMED_STATUS

    def test_wrong_executable_is_not_silently_replaced_by_path_lookup(self, worker_root):
        """Ключевой запрет: поиска по PATH нет.

        В PATH воркера первым идёт каталог ПОДДЕЛЬНЫХ провайдеров. Если бы
        адаптер искал `claude` в PATH, он опросил бы подделку и отрапортовал
        центру её версию как настоящую.
        """
        home = provider_home(worker_root, "claude")
        home.ensure_dirs()
        decoy_dir = worker_root / "fake_providers"
        _write_exe(decoy_dir / "claude", "echo '9.9.9 (подделка)'\nexit 0\n")
        adapter = ClaudeProviderAdapter(home, timeout_sec=5.0)
        os.environ["PATH"] = f"{decoy_dir}:{os.environ.get('PATH', '')}"
        try:
            assert adapter.executable_path() is None
            assert adapter.version() is None
        finally:
            os.environ["PATH"] = os.environ["PATH"].split(":", 1)[1]

    def test_output_is_redacted_before_it_leaves_the_adapter(self, worker_root):
        adapter = _claude(
            worker_root,
            "echo 'ANTHROPIC_API_KEY=sk-ant-abcdefghijklmnop'\n"
            "echo 'Authorization: Bearer eyJhbGciOi.payloadpart.signature'\n"
            "exit 0\n",
        )
        result = adapter.run(["--version"])
        assert "sk-ant-abcdefghijklmnop" not in result.stdout
        assert "eyJhbGciOi.payloadpart.signature" not in result.stdout
        assert "<redacted" in result.stdout

    def test_environment_is_built_from_scratch(self, worker_root, monkeypatch):
        """Секреты воркера физически не доходят до подпроцесса."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_secret")
        monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.example")
        adapter = _claude(worker_root, _CLAUDE_LOGGED_OUT)
        env = adapter.build_env()
        assert "ANTHROPIC_API_KEY" not in env
        assert "AUDIT_WORKER_TOKEN" not in env
        assert "AUDIT_WORKER_DISPATCHER_URL" not in env
        assert env["HOME"] == str(adapter.home.home)

    def test_cwd_is_the_empty_runtime_dir(self, worker_root):
        adapter = _claude(worker_root, "pwd\nexit 0\n")
        result = adapter.run(["--version"])
        assert result.stdout.strip().endswith("runtime")

    def test_classify_error_covers_the_documented_set(self):
        assert errors.classify_text("Not logged in") == errors.ERR_AUTH_REQUIRED
        assert errors.classify_text("rate limit reached") == errors.ERR_RATE_LIMITED
        assert errors.classify_text("connection refused") == errors.ERR_NETWORK
        assert errors.classify_text("service unavailable") == errors.ERR_PROVIDER_UNAVAILABLE
        # «context limit» — НЕ лимит подписки. Раньше наивный шаблон утащил бы
        # его в rate_limited, и оператор увидел бы исчерпанную подписку.
        assert errors.classify_text("context limit exceeded for model") != (
            errors.ERR_RATE_LIMITED
        )
        assert errors.classify_text("совершенно новое сообщение") is None


# ═════════════════════════════════════════════════════════════════════════════
# 31.2 Разборщик квоты
# ═════════════════════════════════════════════════════════════════════════════
class TestQuotaParser:
    def _parse(self, payload: dict, **kw):
        params = dict(
            provider="codex", auth_state=AUTH_LOGGED_IN, account_group_id=None,
            observed_at=1_000_000.0, stale_after=1_000_900.0,
            parser_version="codex-appserver-1", low_threshold_pct=None,
        )
        params.update(kw)
        return _snapshot_from_rate_limits(payload, **params)

    def test_structured_full_data(self):
        snapshot = self._parse(_RATE_OK["result"])
        assert snapshot.quota_state == quota.QUOTA_READY
        # Берётся ХУДШЕЕ окно: недельное выбрано на 60 %, значит осталось 40 %,
        # а не 75 % пятичасового. Иначе «свободно» было бы неправдой.
        assert snapshot.estimated_remaining_pct == pytest.approx(40.0)
        assert snapshot.raw_remaining_supported is True
        assert snapshot.source == quota.SOURCE_OFFICIAL_APP_SERVER_RPC
        assert snapshot.confidence == quota.CONFIDENCE_HIGH
        # Честная оговорка обязана доехать до центра.
        assert snapshot.source_stability == quota.STABILITY_EXPERIMENTAL
        assert snapshot.next_reset_at == 4_000_000_000

    def test_percentage_unavailable_gives_unknown_not_zero(self):
        payload = {"rateLimits": {"limitId": "codex",
                                  "primary": {"resetsAt": 4_000_000_000},
                                  "secondary": None, "rateLimitReachedType": None}}
        snapshot = self._parse(payload)
        assert snapshot.quota_state == quota.QUOTA_UNKNOWN
        assert snapshot.estimated_remaining_pct is None
        assert snapshot.raw_remaining_supported is False
        # Дата сброса известна и сохраняется: это самостоятельная новость.
        assert snapshot.next_reset_at == 4_000_000_000

    def test_reset_only_window_is_kept(self):
        payload = {"rateLimits": {"limitId": "codex",
                                  "primary": {"resetsAt": 4_000_000_000},
                                  "rateLimitReachedType": None}}
        snapshot = self._parse(payload)
        assert snapshot.primary_window is not None
        assert snapshot.primary_window.reset_at == 4_000_000_000
        assert snapshot.primary_window.used_pct is None

    def test_rate_limited_without_percentage(self):
        payload = {"rateLimits": {"limitId": "codex", "primary": {},
                                  "rateLimitReachedType": "primary"}}
        snapshot = self._parse(payload)
        assert snapshot.quota_state == quota.QUOTA_LIMITED
        assert snapshot.estimated_remaining_pct is None

    def test_malformed_payload_raises_and_becomes_unknown(self):
        with pytest.raises(quota.QuotaContractError):
            self._parse({"nothing": "useful"})

    def test_multi_bucket_view_prefers_codex_and_does_not_mix(self):
        payload = {
            "rateLimitsByLimitId": {
                "codex": {"limitId": "codex",
                          "primary": {"usedPercent": 10.0, "windowDurationMins": 300,
                                      "resetsAt": 4_000_000_000},
                          "rateLimitReachedType": None},
                "codex_other": {"limitId": "codex_other",
                                "primary": {"usedPercent": 90.0,
                                            "windowDurationMins": 60,
                                            "resetsAt": 4_000_100_000},
                                "rateLimitReachedType": None},
            }
        }
        snapshot = self._parse(payload)
        assert snapshot.primary_window.window_id == "codex:primary"
        # Чужое ведро попадает во вторичные окна, но не подменяет основное.
        ids = {w.window_id for w in snapshot.secondary_windows}
        assert "codex_other:primary" in ids

    def test_low_threshold_requires_configuration(self):
        payload = _RATE_OK["result"]
        without = self._parse(payload, low_threshold_pct=None)
        assert without.quota_state == quota.QUOTA_READY
        with_threshold = self._parse(payload, low_threshold_pct=50)
        assert with_threshold.quota_state == quota.QUOTA_LOW

    def test_parser_version_is_recorded(self):
        snapshot = self._parse(_RATE_OK["result"])
        assert snapshot.parser_version == "codex-appserver-1"

    def test_contract_forbids_percentage_without_source(self):
        with pytest.raises(quota.QuotaContractError):
            quota.QuotaWindow(
                window_id="w", source=quota.SOURCE_UNAVAILABLE,
                confidence=quota.CONFIDENCE_HIGH, used_pct=10.0,
            )
        with pytest.raises(quota.QuotaContractError):
            quota.QuotaWindow(
                window_id="w", source=quota.SOURCE_OFFICIAL_APP_SERVER_RPC,
                confidence=quota.CONFIDENCE_NONE, used_pct=10.0,
            )
        with pytest.raises(quota.QuotaContractError):
            quota.ProviderQuotaSnapshot(
                provider="codex", quota_state=quota.QUOTA_READY,
                observed_at=1.0, source=quota.SOURCE_OFFICIAL_APP_SERVER_RPC,
                confidence=quota.CONFIDENCE_HIGH, auth_state=AUTH_LOGGED_IN,
                estimated_remaining_pct=50.0, raw_remaining_supported=False,
            )

    def test_stale_snapshot_is_not_served_as_current(self, worker_root):
        manager = ProviderManager(worker_root=worker_root, stale_after_sec=60.0)
        snapshot = quota.ProviderQuotaSnapshot(
            provider="codex", quota_state=quota.QUOTA_READY, observed_at=1_000.0,
            source=quota.SOURCE_OFFICIAL_APP_SERVER_RPC,
            confidence=quota.CONFIDENCE_HIGH, auth_state=AUTH_LOGGED_IN,
            estimated_remaining_pct=70.0, raw_remaining_supported=True,
            stale_after=1_060.0,
        )
        manager._quotas["codex"] = snapshot
        served = manager.quota("codex", now=5_000.0)
        assert served.quota_state == quota.QUOTA_STALE
        assert "устарел" in (served.detail or "")


class TestClaudeQuotaHonesty:
    """Claude: официального ОПРОСА нет, а единственный источник — локальный кеш.

    Обновлено на 12J. Раньше здесь утверждалось «остаток недоступен всегда», и
    это было верно ровно до тех пор, пока единственным кандидатом считался
    опрос. Кеш, который Claude Code пишет себе сам, опросом не является: его
    чтение не стоит ни запроса, ни токена. Поэтому требование сместилось —
    не «числа нет никогда», а «число появляется ТОЛЬКО из локального кеша и
    только с признанием, что источник недокументирован».
    """

    def test_claude_reads_quota_without_any_inference(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        assert adapter.supports_zero_inference_quota() is True
        assert adapter.quota_source_name() == quota.SOURCE_LOCAL_USAGE_STATS
        assert adapter.quota_source_stability() == quota.STABILITY_UNDOCUMENTED

    def test_claude_quota_is_unknown_without_local_cache(self, worker_root):
        """Кеша нет — числа нет. Выдумывать его по факту авторизации нельзя."""
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_UNKNOWN
        assert snapshot.estimated_remaining_pct is None
        assert snapshot.raw_remaining_supported is False
        assert snapshot.reason_code == quota.REASON_LOCAL_CACHE_MISSING

    def test_claude_quota_from_local_cache(self, worker_root):
        """Кеш есть — остаток берётся из него, а не из запроса к модели."""
        import json as _json
        import time as _time

        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        config_dir = adapter.home.config_dir
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / ".claude.json").write_text(_json.dumps({
            "cachedUsageUtilization": {
                "fetchedAtMs": int(_time.time() * 1000),
                "utilization": {
                    "five_hour": {"utilization": 16},
                    "seven_day": {"utilization": 12},
                },
            },
        }), encoding="utf-8")
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_READY
        assert snapshot.estimated_remaining_pct == 84.0
        assert snapshot.source == quota.SOURCE_LOCAL_USAGE_STATS
        assert snapshot.confidence == quota.CONFIDENCE_MEDIUM
        assert snapshot.source_stability == quota.STABILITY_UNDOCUMENTED
        assert [w.window_id for w in snapshot.secondary_windows] == ["seven_day"]

    def test_logged_out_claude_says_auth_required(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_OUT)
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_AUTH_REQUIRED
        assert snapshot.estimated_remaining_pct is None

    def test_manager_never_probes_claude_quota_automatically(self, worker_root):
        """Автоматический опрос там, где он стоил бы вызова модели, запрещён."""
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        manager = ProviderManager(worker_root=worker_root)
        manager.adapters["claude"] = adapter
        calls: list[str] = []
        original = adapter.run

        def spy(args, **kwargs):
            calls.append(" ".join(str(a) for a in args))
            return original(args, **kwargs)

        adapter.run = spy                                   # type: ignore[method-assign]
        manager.refresh(force=True)
        # Ни одного вызова с `-p`: это был бы запрос к модели.
        assert not any(call.startswith("-p") for call in calls), calls


# ═════════════════════════════════════════════════════════════════════════════
# 31.1 (продолжение) Codex через настоящий app-server-протокол
# ═════════════════════════════════════════════════════════════════════════════
class TestCodexAdapter:
    def test_reads_account_and_rate_limits_over_jsonrpc(self, worker_root):
        adapter = _codex(
            worker_root, _codex_script(account=_ACCOUNT_OK, rate=_RATE_OK)
        )
        auth = adapter.auth_status()
        assert auth.auth_state == AUTH_LOGGED_IN
        assert auth.auth_method == "chatgpt"
        assert auth.plan_type == "pro"
        snapshot = adapter.quota_status(auth=auth)
        assert snapshot.quota_state == quota.QUOTA_READY
        assert snapshot.estimated_remaining_pct == pytest.approx(40.0)

    def test_auth_required_error_is_not_reported_as_breakage(self, worker_root):
        rate_error = {
            "error": {"code": -32600,
                      "message": "codex account authentication required to read rate limits"}
        }
        account_out = {"result": {"account": None, "requiresOpenaiAuth": True}}
        adapter = _codex(
            worker_root,
            _codex_script(account=account_out, rate=rate_error,
                          login_exit=1, login_status_text="Not logged in"),
        )
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_AUTH_REQUIRED
        assert snapshot.probe_error_code == errors.ERR_AUTH_REQUIRED
        assert snapshot.estimated_remaining_pct is None

    def test_email_never_reaches_the_center_payload(self, worker_root):
        adapter = _codex(worker_root, _codex_script(account=_ACCOUNT_OK, rate=_RATE_OK))
        identity = adapter.identity()
        payload = json.dumps(identity.as_center_payload(), ensure_ascii=False)
        assert "person@example.com" not in payload
        # Отпечаток при этом есть — он и отвечает на «сменился ли аккаунт».
        assert identity.account_fingerprint

    def test_fingerprint_is_salted_per_worker(self, tmp_path):
        """Два воркера с ОДНИМ аккаунтом дают РАЗНЫЕ отпечатки.

        Так и задумано: сопоставление аккаунтов между VPS делает оператор
        через account_group_id, а не догадка по секретным данным.
        """
        adapters = []
        for name in ("a", "b"):
            root = tmp_path / name
            root.mkdir()
            adapters.append(
                _codex(root, _codex_script(account=_ACCOUNT_OK, rate=_RATE_OK))
            )
        first, second = (a.identity().account_fingerprint for a in adapters)
        assert first and second and first != second


# ═════════════════════════════════════════════════════════════════════════════
# 31.4 Безопасность и изоляция
# ═════════════════════════════════════════════════════════════════════════════
class TestSecurity:
    def test_provider_homes_are_separate_and_narrow(self, worker_root):
        manager = ProviderManager(worker_root=worker_root)
        claude = manager.adapters["claude"].home
        codex = manager.adapters["codex"].home
        assert claude.home != codex.home
        assert claude.config_dir.name == ".claude"
        assert codex.config_dir.name == ".codex"
        for path in (claude.home, codex.home):
            mode = stat.S_IMODE(os.stat(path).st_mode)
            assert mode == 0o700, oct(mode)

    def test_claude_subprocess_cannot_see_codex_home(self, worker_root):
        manager = ProviderManager(worker_root=worker_root)
        claude_env = manager.adapters["claude"].build_env()
        codex_env = manager.adapters["codex"].build_env()
        assert "CODEX_HOME" not in claude_env
        assert "CLAUDE_CONFIG_DIR" not in codex_env
        assert claude_env["HOME"] != codex_env["HOME"]
        # И наоборот: HOME одного не лежит внутри HOME другого.
        assert not str(codex_env["HOME"]).startswith(str(claude_env["HOME"]))

    def test_forbidden_env_names_are_rejected_even_if_added_later(self, worker_root):
        """Рубеж против БУДУЩЕЙ правки, а не против текущего кода."""
        from audit_worker.providers.base import ProviderEnvironmentError

        home = provider_home(worker_root, "claude")
        home.ensure_dirs()

        class Leaky(ClaudeProviderAdapter):
            def provider_env(self):
                env = super().provider_env()
                env["ANTHROPIC_API_KEY"] = "sk-ant-oops"
                return env

        with pytest.raises(ProviderEnvironmentError):
            Leaky(home).build_env()

    def test_no_token_in_process_argv_or_env(self, worker_root, monkeypatch):
        """Проверка на СОБСТВЕННОМ тестовом процессе (§28 задания)."""
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_supersecret_value")
        adapter = _claude(
            worker_root,
            'tr "\\0" "\\n" < /proc/$$/environ\n'
            'echo "ARGV:$0 $@"\n'
            "exit 0\n",
        )
        result = adapter.run(["--version"])
        assert "wtk_supersecret_value" not in result.stdout
        assert "CODEX_HOME" not in result.stdout
        assert "ANTHROPIC_API_KEY" not in result.stdout

    def test_credential_facts_never_open_the_file(self, tmp_path):
        secret = tmp_path / ".credentials.json"
        secret.write_text('{"accessToken": "СЕКРЕТ-НЕ-ЧИТАТЬ"}', encoding="utf-8")
        secret.chmod(0o600)
        facts = credential_file_facts(secret)
        assert facts["exists"] is True
        assert facts["mode"] == "0600"
        assert facts["world_readable"] is False
        # Содержимого нет ни в одном значении — структурная гарантия.
        assert "СЕКРЕТ-НЕ-ЧИТАТЬ" not in json.dumps(facts, ensure_ascii=False)

    def test_insecure_credential_permissions_raise_a_warning(self, worker_root):
        home = provider_home(worker_root, "claude")
        home.ensure_dirs()
        home.config_dir.mkdir(parents=True, exist_ok=True)
        creds = home.credential_path
        creds.write_text("{}", encoding="utf-8")
        creds.chmod(0o644)
        exe = _write_exe(home.home / ".local" / "bin" / "claude", _CLAUDE_LOGGED_IN)
        manager = ProviderManager(worker_root=worker_root, executables={"claude": exe})
        manager.refresh(force=True)
        codes = {w["code"] for w in manager.warnings()}
        assert "provider_claude_credential_permissions" in codes

    def test_center_payload_has_no_absolute_paths(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        payload = json.dumps(adapter.identity().as_center_payload(), ensure_ascii=False)
        assert str(worker_root) not in payload
        assert "/home/" not in payload

    def test_probe_is_forbidden_by_default(self, worker_root):
        for adapter in (
            _claude(worker_root, _CLAUDE_LOGGED_IN),
            _codex(worker_root, _codex_script(account=_ACCOUNT_OK, rate=_RATE_OK)),
        ):
            result = adapter.minimal_probe(confirmed_by_operator=True)
            assert result.performed is False
            assert result.allowed is False
            assert result.error_code == errors.ERR_POLICY_BLOCKED

    def test_probe_needs_two_independent_permissions(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        adapter.inference_allowed = True
        result = adapter.minimal_probe(confirmed_by_operator=False)
        assert result.performed is False
        assert result.allowed is True
        assert "подтверждения оператора" in (result.detail or "")


# ═════════════════════════════════════════════════════════════════════════════
# 31.5 Устойчивость
# ═════════════════════════════════════════════════════════════════════════════
class TestResilience:
    def test_provider_failure_does_not_raise_out_of_manager(self, worker_root):
        adapter = _claude(worker_root, "exit 1\n")
        manager = ProviderManager(worker_root=worker_root)
        manager.adapters["claude"] = adapter
        manager.refresh(force=True)                       # не должно бросить
        snapshot = manager.quota("claude")
        assert snapshot is not None
        assert snapshot.estimated_remaining_pct is None

    def test_broken_adapter_is_isolated_from_the_other_provider(self, worker_root):
        codex = _codex(worker_root, _codex_script(account=_ACCOUNT_OK, rate=_RATE_OK))
        manager = ProviderManager(worker_root=worker_root)
        manager.adapters["codex"] = codex

        class Exploding(ProviderAdapter):
            provider = "claude"

            def version(self):
                raise RuntimeError("взрыв")

            def auth_status(self):
                raise RuntimeError("взрыв")

            def quota_status(self, *, auth=None):
                raise RuntimeError("взрыв")

            def minimal_probe(self, *, confirmed_by_operator=False):
                raise RuntimeError("взрыв")

            def structured_inference(self, prompt, *, purpose, timeout_sec=None):
                # Добавлено этапом 11C: рабочий вызов модели — обязательная
                # часть интерфейса адаптера, и заглушка обязана его иметь.
                raise RuntimeError("взрыв")

            def provider_env(self):
                return {}

            def supports_zero_inference_quota(self):
                return False

            def quota_source_name(self):
                return quota.SOURCE_UNAVAILABLE

            def quota_source_stability(self):
                return quota.STABILITY_NOT_APPLICABLE

            def installed(self):
                return True

        manager.adapters["claude"] = Exploding(provider_home(worker_root, "claude"))
        manager.refresh(force=True)
        # Codex опрошен нормально, несмотря на взорвавшийся Claude.
        assert manager.quota("codex").quota_state == quota.QUOTA_READY
        assert manager.quota("claude").quota_state == quota.QUOTA_ERROR

    def test_cli_disappears_between_probes(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        manager = ProviderManager(worker_root=worker_root)
        manager.adapters["claude"] = adapter
        manager.refresh(force=True)
        assert manager.identity("claude").installation_status == INSTALL_INSTALLED
        adapter.executable_path().unlink()
        manager.refresh(force=True)
        assert manager.identity("claude").installation_status == INSTALL_MISSING

    def test_heartbeat_payload_survives_without_any_provider(self, worker_root):
        manager = ProviderManager(worker_root=worker_root, enabled=False)
        assert manager.heartbeat_payload() == []
        assert manager.warnings() == []


# ═════════════════════════════════════════════════════════════════════════════
# 31.3 Учётные записи подписок + 31.4 роли/CSRF/XSS — ЦЕНТРАЛЬНАЯ часть
# ═════════════════════════════════════════════════════════════════════════════
BOOTSTRAP = "test-bootstrap-secret-0123456789abcdef"


@pytest.fixture()
def center(tmp_path, monkeypatch):
    """Центр с включённой подсистемой и настроенными ролями."""
    from tests.distributed_workers_helpers import enable_portal_roles

    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_QUOTA_LOW_THRESHOLD_PCT", "25")
    enable_portal_roles(monkeypatch)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    st = get_settings()
    database.ensure_ready(st)
    yield st
    database.reset_state_for_tests()


def _worker(settings, name="VPS-1", instance="inst_pg000001"):
    from backend.app.services.distributed_workers import (
        registration_service, repositories, worker_registry,
    )

    row = repositories.create_worker(
        display_name=name, instance_id=instance, worker_version="0.2.0",
        protocol_version=1, pipeline_revision=None,
        capabilities={"job_types": ["test_pipeline_v1"]},
        configured_max_slots=1, settings=settings,
    )
    registration_service.approve_worker(
        worker_id=row["worker_id"], display_name=None,
        configured_max_slots=1, settings=settings,
    )
    worker_registry.record_heartbeat(
        worker_id=row["worker_id"], instance_id=instance, worker_state="idle",
        configured_max_slots=1, calculated_free_slots=1, active_jobs=[],
        resource_snapshot={"at": time.time()}, warnings=[], settings=settings,
    )
    return repositories.get_worker(row["worker_id"], settings=settings)


def _snapshot(provider="codex", group="codex-account-01", *, remaining=40.0,
              state="ready", source="official_app_server_rpc", confidence="high",
              reset_at=4_000_000_000, auth="logged_in", observed_at=None):
    return {
        "provider": provider,
        "account_group_id": group,
        "installation_status": "installed",
        "cli_version": "0.147.0",
        "auth_state": auth,
        "auth_method": "chatgpt",
        "plan_type": "pro",
        "policy_state": "allowed",
        "inference_allowed": False,
        "account_fingerprint": "abc123",
        "credential_present": True,
        "credential_mode": "0600",
        "capability": {"zero_inference_quota": True},
        "observed_at": observed_at if observed_at is not None else time.time(),
        "quota": {
            "provider": provider,
            "quota_state": state,
            "observed_at": observed_at if observed_at is not None else time.time(),
            "source": source,
            "confidence": confidence,
            "source_stability": "experimental",
            "parser_version": "codex-appserver-1",
            "primary_window": {
                "window_id": "codex:primary", "used_pct": 100.0 - (remaining or 0.0),
                "remaining_pct": remaining, "reset_at": reset_at,
                "duration_sec": 18000, "source": source, "confidence": confidence,
            },
            "secondary_windows": [],
            "next_reset_at": reset_at,
            "estimated_remaining_pct": remaining,
            "raw_remaining_supported": remaining is not None,
            "auth_state": auth,
        },
    }


class TestAccounts:
    def test_snapshot_creates_account_and_binds_worker(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        accounts = provider_accounts.list_accounts(settings=center)
        assert len(accounts) == 1
        assert accounts[0]["account_group_id"] == "codex-account-01"
        # Комплаенс по учётной записи — решение человека, автоматика его не
        # принимает за него.
        assert accounts[0]["policy_state"] == "review_required"

    def test_two_workers_one_account_are_not_summed(self, center):
        """Ключевой запрет §15: 40 % + 40 % ≠ 80 %."""
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        a = _worker(center, "VPS-A", "inst_pg00000a")
        b = _worker(center, "VPS-B", "inst_pg00000b")
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=a["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=40.0, observed_at=now - 100)],
        )
        provider_accounts.record_worker_providers(
            worker_id=b["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=40.0, observed_at=now)],
        )
        views = provider_view.accounts_overview(settings=center, now=now)
        assert len(views) == 1
        view = views[0]
        assert view["observed_remaining_pct"] == pytest.approx(40.0)
        assert view["reconciliation"]["aggregated"] is False
        assert len(view["attached_worker_ids"]) == 2
        # Выбран самый свежий из равнонадёжных.
        assert view["reconciliation"]["chosen_worker_id"] == b["worker_id"]

    def test_more_trustworthy_source_wins_over_fresher(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        a = _worker(center, "VPS-A", "inst_pg00000a")
        b = _worker(center, "VPS-B", "inst_pg00000b")
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=a["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=70.0, observed_at=now - 100,
                                 source="official_app_server_rpc")],
        )
        provider_accounts.record_worker_providers(
            worker_id=b["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=10.0, observed_at=now,
                                 source="local_usage_statistics", confidence="low")],
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["observed_remaining_pct"] == pytest.approx(70.0)
        assert view["reconciliation"]["chosen_worker_id"] == a["worker_id"]

    def test_manual_and_observed_reset_coexist(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        worker = _worker(center)
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(reset_at=now + 2 * 86400, observed_at=now)],
        )
        account = provider_accounts.list_accounts(settings=center)[0]
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-account-01", settings=center,
            manual_next_reset_at=now + 5 * 86400, reset_timezone="Europe/Moscow",
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["days_to_observed_reset"] == pytest.approx(2.0, abs=0.01)
        assert view["days_to_manual_reset"] == pytest.approx(5.0, abs=0.01)
        # Расхождение видно и НЕ «исправлено» автоматикой.
        assert view["reset_dates_disagree"] is True
        assert account["account_id"]

    def test_observation_never_overwrites_manual_reset(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        now = time.time()
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-account-01", settings=center,
            manual_next_reset_at=now + 10 * 86400,
        )
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(reset_at=now + 1 * 86400)],
        )
        account = provider_accounts.list_accounts(settings=center)[0]
        assert account["manual_next_reset_at"] == pytest.approx(now + 10 * 86400)

    @pytest.mark.parametrize("days_ahead,expected", [(6.5, 7), (2.5, 3), (0.5, 1)])
    def test_warning_thresholds(self, center, days_ahead, expected):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        _worker(center)
        now = time.time()
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-account-01", settings=center,
            manual_next_reset_at=now + days_ahead * 86400,
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        thresholds = [w["threshold_days"] for w in view["warnings_triggered"]]
        assert expected in thresholds

    def test_no_fake_percentage_when_source_absent(self, center):
        """Воркер прислал процент без признания источника — процент отброшен."""
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        bad = _snapshot(remaining=62.0)
        bad["quota"]["raw_remaining_supported"] = False
        bad["quota"]["source"] = "unavailable"
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[bad], settings=center,
        )
        state = provider_accounts.list_worker_provider_states(settings=center)[0]
        assert state["remaining_pct"] is None
        assert state["quota"]["estimated_remaining_pct"] is None
        # `ready` без остатка честно понижается до `unknown`.
        assert state["quota"]["quota_state"] == "unknown"

    def test_out_of_range_percentage_is_dropped_not_clamped(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        bad = _snapshot(remaining=40.0)
        bad["quota"]["estimated_remaining_pct"] = 4200.0
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[bad], settings=center,
        )
        state = provider_accounts.list_worker_provider_states(settings=center)[0]
        assert state["remaining_pct"] is None

    def test_reset_soon_unused_needs_known_remaining_or_operator_mark(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        worker = _worker(center)
        now = time.time()
        # Остаток неизвестен, сброс завтра → предупреждения НЕТ.
        unknown = _snapshot(remaining=None, state="unknown", reset_at=now + 86400)
        unknown["quota"]["raw_remaining_supported"] = False
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[unknown], settings=center,
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["reset_soon_unused"]["active"] is False
        assert "остаток неизвестен" in view["reset_soon_unused"]["reason"]

        # Оператор сам отметил «почти не использован» → предупреждение есть.
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-account-01",
            settings=center, operator_marked_unused=True,
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["reset_soon_unused"]["active"] is True
        assert view["reset_soon_unused"]["remaining_source"] == "operator_manual"

    def test_reset_soon_unused_with_known_high_remaining(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        worker = _worker(center)
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=85.0, reset_at=now + 86400)],
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["reset_soon_unused"]["active"] is True
        assert view["reset_soon_unused"]["remaining_source"] == "observed"

    def test_history_does_not_grow_on_every_heartbeat(self, center):
        """§24: 30-секундные повторы одного и того же в историю не идут."""
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        now = time.time()
        for i in range(10):
            provider_accounts.record_worker_providers(
                worker_id=worker["worker_id"], settings=center,
                snapshots=[_snapshot(remaining=40.0, observed_at=now + i * 30)],
            )
        account = provider_accounts.list_accounts(settings=center)[0]
        rows = provider_accounts.account_history(
            account["account_id"], settings=center
        )
        assert len(rows) == 1, rows

        # А смена значения историю двигает.
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=30.0, observed_at=now + 400)],
        )
        rows = provider_accounts.account_history(
            account["account_id"], settings=center
        )
        assert len(rows) == 2

    def test_group_id_validation(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        assert provider_accounts.normalize_group_id("Claude-Account-01") == (
            "claude-account-01"
        )
        for bad in ("", "a/b", "с кириллицей", "x" * 65, "-lead"):
            with pytest.raises(provider_accounts.ProviderAccountError):
                provider_accounts.normalize_group_id(bad)


class TestCenterApiAndRoles:
    @pytest.fixture()
    def app(self, center):
        from tests.distributed_workers_helpers import make_center_app

        return make_center_app()

    def test_viewer_can_read_but_not_mutate(self, center, app):
        from tests.distributed_workers_helpers import (
            OPERATOR_USER, VIEWER_USER, portal_client,
        )
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        account = provider_accounts.list_accounts(settings=center)[0]

        viewer = portal_client(app, username=VIEWER_USER)
        assert viewer.get("/api/workers/providers/overview").status_code == 200
        denied = viewer.put(
            f"/api/workers/providers/accounts/{account['account_id']}",
            json={"display_name": "попытка"},
        )
        assert denied.status_code == 403

        operator = portal_client(app, username=OPERATOR_USER)
        allowed = operator.put(
            f"/api/workers/providers/accounts/{account['account_id']}",
            json={"display_name": "Основная подписка Codex"},
        )
        assert allowed.status_code == 200
        assert allowed.json()["account"]["display_name"] == "Основная подписка Codex"

    def test_csrf_header_is_required(self, center, app):
        from tests.distributed_workers_helpers import (
            OPERATOR_USER, portal_client,
        )
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        account = provider_accounts.list_accounts(settings=center)[0]
        client = portal_client(app, username=OPERATOR_USER)
        blocked = client.put(
            f"/api/workers/providers/accounts/{account['account_id']}",
            json={"display_name": "без заголовка"},
            headers={"X-Requested-With": ""},
        )
        assert blocked.status_code == 403

    def test_anonymous_gets_no_access(self, center, app):
        import httpx
        from tests.distributed_workers_helpers import SyncASGITransport

        client = httpx.Client(
            transport=SyncASGITransport(app), base_url="http://center",
            headers={"X-Requested-With": "audit-workers"},
        )
        assert client.get("/api/workers/providers/overview").status_code in (401, 403)

    def test_account_name_and_notes_are_stored_verbatim_not_executed(self, center, app):
        """XSS: сервер хранит текст как есть; экранирует ОТОБРАЖЕНИЕ.

        Экран строится безопасными DOM-API (`textContent`), поэтому правильный
        инвариант здесь — «сервер не пытается чистить разметку сам и не портит
        данные», а не «сервер вырезал теги».
        """
        from tests.distributed_workers_helpers import OPERATOR_USER, portal_client
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        account = provider_accounts.list_accounts(settings=center)[0]
        payload = '<img src=x onerror=alert(1)>"; DROP TABLE workers;--'
        client = portal_client(app, username=OPERATOR_USER)
        response = client.put(
            f"/api/workers/providers/accounts/{account['account_id']}",
            json={"display_name": payload, "notes": payload},
        )
        assert response.status_code == 200
        assert response.json()["account"]["display_name"] == payload
        # Таблица на месте — SQL-инъекции не произошло.
        assert provider_accounts.list_worker_provider_states(settings=center)

    def test_frontend_never_uses_innerhtml_for_provider_data(self):
        """Структурный инвариант экрана: разметка из данных не собирается."""
        js = (_ROOT / "frontend" / "static" / "js" / "audit-workers.js").read_text(
            encoding="utf-8"
        )
        assert "innerHTML" not in js
        assert "insertAdjacentHTML" not in js

    def test_worker_provider_binding_requires_operator(self, center, app):
        from tests.distributed_workers_helpers import (
            OPERATOR_USER, VIEWER_USER, portal_client,
        )

        worker = _worker(center)
        url = f"/api/workers/{worker['worker_id']}/providers/claude/account-group"
        viewer = portal_client(app, username=VIEWER_USER)
        assert viewer.put(url, json={"account_group_id": "claude-01"}).status_code == 403
        operator = portal_client(app, username=OPERATOR_USER)
        ok = operator.put(url, json={"account_group_id": "claude-01"})
        assert ok.status_code == 200
        assert ok.json()["binding"]["account_group_id"] == "claude-01"

    def test_heartbeat_carries_providers_and_survives_bad_snapshot(self, center, app):
        """Провайдерская новость не имеет права провалить heartbeat (§27)."""
        from backend.app.services.distributed_workers import (
            provider_accounts, registration_service, repositories,
        )
        import httpx
        from tests.distributed_workers_helpers import SyncASGITransport

        row = repositories.create_worker(
            display_name="VPS-HB", instance_id="inst_pg0000hb",
            worker_version="0.2.0", protocol_version=1, pipeline_revision=None,
            capabilities={"job_types": ["test_pipeline_v1"]},
            configured_max_slots=1, settings=center,
        )
        registration_service.approve_worker(
            worker_id=row["worker_id"], display_name=None,
            configured_max_slots=1, settings=center,
        )
        # Токен выдаётся отдельным действием — ровно как в проде: одобрение и
        # выдача секрета разведены намеренно (миграция 2 схемы).
        _, token = registration_service.rotate_token(
            worker_id=row["worker_id"], settings=center,
        )
        client = httpx.Client(
            transport=SyncASGITransport(app), base_url="http://center",
            headers={"Authorization": f"Bearer {token}"},
        )
        response = client.post("/api/v1/worker/heartbeat", json={
            "instance_id": "inst_pg0000hb", "sent_at": time.time(),
            "worker_state": "idle", "configured_max_slots": 1,
            "calculated_free_slots": 1, "active_jobs": [],
            "providers": [_snapshot(), {"provider": "мусор"}, "не объект"],
        })
        assert response.status_code == 200
        states = provider_accounts.list_worker_provider_states(settings=center)
        assert [s["provider"] for s in states] == ["codex"]


class TestRankingPreview:
    def test_preview_does_not_dispatch_and_explains_itself(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view, repositories,
        )

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        result = provider_view.rank_workers_for_future_job(
            provider="codex", settings=center,
            workers=repositories.list_workers(settings=center),
        )
        assert result["auto_dispatch_enabled"] is False
        assert result["workers"]
        assert result["workers"][0]["remaining_known"] is True

    def test_unknown_remaining_does_not_float_to_the_top(self, center):
        """Ручная дата — повод предупредить, а не повод потратить лимит вслепую."""
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view, repositories,
        )

        known = _worker(center, "VPS-known", "inst_pg0000k1")
        unknown = _worker(center, "VPS-unknown", "inst_pg0000u1")
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=known["worker_id"], settings=center,
            snapshots=[_snapshot(group="codex-known", remaining=55.0,
                                 reset_at=now + 6 * 86400)],
        )
        blind = _snapshot(group="codex-unknown", remaining=None, state="unknown",
                          reset_at=None)
        blind["quota"]["raw_remaining_supported"] = False
        provider_accounts.record_worker_providers(
            worker_id=unknown["worker_id"], settings=center, snapshots=[blind],
        )
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-unknown", settings=center,
            manual_next_reset_at=now + 3600,
        )
        result = provider_view.rank_workers_for_future_job(
            provider="codex", settings=center,
            workers=repositories.list_workers(settings=center), now=now,
        )
        order = [row["worker_id"] for row in result["workers"]]
        assert order.index(known["worker_id"]) < order.index(unknown["worker_id"])


class TestNoSecretsAtRest:
    def test_center_schema_has_no_token_column(self, center):
        from backend.app.services.distributed_workers import database

        banned = {"token", "password", "refresh_token", "cookie", "api_key",
                  "access_token", "secret"}
        offenders = []
        with database.read_conn(center) as conn:
            tables = [
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            ]
            for table in ("subscription_accounts", "worker_provider_states",
                          "provider_quota_snapshots"):
                assert table in tables
                for row in conn.execute(f"PRAGMA table_info({table})"):
                    if row["name"].lower() in banned:
                        offenders.append(f"{table}.{row['name']}")
        assert not offenders, offenders

    def test_stored_snapshot_contains_no_credentials(self, center):
        from backend.app.services.distributed_workers import database, provider_accounts

        worker = _worker(center)
        snap = _snapshot()
        # Даже если воркер по ошибке приложит секрет — санитайзер собирает
        # объект из РАЗРЕШЁННЫХ полей, а не вычищает запрещённые.
        snap["access_token"] = "sk-ant-should-never-be-stored"
        snap["quota"]["access_token"] = "sk-ant-nope"
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[snap], settings=center,
        )
        with database.read_conn(center) as conn:
            blob = json.dumps(
                [dict(r) for r in conn.execute("SELECT * FROM worker_provider_states")],
                ensure_ascii=False,
            )
            blob += json.dumps(
                [dict(r) for r in conn.execute("SELECT * FROM provider_quota_snapshots")],
                ensure_ascii=False,
            )
        assert "sk-ant-should-never-be-stored" not in blob
        assert "sk-ant-nope" not in blob


# ═════════════════════════════════════════════════════════════════════════════
# Adversarial-находки этапа: каждый дефект закреплён тестом
# ═════════════════════════════════════════════════════════════════════════════
class TestAdversarialFindings:
    """Пять независимых проверок нашли 15 дефектов. Ниже — их могилы."""

    # ── редакция ответов JSON-RPC ────────────────────────────────────────────
    def test_jsonrpc_responses_are_redacted(self, worker_root):
        """Ответ app-server проходит редактор, а не едет в центр дословно.

        `limitId` из ответа попадает в `window_id` и в `detail` снимка на
        КАЖДОМ успешном опросе, а текст ошибки обновления токена — в `detail`
        при сбое. Раньше редактировались только служебные поля.
        """
        rate = {"result": {"rateLimits": {
            "limitId": "codex", "primary": {"usedPercent": 10.0,
                                            "windowDurationMins": 300,
                                            "resetsAt": 4_000_000_000},
            "rateLimitReachedType": None}}}
        account = {"result": {"account": None, "requiresOpenaiAuth": True,
                              "leaked": "Authorization: Bearer eyJhbGciOi.aaaa.bbbb"}}
        adapter = _codex(worker_root, _codex_script(account=account, rate=rate))
        rpc = adapter._app_server()
        blob = json.dumps(rpc.responses, ensure_ascii=False)
        assert "eyJhbGciOi.aaaa.bbbb" not in blob
        assert "<redacted" in blob

    # ── разбор квоты ─────────────────────────────────────────────────────────
    def test_non_numeric_percentage_gives_unknown_not_optimism(self, worker_root):
        """Строка вместо числа — расхождение контракта, а не «поля нет».

        Раньше `primary.usedPercent="97.5"` молча превращался в None, и
        снимок уходил с остатком СОСЕДНЕГО окна: оператор видел «готов,
        остаток 90 %» при выбранном на 97,5 % пятичасовом окне.
        """
        rate = {"result": {"rateLimits": {
            "limitId": "codex",
            "primary": {"usedPercent": "97.5", "windowDurationMins": 300,
                        "resetsAt": 4_000_000_000},
            "secondary": {"usedPercent": 10.0, "windowDurationMins": 10080,
                          "resetsAt": 4_000_500_000},
            "rateLimitReachedType": None}}}
        adapter = _codex(
            worker_root, _codex_script(account=_ACCOUNT_OK, rate=rate)
        )
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_UNKNOWN
        assert snapshot.estimated_remaining_pct is None
        assert snapshot.probe_error_code == errors.ERR_MALFORMED_STATUS

    def test_boolean_is_not_a_percentage(self):
        with pytest.raises(quota.QuotaContractError):
            quota.QuotaWindow(
                window_id="w", source=quota.SOURCE_OFFICIAL_APP_SERVER_RPC,
                confidence=quota.CONFIDENCE_HIGH, used_pct=True,
            )

    def test_other_buckets_do_not_drive_the_decision(self):
        """У чужого ведра свой лимит: подменять им основной остаток нельзя."""
        payload = {"rateLimitsByLimitId": {
            "codex": {"limitId": "codex",
                      "primary": {"usedPercent": 10.0, "windowDurationMins": 300,
                                  "resetsAt": 4_000_000_000},
                      "secondary": {"usedPercent": 20.0, "windowDurationMins": 10080,
                                    "resetsAt": 4_000_400_000},
                      "rateLimitReachedType": None},
            "code_review": {"limitId": "code_review",
                            "primary": {"usedPercent": 90.0, "windowDurationMins": 60,
                                        "resetsAt": 3_999_000_000},
                            "secondary": {"usedPercent": 99.0,
                                          "windowDurationMins": 10080,
                                          "resetsAt": 3_999_500_000},
                            "rateLimitReachedType": None}}}
        snapshot = _snapshot_from_rate_limits(
            payload, provider="codex", auth_state=AUTH_LOGGED_IN,
            account_group_id=None, observed_at=1_000_000.0, stale_after=1_000_900.0,
            parser_version="codex-appserver-1", low_threshold_pct=None,
        )
        # Решение — по худшему окну ВЫБРАННОГО ведра: 100 − 20 = 80.
        assert snapshot.estimated_remaining_pct == pytest.approx(80.0)
        # Дата сброса — тоже своего ведра, а не чужого (3_999_000_000).
        assert snapshot.next_reset_at == 4_000_000_000
        # Чужие окна видны справочно.
        ids = {w.window_id for w in snapshot.secondary_windows}
        assert {"code_review:primary", "code_review:secondary"} <= ids

    def test_unnamed_bucket_is_not_labelled_codex(self):
        payload = {"rateLimitsByLimitId": {
            "code_review": {"primary": {"usedPercent": 10.0,
                                        "windowDurationMins": 60,
                                        "resetsAt": 4_000_000_000}}}}
        snapshot = _snapshot_from_rate_limits(
            payload, provider="codex", auth_state=AUTH_LOGGED_IN,
            account_group_id=None, observed_at=1_000_000.0, stale_after=1_000_900.0,
            parser_version="codex-appserver-1", low_threshold_pct=None,
        )
        assert snapshot.primary_window.window_id == "code_review:primary"
        assert "limit_id=code_review" in (snapshot.detail or "")

    # ── раскладка и процессы ─────────────────────────────────────────────────
    def test_config_dir_is_created(self, worker_root):
        """`CODEX_HOME` обязан существовать заранее — документированное требование."""
        home = provider_home(worker_root, "codex")
        home.ensure_dirs()
        assert home.config_dir.is_dir()
        assert stat.S_IMODE(os.stat(home.config_dir).st_mode) == 0o700

    def test_runtime_recreated_with_narrow_mode(self, worker_root):
        adapter = _claude(worker_root, _CLAUDE_LOGGED_IN)
        import shutil

        shutil.rmtree(adapter.home.runtime)
        adapter.run(["--version"])
        assert stat.S_IMODE(os.stat(adapter.home.runtime).st_mode) == 0o700


class TestAdversarialCenterFindings:
    """Центральные находки: привязка, лимиты записей, устаревание, журнал."""

    def test_heartbeat_does_not_erase_operator_binding(self, center):
        """Главная находка: heartbeat стирал ручную привязку через такт."""
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.set_worker_provider_group(
            worker_id=worker["worker_id"], provider="codex",
            account_group_id="codex-account-01", settings=center,
        )
        # Воркер без заданной переменной шлёт account_group_id=None.
        blind = _snapshot(group=None)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[blind], settings=center,
        )
        state = provider_accounts.list_worker_provider_states(settings=center)[0]
        assert state["account_group_id"] == "codex-account-01"

    def test_worker_cannot_hijack_someone_elses_account(self, center):
        """Воркер не становится источником истины по чужой учётной записи."""
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        honest = _worker(center, "VPS-honest", "inst_pg0000h1")
        rogue = _worker(center, "VPS-rogue", "inst_pg0000r1")
        now = time.time()
        provider_accounts.set_worker_provider_group(
            worker_id=honest["worker_id"], provider="codex",
            account_group_id="codex-account-01", settings=center,
        )
        provider_accounts.record_worker_providers(
            worker_id=honest["worker_id"], settings=center,
            snapshots=[_snapshot(group=None, remaining=12.0, observed_at=now)],
        )
        # Чужой воркер объявляет ту же группу и «более надёжный» источник.
        provider_accounts.record_worker_providers(
            worker_id=rogue["worker_id"], settings=center,
            snapshots=[_snapshot(group="codex-account-01", remaining=99.0,
                                 source="official_structured_api", observed_at=now)],
        )
        views = provider_view.accounts_overview(settings=center, now=now)
        account = next(
            v for v in views if v["account_group_id"] == "codex-account-01"
        )
        # Число берётся у воркера, привязанного ОПЕРАТОРОМ.
        assert account["observed_remaining_pct"] == pytest.approx(12.0)
        assert account["reconciliation"]["chosen_worker_id"] == honest["worker_id"]
        # Самопривязавшийся воркер при этом ВИДЕН — и это правильно: прятать
        # машину, объявившую себя участником чужой подписки, значило бы лишить
        # оператора единственного шанса это заметить. Он виден и помечен как
        # самопривязка, но на число не влияет.
        contributors = {
            row["worker_id"]: row
            for row in account["reconciliation"]["contributing_workers"]
        }
        assert rogue["worker_id"] in contributors
        assert contributors[rogue["worker_id"]]["account_group_source"] == "worker"
        assert contributors[honest["worker_id"]]["account_group_source"] == "operator"

    def test_worker_cannot_spawn_unlimited_accounts(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        for i in range(30):
            provider_accounts.record_worker_providers(
                worker_id=worker["worker_id"], settings=center,
                snapshots=[_snapshot(group=f"codex-{i:03d}")],
            )
        accounts = provider_accounts.list_accounts(settings=center)
        # Первая группа закрепилась, остальные снимки её не меняют.
        assert len(accounts) == 1
        assert accounts[0]["account_group_id"] == "codex-000"

    def test_capability_size_is_bounded(self, center):
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        fat = _snapshot()
        fat["capability"] = {f"k{i}": "x" * 4000 for i in range(200)}
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[fat], settings=center,
        )
        state = provider_accounts.list_worker_provider_states(settings=center)[0]
        assert len(json.dumps(state["capability"], ensure_ascii=False)) < 20_000

    def test_future_observed_at_does_not_make_snapshot_immortal(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        worker = _worker(center)
        now = time.time()
        cheating = _snapshot(remaining=88.0, observed_at=1e18)
        cheating["quota"]["observed_at"] = 1e18
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[cheating], settings=center,
        )
        state = provider_accounts.list_worker_provider_states(settings=center)[0]
        assert state["observed_at"] <= now + 60
        view = provider_view.accounts_overview(settings=center, now=now + 10 * 3600)[0]
        assert view["quota_state"] == "stale"

    def test_low_threshold_is_applied_by_the_center(self, center):
        """Экран обещал порог, которого никто не применял."""
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view,
        )

        worker = _worker(center)
        now = time.time()
        # Воркер прислал `ready` с остатком 4 % (порог у него не настроен).
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=4.0, observed_at=now)],
        )
        view = provider_view.accounts_overview(settings=center, now=now)[0]
        assert view["quota_state"] == "low"
        assert view["low_threshold_pct"] == 25

    def test_ranking_ignores_stale_remaining(self, center):
        from backend.app.services.distributed_workers import (
            provider_accounts, provider_view, repositories,
        )

        worker = _worker(center)
        now = time.time()
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=62.0, observed_at=now - 3 * 86400)],
        )
        result = provider_view.rank_workers_for_future_job(
            provider="codex", settings=center,
            workers=repositories.list_workers(settings=center), now=now,
        )
        row = result["workers"][0]
        assert row["snapshot_stale"] is True
        assert row["remaining_known"] is False
        assert row["remaining_pct"] is None

    def test_worker_provider_row_is_marked_stale_in_overview(self, center):
        """Карточка VPS и карточка аккаунта не должны спорить друг с другом."""
        from tests.distributed_workers_helpers import (
            VIEWER_USER, make_center_app, portal_client,
        )
        from backend.app.services.distributed_workers import provider_accounts

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], settings=center,
            snapshots=[_snapshot(remaining=62.0, observed_at=time.time() - 3 * 86400)],
        )
        client = portal_client(make_center_app(), username=VIEWER_USER)
        data = client.get("/api/workers/providers/overview").json()
        row = data["worker_providers"][worker["worker_id"]][0]
        assert row["stale"] is True
        assert row["quota"]["quota_state"] == "stale"

    def test_clearing_manual_reset_is_recorded_in_the_audit_log(self, center):
        """Самая разрушительная операция формы не должна выглядеть как «ничего»."""
        from tests.distributed_workers_helpers import (
            OPERATOR_USER, make_center_app, portal_client,
        )
        from backend.app.services.distributed_workers import (
            provider_accounts, repositories,
        )

        worker = _worker(center)
        provider_accounts.record_worker_providers(
            worker_id=worker["worker_id"], snapshots=[_snapshot()], settings=center,
        )
        account = provider_accounts.list_accounts(settings=center)[0]
        provider_accounts.upsert_account(
            provider="codex", account_group_id="codex-account-01",
            settings=center, manual_next_reset_at=time.time() + 86400,
        )
        client = portal_client(make_center_app(), username=OPERATOR_USER)
        response = client.put(
            f"/api/workers/providers/accounts/{account['account_id']}",
            json={"clear_manual_reset": True},
        )
        assert response.status_code == 200
        assert provider_accounts.get_account(
            account["account_id"], settings=center
        )["manual_next_reset_at"] is None
        actions = repositories.list_admin_actions(settings=center)
        entry = next(
            a for a in actions if a["action_type"] == "provider_account_update"
        )
        assert "clear_manual_reset" in (entry.get("reason") or "")
