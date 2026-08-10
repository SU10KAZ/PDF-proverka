"""Этап 11C — конвейер `audit_pipeline_v1` доходит до модели через ProviderAdapter.

Что защищает этот файл.

11b оставил объявленный разрыв: провайдерский слой умел вызывать модель, а
конвейер о нём не знал и звал CLI сам — из-под изолированного `HOME`, то есть
неавторизованным. 11C разрыв закрывает, и вместе с ним появляется новая
опасность: путь к чужой подписке теперь идёт ЧЕРЕЗ автоматический код, а не
через команду человека. Поэтому здесь проверяются не «работает ли вызов», а
рубежи, которые он не имеет права обойти:

  * ни одно из трёх разрешений (машина / оператор / центр) не открывает вызов в
    одиночку;
  * разрешение привязано к ЗАДАНИЮ, имеет срок и списывается атомарно;
  * I-P9: одна попытка + один вызов = не больше одного оплаченного обращения,
    сколько бы раз процесс ни перезапускали и сколько бы раз результат ни
    доставляли повторно;
  * промпт не попадает в argv (I-P5) и уходит только через stdin (I-P8);
  * инструменты по-прежнему отключены, личный контекст нейтрализован;
  * секреты, приватные пути и контрольные литералы не проходят проверку
    результата;
  * SSH в рантайме вызова модели нет.

НИ ОДИН тест этого файла не обращается к настоящей модели: везде подставной
исполняемый файл. Это осознанное требование этапа — бюджет реальных вызовов
равен двум на весь этап, и тратить его на регрессии нельзя.

Прогон:
    python -m pytest tests/test_distributed_workers_pipeline_provider.py -v
"""
from __future__ import annotations

import ast
import json
import os
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from audit_worker import audit_runner                               # noqa: E402
from audit_worker.providers import (                                # noqa: E402
    errors,
    inference,
    inference_grant,
    inference_ledger,
    pipeline_bridge,
    pipeline_status,
    resolver,
)
from audit_worker.providers.auth_mode import (                      # noqa: E402
    AUTH_MODE_AMBIENT_USER,
    AUTH_MODE_ISOLATED_PROVIDER_HOME,
)
from audit_worker.providers.claude_adapter import (                 # noqa: E402
    ClaudeProviderAdapter,
    _inference_argv,
)
from audit_worker.providers.identity import AUTH_LOGGED_IN          # noqa: E402
from audit_worker.providers.manager import ProviderManager          # noqa: E402
from audit_worker.providers.paths import ProviderHome               # noqa: E402


# ─── Подставной CLI ──────────────────────────────────────────────────────────
#: Ответ модели, который отдаёт подделка. Ровно та форма, которую требует этап.
_ANSWER = {
    "contradiction_found": True,
    "values": [10, 12],
    "unit": "м3/ч",
    "source_quotes": ["проектный расход 10 м3/ч", "расход 12 м3/ч"],
    "marker": "AUDIT_PIPELINE_11C_OK",
}


def _write_exe(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body if body.startswith("#!") else "#!/bin/bash\n" + body,
                    encoding="utf-8")
    path.chmod(0o755)
    return path


def _fake_claude(path: Path, journal: Path, *, answer: str | None = None,
                 exit_code: int = 0, stall: float = 0.0) -> Path:
    """Подделка `claude`, которая ведёт журнал argv и stdin.

    Журнал — единственный канал, которого НЕ касается редактор секретов
    (I-P6), и потому единственный, на котором утверждения про argv и stdin
    осмысленны. Тот же приём, что и в тестах 11b.
    """
    payload = json.dumps(answer if answer is not None else json.dumps(
        _ANSWER, ensure_ascii=False))
    return _write_exe(path, f"""#!/bin/bash
JOURNAL={journal}
case "$1" in
  --version) echo "2.1.220 (Claude Code)"; exit 0 ;;
esac
for a in "$@"; do
  if [ "$a" = "auth" ]; then
    echo '{{"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty", "subscriptionType": "max"}}'
    exit 0
  fi
done
STDIN=$(cat)
{{
  echo "ARGV:$*"
  echo "STDIN_BYTES:${{#STDIN}}"
  echo "STDIN:$STDIN"
  tr "\\0" "\\n" < /proc/$$/environ | sed 's/^/ENV:/'
}} >> "$JOURNAL"
sleep {stall}
python3 - "$JOURNAL" <<'PYEOF'
import json, sys
answer = {payload}
print(json.dumps({{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer,
    "usage": {{"input_tokens": 120, "output_tokens": 40}},
    "modelUsage": {{"claude-opus-5[1m]": {{"inputTokens": 120}}}},
    "total_cost_usd": 0.0,
    "num_turns": 1,
}}, ensure_ascii=False))
PYEOF
exit {exit_code}
""")


@pytest.fixture()
def worker_root(tmp_path: Path) -> Path:
    root = tmp_path / "worker"
    (root / "config").mkdir(parents=True)
    (root / "runtime").mkdir(parents=True)
    # Локальная политика моделей — обязательная часть состояния воркера с 11D:
    # без неё способность заданию не во что превратить, и с 11G резолвер прямо
    # отказывается писать привязку без точной модели. Корень воркера БЕЗ
    # политики — это не «минимальная фикстура», а неисправная машина.
    (root / "provider_policy.json").write_text(json.dumps({
        "policy_version": 1,
        "claude": {
            "auth_mode": "ambient_user",
            "capabilities": {"strong_audit": {"model": "claude-opus-5"}},
        },
    }, ensure_ascii=False), encoding="utf-8")
    return root


@pytest.fixture()
def job_dir(tmp_path: Path) -> Path:
    path = tmp_path / "jobs" / "job-1" / "attempt-1"
    (path / "metadata").mkdir(parents=True)
    return path


def _binding(job_dir: Path, *, executable: Path, provider: str = "claude",
             max_inferences: int = 1, stages=("provider_selfcheck",),
             literals=()) -> resolver.ProviderBinding:
    return resolver.ProviderBinding(
        schema_version=resolver.BINDING_SCHEMA_VERSION,
        provider=provider,
        auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(resolver.ambient_root_for_attempt(job_dir, provider)),
        executable=str(executable),
        timeout_sec=30.0,
        job_id="job-1",
        attempt_id="attempt-1",
        task_id="job-1",
        grant_id="g-test-0001",
        max_inferences=max_inferences,
        allowed_stages=tuple(stages),
        # Этап 11D: рабочий вызов без НАЗНАЧЕННОЙ модели больше не выполняется
        # (иначе отвечала бы модель учётной записи по умолчанию, и ни одна
        # проверка этого не заметила бы — ровно то расхождение, которое
        # наблюдалось на боевом прогоне 11C). Привязка теперь всегда несёт
        # модель локальной политики воркера и список её допустимых форм.
        model="claude-opus-5",
        capability="strong_audit",
        accepted_reported_models=("claude-opus-5", "claude-opus-5[1m]"),
        forbidden_literals=tuple(literals),
    )


def _activate(monkeypatch, binding: resolver.ProviderBinding, job_dir: Path) -> Path:
    path = binding.write(job_dir / "metadata")
    monkeypatch.setenv(resolver.BINDING_ENV, str(path))
    return path


# ═════════════ A. Маршрутизация «конвейер → ProviderAdapter» ═════════════════
class TestPipelineRouting:
    """Штатная точка вызова CLI конвейера уходит в провайдерский слой."""

    def test_bridge_is_inactive_without_binding(self, monkeypatch):
        """На центре переменной нет — и поведение обязано остаться прежним."""
        monkeypatch.delenv(resolver.BINDING_ENV, raising=False)
        assert pipeline_bridge.active() is False

    def test_binding_pointing_nowhere_is_an_error(self, monkeypatch, tmp_path):
        """Переменная без файла — ошибка развёртывания, и она ПАДАЕТ.

        До 11D здесь возвращался `False`, и вызывающий тихо уходил на прежний
        транспорт — то есть на неавторизованный `claude -p` из-под
        изолированного HOME. Обещание докстринга «заметно на первом же вызове»
        теперь выполняется буквально.
        """
        monkeypatch.setenv(resolver.BINDING_ENV, str(tmp_path / "нет-такого.json"))
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            pipeline_bridge.active()

    @pytest.mark.asyncio
    async def test_run_cli_routes_through_the_adapter(self, monkeypatch, tmp_path,
                                                      job_dir):
        """`claude_runner._run_cli` при активной привязке зовёт адаптер.

        Проверка поведенческая: подделка CLI пишет журнал, и факт её запуска
        (а не запуска чего-то другого по PATH) виден по этому журналу.
        """
        from backend.app.services.llm import claude_runner

        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        exit_code, text, cli = await claude_runner._run_cli(
            "промпт этапа", tools="", timeout=30, stage="provider_selfcheck",
            project_id="ТЕСТ",
        )
        assert exit_code == 0
        assert json.loads(text)["marker"] == "AUDIT_PIPELINE_11C_OK"
        assert cli.input_tokens == 120 and cli.output_tokens == 40
        assert journal.is_file()

    @pytest.mark.asyncio
    async def test_stage_outside_the_whitelist_is_refused(self, monkeypatch,
                                                          tmp_path, job_dir):
        """Этап не из белого списка получает ОТКАЗ, а не обход моста.

        Молчаливый возврат к прежнему пути был бы худшим исходом: конвейер
        нашёл бы бинарь по PATH и запустил бы его неавторизованным, а выглядело
        бы это как обычная ошибка этапа.
        """
        from backend.app.services.llm import claude_runner

        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            await claude_runner._run_cli(
                "промпт", tools="", timeout=30, stage="findings_merge",
                project_id="ТЕСТ",
            )


# ═════════════ B. Выбор провайдера ═══════════════════════════════════════════
class TestProviderSelection:
    def _manager(self, worker_root: Path, exe: Path, *, mode=AUTH_MODE_AMBIENT_USER):
        manager = ProviderManager(
            worker_root=worker_root, enabled=True, timeout_sec=20.0,
            auth_modes={"claude": mode}, executables={"claude": exe},
            inference_allowed=False,
        )
        manager.refresh(force=True)
        return manager

    def test_resolver_picks_the_requested_provider(self, worker_root, tmp_path,
                                                   job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        manager = self._manager(worker_root, exe)
        assert manager.identity("claude").auth_state == AUTH_LOGGED_IN
        binding = resolver.ProviderResolver(manager, worker_root=worker_root).resolve(
            resolver.ProviderRequirement(
                provider="claude", allowed_stages=("provider_selfcheck",),
                max_inferences=1, capability="strong_audit",
            ),
            job_id="job-1", attempt_id="attempt-1", task_id="job-1",
            grant_id="g-1",
            provider_root=resolver.ambient_root_for_attempt(job_dir, "claude"),
        )
        assert binding.provider == "claude"
        assert binding.auth_mode == AUTH_MODE_AMBIENT_USER
        assert binding.executable == str(exe)
        # Точную модель дала ЛОКАЛЬНАЯ политика, а не требование.
        assert binding.model == "claude-opus-5"
        assert binding.capability == "strong_audit"

    def test_binding_without_a_model_is_refused(self, worker_root, tmp_path, job_dir):
        """Привязка без модели при разрешённых вызовах — отказ (11G).

        Иначе адаптер не передал бы CLI флаг `--model`, и вызов ушёл бы на
        модель учётной записи по умолчанию: та самая тихая подмена 11C.
        """
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        manager = self._manager(worker_root, exe)
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderResolver(manager, worker_root=worker_root).resolve(
                resolver.ProviderRequirement(
                    provider="claude", allowed_stages=("provider_selfcheck",),
                    max_inferences=1,          # способности нет
                ),
                job_id="job-1", attempt_id="attempt-1", task_id="job-1",
                grant_id="g-1",
                provider_root=resolver.ambient_root_for_attempt(job_dir, "claude"),
            )

    def test_unauthorized_provider_is_refused(self, worker_root, tmp_path, job_dir):
        exe = _write_exe(tmp_path / "bin" / "claude", """
case "$1" in --version) echo "2.1.220 (Claude Code)"; exit 0 ;; esac
echo '{"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"}'
exit 1
""")
        manager = self._manager(worker_root, exe)
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderResolver(manager, worker_root=worker_root).resolve(
                resolver.ProviderRequirement(provider="claude", max_inferences=1),
                job_id="j", attempt_id="a", task_id="j", grant_id="g",
                provider_root=job_dir / "providers" / "claude",
            )

    def test_requirement_rejects_unknown_fields(self):
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderRequirement.from_payload(
                {"provider": "claude", "temperature": 0.7}
            )

    def test_requirement_rejects_unknown_provider(self):
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderRequirement.from_payload({"provider": "gpt"})

    def test_absent_requirement_is_not_an_error(self):
        """Отсутствие требования — это «как раньше», а не негодное задание."""
        assert resolver.ProviderRequirement.from_payload(None) is None


# ═════════════ C. ambient_user ═══════════════════════════════════════════════
class TestAmbientUser:
    def test_binding_carries_the_declared_auth_mode(self, tmp_path, job_dir):
        binding = _binding(job_dir, executable=tmp_path / "claude")
        assert binding.auth_mode == AUTH_MODE_AMBIENT_USER
        assert binding.as_public_dict()["auth_mode"] == AUTH_MODE_AMBIENT_USER

    def test_provider_root_stays_inside_the_attempt(self, job_dir):
        """Каталог воркера процессу конвейера не сообщается вовсе.

        В ambient-режиме от раскладки нужны только пустой `runtime` и
        `metadata`; учётные данные лежат в личном каталоге, который адаптер
        находит через базу учётных записей. Значит и указывать корень внутри
        каталога данных воркера незачем — а «незачем» здесь означает «нельзя»:
        рядом лежат `worker.db` и токен.
        """
        root = resolver.ambient_root_for_attempt(job_dir, "claude")
        assert job_dir.resolve() in root.resolve().parents

    def test_public_view_has_no_absolute_paths(self, tmp_path, job_dir):
        public = _binding(job_dir, executable=tmp_path / "claude").as_public_dict()
        assert "provider_root" not in public
        assert "executable" not in public
        assert "forbidden_literals" not in public


# ═════════════ D/E. Разрешение оператора ═════════════════════════════════════
class TestInferenceGrant:
    def test_missing_grant_file_is_refused(self, worker_root):
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")

    def test_grant_is_bound_to_the_task(self, worker_root):
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-2")

    def test_grant_is_bound_to_the_provider(self, worker_root):
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="codex", task_id="job-1")

    def test_expired_grant_is_refused(self, worker_root):
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=-1)
        with pytest.raises(inference_grant.InferenceGrantError) as exc:
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")
        assert "просроч" in str(exc.value)

    def test_single_use_budget_is_consumed_once(self, worker_root):
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600, max_uses=1)
        record = inference_grant.consume(worker_root, provider="claude",
                                         task_id="job-1")
        assert record.used == 1 and record.remaining == 0
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")

    def test_consumption_survives_a_crash(self, worker_root):
        """Списание попадает на диск ДО вызова модели.

        Проверяется тем же способом, что и у `probe_grant`: состояние читается
        заново, из файла, а не из объекта в памяти.
        """
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600, max_uses=1)
        inference_grant.consume(worker_root, provider="claude", task_id="job-1")
        reread = inference_grant.read_records(worker_root)
        assert reread[0].used == 1

    def test_world_readable_grant_is_rejected(self, worker_root):
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600)
        os.chmod(inference_grant.grant_path(worker_root), 0o644)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")

    def test_symlink_is_rejected(self, worker_root, tmp_path):
        real = tmp_path / "real_grant"
        real.write_text('{"schema_version": 1, "grants": []}', encoding="utf-8")
        real.chmod(0o600)
        inference_grant.grant_path(worker_root).symlink_to(real)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")

    def test_malformed_file_is_an_error_not_an_empty_list(self, worker_root):
        path = inference_grant.grant_path(worker_root)
        path.write_text("{не json}", encoding="utf-8")
        path.chmod(0o600)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.read_records(worker_root)

    def test_unknown_schema_version_is_refused(self, worker_root):
        path = inference_grant.grant_path(worker_root)
        path.write_text(json.dumps({"schema_version": 99, "grants": []}),
                        encoding="utf-8")
        path.chmod(0o600)
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.read_records(worker_root)

    def test_concurrent_consumption_yields_a_single_winner(self, worker_root):
        """Атомарность списания — на РАЗНЫХ процессах, а не на потоках.

        Потоки одного питона делили бы GIL и могли бы «сойтись» случайно;
        процессы такой поблажки не дают.
        """
        import subprocess

        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600, max_uses=1)
        script = (
            "import sys;sys.path.insert(0, %r);"
            "from audit_worker.providers import inference_grant as g;"
            "\ntry:\n"
            "    g.consume(%r, provider='claude', task_id='job-1');print('WON')\n"
            "except Exception:\n    print('LOST')\n"
        ) % (str(REPO_ROOT), str(worker_root))
        procs = [
            subprocess.Popen([sys.executable, "-c", script],
                             stdout=subprocess.PIPE, text=True)
            for _ in range(6)
        ]
        outs = [p.communicate()[0].strip() for p in procs]
        assert outs.count("WON") == 1, outs


# ═════════════ F/G/Q. I-P9 — exactly once ════════════════════════════════════
class TestExactlyOnceInference:
    def _ledger(self, job_dir: Path) -> inference_ledger.InferenceLedger:
        return inference_ledger.InferenceLedger(job_dir, attempt_id="attempt-1",
                                                job_id="job-1")

    def test_first_entry_is_allowed(self, job_dir):
        ledger = self._ledger(job_dir)
        entry = ledger.inspect("k1")
        assert entry.state == inference_ledger.STATE_ALLOWED

    def test_claim_blocks_the_second_call(self, job_dir):
        ledger = self._ledger(job_dir)
        assert ledger.begin("k1", provider="claude", purpose="p",
                            prompt_sha256="x").state == inference_ledger.STATE_ALLOWED
        second = ledger.begin("k1", provider="claude", purpose="p", prompt_sha256="x")
        assert second.state == inference_ledger.STATE_INDETERMINATE

    def test_saved_result_is_replayed_not_recomputed(self, job_dir):
        ledger = self._ledger(job_dir)
        ledger.begin("k1", provider="claude", purpose="p", prompt_sha256="x")
        ledger.complete("k1", inference.ProviderInferenceResult(
            provider="claude", model="m", status=inference.STATUS_SUCCESS,
            result={"a": 1}, exit_code=0, auth_mode=AUTH_MODE_AMBIENT_USER,
        ))
        entry = ledger.inspect("k1")
        assert entry.state == inference_ledger.STATE_REPLAY
        assert entry.result.result == {"a": 1}

    def test_error_result_is_also_recorded(self, job_dir):
        """Ошибочный ответ — тоже израсходованная попытка."""
        ledger = self._ledger(job_dir)
        ledger.begin("k1", provider="claude", purpose="p", prompt_sha256="x")
        ledger.complete("k1", inference.ProviderInferenceResult(
            provider="claude", model=None, status=inference.STATUS_ERROR,
            exit_code=1, auth_mode=AUTH_MODE_AMBIENT_USER,
        ))
        assert self._ledger(job_dir).inspect("k1").state == inference_ledger.STATE_REPLAY

    def test_bridge_replays_instead_of_calling_the_model(self, monkeypatch,
                                                        tmp_path, job_dir):
        """Второй проход НЕ запускает подпроцесс — это видно по журналу."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        binding = _binding(job_dir, executable=exe)
        _activate(monkeypatch, binding, job_dir)

        first = pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="фрагмент",
        )
        assert first.performed is True
        calls_after_first = journal.read_text(encoding="utf-8").count("ARGV:")

        second = pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="фрагмент",
        )
        assert second.performed is False
        assert second.provider_result.raw_sha256 == first.provider_result.raw_sha256
        assert journal.read_text(encoding="utf-8").count("ARGV:") == calls_after_first

    def test_crash_after_call_forbids_an_automatic_retry(self, monkeypatch,
                                                        tmp_path, job_dir):
        """Заявка без результата = исход неизвестен → повтор запрещён.

        Это и есть безопасная сторона инварианта: «на всякий случай повторить»
        означало бы второй оплаченный вызов там, где первый мог пройти.
        """
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        binding = _binding(job_dir, executable=exe)
        _activate(monkeypatch, binding, job_dir)
        key = inference_ledger.call_key(
            attempt_id="attempt-1", provider="claude",
            purpose="provider_selfcheck", prompt="фрагмент",
        )
        self._ledger(job_dir).begin(key, provider="claude",
                                    purpose="provider_selfcheck",
                                    prompt_sha256="x")
        with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
            pipeline_bridge.run_stage_inference(
                job_dir=job_dir, stage="provider_selfcheck", prompt="фрагмент",
            )
        assert "I-P9" in str(exc.value)

    def test_ceiling_of_calls_per_attempt(self, monkeypatch, tmp_path, job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _activate(monkeypatch, _binding(job_dir, executable=exe, max_inferences=1),
                  job_dir)
        pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="первый",
        )
        with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
            pipeline_bridge.run_stage_inference(
                job_dir=job_dir, stage="provider_selfcheck", prompt="второй",
            )
        assert "потолок" in str(exc.value)

    def test_binding_without_grant_id_refuses_the_call(self, monkeypatch,
                                                       tmp_path, job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        binding = _binding(job_dir, executable=exe)
        binding = resolver.ProviderBinding.from_dict(
            {**binding.as_dict(), "grant_id": ""}
        )
        _activate(monkeypatch, binding, job_dir)
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            pipeline_bridge.run_stage_inference(
                job_dir=job_dir, stage="provider_selfcheck", prompt="ф",
            )

    def test_ledger_survives_a_process_restart(self, monkeypatch, tmp_path, job_dir):
        """Журнал — файлы, а не память: новый процесс видит то же состояние."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф",
        )
        import subprocess

        script = (
            "import sys, json;sys.path.insert(0, %r);"
            "from audit_worker.providers.inference_ledger import InferenceLedger;"
            "print(json.dumps(InferenceLedger(%r, attempt_id='attempt-1').summary()))"
        ) % (str(REPO_ROOT), str(job_dir))
        out = subprocess.run([sys.executable, "-c", script], capture_output=True,
                             text=True, timeout=120).stdout
        summary = json.loads(out)
        assert summary["calls_started"] == 1 and summary["calls_completed"] == 1


# ═════════════ I/J/K. Проверка результата ════════════════════════════════════
class TestResultValidation:
    def _result(self, **kwargs):
        base = dict(provider="claude", model="claude-opus-5[1m]",
                    status=inference.STATUS_SUCCESS, exit_code=0,
                    auth_mode=AUTH_MODE_AMBIENT_USER, result=dict(_ANSWER))
        base.update(kwargs)
        return inference.ProviderInferenceResult(**base)

    def _validate(self, result, **kwargs):
        params = dict(
            expected_provider="claude", expected_auth_mode=AUTH_MODE_AMBIENT_USER,
            required_result_fields=("contradiction_found", "values", "marker"),
            field_types={"contradiction_found": bool, "values": list},
            expected_semantics={"contradiction_found": True,
                                "marker": "AUDIT_PIPELINE_11C_OK"},
            task_id="job-1", attempt_id="attempt-1",
            claim_task_id="job-1", claim_attempt_id="attempt-1",
        )
        params.update(kwargs)
        return inference.validate_inference(result, **params)

    def test_good_result_passes(self):
        assert self._validate(self._result()).passed

    def test_nonzero_exit_fails(self):
        report = self._validate(self._result(exit_code=3,
                                             status=inference.STATUS_ERROR))
        assert not report.passed
        assert "exit_code" in report.failed_names

    def test_unparsed_json_fails(self):
        report = self._validate(self._result(result={},
                                             status=inference.STATUS_ERROR))
        assert "json_parsed" in report.failed_names

    def test_missing_field_fails(self):
        payload = dict(_ANSWER)
        payload.pop("marker")
        assert "required_fields" in self._validate(
            self._result(result=payload)).failed_names

    def test_wrong_type_fails(self):
        payload = dict(_ANSWER, values="10 и 12")
        assert "field_types" in self._validate(
            self._result(result=payload)).failed_names

    def test_wrong_semantics_fails(self):
        payload = dict(_ANSWER, contradiction_found=False)
        assert "expected_semantics" in self._validate(
            self._result(result=payload)).failed_names

    def test_credential_like_string_fails(self):
        payload = dict(_ANSWER, unit="sk-ant-api03-AAAAAAAAAAAAAAAA")
        report = self._validate(self._result(result=payload))
        assert "no_credential_like" in report.failed_names

    def test_private_path_fails(self):
        payload = dict(_ANSWER, unit="/home/coder/.claude/.credentials.json")
        report = self._validate(self._result(result=payload))
        assert "no_private_paths" in report.failed_names

    def test_forbidden_literal_fails(self):
        """Контрольная строка (canary) в ответе — провал проверки."""
        # Строка НАМЕРЕННО не похожа на настоящий контрольный файл: даже
        # префикс реальной канарейки в репозитории заводить незачем.
        secret = "ПРОВЕРОЧНАЯ-СТРОКА-ОПЕРАТОРА-4711"
        payload = dict(_ANSWER, unit=f"нашёл {secret}")
        report = self._validate(self._result(result=payload),
                                forbidden_literals=(secret,))
        assert "no_forbidden_literals" in report.failed_names

    def test_report_never_contains_the_secret_itself(self):
        # Строка НАМЕРЕННО не похожа на настоящий контрольный файл: даже
        # префикс реальной канарейки в репозитории заводить незачем.
        secret = "ПРОВЕРОЧНАЯ-СТРОКА-ОПЕРАТОРА-4711"
        payload = dict(_ANSWER, unit=f"нашёл {secret}")
        report = self._validate(self._result(result=payload),
                                forbidden_literals=(secret,))
        assert secret not in json.dumps(report.as_dict(), ensure_ascii=False)

    def test_foreign_provider_fails(self):
        assert "provider_matches_task" in self._validate(
            self._result(provider="codex")).failed_names

    def test_identity_mismatch_fails(self):
        assert "identity_matches_claim" in self._validate(
            self._result(), claim_attempt_id="attempt-2").failed_names

    def test_pipeline_result_is_failed_when_validation_fails(self):
        result = self._result(result={})
        report = self._validate(result)
        envelope = inference.build_pipeline_result(
            task_id="job-1", attempt_id="attempt-1",
            provider_result=result, validation=report,
        )
        assert envelope["status"] == "failed"
        assert envelope["pipeline"] == "audit_pipeline_v1"

    def test_raw_answer_is_not_stored_in_the_contract(self):
        """В контракте — отпечаток, а не текст: он уезжает в пакет и в лог."""
        result = self._result(raw_sha256=inference.sha256_text("длинный ответ"))
        payload = result.as_dict()
        assert "raw_text" not in payload and "stdout" not in payload
        assert len(payload["raw_sha256"]) == 64


# ═════════════ L/M/N/O. Плохие ответы провайдера ═════════════════════════════
class TestProviderFailures:
    def test_invalid_json_is_an_error_not_an_empty_success(self, monkeypatch,
                                                           tmp_path, job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt",
                           answer="это не json")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        outcome = pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф",
        )
        assert outcome.provider_result.status == inference.STATUS_ERROR
        assert outcome.provider_result.error_code == errors.ERR_MALFORMED_STATUS

    def test_nonzero_exit_code_is_reported(self, monkeypatch, tmp_path, job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt",
                           exit_code=7)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        outcome = pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф",
        )
        assert outcome.provider_result.exit_code == 7
        assert outcome.provider_result.status == inference.STATUS_ERROR

    def test_timeout_kills_the_process_group(self, monkeypatch, tmp_path, job_dir):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt", stall=30)
        binding = resolver.ProviderBinding.from_dict(
            {**_binding(job_dir, executable=exe).as_dict(), "timeout_sec": 1.0}
        )
        _activate(monkeypatch, binding, job_dir)
        outcome = pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф", timeout_sec=1.0,
        )
        assert outcome.provider_result.error_code == errors.ERR_TIMEOUT

    def test_rate_limit_text_is_classified(self):
        """Отказ по лимиту обязан отличаться от «сломался»."""
        assert errors.classify_text("Claude usage limit reached") == (
            errors.ERR_RATE_LIMITED
        )

    def test_timed_out_call_is_still_recorded_in_the_ledger(self, monkeypatch,
                                                            tmp_path, job_dir):
        """Таймаут — израсходованная попытка: запрос мог уйти и быть оплачен."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt", stall=30)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф", timeout_sec=1.0,
        )
        summary = inference_ledger.InferenceLedger(
            job_dir, attempt_id="attempt-1").summary()
        assert summary["calls_started"] == 1 and summary["calls_completed"] == 1


# ═════════════ U/V/W. Форма запуска CLI ══════════════════════════════════════
class TestInvocationShape:
    def test_tools_are_disabled_and_personal_context_neutralized(self):
        argv = _inference_argv()
        assert "--tools=" in argv
        assert "--safe-mode" in argv
        assert "--strict-mcp-config" in argv
        assert "--setting-sources=" in argv
        assert "--no-session-persistence" in argv
        assert any(a.startswith("--disallowed-tools=") for a in argv)

    def test_no_variadic_flag_takes_a_separate_value(self):
        """Регрессия I-P8: вариадические флаги только в форме `--флаг=значение`.

        Записанные как `--tools ""` они забирают следующий токен. Один раз это
        уже стоило незапланированного запроса к модели.
        """
        variadic = ("--tools", "--disallowed-tools", "--allowedTools", "--add-dir")
        argv = _inference_argv()
        for name in variadic:
            assert name not in argv, f"{name} записан отдельным токеном"

    def test_prompt_never_appears_in_argv(self, monkeypatch, tmp_path, job_dir):
        """I-P5 дословно: argv состоит только из констант модуля.

        Проверка поведенческая — по журналу подделки, а не по константам:
        именно так ловится будущая правка, которая решит «ну добавим промпт
        последним аргументом».
        """
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        secret_prompt = "СЕКРЕТНЫЙ-ФРАГМЕНТ-ЗАДАНИЯ-42"
        pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt=secret_prompt,
        )
        text = journal.read_text(encoding="utf-8")
        argv_line = next(line for line in text.splitlines() if line.startswith("ARGV:"))
        assert secret_prompt not in argv_line
        assert f"STDIN:{secret_prompt}" in text

    def test_worker_secrets_do_not_reach_the_subprocess(self, monkeypatch,
                                                        tmp_path, job_dir):
        """I-P2 на живом процессе и по НЕредактированному каналу."""
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_supersecret_value")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-supersecret-value")
        monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.example")
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        pipeline_bridge.run_stage_inference(
            job_dir=job_dir, stage="provider_selfcheck", prompt="ф",
        )
        dump = journal.read_text(encoding="utf-8")
        assert "wtk_supersecret_value" not in dump
        assert "sk-ant-supersecret-value" not in dump
        assert "center.example" not in dump
        assert "AUDIT_WORKER_PROVIDER_BINDING" not in dump

    def test_stdin_is_devnull_for_status_calls(self, tmp_path):
        """I-P8: там, где своего ввода нет, подпроцесс получает /dev/null.

        Подделка читает stdin и падает, если он унаследован от вызывающего с
        непустым содержимым.
        """
        journal = tmp_path / "stdin.txt"
        exe = _write_exe(tmp_path / "bin" / "claude", f"""
if [ "$1" = "--version" ]; then echo "2.1.220 (Claude Code)"; exit 0; fi
cat > {journal}
echo '{{"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty"}}'
exit 0
""")
        home = ProviderHome(provider="claude", root=tmp_path / "prov",
                            auth_mode=AUTH_MODE_ISOLATED_PROVIDER_HOME)
        adapter = ClaudeProviderAdapter(home, executable=exe, timeout_sec=20.0)
        adapter.auth_status()
        assert journal.read_text(encoding="utf-8") == ""


# ═════════════ X. SSH не участвует в вызове модели ═══════════════════════════
class TestNoSshInference:
    #: Модули, через которые проходит вызов модели этапа 11C.
    RUNTIME_MODULES = (
        "audit_worker/providers/pipeline_bridge.py",
        "audit_worker/providers/resolver.py",
        "audit_worker/providers/inference.py",
        "audit_worker/providers/inference_grant.py",
        "audit_worker/providers/inference_ledger.py",
        "audit_worker/providers/base.py",
        "audit_worker/providers/claude_adapter.py",
        "audit_worker/providers/codex_adapter.py",
        "backend/app/pipeline/stages/provider_selfcheck/__init__.py",
    )

    FORBIDDEN = ("ssh", "scp", "paramiko", "fabric", "sshpass", "rsync")

    def test_runtime_path_has_no_remote_execution(self):
        """Структурная проверка: в рантайме вызова модели нет удалённого запуска.

        Инвариант структурный по природе — «SSH не используется» иначе как
        отсутствием вызовов не выражается. Докстринги исключены: в них про SSH
        написано намеренно.
        """
        for rel in self.RUNTIME_MODULES:
            source = (REPO_ROOT / rel).read_text(encoding="utf-8")
            tree = ast.parse(source)
            docstrings = set()
            # ТОЛЬКО узлы, у которых `body` — список операторов. У `IfExp`,
            # `Lambda` и прочих `body` тоже есть, но это ВЫРАЖЕНИЕ, и `body[0]`
            # на нём падает. Та же ловушка, что описана в тестах 11b, только с
            # другой стороны.
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                         ast.AsyncFunctionDef)):
                    continue
                body = getattr(node, "body", None)
                if not body:
                    continue
                first = body[0]
                if (isinstance(first, ast.Expr)
                        and isinstance(first.value, ast.Constant)
                        and isinstance(first.value.value, str)):
                    docstrings.add(id(first.value))
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    if id(node) in docstrings:
                        continue
                    lowered = node.value.lower()
                    for word in self.FORBIDDEN:
                        assert word not in lowered.split(), (
                            f"{rel}: строка со словом {word!r} на пути вызова модели"
                        )
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    names = [a.name for a in getattr(node, "names", [])]
                    names.append(getattr(node, "module", "") or "")
                    for name in names:
                        assert not any(
                            name.startswith(word) for word in ("paramiko", "fabric")
                        ), f"{rel}: импорт {name}"

    def test_adapter_runs_a_local_absolute_executable(self, tmp_path):
        """Путь к CLI абсолютный и локальный — удалённого запуска не бывает."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        home = ProviderHome(provider="claude", root=tmp_path / "prov",
                            auth_mode=AUTH_MODE_ISOLATED_PROVIDER_HOME)
        adapter = ClaudeProviderAdapter(home, executable=exe)
        assert Path(adapter.executable_path()).is_absolute()
        assert Path(adapter.executable_path()).is_file()


# ═════════════ Контракты задания и границы модулей ═══════════════════════════
class TestJobContract:
    def test_binding_env_name_matches_provider_layer(self):
        """Литерал в `audit_runner` и константа слоя обязаны совпадать.

        `audit_runner` намеренно не импортирует провайдерский слой (граница
        11b), поэтому имя переменной там записано литералом. Единственное, что
        удерживает два места вместе, — этот тест.
        """
        assert audit_runner.PROVIDER_BINDING_ENV == resolver.BINDING_ENV

    def test_pipeline_runner_still_does_not_import_the_provider_layer(self):
        """Граница 11b СУЖЕНА, но не снята.

        До 11C правило звучало «конвейер о провайдерском слое не знает». Теперь
        оно звучит точнее: **модуль, который строит argv и окружение процесса
        конвейера, о провайдерском слое по-прежнему не знает** — ему нужно
        только имя переменной. Знание о провайдерах живёт там, где принимается
        решение (исполнитель), и там, где вызывается модель (мост).
        """
        tree = ast.parse((REPO_ROOT / "audit_worker/audit_runner.py").read_text(
            encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("audit_worker.providers")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("audit_worker.providers")

    def test_synthetic_action_has_its_own_required_artifacts(self):
        synthetic = audit_runner.required_artifacts_for("provider_selfcheck")
        assert "result/provider_selfcheck.json" in synthetic
        assert "result/03_findings.json" not in synthetic
        assert "result/03_findings.json" in audit_runner.required_artifacts_for("full")

    def test_selfcheck_without_requirement_is_rejected(self, tmp_path):
        class _Config:
            pipeline_revision = "rev"
            audit_pipeline_enabled = True
            pipeline_root = tmp_path

        with pytest.raises(audit_runner.AuditJobRejected):
            audit_runner.validate_params(
                {
                    "execution_profile": audit_runner.SUPPORTED_PROFILE,
                    "action": "provider_selfcheck",
                    "pipeline_revision": "rev",
                    "runtime_snapshot_hash": "a" * 16,
                    "discipline_id": "VK",
                    "discipline_profile_hash": "b" * 16,
                },
                config=_Config(),
            )

    def test_requirement_travels_into_the_spec(self, tmp_path):
        class _Config:
            pipeline_revision = "rev"
            audit_pipeline_enabled = True
            pipeline_root = tmp_path

        params = audit_runner.validate_params(
            {
                "execution_profile": audit_runner.SUPPORTED_PROFILE,
                "action": "provider_selfcheck",
                "pipeline_revision": "rev",
                "runtime_snapshot_hash": "a" * 16,
                "discipline_id": "VK",
                "discipline_profile_hash": "b" * 16,
                "provider_requirement": {
                    "provider": "claude",
                    "capability": "strong_audit",
                    "allowed_stages": ["provider_selfcheck"],
                    "max_inferences": 1,
                },
            },
            config=_Config(),
        )
        assert params.provider_requirement["provider"] == "claude"
        assert params.provider_requirement["capability"] == "strong_audit"
        assert params.as_dict()["provider_requirement"]["max_inferences"] == 1
        # Точной модели в требовании нет ни в каком виде (11G).
        assert params.provider_requirement["model"] is None

    def test_binding_is_only_added_to_env_when_present(self, tmp_path):
        class _Config:
            pipeline_root = tmp_path
            allow_real_llm = True

        without = audit_runner.build_env(config=_Config(), job_dir=tmp_path,
                                         provider_dir=None)
        assert resolver.BINDING_ENV not in without
        with_binding = audit_runner.build_env(
            config=_Config(), job_dir=tmp_path, provider_dir=None,
            provider_binding=tmp_path / "metadata" / "provider_binding.json",
        )
        assert with_binding[resolver.BINDING_ENV].endswith("provider_binding.json")

    def test_worker_runtime_never_issues_a_freeform_grant(self):
        """`issue()` — инструмент ОПЕРАТОРА. В рантайме воркера его нет.

        Что изменилось на 11G и что осталось. Осталось: воркер не вправе
        выписать себе разрешение с произвольными параметрами — `issue()`
        принимает и число использований, и срок, и задание как есть, и вызов
        такой функции из рантайма означал бы «сам себе разрешил сколько
        захотел». Изменилось: появился `issue_for_job()`, у которого свободных
        параметров нет вовсе — всё выводится из задания центра и зажимается
        потолком МАШИНЫ, заданным администратором VPS заранее.

        Поэтому проверка стала точнее, а не слабее: свободная форма запрещена
        по-прежнему, а связанная разрешена ровно одному месту — исполнителю,
        который и так единственный имеет право войти в оплачиваемый канал.
        """
        runtime = [
            "audit_worker/executor.py",
            "audit_worker/agent.py",
            "audit_worker/providers/pipeline_bridge.py",
            "audit_worker/providers/manager.py",
        ]
        for rel in runtime:
            source = (REPO_ROOT / rel).read_text(encoding="utf-8")
            assert "inference_grant.issue(" not in source, rel
            if rel != "audit_worker/executor.py":
                assert "issue_for_job" not in source, rel
        executor = (REPO_ROOT / "audit_worker/executor.py").read_text(encoding="utf-8")
        assert executor.count("inference_grant.issue_for_job(") == 1

    def test_auto_grant_refuses_without_a_machine_ceiling(self, worker_root):
        """Потолок задаёт владелец VPS. Ноль означает «автоматических нет»."""
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.issue_for_job(
                worker_root, provider="claude", job_id="job-1",
                attempt_id="a-1", capability="strong_audit",
                requested_max_inferences=4, machine_ceiling=0, ttl_sec=600,
            )

    def test_auto_grant_refuses_to_silently_trim_the_request(self, worker_root):
        """Урезать требование молча нельзя: аудит оборвался бы оплаченным."""
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.issue_for_job(
                worker_root, provider="claude", job_id="job-1",
                attempt_id="a-1", capability="strong_audit",
                requested_max_inferences=9, machine_ceiling=4, ttl_sec=600,
            )

    def test_auto_grant_is_idempotent_per_attempt(self, worker_root):
        """Повторный вход не обнуляет потраченное и не создаёт вторую запись."""
        first = inference_grant.issue_for_job(
            worker_root, provider="claude", job_id="job-1", attempt_id="a-1",
            capability="strong_audit", requested_max_inferences=9,
            machine_ceiling=12, ttl_sec=600,
        )
        inference_grant.consume(worker_root, provider="claude", task_id="job-1")
        again = inference_grant.issue_for_job(
            worker_root, provider="claude", job_id="job-1", attempt_id="a-1",
            capability="strong_audit", requested_max_inferences=9,
            machine_ceiling=12, ttl_sec=600,
        )
        assert again.grant_id == first.grant_id
        assert again.used == 1, "перевыписка вернула бы оплаченной попытке новый прогон"
        assert len(inference_grant.read_records(worker_root)) == 1

    def test_second_attempt_of_the_same_job_gets_its_own_grant(self, worker_root):
        """Разрешение исчерпанной попытки не блокирует новую.

        `consume` ищет ПРИГОДНУЮ запись, а не первую совпавшую по заданию:
        под одним заданием их теперь несколько — по одной на попытку.
        """
        inference_grant.issue_for_job(
            worker_root, provider="claude", job_id="job-1", attempt_id="a-1",
            capability="strong_audit", requested_max_inferences=9,
            machine_ceiling=12, ttl_sec=600,
        )
        for _ in range(inference_grant.GRANT_MAX_ENTRIES_PER_ATTEMPT):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.consume(worker_root, provider="claude", task_id="job-1")
        inference_grant.issue_for_job(
            worker_root, provider="claude", job_id="job-1", attempt_id="a-2",
            capability="strong_audit", requested_max_inferences=9,
            machine_ceiling=12, ttl_sec=600,
        )
        used = inference_grant.consume(
            worker_root, provider="claude", task_id="job-1"
        )
        assert used.grant_id == "auto-a-2"


# ═════════════ Heartbeat ═════════════════════════════════════════════════════
class TestHeartbeat:
    def test_capability_reports_bridge_and_grant(self, worker_root, tmp_path):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="job-1", ttl_sec=600)
        manager = ProviderManager(
            worker_root=worker_root, enabled=True, timeout_sec=20.0,
            auth_modes={"claude": AUTH_MODE_AMBIENT_USER},
            executables={"claude": exe}, inference_allowed=False,
            pipeline_bridge_enabled=True,
        )
        manager.refresh(force=True)
        payload = {row["provider"]: row for row in manager.heartbeat_payload()}
        capability = payload["claude"]["capability"]
        assert capability["pipeline_bridge_enabled"] is True
        assert capability["pipeline_inference_grant"]["remaining_total"] == 1
        assert capability["real_inference_allowed"] is True

    def test_real_inference_stays_forbidden_without_a_grant(self, worker_root,
                                                            tmp_path):
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        manager = ProviderManager(
            worker_root=worker_root, enabled=True, timeout_sec=20.0,
            auth_modes={"claude": AUTH_MODE_AMBIENT_USER},
            executables={"claude": exe}, inference_allowed=False,
            pipeline_bridge_enabled=True,
        )
        manager.refresh(force=True)
        payload = {row["provider"]: row for row in manager.heartbeat_payload()}
        assert payload["claude"]["capability"]["real_inference_allowed"] is False

    def test_heartbeat_carries_no_paths_or_task_ids(self, worker_root, tmp_path):
        """Ни домашнего каталога, ни имени задания, ни пути к учётным данным."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        inference_grant.issue(worker_root, grant_id="g1", provider="claude",
                              task_id="СЕКРЕТНОЕ-ЗАДАНИЕ", ttl_sec=600,
                              note="заметка оператора")
        manager = ProviderManager(
            worker_root=worker_root, enabled=True, timeout_sec=20.0,
            auth_modes={"claude": AUTH_MODE_AMBIENT_USER},
            executables={"claude": exe}, inference_allowed=False,
            pipeline_bridge_enabled=True,
        )
        manager.refresh(force=True)
        dump = json.dumps(manager.heartbeat_payload(), ensure_ascii=False,
                          default=str)
        assert "СЕКРЕТНОЕ-ЗАДАНИЕ" not in dump
        assert "заметка оператора" not in dump
        assert str(worker_root) not in dump

    def test_pipeline_status_marker_roundtrip(self, worker_root):
        pipeline_status.record(worker_root, provider="claude", calls_started=1,
                               calls_completed=1)
        marker = pipeline_status.read(worker_root)
        assert marker["provider"] == "claude" and marker["calls_completed"] == 1
        assert marker["observed_at"] <= time.time() + 1


# ═════════════ Синтетическая фикстура ════════════════════════════════════════
class TestSyntheticFixture:
    def test_fragment_is_extracted_from_the_version_markdown(self):
        from backend.app.pipeline.stages import provider_selfcheck as stage

        text = (
            "# Документ\n\n## Лист 1\n\nтекст\n\n"
            f"{stage.FIXTURE_HEADING}\n\n"
            "Насос P-1: проектный расход 10 м3/ч.\n"
            "Таблица оборудования: 12 м3/ч.\n"
        )
        fragment = stage.extract_fragment(text)
        assert "10" in fragment and "12" in fragment
        assert "Лист 1" not in fragment

    def test_missing_fixture_section_is_an_error(self):
        from backend.app.pipeline.stages import provider_selfcheck as stage

        with pytest.raises(stage.ProviderSelfcheckError):
            stage.extract_fragment("# Документ без фикстуры\n")

    def test_oversized_fragment_is_refused(self):
        """Рубеж расхода: в модель уходит маленькая фикстура, а не документ."""
        from backend.app.pipeline.stages import provider_selfcheck as stage

        text = f"{stage.FIXTURE_HEADING}\n\n" + ("х" * (stage.MAX_FRAGMENT_CHARS + 1))
        with pytest.raises(stage.ProviderSelfcheckError):
            stage.extract_fragment(text)

    def test_prompt_asks_for_a_contradiction_not_an_echo(self):
        from backend.app.pipeline.stages import provider_selfcheck as stage

        prompt = stage.build_prompt("Насос P-1: 10 м3/ч. Таблица: 12 м3/ч.")
        assert "противоречие" in prompt
        assert stage.EXPECTED_MARKER in prompt

    def test_e2e_fixture_markdown_contains_the_section(self):
        from tests.distributed_audit_e2e import fixture as fx
        from backend.app.pipeline.stages import provider_selfcheck as stage

        assert stage.FIXTURE_HEADING in fx._MD_TEMPLATE
        rendered = fx._MD_TEMPLATE.format(code="X", discipline="ВК")
        fragment = stage.extract_fragment(rendered)
        numbers = stage.expected_values(fragment)
        assert 10.0 in numbers and 12.0 in numbers

    def test_canary_marker_is_not_stored_in_the_repository(self):
        """Контрольные литералы приходят файлом оператора, а не из Git.

        Хранить контрольную строку в репозитории значит превратить «в ответе её
        не нашли» в утверждение о репозитории.
        """
        source = (REPO_ROOT / "audit_worker/providers/resolver.py").read_text(
            encoding="utf-8")
        assert "forbidden_literals" in source
        assert "CANARY" not in source
