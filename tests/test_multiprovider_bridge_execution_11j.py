"""Этап 11J: ансамбль этапа 01 исполняется ТРЕМЯ провайдерами на самом деле.

Чем это отличается от `test_openrouter_worker_provider_11j.py`.

Тот файл проверяет ПРОВАЙДЕРА: адаптер, ключ, классификацию отказов, и
топологию плана как описание. Здесь проверяется ИСПОЛНЕНИЕ: один и тот же
блок прогоняется через настоящий мост четырьмя действиями плана, и каждое из
них физически доходит до своей внешней точки.

Что настоящее в этом файле:

  * мост (`pipeline_bridge.run_stage_inference`) — тот же код, что и в бою;
  * привязка провайдера — настоящий файл в каталоге попытки, прочитанный с
    диска, с четырьмя маршрутами;
  * журнал вызовов и exactly-once — настоящие;
  * две codex-ноги и судья — НАСТОЯЩИЕ ПОДПРОЦЕССЫ (поддельный CLI);
  * нога OpenRouter — НАСТОЯЩИЙ HTTP-запрос на локальный сокет (поддельный шлюз).

Что поддельное: только ответы. Ни один запрос к настоящей модели не делается —
ни к Claude, ни к Codex, ни к OpenRouter.

Именно эту связку 11I исполнить не мог: до 11J провайдера `openrouter` на
воркере не существовало как имени, и «ансамбль из трёх ног» на удалённой
машине означал ансамбль из двух.

Прогон:
    python -m pytest tests/test_multiprovider_bridge_execution_11j.py -v
"""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from audit_worker.providers import openrouter_secret, paths, pipeline_bridge  # noqa: E402
from audit_worker.providers.openrouter_adapter import (                       # noqa: E402
    BASE_URL_ENV,
    STUBBED_ENDPOINTS_ENV,
)
from audit_worker.providers.resolver import ProviderBinding, RouteBinding     # noqa: E402
from tests.distributed_audit_e2e import openrouter_stub                       # noqa: E402

TEST_KEY = "sk-or-v1-TESTONLY-11J-0123456789abcdef0123456789abcdef"

#: Четыре действия этапа 01 в обоих пресетах. Порядок — порядок плана.
ENSEMBLE = (
    ("detector_openrouter", "openrouter", "block_detector", "openai/gpt-5.4"),
    ("detector_codex_standard", "codex", "block_detector", "gpt-5.4-codex"),
    ("detector_codex_strong", "codex", "block_detector_strong", "gpt-5.4-codex-high"),
    ("judge_gap_search", "codex", "block_judge", "gpt-5.4-codex"),
)

PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256))
PROMPT = "Проанализируй блок Б-1 и верни findings."

#: Ответ поддельного codex. Форма — фактический контракт `codex exec --json`.
_CODEX_STUB = r'''#!/bin/bash
case "$1" in --version) echo "codex-cli 0.0.0-fake" ; exit 0 ;; esac
if [ "$1" = "app-server" ]; then
  python3 - <<'PYEOF'
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except Exception:
        continue
    mid = msg.get("id")
    if msg.get("method") == "account/read":
        print(json.dumps({"id": mid, "result": {"account": {"type": "chatgpt", "planType": "pro"}, "requiresOpenaiAuth": True}}), flush=True)
    elif msg.get("method") == "account/rateLimits/read":
        print(json.dumps({"id": mid, "result": {"rateLimits": {"limitId": "fake", "primary": {"usedPercent": 0, "windowDurationMins": 300, "resetsAt": 0}}}}), flush=True)
    elif mid is not None:
        print(json.dumps({"id": mid, "result": {}}), flush=True)
PYEOF
  exit 0
fi
STDIN=$(cat)
echo "CALL argv=$* stdin_bytes=${#STDIN}" >> "__JOURNAL__"
python3 -c '
import json, sys
print(json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps({"findings": []}, ensure_ascii=False)}}, ensure_ascii=False))
print(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 30}}))
'
exit 0
'''


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.fixture
def stand(tmp_path: Path, monkeypatch):
    """Стенд: поддельный шлюз на сокете, поддельный codex-бинарь, привязка на диске."""
    worker_root = tmp_path / "worker_data"
    job_dir = tmp_path / "attempt"
    (job_dir / "metadata").mkdir(parents=True)

    # ─── Нога OpenRouter: настоящий сокет ────────────────────────────────────
    port = _free_port()
    http_log = tmp_path / "openrouter_calls.jsonl"
    threading.Thread(
        target=openrouter_stub.serve,
        kwargs={"port": port, "behaviour": openrouter_stub.BEHAVIOUR_OK,
                "log_path": http_log},
        daemon=True,
    ).start()
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.05)
    monkeypatch.setenv(STUBBED_ENDPOINTS_ENV, "true")
    monkeypatch.setenv(BASE_URL_ENV, f"http://127.0.0.1:{port}")

    or_home = paths.provider_home(worker_root, paths.PROVIDER_OPENROUTER)
    or_home.ensure_dirs()
    openrouter_secret.write_secret_for_tests(or_home.credential_path, TEST_KEY)

    # ─── Ноги Codex: настоящий подпроцесс поддельного CLI ────────────────────
    codex_home = paths.provider_home(worker_root, paths.PROVIDER_CODEX)
    codex_home.ensure_dirs()
    codex_bin = tmp_path / "bin" / "codex"
    codex_bin.parent.mkdir(parents=True, exist_ok=True)
    cli_log = tmp_path / "codex_calls.log"
    cli_log.touch()
    # Путь журнала ВПЕЧАТАН в скрипт, а не передан переменной окружения. Иначе
    # и быть не может: окружение подпроцесса собирается с нуля по белому списку
    # (инвариант I-P1), и переменная теста до поддельного CLI не доехала бы —
    # ровно как не доедет до настоящего никакая переменная центра.
    codex_bin.write_text(
        _CODEX_STUB.replace("__JOURNAL__", str(cli_log)), encoding="utf-8",
    )
    codex_bin.chmod(0o755)

    binding = ProviderBinding(
        schema_version=1,
        provider="codex",
        auth_mode="isolated_provider_home",
        provider_root=str(codex_home.root),
        executable=str(codex_bin),
        timeout_sec=60.0,
        job_id="job-11j",
        attempt_id="attempt-11j",
        task_id="task-11j",
        grant_id="grant-11j",
        max_inferences=16,
        allowed_stages=("block_analysis",),
        model="gpt-5.4-codex",
        capability="block_detector",
        accepted_reported_models=("gpt-5.4-codex",),
        model_report="unsupported",
        routing_plan_hash="sha256:test",
        routes=tuple(
            RouteBinding(
                provider=provider,
                capability=capability,
                model=model,
                accepted_reported_models=(model,),
                # Codex не сообщает фактическую модель — это свойство CLI, а не
                # наша слепота (см. `model_policy.MODEL_REPORT_MODES`).
                model_report=("unsupported" if provider == "codex" else "required"),
                auth_mode="isolated_provider_home",
                provider_root=str(
                    (or_home if provider == "openrouter" else codex_home).root
                ),
                executable=(None if provider == "openrouter" else str(codex_bin)),
                timeout_sec=60.0,
            )
            for _action, provider, capability, model in ENSEMBLE
        ),
    )
    binding_path = binding.write(job_dir / "metadata")
    monkeypatch.setenv(pipeline_bridge.BINDING_ENV, str(binding_path))
    return {
        "job_dir": job_dir,
        "binding": binding,
        "binding_path": binding_path,
        "http_log": http_log,
        "cli_log": cli_log,
        "worker_root": worker_root,
    }


def _run(stand, action_id: str, provider: str, capability: str, *, prompt: str = PROMPT):
    return pipeline_bridge.run_stage_inference(
        job_dir=stand["job_dir"],
        stage="block_analysis",
        prompt=prompt,
        purpose="block_analysis:Б-1",
        action_id=action_id,
        provider=provider,
        capability=capability,
        images=[("image/png", PNG)],
        timeout_sec=30.0,
    )


def test_bridge_is_active_and_carries_four_routes(stand):
    """Мост читает привязку с четырьмя маршрутами — включая шлюз без CLI."""
    assert pipeline_bridge.active()
    binding = pipeline_bridge.load_binding()
    assert {(r.provider, r.capability) for r in binding.routes} == {
        (p, c) for _a, p, c, _m in ENSEMBLE
    }
    # Круг через диск не теряет маршрут HTTP-провайдера: у него нет
    # исполняемого файла, и разбор обязан это пережить.
    or_route = next(r for r in binding.routes if r.provider == "openrouter")
    assert or_route.executable is None
    assert or_route.model == "openai/gpt-5.4"


def test_all_four_ensemble_actions_execute_through_their_own_channel(stand):
    """Четыре действия — четыре ФАКТИЧЕСКИХ обращения по трём разным каналам.

    Это и есть утверждение 11J, недостижимое до него: нога шлюза уходит в
    сокет, две ноги Codex и судья — в подпроцессы, и всё это одним прогоном
    одного блока.
    """
    outcomes = {}
    for action_id, provider, capability, _model in ENSEMBLE:
        outcome = _run(stand, action_id, provider, capability)
        outcomes[action_id] = outcome
        assert outcome.provider_result.status == "success", (
            f"{action_id}: {outcome.provider_result.error_code} "
            f"{outcome.provider_result.detail}"
        )
        assert outcome.performed, f"{action_id}: вызов не состоялся, взят из журнала"

    http_rows = [
        json.loads(line)
        for line in stand["http_log"].read_text(encoding="utf-8").splitlines()
    ]
    assert len(http_rows) == 1, "нога шлюза сделала не один запрос"
    assert http_rows[0]["images"] == 1

    cli_calls = [
        line for line in stand["cli_log"].read_text(encoding="utf-8").splitlines()
        if line.startswith("CALL")
    ]
    assert len(cli_calls) == 3, (
        f"три ноги Codex дали {len(cli_calls)} запусков подпроцесса: ансамбль "
        "схлопнулся"
    )
    # Ноги различимы по argv: у усиленной своя модель, у судьи — своя.
    argv_blob = "\n".join(cli_calls)
    assert "--model=gpt-5.4-codex-high" in argv_blob
    assert argv_blob.count("--model=gpt-5.4-codex") >= 2

    providers = {
        action_id: outcome.provider_result.provider
        for action_id, outcome in outcomes.items()
    }
    assert providers["detector_openrouter"] == "openrouter"
    assert providers["detector_codex_standard"] == "codex"
    assert providers["judge_gap_search"] == "codex"


def test_exactly_once_distinguishes_every_leg(stand):
    """Повтор ноги читается из журнала; РАЗНЫЕ ноги друг друга не подменяют.

    Три детектора получают ОДИН промпт и ОДНУ картинку, две codex-ноги вдобавок
    идут через одного провайдера. Без `action_id` их ключи совпали бы
    побайтово: ансамбль из трёх моделей выродился бы в одну, скопированную
    трижды, и заметить это по артефактам было бы нельзя — ответы совпадают
    ровно потому, что это один ответ.
    """
    for action_id, provider, capability, _model in ENSEMBLE:
        first = _run(stand, action_id, provider, capability)
        assert first.performed, action_id

    http_before = len(stand["http_log"].read_text(encoding="utf-8").splitlines())
    cli_before = len([
        x for x in stand["cli_log"].read_text(encoding="utf-8").splitlines()
        if x.startswith("CALL")
    ])

    for action_id, provider, capability, _model in ENSEMBLE:
        again = _run(stand, action_id, provider, capability)
        assert not again.performed, (
            f"{action_id}: повтор оплачен второй раз вместо чтения из журнала"
        )

    http_after = len(stand["http_log"].read_text(encoding="utf-8").splitlines())
    cli_after = len([
        x for x in stand["cli_log"].read_text(encoding="utf-8").splitlines()
        if x.startswith("CALL")
    ])
    assert http_after == http_before, "шлюз был вызван повторно"
    assert cli_after == cli_before, "CLI был запущен повторно"

    keys = {row["key"] for row in _ledger_rows(stand)}
    assert len(keys) == 4, f"четыре ноги дали {len(keys)} ключей журнала"


def _ledger_rows(stand) -> list[dict]:
    from audit_worker.providers.inference_ledger import InferenceLedger

    ledger = InferenceLedger(
        stand["job_dir"], attempt_id="attempt-11j", job_id="job-11j",
    )
    return list(ledger.summary().get("keys") or [])


def test_missing_route_is_refused_not_substituted(stand):
    """Действие без маршрута — отказ. Ни подмены провайдера, ни пропуска ноги."""
    with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
        _run(stand, "detector_gemini", "openrouter", "visual_reasoning")
    assert "Подмена другим провайдером запрещена" in str(exc.value)

    # И ни одного обращения «вместо»: ни в сокет, ни в подпроцесс.
    assert not stand["http_log"].exists() or not stand["http_log"].read_text().strip()
    assert not stand["cli_log"].exists() or "CALL" not in stand["cli_log"].read_text()


def test_call_without_action_id_is_refused_when_the_plan_is_present(stand):
    """Обращение мимо плана невозможно, пока привязка несёт маршруты."""
    with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
        pipeline_bridge.run_stage_inference(
            job_dir=stand["job_dir"], stage="block_analysis", prompt=PROMPT,
            purpose="block_analysis:Б-1", provider="codex",
            capability="block_detector", images=[("image/png", PNG)],
        )
    assert "action_id" in str(exc.value)


def test_key_is_absent_from_everything_the_attempt_leaves_behind(stand):
    """После исполнения ансамбля ключа нет ни в одном файле каталога попытки.

    Каталог попытки уезжает центру пакетом результата целиком: привязка,
    журнал вызовов, отчёты этапов. Проверяется весь он, а не выбранные файлы.
    """
    for action_id, provider, capability, _model in ENSEMBLE:
        _run(stand, action_id, provider, capability)

    leaked: list[str] = []
    for path in sorted(stand["job_dir"].rglob("*")):
        if not path.is_file():
            continue
        blob = path.read_bytes()
        if TEST_KEY.encode() in blob or b"sk-or-v1-" in blob:
            leaked.append(str(path.relative_to(stand["job_dir"])))
    assert not leaked, f"ключ найден в артефактах попытки: {leaked}"

    # И в самом каталоге провайдера — только его собственный файл, с 0600.
    cred = paths.provider_home(
        stand["worker_root"], paths.PROVIDER_OPENROUTER
    ).credential_path
    assert cred.is_file()
    assert oct(cred.stat().st_mode & 0o777) == "0o600"
    assert str(cred).startswith(str(stand["worker_root"]))


def test_key_disappearing_mid_run_fails_only_that_action(stand):
    """§24: ключ пропал между действиями — падает КОНКРЕТНОЕ, соседние идут.

    Ни подмены провайдера, ни пропуска ноги, ни исполнения ноги шлюза центром
    за спиной воркера.
    """
    codex = _run(stand, "detector_codex_standard", "codex", "block_detector")
    assert codex.provider_result.ok

    cred = paths.provider_home(
        stand["worker_root"], paths.PROVIDER_OPENROUTER
    ).credential_path
    cred.unlink()

    with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
        _run(stand, "detector_openrouter", "openrouter", "block_detector")
    assert "ключ" in str(exc.value).lower()

    # Соседняя нога того же блока продолжает работать.
    judge = _run(stand, "judge_gap_search", "codex", "block_judge")
    assert judge.provider_result.ok
    rows = json.loads(json.dumps(_ledger_rows(stand)))
    assert rows, "журнал пуст — исполнение не прослеживается"
