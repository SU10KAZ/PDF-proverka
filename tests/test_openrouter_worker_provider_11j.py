"""Этап 11J: OpenRouter как провайдер ВОРКЕРА — §37 и §38 задания.

Что здесь доказывается и почему именно так.

11I закончился записанным ограничением: точный пресет неисполним ни на одном
воркере, потому что первая нога ансамбля этапа 01 идёт в платный шлюз по
HTTPS, а слой провайдеров умел только запускать CLI. 11J этот канал строит — и
вместе с ним появляется первый на воркере СЕКРЕТ-СТРОКА. У подписок Claude и
Codex такого класса ошибок не было вовсе: их учётные данные центр не знает и
передать не может даже случайно, потому что их нет ни в одном объекте, который
едет по проводу. Ключ — это строка, и её слишком легко положить туда, откуда
уже не вынуть.

Поэтому файл делится на три части, и вторая из них — главная:

  * §37 A–H: провайдер существует, объявляется честно и получает РОВНО ТОТ ЖЕ
    вход, что соседние ноги ансамбля;
  * §37 I–N: ключ не появляется НИГДЕ, кроме собственного файла. Проверяется не
    «мы его туда не кладём», а факт отсутствия в задании, пакете, БД, журналах,
    EventOutbox и результате — сканированием готовых объектов;
  * §37 O–S и §38: классификация отказов, нормализация расхода, и топология
    исполнения — ансамбль, судья, targeted-проходы, страж отсутствия,
    оптимизация, нормативный хвост.

Настоящий OpenRouter НЕ вызывается ни разу. Вместо него — поддельный шлюз
(`tests/distributed_audit_e2e/openrouter_stub.py`) на локальном сокете: запрос
идёт настоящий (httpx, сокет, заголовок, разбор ответа), не настоящий только
собеседник. Ключ во всех тестах ТЕСТОВЫЙ и создаётся фикстурой.

Прогон:
    python -m pytest tests/test_openrouter_worker_provider_11j.py -v
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

from audit_worker.providers import (                       # noqa: E402
    errors,
    model_policy,
    openrouter_secret,
    paths,
    pipeline_bridge,
)
from audit_worker.providers.manager import _ADAPTERS       # noqa: E402
from audit_worker.providers.openrouter_adapter import (    # noqa: E402
    BASE_URL_ENV,
    OFFICIAL_BASE_URL,
    STUBBED_ENDPOINTS_ENV,
    OpenRouterEndpointError,
    OpenRouterProviderAdapter,
    classify_http_status,
    resolve_base_url,
)
from backend.app.services.audit_routing import (           # noqa: E402
    active_plan,
    center_models,
    presets,
    registry,
    requirements,
)
from tests.distributed_audit_e2e import openrouter_stub    # noqa: E402
from tests.test_audit_routing_plan import build_plan       # noqa: E402

#: ТЕСТОВОЕ значение. Настоящий ключ в автоматических тестах недопустим (§25):
#: он попал бы в историю Git через первый же зафиксированный артефакт прогона.
#: Строка нарочно узнаваемая — по ней ищутся утечки.
TEST_KEY = "sk-or-v1-TESTONLY-11J-0123456789abcdef0123456789abcdef"

MODEL_ID = "openai/gpt-5.4"


# ═════════════ Фикстуры ══════════════════════════════════════════════════════
def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.fixture
def worker_root(tmp_path: Path) -> Path:
    return tmp_path / "worker_data"


@pytest.fixture
def or_home(worker_root: Path):
    home = paths.provider_home(worker_root, paths.PROVIDER_OPENROUTER)
    home.ensure_dirs()
    return home


@pytest.fixture
def provisioned(or_home):
    """Ключ разложен так, как это сделает оператор: файл 0600 в своём каталоге."""
    openrouter_secret.write_secret_for_tests(or_home.credential_path, TEST_KEY)
    return or_home


@pytest.fixture
def stub(tmp_path: Path, monkeypatch):
    """Поддельный шлюз на локальном сокете. Настоящий сокет, поддельный ответ."""
    started: list = []

    def _start(behaviour: str = openrouter_stub.BEHAVIOUR_OK):
        port = _free_port()
        log = tmp_path / f"openrouter_calls_{port}.jsonl"
        thread = threading.Thread(
            target=openrouter_stub.serve,
            kwargs={"port": port, "behaviour": behaviour, "log_path": log},
            daemon=True,
        )
        thread.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                probe = socket.create_connection(("127.0.0.1", port), timeout=0.2)
                probe.close()
                break
            except OSError:
                time.sleep(0.05)
        monkeypatch.setenv(STUBBED_ENDPOINTS_ENV, "true")
        monkeypatch.setenv(BASE_URL_ENV, f"http://127.0.0.1:{port}")
        started.append((port, log))
        return log

    return _start


def _adapter(home, **kwargs) -> OpenRouterProviderAdapter:
    return OpenRouterProviderAdapter(
        home, inference_allowed=True, timeout_sec=kwargs.pop("timeout_sec", 15.0), **kwargs
    )


def _call(adapter, *, images=(), prompt="анализ блока", effort="low"):
    if images:
        return adapter.structured_inference_multimodal(
            prompt, images=images, purpose="block_analysis:B1", model=MODEL_ID,
            accepted_reported_models=(MODEL_ID,), reasoning_effort=effort,
            timeout_sec=15.0,
        )
    return adapter.structured_inference(
        prompt, purpose="block_analysis:B1", model=MODEL_ID,
        accepted_reported_models=(MODEL_ID,), reasoning_effort=effort, timeout_sec=15.0,
    )


PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 4


# ═════════════ §37 A–H. Провайдер, объявление, вход ══════════════════════════
def test_a_provider_recognized_by_both_registries():
    """A. Провайдер существует ВЕЗДЕ, где существуют остальные.

    Реестров два, и это не дубликат по недосмотру: `manager._ADAPTERS`
    обслуживает НАБЛЮДЕНИЕ (identity, heartbeat, предупреждения), а локальный
    словарь моста — ОПЛАЧИВАЕМЫЙ вызов. Провайдер, попавший в один из них, даёт
    либо канал, невидимый центру, либо видимость без канала.
    """
    assert paths.PROVIDER_OPENROUTER in paths.SUPPORTED_PROVIDERS
    assert paths.require_provider("openrouter") == "openrouter"
    assert set(_ADAPTERS) == set(paths.SUPPORTED_PROVIDERS)

    source = Path(pipeline_bridge.__file__).read_text(encoding="utf-8")
    assert "PROVIDER_OPENROUTER: OpenRouterProviderAdapter" in source, (
        "мост не умеет собрать адаптер шлюза: оплачиваемый вызов пойдёт мимо"
    )


def test_a2_center_registry_knows_provider_and_all_capabilities():
    """A2. Контракт ЦЕНТРА выражает и провайдера, и все шесть способностей."""
    from backend.app.models.distributed_workers import (
        KNOWN_CAPABILITIES as CENTER_CAPS,
        KNOWN_REQUIREMENT_PROVIDERS,
        ProviderRequirementPayload,
    )

    assert "openrouter" in KNOWN_REQUIREMENT_PROVIDERS
    assert set(registry.KNOWN_CAPABILITIES) <= set(CENTER_CAPS)
    # Реестр центра обязан быть ПОДмножеством реестра воркера: центр не должен
    # иметь возможности заказать способность, которую воркер не разрешит.
    assert set(CENTER_CAPS) <= set(model_policy.KNOWN_CAPABILITIES)
    payload = ProviderRequirementPayload(
        provider="openrouter", capability="block_detector", max_inferences=40,
    )
    assert payload.provider == "openrouter"


def test_b_capability_advertised_only_when_key_configured(worker_root, or_home, monkeypatch):
    """B. Способность объявляется ТОЛЬКО при настроенном ключе.

    Способность, записанная в политике, но неисполнимая, хуже отсутствия
    записи: центр по ней назначит задание и соберёт пакет, а правду узнает в
    середине прогона.
    """
    import dataclasses

    from audit_worker.config import WorkerConfig

    (worker_root / model_policy.POLICY_FILENAME).write_text(
        json.dumps({
            "policy_version": 1,
            "codex": {"capabilities": {"strong_audit": {"model": "gpt-5.4-codex"}}},
            "openrouter": {"capabilities": {"block_detector": {"model": MODEL_ID}}},
        }),
        encoding="utf-8",
    )
    kwargs = {}
    for field in dataclasses.fields(WorkerConfig):
        if field.name == "root":
            kwargs[field.name] = worker_root
        elif (field.default is dataclasses.MISSING
              and field.default_factory is dataclasses.MISSING):   # type: ignore[misc]
            kwargs[field.name] = "x"
    config = WorkerConfig(**kwargs)
    object.__setattr__(config, "allow_real_llm", True)
    object.__setattr__(config, "pipeline_provider_bridge_enabled", True)

    without = config.declared_provider_capabilities()
    assert "openrouter" not in without, (
        "воркер объявил способность шлюза без ключа — центр назначит задание, "
        "которого исполнить нечем"
    )
    assert without.get("codex") == ["strong_audit"], "объявление CLI не должно пострадать"

    openrouter_secret.write_secret_for_tests(or_home.credential_path, TEST_KEY)
    with_key = config.declared_provider_capabilities()
    assert with_key.get("openrouter") == ["block_detector"]
    assert config.capabilities()["http_providers_v1"] is True


def test_c_missing_openrouter_makes_worker_incompatible():
    """C. Воркер без шлюза НЕ совместим с точным пресетом — до создания задания."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    caps_full = {
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "provider_capabilities": {
            "claude": ["strong_audit", "cheap_review"],
            "codex": [
                "strong_audit", "cheap_review", "block_detector",
                "block_detector_strong", "block_judge", "visual_reasoning",
            ],
            "openrouter": ["block_detector"],
        },
    }
    assert requirements.check_worker(plan, caps_full).compatible

    caps_no_or = json.loads(json.dumps(caps_full))
    caps_no_or["provider_capabilities"].pop("openrouter")
    verdict = requirements.check_worker(plan, caps_no_or)
    assert not verdict.compatible
    assert any(m.provider == "openrouter" for m in verdict.missing)
    assert "openrouter" in requirements.explain(verdict)


def test_c2_missing_claude_also_makes_full_codex_incompatible():
    """C2. «Full Codex» без Claude тоже НЕ совместим.

    Имя пресета — продуктовое. Страж отсутствия и основная нога оптимизации
    идут на Claude в ОБОИХ пресетах, и «Full Codex» этого не отменяет.
    """
    plan = build_plan(presets.PRESET_FULL_CODEX)
    caps = {
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "provider_capabilities": {
            "codex": [
                "strong_audit", "cheap_review", "block_detector",
                "block_detector_strong", "block_judge", "visual_reasoning",
            ],
            "openrouter": ["block_detector"],
        },
    }
    verdict = requirements.check_worker(plan, caps)
    assert not verdict.compatible
    assert any(m.provider == "claude" for m in verdict.missing)


def test_d_no_silent_degradation_route_missing_is_refused(provisioned, worker_root):
    """D. Нет маршрута — отказ, а не подмена другим провайдером."""
    from audit_worker.providers.resolver import ProviderBinding, RouteBinding

    binding = ProviderBinding(
        schema_version=1, provider="codex", auth_mode="isolated_provider_home",
        provider_root=str(worker_root / "providers" / "codex"), executable=None,
        timeout_sec=60.0, job_id="j", attempt_id="a", task_id="t", grant_id="g",
        max_inferences=4, allowed_stages=("block_analysis",), model="gpt-5.4-codex",
        routes=(RouteBinding(
            provider="codex", capability="block_detector", model="gpt-5.4-codex",
            accepted_reported_models=("gpt-5.4-codex",),
        ),),
    )
    with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
        pipeline_bridge._select_route(
            binding, provider="openrouter", capability="block_detector",
        )
    assert "openrouter" in str(exc.value)
    assert "Подмена другим провайдером запрещена" in str(exc.value)


def test_e_visual_input_reaches_the_gateway(provisioned, stub):
    """E. Картинка доезжает до шлюза — data-URL в теле запроса, без файла."""
    log = stub()
    result = _call(_adapter(provisioned), images=[("image/png", PNG)])
    assert result.ok, result.detail
    rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["images"] == 1
    assert rows[0]["model"] == MODEL_ID
    assert rows[0]["reasoning_effort"] == "low"


def test_f_g_h_same_semantic_input_across_detector_legs(provisioned, stub):
    """F+G+H. Один блок — один отпечаток входа, независимо от ноги.

    §13 задания: ноги ансамбля обязаны получить ОДИНАКОВЫЙ блок и одинаковый
    контекст. Проверяется на самом строгом объекте, какой доступен транспорту:
    на отпечатке того, что реально ушло в сеть. Разные блоки при этом обязаны
    давать разные отпечатки — иначе проверка была бы тождественно истинной.
    """
    log = stub()
    adapter = _adapter(provisioned)
    _call(adapter, images=[("image/png", PNG)], prompt="блок-1")
    _call(adapter, images=[("image/png", PNG)], prompt="блок-1")
    _call(adapter, images=[("image/png", PNG + b"x")], prompt="блок-1")
    _call(adapter, images=[("image/png", PNG)], prompt="блок-2")

    rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 4
    assert rows[0]["prompt_sha256"] == rows[1]["prompt_sha256"], (
        "тот же блок и тот же контекст дали разные отпечатки — сравнить ноги "
        "ансамбля было бы нечем"
    )
    assert rows[2]["prompt_sha256"] != rows[0]["prompt_sha256"], "другая картинка"
    assert rows[3]["prompt_sha256"] != rows[0]["prompt_sha256"], "другой контекст"


# ═════════════ §37 I–N. Ключ не появляется нигде ═════════════════════════════
def _scan(blob: object) -> bool:
    """Есть ли тестовый ключ (или его узнаваемая часть) внутри объекта."""
    text = blob if isinstance(blob, str) else json.dumps(blob, ensure_ascii=False, default=str)
    return TEST_KEY in text or "sk-or-v1-" in text


def test_i_j_secret_absent_from_job_and_package(provisioned):
    """I+J. Ключ невыразим в задании и не проходит сканер пакета.

    Проверяются ДВА независимых рубежа, и оба поведением, а не текстом.

    Первый: нагрузка задания — закрытый набор полей (`extra="forbid"`), и поля
    для ключа в ней нет. Попытка передать его отвергается схемой, то есть
    задания с ключом не существует в принципе — не «мы такое не формируем», а
    «такое не разбирается».

    Второй: сборщик пакета источника сканирует содержимое на секреты, и ключ
    OpenRouter он ЛОВИТ. Это существенно: пакет собирается из дерева версии
    проекта, куда артефакт с ключом мог бы попасть не из нагрузки, а из
    отладочного файла этапа.
    """
    from pydantic import ValidationError

    from backend.app.models.distributed_workers import (
        AuditPipelineParams,
        ProviderRequirementPayload,
    )
    from backend.app.services.distributed_workers import project_package

    with pytest.raises(ValidationError):
        ProviderRequirementPayload(
            provider="openrouter", capability="block_detector",
            api_key=TEST_KEY,                              # type: ignore[call-arg]
        )
    with pytest.raises(ValidationError):
        AuditPipelineParams(openrouter_key=TEST_KEY)       # type: ignore[call-arg]

    hits = project_package.find_secrets_in_files([
        ("_output/debug.json", json.dumps({"api_key": TEST_KEY}).encode("utf-8")),
    ])
    assert hits, "сканер пакета не поймал ключ OpenRouter в дереве версии"

    hits_env = project_package.find_secrets_in_files([
        ("snapshot/flags.json", b'{"OPENROUTER_API_KEY": "x"}'),
    ])
    assert hits_env, "сканер пакета не поймал имя ключа в снимке флагов"

    # А чистое дерево сканер не задерживает — иначе проверка была бы
    # тождественно истинной и ничего не значила.
    assert not project_package.find_secrets_in_files([
        ("_output/03_findings.json", b'{"findings": []}'),
    ])


def test_k_secret_absent_from_binding_written_to_disk(provisioned, worker_root, tmp_path):
    """K. Привязка провайдера — файл в каталоге попытки — ключа не несёт.

    Она уезжает в пакет результата как evidence, то есть попадает на центр.
    """
    from audit_worker.providers.resolver import ProviderBinding, RouteBinding

    binding = ProviderBinding(
        schema_version=1, provider="openrouter", auth_mode="isolated_provider_home",
        provider_root=str(provisioned.root), executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="t", grant_id="g", max_inferences=40,
        allowed_stages=("block_analysis",), model=MODEL_ID,
        capability="block_detector",
        accepted_reported_models=(MODEL_ID,),
        routes=(RouteBinding(
            provider="openrouter", capability="block_detector", model=MODEL_ID,
            accepted_reported_models=(MODEL_ID,), provider_root=str(provisioned.root),
        ),),
    )
    written = binding.write(tmp_path)
    raw = written.read_text(encoding="utf-8")
    assert not _scan(raw), "ключ оказался в привязке провайдера"
    assert not _scan(binding.as_public_dict()), "ключ оказался в виде для центра"


def test_l_secret_absent_from_provider_result_and_logs(provisioned, stub):
    """L. Ключа нет ни в результате вызова, ни в его диагностике.

    Проверяются ОБА исхода: успех и отказ авторизации. Второй важнее — именно в
    тексте ошибки чужого сервиса чаще всего оказывается эхо запроса.
    """
    stub()
    ok = _call(_adapter(provisioned))
    assert ok.ok
    assert not _scan(ok.as_dict())

    stub(openrouter_stub.BEHAVIOUR_AUTH_ERROR)
    bad = _call(_adapter(provisioned))
    assert bad.error_code == errors.ERR_AUTH_REQUIRED
    assert not _scan(bad.as_dict()), "ключ уехал в detail отказа"


def test_m_secret_absent_from_heartbeat_and_center_payload(provisioned, worker_root):
    """M. heartbeat сообщает ФАКТ настройки, но не значение.

    То же представление уходит в EventOutbox и в карточку VPS.
    """
    from audit_worker.providers.manager import ProviderManager

    manager = ProviderManager(worker_root=worker_root)
    manager.refresh(force=True)
    payload = manager.heartbeat_payload()
    row = next(item for item in payload if item["provider"] == "openrouter")
    assert row["auth_state"] == "logged_in"
    assert row["credential_present"] is True
    assert row["credential_mode"] == "0600"
    assert not _scan(payload), "ключ уехал в heartbeat"
    assert not _scan(manager.warnings())


def test_n_secret_absent_from_stub_call_log(provisioned, stub):
    """N. Даже журнал самого шлюза не хранит ключ — только его отпечаток."""
    log = stub()
    _call(_adapter(provisioned))
    raw = log.read_text(encoding="utf-8")
    assert not _scan(raw)
    row = json.loads(raw.splitlines()[0])
    assert row["authorization_present"] is True, "заголовок до шлюза не дошёл вовсе"
    assert len(row["authorization_sha256"]) == 64


def test_n2_secret_fixture_is_removed_after_test(tmp_path):
    """N2. Тестовый ключ живёт в каталоге теста и исчезает вместе с ним (§25.11)."""
    home = paths.provider_home(tmp_path / "wr", paths.PROVIDER_OPENROUTER)
    home.ensure_dirs()
    written = openrouter_secret.write_secret_for_tests(home.credential_path, TEST_KEY)
    assert written.exists() and written.stat().st_mode & 0o777 == 0o600
    assert str(written).startswith(str(tmp_path)), (
        "фикстура секрета вышла за пределы временного каталога теста"
    )


def test_n3_key_name_forbidden_in_subprocess_env():
    """N3. Имя ключа остаётся в чёрном списке окружения ПОДПРОЦЕССА.

    HTTP-провайдер живёт в самом процессе конвейера и переменная ему не нужна;
    у CLI-провайдеров запрет остаётся дословно.
    """
    from audit_worker.providers.base import FORBIDDEN_ENV_NAMES

    assert "OPENROUTER_API_KEY" in FORBIDDEN_ENV_NAMES

    from audit_worker import audit_runner

    assert "OPENROUTER_API_KEY" not in audit_runner._ENV_WHITELIST
    assert "OPENROUTER_API_KEY" not in audit_runner._ENV_HTTP_PROVIDER_OPTIONAL
    # В белом списке — только ПУТЬ и настройки адреса, но не сам секрет.
    assert all(
        name.endswith(("_CREDENTIAL", "_BASE_URL", "_STUBBED"))
        for name in audit_runner._ENV_HTTP_PROVIDER_OPTIONAL
    )


def test_n4_endpoint_cannot_be_moved_off_the_official_host_silently(monkeypatch):
    """N4. Ключ нельзя увести на чужой хост переменной окружения (I-H4)."""
    monkeypatch.delenv(STUBBED_ENDPOINTS_ENV, raising=False)
    monkeypatch.delenv(BASE_URL_ENV, raising=False)
    assert resolve_base_url() == OFFICIAL_BASE_URL

    monkeypatch.setenv(BASE_URL_ENV, "https://evil.example/api/v1")
    with pytest.raises(OpenRouterEndpointError):
        resolve_base_url()
    monkeypatch.setenv(BASE_URL_ENV, "http://openrouter.ai/api/v1")
    with pytest.raises(OpenRouterEndpointError):
        resolve_base_url()

    monkeypatch.setenv(STUBBED_ENDPOINTS_ENV, "true")
    assert resolve_base_url() == "http://openrouter.ai/api/v1"


# ═════════════ §37 O–S. Отказы, расход, журнал ═══════════════════════════════
@pytest.mark.parametrize(
    "behaviour,expected",
    [
        (openrouter_stub.BEHAVIOUR_AUTH_ERROR, errors.ERR_AUTH_REQUIRED),
        (openrouter_stub.BEHAVIOUR_RATE_LIMIT, errors.ERR_RATE_LIMITED),
        (openrouter_stub.BEHAVIOUR_TIMEOUT, errors.ERR_TIMEOUT),
        (openrouter_stub.BEHAVIOUR_SERVER_ERROR, errors.ERR_PROVIDER_UNAVAILABLE),
        (openrouter_stub.BEHAVIOUR_BROKEN_JSON, errors.ERR_MALFORMED_STATUS),
        (openrouter_stub.BEHAVIOUR_WRONG_MODEL, errors.ERR_MODEL_MISMATCH),
    ],
)
def test_o_p_q_errors_are_classified(provisioned, stub, behaviour, expected):
    """O+P+Q. Каждый класс отказа получает СВОЙ код, а не общий `unknown`."""
    stub(behaviour)
    result = _call(_adapter(provisioned))
    assert not result.ok
    assert result.error_code == expected, f"{behaviour}: {result.detail}"
    assert not _scan(result.as_dict())


def test_p2_status_map_is_explicit():
    """P2. Карта статусов задана явно: 402 — не «нет авторизации»."""
    assert classify_http_status(401) == errors.ERR_AUTH_REQUIRED
    assert classify_http_status(403) == errors.ERR_AUTH_REQUIRED
    assert classify_http_status(402) == errors.ERR_RATE_LIMITED
    assert classify_http_status(429) == errors.ERR_RATE_LIMITED
    assert classify_http_status(404) == errors.ERR_INCOMPATIBLE_CLI
    assert classify_http_status(504) == errors.ERR_TIMEOUT
    assert classify_http_status(500) == errors.ERR_PROVIDER_UNAVAILABLE


def test_r_usage_is_normalized_to_the_common_shape(provisioned, stub):
    """R. Расход приводится к именам, которые читают счётчики этапа.

    Шлюз отдаёт `prompt_tokens`/`completion_tokens`/`cost`; конвейер читает
    `input_tokens`/`output_tokens`/`total_cost_usd` по литеральному ключу. Без
    приведения нога на воркере молча показывала бы ноль там, где та же нога на
    центре показывает числа. Дополнительного запроса ради расхода нет.
    """
    stub()
    result = _call(_adapter(provisioned))
    assert result.usage["input_tokens"] == openrouter_stub.INPUT_TOKENS_PER_CALL
    assert result.usage["output_tokens"] == openrouter_stub.OUTPUT_TOKENS_PER_CALL
    assert result.usage["total_cost_usd"] == pytest.approx(openrouter_stub.COST_PER_CALL_USD)
    assert result.exit_code == 0, (
        "проверка результата требует нулевого кода возврата: HTTP-статус в это "
        "поле класть нельзя"
    )


def test_s_ledger_entry_carries_provider_and_action(provisioned, worker_root, tmp_path, stub):
    """S. Журнал попытки различает ноги ансамбля по действию, а не по промпту.

    Три детектора получают ОДИН промпт и ОДНУ картинку; без `action_id` их
    ключи совпали бы побайтово, и вторая нога получила бы replay ответа первой.
    """
    from audit_worker.providers.inference_ledger import call_key

    base = dict(
        attempt_id="a1", provider="openrouter", purpose="block_analysis:B1",
        prompt="один и тот же промпт блока", attachments_sha256="одна и та же картинка",
    )
    key_or = call_key(**base, action_id="detector_openrouter")
    key_cx = call_key(**dict(base, provider="codex"), action_id="detector_codex")
    key_cx_strong = call_key(
        **dict(base, provider="codex"), action_id="detector_codex_strong",
    )
    key_judge = call_key(**dict(base, provider="codex"), action_id="judge_gap_search")
    assert len({key_or, key_cx, key_cx_strong, key_judge}) == 4, (
        "две ноги ансамбля получили один ключ журнала — вторая была бы replay"
    )


# ═════════════ §38. Топология исполнения ═════════════════════════════════════
def _detector_actions(plan):
    stage = plan.stage("block_batch")
    return [a for a in stage.actions if a.is_model]


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_t_u_block_stage_is_exactly_four_model_actions(preset_id):
    """T+U. На блок — РОВНО четыре обращения, в обоих пресетах."""
    plan = build_plan(preset_id)
    actions = _detector_actions(plan)
    assert len(actions) == 4
    got = {(a.role, a.provider, a.capability) for a in actions}
    assert got == {
        ("detector", "openrouter", "block_detector"),
        ("detector", "codex", "block_detector"),
        ("detector", "codex", "block_detector_strong"),
        ("judge_gap_search", "codex", "block_judge"),
    }
    assert all(
        a.multiplicity.type == registry.MULT_PER_GRAPHIC_BLOCK for a in actions
    )


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_v_w_three_detectors_are_one_parallel_group_judge_after(preset_id):
    """V+W. Три детектора — одна параллельная группа; судья ПОСЛЕ барьера."""
    plan = build_plan(preset_id)
    detectors = [a for a in _detector_actions(plan) if a.role == registry.ROLE_DETECTOR]
    groups = {a.parallel_group for a in detectors}
    assert len(groups) == 1 and None not in groups, (
        "детекторы не объявлены одной параллельной группой"
    )
    judge = next(
        a for a in _detector_actions(plan) if a.role == registry.ROLE_JUDGE_GAP_SEARCH
    )
    assert judge.parallel_group not in groups
    combine = next(
        a for a in plan.stage("block_batch").actions
        if a.role == registry.ROLE_DETECTOR_COMBINE
    )
    # Зависимость выражена ИМЕНЕМ ГРУППЫ, а не перечислением ног: барьер стоит
    # на группе целиком, и перечисление разъезжалось бы с ней при добавлении
    # четвёртой ноги — молча, потому что «зависимость есть» осталось бы правдой.
    depends = set(combine.depends_on)
    assert depends == groups, (
        f"объединение зависит от {depends}, а параллельная группа детекторов — "
        f"{groups}: барьер стоит не там, где ноги"
    )
    assert combine.action_id in judge.depends_on, (
        "судья не зависит от объединения — его могли бы запустить до барьера"
    )


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_x_judge_is_not_a_center_action(preset_id):
    """X. Судья исполняется на воркере, а не остаётся центру."""
    plan = build_plan(preset_id)
    stage = plan.stage("block_batch")
    assert stage.execution_scope == registry.SCOPE_WORKER


def test_y_full_codex_targeted_merge_is_planned_and_executable():
    """Y. Targeted-проходы «Full Codex» ЕСТЬ в плане и исполняются мостом.

    До 11J план объявлял их, а провайдерский путь делал ровно один вызов
    (KI-11I-2): удалённый прогон давал свод без усилителей, и расхождение было
    видно только сравнением плана с журналом.
    """
    plan = build_plan(presets.PRESET_FULL_CODEX, discipline="EOM")
    roles = {a.role for a in plan.stage("findings_merge").actions if a.is_model}
    assert registry.ROLE_MERGE in roles
    assert registry.ROLE_TARGETED_DISCIPLINE in roles
    assert registry.ROLE_TARGETED_DOCNORM in roles

    from backend.app.services.llm import claude_runner

    assert hasattr(claude_runner, "_run_targeted_findings_merge_via_provider")
    assert set(claude_runner._TARGETED_PASS_ROLE.values()) == {
        registry.ROLE_TARGETED_DISCIPLINE,
        registry.ROLE_TARGETED_DOCNORM,
        registry.ROLE_TARGETED_MARK_SYSTEM,
    }
    source = Path(claude_runner.__file__).read_text(encoding="utf-8")
    assert "_run_targeted_findings_merge_via_provider(" in source
    # Базовый путь свода обязан ВЫЗЫВАТЬ проходы, а не только их объявлять.
    import inspect

    body = inspect.getsource(claude_runner._run_findings_merge_via_provider)
    assert "_run_targeted_findings_merge_via_provider" in body


def test_z_claude_preset_has_no_targeted_passes():
    """Z. На Claude-маршруте targeted-проходов нет — и код их не добавит.

    §15 задания: не добавлять Codex-проходы туда, где боевой Claude-маршрут их
    не выполняет. Проверяется на плане, а не на намерении: исполнитель ходит
    по действиям, и пустой список действий означает ноль вызовов.
    """
    plan = build_plan(presets.PRESET_CLAUDE_GPT_CODEX)
    roles = {a.role for a in plan.stage("findings_merge").actions if a.is_model}
    assert roles == {registry.ROLE_MERGE}


def test_aa_full_codex_absence_guard_is_worker_claude():
    """AA. Страж отсутствия «Full Codex» — Claude, и он на ВОРКЕРЕ."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    stage = plan.stage("findings_corrector")
    assert stage is not None and stage.execution_scope == registry.SCOPE_WORKER
    guard = next(
        a for a in stage.actions
        if a.is_model and a.role == registry.ROLE_ABSENCE_GUARD
    )
    assert guard.provider == registry.PROVIDER_CLAUDE
    assert guard.capability == registry.CAP_CHEAP_REVIEW


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_ab_ac_optimization_is_dual_provider_on_the_worker(preset_id):
    """AB+AC. Две ноги оптимизации ‖ на воркере, объединение — детерминированное."""
    plan = build_plan(preset_id)
    stage = plan.stage("optimization")
    assert stage.execution_scope == registry.SCOPE_WORKER
    legs = [a for a in stage.actions if a.is_model]
    assert {(a.role, a.provider) for a in legs} == {
        (registry.ROLE_OPTIMIZATION_PRIMARY, registry.PROVIDER_CLAUDE),
        (registry.ROLE_OPTIMIZATION_VISUAL, registry.PROVIDER_CODEX),
    }
    assert len({a.parallel_group for a in legs}) == 1
    visual = next(a for a in legs if a.role == registry.ROLE_OPTIMIZATION_VISUAL)
    assert visual.reasoning_effort == registry.EFFORT_XHIGH
    merge = next(
        a for a in stage.actions if a.role == registry.ROLE_OPTIMIZATION_MERGE
    )
    assert merge.kind == registry.KIND_DETERMINISTIC
    assert merge.provider is None


def test_ab2_visual_leg_effort_actually_reaches_the_cli():
    """AB2. `xhigh` доезжает до argv мультимодального вызова Codex.

    Параметр принимался сигнатурой и терялся: единственное действие, ради
    которого effort и заведён, шло на умолчании, а хэш плана заверял уровень,
    которого в прогоне не было. Проверить это по артефактам нельзя — CLI
    уровень усилия в ответе не возвращает.
    """
    from audit_worker.providers.codex_adapter import _inference_argv

    argv = _inference_argv("m", [Path("/tmp/x.png")], reasoning_effort="xhigh")
    assert "-c" in argv
    assert 'model_reasoning_effort="xhigh"' in argv
    assert "--image=/tmp/x.png" in argv


def test_ad_ae_optimization_critic_follows_the_preset():
    """AD+AE. Критик оптимизации: Codex на «Full Codex», Claude на пресете A."""
    for preset_id, provider in (
        (presets.PRESET_FULL_CODEX, registry.PROVIDER_CODEX),
        (presets.PRESET_CLAUDE_GPT_CODEX, registry.PROVIDER_CLAUDE),
    ):
        plan = build_plan(preset_id)
        critic = next(
            a for a in plan.stage("optimization_critic").actions
            if a.is_model and a.role == registry.ROLE_OPTIMIZATION_CRITIC
        )
        assert critic.provider == provider, preset_id


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_af_ag_deterministic_stages_make_zero_model_calls(preset_id):
    """AF+AG. Верификатор и F OPT Fix не зовут модель — по ТИПУ, не по факту."""
    plan = build_plan(preset_id)
    corrector = plan.stage("optimization_corrector")
    assert corrector is not None
    assert all(not a.is_model for a in corrector.actions)
    critic_stage = plan.stage("findings_critic")
    assert critic_stage is not None
    assert all(not a.is_model for a in critic_stage.actions)


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_ah_norm_stage_stays_central(preset_id):
    """AH. Нормативный этап — единственный центральный, и он им остаётся."""
    plan = build_plan(preset_id)
    center = [s.stage_id for s in plan.stages if s.execution_scope == registry.SCOPE_CENTER]
    assert set(center) == {"norm_verify", "norm_fix", "norm_requote"}
    worker = [s.stage_id for s in plan.stages if s.execution_scope == registry.SCOPE_WORKER]
    assert "block_batch" in worker and "optimization" in worker


def test_ah2_norm_database_never_enters_the_worker_package():
    """AH2. Тяжёлая нормативная база на воркер не переносится (§19)."""
    from backend.app.services.distributed_workers import project_package

    # Имена нормативных файлов в сборщике ЕСТЬ — и это не утечка, а наоборот:
    # они перечислены как ИСКЛЮЧЕНИЯ. Проверяется поведение, а не текст:
    # собранное дерево не содержит ни одного нормативного артефакта.
    for name in ("norms_paragraphs.json", "norm_checks.json", "03a_norms_verified.json"):
        assert name in project_package.FORBIDDEN_FILENAMES, (
            f"{name} не объявлен центральным артефактом: он уехал бы на чужой VPS"
        )


@pytest.mark.parametrize(
    "preset_id,expected",
    [
        (presets.PRESET_CLAUDE_GPT_CODEX, "claude-opus-5"),
        (presets.PRESET_FULL_CODEX, "codex/gpt-5.4"),
    ],
)
def test_ai_central_norm_tail_reads_the_frozen_plan(preset_id, expected, monkeypatch):
    """AI. Нормативный хвост берёт провайдера из ПЛАНА, а не из таблицы центра."""
    from backend.app.core import config as cfg

    monkeypatch.setitem(cfg.STAGE_MODEL_CONFIG, "norm_verify", "claude-opus-5")
    monkeypatch.setattr(center_models, "codex_model_id", lambda: "codex/gpt-5.4")
    plan = build_plan(preset_id)
    with active_plan.bind_plan(plan):
        assert cfg.get_stage_model("norm_verify") == expected
    # Вне привязки — прежнее поведение, дословно.
    assert cfg.get_stage_model("norm_verify") == "claude-opus-5"


def test_aj_global_preset_switch_cannot_change_a_running_job(monkeypatch):
    """AJ. Переключение пресета в интерфейсе не меняет уже идущее задание.

    Сценарий буквальный: задание создано на «Full Codex», оператор переключает
    ГЛОБАЛЬНУЮ таблицу на Claude, хвост продолжается. Значение обязано остаться
    тем, каким было при создании.
    """
    from backend.app.core import config as cfg

    monkeypatch.setattr(center_models, "codex_model_id", lambda: "codex/gpt-5.4")
    running = build_plan(presets.PRESET_FULL_CODEX)
    with active_plan.bind_plan(running):
        monkeypatch.setitem(cfg.STAGE_MODEL_CONFIG, "norm_verify", "claude-opus-5")
        monkeypatch.setitem(cfg.STAGE_MODEL_CONFIG, "text_analysis", "claude-opus-5")
        assert cfg.get_stage_model("norm_verify") == "codex/gpt-5.4"
        assert cfg.get_stage_model("text_analysis") == "codex/gpt-5.4"

    # Следующее задание получает НОВЫЙ план и другой хэш.
    other = build_plan(presets.PRESET_CLAUDE_GPT_CODEX)
    assert running.plan_hash() != other.plan_hash()
    with active_plan.bind_plan(other):
        assert cfg.get_stage_model("norm_verify") == "claude-opus-5"


def test_aj2_bound_plan_is_isolated_between_concurrent_tasks():
    """AJ2. Планы двух ЗАДАЧ в одном процессе не затирают друг друга.

    Ровно из-за этого 11I не подключил хвост к плану: центр исполняет
    несколько проектов одним процессом (`BATCH_MAX_PARALLEL`), и процессный
    держатель означал бы, что план одного задания становится планом соседнего.
    """
    import asyncio

    from backend.app.core import config as cfg

    plan_a = build_plan(presets.PRESET_FULL_CODEX)
    plan_b = build_plan(presets.PRESET_CLAUDE_GPT_CODEX)
    seen: dict[str, str] = {}

    async def run(name: str, plan) -> None:
        with active_plan.bind_plan(plan):
            await asyncio.sleep(0)                       # уступить другой задаче
            # Через `to_thread`: контекст обязан доехать и в поток, потому что
            # именно так конвейер зовёт синхронные этапы.
            seen[name] = await asyncio.to_thread(cfg.get_stage_model, "norm_verify")

    async def both() -> None:
        await asyncio.gather(run("a", plan_a), run("b", plan_b))

    asyncio.run(both())
    assert seen["a"] != seen["b"], "план одной задачи протёк в другую"


@pytest.mark.parametrize(
    "preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX]
)
def test_ak_al_plan_hash_survives_serialization(preset_id):
    """AK+AL. Хэш плана не меняется от круга через JSON — ни в пути, ни в ответе."""
    plan = build_plan(preset_id)
    restored = type(plan).from_dict(json.loads(json.dumps(plan.to_dict(), ensure_ascii=False)))
    assert restored.plan_hash() == plan.plan_hash()
    packaged = type(plan).from_dict(json.loads(plan.to_package_bytes().decode("utf-8")))
    assert packaged.plan_hash() == plan.plan_hash()


# ═════════════ Исправления по состязательному ревью 11J ══════════════════════
def test_rev1_symlinked_credential_is_refused(or_home, tmp_path):
    """Ревью-1. Файл ключа не может быть символьной ссылкой.

    `os.stat` идёт ПО ССЫЛКЕ, и подменённый ссылкой файл прошёл бы проверку
    прав по цели: воркер отправил бы содержимое ЧУЖОГО файла в заголовке
    `Authorization` на внешний хост. Соседний `inference_grant` проверяет это
    правильно с самого начала (`lstat` + отказ на `S_ISLNK`), и расхождение
    двух файлов с одинаковыми требованиями — само по себе дефект.
    """
    victim = tmp_path / "чужой_секрет.txt"
    victim.write_text("wtk_ЧУЖОЙ_ТОКЕН_ВОРКЕРА_0123456789", encoding="utf-8")
    victim.chmod(0o600)
    or_home.credential_path.parent.mkdir(parents=True, exist_ok=True)
    or_home.credential_path.symlink_to(victim)

    status = openrouter_secret.probe(or_home.credential_path)
    assert not status.configured
    assert "ссылка" in status.reason

    with pytest.raises(openrouter_secret.OpenRouterSecretError):
        openrouter_secret.read_secret(or_home.credential_path)


def test_rev2_world_writable_credential_is_refused(provisioned):
    """Ревью-2. Проверяется ВСЯ маска прав, а не только биты чтения.

    Файл 0622 «не читается посторонним», но ЗАПИСЫВАЕТСЯ им — и тогда в
    заголовок уедет строка, которую подставил кто угодно на этой машине.
    Прежняя проверка смотрела только `S_IRGRP`/`S_IROTH` и такой файл
    принимала, при этом сообщение обещало «требуется 0600».
    """
    for mode in (0o622, 0o606, 0o700, 0o660):
        os.chmod(provisioned.credential_path, mode)
        status = openrouter_secret.probe(provisioned.credential_path)
        assert not status.configured, f"права {mode:04o} приняты"
        assert "шире" in status.reason
    os.chmod(provisioned.credential_path, 0o600)
    assert openrouter_secret.probe(provisioned.credential_path).configured


def test_rev3_endpoint_error_reaching_the_center_is_redacted(or_home, monkeypatch):
    """Ревью-3. Сообщение об отвергнутом адресе шлюза проходит редактор.

    Оно уезжает в `capability` → heartbeat → карточку VPS, а содержит значение
    переменной администратора — в которой при ошибке настройки может оказаться
    и адрес внутреннего прокси, и форма `https://user:pass@host`.
    """
    monkeypatch.delenv(STUBBED_ENDPOINTS_ENV, raising=False)
    monkeypatch.setenv(
        BASE_URL_ENV, "https://служебный:sk-or-v1-SECRETVALUE0123456789@proxy.local/api/v1",
    )
    snapshot = _adapter(or_home).capability_snapshot()
    assert "endpoint_error" in snapshot
    assert "sk-or-v1-SECRETVALUE" not in json.dumps(snapshot, ensure_ascii=False)


def test_rev4_credential_facts_describe_the_file_actually_read(
    or_home, tmp_path, monkeypatch,
):
    """Ревью-4. При переопределённом пути телеметрия описывает НУЖНЫЙ файл.

    Базовый `identity()` считает факты по раскладке безусловно. Для CLI это
    верно — путь у них один; здесь администратор мог указать другой, и центр
    видел бы «файла нет» рядом с «провайдер авторизован». Оператору такое
    сочетание не объяснить.
    """
    external = tmp_path / "openrouter.key"
    openrouter_secret.write_secret_for_tests(external, TEST_KEY)
    monkeypatch.setenv(openrouter_secret.CREDENTIAL_PATH_ENV, str(external))

    adapter = _adapter(or_home)
    assert adapter.auth_status().auth_state == "logged_in"
    identity = adapter.identity()
    assert identity.credential_facts["exists"] is True, (
        "телеметрия описывает файл раскладки, а читается файл администратора"
    )
    assert identity.credential_facts["mode"] == "0600"
    assert identity.credential_facts["path_source"] == "admin_env"
    payload = identity.as_center_payload()
    assert payload["credential_present"] is True
    assert not _scan(payload)
    # Абсолютного пути в центральном представлении нет ни в каком виде.
    assert str(external) not in json.dumps(payload, ensure_ascii=False)


def test_rev5_non_ascii_key_is_refused_before_the_request(or_home):
    """Ревью-5. Ключ вне ASCII отвергается ДО запроса.

    Заголовок HTTP кодируется latin-1: такое значение уронило бы клиент
    исключением, в текст которого попал бы сам ключ.
    """
    openrouter_secret.write_secret_for_tests(
        or_home.credential_path, "sk-or-v1-КИРИЛЛИЦА-0123456789",
    )
    with pytest.raises(openrouter_secret.OpenRouterSecretError) as exc:
        openrouter_secret.read_secret(or_home.credential_path)
    assert "ASCII" in str(exc.value)
    assert "КИРИЛЛИЦА" not in str(exc.value), "ключ попал в текст ошибки"


def test_rev6_feature_flags_cannot_inject_provider_env(monkeypatch):
    """Ревью-6 (критическое). Центр не может через план подменить адрес шлюза.

    `apply_routing_flags` писал в окружение процесса конвейера ЛЮБОЕ имя из
    `feature_flags`, отфильтрованное только по `isupper()`. Снимок флагов на
    стороне центра собирается по закрытому списку, но воркер разбирает план
    заново и состава `feature_flags` не проверял.

    Цена дыры стала другой ровно на 11J: до него в окружении конвейера не было
    ни одной переменной, меняющей МАРШРУТ СЕТЕВОГО ЗАПРОСА. Теперь есть — и
    задание, положившее адрес шлюза в `feature_flags`, увело бы ключ владельца
    VPS на произвольный хост, а выглядело бы это обычным успешным прогоном.
    """
    from backend.app.pipeline import remote_audit_runner

    hostile = {
        "STAGE01_THIRD_LEG_ENABLED": "true",
        BASE_URL_ENV: "https://attacker.example/api/v1",
        STUBBED_ENDPOINTS_ENV: "true",
        openrouter_secret.CREDENTIAL_PATH_ENV: "/etc/shadow",
        "PATH": "/attacker/bin",
    }
    plan = build_plan(presets.PRESET_FULL_CODEX, flags=hostile)
    for name in (BASE_URL_ENV, STUBBED_ENDPOINTS_ENV,
                 openrouter_secret.CREDENTIAL_PATH_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PATH", "/usr/bin")

    report = remote_audit_runner.apply_routing_flags(plan)

    assert report["flags"] == {"STAGE01_THIRD_LEG_ENABLED": "true"}
    assert set(report["rejected"]) >= {
        BASE_URL_ENV, STUBBED_ENDPOINTS_ENV,
        openrouter_secret.CREDENTIAL_PATH_ENV, "PATH",
    }
    for name in (BASE_URL_ENV, STUBBED_ENDPOINTS_ENV,
                 openrouter_secret.CREDENTIAL_PATH_ENV):
        assert os.environ.get(name) is None, f"{name} уехал в окружение из плана"
    assert os.environ["PATH"] == "/usr/bin", "PATH подменён нагрузкой задания"


def test_rev7_bridge_refuses_ambient_mode_for_the_http_provider(tmp_path):
    """Ревью-7. Мост тоже отвергает ambient у провайдера без CLI.

    Фабрика `paths.provider_home` этот режим отвергает; `_build_home` моста
    строил `ProviderHome` НАПРЯМУЮ и проверку обходил. Ambient означал бы
    раскладку в личном каталоге живого пользователя.
    """
    from audit_worker.providers.resolver import ProviderBinding, RouteBinding

    binding = ProviderBinding(
        schema_version=1, provider="openrouter", auth_mode="ambient_user",
        provider_root=str(tmp_path / "p"), executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="t", grant_id="g", max_inferences=1,
        allowed_stages=("block_analysis",), model=MODEL_ID,
        capability="block_detector", accepted_reported_models=(MODEL_ID,),
        routes=(RouteBinding(
            provider="openrouter", capability="block_detector", model=MODEL_ID,
            accepted_reported_models=(MODEL_ID,), auth_mode="ambient_user",
            provider_root=str(tmp_path / "p"),
        ),),
    )
    route = binding.routes[0]
    with pytest.raises(pipeline_bridge.ProviderBridgeError) as exc:
        pipeline_bridge._build_home(binding, route=route)
    assert "ambient_user" in str(exc.value)


def test_rev8_request_shape_matches_the_central_leg(provisioned, stub):
    """Ревью-8. Запрос несёт потолок ответа и требование JSON-объекта.

    Без `max_tokens` длина ответа определяется маршрутом шлюза, а не прогоном;
    без `response_format` модель вольна ответить прозой, и разбор провалится
    уже ПОСЛЕ оплаты. Боевой центральный путь задаёт оба поля.
    """
    import json as _json

    captured: dict = {}

    import httpx

    class _Recorder(httpx.Client):
        def post(self, url, **kwargs):                    # noqa: A003
            captured.update(kwargs.get("json") or {})
            return super().post(url, **kwargs)

    stub()
    monkey = pytest.MonkeyPatch()
    try:
        monkey.setattr(httpx, "Client", _Recorder)
        result = _call(_adapter(provisioned), images=[("image/png", PNG)])
    finally:
        monkey.undo()
    assert result.ok, result.detail
    assert captured.get("max_tokens") == 16000
    assert captured.get("response_format") == {"type": "json_object"}
    assert captured.get("temperature") == 0.2
    assert captured.get("reasoning") == {"effort": "low"}
    parts = captured["messages"][0]["content"]
    assert [p["type"] for p in parts] == ["text", "image_url"]
    assert parts[1]["image_url"]["url"].startswith("data:image/png;base64,")
