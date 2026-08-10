"""Этап 11G — сетевой сквозной путь «центр → воркер» для требования к провайдеру.

Что закрывает этот файл и почему именно это.

До 11G центр УМЕЛ принять `provider_requirement` (`create_audit_job` его
принимал), но штатный путь запуска его не формировал вовсе. Боевое задание
уезжало на воркер без единого слова о провайдере, воркер честно не активировал
мост, и привязку приходилось выписывать оператору руками. 11G разрыв закрывает
— и вместе с ним появляется ровно та опасность, ради которой писался
провайдерский слой: путь к чужой подписке теперь прокладывает автоматический
код, а не команда человека.

Поэтому здесь проверяются не «долетело ли поле», а рубежи, которые оно не имеет
права обойти по дороге:

  * **способность переживает провод побайтово** (центр → JSON → БД → HTTP →
    воркер → резолвер). Потеря её где-нибудь в середине не выглядит как
    поломка: привязка просто уходит без `--model`, и отвечает модель учётной
    записи по умолчанию — тихая подмена 11C;
  * **точной модели в требовании нет ни в каком виде.** Строка задания,
    дошедшая до argv стороннего CLI, ломает I-P5, а «центр назначает модель»
    означает распоряжение чужой подпиской. Это утверждение проверяется дважды:
    поведением (нагрузка с `model` отвергается тремя валидаторами) и
    структурно (в требовании и в исходниках, которые его строят, нет ни одного
    идентификатора модели);
  * **старые нагрузки продолжают разбираться.** Схема с `extra="forbid"`
    отвергает незнакомый ключ целиком, поэтому «просто удалить поле `model`»
    сломало бы разбор всего, что уже лежит в `workers.db`;
  * **fail closed на всех трёх валидаторах сразу.** Требование «зови модель, но
    способность не назову» обязано быть отвергнуто и схемой центра, и приёмом
    задания на воркере, и резолвером — по отдельности, а не «где-нибудь да
    поймается»;
  * **транспорт остаётся тем же, что и был:** ни SSH, ни входящего порта на
    воркере, ни ослабленного TLS, ни секретов в требовании.

НИ ОДИН тест этого файла не ходит в сеть и не зовёт настоящую модель: везде
подставной исполняемый файл и локальные каталоги. Это требование этапа —
бюджет реальных вызовов измеряется единицами, и тратить его на регрессии
нельзя.

Прогон:
    python -m pytest tests/test_distributed_workers_network_e2e_11g.py -v
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import json
import os
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Фикстуры и помощники берутся у соседнего файла этапа ExecutionBackend, а не
# пишутся заново: харнес центра (портальные роли, workers.db во временном
# каталоге, одобренный воркер) там уже выверен, а второй его экземпляр в
# репозитории означал бы два места, которые расходятся при первой же правке.
from tests.test_distributed_workers_execution_backend import (   # noqa: E402,F401
    _approved_worker,
    _audit_params,
    _FakeJob,
    _RecordingManager,
    _worker_config,
    admin,
    center_env,
)

from audit_worker import audit_runner                            # noqa: E402
from audit_worker.providers import (                             # noqa: E402
    inference_grant,
    model_policy,
    resolver,
)
from audit_worker.providers.auth_mode import (                   # noqa: E402
    AUTH_MODE_AMBIENT_USER,
)
from backend.app.models.distributed_workers import (             # noqa: E402
    AuditPipelineParams,
    ProviderRequirementPayload,
)


# ═════════════ Общие помощники ═══════════════════════════════════════════════
#: Способность и провайдер боевого аудита. Названы здесь один раз, чтобы
#: «строка в тесте» и «строка в проде» не разъезжались молча.
CAPABILITY = "strong_audit"
PROVIDER = "claude"

#: Требование в том виде, в каком его формирует центр на 11G.
_WIRE_REQUIREMENT = {
    "provider": PROVIDER,
    "capability": CAPABILITY,
    "model": None,
    "allowed_stages": ["block_analysis", "text_analysis"],
    "max_inferences": 8,
}

#: Способности воркера, при которых центр вправе заказать вызовы модели.
_REAL_WORKER_CAPS = {
    "provider_mode": "real",
    "real_llm_enabled": True,
    "pipeline_provider_bridge_enabled": True,
    "provider_capabilities": {PROVIDER: [CAPABILITY]},
    "job_types": ["test_pipeline_v1", "audit_pipeline_v1"],
    "pipeline_revision": "rev-abc123",
}


def _write_exe(path: Path, body: str) -> Path:
    """Скопировано из tests/test_distributed_workers_pipeline_provider.py."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body if body.startswith("#!") else "#!/bin/bash\n" + body,
                    encoding="utf-8")
    path.chmod(0o755)
    return path


def _fake_claude(path: Path) -> Path:
    """Подделка `claude`: отвечает на `--version` и `auth`, больше ничего.

    Сокращённая копия помощника из tests/test_distributed_workers_pipeline_
    provider.py: здесь модель не вызывается вовсе, нужен только авторизованный
    CLI, чтобы резолвер согласился выписать привязку.
    """
    return _write_exe(path, """
case "$1" in --version) echo "2.1.220 (Claude Code)"; exit 0 ;; esac
for a in "$@"; do
  if [ "$a" = "auth" ]; then
    echo '{"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty", "subscriptionType": "max"}'
    exit 0
  fi
done
echo '{"type":"result","subtype":"success","is_error":false,"result":"{}"}'
exit 0
""")


@pytest.fixture()
def worker_root(tmp_path: Path, monkeypatch) -> Path:
    """Корень данных воркера С локальной политикой моделей.

    Политика — обязательная часть состояния машины с 11D: без неё способности
    не во что превратить. Копия фикстуры из tests/test_distributed_workers_
    pipeline_provider.py плюс снятие `AUDIT_WORKER_PROVIDER_POLICY`: эта
    переменная побеждает корень данных, и унаследованная от окружения она
    увела бы тест на чужой файл.
    """
    monkeypatch.delenv(model_policy.POLICY_ENV, raising=False)
    root = tmp_path / "worker"
    (root / "config").mkdir(parents=True)
    (root / "runtime").mkdir(parents=True)
    (root / model_policy.POLICY_FILENAME).write_text(json.dumps({
        "policy_version": 1,
        PROVIDER: {
            "auth_mode": "ambient_user",
            "capabilities": {CAPABILITY: {"model": "claude-opus-5"}},
        },
    }, ensure_ascii=False), encoding="utf-8")
    return root


def _binding(job_dir: Path, *, executable: Path) -> resolver.ProviderBinding:
    """Скопировано из tests/test_distributed_workers_pipeline_provider.py."""
    return resolver.ProviderBinding(
        schema_version=resolver.BINDING_SCHEMA_VERSION,
        provider=PROVIDER,
        auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(resolver.ambient_root_for_attempt(job_dir, PROVIDER)),
        executable=str(executable),
        timeout_sec=30.0,
        job_id="job-1",
        attempt_id="attempt-1",
        task_id="job-1",
        grant_id="g-test-0001",
        max_inferences=1,
        allowed_stages=("block_analysis",),
        model="claude-opus-5",
        capability=CAPABILITY,
        accepted_reported_models=("claude-opus-5", "claude-opus-5[1m]"),
        forbidden_literals=(),
    )


def _python_files(*relative: str) -> list[Path]:
    """Все .py указанных подкаталогов репозитория (без кэшей)."""
    found: list[Path] = []
    for rel in relative:
        for path in sorted((REPO_ROOT / rel).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            found.append(path)
    assert found, f"каталоги {relative} пусты — проверка ничего не проверяет"
    return found


def _docstring_ids(tree: ast.AST) -> set[int]:
    """Идентификаторы узлов-докстрингов.

    Нужны всем структурным проверкам этого файла: про SSH и про модели в
    докстрингах написано НАМЕРЕННО, и запрещать там слова значило бы запрещать
    объяснение границы вместо самой границы.
    """
    ids: set[int] = set()
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
            ids.add(id(first.value))
    return ids


# ═════════════ A. Способность переживает провод ══════════════════════════════
def test_a_capability_survives_the_center_to_worker_roundtrip():
    """Способность не теряется ни на одном стыке «центр → провод → воркер».

    Дефект, который это ловит, не выглядит как поломка. Потеряйся `capability`
    в сериализации или на приёме — задание всё равно доедет, воркер всё равно
    запустится, резолвер выпишет привязку без модели, адаптер не передаст CLI
    флаг `--model`, и ответит модель учётной записи по умолчанию. Ровно эта
    тихая подмена наблюдалась на 11C и не была замечена ни одной проверкой.
    """
    payload = ProviderRequirementPayload(
        provider=PROVIDER, capability=CAPABILITY,
        allowed_stages=["block_analysis"], max_inferences=3,
    )
    # Объект → dict → JSON → dict: ровно те превращения, что происходят между
    # центром, workers.db и HTTP-ответом воркеру.
    wire = json.loads(json.dumps(payload.model_dump(), ensure_ascii=False))
    accepted = audit_runner._validate_provider_requirement(wire)
    requirement = resolver.ProviderRequirement.from_payload(accepted)

    assert wire["provider"] == payload.provider == PROVIDER
    assert wire["capability"] == payload.capability == CAPABILITY
    assert accepted["provider"] == PROVIDER and accepted["capability"] == CAPABILITY
    assert requirement is not None
    assert requirement.provider == PROVIDER
    assert requirement.capability == CAPABILITY
    assert requirement.max_inferences == 3
    # И точной модели нет ни на одном звене.
    assert wire["model"] is None and accepted["model"] is None
    assert requirement.model is None


# ═════════════ B. Обратная совместимость ═════════════════════════════════════
def test_b_pre_11g_payload_without_capability_still_parses():
    """Нагрузка, сохранённая ДО 11G, обязана разбираться всеми тремя рубежами.

    `extra="forbid"` отвергает незнакомый ключ целиком, поэтому «просто убрать
    поле `model` из схемы» сломало бы разбор всего, что уже лежит в workers.db,
    — и не при выкатке, а при первом же обращении к старому заданию.
    """
    legacy = {"provider": PROVIDER, "model": None, "allowed_stages": [],
              "max_inferences": 0}

    payload = ProviderRequirementPayload(**legacy)
    assert payload.capability is None and payload.model is None

    accepted = audit_runner._validate_provider_requirement(dict(legacy))
    assert accepted["capability"] is None

    requirement = resolver.ProviderRequirement.from_payload(dict(legacy))
    assert requirement is not None and requirement.capability is None


def test_b_params_without_provider_requirement_still_parse(tmp_path):
    """Задание без требования — это «как раньше», а не негодное задание.

    Воркеры и задания старых этапов требования не несут вовсе; если бы поле
    стало обязательным, весь ранее созданный контур перестал бы исполняться.
    """
    params = AuditPipelineParams(**_audit_params())
    assert params.provider_requirement is None

    safe = audit_runner.validate_params(_audit_params(),
                                        config=_worker_config(tmp_path))
    assert safe.provider_requirement is None
    assert resolver.ProviderRequirement.from_payload(None) is None


# ═════════════ C. Fail closed на трёх валидаторах ════════════════════════════
def test_c_provider_required_without_capability_is_refused_by_all_three(tmp_path):
    """«Зови модель, но чем — не скажу» отвергается КАЖДЫМ из трёх рубежей.

    Проверка именно по отдельности, а не «где-нибудь да поймается». Рубежи
    стоят на разных машинах и правятся разными людьми: центр может перестать
    формировать задание, приём на воркере — смягчиться, резолвер — начать
    подставлять умолчание. Каждая из трёх правок по отдельности выглядит
    безобидно, а вместе они возвращают вызов без `--model`.
    """
    import pydantic

    bad = {"provider": PROVIDER, "allowed_stages": ["block_analysis"],
           "max_inferences": 4}

    with pytest.raises(pydantic.ValidationError):
        ProviderRequirementPayload(**bad)

    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner._validate_provider_requirement(dict(bad))

    with pytest.raises(resolver.ProviderResolutionError):
        resolver.ProviderRequirement.from_payload(dict(bad))

    # И то же самое на полном приёме задания, а не только на разборе поля.
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(
            _audit_params(provider_requirement=dict(bad)),
            config=_worker_config(tmp_path),
        )


# ═════════════ D. Требование доезжает до создания задания ════════════════════
def _version_with_blocks(root: Path, *, graphic_blocks: int) -> Path:
    """Минимальное дерево версии со структурой документа для оценки бюджета."""
    version = root / "версия"
    (version / "01_input").mkdir(parents=True)
    (version / "01_input" / "проект_result.json").write_text(
        json.dumps({
            "pages": [{
                "blocks": [
                    {"type": "image", "crop_url": f"https://portal/crop/{i}"}
                    for i in range(graphic_blocks)
                ] + [{"type": "text"}],
            }],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    return version


def test_d_backend_passes_the_requirement_into_create_audit_job(
    center_env, admin, tmp_path, monkeypatch
):
    """Требование обязано доехать до `create_audit_job`, а не остаться в коде.

    Это и есть сам разрыв 11G: до него центр УМЕЛ принять требование, но
    штатный путь запуска его не формировал. Задание уезжало на воркер пустым,
    воркер не активировал мост, аудит шёл к подделкам — и «прошёл успешно»
    ничего не значило. Проверяется на настоящем `RemoteWorkerExecutionBackend`
    с перехватом аргументов создания задания, а не на чтении исходников.
    """
    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionMode,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import audit_job_service, repositories

    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    worker_id, _ = _approved_worker(admin, instance_id="inst_11g_pass")
    # Воркер объявляет НАСТОЯЩИЕ модели и нужную способность: без этого центр
    # обязан не формировать требование вовсе (см. тест N).
    repositories.update_worker_fields(
        worker_id,
        {"capabilities": json.dumps(_REAL_WORKER_CAPS, ensure_ascii=False)},
        settings=center_env,
    )
    version_dir = _version_with_blocks(tmp_path, graphic_blocks=2)

    captured: dict = {}

    def _capture(**kwargs):
        captured.update(kwargs)
        return {"job_id": "job-11g", "attempt_id": "att-11g",
                "execution_profile": "remote_audit_pilot_v1"}

    monkeypatch.setattr(audit_job_service, "create_audit_job", _capture)

    manager = _RecordingManager()
    manager._resolve_job_paths = lambda job: (
        version_dir.parent, version_dir, version_dir / "03_analysis"
    )
    backend = RemoteWorkerExecutionBackend(manager)
    item = BatchQueueItem(project_id="ИСП/11G")
    ctx = ExecutionContext(item=item, job=_FakeJob())
    handle = asyncio.run(
        backend.prepare(
            ExecutionRequest(
                project_id="ИСП/11G", job_id="job-1",
                execution_mode=ExecutionMode.REMOTE_WORKER,
                assigned_worker_id=worker_id,
            ),
            ctx,
        )
    )

    assert handle.attempt_id == "att-11g"
    requirement = captured.get("provider_requirement")
    assert requirement is not None, "задание создано БЕЗ требования к провайдеру"
    assert requirement["provider"] == PROVIDER
    assert requirement["capability"] == CAPABILITY
    assert requirement["model"] is None
    # Бюджет посчитан от структуры документа, а не взят круглым числом:
    # 2 графических блока + 6 текстовых этапов + 2 на технические повторы.
    assert requirement["max_inferences"] == 10
    assert "block_analysis" in requirement["allowed_stages"]


# ═════════════ E. Хранение в БД ══════════════════════════════════════════════
def test_e_pipeline_params_survive_the_json_round_trip():
    """Нагрузка задания хранится в БД строкой JSON — способность обязана выжить.

    Между созданием задания и его выдачей воркеру нагрузка ложится в
    `workers.db` текстом и читается оттуда обратно. Поле, потерянное на этом
    круге, не проявится ни в одном логе центра: он покажет требование, которое
    было в памяти, а на воркер уедет то, что осталось в базе.
    """
    params = AuditPipelineParams(
        **_audit_params(provider_requirement=dict(_WIRE_REQUIREMENT))
    )
    blob = json.dumps(params.model_dump(), ensure_ascii=False)
    restored = AuditPipelineParams(**json.loads(blob))

    assert restored.provider_requirement is not None
    assert restored.provider_requirement.capability == CAPABILITY
    assert restored.provider_requirement.provider == PROVIDER
    assert restored.provider_requirement.max_inferences == 8
    assert restored.provider_requirement.model is None
    assert "claude-opus" not in blob


# ═════════════ F. Сериализация выдачи задания ════════════════════════════════
def test_f_assignment_params_keep_the_capability():
    """`JobAssignment.params` — объединение двух моделей, и оно теряет поля молча.

    `params: Union[AuditPipelineParams, TestJobParams]`; обе с `extra="forbid"`.
    Если объединение разберёт нагрузку не той моделью (или pydantic приведёт её
    к соседней), лишние поля не вызовут ошибки — они просто исчезнут, и воркер
    получит задание без требования, а центр будет уверен, что отправил его.
    """
    from fastapi.encoders import jsonable_encoder

    from backend.app.api.routers.audit_worker_agent import _assignment_params
    from backend.app.models.distributed_workers import (
        JobAssignment,
        JobType,
        PackageRef,
    )

    params = _assignment_params(
        {"job_type": JobType.AUDIT_PIPELINE_V1.value},
        {"params": _audit_params(provider_requirement=dict(_WIRE_REQUIREMENT))},
    )
    assert isinstance(params, AuditPipelineParams)
    assert params.provider_requirement.capability == CAPABILITY

    assignment = JobAssignment(
        job_id="job-11g", attempt_id="att-11g", attempt_no=1,
        execution_token="tok", assigned_at=0.0, assign_ttl_sec=600,
        job_type=JobType.AUDIT_PIPELINE_V1, project_id="ИСП/11G",
        params=params,
        package=PackageRef(
            package_id="pkg", package_type="source", url="/api/v1/worker/x",
            size_bytes=1, sha256="a" * 64, compression="gzip", manifest_version=1,
        ),
    )
    wire = json.loads(json.dumps(jsonable_encoder(assignment), ensure_ascii=False))
    assert wire["params"]["provider_requirement"]["capability"] == CAPABILITY
    assert wire["params"]["provider_requirement"]["model"] is None


# ═════════════ G. Разбор нагрузки на воркере ═════════════════════════════════
def test_g_worker_parses_full_central_params_with_capability(tmp_path):
    """Полная нагрузка центра принимается воркером, и способность доезжает в spec.

    Приём задания — отдельный рубеж со своим разбором (`audit_runner`), он
    намеренно не импортирует ни pydantic-схему центра, ни провайдерский слой.
    Значит его согласие с центром ничем, кроме этого теста, не удерживается:
    ужесточение любой из сторон превращается в «задание отвергнуто на воркере»
    уже в бою.
    """
    params = audit_runner.validate_params(
        _audit_params(provider_requirement=dict(_WIRE_REQUIREMENT)),
        config=_worker_config(tmp_path),
    )
    assert params.provider_requirement["provider"] == PROVIDER
    assert params.provider_requirement["capability"] == CAPABILITY
    assert params.provider_requirement["model"] is None
    assert params.as_dict()["provider_requirement"]["max_inferences"] == 8


# ═════════════ H/I. Неизвестный провайдер и неизвестная способность ══════════
def test_h_unsupported_provider_is_refused_on_both_sides():
    """Провайдер вне закрытого набора отвергается и на центре, и на воркере.

    На воркере отказ даёт РЕЗОЛВЕР, а не приём задания: `audit_runner` проверяет
    только форму строки (он намеренно не знает списка провайдеров — граница
    11b). Тест фиксирует именно такое распределение: если однажды форму сочтут
    достаточной и резолвер смягчат, требование `provider="gpt"` дойдёт до
    выбора адаптера и упадёт уже в середине оплаченного прогона.
    """
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        ProviderRequirementPayload(provider="gpt", capability=CAPABILITY,
                                   max_inferences=1)

    with pytest.raises(resolver.ProviderResolutionError):
        resolver.ProviderRequirement.from_payload(
            {"provider": "gpt", "capability": CAPABILITY, "max_inferences": 1}
        )
    # Форма имени на воркере проверяется отдельно и раньше.
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner._validate_provider_requirement(
            {"provider": "gpt-5!", "capability": CAPABILITY, "max_inferences": 1}
        )


def test_i_unsupported_capability_is_refused_on_both_sides():
    """Незнакомая способность — отказ, а не «возьмём модель по умолчанию».

    Реестр способностей закрыт с обеих сторон намеренно. Открытый набор
    означал бы, что опечатка в требовании («strong_аudit» с кириллической «а»)
    проходит все проверки и заканчивается вызовом без назначенной модели.
    """
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        ProviderRequirementPayload(provider=PROVIDER, capability="cheap_audit",
                                   max_inferences=1)

    with pytest.raises(resolver.ProviderResolutionError):
        resolver.ProviderRequirement.from_payload(
            {"provider": PROVIDER, "capability": "cheap_audit", "max_inferences": 1}
        )
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner._validate_provider_requirement(
            {"provider": PROVIDER, "capability": "не способность",
             "max_inferences": 1}
        )


def test_center_capabilities_are_a_subset_of_the_worker_policy():
    """Центр не вправе заказать способность, которую ни один воркер не разрешит.

    Два закрытых реестра в разных пакетах (`backend/app/models` и
    `audit_worker/providers`) не связаны ни импортом, ни сборкой: пакет воркера
    ставится на чужой VPS отдельно. Разъедься они — центр начнёт формировать
    требования, которые отвергаются уже ПОСЛЕ сборки пакета и выдачи задания,
    то есть после потраченных места и времени на обеих сторонах.
    """
    from backend.app.models.distributed_workers import (
        CAPABILITY_STRONG_AUDIT,
        KNOWN_CAPABILITIES as CENTER_CAPABILITIES,
    )

    assert set(CENTER_CAPABILITIES) <= set(model_policy.KNOWN_CAPABILITIES)
    assert CAPABILITY_STRONG_AUDIT == model_policy.CAPABILITY_STRONG_AUDIT


# ═════════════ J. Точную модель называет ЛОКАЛЬНАЯ политика ══════════════════
def test_j_same_capability_resolves_to_different_models_per_machine(tmp_path,
                                                                    monkeypatch):
    """Одна способность на двух машинах даёт РАЗНЫЕ модели — и это правильно.

    Утверждение «модель выбирает машина» проверяемо только так: если бы
    идентификатор приходил из требования или лежал константой в коде, обе
    машины ответили бы одинаково. Тест этим и отличает «воркер читает свою
    политику» от «воркер делает вид, что читает».
    """
    monkeypatch.delenv(model_policy.POLICY_ENV, raising=False)
    roots = {}
    for name, model in (("вппс-1", "claude-opus-5"), ("вппс-2", "claude-sonnet-5")):
        root = tmp_path / name
        root.mkdir()
        (root / model_policy.POLICY_FILENAME).write_text(json.dumps({
            "policy_version": 1,
            PROVIDER: {"auth_mode": "ambient_user",
                       "capabilities": {CAPABILITY: {"model": model}}},
        }, ensure_ascii=False), encoding="utf-8")
        roots[name] = root

    first = model_policy.load_policy(roots["вппс-1"]).resolve(PROVIDER, CAPABILITY)
    second = model_policy.load_policy(roots["вппс-2"]).resolve(PROVIDER, CAPABILITY)

    assert first.model == "claude-opus-5"
    assert second.model == "claude-sonnet-5"
    assert first.capability == second.capability == CAPABILITY
    # Способность, которой политика не описывает, — отказ, а не умолчание.
    with pytest.raises(model_policy.ProviderPolicyError):
        model_policy.load_policy(roots["вппс-1"]).resolve(PROVIDER, "cheap_audit")


# ═════════════ K. Запрет на точную модель от центра ══════════════════════════
def test_k_payload_carrying_an_exact_model_is_refused():
    """Требование с `model` отвергается всеми тремя валидаторами.

    Два независимых основания, и оба существенные: строка задания, дошедшая до
    argv стороннего CLI, ломает I-P5, а «центр назначает модель» означает, что
    чужой машине передано право распоряжаться подпиской человека, на чьём VPS
    она исполняется. Поле оставлено в схеме только ради разбора СТАРЫХ
    нагрузок — и принимает исключительно пустое значение.
    """
    import pydantic

    with_model = {"provider": PROVIDER, "capability": CAPABILITY,
                  "model": "claude-opus-5", "max_inferences": 1}

    with pytest.raises(pydantic.ValidationError):
        ProviderRequirementPayload(**with_model)
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner._validate_provider_requirement(dict(with_model))
    with pytest.raises(resolver.ProviderResolutionError):
        resolver.ProviderRequirement.from_payload(dict(with_model))


#: Куски идентификаторов моделей. Список намеренно грубый: он должен ловить и
#: те строки, которых сегодня в коде нет.
_MODEL_MARKERS = ("claude-opus", "opus", "sonnet", "gpt-", "codex/")


def test_k_no_model_identifier_reaches_the_requirement_or_its_builders(tmp_path):
    """Ни в требовании, ни в строящем его коде центра нет идентификатора модели.

    Почему здесь уместен подстрочный поиск, а не что-то «умнее». Утверждение
    проверяется отрицательное — «такого нет НИГДЕ», — и относится оно к
    содержимому строк. У центра нет и не может быть источника, откуда
    идентификатор модели взялся бы вычислением: реестра моделей на центре не
    существует, локальная политика лежит на воркере. Значит попасть в
    требование он способен ровно одним путём — литералом в этих двух модулях,
    и поиск по строковым константам накрывает этот путь целиком.

    Честность проверки держится на двух ограничениях: докстринги исключены (в
    них про модели написано намеренно), а сканируются только строковые
    константы дерева разбора, а не текст файла, — иначе тест ловил бы
    собственные комментарии и провоцировал бы правку комментария вместо кода.
    """
    from backend.app.services.distributed_workers import provider_requirement as prs

    payload, rationale = prs.build_audit_requirement(
        version_dir=_version_with_blocks(tmp_path, graphic_blocks=3)
    )
    blob = json.dumps(payload.model_dump(), ensure_ascii=False).lower()
    for marker in _MODEL_MARKERS:
        assert marker not in blob, f"в требовании оказался идентификатор модели: {marker}"
    assert payload.model is None
    assert rationale["exact_model_in_payload"] is False
    assert json.dumps(rationale, ensure_ascii=False).lower().count("opus") == 0

    for rel in (
        "backend/app/services/distributed_workers/provider_requirement.py",
        "backend/app/pipeline/execution/remote.py",
    ):
        tree = ast.parse((REPO_ROOT / rel).read_text(encoding="utf-8"))
        docstrings = _docstring_ids(tree)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if id(node) in docstrings:
                continue
            lowered = node.value.lower()
            for marker in _MODEL_MARKERS:
                assert marker not in lowered, (
                    f"{rel}:{node.lineno}: идентификатор модели {marker!r} в коде центра"
                )


# ═════════════ L/M. Автоматическая привязка и разрешение ═════════════════════
def _executor_for_binding(config) -> "object":
    """Исполнитель БЕЗ фонового цикла: только то, чем пользуется привязка.

    Полный `Executor.__init__` поднимает worker.db, реестр процессов, менеджер
    хранения и регистрирует экземпляр исполнителя — всё это к выписке привязки
    отношения не имеет, а в тесте означало бы минуты на посторонние побочные
    эффекты. Поэтому объект собирается вручную из четырёх полей, которые
    `prepare_provider_binding` действительно читает.
    """
    from audit_worker.executor import Executor
    from audit_worker.local_store import LocalJobStore

    executor = object.__new__(Executor)
    executor.config = config
    executor.jobs = LocalJobStore(config.jobs_dir)
    executor._bound_providers = {}
    executor._grant_wait_since = {}
    return executor


def _auto_grant_config(worker_root: Path, exe: Path, tmp_path: Path):
    from audit_worker.config import WorkerConfig

    pipeline_root = tmp_path / "platform"
    pipeline_root.mkdir(parents=True, exist_ok=True)
    config = WorkerConfig(
        dispatcher_url="https://center.example",
        root=worker_root,
        display_name="vps-11g",
        pipeline_revision="rev-abc123",
        pipeline_root=pipeline_root,
        audit_pipeline_enabled=True,
        allow_real_llm=True,
        pipeline_provider_bridge_enabled=True,
        pipeline_provider_auto_grant_enabled=True,
        pipeline_provider_max_inferences=12,
        pipeline_provider_grant_ttl_sec=600.0,
        provider_auth_modes={PROVIDER: AUTH_MODE_AMBIENT_USER},
        provider_executables={PROVIDER: exe},
    )
    config.ensure_dirs()
    return config


def test_l_binding_is_written_without_any_operator_grant_file(worker_root, tmp_path):
    """Привязка выписывается САМА, без файла, созданного человеком.

    Это и есть содержательная часть 11G. До него третье разрешение из тройки
    («оператор разрешает конкретное задание») существовало файлом, который
    человек создавал руками ПОСЛЕ появления задания, — то есть удалённый аудит
    не мог начаться, пока кто-то не зайдёт на VPS. Тест доказывает, что файла
    больше не нужно, и одновременно — что ни одно свойство разрешения при этом
    не потерялось: оно выписано, привязано к заданию и уже списано.
    """
    exe = _fake_claude(tmp_path / "bin" / "claude")
    config = _auto_grant_config(worker_root, exe, tmp_path)
    executor = _executor_for_binding(config)

    assert not inference_grant.grant_path(worker_root).exists(), (
        "файл разрешения существует ДО вызова — тест доказывал бы не то"
    )

    params = SimpleNamespace(provider_requirement=dict(_WIRE_REQUIREMENT))
    path = executor.prepare_provider_binding(
        {"job_id": "job-11g", "attempt_id": "att-11g"}, params
    )

    assert path is not None and path.name == "provider_binding.json"
    binding = json.loads(path.read_text(encoding="utf-8"))
    assert binding["provider"] == PROVIDER
    assert binding["capability"] == CAPABILITY
    # Точная модель пришла из ЛОКАЛЬНОЙ политики, а не из требования.
    assert binding["model"] == "claude-opus-5"
    assert binding["grant_id"] == "auto-att-11g"
    assert binding["job_id"] == "job-11g" and binding["attempt_id"] == "att-11g"


def test_m_auto_grant_records_its_provenance_and_stays_private(worker_root):
    """У автоматического разрешения обязан быть читаемый след и режим 0600.

    Автор записи сменился с человека на код — и единственное, что позволяет
    потом ответить «кто и на каком основании потратил вызовы», это её
    содержимое. Запись без задания, попытки и способности в примечании
    неотличима от разрешения, выписанного кем угодно и на что угодно.

    Режим файла проверяется здесь же, а не «где-то в другом тесте»: разрешение
    — ключ к чужой подписке, и мировая читаемость делает рубеж декоративным
    (`consume` такой файл и отвергает — см. соседний тест 11C).
    """
    record = inference_grant.issue_for_job(
        worker_root, provider=PROVIDER, job_id="job-11g", attempt_id="att-11g",
        capability=CAPABILITY, requested_max_inferences=8,
        machine_ceiling=12, ttl_sec=600,
    )
    assert record.grant_id == "auto-att-11g"
    assert "job=job-11g" in record.note
    assert "attempt=att-11g" in record.note
    assert f"capability={CAPABILITY}" in record.note
    assert "8/12" in record.note, "в следе не видно ни запроса, ни потолка машины"

    path = inference_grant.grant_path(worker_root)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600, oct(path.stat().st_mode)
    # Срок жизни задан: разрешение без срока — открытая дверь, о которой никто
    # не помнит.
    assert record.expires_at > 0


# ═════════════ N. Несовместимый воркер ═══════════════════════════════════════
def test_n_requirement_is_not_built_for_a_worker_the_center_cannot_run_on(
    center_env, monkeypatch
):
    """Для негодного воркера требование не СОЗДАЁТСЯ — отказ раньше задания.

    Отказ `create_audit_job` несовместимому воркеру проверяют существующие
    тесты (`test_worker_rejects_revision_mismatch`,
    `test_audit_launch_rejects_incompatible_worker` в
    tests/test_distributed_workers_execution_backend.py) — здесь они не
    повторяются. Утверждение 11G другое и своё: требование к провайдеру не
    доходит даже до попытки создать задание, если воркер не объявил
    способность. Разница практическая: требование — это авторизация расхода
    ЧУЖОЙ подписки, и формировать её для машины, которая её всё равно не
    исполнит, значит выпускать подпись впустую.
    """
    from backend.app.core import config as core_config
    from backend.app.services.distributed_workers import (
        audit_job_service,
        provider_requirement as prs,
    )

    monkeypatch.setattr(core_config, "AUDIT_PIPELINE_REVISION", "rev-центра")
    worker = {
        "worker_id": "wrk_11g",
        "display_name": "чужая ревизия",
        "protocol_version": center_env.protocol_version,
        "registration_status": "approved",
        "connection_status": "online",
        "pipeline_revision": "rev-другая",
        # Настоящие модели объявлены, а вот способность — нет: ровно машина без
        # локальной политики моделей.
        "capabilities": json.dumps({
            "provider_mode": "real",
            "real_llm_enabled": True,
            "pipeline_provider_bridge_enabled": True,
            "job_types": ["audit_pipeline_v1"],
            "pipeline_revision": "rev-другая",
        }, ensure_ascii=False),
        "resource_snapshot": json.dumps({"executor": {"status": "online"}}),
    }

    report = audit_job_service.compatibility_report(
        worker, settings=center_env, active_attempts=[]
    )
    assert report["compatible"] is False
    assert "code_revision_mismatch" in {r["code"] for r in report["reasons"]}

    ok, why = prs.worker_supports(worker, provider=PROVIDER, capability=CAPABILITY)
    # Причина названа словами, а не молчаливым «недоступен»: оператор обязан
    # понять, что чинить — здесь это отсутствующая локальная политика моделей.
    assert ok is False and "способност" in why
    with pytest.raises(prs.ProviderRequirementError):
        prs.build_audit_requirement(version_dir=Path("/nonexistent"), worker=worker)


# ═════════════ AC. SSH не является транспортом заданий ═══════════════════════
#: Имена, которыми в этом репозитории мог бы появиться удалённый запуск.
_REMOTE_EXEC_BINARIES = {
    "ssh", "scp", "sftp", "rsync", "sshpass", "ssh-copy-id", "ssh-keygen",
}
_REMOTE_EXEC_MODULES = ("paramiko", "fabric", "pexpect", "asyncssh", "spur")


def _command_head_literals(call: ast.Call) -> list[str]:
    """Первые токены командных строк, переданных в вызов.

    Смотрим и на строковую константу («ssh host …»), и на первый элемент
    списка/кортежа (`["ssh", host]`) — argv собирают и так и так.
    """
    heads: list[str] = []
    for arg in list(call.args) + [kw.value for kw in call.keywords]:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            parts = arg.value.strip().split()
            if parts:
                heads.append(parts[0])
        elif isinstance(arg, (ast.List, ast.Tuple)) and arg.elts:
            first = arg.elts[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                parts = first.value.strip().split()
                if parts:
                    heads.append(parts[0])
    return heads


def test_ac_ssh_is_not_a_job_transport():
    """Задание не переносится по SSH ни на воркер, ни обратно.

    Инвариант структурный по природе: «удалённого запуска нет» иначе как
    отсутствием вызовов не выражается. Проверка идёт по дереву разбора, а не
    подстрокой: слова `ssh`/`scp` в докстрингах и комментариях стоят
    НАМЕРЕННО (`probe_grant` объясняет, как оператор заходит на машину руками;
    `project_package` исключает `.ssh` из пакета), и текстовый греп на них
    ложно срабатывал бы, подталкивая править объяснение вместо кода.

    `subprocess` при этом разрешён и обязан быть разрешён: воркер запускает
    ЛОКАЛЬНЫЕ CLI провайдеров. Запрещено ровно одно — чтобы запускаемая
    команда оказалась средством удалённого доступа.
    """
    offenders: list[str] = []
    for path in _python_files("audit_worker",
                              "backend/app/services/distributed_workers"):
        rel = path.relative_to(REPO_ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in getattr(node, "names", [])]
                names.append(getattr(node, "module", "") or "")
                for name in names:
                    if any(name.split(".")[0] == mod for mod in _REMOTE_EXEC_MODULES):
                        offenders.append(f"{rel}:{node.lineno}: import {name}")
            elif isinstance(node, ast.Call):
                for head in _command_head_literals(node):
                    if Path(head).name in _REMOTE_EXEC_BINARIES:
                        offenders.append(f"{rel}:{node.lineno}: запуск {head!r}")
    assert not offenders, offenders


# ═════════════ AD. Воркер не открывает входящих портов ═══════════════════════
def test_ad_worker_opens_no_inbound_port():
    """Воркер только ЗВОНИТ центру и никогда не слушает сам.

    Модель связи — исходящий long-poll: воркер стоит на чужом VPS, часто за
    NAT, и любой слушающий сокет на нём — это новая поверхность атаки, которую
    никто не администрирует. Дефект такого рода не проявляется в тестах вовсе:
    открытый порт работает исправно ровно до того дня, когда его находит не
    центр.
    """
    banned_modules = ("socketserver", "http.server", "uvicorn", "flask",
                      "fastapi", "aiohttp.web")
    banned_calls = {"bind", "listen", "serve_forever"}
    offenders: list[str] = []
    for path in _python_files("audit_worker"):
        rel = path.relative_to(REPO_ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in getattr(node, "names", [])]
                names.append(getattr(node, "module", "") or "")
                for name in names:
                    if not name:
                        continue
                    if name == "socket" or any(
                        name == mod or name.startswith(mod + ".")
                        for mod in banned_modules
                    ):
                        offenders.append(f"{rel}:{node.lineno}: import {name}")
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr in banned_calls:
                    offenders.append(f"{rel}:{node.lineno}: .{func.attr}()")
                if (isinstance(func, ast.Attribute) and func.attr == "run"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "uvicorn"):
                    offenders.append(f"{rel}:{node.lineno}: uvicorn.run()")
    assert not offenders, offenders


# ═════════════ AE. TLS ═══════════════════════════════════════════════════════
def test_ae_client_verifies_tls_and_never_follows_redirects():
    """Проверка сертификата включена всегда, редиректы не выполняются никогда.

    Два независимых дефекта. Первый: глобальный `verify=False` — тихое снятие
    защиты канала, по которому едут токен воркера и исходники проекта; его
    нельзя сделать настраиваемым, иначе он однажды окажется выключенным «на
    время отладки». Второй: следование редиректу переносит заголовок
    `Authorization` на адрес, который назвал ответ, — то есть отдаёт токен туда,
    куда его никто не собирался отправлять.
    """
    import httpx

    from audit_worker.client import CenterClient

    signature = inspect.signature(CenterClient.__init__)
    assert signature.parameters["verify"].default is True

    client = CenterClient(
        "https://center.example",
        transport=httpx.MockTransport(lambda request: httpx.Response(204)),
    )
    try:
        assert client._client.follow_redirects is False
    finally:
        client.close()

    # И нигде в пакете воркера защита не ослабляется параметром.
    for path in _python_files("audit_worker"):
        rel = path.relative_to(REPO_ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if not isinstance(kw.value, ast.Constant):
                    continue
                assert not (kw.arg == "verify" and kw.value.value is False), (
                    f"{rel}:{node.lineno}: verify=False"
                )
                assert not (kw.arg == "follow_redirects" and kw.value.value is True), (
                    f"{rel}:{node.lineno}: follow_redirects=True"
                )
    # Переменной окружения, отключающей TLS, не существует.
    source = (REPO_ROOT / "audit_worker/config.py").read_text(encoding="utf-8")
    assert "AUDIT_WORKER_VERIFY_TLS" not in source


def test_ae_http_to_a_foreign_host_is_refused(tmp_path):
    """HTTP к внешнему хосту роняет агента на старте, а не работает молча.

    Тихая работа открытым текстом — худший исход: воркер выглядит здоровым,
    heartbeat идёт, а токен и исходники проекта едут по сети как есть.
    Послабление есть ровно одно и только для localhost, и оно требует явного
    флага — иначе dev-режим стал бы боевым по умолчанию.
    """
    from audit_worker.config import (
        InsecureTransportError,
        WorkerConfig,
        validate_transport_security,
    )

    def _config(url: str, **extra) -> WorkerConfig:
        return WorkerConfig(dispatcher_url=url, root=tmp_path / "w",
                            display_name="tls", **extra)

    with pytest.raises(InsecureTransportError):
        validate_transport_security(_config("http://center.example"))
    with pytest.raises(InsecureTransportError):
        validate_transport_security(_config("ftp://center.example"))
    # localhost без явного флага — тоже отказ.
    with pytest.raises(InsecureTransportError):
        validate_transport_security(_config("http://127.0.0.1:8081"))
    # И два законных случая.
    validate_transport_security(_config("https://center.example"))
    validate_transport_security(
        _config("http://127.0.0.1:8081", allow_insecure_localhost=True)
    )


# ═════════════ AF. Секреты и приватные пути ══════════════════════════════════
def test_af_requirement_and_binding_carry_no_credentials_or_paths(tmp_path):
    """Ни в требовании, ни в публичном виде привязки нет секретов и путей.

    Требование едет от центра к воркеру, публичный вид привязки — обратно, в
    события и на экран оператора. И то и другое многократно копируется, поэтому
    каждое лишнее поле здесь означает секрет, размноженный по логам: путь к
    учётным данным, домашний каталог человека, контрольные литералы оператора.
    """
    from backend.app.services.distributed_workers import provider_requirement as prs

    payload, _ = prs.build_audit_requirement(
        version_dir=_version_with_blocks(tmp_path, graphic_blocks=1)
    )
    requirement_blob = json.dumps(payload.model_dump(), ensure_ascii=False)
    public = _binding(tmp_path / "jobs" / "j" / "a",
                      executable=tmp_path / "bin" / "claude").as_public_dict()
    public_blob = json.dumps(public, ensure_ascii=False)

    for blob, where in ((requirement_blob, "требование"), (public_blob, "привязка")):
        lowered = blob.lower()
        for marker in ("token", "secret", "api_key", "sk-ant", "wtk_",
                       "/home/", "/root/", ".credentials", "authorization"):
            assert marker not in lowered, f"{where}: найдено {marker!r} → {blob}"

    # У требования полей для этого нет по построению — перечисляем их поимённо.
    assert set(payload.model_dump()) == {
        "provider", "capability", "model", "allowed_stages", "max_inferences",
    }
    # А публичный вид привязки не показывает ни путей, ни канареек оператора.
    for banned in ("provider_root", "executable", "forbidden_literals"):
        assert banned not in public


def test_af_environment_secrets_do_not_leak_into_the_requirement(tmp_path,
                                                                 monkeypatch):
    """Снимок окружения не имеет права попасть в требование через чёрный ход.

    Требование строится на центре, где в окружении лежат и токен портала, и
    ключи платных провайдеров. Любая попытка «заодно передать флаги» превратила
    бы это поле в канал утечки — поэтому проверка идёт на живом окружении с
    подставленными секретами, а не на чтении кода.
    """
    from backend.app.services.distributed_workers import provider_requirement as prs

    monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_supersecret_value")
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-supersecret-value")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "портальный-секрет-4711")

    payload, rationale = prs.build_audit_requirement(
        version_dir=_version_with_blocks(tmp_path, graphic_blocks=1)
    )
    blob = json.dumps(
        {"requirement": payload.model_dump(), "rationale": rationale},
        ensure_ascii=False,
    )
    assert "wtk_supersecret_value" not in blob
    assert "sk-or-supersecret-value" not in blob
    assert "портальный-секрет-4711" not in blob
    assert os.environ["AUDIT_WORKER_TOKEN"] == "wtk_supersecret_value"


# ═════════════ Дефект, найденный ПЕРВЫМ сетевым прогоном ═════════════════════
def test_md_prescan_report_path_is_portable(tmp_path):
    """Пакет результата не должен отвергаться из-за `01_text_prescan.json`.

    Что произошло. Первый сетевой прогон 11G дошёл до приёма пакета центром и
    получил отказ целиком: `md_prescan` пишет в отчёт поле `md_file` с
    АБСОЛЮТНЫМ путём каталога попытки на воркере, а контракт переносимости
    (`portable_paths`) знал соседнее имя `md_path` и не знал этого. Отказ был
    правильным — недоставало строки в таблице; молча пропустить чужой путь
    было бы хуже, он остался бы в дереве заказчика навсегда.

    Почему дефект не всплыл на 11F: предсканирование MD там молча не
    выполнялось из-за неверного корня версии (исправлено b2247d8a), то есть
    файла с этим полем не возникало вовсе.

    Тест держит ОБА утверждения: поле описано контрактом и превращается в путь
    относительно каталога версии (а не отбрасывается — на центре он осмыслен).
    """
    from backend.app.services.distributed_workers import portable_paths

    version = (
        tmp_path / "objects" / "О" / "disciplines" / "ВК" / "documents" / "Д"
        / "versions" / "v001"
    )
    run_dir = version / "03_analysis" / "runs" / "job-1"
    run_dir.mkdir(parents=True)
    (run_dir / "01_text_prescan.json").write_text(
        json.dumps(
            {
                "md_file": (
                    "/home/coder/audit-worker-11g/data/jobs/j/a/project/objects/О"
                    "/disciplines/ВК/documents/Д/versions/v001/02_work/document.md"
                ),
                "prescan_total": 3,
                "candidates": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    report = portable_paths.normalize_staged_tree(tmp_path, version_id="v001")

    assert report.violations == [], "пакет был бы отвергнут целиком"
    assert [row["key"] for row in report.rewritten] == ["md_file"]
    written = json.loads((run_dir / "01_text_prescan.json").read_text(encoding="utf-8"))
    assert written["md_file"] == "02_work/document.md"
    assert "md_file" in portable_paths.RELATIVIZE_KEYS
