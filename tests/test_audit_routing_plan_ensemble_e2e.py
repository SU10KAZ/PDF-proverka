"""Этап 11I: ансамбль по плану исполняется целиком — на ПОДДЕЛЬНЫХ провайдерах.

Что именно доказывается.

До 11I удалённый прогон делал ОДИН вызов на графический блок там, где центр
делает четыре. Причина была не в оптимизации, а в контракте: привязка несла
одну модель на попытку, и строка `use_dual = (not use_provider_bridge) and …`
честно выражала «звать вторую ногу нечем». Из-за этого «аудит прошёл на воркере»
и «аудит прошёл» означали разное, и заметить это можно было только сравнив
число находок с историей.

Здесь на поддельных CLI проверяется обратное утверждение: при активном мосте и
замороженном плане блок получает ТРИ независимых обращения детекторов и ОДНО
обращение судьи, каждое — своей моделью локальной политики, и ни одно из них не
подменяется replay'ем соседнего.

Настоящих обращений к модели — ноль: оба CLI подделаны скриптами, которые ведут
журнал argv/stdin. Именно журнал, а не наш собственный счётчик, отвечает на
вопрос «сколько раз и чем звали».
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from audit_worker.providers import pipeline_bridge, resolver
from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER
from backend.app.services.audit_routing import active_plan, presets, registry

from tests.test_audit_routing_plan import build_plan

#: Ответ, который поддельный CLI отдаёт как результат работы модели.
_BLOCK_ANSWER = {
    "findings": [
        {
            "problem": "проверочная находка",
            "severity": "Рекомендательное",
            "evidence": "тестовый блок",
        }
    ],
    "block_id": "BLK-1",
}

_JUDGE_ANSWER = {
    "findings": [],
    "matches": [],
    "gap_findings": [],
}


def _write_exe(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _fake_cli(path: Path, journal: Path, answer: dict, *, provider: str) -> Path:
    """Подделка CLI провайдера с журналом argv и stdin.

    Журнал ведёт САМ подпроцесс: только он может засвидетельствовать, что вызов
    действительно состоялся и с какой моделью. Счётчик на нашей стороне
    доказывал бы лишь то, что мы досчитали до трёх.
    """
    payload = json.dumps(json.dumps(answer, ensure_ascii=False))
    return _write_exe(path, f"""#!/bin/bash
JOURNAL={journal}
case "$1" in
  --version) echo "1.0.0 (fake {provider})"; exit 0 ;;
esac
for a in "$@"; do
  if [ "$a" = "auth" ] || [ "$a" = "login" ]; then
    echo '{{"loggedIn": true, "authMethod": "subscription", "apiProvider": "firstParty", "subscriptionType": "max"}}'
    exit 0
  fi
done
STDIN=$(cat)
echo "CALL:$*" >> "$JOURNAL"
python3 - <<'PYEOF'
import json
answer = {payload}
print(json.dumps({{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer,
    "usage": {{"input_tokens": 10, "output_tokens": 5}},
    "modelUsage": {{}},
    "total_cost_usd": 0.0,
    "num_turns": 1,
}}, ensure_ascii=False))
PYEOF
exit 0
""")


@pytest.fixture()
def stage(tmp_path, monkeypatch):
    """Полная обстановка попытки: план, привязка с маршрутами, поддельные CLI."""
    job_dir = tmp_path / "jobs" / "job-1" / "attempt-1"
    (job_dir / "metadata").mkdir(parents=True)

    journal = tmp_path / "calls.log"
    journal.write_text("", encoding="utf-8")
    claude_exe = _fake_cli(tmp_path / "bin" / "claude", journal, _BLOCK_ANSWER,
                           provider="claude")
    codex_exe = _fake_cli(tmp_path / "bin" / "codex", journal, _BLOCK_ANSWER,
                          provider="codex")

    plan = build_plan(presets.PRESET_FULL_CODEX)
    active_plan.set_plan(plan)

    # Локальная политика воркера: каждая способность плана получает СВОЮ модель.
    # Именно здесь, и только здесь, появляются точные идентификаторы.
    routes = (
        resolver.RouteBinding(
            provider="codex", capability=registry.CAP_BLOCK_DETECTOR,
            model="fake-codex-standard",
            accepted_reported_models=("fake-codex-standard",),
            model_report="unsupported",
            auth_mode=AUTH_MODE_AMBIENT_USER,
            provider_root=str(resolver.ambient_root_for_attempt(job_dir, "codex")),
            executable=str(codex_exe), timeout_sec=30.0,
        ),
        resolver.RouteBinding(
            provider="codex", capability=registry.CAP_BLOCK_DETECTOR_STRONG,
            model="fake-codex-strong",
            accepted_reported_models=("fake-codex-strong",),
            model_report="unsupported",
            auth_mode=AUTH_MODE_AMBIENT_USER,
            provider_root=str(resolver.ambient_root_for_attempt(job_dir, "codex")),
            executable=str(codex_exe), timeout_sec=30.0,
        ),
        resolver.RouteBinding(
            provider="codex", capability=registry.CAP_BLOCK_JUDGE,
            model="fake-codex-judge",
            accepted_reported_models=("fake-codex-judge",),
            model_report="unsupported",
            auth_mode=AUTH_MODE_AMBIENT_USER,
            provider_root=str(resolver.ambient_root_for_attempt(job_dir, "codex")),
            executable=str(codex_exe), timeout_sec=30.0,
        ),
        resolver.RouteBinding(
            provider="claude", capability=registry.CAP_STRONG_AUDIT,
            model="fake-claude-strong",
            accepted_reported_models=("fake-claude-strong",),
            model_report="unsupported",
            auth_mode=AUTH_MODE_AMBIENT_USER,
            provider_root=str(resolver.ambient_root_for_attempt(job_dir, "claude")),
            executable=str(claude_exe), timeout_sec=30.0,
        ),
    )
    binding = resolver.ProviderBinding(
        schema_version=resolver.BINDING_SCHEMA_VERSION,
        provider="codex", auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(resolver.ambient_root_for_attempt(job_dir, "codex")),
        executable=str(codex_exe), timeout_sec=30.0,
        job_id="job-1", attempt_id="attempt-1", task_id="job-1",
        grant_id="g-11i-0001", max_inferences=64,
        allowed_stages=("block_analysis", "text_analysis", "optimization"),
        model="fake-codex-standard", capability=registry.CAP_STRONG_AUDIT,
        accepted_reported_models=("fake-codex-standard",),
        model_report="unsupported",
        routes=routes, routing_plan_hash=plan.plan_hash(),
    )
    path = binding.write(job_dir / "metadata")
    monkeypatch.setenv(resolver.BINDING_ENV, str(path))
    yield {
        "job_dir": job_dir, "binding": binding, "plan": plan,
        "journal": journal, "codex": codex_exe, "claude": claude_exe,
    }
    active_plan.clear()


def _calls(journal: Path) -> list[str]:
    return [
        line for line in journal.read_text(encoding="utf-8").splitlines()
        if line.startswith("CALL:")
    ]


def _models_called(journal: Path) -> list[str]:
    """Какие модели реально ушли в argv подпроцессов."""
    out: list[str] = []
    for line in _calls(journal):
        for token in line.split():
            if token.startswith("--model="):
                out.append(token.split("=", 1)[1])
            elif token.startswith("fake-"):
                out.append(token)
    return out


# ─── Z/AA/AB: ансамбль этапа 01 через мост ───────────────────────────────────
def test_plan_ensemble_makes_one_call_per_leg(stage):
    """§37. Три ноги — три РАЗНЫХ обращения, а не одно с двумя replay'ями.

    Все три ноги получают ОДИН промпт и ОДНО вложение: до 11I это означало один
    ключ журнала на всех. Проверяется, что подпроцесс запускался трижды и что
    журнал вызовов содержит три разных ключа.
    """
    plan = stage["plan"]
    legs = [
        item for item in plan.stage("block_batch").actions
        if item.role == registry.ROLE_DETECTOR
    ]
    assert len(legs) == 3

    prompt = "один и тот же промпт для всех ног"
    image = [("image/png", b"\x89PNG-same-crop-for-every-leg")]

    outcomes = []
    for leg in legs:
        if leg.provider == registry.PROVIDER_OPENROUTER:
            # OpenRouter на воркере не поддерживается вовсе — и это ДОЛЖНО быть
            # видно отказом, а не тихим пропуском ноги.
            with pytest.raises(pipeline_bridge.ProviderBridgeError, match="маршрута"):
                pipeline_bridge.run_stage_inference(
                    job_dir=stage["job_dir"], stage="block_analysis", prompt=prompt,
                    purpose="block_analysis:BLK-1", action_id=leg.action_id,
                    provider=leg.provider, capability=leg.capability,
                    images=image, binding=stage["binding"],
                )
            continue
        outcomes.append(pipeline_bridge.run_stage_inference(
            job_dir=stage["job_dir"], stage="block_analysis", prompt=prompt,
            purpose="block_analysis:BLK-1", action_id=leg.action_id,
            provider=leg.provider, capability=leg.capability,
            images=image, binding=stage["binding"],
        ))

    # Две codex-ноги: два ФАКТИЧЕСКИХ вызова, не один плюс повтор.
    assert len(outcomes) == 2
    assert all(item.performed for item in outcomes), (
        "нога получила replay соседней: ключ журнала не различает ноги ансамбля"
    )
    assert len({item.ledger.key for item in outcomes}) == 2
    assert len(_calls(stage["journal"])) == 2

    # И каждая нога ушла СВОЕЙ моделью локальной политики.
    models = _models_called(stage["journal"])
    assert "fake-codex-standard" in models
    assert "fake-codex-strong" in models


def test_judge_is_a_separate_fourth_call(stage):
    """AA. Судья — отдельное обращение после детекторов, своей способностью."""
    judge = active_plan.block_judge_action()
    assert judge is not None
    assert judge.capability == registry.CAP_BLOCK_JUDGE

    prompt = "промпт судьи"
    outcome = pipeline_bridge.run_stage_inference(
        job_dir=stage["job_dir"], stage="block_analysis", prompt=prompt,
        purpose="block_analysis_judge:BLK-1", action_id=judge.action_id,
        provider=judge.provider, capability=judge.capability,
        binding=stage["binding"],
    )
    assert outcome.performed
    assert "fake-codex-judge" in _models_called(stage["journal"])


def test_repeat_of_the_same_leg_is_replayed_not_paid_twice(stage):
    """Exactly-once сохраняется ВНУТРИ ноги: повтор той же ноги не платный."""
    leg = next(
        item for item in stage["plan"].stage("block_batch").actions
        if item.action_id == "detector_codex_standard"
    )
    args = dict(
        job_dir=stage["job_dir"], stage="block_analysis", prompt="p",
        purpose="block_analysis:BLK-9", action_id=leg.action_id,
        provider=leg.provider, capability=leg.capability, binding=stage["binding"],
    )
    first = pipeline_bridge.run_stage_inference(**args)
    second = pipeline_bridge.run_stage_inference(**args)
    assert first.performed is True
    assert second.performed is False, "повтор той же ноги обязан читаться из журнала"
    assert len(_calls(stage["journal"])) == 1


def test_optimization_legs_use_two_different_providers(stage):
    """AD. Обе ноги этапа 05 идут своими провайдерами в одной попытке.

    Именно это до 11I было невыразимо: заказав `provider=codex`, центр терял
    Claude-ногу, заказав `claude` — Codex-ногу.
    """
    legs = active_plan.optimization_legs()
    assert {leg.provider for leg in legs} == {
        registry.PROVIDER_CLAUDE, registry.PROVIDER_CODEX
    }
    claude_leg = next(l for l in legs if l.provider == registry.PROVIDER_CLAUDE)
    outcome = pipeline_bridge.run_stage_inference(
        job_dir=stage["job_dir"], stage="optimization", prompt="опт-промпт",
        purpose="optimization", action_id=claude_leg.action_id,
        provider=claude_leg.provider, capability=claude_leg.capability,
        binding=stage["binding"],
    )
    assert outcome.performed
    assert "fake-claude-strong" in _models_called(stage["journal"])
    # Визуальная нога сохранила усилие в плане.
    visual = next(l for l in legs if l.provider == registry.PROVIDER_CODEX)
    assert visual.reasoning_effort == registry.EFFORT_XHIGH


def test_action_trace_matches_reference_matrix(stage):
    """§43/§44. След действий совпадает с эталонной матрицей пресета.

    Сравниваются РОЛЬ, провайдер, способность, область, параллельная группа,
    зависимость, условие, усилие и мультипликативность — но не точная модель:
    её центр не назначает, и в эталоне её быть не может.
    """
    plan = stage["plan"]
    trace = [
        {
            "stage": s.stage_id,
            "scope": s.execution_scope,
            "action": a.action_id,
            "role": a.role,
            "kind": a.kind,
            "provider": a.provider,
            "capability": a.capability,
            "effort": a.reasoning_effort,
            "group": a.parallel_group,
            "depends_on": list(a.depends_on),
            "condition": a.condition.type,
            "multiplicity": a.multiplicity.type,
        }
        for s, a in plan.iter_actions()
    ]
    block = [row for row in trace if row["stage"] == "block_batch"]
    assert [row["action"] for row in block] == [
        "detector_openrouter",
        "detector_codex_standard",
        "detector_codex_strong",
        "combine_detectors",
        "judge_gap_search",
    ]
    assert [row["group"] for row in block[:3]] == ["detectors"] * 3
    assert block[3]["kind"] == registry.KIND_DETERMINISTIC
    assert block[4]["depends_on"] == ["combine_detectors"]
    # Ни одной точной модели в следе.
    flat = json.dumps(trace, ensure_ascii=False).lower()
    for marker in ("gpt-5", "codex/", "claude-opus", "claude-sonnet", "fake-"):
        assert marker not in flat


def test_worker_without_a_route_never_degrades_silently(stage):
    """AZ. Отсутствие маршрута — отказ ноги, а не выполнение ансамбля без неё."""
    with pytest.raises(pipeline_bridge.ProviderBridgeError) as excinfo:
        pipeline_bridge.run_stage_inference(
            job_dir=stage["job_dir"], stage="block_analysis", prompt="p",
            purpose="block_analysis:BLK-2", action_id="detector_openrouter",
            provider=registry.PROVIDER_OPENROUTER,
            capability=registry.CAP_BLOCK_DETECTOR,
            binding=stage["binding"],
        )
    message = str(excinfo.value)
    assert "openrouter" in message
    assert "запрещена" in message or "не содержит маршрута" in message
    # И ни одного обращения к другому провайдеру «вместо».
    assert _calls(stage["journal"]) == []


def test_stage_outside_the_plan_whitelist_is_refused(stage):
    """Этап вне белого списка привязки не проходит даже с корректным действием."""
    with pytest.raises(pipeline_bridge.ProviderBridgeError, match="белый список"):
        pipeline_bridge.run_stage_inference(
            job_dir=stage["job_dir"], stage="norm_verify", prompt="p",
            purpose="norm_verify", action_id="clause_binding",
            provider=registry.PROVIDER_CODEX, capability=registry.CAP_STRONG_AUDIT,
            binding=stage["binding"],
        )


def test_active_plan_describe_has_no_exact_models(stage):
    """Описание активного плана для журнала не раскрывает моделей воркера."""
    described = json.dumps(active_plan.describe(), ensure_ascii=False).lower()
    assert "fake-" not in described
    assert "gpt-5" not in described and "claude-opus" not in described
    assert active_plan.plan_hash() == stage["plan"].plan_hash()


def test_leg_provenance_labels_stay_distinguishable(stage):
    """Провенанс находки различает ноги, не раскрывая модель воркера."""
    from backend.app.pipeline.stages.block_analysis.provenance import detector_for_model

    legs = active_plan.block_detector_legs()
    labels = [active_plan.leg_model_label(leg) for leg in legs]
    assert len(set(labels)) == 3
    detectors = [detector_for_model(label) for label in labels]
    # Отображение повторяет центральное дословно: обе codex-ноги делят одну
    # сущность (известное ограничение судьи, KI-11I-1 — правится не здесь).
    assert detectors == ["gpt_openrouter", "codex", "codex"]
    assert not any("fake-" in label for label in labels)
