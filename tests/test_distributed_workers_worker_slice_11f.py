"""Тесты этапа 11F — весь worker-участок конвейера через ProviderAdapter.

Что здесь проверяется и почему именно это.

11F закрыл четыре дыры, каждая из которых на воркере выглядела бы как исправная
работа:

  1. `block_analysis` ходил к модели ПРЯМЫМ `create_subprocess_exec` с
     `--allowedTools Read,Write` — мимо провайдерского слоя и со свободным
     доступом к файловой системе;
  2. общий путь моста молча ТЕРЯЛ изображения (`_run_cli` принимает
     `image_paths`, но ветка моста их не передавала);
  3. `optimization`/`optimization_critic`/`optimization_corrector` формально шли
     через мост, но получали промпт с инструкциями `Read`/`Write` в вызове с
     `--tools=`;
  4. «страж отсутствия» в `findings_review` ходил прямым `subprocess.run` и был
     fail-soft — на воркере это давало ТИХУЮ деградацию, неотличимую от
     «кандидатов не нашлось».

Плюс два дефекта, найденных попутно: ключ вызова не учитывал вложения (два
разных чертежа с одинаковым текстом задания дали бы один ключ), и фактическая
модель бралась как первый ключ `modelUsage`, куда CLI кладёт и свои служебные
модели.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ─── 1. Ключ вызова учитывает вложения ───────────────────────────────────────

def test_call_key_text_only_is_byte_compatible_with_pre_11f():
    """Текстовые вызовы обязаны сохранить ПРЕЖНИЙ ключ.

    Иначе повтор попытки 11D/11E не нашёл бы своего результата в журнале и
    оплатил бы вызов заново.
    """
    from audit_worker.providers.inference_ledger import call_key

    without = call_key(
        attempt_id="a1", provider="claude", purpose="text_analysis", prompt="X",
    )
    with_empty = call_key(
        attempt_id="a1", provider="claude", purpose="text_analysis", prompt="X",
        attachments_sha256="",
    )
    assert without == with_empty


def test_call_key_separates_blocks_with_identical_prompt():
    """Два блока с одинаковым текстом задания — РАЗНЫЕ оплачиваемые вызовы."""
    from audit_worker.providers.inference_ledger import call_key
    from audit_worker.providers.pipeline_bridge import attachments_digest

    prompt = "проанализируй чертёж"
    first = call_key(
        attempt_id="a1", provider="claude", purpose="block_analysis", prompt=prompt,
        attachments_sha256=attachments_digest([("image/png", b"\x89PNG-A")]),
    )
    second = call_key(
        attempt_id="a1", provider="claude", purpose="block_analysis", prompt=prompt,
        attachments_sha256=attachments_digest([("image/png", b"\x89PNG-B")]),
    )
    text_only = call_key(
        attempt_id="a1", provider="claude", purpose="block_analysis", prompt=prompt,
    )
    assert first != second, "разные чертежи получили один ключ — второй блок читал бы чужой ответ"
    assert first != text_only


def test_attachments_digest_is_order_sensitive():
    from audit_worker.providers.pipeline_bridge import attachments_digest

    a = attachments_digest([("image/png", b"A"), ("image/png", b"B")])
    b = attachments_digest([("image/png", b"B"), ("image/png", b"A")])
    assert a != b
    assert attachments_digest([]) == ""


# ─── 2. Фактическая модель ───────────────────────────────────────────────────

def test_model_resolution_ignores_cli_auxiliary_model():
    """Служебный haiku CLI не должен выдаваться за модель, ответившую на задание.

    Живой вызов 11F вернул `modelUsage` с двумя ключами, и служебный стоял
    ПЕРВЫМ. Прежняя реализация брала первый ключ — строгий гейт модели отверг бы
    исправный ответ Opus как подмену.
    """
    from audit_worker.providers.claude_adapter import _model_from_envelope

    envelope = {
        "modelUsage": {
            "claude-haiku-4-5-20251001": {"outputTokens": 19},
            "claude-opus-5": {"outputTokens": 28800},
        }
    }
    assert _model_from_envelope(envelope) == "claude-opus-5"


def test_model_resolution_prefers_top_level_field():
    from audit_worker.providers.claude_adapter import _model_from_envelope

    assert _model_from_envelope({"model": "claude-opus-5", "modelUsage": {"x": {}}}) == "claude-opus-5"
    assert _model_from_envelope({}) is None


# ─── 3. Разбор потокового вывода ─────────────────────────────────────────────

def test_parse_stream_json_takes_result_and_assistant_model():
    from audit_worker.providers.claude_adapter import parse_stream_json

    stdout = "\n".join([
        '{"type":"system","subtype":"init"}',
        '{"type":"assistant","message":{"model":"claude-opus-5","content":[]}}',
        '{"type":"result","is_error":false,"result":"{\\"findings\\": []}","usage":{"output_tokens":7}}',
        "",
    ])
    envelope = parse_stream_json(stdout)
    assert envelope is not None
    assert envelope["model"] == "claude-opus-5"
    assert json.loads(envelope["result"]) == {"findings": []}


def test_parse_stream_json_returns_none_without_result_event():
    """Отсутствие итогового объекта — ошибка CLI, а не пустой успех."""
    from audit_worker.providers.claude_adapter import parse_stream_json

    assert parse_stream_json('{"type":"system"}\n') is None
    assert parse_stream_json("") is None


# ─── 4. Мультимодальный argv ─────────────────────────────────────────────────

def test_multimodal_argv_keeps_every_isolation_flag():
    """Вызов с картинкой не имеет права быть «мягче» текстового."""
    from audit_worker.providers.claude_adapter import (
        _inference_argv,
        _inference_argv_multimodal,
    )

    text = _inference_argv("claude-opus-5")
    image = _inference_argv_multimodal("claude-opus-5")
    for flag in ("--safe-mode", "--strict-mcp-config", "--disable-slash-commands",
                 "--no-session-persistence", "--setting-sources=", "--tools=",
                 "--model=claude-opus-5"):
        assert flag in text, flag
        assert flag in image, f"мультимодальный argv потерял {flag}"
    assert "--input-format" in image and "stream-json" in image
    # Запрет инструментов поимённо тоже обязан сохраниться.
    assert any(a.startswith("--disallowed-tools=") for a in image)


# ─── 5. Провайдерский транспорт block_analysis ───────────────────────────────

def test_block_provider_prompt_carries_severity_and_forbids_files(tmp_path):
    from backend.app.pipeline.stages.block_analysis import provider_transport as pt

    built = pt.build_provider_prompt(
        system_prompt="Ты эксперт по КМ.", user_text="БЛОК 1",
    )
    assert "Ты эксперт по КМ." in built["prompt"]
    assert "БЛОК 1" in built["prompt"]
    # Шкала важности — то, что жило только в CLAUDE.md (дефект 11D.1).
    assert "КРИТИЧЕСКОЕ" in built["prompt"].upper()
    assert built["map"]["tools"] == 0
    assert "инструментов у тебя нет" in built["prompt"]


def test_read_crop_refuses_escape_and_missing(tmp_path):
    from backend.app.pipeline.stages.block_analysis import provider_transport as pt

    blocks = tmp_path / "blocks"
    blocks.mkdir()
    (blocks / "block_A.png").write_bytes(b"\x89PNG data")
    assert pt.read_crop(blocks, "block_A.png") == b"\x89PNG data"
    with pytest.raises(pt.BlockInputError):
        pt.read_crop(blocks, "missing.png")
    with pytest.raises(pt.BlockInputError):
        pt.read_crop(blocks, "../outside.png")
    (blocks / "empty.png").write_bytes(b"")
    with pytest.raises(pt.BlockInputError):
        pt.read_crop(blocks, "empty.png")


# ─── 6. Общий транспорт JSON-этапов ──────────────────────────────────────────

def test_json_stage_strips_file_instructions_and_paths():
    from backend.app.services.llm import provider_json_stage as pjs

    messages = [
        {"role": "system", "content": (
            "READ via Read tool: /srv/projects/x/_output/03_findings.json\n"
            "Проанализируй оптимизации.\n"
            "WRITE via Write tool: /srv/projects/x/_output/optimization.json\n"
            "Справка: файл лежит в /srv/projects/x/_output"
        )},
        {"role": "user", "content": "ДАННЫЕ"},
    ]
    built = pjs.build_provider_prompt(messages, root_key="optimizations")
    assert built["file_instructions_stripped"] == 2
    assert built["absolute_paths_remaining_in_instructions"] == 0
    assert "Read tool" not in built["prompt"]
    assert "Write tool" not in built["prompt"]
    assert "Проанализируй оптимизации." in built["prompt"]
    assert "ДАННЫЕ" in built["prompt"]
    assert '"optimizations"' in built["prompt"]
    assert pjs.guard_problems(built, max_prompt_chars=10**6) == []


def test_json_stage_guard_catches_oversized_prompt():
    from backend.app.services.llm import provider_json_stage as pjs

    built = pjs.build_provider_prompt(
        [{"role": "system", "content": "x"}, {"role": "user", "content": "y" * 100}],
        root_key="optimizations",
    )
    problems = pjs.guard_problems(built, max_prompt_chars=10)
    assert any("потолка" in p for p in problems)


# ─── 7. Инлайн блочного контекста в text_analysis ────────────────────────────

def test_text_analysis_prompt_inlines_blocks_context():
    """Гейт 11D отказывал при наличии 01_blocks_for_text.json; 11F его вкладывает."""
    from backend.app.pipeline.stages.text_analysis import provider_transport as pt

    messages = [
        {"role": "system", "content": "## Input Data\nинструкции"},
        {"role": "user", "content": "ДОКУМЕНТ"},
    ]
    without = pt.build_provider_prompt(messages)
    with_blocks = pt.build_provider_prompt(messages, blocks_context='{"blocks": [1]}')
    assert without["map"]["blocks_context_applied"] is False
    assert with_blocks["map"]["blocks_context_applied"] is True
    assert '{"blocks": [1]}' in with_blocks["prompt"]
    assert "BLOCK ANALYSIS" in with_blocks["prompt"]
    # Без контекста промпт обязан остаться прежним побайтово: иначе прогоны
    # 11D/11E перестали бы быть сравнимыми.
    assert without["prompt"] == pt.build_provider_prompt(messages, blocks_context="")["prompt"]


# ─── 8. Потолок вызовов попытки ──────────────────────────────────────────────

def test_max_inferences_ceiling_allows_full_worker_slice():
    """Прежний потолок 8 отвергал бы задание полного участка ещё до старта."""
    from audit_worker.providers import resolver

    assert resolver.MAX_INFERENCES_CEILING >= 16
    payload = {
        "provider": "claude",
        "capability": "strong_audit",
        "allowed_stages": ["block_analysis"],
        "max_inferences": 16,
    }
    parsed = resolver.ProviderRequirement.from_payload(payload)
    assert parsed.max_inferences == 16
    with pytest.raises(resolver.ProviderResolutionError):
        resolver.ProviderRequirement.from_payload({**payload, "max_inferences": 10_000})


# ─── 9. Молчаливая потеря графики запрещена ──────────────────────────────────

@pytest.mark.asyncio
async def test_run_cli_refuses_images_under_bridge(monkeypatch):
    """Общий путь моста не имеет права уронить приложенные изображения."""
    from backend.app.services.llm import claude_runner

    class _Bridge:
        @staticmethod
        def active() -> bool:
            return True

        @staticmethod
        def route_cli_call(**kwargs):        # pragma: no cover — вызова быть не должно
            raise AssertionError("модель звали, хотя изображения теряются")

    monkeypatch.setattr(claude_runner, "_provider_bridge", lambda: _Bridge)
    exit_code, text, result = await claude_runner._run_cli(
        "задание", "", 60, stage="optimization", image_paths=["/tmp/a.png"],
    )
    assert exit_code == 1
    assert result.is_error
    assert "изображени" in text


# ─── 10. Страж отсутствия не ходит мимо моста ────────────────────────────────

def test_absence_guard_uses_bridge_when_active(monkeypatch):
    """При активном мосте прямой subprocess запрещён — даже ценой пустых вердиктов."""
    from backend.app.pipeline.stages.text_analysis import absence_guard

    calls: list[str] = []

    class _Outcome:
        ok = True
        performed = True

        class provider_result:                       # noqa: N801
            result = {"0": "present"}

    class _Bridge:
        @staticmethod
        def active() -> bool:
            return True

        @staticmethod
        def attempt_dir():
            return Path("/tmp")

        @staticmethod
        def run_stage_inference(**kwargs):
            calls.append(kwargs["stage"])
            return _Outcome()

    class _Err(RuntimeError):
        pass

    module = type(sys)("audit_worker.providers.pipeline_bridge")
    module.active = _Bridge.active
    module.attempt_dir = _Bridge.attempt_dir
    module.run_stage_inference = _Bridge.run_stage_inference
    module.ProviderBridgeError = _Err
    pkg = type(sys)("audit_worker.providers")
    pkg.pipeline_bridge = module
    monkeypatch.setitem(sys.modules, "audit_worker.providers", pkg)
    monkeypatch.setitem(sys.modules, "audit_worker.providers.pipeline_bridge", module)

    def _boom(*args, **kwargs):                      # pragma: no cover
        raise AssertionError("прямой subprocess при активном мосте")

    monkeypatch.setattr("subprocess.run", _boom)

    verdicts = absence_guard.run_claude_verification(
        "MD", [{"finding": "нет чего-то"}], timeout_sec=5,
    )
    assert calls == ["findings_review"]
    assert isinstance(verdicts, dict)


# ─── 11. Дефекты, найденные состязательным ревью до боевого прогона ──────────

def test_optimization_root_keys_match_real_schemas():
    """root_key обязан совпадать со СХЕМОЙ боевого шаблона, а не звучать похоже.

    Первая реализация объявила всем трём этапам ключ «optimizations». Реальные
    схемы другие: optimization_task.md → "items", optimization_critic_task.md →
    "reviews", optimization_corrector_task.md → "items". Fake-прогон был зелен
    ровно потому, что подделка отвечала тем же неверным ключом; в бою модель
    следовала бы схеме, и этап отверг бы её ответ — уже оплаченный.
    """
    src = (ROOT / "backend/app/services/llm/claude_runner.py").read_text(encoding="utf-8")
    assert 'artifact_name="optimization.json", root_key="items"' in src
    assert 'artifact_name="optimization_review.json", root_key="reviews"' in src
    # Схемы шаблонов — источник истины, сверяем их существование.
    assert '"items"' in (ROOT / "prompts/pipeline/en/optimization_task.md").read_text(encoding="utf-8")
    assert '"reviews"' in (ROOT / "prompts/pipeline/en/optimization_critic_task.md").read_text(encoding="utf-8")


def test_json_stage_extracts_images_instead_of_dropping_them():
    """Картинки листов-планов обязаны доехать до модели, а не исчезнуть молча."""
    import base64

    from backend.app.services.llm import provider_json_stage as pjs

    messages = [
        {"role": "system", "content": "инструкции"},
        {"role": "user", "content": [
            {"type": "text", "text": "ДАННЫЕ"},
            {"type": "image_url", "image_url": {
                "url": "data:image/png;base64," + base64.b64encode(b"PNG-A").decode()}},
            {"type": "image_url", "image_url": {
                "url": "data:image/png;base64," + base64.b64encode(b"PNG-B").decode()}},
        ]},
    ]
    built = pjs.build_provider_prompt(messages, root_key="items")
    assert [blob for _mt, blob in built["images"]] == [b"PNG-A", b"PNG-B"]
    assert built["map"]["images_attached"] == 2


def test_json_stage_refuses_remote_image_reference():
    """Ссылка вместо данных — отказ: провайдерский путь в сеть за промптом не ходит."""
    from backend.app.services.llm import provider_json_stage as pjs

    messages = [{"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
    ]}]
    with pytest.raises(pjs.ProviderStageRefusal):
        pjs.build_provider_prompt(messages, root_key="items")


def test_json_stage_placeholder_does_not_claim_inputs_unavailable():
    """Нельзя говорить модели «недоступно» про то, что вложено ниже.

    text_analysis подставляет «(not available in this run)» — и там это правда.
    Здесь артефакт вложен в то же сообщение, и та же подстановка заставляла бы
    критика оптимизации ставить `no_traceability` по собственному критерию.
    """
    from backend.app.services.llm import provider_json_stage as pjs

    built = pjs.build_provider_prompt(
        [{"role": "system", "content": "Input: /srv/x/_output/03_findings.json"},
         {"role": "user", "content": "ДАННЫЕ"}],
        root_key="items",
    )
    assert "not available in this run" not in built["prompt"]
    assert pjs.INLINED_PLACEHOLDER in built["prompt"]


def test_worker_provider_model_is_not_reported_as_openrouter():
    """Провенанс замечания не должен называть платного провайдера, которого не было."""
    from backend.app.pipeline.stages.block_analysis.provenance import detector_for_model
    from backend.app.pipeline.stages.block_analysis.provider_transport import (
        PROVIDER_BLOCK_MODEL_ID,
    )

    assert detector_for_model(PROVIDER_BLOCK_MODEL_ID) == "worker_provider"
    assert detector_for_model("openai/gpt-5.4") == "gpt_openrouter"
    assert detector_for_model("claude-opus-5") == "claude"


def test_block_prompt_lists_every_required_finding_field():
    """Без json_schema обязательные поля перечисляются в промпте — иначе гейт их отбросит."""
    from backend.app.pipeline.stages.block_analysis import provider_transport as pt
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import RESPONSE_SCHEMA

    contract = pt.response_contract()
    required = RESPONSE_SCHEMA["schema"]["properties"]["findings"]["items"]["required"]
    for name in required:
        assert name in contract, f"поле {name} не названо модели"


def test_worker_acceptance_gate_uses_same_ceiling_as_resolver():
    """Три валидатора одного поля обязаны иметь ОДИН потолок."""
    from audit_worker import audit_runner
    from audit_worker.providers import resolver
    from backend.app.models.distributed_workers import ProviderRequirementPayload

    params = audit_runner._validate_provider_requirement({
        "provider": "claude", "capability": "strong_audit",
        "allowed_stages": ["block_analysis"], "max_inferences": 16,
    })
    assert params["max_inferences"] == 16
    payload = ProviderRequirementPayload(
        provider="claude", allowed_stages=["block_analysis"], max_inferences=16,
    )
    assert payload.max_inferences == 16
    assert resolver.MAX_INFERENCES_CEILING == 64


def test_broken_stream_output_is_not_reported_as_success():
    """Оборванный поток — ошибка, а не служебное событие CLI, выданное за ответ."""
    from audit_worker.providers.claude_adapter import parse_stream_json

    # В потоке есть только init-событие: итогового `result` нет.
    stdout = '{"type":"system","subtype":"init","session_id":"abc","tools":["Read"]}\n'
    assert parse_stream_json(stdout) is None


# ─── 12. Дефект, найденный БОЕВЫМ прогоном ───────────────────────────────────

def test_provider_block_timeout_is_not_the_openrouter_transport_limit():
    """Срок блочного вызова через провайдера — не 200 с ноги OpenRouter.

    Боевой прогон 11F убил вызов на 200-й секунде сигналом 143, когда модель
    уже отдала 74 090 байт потокового ответа: `DEFAULT_TIMEOUT_S = 200` — это
    лимит ожидания HTTP-ответа GPT-5.4, а провайдерский путь ходит в локальный
    CLI к Opus. Оплата состоялась, результат выброшен, этап упал.

    Проверяется по коду, а не по прогону: срок обязан браться от срока ТОГО ЖЕ
    этапа для CLI-провайдера, и внешний backstop обязан быть шире внутреннего.
    """
    from backend.app.core.config import CLAUDE_BLOCK_ANALYSIS_TIMEOUT
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        BLOCK_HARD_TIMEOUT_BUFFER_S,
        DEFAULT_TIMEOUT_S,
    )

    assert CLAUDE_BLOCK_ANALYSIS_TIMEOUT > DEFAULT_TIMEOUT_S, (
        "срок CLI-этапа обязан быть больше транспортного лимита HTTP-ноги"
    )
    src = (
        ROOT / "backend/app/pipeline/stages/block_analysis/gemma_findings_only.py"
    ).read_text(encoding="utf-8")
    assert "effective_timeout = max(float(timeout), float(CLAUDE_BLOCK_ANALYSIS_TIMEOUT))" in src
    assert "timeout_sec=effective_timeout," in src
    # Backstop шире внутреннего срока — иначе он убьёт работающий вызов.
    assert "int(_cbt) + BLOCK_HARD_TIMEOUT_BUFFER_S" in src
    assert BLOCK_HARD_TIMEOUT_BUFFER_S > 0
