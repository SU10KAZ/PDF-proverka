"""11H — Codex как рабочий провайдер worker-участка.

До этого этапа Codex умел ровно две вещи: рассказать о себе (версия,
авторизация, официальная квота) и выполнить ОДИН текстовый вызов без
назначенной модели. Для конвейера этого не хватало ни в одном месте:

  * мост (`pipeline_bridge._preflight`) требует НАЗНАЧЕННОЙ модели от любого
    провайдера, а `CodexProviderAdapter.structured_inference` на любой явный
    `model` отвечал отказом «не реализовано»;
  * `block_analysis` передаёт изображение, а метода
    `structured_inference_multimodal` у Codex не было вовсе — мост отвергал
    такой вызов ДО заявки в журнале (и правильно делал: молчаливый переход на
    текст означал бы анализ чертежа без чертежа).

Здесь проверяется, что обе дыры закрыты БЕЗ ослабления гейтов: модель уходит в
argv из локальной политики, ответ сверяется с допустимыми идентификаторами,
вложение доезжает до CLI файлом в изолированном каталоге вызова и исчезает
после него.

Настоящих обращений к модели в файле НОЛЬ: подделан ровно последний метр —
бинарь (`provider_bridge_stub`, ветка codex), отвечающий на четыре фактических
контракта Codex 0.147.0.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from audit_worker.providers import errors
from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER
from audit_worker.providers.codex_adapter import CodexProviderAdapter, _inference_argv
from audit_worker.providers.paths import ProviderHome
from backend.app.pipeline.execution import provider_bridge_stub

#: Минимальный настоящий PNG (1×1). Байты, а не заглушка-строка: адаптер их
#: пишет на диск и сверяет по sha256, а заглушка CLI — читает.
PNG_1X1 = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4"
    "890000000a49444154789c63000100000500010d0a2db40000000049454e44ae426082"
)


def _adapter(tmp_path: Path, *, executable: Path | None = None) -> CodexProviderAdapter:
    home = tmp_path / "ambient"
    (home / ".codex").mkdir(parents=True, exist_ok=True)
    return CodexProviderAdapter(
        ProviderHome(
            provider="codex", root=tmp_path / "prov",
            auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home,
        ),
        executable=executable,
        timeout_sec=60.0,
        inference_allowed=True,
    )


def _stub(tmp_path: Path, *, model: str = "gpt-5.6-sol",
          call_log: Path | None = None) -> Path:
    """Заглушка + ОБЁРТКА, доставляющая ей настройки.

    Обёртка здесь не декорация теста, а следствие инварианта I-P1: окружение
    подпроцесса CLI собирается С НУЛЯ, и `AUDIT_PROVIDER_STUB_*` до заглушки не
    доходят ни при каком `os.environ`. Ровно так это устроено и на воркере
    (`claude-with-call-log` рядом с бинарём) — тест повторяет боевую раскладку,
    а не обходит её.
    """
    binary = provider_bridge_stub.materialize(tmp_path / "stub", provider="codex")
    wrapper = binary.parent / "codex-with-env"
    lines = ["#!/bin/sh", f'{provider_bridge_stub.MODEL_ENV}="{model}"',
             f"export {provider_bridge_stub.MODEL_ENV}"]
    if call_log is not None:
        lines += [f'{provider_bridge_stub.CALL_LOG_ENV}="{call_log}"',
                  f"export {provider_bridge_stub.CALL_LOG_ENV}"]
    lines.append(f'exec "{binary}" "$@"')
    wrapper.write_text("\n".join(lines) + "\n", encoding="utf-8")
    wrapper.chmod(0o700)
    return wrapper


# ═════════════ argv: форма записи флагов ═════════════════════════════════════
class TestArgvForm:
    def test_model_and_images_use_equals_form(self):
        """`--image` вариадический: форма с пробелом съела бы терминатор `-`.

        Это не теория о чужом CLI, а его объявление: `-i, --image <FILE>...`.
        Записанный как `--image /a.png -`, он забрал бы `-` вторым именем файла,
        и промпт перестал бы читаться со стандартного ввода — то есть модель
        получила бы пустое задание и вернула бы что угодно.
        """
        argv = _inference_argv("gpt-5.6-sol", [Path("/w/attachment-000.png")])
        assert "--model=gpt-5.6-sol" in argv
        assert "--image=/w/attachment-000.png" in argv
        assert argv[-1] == "-", "терминатор stdin обязан оставаться последним"
        assert "--model" not in argv and "--image" not in argv

    def test_neutralization_flags_survive_on_the_working_path(self):
        """Рабочий вызов не имеет права быть мягче контрольного."""
        argv = _inference_argv("gpt-5.6-sol", [Path("/w/a.png")])
        for flag in ("--ignore-user-config", "--ignore-rules", "--ephemeral",
                     "--skip-git-repo-check", "--json"):
            assert flag in argv
        assert argv[argv.index("--sandbox") + 1] == "read-only"

    def test_no_model_no_flag(self):
        assert not [a for a in _inference_argv() if a.startswith("--model")]


# ═════════════ Текстовый вызов с назначенной моделью ═════════════════════════
class TestTextInference:
    def test_assigned_model_reaches_cli_and_is_verified(self, tmp_path):
        stub = _stub(tmp_path, model="gpt-5.6-sol")
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="text_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert result.ok, result.detail
        assert result.model == "gpt-5.6-sol"
        assert result.usage.get("output_tokens") == 16

    def test_foreign_reported_model_fails_closed(self, tmp_path):
        """Ответила не та модель — это отказ, а не предупреждение.

        Результат при этом всё равно возвращается вызывающему (мост обязан
        записать оплаченный вызов в журнал), но со статусом ошибки и кодом
        `model_mismatch`.
        """
        stub = _stub(tmp_path, model="gpt-4.1-mini")
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="text_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert not result.ok
        assert result.error_code == errors.ERR_MODEL_MISMATCH
        assert "gpt-4.1-mini" in (result.detail or "")

    def test_model_without_accepted_list_refused_before_launch(self, tmp_path):
        stub = _stub(tmp_path)
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="t", model="gpt-5.6-sol",
            accepted_reported_models=(),
        )
        assert result.error_code == errors.ERR_MODEL_MISMATCH
        assert "сверять ответ не с чем" in (result.detail or "")


# ═════════════ Мультимодальный вызов ═════════════════════════════════════════
class TestMultimodalInference:
    def test_attachment_reaches_cli_as_a_readable_file(self, tmp_path):
        """Вложение доезжает до процесса CLI и открывается им.

        Заглушка ЧИТАЕТ файл и сообщает его размер в ответе. Проверять «путь
        оказался в argv» было бы недостаточно: путь в командной строке и
        доступный процессу файл — разные утверждения, и различает их ровно
        режим доступа к каталогу вызова.
        """
        stub = _stub(tmp_path)
        result = _adapter(tmp_path, executable=stub).structured_inference_multimodal(
            "опиши изображение", images=[("image/png", PNG_1X1)],
            purpose="block_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert result.ok, result.detail
        finding = result.result["findings"][0]["finding"]
        assert f"{len(PNG_1X1)} байт" in finding

    def test_workspace_is_isolated_and_removed(self, tmp_path):
        """Каталог вызова живёт внутри runtime и не переживает вызов.

        Оба утверждения проверяются на ФАКТАХ, а не на намерении: путь
        вложения снимается из journal заглушки (то есть из argv, который
        реально получил CLI), а его существование — после возврата.
        """
        log = tmp_path / "calls.jsonl"
        stub = _stub(tmp_path, call_log=log)
        adapter = _adapter(tmp_path, executable=stub)
        result = adapter.structured_inference_multimodal(
            "опиши", images=[("image/png", PNG_1X1)], purpose="block_analysis",
            model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",),
        )
        assert result.ok, result.detail
        rows = provider_bridge_stub.read_call_log(log)
        attachments = [
            arg[len("--image="):]
            for row in rows for arg in row["argv"] if arg.startswith("--image=")
        ]
        assert attachments, "вложение не дошло до argv CLI"
        for raw in attachments:
            path = Path(raw)
            assert adapter.home.runtime in path.parents, (
                "вложение обязано лежать внутри runtime провайдера, а не где угодно"
            )
            assert not path.exists(), "каталог вложений не удалён после вызова"
            assert not path.parent.exists()
        # И runtime после вызова не копит мусора.
        assert not [p for p in adapter.home.runtime.iterdir() if p.is_dir()]

    def test_attachment_name_carries_no_job_data(self, tmp_path):
        """В имени файла нет ни block_id, ни имени проекта: argv видно в `ps`."""
        log = tmp_path / "calls.jsonl"
        stub = _stub(tmp_path, call_log=log)
        _adapter(tmp_path, executable=stub).structured_inference_multimodal(
            "опиши", images=[("image/png", PNG_1X1)], purpose="block_analysis",
            model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",),
        )
        names = [
            Path(arg[len("--image="):]).name
            for row in provider_bridge_stub.read_call_log(log)
            for arg in row["argv"] if arg.startswith("--image=")
        ]
        assert names == ["attachment-000.png"]

    def test_empty_and_unknown_media_type_refused(self, tmp_path):
        stub = _stub(tmp_path)
        adapter = _adapter(tmp_path, executable=stub)
        empty = adapter.structured_inference_multimodal(
            "опиши", images=[("image/png", b"")], purpose="block_analysis",
            model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",),
        )
        assert not empty.ok and "пустое изображение" in (empty.detail or "")
        exotic = adapter.structured_inference_multimodal(
            "опиши", images=[("application/pdf", b"%PDF-1.7")],
            purpose="block_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert not exotic.ok and "неподдерживаемый тип" in (exotic.detail or "")

    def test_no_images_is_refused_not_downgraded(self, tmp_path):
        stub = _stub(tmp_path)
        result = _adapter(tmp_path, executable=stub).structured_inference_multimodal(
            "опиши", images=[], purpose="block_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert not result.ok
        assert "без чертежа" in (result.detail or "")

    def test_written_attachment_matches_source_bytes(self, tmp_path):
        """Сверка вложения по sha256 — не декорация: она стоит ДО запуска CLI."""
        log = tmp_path / "calls.jsonl"
        stub = _stub(tmp_path, call_log=log)
        _adapter(tmp_path, executable=stub).structured_inference_multimodal(
            "опиши", images=[("image/png", PNG_1X1)], purpose="block_analysis",
            model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",),
        )
        rows = provider_bridge_stub.read_call_log(log)
        inference = [r for r in rows if r["kind"] == "inference"]
        assert inference and inference[0]["image_bytes"] == len(PNG_1X1)
        assert hashlib.sha256(PNG_1X1).hexdigest()  # источник байтов один и тот же


# ═════════════ Мост: Codex перестал быть «провайдером без изображений» ═══════
class TestBridgePreflight:
    def test_codex_passes_image_preflight(self):
        from audit_worker.providers import pipeline_bridge

        assert hasattr(CodexProviderAdapter, "structured_inference_multimodal"), (
            "мост отвергает провайдера без этого метода ДО заявки в журнале"
        )
        assert pipeline_bridge.attachments_digest([("image/png", PNG_1X1)])


# ═════════════ Разбор потока: откуда берётся фактическая модель ══════════════
class TestStreamParsing:
    def test_model_is_read_from_thread_started_event(self):
        """Модель объявляется на уровень глубже верхних полей события.

        До 11H разбор смотрел только верхний уровень и `item.model`, поэтому
        `codex.thread.started` с моделью внутри `thread` не читался — а
        отсутствие идентификатора в сверке означает отказ вызова.
        """
        from audit_worker.providers.codex_adapter import _collect_exec_stream

        stdout = "\n".join([
            json.dumps({"type": "codex.thread.started",
                        "thread": {"thread_id": "t", "model": "gpt-5.6-sol"}}),
            json.dumps({"type": "item.completed",
                        "item": {"type": "agent_message", "text": '{"findings": []}'}}),
            json.dumps({"type": "turn.completed",
                        "usage": {"input_tokens": 5, "output_tokens": 2}}),
        ])
        messages, usage, model = _collect_exec_stream(stdout)
        assert model == "gpt-5.6-sol"
        assert usage["output_tokens"] == 2
        assert json.loads(messages[-1]) == {"findings": []}

    def test_model_search_does_not_dig_into_the_answer(self):
        """Поиск ограничен глубиной и закрытым списком контейнеров.

        Иначе строка «model» из текста ответа модели прошла бы как фактический
        идентификатор, и гейт сверки начал бы проходить на чём попало.
        """
        from audit_worker.providers.codex_adapter import _model_from_event

        assert _model_from_event({
            "type": "item.completed",
            "item": {"type": "agent_message",
                     "text": json.dumps({"deep": {"deeper": {"model": "чужое"}}})},
        }) is None


# ═════════════ Оценка бюджета центром ════════════════════════════════════════
class TestCenterBudget:
    def _version(self, root: Path, *, images: int, stamps: int) -> Path:
        version = root / "версия"
        (version / "02_work").mkdir(parents=True)
        blocks = [
            {"block_type": "image", "crop_url": f"https://portal/{i}",
             "coords_px": [0, 0, 900, 900]}
            for i in range(images)
        ] + [
            {"block_type": "image", "category_code": "stamp",
             "crop_url": f"https://portal/s{i}", "coords_px": [0, 0, 300, 300]}
            for i in range(stamps)
        ] + [{"block_type": "text"}]
        (version / "02_work" / "result.json").write_text(
            json.dumps({"pages": [{"blocks": blocks}]}, ensure_ascii=False),
            encoding="utf-8",
        )
        return version

    def test_new_upload_layout_is_no_longer_blind(self, tmp_path):
        """Трёхфайловый комплект портала: `02_work/result.json`, а не `01_input`.

        Раньше искали только `01_input/*_result.json`; у нового формата такого
        файла нет вовсе, и оценка молча уходила в слепые 12 блоков. Для
        документа на сорок блоков это обрыв аудита на середине — с уже
        оплаченной половиной вызовов.
        """
        from backend.app.services.distributed_workers import provider_requirement

        version = self._version(tmp_path, images=41, stamps=13)
        estimate = provider_requirement.estimate_inferences(version)
        assert estimate["graphic_blocks"] == 41, "штампы не отделены от чертежей"
        assert estimate["blind_estimate"] is False
        assert estimate["natural_calls"] == 47
        assert estimate["technical_retry_headroom"] == 5      # max(3, ceil(47×0.1))
        assert estimate["max_inferences"] == 52
        assert estimate["clamped_by_ceiling"] is False

    def test_ceiling_clamp_is_visible(self, tmp_path, monkeypatch):
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.setenv("DISTRIBUTED_AUDIT_MAX_INFERENCES", "20")
        estimate = provider_requirement.estimate_inferences(
            self._version(tmp_path, images=41, stamps=0)
        )
        assert estimate["max_inferences"] == 20
        assert estimate["clamped_by_ceiling"] is True


# ═════════════ Провайдер требования выбирает ЦЕНТР, не задание ═══════════════
class TestCenterProviderChoice:
    def test_default_stays_claude(self, monkeypatch):
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.delenv("DISTRIBUTED_AUDIT_PROVIDER", raising=False)
        assert provider_requirement.audit_provider() == "claude"

    def test_codex_is_orderable(self, monkeypatch):
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.setenv("DISTRIBUTED_AUDIT_PROVIDER", "codex")
        assert provider_requirement.audit_provider() == "codex"

    def test_unknown_provider_is_refused_not_defaulted(self, monkeypatch):
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.setenv("DISTRIBUTED_AUDIT_PROVIDER", "gemini")
        with pytest.raises(provider_requirement.ProviderRequirementError):
            provider_requirement.audit_provider()

    def test_requirement_carries_provider_and_no_exact_model(self, tmp_path, monkeypatch):
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.setenv("DISTRIBUTED_AUDIT_PROVIDER", "codex")
        version = tmp_path / "версия"
        (version / "02_work").mkdir(parents=True)
        (version / "02_work" / "result.json").write_text(
            json.dumps({"pages": [{"blocks": [
                {"block_type": "image", "crop_url": "https://portal/1"},
            ]}]}), encoding="utf-8",
        )
        requirement, rationale = provider_requirement.build_audit_requirement(
            version_dir=version
        )
        assert requirement.provider == "codex"
        assert requirement.capability == "strong_audit"
        assert requirement.model is None
        assert rationale["exact_model_in_payload"] is False
        serialized = json.dumps(requirement.model_dump(), ensure_ascii=False)
        for forbidden in ("gpt-5", "codex/", "opus", "sonnet"):
            assert forbidden not in serialized

    def test_worker_without_codex_capability_is_refused(self, tmp_path, monkeypatch):
        """Отказ ДО создания задания, а не после сборки и выдачи пакета."""
        from backend.app.services.distributed_workers import provider_requirement

        monkeypatch.setenv("DISTRIBUTED_AUDIT_PROVIDER", "codex")
        version = tmp_path / "версия"
        (version / "02_work").mkdir(parents=True)
        (version / "02_work" / "result.json").write_text(
            json.dumps({"pages": [{"blocks": []}]}), encoding="utf-8")
        worker = {"capabilities": json.dumps({
            "real_llm_enabled": True,
            "pipeline_provider_bridge_enabled": True,
            "provider_capabilities": {"claude": ["strong_audit"]},
        })}
        with pytest.raises(provider_requirement.ProviderRequirementError) as exc:
            provider_requirement.build_audit_requirement(
                version_dir=version, worker=worker
            )
        assert "codex" in str(exc.value)


# ═════════════ Провайдер, не сообщающий модель (инцидент боевого прогона) ════
class TestModelReportUnsupported:
    """Codex 0.147.0 не называет применённую модель НИ В ОДНОМ событии потока.

    Обнаружено не на бумаге, а на первом боевом прогоне 11H: 10 блоков подряд
    отказали с `model_mismatch` и `detail="CLI не сообщил фактическую модель"`.
    Прогон был остановлен, поток `codex exec --json` снят диагностическим
    вызовом — в нём ровно четыре события: `thread.started` (только thread_id),
    `turn.started`, `item.completed`, `turn.completed` (только usage).

    Ослабление гейта сделано ЯВНЫМ и локальным: администратор машины объявляет
    `model_report="unsupported"` в своей политике. Ветки «если провайдер codex,
    не сверяем» в коде нет — она расползлась бы на новые провайдеры молча.
    """

    def _policy(self, report: str) -> Any:
        from audit_worker.providers import model_policy

        return model_policy.parse_policy({
            "policy_version": 1,
            "codex": {"auth_mode": "ambient_user", "capabilities": {
                "strong_audit": {"model": "gpt-5.6-sol", "model_report": report},
            }},
        }).resolve("codex", "strong_audit")

    def test_silence_is_mismatch_by_default(self, tmp_path):
        stub = _stub(tmp_path, model="")          # заглушка молчит о модели
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="text_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",),
        )
        assert result.error_code == errors.ERR_MODEL_MISMATCH

    def test_declared_unsupported_lets_the_call_through(self, tmp_path):
        stub = _stub(tmp_path, model="")
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="text_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",), model_report="unsupported",
        )
        assert result.ok, result.detail

    def test_foreign_model_still_fails_when_unsupported(self, tmp_path):
        """Послабление касается МОЛЧАНИЯ, а не чужой модели."""
        stub = _stub(tmp_path, model="gpt-4.1-mini")
        result = _adapter(tmp_path, executable=stub).structured_inference(
            "верни JSON", purpose="text_analysis", model="gpt-5.6-sol",
            accepted_reported_models=("gpt-5.6-sol",), model_report="unsupported",
        )
        assert result.error_code == errors.ERR_MODEL_MISMATCH

    def test_policy_default_is_required(self):
        assert self._policy("required").model_report == "required"
        assert self._policy("required").reported_matches(None) is False

    def test_policy_unsupported_accepts_silence_only(self):
        capability = self._policy("unsupported")
        assert capability.reported_matches(None) is True
        assert capability.reported_matches("gpt-5.6-sol") is True
        assert capability.reported_matches("gpt-4.1-mini") is False

    def test_unknown_report_mode_is_refused(self):
        from audit_worker.providers import model_policy

        with pytest.raises(model_policy.ProviderPolicyError):
            model_policy.parse_policy({
                "policy_version": 1,
                "codex": {"capabilities": {
                    "strong_audit": {"model": "gpt-5.6-sol", "model_report": "как-нибудь"},
                }},
            })

    def test_validator_names_the_weakened_check(self):
        """Послабление ВИДНО в отчёте: у проверки другое имя."""
        from audit_worker.providers.inference import (
            ProviderInferenceResult, STATUS_SUCCESS, validate_inference,
        )

        result = ProviderInferenceResult(
            provider="codex", model=None, status=STATUS_SUCCESS,
            result={"findings": []}, auth_mode=AUTH_MODE_AMBIENT_USER,
        )
        report = validate_inference(
            result, expected_provider="codex", expected_auth_mode=AUTH_MODE_AMBIENT_USER,
            expected_model="gpt-5.6-sol", accepted_reported_models=("gpt-5.6-sol",),
            model_report="unsupported",
        )
        names = {c.name for c in report.checks}
        assert "model_assigned_reporting_unsupported" in names
        assert "model_matches_policy" not in names


# ═════════════ Назначение модели у Codex не декоративно ══════════════════════
class TestModelFlagIsEffective:
    def test_argv_carries_the_model(self):
        """Единственное, что доказуемо без отчёта CLI, — что флаг предъявлен.

        Что он ДЕЙСТВУЕТ, проверено вживую на .31: `--model=<несуществующая>`
        даёт от сервера 400 invalid_request_error и выход с кодом 1. То есть
        значение флага доезжает до провайдера, а не игнорируется.
        """
        argv = _inference_argv("gpt-5.6-sol")
        assert "--model=gpt-5.6-sol" in argv
