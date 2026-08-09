"""Этап 11D — боевой этап `text_analysis` через ProviderAdapter.

Что здесь защищается и почему именно это.

11C довёл до модели СИНТЕТИЧЕСКИЙ этап (`provider_selfcheck`), чей промпт с
самого начала писался под «ответь JSON в stdout». Боевой `text_analysis` устроен
иначе: его промпт годами рассчитывал на то, что модель сама прочитает MD через
Read и сама запишет результат через Write. Отправить такой промпт в
`ProviderAdapter`, где инструментов ноль, — значит получить сводку о невыполнимой
задаче вместо аудита.

Поэтому проверяются не «работает ли вызов», а четыре класса утверждений:

  1. РАСПРЕДЕЛЕНИЕ ОБЯЗАННОСТЕЙ: MD читает конвейер, файл пишет конвейер,
     модель только рассуждает. У модели нет ни инструментов, ни пути проекта.
  2. СОХРАНЕНИЕ ИНЖЕНЕРНОГО СОДЕРЖАНИЯ: смена транспорта не имеет права
     потерять дисциплину, нормы, стража отсутствия, схему и правила severity.
  3. МОДЕЛЬ: назначает её ЛОКАЛЬНАЯ политика воркера, она предъявляется CLI
     явно, а расхождение с фактической — отказ, а не запись в лог.
  4. НЕИЗМЕННОСТЬ ПРЕЖНЕГО ПУТИ: без привязки провайдера код платформы ведёт
     себя ровно как до 11D, и молчаливого отката из provider-режима нет.

НИ ОДИН тест этого файла не обращается к настоящей модели: везде подставной
исполняемый файл. Бюджет реальных вызовов этапа — один, и тратить его на
регрессии нельзя.

Прогон:
    python -m pytest tests/test_distributed_workers_text_analysis_provider.py -v
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")

from audit_worker import audit_runner                                  # noqa: E402
from audit_worker.providers import (                                   # noqa: E402
    errors,
    inference,
    inference_ledger,
    model_policy,
    pipeline_bridge,
    resolver,
)
from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER    # noqa: E402
from audit_worker.providers.claude_adapter import (                    # noqa: E402
    ClaudeProviderAdapter,
    _inference_argv,
)
from audit_worker.providers.codex_adapter import CodexProviderAdapter  # noqa: E402
from audit_worker.providers.paths import ProviderHome                  # noqa: E402

from backend.app.pipeline.stages.text_analysis import provider_transport  # noqa: E402


# ═════════════════════════ Подставной CLI и фикстуры ═════════════════════════

#: Ответ «модели» — форма боевого артефакта `02_text_analysis.json`.
_ANSWER: dict = {
    "stage": "02_text_analysis",
    "project_id": "VK/11d-test",
    "text_source": "md",
    "timestamp": "2026-08-09T00:00:00",
    "project_params": {"object_type": "тест", "key_equipment": ["P-1"]},
    "normative_refs_found": [
        {"ref": "СП 30.13330.2020", "status": "ДЕЙСТВУЕТ", "edition": "", "note": ""}
    ],
    "text_findings": [
        {
            "id": "T-001",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "расчёт",
            "source": "MD стр. 1",
            "finding": "Расход насоса P-1 в тексте 10 м3/ч, в таблице 12 м3/ч",
            "norm": "СП 30.13330.2020, п. 5.1.2",
            "norm_quote": None,
            "related_block_ids": [],
        }
    ],
    "items_verified_from_blocks": [],
}

#: Модель, назначаемая локальной политикой в тестах. Совпадает с той, что
#: назначена на пилотном воркере, — иначе тесты защищали бы другую настройку.
_POLICY_MODEL = "claude-opus-5"
_REPORTED_MODEL = "claude-opus-5[1m]"


def _write_exe(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _fake_claude(
    path: Path,
    journal: Path,
    *,
    answer: object = None,
    reported_model: str = _REPORTED_MODEL,
    exit_code: int = 0,
    stall: float = 0.0,
    stderr_text: str = "",
) -> Path:
    """Подделка `claude`, ведущая журнал argv/stdin/env.

    Журнал — единственный канал, которого не касается редактор секретов, и
    потому единственный, на котором утверждения про argv и stdin осмысленны.
    """
    body = _ANSWER if answer is None else answer
    payload = json.dumps(json.dumps(body, ensure_ascii=False)
                         if not isinstance(body, str) else body)
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
  echo "CWD:$(pwd)"
  echo "CWD_ENTRIES:$(ls -A . | tr '\\n' ',')"
  tr "\\0" "\\n" < /proc/$$/environ | sed 's/^/ENV:/'
}} >> "$JOURNAL"
sleep {stall}
if [ -n "{stderr_text}" ]; then echo "{stderr_text}" >&2; fi
python3 - <<'PYEOF'
import json
answer = {payload}
print(json.dumps({{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer,
    "usage": {{"input_tokens": 1200, "output_tokens": 400,
               "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}},
    "modelUsage": {{"{reported_model}": {{"inputTokens": 1200}}}},
    "total_cost_usd": 0.0,
    "num_turns": 1,
}}, ensure_ascii=False))
PYEOF
exit {exit_code}
""")


@pytest.fixture()
def job_dir(tmp_path: Path) -> Path:
    path = tmp_path / "jobs" / "job-11d" / "attempt-1"
    (path / "metadata").mkdir(parents=True)
    return path


@pytest.fixture()
def project(job_dir: Path) -> dict:
    """Синтетический проект ВНУТРИ каталога попытки — как на воркере."""
    vdir = job_dir / "project" / "vk" / "v1"
    out = vdir / "_output"
    out.mkdir(parents=True)
    md = vdir / "11d_results.md"
    md.write_text(
        "# Раздел ВК — тестовый лист\n\n"
        "Насос P-1 подобран на проектный расход 10 м3/ч, напор 25 м.\n\n"
        "## Спецификация оборудования\n\n"
        "| Поз. | Наименование | Расход |\n|---|---|---|\n"
        "| 1 | Насос P-1 | 12 м3/ч |\n",
        encoding="utf-8",
    )
    return {
        "project_dir": vdir,
        "output_dir": out,
        "md_path": md,
        "project_id": "VK/11d-test",
        "project_info": {
            "project_id": "VK/11d-test",
            "name": "11d-test",
            "section": "VK",
            "md_file": "11d_results.md",
        },
    }


def _policy_file(root: Path, *, model: str = _POLICY_MODEL,
                 accepted: list | None = None) -> Path:
    body: dict = {
        "policy_version": 1,
        "claude": {
            "auth_mode": AUTH_MODE_AMBIENT_USER,
            "capabilities": {"strong_audit": {"model": model}},
        },
    }
    if accepted is not None:
        body["claude"]["capabilities"]["strong_audit"]["accepted_reported_models"] = accepted
    root.mkdir(parents=True, exist_ok=True)
    path = root / model_policy.POLICY_FILENAME
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _binding(
    job_dir: Path,
    *,
    executable: Path,
    stages=("text_analysis",),
    max_inferences: int = 1,
    model: str | None = _POLICY_MODEL,
    accepted: tuple = (_POLICY_MODEL, _REPORTED_MODEL),
    literals=(),
) -> resolver.ProviderBinding:
    return resolver.ProviderBinding(
        schema_version=resolver.BINDING_SCHEMA_VERSION,
        provider="claude",
        auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(resolver.ambient_root_for_attempt(job_dir, "claude")),
        executable=str(executable),
        timeout_sec=60.0,
        job_id="job-11d",
        attempt_id="attempt-1",
        task_id="job-11d",
        grant_id="g-11d-0001",
        max_inferences=max_inferences,
        allowed_stages=tuple(stages),
        model=model,
        forbidden_literals=tuple(literals),
        capability="strong_audit" if model else None,
        accepted_reported_models=tuple(accepted) if model else (),
    )


def _activate(monkeypatch, binding: resolver.ProviderBinding, job_dir: Path) -> Path:
    path = binding.write(job_dir / "metadata")
    monkeypatch.setenv(resolver.BINDING_ENV, str(path))
    return path


def _ambient_home(monkeypatch, home: Path) -> None:
    """Ambient-HOME указывает в подконтрольный каталог, а не в личный."""
    home.mkdir(parents=True, exist_ok=True)
    from audit_worker.providers import auth_mode as auth_mode_mod

    monkeypatch.setattr(auth_mode_mod, "resolve_ambient_home", lambda: home)
    monkeypatch.setattr(auth_mode_mod, "ambient_user_name", lambda: "tester")


def _run_stage(project: dict, on_output=None):
    """Вызвать БОЕВОЙ раннер транспорта этапа и вернуть (код, текст, result)."""
    from backend.app.services.llm import claude_runner

    return asyncio.run(
        claude_runner.run_text_analysis(
            project["project_info"], project["project_id"], on_output,
            output_dir=project["output_dir"],
            version_dir=project["project_dir"],
            version_id="v1",
        )
    )


def _build_messages(project: dict) -> list[dict]:
    from backend.app.services.common import audit_scope
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

    with audit_scope.bind_audit_scope(
        output_dir=project["output_dir"], version_dir=project["project_dir"],
        project_id=project["project_id"], version_id="v1",
    ):
        return prompt_builder.build_text_analysis_messages(
            project["project_info"], project["project_id"]
        )


def _journal_text(journal: Path) -> str:
    return journal.read_text(encoding="utf-8") if journal.exists() else ""


def _argv_lines(journal: Path) -> list[str]:
    return [line[5:] for line in _journal_text(journal).splitlines()
            if line.startswith("ARGV:")]


def _stdin_blob(journal: Path) -> str:
    text = _journal_text(journal)
    marker = "STDIN:"
    start = text.find(marker)
    if start < 0:
        return ""
    end = text.find("\nCWD:", start)
    return text[start + len(marker): end if end > 0 else len(text)]


# ═════════ A/B. Маршрутизация: provider-режим и неизменность legacy ══════════
class TestStageRouting:

    def test_a_binding_routes_text_analysis_into_provider(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Активная привязка уводит боевой этап в ProviderAdapter."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, result = _run_stage(project)

        assert code == 0, _text
        assert (project["output_dir"] / "02_text_analysis.json").is_file()
        assert result.input_tokens == 1200
        # Промпт ушёл через stdin, а не аргументом — данных задания в argv нет.
        assert "Насос P-1" in _stdin_blob(journal)
        assert not any("Насос" in line for line in _argv_lines(journal))

    def test_b_legacy_path_untouched_without_binding(self, monkeypatch, project):
        """Без привязки — прежний код платформы, ни одной новой ветки."""
        from backend.app.services.llm import claude_runner

        monkeypatch.delenv(resolver.BINDING_ENV, raising=False)
        calls: list = []

        async def _fake_run_cli(task_text, tools, timeout, on_output=None, **kwargs):
            calls.append({"tools": tools, "stage": kwargs.get("stage"),
                          "task_text": task_text})
            from backend.app.models.usage import CLIResult
            return 0, "ok", CLIResult(result_text="ok")

        monkeypatch.setattr(claude_runner, "_run_cli", _fake_run_cli)
        monkeypatch.setattr(claude_runner, "is_claude_stage", lambda stage: True)
        monkeypatch.setattr(claude_runner, "is_codex_model", lambda model: False)
        monkeypatch.setattr(claude_runner, "get_stage_model", lambda stage: "claude-opus-5")

        code, _text, _result = _run_stage(project)

        assert code == 0
        assert len(calls) == 1, "legacy-путь обязан пройти через _run_cli ровно раз"
        # И это ИМЕННО прежний промпт — с транспортной оболочкой Read/Write.
        assert "Write tool" in calls[0]["task_text"]

    def test_b2_binding_pointing_nowhere_is_a_loud_error(
        self, monkeypatch, tmp_path, project
    ):
        """Переменная есть, файла нет — падение, а не тихий откат на legacy.

        Раньше здесь стояло `assert active() is False` — и это закрепляло
        дефект: «мост неактивен» уводило боевой этап на `claude -p` по PATH с
        файловыми инструментами и выходом в веб, замаскировав обход
        провайдерского слоя под обычную ошибку этапа.
        """
        monkeypatch.setenv(resolver.BINDING_ENV, str(tmp_path / "нет-такого.json"))
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            pipeline_bridge.active()

        # И этап тоже падает, а не уходит на прежний транспорт.
        from backend.app.services.llm import claude_runner

        called: list = []

        async def _must_not_run(*a, **k):
            called.append(1)
            from backend.app.models.usage import CLIResult
            return 0, "ok", CLIResult(result_text="ok")

        monkeypatch.setattr(claude_runner, "_run_cli", _must_not_run)
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            _run_stage(project)
        assert called == [], "прежний транспорт не имел права быть вызванным"

    def test_b3_missing_binding_file_never_reaches_legacy_cli(
        self, monkeypatch, tmp_path, project
    ):
        """То же на уровне `_run_cli`: перехват падает, а не пропускает дальше."""
        monkeypatch.setenv(resolver.BINDING_ENV, str(tmp_path / "нет.json"))
        from backend.app.services.llm import claude_runner

        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            asyncio.run(
                claude_runner._run_cli("промпт", "", 10, stage="text_analysis")
            )

    def test_a2_stage_outside_whitelist_fails_without_fallback(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Этап вне белого списка — отказ этапа, а не тихий возврат к CLI."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(
            monkeypatch,
            _binding(job_dir, executable=exe, stages=("provider_selfcheck",)),
            job_dir,
        )

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "белый список" in text
        assert not (project["output_dir"] / "02_text_analysis.json").exists()
        assert _journal_text(journal) == "", "модель звать было нельзя"


# ═════════════════ C/D/E. Локальная политика моделей ═════════════════════════
class TestModelPolicy:

    def test_c_capability_resolves_only_from_local_policy(self, tmp_path):
        """`strong_audit` превращается в точную модель ФАЙЛОМ ВОРКЕРА."""
        root = tmp_path / "worker"
        _policy_file(root)
        policy = model_policy.load_policy(root)
        capability = policy.resolve("claude", "strong_audit")
        assert capability.model == _POLICY_MODEL
        assert _REPORTED_MODEL in capability.accepted_reported_models

    def test_c2_missing_policy_is_refusal_not_default(self, tmp_path):
        """Нет политики — нет вызова. Умолчания «какая-нибудь модель» нет."""
        with pytest.raises(model_policy.ProviderPolicyError):
            model_policy.load_policy(tmp_path / "пусто")

    def test_c3_unknown_capability_refused(self, tmp_path):
        root = tmp_path / "worker"
        _policy_file(root)
        policy = model_policy.load_policy(root)
        with pytest.raises(model_policy.ProviderPolicyError):
            policy.resolve("claude", "cheap_draft")

    def test_c4_center_cannot_dictate_model_with_capability(self):
        """Центр не вправе одновременно назвать способность и строку модели."""
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderRequirement.from_payload({
                "provider": "claude", "capability": "strong_audit",
                "model": "claude-sonnet-5", "allowed_stages": ["text_analysis"],
                "max_inferences": 1,
            })
        with pytest.raises(audit_runner.AuditJobRejected):
            audit_runner._validate_provider_requirement({
                "provider": "claude", "capability": "strong_audit",
                "model": "claude-sonnet-5",
            })

    def test_c5_unknown_capability_refused_at_job_boundary(self):
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderRequirement.from_payload({
                "provider": "claude", "capability": "whatever",
            })

    def test_d_exact_model_passed_to_cli_explicitly(self):
        """Модель предъявляется CLI флагом, а не «подразумевается»."""
        argv = _inference_argv(_POLICY_MODEL)
        # Форма с `=`: значение неотделимо от флага, поэтому ни поглотить
        # соседний токен, ни быть разобранным как отдельный флаг оно не может.
        assert f"--model={_POLICY_MODEL}" in argv
        assert "--model" not in argv
        # Порядок сохраняется: вариадические флаги по-прежнему в форме `=`,
        # а промпт-флаг `-p` остаётся последним.
        assert argv[-1] == "-p"
        assert "--tools=" in argv

    def test_d2_argv_without_model_is_unchanged_from_11c(self):
        """Без назначенной модели argv в точности прежний."""
        assert not any(a.startswith("--model") for a in _inference_argv(None))

    def test_e_reported_model_mismatch_fails_closed(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Ответила другая модель — этап падает, артефакт не пишется."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal,
                           reported_model="claude-sonnet-5")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert errors.ERR_MODEL_MISMATCH in text
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_e2_missing_reported_model_fails_closed(self, monkeypatch, tmp_path):
        """«Не знаем, кто ответил» — тоже несовпадение."""
        home = tmp_path / "ambient"
        _ambient_home(monkeypatch, home)
        exe = _write_exe(tmp_path / "bin" / "claude", """#!/bin/bash
cat > /dev/null
echo '{"type":"result","result":"{\\"ok\\": true}","usage":{}}'
""")
        adapter = ClaudeProviderAdapter(
            ProviderHome(provider="claude", root=tmp_path / "prov",
                         auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home),
            executable=exe, timeout_sec=30.0, inference_allowed=True,
        )
        result = adapter.structured_inference(
            "тест", purpose="t", model=_POLICY_MODEL,
            accepted_reported_models=(_POLICY_MODEL, _REPORTED_MODEL),
        )
        assert result.status == inference.STATUS_ERROR
        assert result.error_code == errors.ERR_MODEL_MISMATCH

    def test_e3_model_without_accepted_list_refused_before_call(
        self, monkeypatch, tmp_path
    ):
        """Приказ без проверки не исполняется — отказ ДО запуска процесса."""
        home = tmp_path / "ambient"
        _ambient_home(monkeypatch, home)
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        adapter = ClaudeProviderAdapter(
            ProviderHome(provider="claude", root=tmp_path / "prov",
                         auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home),
            executable=exe, timeout_sec=30.0, inference_allowed=True,
        )
        result = adapter.structured_inference(
            "тест", purpose="t", model=_POLICY_MODEL, accepted_reported_models=(),
        )
        assert result.error_code == errors.ERR_MODEL_MISMATCH
        assert _journal_text(journal) == "", "процесс не должен был запускаться"

    def test_e4_validation_records_model_check_independently(self):
        """Сверка модели живёт и в проверке результата — из ПРИВЯЗКИ."""
        good = inference.ProviderInferenceResult(
            provider="claude", model=_REPORTED_MODEL,
            status=inference.STATUS_SUCCESS, result={"a": 1}, exit_code=0,
            auth_mode=AUTH_MODE_AMBIENT_USER,
        )
        report = inference.validate_inference(
            good, expected_provider="claude", expected_auth_mode=AUTH_MODE_AMBIENT_USER,
            task_id="t", attempt_id="a", claim_task_id="t", claim_attempt_id="a",
            expected_model=_POLICY_MODEL,
            accepted_reported_models=(_POLICY_MODEL, _REPORTED_MODEL),
        )
        assert "model_matches_policy" not in report.failed_names

        bad = inference.ProviderInferenceResult(
            provider="claude", model="claude-opus-4-8[1m]",
            status=inference.STATUS_SUCCESS, result={"a": 1}, exit_code=0,
            auth_mode=AUTH_MODE_AMBIENT_USER,
        )
        report = inference.validate_inference(
            bad, expected_provider="claude", expected_auth_mode=AUTH_MODE_AMBIENT_USER,
            task_id="t", attempt_id="a", claim_task_id="t", claim_attempt_id="a",
            expected_model=_POLICY_MODEL,
            accepted_reported_models=(_POLICY_MODEL, _REPORTED_MODEL),
        )
        assert "model_matches_policy" in report.failed_names


# ═════════ F/G/H/I. Кто читает файлы и что видит модель ══════════════════════
class TestFilesystemResponsibility:

    def test_g_pipeline_reads_md_itself(self, monkeypatch, tmp_path, job_dir, project):
        """MD попадает в промпт СОДЕРЖИМЫМ — значит его прочитал конвейер."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        stdin = _stdin_blob(journal)
        assert "Насос P-1 подобран на проектный расход 10 м3/ч" in stdin
        assert "| 1 | Насос P-1 | 12 м3/ч |" in stdin

    def test_h_prompt_carries_inline_document(self, project):
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert built["document_chars"] > 0
        assert "SOURCE DOCUMENT (inlined by the pipeline)" in built["prompt"]
        assert "Насос P-1" in built["prompt"]

    def test_i_no_project_paths_in_instructions(self, project):
        """§14: пути проекта не уезжают модели как инструкция."""
        messages = _build_messages(project)
        system_before = messages[0]["content"]
        # Сначала убеждаемся, что чистить БЫЛО что: иначе тест защищал бы пустоту.
        assert provider_transport.count_absolute_paths(system_before) > 0

        built = provider_transport.build_provider_prompt(messages)
        assert built["filesystem_refs_stripped"] > 0
        assert built["absolute_paths_remaining_in_instructions"] == 0
        instructions = built["prompt"].split("===== SOURCE DOCUMENT", 1)[0]
        assert str(project["project_dir"]) not in instructions
        assert "01_blocks_for_text.json" not in instructions

    def test_i2_document_body_is_never_rewritten(self, project):
        """Тело документа неприкосновенно: это данные, а не инструкции."""
        messages = _build_messages(project)
        document = messages[1]["content"]
        built = provider_transport.build_provider_prompt(messages)
        assert document in built["prompt"]

    def test_i3_path_stripper_does_not_touch_engineering_text(self):
        """Дроби и «и/или» не являются путями."""
        text = "расход 10 м3/ч и/или 12 м3/ч, дата 01/02/2026, п. 5.1.2"
        cleaned, count = provider_transport.strip_filesystem_references(text)
        assert count == 0
        assert cleaned == text

    def test_f_ambient_auth_preserved(self, monkeypatch, tmp_path):
        """HOME подпроцесса — ambient-каталог: иначе CLI не найдёт авторизацию."""
        home = tmp_path / "ambient"
        _ambient_home(monkeypatch, home)
        adapter = ClaudeProviderAdapter(
            ProviderHome(provider="claude", root=tmp_path / "prov",
                         auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home),
            executable=tmp_path / "claude", timeout_sec=30.0,
        )
        env = adapter.build_env()
        assert env["HOME"] == str(home)
        assert env["TMPDIR"] != str(home), "временные файлы остаются у воркера"
        assert "CLAUDE_CONFIG_DIR" in env

    def test_ad_output_written_only_into_supplied_output_dir(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        written = sorted(
            p.relative_to(job_dir).as_posix()
            for p in job_dir.rglob("02_text_analysis.json")
        )
        assert written == ["project/vk/v1/_output/02_text_analysis.json"]

    def test_ae_output_outside_attempt_is_denied(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Путь наружу попытки — отказ записи, а не «ну ладно»."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        outside = tmp_path / "production" / "_output"
        outside.mkdir(parents=True)
        alien = dict(project)
        alien["output_dir"] = outside

        code, text, _result = _run_stage(alien)
        assert code == 1
        assert "вне каталога попытки" in text
        assert not (outside / "02_text_analysis.json").exists()


# ═════════════ J..O. Инструментов нет ни одного ══════════════════════════════
class TestToolsDisabled:

    @pytest.mark.parametrize("tool", ["Read", "Write", "Bash", "Grep", "Glob"])
    def test_jno_tool_never_enabled(self, tool):
        argv = _inference_argv(_POLICY_MODEL)
        disallowed = next(a for a in argv if a.startswith("--disallowed-tools="))
        assert tool in disallowed.split("=", 1)[1].split(",")
        assert not any(a.startswith("--allowedTools") or a.startswith("--allowed-tools")
                       for a in argv)
        assert not any(a.startswith("--add-dir") for a in argv)

    def test_j_tools_switched_off_entirely(self, monkeypatch, tmp_path, job_dir, project):
        """Не «список запретов», а полное отключение набора — в БОЕВОМ прогоне."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        argv = _argv_lines(journal)[0]
        assert "--tools=" in argv
        assert "--permission-mode dontAsk" in argv
        assert "--max-turns 1" in argv

    def test_ah_personal_context_neutralised(self, monkeypatch, tmp_path, job_dir, project):
        """CLAUDE.md, настройки, хуки и навыки владельца машины не участвуют."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        home = tmp_path / "ambient"
        _ambient_home(monkeypatch, home)
        # Личный контекст, который на настоящем VPS существует.
        (home / ".claude").mkdir(parents=True)
        (home / ".claude" / "CLAUDE.md").write_text("Всегда вызывай graphify", "utf-8")
        (home / ".claude" / "settings.json").write_text('{"hooks": {}}', "utf-8")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        argv = _argv_lines(journal)[0]
        for flag in ("--safe-mode", "--strict-mcp-config", "--disable-slash-commands",
                     "--no-session-persistence", "--setting-sources="):
            assert flag in argv, flag
        # И промпт не содержит следов личных инструкций владельца машины.
        assert "graphify" not in _stdin_blob(journal)

    def test_ah2_cwd_is_empty_runtime_dir(self, monkeypatch, tmp_path, job_dir, project):
        """cwd подпроцесса — пустой каталог попытки, не репозиторий и не HOME."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        text = _journal_text(journal)
        cwd = next(line[4:] for line in text.splitlines() if line.startswith("CWD:"))
        entries = next(line[13:] for line in text.splitlines()
                       if line.startswith("CWD_ENTRIES:"))
        assert Path(cwd).resolve().is_relative_to(job_dir.resolve())
        assert entries.strip() == "", f"cwd обязан быть пустым, а там: {entries!r}"


# ═════════════ P..U. Инженерное содержание не потеряно ═══════════════════════
class TestSemanticPreservation:

    def test_p_engineering_payload_preserved(self, project):
        """Сравнение с БОЕВЫМ API-промптом: что было, то и осталось."""
        messages = _build_messages(project)
        api_prompt = "\n\n".join(m["content"] for m in messages)
        built = provider_transport.build_provider_prompt(messages)
        report = provider_transport.semantic_preservation_report(
            api_prompt=api_prompt, provider_prompt=built["prompt"],
        )
        assert report["engineering_lost"] == []
        assert report["transport_markers_leaked"] == []
        assert report["passed"] is True

    def test_q_discipline_profile_preserved(self, project):
        """Роль, чек-лист и категории именно ВК, а не подставленный EOM."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]
        from backend.app.services.common import discipline_service

        profile = discipline_service.load_discipline("VK")
        assert profile.code == "VK"
        head = profile.role.strip().splitlines()[0][:60]
        assert head and head in prompt
        assert profile.checklist.strip().splitlines()[0][:60] in prompt

    def test_r_normative_context_preserved(self, project):
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert "Normative Reference" in built["prompt"]
        assert "СП 30.13330" in built["prompt"]

    def test_s_absence_guard_parity(self, project):
        """Страж отсутствия ведёт себя в provider-промпте как в API-промпте.

        Флаг `PIPELINE_ABSENCE_GUARD_ENABLED` по умолчанию выключен, и это
        боевое состояние. Требовать «страж обязан быть» значило бы проверять
        не 11D, а настройку. Проверяется ПАРИТЕТ: что есть в API-промпте, то
        есть и в provider-промпте, и наоборот.
        """
        from backend.app.pipeline.stages.prepare.task_builder import _absence_guard_block

        guard = _absence_guard_block().strip()
        messages = _build_messages(project)
        api_prompt = "\n\n".join(m["content"] for m in messages)
        built = provider_transport.build_provider_prompt(messages)
        if guard:
            marker = guard.splitlines()[0][:60]
            assert marker in api_prompt
            assert marker in built["prompt"]
        else:
            assert "{ABSENCE_GUARD}" not in built["prompt"], (
                "плейсхолдер обязан быть подставлен, а не оставлен как есть"
            )

    def test_s2_absence_guard_survives_when_enabled(self, monkeypatch, project):
        """Со включённым флагом текст стража доходит до модели дословно."""
        import backend.app.pipeline.stages.prepare.task_builder as task_builder

        marker = "ABSENCE-GUARD-11D-MARKER: перед утверждением об отсутствии"
        monkeypatch.setattr(task_builder, "_absence_guard_block", lambda: marker)
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert marker in built["prompt"]

    def test_t_prescan_semantics_preserved(self, project):
        """Секция pre-scan попадает в provider-промпт ровно как в API-промпт."""
        from backend.app.pipeline.stages.text_analysis.md_prescan import (
            build_prescan_prompt_section,
        )

        section = build_prescan_prompt_section(str(project["md_path"])) or ""
        messages = _build_messages(project)
        api_prompt = "\n\n".join(m["content"] for m in messages)
        built = provider_transport.build_provider_prompt(messages)
        if section.strip():
            marker = section.strip().splitlines()[0][:60]
            assert marker in api_prompt
            assert marker in built["prompt"]
        else:
            # Пустая секция — законный исход на коротком MD. Тогда её нет
            # в ОБОИХ промптах, и это тоже сохранение семантики.
            assert "Text pre-scan" not in api_prompt
            assert "Text pre-scan" not in built["prompt"]

    def test_u_json_schema_preserved(self, project):
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]
        for needle in ('"text_findings"', '"normative_refs_found"',
                       '"project_params"', '"text_source"', '"norm_quote"'):
            assert needle in prompt, needle

    def test_u2_transport_contract_replaces_shell(self, project):
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert "OUTPUT TRANSPORT" in built["prompt"]
        assert "You have NO tools in this run" in built["prompt"]
        assert "Read tool" not in built["prompt"]
        assert "Write tool" not in built["prompt"]


# ═════════════ V/W. Приём и отказ по контракту результата ════════════════════
class TestResultContract:

    def test_v_valid_json_accepted_and_written_by_pipeline(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 0
        written = json.loads(
            (project["output_dir"] / "02_text_analysis.json").read_text("utf-8")
        )
        assert written["text_source"] == "md"
        assert len(written["text_findings"]) == 1
        report = json.loads(
            (project["output_dir"] / "text_analysis_provider_run.json").read_text("utf-8")
        )
        assert report["validation"]["passed"] is True
        assert report["transport"] == "provider_adapter"

    def test_w_missing_required_field_rejected(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Нет `text_findings` — этап падает, файл не появляется."""
        broken = {k: v for k, v in _ANSWER.items() if k != "text_findings"}
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=broken)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "required_fields" in text
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_w2_non_json_reply_rejected(self, monkeypatch, tmp_path, job_dir, project):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal,
                           answer="Я записал результат в файл, готово.")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 1
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_w3_wrong_text_source_rejected(self, monkeypatch, tmp_path, job_dir, project):
        """`text_source` не `md` — правило платформы, а не пожелание."""
        bad = dict(_ANSWER, text_source="document_graph")
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=bad)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "expected_semantics" in text

    def test_w4_soft_fields_do_not_fail_the_stage(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Косметические поля фиксируются, но исправный аудит не роняют."""
        lean = {k: v for k, v in _ANSWER.items()
                if k not in ("stage", "project_id", "timestamp")}
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=lean)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 0
        report = json.loads(
            (project["output_dir"] / "text_analysis_provider_run.json").read_text("utf-8")
        )
        assert report["soft_contract"]["missing"] == ["project_id", "stage", "timestamp"]


# ═════════════ X/Y/Z. Транспортные отказы ════════════════════════════════════
class TestTransportFailures:

    def test_x_timeout_is_a_stage_failure(self, monkeypatch, tmp_path, job_dir, project):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, stall=5.0)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        monkeypatch.setattr(
            "backend.app.services.llm.claude_runner.CLAUDE_TEXT_ANALYSIS_TIMEOUT", 1
        )

        code, text, _result = _run_stage(project)
        assert code == 1
        assert errors.ERR_TIMEOUT in text
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_y_nonzero_exit_is_a_stage_failure(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, exit_code=1)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 1
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_z_rate_limit_classified(self, monkeypatch, tmp_path, job_dir, project):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(
            tmp_path / "bin" / "claude", journal, exit_code=1,
            stderr_text="Error: rate limit reached, resets at 18:00",
        )
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)
        assert code == 1
        assert errors.ERR_RATE_LIMITED in text


# ═════════════ AA/AB/AC. Ровно один оплаченный вызов ═════════════════════════
class TestExactlyOnce:

    def test_aa_grant_consumed_once(self, monkeypatch, tmp_path, job_dir, project):
        """Второй прогон того же этапа не зовёт модель заново."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe, max_inferences=1),
                  job_dir)

        code1, _t1, _r1 = _run_stage(project)
        argv_after_first = len(_argv_lines(journal))
        code2, _t2, _r2 = _run_stage(project)

        assert (code1, code2) == (0, 0)
        assert len(_argv_lines(journal)) == argv_after_first == 1, (
            "повторный прогон обязан взять результат из журнала попытки"
        )

    def test_ab_replay_reports_not_performed(self, monkeypatch, tmp_path, job_dir, project):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        _run_stage(project)
        report = json.loads(
            (project["output_dir"] / "text_analysis_provider_run.json").read_text("utf-8")
        )
        assert report["performed_now"] is False, "второй прогон — воспроизведение"

    def test_ac_crash_after_inference_is_not_auto_retried(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Неизвестный исход не повторяется автоматически — решает оператор."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        messages = _build_messages(project)
        prompt = provider_transport.build_provider_prompt(messages)["prompt"]
        ledger = inference_ledger.InferenceLedger(
            job_dir, attempt_id="attempt-1", job_id="job-11d"
        )
        key = inference_ledger.call_key(
            attempt_id="attempt-1", provider="claude",
            purpose="text_analysis", prompt=prompt,
        )
        ledger.begin(key, provider="claude", purpose="text_analysis",
                     prompt_sha256=inference.sha256_text(prompt))
        ledger.mark_indeterminate(key, reason="смоделированное падение процесса")

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "не сохранён" in text or "I-P9" in text
        assert _journal_text(journal) == "", "второго оплаченного вызова не было"

    def test_aa2_inference_cap_respected(self, monkeypatch, tmp_path, job_dir, project):
        """Потолок вызовов попытки — рубеж, а не подсказка."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe, max_inferences=0),
                  job_dir)

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "потолок вызовов" in text
        assert _journal_text(journal) == ""


# ═════════════ AF/AG. Секреты и контрольный файл ═════════════════════════════
class TestSecretsAndCanary:

    def test_af_credentials_never_reach_subprocess_or_artifacts(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Токен воркера и адрес центра до подпроцесса не доходят."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_11d_secret_value_0001")
        monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.invalid")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        text = _journal_text(journal)
        assert "wtk_11d_secret_value_0001" not in text
        assert "center.invalid" not in text
        for artifact in job_dir.rglob("*.json"):
            assert "wtk_11d_secret_value_0001" not in artifact.read_text("utf-8")

    def test_af2_credential_shape_never_reaches_artifacts(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Форма ключа в ответе модели вычищается ДО записи артефактов.

        Два рубежа делают здесь разную работу, и важно не спутать их. Редактор
        (`audit_worker.redaction`) ЧИСТИТ вывод подпроцесса — поэтому такой
        ответ не роняет этап, он приходит уже без ключа. Проверка результата
        (`no_credential_like`) ЛОВИТ формы, до которых редактор не дотянулся, —
        её отдельно закрывает `test_af3`.
        """
        poisoned = dict(_ANSWER)
        poisoned["project_params"] = {"note": "sk-ant-api03-AAAAAAAABBBBBBBB"}
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=poisoned)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        for artifact in job_dir.rglob("*.json"):
            assert "sk-ant-api03-AAAAAAAABBBBBBBB" not in artifact.read_text("utf-8")

    def test_af3_private_path_in_answer_fails_validation(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Приватный путь, до которого редактор не дотягивается, — отказ.

        Выбран `/root/` намеренно. `/home/<user>/` редактор схлопывает в `~/`
        сам, и на нём проверка результата ничего не доказывала бы: она бы
        «прошла» просто потому, что чистить уже нечего. `/root/` редактор не
        трогает — значит именно здесь работает второй рубеж.
        """
        poisoned = dict(_ANSWER)
        poisoned["project_params"] = {"note": "см. /root/secret/notes.json"}
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=poisoned)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "no_private_paths" in text
        assert not (project["output_dir"] / "02_text_analysis.json").exists()

    def test_ag_canary_is_unreachable_for_the_subprocess(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Контрольный файл вне попытки: подпроцесс его не читает и не видит."""
        canary_dir = tmp_path / "canary"
        canary_dir.mkdir()
        canary = canary_dir / "DO_NOT_READ.txt"
        canary.write_text("CANARY-11D-DO-NOT-READ-12345678", encoding="utf-8")
        before = canary.stat().st_mtime_ns

        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(
            monkeypatch,
            _binding(job_dir, executable=exe,
                     literals=("CANARY-11D-DO-NOT-READ-12345678",)),
            job_dir,
        )

        _run_stage(project)
        assert canary.stat().st_mtime_ns == before
        assert "CANARY-11D" not in _journal_text(journal)
        # Привязка ИСКЛЮЧЕНА намеренно: контрольная строка лежит там по замыслу
        # (её кладёт туда оператор как `forbidden_literals`, файл 0600). Искать
        # её там значило бы объявить утечкой сам механизм проверки на утечку.
        for artifact in job_dir.rglob("*.json"):
            if artifact.name == resolver.BINDING_FILENAME:
                continue
            assert "CANARY-11D" not in artifact.read_text("utf-8"), artifact

    def test_ag2_forbidden_literal_in_answer_fails_validation(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Если контрольная строка ВСЁ-ТАКИ окажется в ответе — это отказ."""
        leaked = dict(_ANSWER)
        leaked["project_params"] = {"note": "CANARY-11D-DO-NOT-READ-12345678"}
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=leaked)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(
            monkeypatch,
            _binding(job_dir, executable=exe,
                     literals=("CANARY-11D-DO-NOT-READ-12345678",)),
            job_dir,
        )

        code, text, _result = _run_stage(project)
        assert code == 1
        assert "no_forbidden_literals" in text


# ═════════════ AI. Codex не участвует ════════════════════════════════════════
class TestCodexNotInvoked:

    def test_ai_codex_runner_never_called(self, monkeypatch, tmp_path, job_dir, project):
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        import backend.app.services.llm.codex_runner as codex_runner

        called: list = []
        for name in ("run_codex_exec", "run_codex_json_messages"):
            monkeypatch.setattr(
                codex_runner, name,
                lambda *a, **k: called.append(name),
            )

        code, _text, _result = _run_stage(project)
        assert code == 0
        assert called == []

    def test_ai2_codex_adapter_refuses_explicit_model(self, monkeypatch, tmp_path):
        """Явное назначение модели для codex не реализовано — и молчать нельзя."""
        home = tmp_path / "ambient"
        _ambient_home(monkeypatch, home)
        adapter = CodexProviderAdapter(
            ProviderHome(provider="codex", root=tmp_path / "prov",
                         auth_mode=AUTH_MODE_AMBIENT_USER, ambient_home=home),
            executable=tmp_path / "codex", timeout_sec=30.0, inference_allowed=True,
        )
        result = adapter.structured_inference(
            "тест", purpose="t", model="gpt-5.4", accepted_reported_models=("gpt-5.4",),
        )
        assert result.error_code == errors.ERR_MODEL_MISMATCH


# ═════════════ Рубеж формы требования центра ═════════════════════════════════
class TestReviewFindings:
    """Рубежи, добавленные по итогам состязательного разбора перед прогоном."""

    def test_center_model_field_refused_outright(self):
        """Точный идентификатор от центра не принимается ВООБЩЕ, даже без capability.

        Раньше задание только с `model` проходило оба рубежа, и от попадания
        произвольной строки центра в argv спасала лишь проверка тремя слоями
        ниже. I-P5 не имеет права держаться на побочном эффекте чужой проверки.
        """
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderRequirement.from_payload({
                "provider": "claude", "model": "claude-sonnet-5",
                "allowed_stages": ["text_analysis"], "max_inferences": 1,
            })

    def test_binding_without_model_refuses_to_call(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Вызов без назначенной модели — отказ, а не слепота 11C."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe, model=None), job_dir)

        code, text, _r = _run_stage(project)
        assert code == 1
        assert "нет назначенной модели" in text
        assert _journal_text(journal) == "", "модель звать было нельзя"

    def test_binding_model_string_is_validated_on_read(self, job_dir):
        """Строка модели из ФАЙЛА проходит ту же проверку, что и из политики."""
        payload = _binding(job_dir, executable=Path("/bin/true")).as_dict()
        payload["model"] = "--dangerously-skip-permissions"
        with pytest.raises(resolver.ProviderResolutionError):
            resolver.ProviderBinding.from_dict(payload)

    def test_model_id_rejects_leading_dash(self):
        with pytest.raises(model_policy.ProviderPolicyError):
            model_policy.validate_model_id("-opus", where="тест")

    def test_attempt_dir_requires_metadata_layout(self, monkeypatch, tmp_path):
        """Привязка вне раскладки попытки — отказ, иначе гейт записи вырождается."""
        stray = tmp_path / "provider_binding.json"
        stray.write_text("{}", encoding="utf-8")
        monkeypatch.setenv(resolver.BINDING_ENV, str(stray))
        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            pipeline_bridge.attempt_dir()

    def test_grant_cannot_be_reissued_after_use(self, tmp_path):
        """Перевыписка использованного разрешения не обнуляет счётчик."""
        from audit_worker.providers import inference_grant

        root = tmp_path / "worker"
        inference_grant.issue(root, grant_id="g-1", provider="claude",
                              task_id="t-1", ttl_sec=600.0, max_uses=1)
        inference_grant.consume(root, provider="claude", task_id="t-1")
        with pytest.raises(inference_grant.InferenceGrantError):
            inference_grant.issue(root, grant_id="g-1", provider="claude",
                                  task_id="t-1", ttl_sec=600.0, max_uses=1)

    def test_unreachable_call_does_not_consume_the_ledger(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """«До модели не дошли» не съедает попытку и не залипает в журнале."""
        _ambient_home(monkeypatch, tmp_path / "ambient")
        missing = tmp_path / "bin" / "claude-которого-нет"
        _activate(monkeypatch, _binding(job_dir, executable=missing), job_dir)

        code, text, _r = _run_stage(project)
        assert code == 1
        assert "не найден" in text
        summary = inference_ledger.InferenceLedger(
            job_dir, attempt_id="attempt-1", job_id="job-11d"
        ).summary()
        assert summary["calls_started"] == 0, (
            "несостоявшийся вызов не имеет права выглядеть как начатый"
        )

    def test_prompt_over_ceiling_refused_before_the_call(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Планировщика нарезки нет — значит нужен потолок, а не усечение."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        monkeypatch.setattr(
            "backend.app.services.llm.claude_runner.PROVIDER_PROMPT_MAX_CHARS", 100
        )
        code, text, _r = _run_stage(project)
        assert code == 1
        assert "потолка" in text
        assert _journal_text(journal) == ""

    def test_prompt_override_refuses_silent_substitution(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Кастомный промпт проекта не подменяется стоковым молча."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        import backend.app.pipeline.stages.prepare.task_builder as task_builder

        monkeypatch.setattr(
            task_builder, "_load_prompt_override",
            lambda project_id, stage: "мой особый промпт",
        )
        code, text, _r = _run_stage(project)
        assert code == 1
        assert "кастомный промпт" in text
        assert _journal_text(journal) == ""

    def test_existing_block_context_is_not_silently_dropped(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Собранный блочный контекст есть, а вложить его нечем — отказ."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        from backend.app.services.storage.stage_artifacts import BLOCKS_FOR_TEXT_FILENAME

        (project["output_dir"] / BLOCKS_FOR_TEXT_FILENAME).write_text(
            json.dumps({"blocks": []}), encoding="utf-8"
        )
        code, text, _r = _run_stage(project)
        assert code == 1
        assert "блочный контекст" in text
        assert _journal_text(journal) == ""

    def test_run_report_carries_no_document_text(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Ни промпта, ни ответа модели в отчёте о прогоне."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        _run_stage(project)
        raw = (project["output_dir"] / "text_analysis_provider_run.json").read_text("utf-8")
        assert "Насос P-1 подобран" not in raw, "тело документа уехало в отчёт"
        assert "Расход насоса P-1 в тексте" not in raw, "ответ модели уехал в отчёт"
        report = json.loads(raw)
        assert "prompt" not in report["prompt_build"]
        assert "result" not in report["provider_result"]
        assert report["prompt_sha256"]
        assert report["provider_result"]["raw_sha256"]

    def test_input_data_note_replaces_the_mangled_section(self, project):
        """Справка о входных данных говорит правду о Stage 02."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]
        assert "## Input Data (this run)" in prompt
        assert "Stage 02 block analysis — NOT available in this run" in prompt
        assert provider_transport.FILESYSTEM_PLACEHOLDER == "(not available in this run)"
        assert "inlined below; no filesystem access" not in prompt

    @pytest.mark.parametrize("text", [
        "тип н.з./н.о./н.з.",
        "см. https://docs.cntd.ru/document/1200084848",
        "категории (А)/(Б)/(В) по СП 12.13130",
        "режимы «ВКЛ»/«ОТКЛ»/«АВАРИЯ»",
        "блоки [TEXT]/[IMAGE]/[TABLE]",
        "воздухообмен, м3/ч /приток/вытяжка/",
        "трубы Ду (25)/(32)/(40)",
        "кабель ВВГнг(А)-LS 5х16, 220/380 В, L1/L2/L3/N/PE",
        "п. 7.4.5/7.4.6 СП 256.1325800.2016",
    ])
    def test_path_stripper_leaves_engineering_text_intact(self, text):
        """Зачистка путей не трогает инженерный текст со слэшами."""
        cleaned, count = provider_transport.strip_filesystem_references(text)
        assert count == 0, f"ложное срабатывание на {text!r} → {cleaned!r}"
        assert cleaned == text

    @pytest.mark.parametrize("text", [
        "READ: /home/coder/projects/x/_output/01_blocks_for_text.json",
        "см. /srv/audit/attempt/metadata/provider_binding.json",
        "путь /opt/worker/data/worker.db",
        "файл ./relative/../_output/02_text_analysis.json",
    ])
    def test_path_stripper_still_catches_real_paths(self, text):
        cleaned, count = provider_transport.strip_filesystem_references(text)
        assert count >= 1, f"путь не вычищен: {text!r}"
        assert provider_transport.FILESYSTEM_PLACEHOLDER in cleaned


class TestJobContract:

    def test_capability_accepted_at_job_boundary(self):
        safe = audit_runner._validate_provider_requirement({
            "provider": "claude",
            "capability": "strong_audit",
            "allowed_stages": ["text_analysis"],
            "max_inferences": 1,
        })
        assert safe["capability"] == "strong_audit"
        assert safe["model"] is None

    def test_unknown_field_still_refused(self):
        with pytest.raises(audit_runner.AuditJobRejected):
            audit_runner._validate_provider_requirement({
                "provider": "claude", "capability": "strong_audit",
                "секрет": "нет",
            })
