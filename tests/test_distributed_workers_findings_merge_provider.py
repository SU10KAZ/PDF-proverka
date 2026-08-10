"""Этап 11E — боевой этап `findings_merge` через ProviderAdapter.

Чем задача 11E отличается от 11D и почему тесты другие.

У `text_analysis` вход был ОДИН — Markdown документа, и главный риск состоял в
том, что модель не сможет его прочитать. У свода вход СОСТАВНОЙ: два готовых
артефакта предыдущих этапов, и главный риск другой — молчаливая потеря половины
входа. Боевой сборщик промпта при отсутствии артефакта кладёт в промпт строку
«(файл … не найден)» и идёт дальше; на центре это давняя данность, на воркере —
оплаченный вызов, который выглядит успешным и сводит аудит по одному источнику.

Поэтому проверяются пять классов утверждений:

  1. РАСПРЕДЕЛЕНИЕ ОБЯЗАННОСТЕЙ: оба артефакта читает конвейер, файл пишет
     конвейер, модель только сводит. У модели нет ни инструментов, ни путей.
  2. ПОЛНОТА ВХОДА: каждое входное замечание T-NNN/G-NNN физически доехало до
     промпта ДО вызова модели; отсутствующий или битый вход — отказ этапа.
  3. СОХРАНЕНИЕ ИНЖЕНЕРНОГО СОДЕРЖАНИЯ: смена транспорта не имеет права
     потерять правила дедупликации, объединения, severity, трассировки
     источников, schema и правила sheet/page.
  4. МОДЕЛЬ И РАЗРЕШЕНИЕ: модель назначает ЛОКАЛЬНАЯ политика воркера,
     расхождение — отказ; вызов ровно один и списывает ровно одно разрешение.
  5. НЕИЗМЕННОСТЬ ПРЕЖНЕГО ПУТИ: без привязки провайдера код платформы ведёт
     себя ровно как до 11E, и молчаливого отката из provider-режима нет.

НИ ОДИН тест этого файла не обращается к настоящей модели: везде подставной
исполняемый файл.

Прогон:
    python -m pytest tests/test_distributed_workers_findings_merge_provider.py -v
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

from audit_worker.providers import (                                   # noqa: E402
    inference_grant,
    inference_ledger,
    model_policy,
    pipeline_bridge,
    resolver,
)
from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER    # noqa: E402
from audit_worker.providers.claude_adapter import _inference_argv      # noqa: E402
from audit_worker.providers.codex_adapter import CodexProviderAdapter  # noqa: E402

from backend.app.pipeline.stages.findings_merge import provider_transport  # noqa: E402
from backend.app.pipeline.stages.text_analysis import (                    # noqa: E402
    provider_transport as text_provider_transport,
)


# ═════════════════════════ Подставной CLI и фикстуры ═════════════════════════

_POLICY_MODEL = "claude-opus-5"
_REPORTED_MODEL = "claude-opus-5[1m]"
_STAGE = "findings_merge"

#: Ответ «модели» — форма боевого артефакта `03_findings.json`.
_ANSWER: dict = {
    "meta": {
        "project_id": "EOM/11e-test",
        "audit_completed": "2026-08-10T00:00:00",
        "total_findings": 2,
        "blocks_analyzed": 2,
        "by_severity": {
            "КРИТИЧЕСКОЕ": 1, "ЭКОНОМИЧЕСКОЕ": 0, "ЭКСПЛУАТАЦИОННОЕ": 0,
            "РЕКОМЕНДАТЕЛЬНОЕ": 1, "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 0,
        },
    },
    "findings": [
        {
            "id": "F-001",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable",
            "sheet": "Лист 2",
            "page": 2,
            "problem": "Сечение кабеля не покрывает расчётный ток",
            "description": "Текст и чертёж расходятся: 4 мм2 против 6 мм2",
            "norm": "СП 256.1325800.2016, п. 7.1.2",
            "norm_quote": None,
            "solution": "Пересчитать сечение",
            "risk": "Перегрев кабеля",
            "source_finding_ids": ["T-001", "G-001"],
            "source_block_ids": ["BLK-1"],
            "related_block_ids": ["BLK-1"],
            "evidence_text_refs": [],
            "evidence": [{"type": "image", "block_id": "BLK-1", "page": 2}],
            "highlight_regions": [],
        },
        {
            "id": "F-002",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "documentation",
            "sheet": None,
            "page": None,
            "problem": "Опечатка в наименовании щита",
            "description": "ЩР-1 против ЩР1",
            "norm": None,
            "norm_quote": None,
            "solution": "Привести к одному написанию",
            "risk": "Путаница при монтаже",
            "source_finding_ids": ["T-002"],
            "source_block_ids": [],
            "related_block_ids": [],
            "evidence_text_refs": [],
            "evidence": [],
            "highlight_regions": [],
        },
    ],
}


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
) -> Path:
    """Подделка `claude`, ведущая журнал argv/stdin/env/cwd."""
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
python3 - <<'PYEOF'
import json
answer = {payload}
print(json.dumps({{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer,
    "usage": {{"input_tokens": 1500, "output_tokens": 700,
               "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}},
    "modelUsage": {{"{reported_model}": {{"inputTokens": 1500}}}},
    "total_cost_usd": 0.0,
    "num_turns": 1,
}}, ensure_ascii=False))
PYEOF
exit {exit_code}
""")


_TEXT_ANALYSIS: dict = {
    "stage": "02_text_analysis",
    "project_id": "EOM/11e-test",
    "text_source": "md",
    "timestamp": "2026-08-09T00:00:00",
    "project_params": {"object_type": "тест", "total_load_kw": 120},
    "normative_refs_found": [
        {"ref": "СП 256.1325800.2016", "status": "ДЕЙСТВУЕТ", "edition": "", "note": ""}
    ],
    "text_findings": [
        {
            "id": "T-001",
            "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "cable",
            "source": "MD стр. 2",
            "finding": "Сечение кабеля 4 мм2 при расчётном токе 32 А",
            "norm": "СП 256.1325800.2016, п. 7.1.2",
            "norm_quote": None,
            "related_block_ids": [],
        },
        {
            "id": "T-002",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "documentation",
            "source": "MD стр. 1",
            "finding": "Щит назван и ЩР-1, и ЩР1",
            "norm": None,
            "norm_quote": None,
            "related_block_ids": [],
        },
    ],
    "items_verified_from_blocks": [],
}

_BLOCKS_ANALYSIS: dict = {
    "stage": "01_blocks_analysis",
    "stage01_meta": {"blocks_reviewed": 2, "total_blocks_expected": 2},
    "block_analyses": [
        {
            "block_id": "BLK-1",
            "page": 2,
            "sheet": "Лист 2",
            "label": "Однолинейная схема ВРУ",
            "sheet_type": "single_line_diagram",
            "findings": [
                {
                    "id": "G-001",
                    "severity": "КРИТИЧЕСКОЕ",
                    "category": "cable",
                    "finding": "На схеме кабель 6 мм2, в спецификации 4 мм2",
                    "norm": "СП 256.1325800.2016, п. 7.1.2",
                    "value_found": "6 мм2",
                    "highlight_regions": [
                        {"x": 0.1, "y": 0.2, "w": 0.1, "h": 0.1, "label": "кабель"}
                    ],
                }
            ],
            "highlight_regions": [
                {"x": 0.1, "y": 0.2, "w": 0.1, "h": 0.1, "label": "кабель"}
            ],
        },
        {
            "block_id": "BLK-2",
            "page": 3,
            "sheet": "Лист 3",
            "label": "План силового оборудования",
            "sheet_type": "floor_plan",
            "findings": [
                {
                    "id": "G-002",
                    "severity": "ЭКОНОМИЧЕСКОЕ",
                    "category": "equipment",
                    "finding": "Позиция 7 на плане отсутствует в спецификации",
                    "norm": None,
                    "value_found": "поз. 7",
                    "highlight_regions": [],
                }
            ],
        },
    ],
}


@pytest.fixture()
def job_dir(tmp_path: Path) -> Path:
    path = tmp_path / "jobs" / "job-11e" / "attempt-1"
    (path / "metadata").mkdir(parents=True)
    return path


@pytest.fixture()
def project(job_dir: Path) -> dict:
    """Синтетический проект ВНУТРИ каталога попытки — как на воркере."""
    vdir = job_dir / "project" / "eom" / "v1"
    out = vdir / "_output"
    out.mkdir(parents=True)
    (out / "02_text_analysis.json").write_text(
        json.dumps(_TEXT_ANALYSIS, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    (out / "01_blocks_analysis.json").write_text(
        json.dumps(_BLOCKS_ANALYSIS, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return {
        "project_dir": vdir,
        "output_dir": out,
        "project_id": "EOM/11e-test",
        "project_info": {
            "project_id": "EOM/11e-test",
            "name": "11e-test",
            "section": "EOM",
            "md_file": "",
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
    stages=(_STAGE,),
    max_inferences: int = 1,
    model: str | None = _POLICY_MODEL,
    accepted: tuple = (_POLICY_MODEL, _REPORTED_MODEL),
    literals=(),
    grant_id: str = "g-11e-0001",
) -> resolver.ProviderBinding:
    return resolver.ProviderBinding(
        schema_version=resolver.BINDING_SCHEMA_VERSION,
        provider="claude",
        auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(resolver.ambient_root_for_attempt(job_dir, "claude")),
        executable=str(executable),
        timeout_sec=60.0,
        job_id="job-11e",
        attempt_id="attempt-1",
        task_id="job-11e",
        grant_id=grant_id,
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
    home.mkdir(parents=True, exist_ok=True)
    from audit_worker.providers import auth_mode as auth_mode_mod

    monkeypatch.setattr(auth_mode_mod, "resolve_ambient_home", lambda: home)
    monkeypatch.setattr(auth_mode_mod, "ambient_user_name", lambda: "tester")


def _run_stage(project: dict, on_output=None):
    """Вызвать БОЕВОЙ транспорт этапа и вернуть (код, текст, result)."""
    from backend.app.services.llm import claude_runner

    return asyncio.run(
        claude_runner.run_findings_merge(
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
        return prompt_builder.build_findings_merge_messages(
            project["project_info"], project["project_id"]
        )


def _legacy_task(project: dict) -> str:
    from backend.app.services.common import audit_scope
    from backend.app.pipeline.stages.prepare.task_builder import (
        prepare_findings_merge_task,
    )

    with audit_scope.bind_audit_scope(
        output_dir=project["output_dir"], version_dir=project["project_dir"],
        project_id=project["project_id"], version_id="v1",
    ):
        return prepare_findings_merge_task(
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


def _run_report(project: dict) -> dict:
    path = project["output_dir"] / "findings_merge_provider_run.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


# ═════════ A/C. Маршрутизация: provider-режим и неизменность legacy ══════════
class TestStageRouting:

    def test_a_binding_routes_findings_merge_into_provider(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 A/B: активная привязка уводит боевой свод в ProviderAdapter."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, result = _run_stage(project)

        assert code == 0, _text
        assert (project["output_dir"] / "03_findings.json").is_file()
        assert result.input_tokens == 1500
        # Промпт ушёл через stdin, а не аргументом — данных задания в argv нет.
        assert "Сечение кабеля 4 мм2" in _stdin_blob(journal)
        assert not any("кабел" in line for line in _argv_lines(journal))

    def test_c_legacy_path_untouched_without_binding(self, monkeypatch, project):
        """§23 C: без привязки — прежний код платформы, ни одной новой ветки."""
        from backend.app.services.llm import claude_runner

        monkeypatch.delenv(resolver.BINDING_ENV, raising=False)
        calls: list = []

        async def _fake_run_cli(task_text, tools, timeout, on_output=None, **kwargs):
            calls.append({"tools": tools, "stage": kwargs.get("stage"),
                          "task_text": task_text})
            from backend.app.models.usage import CLIResult
            return 0, "ok", CLIResult(result_text="ok")

        monkeypatch.setattr(claude_runner, "_run_cli", _fake_run_cli)
        monkeypatch.setattr(claude_runner, "get_stage_model",
                            lambda stage: "claude-opus-5")
        monkeypatch.setattr(claude_runner, "is_claude_stage", lambda stage: True)
        monkeypatch.setattr(claude_runner, "is_codex_model", lambda model: False)

        code, _text, _result = _run_stage(project)

        assert code == 0
        assert len(calls) == 1
        # Прежняя ветка получает ИМЕННО файловые инструменты и файловый шаблон.
        assert "Read" in calls[0]["tools"] and "Write" in calls[0]["tools"]
        assert "Write tool" in calls[0]["task_text"]
        assert not (project["output_dir"] / "findings_merge_provider_run.json").exists()

    def test_ac_provider_failure_never_falls_back_to_legacy(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AC: отказ моста — отказ ЭТАПА, а не переход на прежний CLI."""
        from backend.app.services.llm import claude_runner

        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        # Этап вне белого списка привязки — мост обязан отказать.
        _activate(monkeypatch,
                  _binding(job_dir, executable=exe, stages=("optimization",)),
                  job_dir)

        called: list = []

        async def _guard(*args, **kwargs):
            called.append(kwargs.get("stage"))
            raise AssertionError("прежний транспорт не имеет права быть вызван")

        monkeypatch.setattr(claude_runner, "_run_cli", _guard)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "provider_bridge" in text
        assert called == []
        assert not (project["output_dir"] / "03_findings.json").exists()

    def test_codex_route_unreachable_in_provider_mode(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AM: развилка codex стоит НИЖЕ моста и в provider-режиме мертва."""
        from backend.app.services.llm import claude_runner

        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        # Даже если конфигурация этапа называет codex — вызова codex не будет.
        monkeypatch.setattr(claude_runner, "get_stage_model",
                            lambda stage: "codex/gpt-5.4")

        def _guard(*args, **kwargs):
            raise AssertionError("codex не имеет права быть вызван в 11E")

        monkeypatch.setattr(claude_runner, "_run_codex_json_stage", _guard)

        code, _text, _result = _run_stage(project)

        assert code == 0
        assert _run_report(project)["transport"] == "provider_adapter"


# ═════════════ D/E/F. Контракт входа: наличие, целостность, отказ ════════════
class TestInputContract:

    def test_d_pipeline_reads_both_required_artifacts(self, project):
        """§23 D: конвейер сам читает оба обязательных входа."""
        inputs = provider_transport.resolve_merge_inputs(project["output_dir"])
        facts = inputs.as_facts()

        assert facts["text_analysis"]["text_findings"] == 2
        assert facts["blocks_analysis"]["block_findings"] == 2
        assert facts["blocks_analysis"]["block_analyses"] == 2
        assert facts["expected_input_finding_ids"] == [
            "G-001", "G-002", "T-001", "T-002",
        ]
        assert len(facts["text_analysis"]["sha256"]) == 64

    @pytest.mark.parametrize("missing", ["02_text_analysis.json",
                                         "01_blocks_analysis.json"])
    def test_e_missing_required_input_fails(self, project, missing):
        """§23 E: отсутствующий обязательный вход — отказ, а не пустой промпт."""
        (project["output_dir"] / missing).unlink()

        with pytest.raises(provider_transport.MergeInputError) as exc:
            provider_transport.resolve_merge_inputs(project["output_dir"])
        assert missing in str(exc.value)

    def test_e_stage_fails_before_model_when_input_missing(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 E: до модели дело не доходит — вызовов ноль, артефакта нет."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        (project["output_dir"] / "01_blocks_analysis.json").unlink()

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "01_blocks_analysis.json" in text
        assert _argv_lines(journal) == []
        assert not (project["output_dir"] / "03_findings.json").exists()

    @pytest.mark.parametrize("body", ["{не json", '["список, а не объект"]'])
    def test_f_malformed_input_fails(self, project, body):
        """§23 F: битый или не-объектный вход — отказ этапа."""
        (project["output_dir"] / "02_text_analysis.json").write_text(
            body, encoding="utf-8",
        )
        with pytest.raises(provider_transport.MergeInputError):
            provider_transport.resolve_merge_inputs(project["output_dir"])

    def test_missing_input_is_not_masked_by_builder_placeholder(self, project):
        """Сборщик подставляет «(файл … не найден)» — гейт обязан сработать РАНЬШЕ.

        Тест фиксирует именно то поведение, ради которого написан отдельный
        резолвер входа: сам сборщик молча продолжает.
        """
        (project["output_dir"] / "01_blocks_analysis.json").unlink()
        messages = _build_messages(project)
        payload = messages[1]["content"]
        assert "не найден" in payload  # боевой сборщик — молчит
        with pytest.raises(provider_transport.MergeInputError):  # гейт 11E — нет
            provider_transport.resolve_merge_inputs(project["output_dir"])


# ═════════════ G/H/I/J/K. Транспорт входа: полнота и сериализация ════════════
class TestInputTransport:

    def test_g_all_input_ids_present_before_model(self, project):
        """§23 G: каждый входной T-/G-идентификатор физически есть в промпте."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        inputs = provider_transport.resolve_merge_inputs(project["output_dir"])
        report = provider_transport.input_coverage_report(
            built["prompt"], inputs.as_facts()["expected_input_finding_ids"],
        )

        assert report["missing_before_inference"] == []
        assert report["expected_count"] == 4
        assert report["encoded_count"] == 4
        assert report["passed"] is True

    def test_g_coverage_catches_a_lost_input(self, project):
        """§23 G: проверка не декоративна — потерю она видит."""
        report = provider_transport.input_coverage_report(
            'payload with "T-001" only', ["T-001", "G-001"],
        )
        assert report["missing_before_inference"] == ["G-001"]
        assert report["passed"] is False

    def test_g_coverage_does_not_confuse_prefixes(self):
        """`T-001` не считается найденным по вхождению в `T-0010`."""
        report = provider_transport.input_coverage_report('"T-0010"', ["T-001"])
        assert report["missing_before_inference"] == ["T-001"]

    def test_g_duplicate_input_ids_are_reported(self, project):
        """Дубли на входе фиксируются, а не схлопываются молча."""
        report = provider_transport.input_coverage_report(
            '"T-001"', ["T-001", "T-001"],
        )
        assert report["duplicate_input_ids"] == ["T-001"]

    def test_h_inline_block_findings_are_complete(self, project):
        """§23 H: блочные замечания уезжают inline со всеми полями контракта."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]

        assert "Однолинейная схема ВРУ" in prompt       # label блока
        assert "На схеме кабель 6 мм2" in prompt        # текст замечания
        assert '"highlight_regions"' in prompt          # координаты
        assert '"sheet": "Лист 2"' in prompt            # привязка к листу

    def test_i_no_crop_or_pdf_read_dependency(self, project):
        """§23 I: в промпте нет ни пути к кропу, ни пути к PDF."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        head = built["prompt"].split("===== STAGE OUTPUTS", 1)[0]

        assert built["absolute_paths_remaining_in_instructions"] == 0
        for needle in (".pdf", "crop_url", "blocks_stage02", "/_output/"):
            assert needle not in head
        assert "NOT available in this run" in head      # про MD сказано прямо

    def test_j_serialization_is_deterministic(self, project):
        """§23 J: две сборки подряд дают побайтово один промпт."""
        first = provider_transport.build_provider_prompt(_build_messages(project))
        second = provider_transport.build_provider_prompt(_build_messages(project))
        assert first["prompt"] == second["prompt"]
        assert pipeline_bridge.sha256_text(first["prompt"]) == \
            pipeline_bridge.sha256_text(second["prompt"])

    def test_j_payload_is_valid_json_not_python_repr(self, project):
        """Полезная нагрузка — настоящий JSON, а не `str(dict)`."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        payload = built["prompt"].split(
            "===== STAGE OUTPUTS TO CONSOLIDATE (inlined by the pipeline) =====", 1
        )[1].split("===== END OF STAGE OUTPUTS =====", 1)[0]

        assert "'id':" not in payload            # не Python repr
        assert "True" not in payload.replace("Truep", "")
        chunk = payload.split("## 01_blocks_analysis.json:", 1)[0]
        chunk = chunk.split("## 02_text_analysis.json:", 1)[1].strip()
        assert json.loads(chunk)["text_findings"][0]["id"] == "T-001"

    def test_j_unicode_survives_intact(self, project):
        """Кириллица не уезжает в `\\uXXXX` и не ломается."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert "Сечение кабеля 4 мм2 при расчётном токе 32 А" in built["prompt"]
        assert "\\u0421" not in built["prompt"]

    def test_k_no_truncation_of_either_artifact(self, project):
        """§23 K: оба артефакта доехали целиком, а не «первые N символов»."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]

        assert "## 02_text_analysis.json:" in prompt
        assert "## 01_blocks_analysis.json:" in prompt
        assert "Позиция 7 на плане отсутствует" in prompt   # хвост второго блока
        assert built["payload_chars"] >= (
            len(json.dumps(_TEXT_ANALYSIS, ensure_ascii=False, indent=2))
        )

    def test_k_oversized_prompt_is_refused_before_model(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Потолок промпта — отказ ДО вызова, а не молчаливое усечение."""
        from backend.app.services.llm import claude_runner

        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)
        monkeypatch.setattr(claude_runner, "PROVIDER_MERGE_PROMPT_MAX_CHARS", 100)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "потолка" in text
        assert _argv_lines(journal) == []


# ═══════════ L..S. Сохранение инженерного содержания (§13, §14) ══════════════
class TestSemanticPreservation:

    def test_l_discipline_context_preserved(self, project):
        """§23 L: роль дисциплины доезжает до модели."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert "эксперт-проектировщик по электроснабжению" in built["prompt"]

    @pytest.mark.parametrize("marker,needle", [
        ("dedup", "**Deduplication**"),
        ("merge_step", "### Step 2: Merge Findings"),
        ("merge_rules", "### Merge Rules"),
        ("evidence", "evidence_text_refs"),
        ("source_tracing", "source_finding_ids"),
        ("severity_elevation", "**Severity elevation**"),
        ("severity_reduction", "**Severity reduction**"),
        ("schema", '"by_severity"'),
        ("sheet_page", "Sheet and Page Rules"),
        ("norm_quote", "norm_quote"),
        ("disputed", "`disputed`"),
        ("coverage_warning", "Coverage Warning Sections"),
        ("no_internal_ids", "No internal identifiers in human-readable text"),
        ("output_language", "OUTPUT LANGUAGE"),
    ])
    def test_mnopq_engineering_rules_preserved(self, project, marker, needle):
        """§23 M/N/O/P/Q: правила свода не теряются при смене транспорта."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert needle in built["prompt"], marker

    def test_semantic_report_vs_api_prompt_loses_nothing(self, project):
        """§13: сводный отчёт сверки — потеряно 0, транспортного просочилось 0."""
        messages = _build_messages(project)
        api_prompt = "\n\n".join(m["content"] for m in messages)
        built = provider_transport.build_provider_prompt(messages)

        report = provider_transport.semantic_preservation_report(
            api_prompt=api_prompt, provider_prompt=built["prompt"],
        )
        assert report["engineering_lost"] == []
        assert report["transport_markers_leaked"] == []
        assert report["absolute_paths_in_provider_instructions"] == 0
        assert report["passed"] is True

    def test_semantic_report_detects_a_real_loss(self):
        """Сверка не декоративна: убранное правило она замечает."""
        report = provider_transport.semantic_preservation_report(
            api_prompt="### Merge Rules\n**Deduplication**",
            provider_prompt="### Merge Rules",
        )
        assert "dedup_rule" in report["engineering_lost"]
        assert report["passed"] is False

    def test_r_no_hidden_claude_md_dependency(self, project):
        """§23 R: скрытой опоры на CLAUDE.md нет — контекст подавлен адаптером."""
        argv = _inference_argv(model=_POLICY_MODEL)
        joined = " ".join(argv)
        assert "--setting-sources=" in joined
        assert "--strict-mcp-config" in joined
        assert "--safe-mode" in joined
        assert "--disable-slash-commands" in joined

    def test_s_severity_semantics_included_explicitly(self, project):
        """§23 S/§14: смысл шкалы severity перенесён в промпт ЯВНО."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]

        assert "## Severity Semantics (what each value means)" in prompt
        for value in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
                      "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"):
            assert value in prompt
        assert built["map"]["severity_semantics_applied"] is True
        # Якорь — рядом с перечнем значений, а не в хвосте инструкций.
        assert built["map"]["severity_semantics_anchor"] == "### Finding Fields"

    def test_s_severity_semantics_shared_with_stage01(self):
        """Определения — ОДИН экземпляр на платформу, а не копия.

        Второй экземпляр тех же формулировок разошёлся бы с первым на первой же
        правке, и два этапа начали бы мерить severity по-разному.
        """
        assert provider_transport.SEVERITY_SEMANTICS is \
            text_provider_transport.SEVERITY_SEMANTICS

    def test_s_severity_semantics_is_not_tuned_to_one_document(self):
        """Формулировка симметрична и не упоминает конкретных тем документа."""
        text = provider_transport.SEVERITY_SEMANTICS
        assert "Do not soften it and\ndo not inflate it." in text
        for forbidden in ("ОСУП", "TN-S", "7.35", "133-23"):
            assert forbidden not in text

    def test_input_data_note_tells_the_truth(self, project):
        """Справка о входе не обещает того, чего в прогоне нет."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        head = built["prompt"].split("===== STAGE OUTPUTS", 1)[0]

        assert "**Project MD file — NOT available in this run.**" in head
        assert "**Normative reference — NOT available in this run.**" in head
        # И при этом честно называет то, что ЕСТЬ.
        assert "inlined below under `## 02_text_analysis.json`" in head
        assert "inlined below under `## 01_blocks_analysis.json`" in head

    def test_false_template_claim_is_refuted_not_merely_contradicted(self, project):
        """Ложное утверждение шаблона опровергается ЯВНО, а не молча.

        Шаблон свода утверждает «**Normative reference** — provided in system
        context», и это неправда ни на одной ветке: в EN-шаблоне есть только
        `{DISCIPLINE_ROLE}`. Пока справка о входе стояла рядом молча, промпт
        содержал два соседних противоположных утверждения, причём ложное —
        позже по тексту, то есть с большей рецентностью.
        """
        built = provider_transport.build_provider_prompt(_build_messages(project))
        head = built["prompt"].split("===== STAGE OUTPUTS", 1)[0]

        # Строка шаблона на месте — продовый шаблон не тронут.
        claim = "**Normative reference** — provided in system context."
        assert claim in head
        # И она названа и опровергнута, а не просто окружена другим текстом.
        assert 'states that it is "provided in system context"' in head
        assert "In this run it is not" in head
        # Опровержение стоит РАНЬШЕ ложной строки — иначе последнее слово
        # осталось бы за ней.
        assert head.index("In this run it is not") < head.index(claim)

    def test_norms_reference_really_is_absent(self, project):
        """Опровержение соответствует действительности, а не перестраховке."""
        messages = _build_messages(project)
        api_prompt = "\n\n".join(m["content"] for m in messages)
        # Признак вложенного справочника норм — заголовки norms_reference.md
        # дисциплины. Роль дисциплины при этом на месте.
        assert "эксперт-проектировщик по электроснабжению" in api_prompt
        assert "norms_reference" not in api_prompt

    def test_transport_contract_limits_tools_not_subject(self, project):
        """Урок 11D.1: ограничение касается ИНСТРУМЕНТОВ, а не предмета аудита."""
        text = provider_transport.TRANSPORT_CONTRACT
        assert "This restriction is about TOOL ACCESS ONLY" in text
        assert "must be\nreported as usual" in text
        assert "Silently skipping an input finding is not one of the options" in text


# ═════════════ T/U/V/W/X/Y/Z. Транспортная оболочка и изоляция ═══════════════
class TestTransportShell:

    def test_tu_read_and_write_instructions_removed(self, project):
        """§23 T/U: файловые инструкции сняты — и ровно они, а не смысл."""
        legacy = _legacy_task(project)
        built = provider_transport.build_provider_prompt(_build_messages(project))
        prompt = built["prompt"]

        # В заменяемом CLI-шаблоне они были.
        assert "READ via Read tool" in legacy
        assert "WRITE via Write tool" in legacy
        assert "DO NOT output to chat" in legacy
        # В provider-промпте их нет ни в каком виде.
        for needle in ("Read tool", "Write tool", "READ via", "WRITE via",
                       "DO NOT output to chat",
                       "After writing, output a brief summary"):
            assert needle not in prompt
        # А вместо них — явный транспортный контракт.
        assert "## OUTPUT TRANSPORT" in prompt

    def test_u_output_path_is_not_in_the_prompt(self, project):
        """Путь выходного файла модели не сообщается вовсе."""
        built = provider_transport.build_provider_prompt(_build_messages(project))
        assert "03_findings.json" not in built["prompt"].split(
            "===== STAGE OUTPUTS", 1
        )[0]

    @pytest.mark.parametrize("tool", ["Bash", "Grep", "Glob", "Read", "Write"])
    def test_vwxy_tools_disabled_by_name(self, tool):
        """§23 V/W/X/Y: набор инструментов пуст И каждый запрещён поимённо."""
        argv = _inference_argv(model=_POLICY_MODEL)
        joined = " ".join(argv)
        assert "--tools=" in joined
        disallowed = next(a for a in argv if a.startswith("--disallowed-tools="))
        assert tool in disallowed.split("=", 1)[1].split(",")

    def test_y_max_turns_is_one(self):
        """Один ход: агентного цикла с добором данных не предусмотрено."""
        argv = _inference_argv(model=_POLICY_MODEL)
        assert "--max-turns" in argv
        assert argv[argv.index("--max-turns") + 1] == "1"

    def test_z_controlled_cwd(self, monkeypatch, tmp_path, job_dir, project):
        """§23 Z: рабочий каталог CLI — пустой каталог ВНУТРИ попытки."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 0

        cwd_line = next(line for line in _journal_text(journal).splitlines()
                        if line.startswith("CWD:"))
        cwd = Path(cwd_line[4:])
        assert cwd.resolve().is_relative_to(job_dir.resolve())
        entries_line = next(line for line in _journal_text(journal).splitlines()
                            if line.startswith("CWD_ENTRIES:"))
        assert entries_line[len("CWD_ENTRIES:"):].strip() == ""


# ═════════════════ AA/AB. Модель: локальная политика и сверка ════════════════
class TestModelPolicy:

    def test_aa_exact_model_passed_to_cli(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AA: в argv уходит модель ИЗ ПРИВЯЗКИ, а не умолчание CLI."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)

        assert code == 0
        work_argv = [line for line in _argv_lines(journal) if "-p" in line.split()]
        assert work_argv, _journal_text(journal)
        assert f"--model={_POLICY_MODEL}" in work_argv[-1]

    def test_ab_model_mismatch_fails_closed(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AB: чужая фактическая модель — отказ, артефакт не пишется."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal,
                           reported_model="claude-sonnet-5")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "model_matches_policy" in text
        assert not (project["output_dir"] / "03_findings.json").exists()
        # Отчёт о прогоне пишется и при отказе — разбирать иначе нечем.
        assert _run_report(project)["validation"]["passed"] is False

    def test_ab_policy_resolves_capability_locally(self, tmp_path):
        """Строка модели берётся из файла администратора машины, не из задания."""
        root = tmp_path / "worker-root"
        _policy_file(root)
        policy = model_policy.load_policy(root)
        capability = policy.resolve("claude", "strong_audit")

        assert capability.model == _POLICY_MODEL
        assert capability.reported_matches(_REPORTED_MODEL) is True
        assert capability.reported_matches("claude-opus-4-8") is False
        assert capability.reported_matches(None) is False


# ═════════════ AD/AE/AF. Ровно один вызов и поведение при повторе ════════════
class TestExactlyOnce:

    def test_ad_single_call_and_single_ledger_entry(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AD: один этап — один вызов CLI и одна запись журнала."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        binding = _binding(job_dir, executable=exe)
        _activate(monkeypatch, binding, job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 0

        work_argv = [line for line in _argv_lines(journal) if "-p" in line.split()]
        assert len(work_argv) == 1
        summary = inference_ledger.InferenceLedger(
            job_dir, attempt_id=binding.attempt_id, job_id=binding.job_id,
        ).summary()
        assert summary["calls_started"] == 1
        assert summary["calls_completed"] == 1

    def test_ae_replay_does_not_call_the_model_again(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AE: повтор завершённой попытки берёт результат из журнала."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, max_inferences=5, executable=exe),
                  job_dir)

        assert _run_stage(project)[0] == 0
        first = len([l for l in _argv_lines(journal) if "-p" in l.split()])
        (project["output_dir"] / "03_findings.json").unlink()

        code, _text, _result = _run_stage(project)

        assert code == 0
        second = len([l for l in _argv_lines(journal) if "-p" in l.split()])
        assert second == first == 1
        assert _run_report(project)["performed_now"] is False
        # Артефакт восстановлен из сохранённого ответа, без новой оплаты.
        assert (project["output_dir"] / "03_findings.json").is_file()

    def test_af_indeterminate_call_is_not_retried(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AF: исход неизвестен — автоповтора нет, решает оператор."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        binding = _binding(job_dir, executable=exe)
        _activate(monkeypatch, binding, job_dir)

        built = provider_transport.build_provider_prompt(_build_messages(project))
        ledger = inference_ledger.InferenceLedger(
            job_dir, attempt_id=binding.attempt_id, job_id=binding.job_id,
        )
        key = inference_ledger.call_key(
            attempt_id=binding.attempt_id, provider="claude",
            purpose=_STAGE, prompt=built["prompt"],
        )
        ledger.begin(key, provider="claude", purpose=_STAGE,
                     prompt_sha256=pipeline_bridge.sha256_text(built["prompt"]))
        ledger.mark_indeterminate(key, reason="имитация падения между вызовом и записью")

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "I-P9" in text or "не сохранён" in text
        assert _argv_lines(journal) == []

    def test_ad_grant_is_consumed_exactly_once(self, tmp_path):
        """§28: разрешение с max_uses=1 списывается один раз и больше не годится."""
        root = tmp_path / "worker-root"
        root.mkdir(parents=True)
        inference_grant.issue(root, grant_id="g-11e-test", provider="claude",
                              task_id="t-11e", ttl_sec=600.0, max_uses=1,
                              note="тест 11E")
        consumed = inference_grant.consume(root, provider="claude", task_id="t-11e")

        assert consumed.remaining == 0
        assert inference_grant.find(root, provider="claude", task_id="t-11e") is None

    def test_ad_no_grant_in_binding_blocks_the_call(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Рабочий вызов без списанного разрешения не выполняется."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe, grant_id=""),
                  job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "разрешения" in text
        assert _argv_lines(journal) == []


# ═══════════ AG/AH. Запись: только конвейер и только внутрь попытки ══════════
class TestOutputWriting:

    def test_ag_pipeline_writes_the_artifact_not_the_model(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AG: файл создаёт конвейер; у модели инструмента записи нет."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, _text, _result = _run_stage(project)
        assert code == 0

        artifact = project["output_dir"] / "03_findings.json"
        data = json.loads(artifact.read_text(encoding="utf-8"))
        assert [f["id"] for f in data["findings"]] == ["F-001", "F-002"]
        # Инструмент записи запрещён поимённо — записать модель не могла.
        disallowed = next(a for a in _inference_argv(model=_POLICY_MODEL)
                          if a.startswith("--disallowed-tools="))
        assert "Write" in disallowed

    def test_ah_output_outside_attempt_is_refused(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AH: путь записи вне каталога попытки — отказ, а не тихая запись."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        outside = tmp_path / "outside" / "_output"
        outside.mkdir(parents=True)
        for name in ("02_text_analysis.json", "01_blocks_analysis.json"):
            (outside / name).write_text(
                (project["output_dir"] / name).read_text(encoding="utf-8"),
                encoding="utf-8",
            )

        from backend.app.services.llm import claude_runner

        code, text, _result = asyncio.run(
            claude_runner.run_findings_merge(
                project["project_info"], project["project_id"], None,
                output_dir=outside, version_dir=tmp_path / "outside",
                version_id="v1",
            )
        )

        assert code == 1
        assert "вне каталога попытки" in text
        assert not (outside / "03_findings.json").exists()

    def test_ai_full_prompt_is_not_in_the_run_report(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AI/AJ: в отчёт о прогоне не попадают ни промпт, ни ответ модели."""
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        assert _run_stage(project)[0] == 0
        raw = (project["output_dir"] / "findings_merge_provider_run.json").read_text(
            encoding="utf-8"
        )

        assert "Сечение кабеля 4 мм2" not in raw       # входные замечания
        assert "Сечение кабеля не покрывает" not in raw  # выходные замечания
        assert "### Merge Rules" not in raw            # тело инструкций
        report = json.loads(raw)
        assert len(report["prompt_sha256"]) == 64
        assert "result" not in report["provider_result"]
        assert report["provider_result"]["result_findings"] == 2

    def test_run_report_carries_input_facts_without_content(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Отчёт даёт счётчики и хэши входа — этого хватает для разбора."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        assert _run_stage(project)[0] == 0
        report = _run_report(project)

        assert report["input_contract"]["text_analysis"]["text_findings"] == 2
        assert report["input_contract"]["blocks_analysis"]["block_findings"] == 2
        assert report["input_coverage"]["passed"] is True
        # Список идентификаторов в отчёт не кладётся целиком — только счётчики
        # и то, чего не хватило.
        assert report["input_coverage"]["missing_before_inference"] == []


# ═══════════ AK/AL/AM/AN/AO. Утечки, канарейка, соседние этапы ═══════════════
class TestSafety:

    def test_ak_credential_sanitizer_rejects_leaks(self):
        """§23 AK: форма учётных данных в ответе — отказ валидатора."""
        from audit_worker.providers import inference

        result = inference.ProviderInferenceResult(
            provider="claude", model=_POLICY_MODEL, status=inference.STATUS_SUCCESS,
            exit_code=0,
            result={"findings": [], "note": "sk-ant-api03-AAAABBBBCCCCDDDD"},
        )
        report = inference.validate_inference(
            result, expected_provider="claude",
            expected_auth_mode=AUTH_MODE_AMBIENT_USER,
            required_result_fields=provider_transport.REQUIRED_RESULT_FIELDS,
            field_types=provider_transport.FIELD_TYPES,
            expected_model=_POLICY_MODEL,
            accepted_reported_models=(_POLICY_MODEL, _REPORTED_MODEL),
        )
        assert "no_credential_like" in report.failed_names

    def test_al_canary_literal_is_refused(self, monkeypatch, tmp_path, job_dir,
                                          project):
        """§23 AL: маркер контрольного файла в ответе — отказ этапа."""
        marker = "CANARY-11E-DO-NOT-READ-MARKER"
        answer = dict(_ANSWER)
        answer["findings"] = [dict(_ANSWER["findings"][0], description=marker)]
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal, answer=answer)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch,
                  _binding(job_dir, executable=exe, literals=(marker,)), job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "no_forbidden_literals" in text
        assert not (project["output_dir"] / "03_findings.json").exists()

    def test_cli_failure_detail_carries_the_providers_own_message(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Ошибка CLI доезжает СЛОВАМИ CLI, а не константой.

        Дефект вскрылся на единственном оплаченном вызове 11E: CLI вернул
        ошибку в 99 байт, а в журнал попытки уехал только её `sha256`.
        Разобрать причину оказалось нечем — бюджет вызовов исчерпан, повтор
        запрещён. Диагностическое сообщение провайдера данными заказчика не
        является и на границе, где деньги уже потрачены, теряться не должно.
        """
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal,
                           answer="API Error: 529 overloaded_error", exit_code=1)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        assert "CLI завершился ошибкой" in text
        assert "529" in text and "overloaded_error" in text
        assert not (project["output_dir"] / "03_findings.json").exists()

    def test_cli_failure_detail_is_bounded(self, monkeypatch, tmp_path, job_dir,
                                           project):
        """Сообщение обрезается: поле диагностики не канал для выгрузки входа."""
        from audit_worker.providers import claude_adapter

        long_message = "x" * 5000
        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal,
                           answer=long_message, exit_code=1)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        code, text, _result = _run_stage(project)

        assert code == 1
        detail = json.loads(
            (project["output_dir"] / "findings_merge_provider_run.json").read_text(
                encoding="utf-8"
            )
        )["provider_result"]["detail"]
        assert len(detail) < claude_adapter._CLI_FAILURE_DETAIL_MAX_CHARS + 200
        assert detail.endswith("…")

    def test_am_codex_adapter_refuses_explicit_model(self, tmp_path):
        """§23 AM: codex не притворяется, что умеет назначенную модель."""
        from audit_worker.providers.paths import provider_home

        ambient = tmp_path / "ambient"
        ambient.mkdir(parents=True, exist_ok=True)
        home = provider_home(tmp_path / "worker-root", "codex",
                             auth_mode=AUTH_MODE_AMBIENT_USER,
                             ambient_home=ambient)
        home.ensure_dirs()
        adapter = CodexProviderAdapter(home, inference_allowed=True)
        result = adapter.structured_inference(
            "промпт", purpose=_STAGE, model=_POLICY_MODEL,
            accepted_reported_models=(_POLICY_MODEL,),
        )
        assert result.ok is False

    def test_an_norm_verify_is_not_reachable_from_merge(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AN: свод не зовёт верификацию норм и не поднимает норм-MCP."""
        from backend.app.services.llm import claude_runner

        journal = tmp_path / "journal.txt"
        exe = _fake_claude(tmp_path / "bin" / "claude", journal)
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        def _guard(*args, **kwargs):
            raise AssertionError("norm_verify не имеет права быть вызван в 11E")

        monkeypatch.setattr(claude_runner, "run_norm_verify", _guard)

        assert _run_stage(project)[0] == 0
        assert "mcp__norms" not in _journal_text(journal)
        assert "--mcp-config" not in " ".join(_argv_lines(journal))

    def test_ao_no_downstream_artifacts_appear(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """§23 AO: после свода не появляется ни одного артефакта следующих этапов."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        assert _run_stage(project)[0] == 0

        produced = {p.name for p in project["output_dir"].iterdir()}
        for downstream in ("03_findings_review.json", "norm_checks.json",
                           "03a_norms_verified.json", "optimization.json",
                           "optimization_review.json"):
            assert downstream not in produced

    def test_binding_whitelist_is_stage_scoped(self, monkeypatch, tmp_path,
                                               job_dir, project):
        """Привязка под свод не открывает дорогу другим этапам."""
        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        binding = _binding(job_dir, executable=exe)
        _activate(monkeypatch, binding, job_dir)

        with pytest.raises(pipeline_bridge.ProviderBridgeError):
            pipeline_bridge.run_stage_inference(
                job_dir=job_dir, stage="optimization", prompt="x",
            )


# ══════════════════ Боевой раннер этапа поверх provider-режима ═══════════════
class TestProductionStageRunner:

    def test_production_runner_completes_over_provider(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Боевой раннер `stages/findings_merge/runner.py` доходит до конца.

        Это не дубль теста транспорта: раннер после вызова модели гонит все
        post-merge проходы (канон схемы, провенанс, объединение похожих,
        сплошная нумерация, подписи блоков). Если бы provider-маршрут отдавал
        результат в неожиданной форме, падение случилось бы именно здесь.
        """
        from backend.app.pipeline.context import PipelineStageContext
        from backend.app.pipeline.stages.findings_merge.runner import (
            run_findings_merge as run_stage,
        )
        from backend.app.services.common import audit_scope

        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        logged: list = []

        async def _log(message, level="info"):
            logged.append((level, message))

        async def _true():
            return True

        async def _wait(reason, output):
            return False

        ctx = PipelineStageContext(
            project_dir=project["project_dir"],
            project_id=project["project_id"],
            output_dir=project["output_dir"],
            log=_log,
            check_before_launch=_true,
            check_pause=_true,
            wait_for_rate_limit=_wait,
            record_cli_usage=lambda *a, **k: None,
            update_pipeline_log=lambda *a, **k: None,
            run_subprocess=None,
            project_info=project["project_info"],
            version_id="v1",
            job_id="11e",
        )

        async def _go():
            with audit_scope.bind_audit_scope(
                output_dir=project["output_dir"],
                version_dir=project["project_dir"],
                project_id=project["project_id"], version_id="v1",
            ):
                return await run_stage(ctx)

        result = asyncio.run(_go())

        assert result.success is True
        assert result.findings_count == 2
        data = json.loads(
            (project["output_dir"] / "03_findings.json").read_text(encoding="utf-8")
        )
        assert [f["id"] for f in data["findings"]] == ["F-001", "F-002"]

    def test_production_runner_reports_provider_failure_as_stage_failure(
        self, monkeypatch, tmp_path, job_dir, project
    ):
        """Отказ provider-маршрута доезжает до раннера как отказ этапа."""
        from backend.app.pipeline.context import PipelineStageContext
        from backend.app.pipeline.stages.findings_merge.runner import (
            run_findings_merge as run_stage,
        )
        from backend.app.services.common import audit_scope

        exe = _fake_claude(tmp_path / "bin" / "claude", tmp_path / "j.txt",
                           reported_model="claude-sonnet-5")
        _ambient_home(monkeypatch, tmp_path / "ambient")
        _activate(monkeypatch, _binding(job_dir, executable=exe), job_dir)

        async def _log(message, level="info"):
            return None

        async def _true():
            return True

        async def _wait(reason, output):
            return False

        ctx = PipelineStageContext(
            project_dir=project["project_dir"],
            project_id=project["project_id"],
            output_dir=project["output_dir"],
            log=_log,
            check_before_launch=_true,
            check_pause=_true,
            wait_for_rate_limit=_wait,
            record_cli_usage=lambda *a, **k: None,
            update_pipeline_log=lambda *a, **k: None,
            run_subprocess=None,
            project_info=project["project_info"],
            version_id="v1",
            job_id="11e",
        )

        async def _go():
            with audit_scope.bind_audit_scope(
                output_dir=project["output_dir"],
                version_dir=project["project_dir"],
                project_id=project["project_id"], version_id="v1",
            ):
                return await run_stage(ctx)

        result = asyncio.run(_go())

        assert result.success is False
        assert not (project["output_dir"] / "03_findings.json").exists()
