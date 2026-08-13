"""Этап 11D.1 — смысловая эквивалентность legacy и provider путей `text_analysis`.

Что здесь защищается.

11D перевёл боевой этап `text_analysis` с «модель сама читает MD через Read и
сама пишет артефакт через Write» на «конвейер читает → промпт inline →
ProviderAdapter без инструментов → конвейер пишет». Тесты 11D доказали, что
транспорт работает. Они НЕ доказывали, что модель получает то же инженерное
задание: сверка `semantic_preservation_report` берёт базой API-промпт ветки
OpenRouter, а не тот CLI-промпт, который этап заменил.

Здесь база сравнения другая и правильная для вопроса 11D.1 — **ветка B**,
`task_builder.prepare_text_analysis_task`, то есть буквально «старый Claude CLI
с файловыми инструментами». Утверждается четыре вещи:

  1. НИ ОДНА инженерная строка legacy-промпта не пропала вместе с транспортной
     оболочкой (тест I — самый сильный: построчное покрытие).
  2. Транспортная оболочка снята полностью и путей проекта не осталось.
  3. Тело документа доезжает до модели дословно и целиком.
  4. Смысл шкалы severity, который до 11D доходил до модели ТОЛЬКО через
     проектную память CLI (`CLAUDE.md` в cwd), теперь стоит в промпте явным
     текстом — а личный контекст остаётся исключённым.

НИ ОДИН тест этого файла не обращается к модели: промпты собираются офлайн,
подставного CLI здесь тоже нет — он не нужен.

Прогон:
    python -m pytest tests/test_11d1_text_analysis_semantic_equivalence.py -v
"""
from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")

from backend.app.pipeline.stages.text_analysis import provider_transport  # noqa: E402


# ═══════════════════════════════ Фикстуры ════════════════════════════════════

#: Строки, которые ветка C снимает как транспорт (`prompt_builder._CLI_PATTERNS`).
#: Дублируются здесь НАМЕРЕННО: тест обязан упасть, если фильтр расширят и он
#: начнёт уносить инженерный текст, а не молча согласиться с новым фильтром.
_TRANSPORT_LINE_MARKERS = (
    "Read tool", "Write tool", "Read file", "WRITE via Write",
    "read EACH one via Read", "Write JSON via Write tool",
    "After writing, output a brief summary",
    "DO NOT output to chat", "Do not output to chat",
)


@pytest.fixture()
def project(tmp_path: Path) -> dict:
    """Синтетический проект ЭОМ. Дисциплина настоящая — профиль берётся с диска."""
    vdir = tmp_path / "project" / "eom" / "v1"
    out = vdir / "_output"
    out.mkdir(parents=True)
    md = vdir / "11d1_document.md"
    md.write_text(
        "# Раздел ЭОМ — тестовый том\n\n"
        "## СТРАНИЦА 1\n\n"
        "### BLOCK [TEXT]: AAAA-BBBB-CCCC\n\n"
        "- Заземление выполнить в соответствии со СП 76.13330.2016.\n"
        "- В соответствии с ПУЭ п.7.35 использовать естественные заземлители.\n"
        "- Внутренний контур разместить на 400мм от пола и 10мм от стены.\n\n"
        "| Поз. | Наименование | Кол-во | Ед. |\n|---|---|---|---|\n"
        "| 1 | Полоса 40х4 | 60 | м |\n| 2 | Кронштейн | 200 | шт |\n",
        encoding="utf-8",
    )
    return {
        "version_dir": vdir,
        "output_dir": out,
        "md_path": md,
        "md_text": md.read_text(encoding="utf-8"),
        "project_id": "EOM/11d1-test",
        "project_info": {
            "project_id": "EOM/11d1-test",
            "name": "11d1-test",
            "section": "EOM",
            "md_file": md.name,
        },
    }


def _legacy_prompt(project: dict) -> str:
    """Боевой промпт ветки B — «старый Claude CLI + файловые инструменты»."""
    from backend.app.services.common import audit_scope
    from backend.app.pipeline.stages.prepare.task_builder import (
        prepare_text_analysis_task,
    )

    with audit_scope.bind_audit_scope(
        output_dir=project["output_dir"], version_dir=project["version_dir"],
        project_id=project["project_id"], version_id="v1",
    ):
        return prepare_text_analysis_task(project["project_info"], project["project_id"])


def _api_messages(project: dict) -> list[dict]:
    from backend.app.services.common import audit_scope
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

    with audit_scope.bind_audit_scope(
        output_dir=project["output_dir"], version_dir=project["version_dir"],
        project_id=project["project_id"], version_id="v1",
    ):
        return prompt_builder.build_text_analysis_messages(
            project["project_info"], project["project_id"]
        )


def _provider_built(project: dict) -> dict:
    return provider_transport.build_provider_prompt(_api_messages(project))


def _normalize(line: str) -> str:
    """Привести строку к виду, в котором её можно сравнивать между путями.

    Абсолютные пути в provider-промпте заменены плейсхолдером — значит и в
    legacy-строке их надо заменить тем же, иначе тест ловил бы транспорт вместо
    инженерии. Пробелы схлопываются: переносы строк в двух путях разные.
    """
    text, _ = provider_transport.strip_filesystem_references(line)
    return re.sub(r"\s+", " ", text).strip()


def _engineering_lines(legacy: str) -> list[str]:
    """Строки legacy-промпта, несущие инженерный смысл, а не транспорт."""
    out = []
    for raw in legacy.split("\n"):
        line = raw.strip()
        if not line:
            continue
        if any(marker.lower() in line.lower() for marker in _TRANSPORT_LINE_MARKERS):
            continue
        out.append(line)
    return out


# ═════════ A/M. Тело документа доезжает до модели дословно и целиком ═════════

class TestDocumentPayload:
    def test_a_provider_inlines_the_whole_md_verbatim(self, project):
        """A + M: тот же MD → тот же полезный текст, целиком и без правок."""
        built = _provider_built(project)
        assert project["md_text"] in built["prompt"]
        # Не «похоже, что весь», а именно весь: последняя строка тоже на месте.
        tail = project["md_text"].rstrip().split("\n")[-1]
        assert tail in built["prompt"]

    def test_a_document_is_not_touched_by_path_cleanup(self, project):
        """Зачистка путей применяется к ИНСТРУКЦИЯМ, тело документа не трогается."""
        built = _provider_built(project)
        body = built["prompt"].split(
            "===== SOURCE DOCUMENT (inlined by the pipeline) =====", 1)[1]
        body = body.split("===== END OF SOURCE DOCUMENT =====", 1)[0]
        assert provider_transport.FILESYSTEM_PLACEHOLDER not in body

    def test_m_document_chars_match_user_message(self, project):
        built = _provider_built(project)
        user = _api_messages(project)[1]["content"]
        assert built["document_chars"] == len(user)

    def test_legacy_delivers_document_by_path_not_inline(self, project):
        """Опора для сравнения: в ветке B тела документа в промпте НЕТ."""
        legacy = _legacy_prompt(project)
        assert project["md_text"] not in legacy
        assert str(project["md_path"]) in legacy


# ═══════ B/C/D/E/F/G/I. Инженерное содержание не потеряно ════════════════════

class TestEngineeringPreserved:
    def test_i_every_engineering_line_of_legacy_survives(self, project):
        """I — главный тест 11D.1.

        Каждая непустая НЕтранспортная строка legacy-промпта обязана найтись в
        provider-промпте. Это и есть «инженерное правило не уехало вместе с
        оболочкой Read/Write».
        """
        legacy = _legacy_prompt(project)
        provider = _normalize(_provider_built(project)["prompt"])
        missing = [
            line for line in _engineering_lines(legacy)
            if _normalize(line) and _normalize(line) not in provider
        ]
        assert missing == [], f"инженерные строки потеряны: {missing[:5]}"

    def test_b_discipline_profile_reaches_both_paths(self, project):
        """B: роль, чек-лист и таблица категорий дисциплины — в обоих путях."""
        from backend.app.services.common.discipline_service import load_discipline

        profile = load_discipline("EOM")
        assert profile is not None
        legacy = _legacy_prompt(project)
        provider = _provider_built(project)["prompt"]
        for name, text in (
            ("role", profile.role),
            ("checklist", profile.checklist),
            ("finding_categories", profile.finding_categories),
        ):
            head = _normalize(text.strip().split("\n")[0])
            assert head in _normalize(legacy), f"{name} нет в legacy"
            assert head in _normalize(provider), f"{name} нет в provider"

    def test_c_severity_rules_identical_in_both(self, project):
        """C: перечень значений, правило «только одно из 5» и критерии смежных."""
        legacy = _normalize(_legacy_prompt(project))
        provider = _normalize(_provider_built(project)["prompt"])
        for rule in (
            "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
            "severity — ONLY one of the 5 values",
            "Criteria for the «ПРОВЕРИТЬ ПО СМЕЖНЫМ» severity",
            "Assign «ПРОВЕРИТЬ ПО СМЕЖНЫМ» ONLY when:",
            "Do NOT assign «ПРОВЕРИТЬ ПО СМЕЖНЫМ» when:",
            "inflate severity",
        ):
            assert _normalize(rule) in legacy, f"нет в legacy: {rule}"
            assert _normalize(rule) in provider, f"нет в provider: {rule}"

    def test_c_severity_semantics_carried_into_provider_prompt(self, project):
        """C (правка 11D.1): смысл шкалы теперь в промпте, а не в памяти CLI.

        До 11D определения «Критическое — нельзя строить» доходили до модели
        только потому, что `claude -p` стартовал из корня репозитория и подбирал
        проектный CLAUDE.md. ProviderAdapter этот канал закрывает намеренно —
        значит определения обязаны стоять в самом промпте.
        """
        built = _provider_built(project)
        prompt = built["prompt"]
        assert "## Severity Semantics" in prompt
        for value in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
                      "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"):
            assert f"**{value}**" in prompt
        assert built["map"]["severity_semantics_applied"] is True
        # Блок стоит РЯДОМ со шкалой, а не в хвосте инструкций.
        assert built["map"]["severity_semantics_anchor"] == "## Output JSON Schema"
        assert prompt.index("## Severity Semantics") < prompt.index("## Output JSON Schema")

    def test_c_severity_semantics_is_symmetric(self):
        """Правка не имеет права толкать оценку вверх: проверить её прогоном на
        11D.1 нельзя, поэтому формулировка обязана быть симметричной."""
        text = provider_transport.SEVERITY_SEMANTICS
        assert "Do not soften it and\ndo not inflate it." in text
        for pushy in ("prefer", "err on the side", "when in doubt", "escalate"):
            assert pushy not in text.lower()

    def test_c_severity_semantics_has_no_project_specific_content(self):
        """§22: правка НЕ имеет права быть подгонкой под один документ."""
        text = provider_transport.SEVERITY_SEMANTICS
        for overfit in ("ОСУП", "ДСУП", "TN-S", "TN-C-S", "7.35", "заземлен", "133-23"):
            assert overfit not in text

    def test_d_absence_guard_reaches_both_paths_when_enabled(self, project, monkeypatch):
        """D: страж отсутствия по умолчанию выключен в ОБОИХ путях; включённый
        доходит до обоих. Асимметрии здесь нет и не должно появиться."""
        import backend.app.pipeline.stages.prepare.task_builder as task_builder

        assert task_builder._absence_guard_block() == ""

        monkeypatch.setattr(task_builder, "PIPELINE_ABSENCE_GUARD_ENABLED", True)
        anchor = _normalize(task_builder._ABSENCE_GUARD_TEXT.strip().split("\n")[0])
        assert anchor in _normalize(_legacy_prompt(project))
        assert anchor in _normalize(_provider_built(project)["prompt"])

    def test_e_md_prescan_reaches_both_paths(self, project, monkeypatch):
        """E: секция pre-scan подключена симметрично."""
        import backend.app.pipeline.stages.prepare.task_builder as task_builder
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
        import backend.app.pipeline.stages.text_analysis.md_prescan as md_prescan

        marker = "## Deterministic MD Pre-scan"
        monkeypatch.setattr(
            md_prescan, "build_prescan_prompt_section",
            lambda *a, **kw: f"{marker}: обязательные точки перепроверки\n\n- `x` [КРИТИЧЕСКОЕ] MD стр. 1",
        )
        assert marker in _legacy_prompt(project)
        assert marker in _provider_built(project)["prompt"]
        assert task_builder is not None and prompt_builder is not None

    def test_f_normative_reference_is_not_weaker_in_provider(self, project):
        """F: legacy давал ПУТЬ к норм-базе, provider вкладывает её ЦЕЛИКОМ."""
        from backend.app.services.common.discipline_service import load_discipline

        profile = load_discipline("EOM")
        norms = Path(profile.norms_reference_path).read_text(encoding="utf-8")
        legacy = _legacy_prompt(project)
        provider = _provider_built(project)["prompt"]

        assert profile.norms_reference_path in legacy   # только путь
        assert norms not in legacy                      # содержимого нет
        assert norms in provider                        # содержимое целиком
        assert profile.norms_reference_path not in provider  # пути нет

    def test_g_output_schema_preserved(self, project):
        """G: контракт выхода — тот же, поле в поле."""
        provider = _normalize(_provider_built(project)["prompt"])
        legacy = _normalize(_legacy_prompt(project))
        for field in ('"stage": "02_text_analysis"', '"text_source": "md"',
                      '"project_params"', '"normative_refs_found"',
                      '"text_findings"', '"items_verified_from_blocks"',
                      '"norm_quote"', '"related_block_ids"'):
            assert _normalize(field) in legacy, f"нет в legacy: {field}"
            assert _normalize(field) in provider, f"нет в provider: {field}"


# ═══════════ H/J/K/L. Транспортная оболочка снята полностью ══════════════════

class TestTransportRemoved:
    def test_h_tool_instructions_present_in_legacy_absent_in_provider(self, project):
        legacy = _legacy_prompt(project)
        provider = _provider_built(project)["prompt"]
        assert "Read tool" in legacy and "Write tool" in legacy
        for name, needle in provider_transport.FORBIDDEN_TRANSPORT_MARKERS:
            assert needle not in provider, f"транспорт просочился: {name}"

    def test_j_no_filesystem_dependency_in_provider_instructions(self, project):
        built = _provider_built(project)
        assert built["absolute_paths_remaining_in_instructions"] == 0
        head = built["prompt"].split("===== SOURCE DOCUMENT", 1)[0]
        assert str(project["version_dir"]) not in head
        assert str(project["output_dir"]) not in head

    def test_k_no_write_requirement(self, project):
        provider = _provider_built(project)["prompt"]
        assert "02_text_analysis.json" not in provider
        assert "The pipeline itself parses your reply" in provider

    def test_l_no_tool_requirement(self, project):
        provider = _provider_built(project)["prompt"]
        assert "You have NO tools in this run" in provider

    def test_l_tool_restriction_does_not_silence_absence_findings(self):
        """Правка 11D.1: «нет инструментов» не имеет права читаться как
        «не сообщай о том, чего в документации не хватает».

        Замечание класса «в проекте не указано X» — штатный и самый частый
        результат этого этапа. Прежняя фраза «do not report that a file is
        missing» стояла ПОСЛЕДНЕЙ в промпте и допускала расширительное чтение.
        """
        text = provider_transport.TRANSPORT_CONTRACT
        assert "do not report that a file is missing" not in text
        assert "TOOL ACCESS ONLY" in text
        assert "must be reported as usual" in text


# ═════════════════ N/O. Порядок секций и отпечаток промпта ═══════════════════

class TestOrderAndFingerprint:
    def test_n_section_order_matches_legacy(self, project):
        """N: порядок инженерных секций не переставлен."""
        legacy = _legacy_prompt(project)
        provider = _provider_built(project)["prompt"]
        order = ["## Role", "## Input Data", "## Task",
                 "## Finding Categories", "## Output JSON Schema",
                 "## Normative Accuracy (norm_quote)", "## Rules",
                 "## Criteria for the «ПРОВЕРИТЬ ПО СМЕЖНЫМ» severity"]
        legacy_pos = [legacy.index(s) for s in order]
        provider_pos = [provider.index(s) for s in order]
        assert legacy_pos == sorted(legacy_pos)
        assert provider_pos == sorted(provider_pos)

    def test_n_document_and_transport_contract_are_last(self, project):
        provider = _provider_built(project)["prompt"]
        assert provider.index("## Rules") < provider.index("===== SOURCE DOCUMENT")
        assert provider.index("===== END OF SOURCE DOCUMENT") < provider.index(
            "## OUTPUT TRANSPORT")

    def test_o_prompt_is_deterministic(self, project):
        """O: одна и та же сборка даёт побайтово тот же промпт.

        Отпечаток промпта уезжает в отчёт о прогоне как доказательство; если бы
        сборка была недетерминированной, доказывать им было бы нечего.
        """
        first = _provider_built(project)["prompt"]
        second = _provider_built(project)["prompt"]
        assert hashlib.sha256(first.encode()).hexdigest() == \
               hashlib.sha256(second.encode()).hexdigest()

    def test_o_engineering_markers_survive(self, project):
        api = _api_messages(project)
        api_prompt = api[0]["content"] + "\n\n" + api[1]["content"]
        report = provider_transport.semantic_preservation_report(
            api_prompt=api_prompt,
            provider_prompt=_provider_built(project)["prompt"],
        )
        assert report["engineering_lost"] == []
        assert report["transport_markers_leaked"] == []
        assert report["passed"] is True


# ═════════════ P/Q/R. Прежний путь, личный контекст, отчёты ══════════════════

class TestBoundaries:
    def test_p_legacy_path_unchanged_by_11d1(self, project):
        """P: правки 11D.1 живут в provider-транспорте и до ветки B не доходят."""
        legacy = _legacy_prompt(project)
        assert "## Severity Semantics" not in legacy
        assert "## OUTPUT TRANSPORT" not in legacy
        assert "## Input Data (this run)" not in legacy
        assert "TOOL ACCESS ONLY" not in legacy
        # Ветка B по-прежнему требует файловых инструментов — её не «чинили».
        assert "WRITE via Write tool" in legacy
        assert "READ via Read tool" in legacy

    def test_p_shared_template_untouched(self):
        """Общий шаблон этапа не правился: иначе изменилось бы и поведение
        центра, где боевой путь — не provider."""
        from backend.app.core.config import TEXT_ANALYSIS_TASK_TEMPLATE
        from backend.app.pipeline.stages.prepare.task_builder import (
            load_template_for_llm,
        )
        template = load_template_for_llm(TEXT_ANALYSIS_TASK_TEMPLATE)
        assert "Severity Semantics" not in template
        assert "TOOL ACCESS ONLY" not in template

    def test_q_personal_context_stays_excluded(self, project):
        """Q: перенесены ОПРЕДЕЛЕНИЯ, а не файл проектной памяти.

        Блок severity — литеральная константа модуля. Ни чтения `CLAUDE.md`, ни
        любого другого обращения к файловой системе за контекстом в транспорте
        нет: вернуть личный контекст запрещено §17.
        """
        source = Path(provider_transport.__file__).read_text(encoding="utf-8")
        assert "CLAUDE.md" not in provider_transport.SEVERITY_SEMANTICS
        for forbidden in ("read_text(", "open(", "Path("):
            assert forbidden not in source, f"транспорт полез в файлы: {forbidden}"
        provider = _provider_built(project)["prompt"]
        for leak in ("settings.json", ".claude/", "hooks", "skills"):
            assert leak not in provider

    def test_r_build_map_carries_no_content(self, project):
        """R: карта сборки, которая уезжает в отчёт, не содержит ни промпта, ни
        документа — только счётчики и флаги."""
        built = _provider_built(project)
        blob = repr(built["map"])
        assert project["md_text"][:80] not in blob
        assert "Severity Semantics (what each value means)" not in blob
        for value in built["map"].values():
            assert isinstance(value, (int, bool, str))
            if isinstance(value, str):
                assert len(value) < 120

    def test_r_soft_contract_report_carries_no_content(self):
        report = provider_transport.soft_contract_report(
            {"text_findings": [{"finding": "секретный текст документа"}],
             "text_source": "md"}
        )
        assert "секретный текст документа" not in repr(report)
