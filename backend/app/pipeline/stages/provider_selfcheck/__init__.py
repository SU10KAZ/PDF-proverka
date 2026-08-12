"""Этап `provider_selfcheck` — синтетическая проверка сквозного пути к модели.

Зачем отдельный этап, а не скрипт.

Доказательство, ради которого этап существует, звучит так: «задание, пришедшее
воркеру, доходит до модели ЧЕРЕЗ настоящий конвейер и через провайдерский слой,
а результат возвращается проверенным». Скрипт, который зовёт `ProviderAdapter`
напрямую, доказал бы только работоспособность адаптера — то есть ровно то, что
уже доказал контрольный запрос этапа 11b. Значение имеет именно ПУТЬ:

    задание audit_pipeline_v1
      → Executor.run_audit_attempt
      → audit_runner (argv/env/спека)
      → remote_audit_runner (снимки центра, изоляция)
      → ЭТОТ этап
      → claude_runner._run_cli — штатная точка выбора CLI в конвейере
      → pipeline_bridge → ProviderAdapter → claude|codex

Каждое звено здесь настоящее, включая проверку снимков центра и раскладку
каталогов попытки. Синтетично только СОДЕРЖИМОЕ задания.

Почему содержимое именно такое. Модель просят не повторить строку, а найти
числовое противоречие между двумя утверждениями исходного текста и вернуть его
структурой. Повтор строки не отличает работающую модель от подставленного
эха; поиск противоречия — отличает, оставаясь при этом задачей на десяток
секунд и на сотню токенов.

Материал берётся из Markdown ВЕРСИИ ПРОЕКТА — тем же резолвером
(`resolve_v2_source_files`), которым его берёт настоящий аудит. Никакого
второго источника входных данных у этапа нет.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Optional

#: Имя этапа. Оно же — ключ белого списка привязки провайдера и ключ в
#: `pipeline_log.json`.
STAGE_NAME = "provider_selfcheck"

#: Артефакт этапа. Единственный обязательный результат прогона.
ARTIFACT_NAME = "provider_selfcheck.json"

#: Поля, которые обязана вернуть модель.
REQUIRED_RESULT_FIELDS: tuple[str, ...] = (
    "contradiction_found", "values", "unit", "source_quotes", "marker",
)

#: Типы этих полей. Проверяются отдельно от наличия: поле-строка вместо
#: поля-списка — это не «поле есть», это другой контракт.
FIELD_TYPES: dict[str, Any] = {
    "contradiction_found": bool,
    "values": list,
    "unit": str,
    "source_quotes": list,
    "marker": str,
}

#: Детерминированный маркер задания. Не «доказательство инференса» (его даёт
#: найденное противоречие), а признак того, что модель отвечала ИМЕННО на этот
#: запрос, а не отдала кэш прошлого.
EXPECTED_MARKER = "AUDIT_PIPELINE_11C_OK"

#: Смысловые ожидания. Числа заданы явно: «модель что-то нашла» — не результат.
EXPECTED_SEMANTICS: dict[str, Any] = {
    "contradiction_found": True,
    "marker": EXPECTED_MARKER,
}

#: Заголовок раздела фикстуры в Markdown версии. Этап берёт ТОЛЬКО его: весь
#: документ модели не показывают — ни к чему, и это лишние токены подписки.
FIXTURE_HEADING = "## Контрольный фрагмент 11C"

#: Сколько символов фрагмента максимум уходит в модель. Рубеж, а не оценка:
#: фикстура может однажды разрастись, а расход подписки расти не должен.
MAX_FRAGMENT_CHARS = 1200

_PROMPT_TEMPLATE = """Ты проверяешь фрагмент проектной документации.

ФРАГМЕНТ:
<<<
{fragment}
>>>

ЗАДАЧА: найди числовое противоречие между расходом насоса, указанным в тексте,
и расходом того же насоса в таблице оборудования.

Ответь ОДНИМ объектом JSON и ничем больше. Схема:
{{
  "contradiction_found": true|false,
  "values": [<число из текста>, <число из таблицы>],
  "unit": "<единица измерения>",
  "source_quotes": ["<цитата из текста>", "<цитата из таблицы>"],
  "marker": "{marker}"
}}

Правила:
- "values" — ровно два числа в том порядке, в каком они встречаются во фрагменте;
- "source_quotes" — дословные подстроки фрагмента, по одной на каждое значение;
- "marker" — верни ровно строку {marker};
- никакого текста вне JSON, никаких пояснений, никаких блоков ```.
"""


class ProviderSelfcheckError(RuntimeError):
    """Этап не может быть выполнен. Тихого успеха здесь не бывает."""


def extract_fragment(md_text: str) -> str:
    """Достать контрольный фрагмент из Markdown версии.

    Отсутствие раздела — ошибка, а не «возьмём весь документ». Взять весь
    документ значило бы отправить в модель проектную документацию там, где
    задача — проверить канал.
    """
    text = str(md_text or "")
    start = text.find(FIXTURE_HEADING)
    if start < 0:
        raise ProviderSelfcheckError(
            f"в Markdown версии нет раздела {FIXTURE_HEADING!r}: "
            "синтетическая фикстура не подготовлена"
        )
    rest = text[start + len(FIXTURE_HEADING):]
    # До следующего заголовка того же уровня либо до конца.
    end = rest.find("\n## ")
    fragment = (rest[:end] if end >= 0 else rest).strip()
    if not fragment:
        raise ProviderSelfcheckError("контрольный фрагмент пуст")
    if len(fragment) > MAX_FRAGMENT_CHARS:
        raise ProviderSelfcheckError(
            f"контрольный фрагмент длиннее {MAX_FRAGMENT_CHARS} символов "
            f"({len(fragment)}): в модель уходит только маленькая фикстура"
        )
    return fragment


def build_prompt(fragment: str) -> str:
    return _PROMPT_TEMPLATE.format(fragment=fragment, marker=EXPECTED_MARKER)


def read_version_markdown(version_dir: Path, document_code: Optional[str] = None) -> str:
    """Markdown версии — тем же резолвером, что и у настоящего аудита."""
    from backend.app.services.storage.projects_v2_source_resolver import (
        resolve_v2_source_files,
    )

    sources = resolve_v2_source_files(version_dir, document_code)
    if not sources.md_path or not Path(sources.md_path).is_file():
        raise ProviderSelfcheckError(
            f"Markdown версии не найден в {version_dir}: этап не запускается "
            "(запрет фолбэка на extracted_text распространяется и сюда)"
        )
    return Path(sources.md_path).read_text(encoding="utf-8")


def expected_values(fragment: str) -> list[float]:
    """Числа, которые ДОЛЖНА была найти модель — вычислены детерминированно.

    Существует ради одного: сверить ответ модели с фрагментом, не полагаясь на
    сам ответ. «Модель сказала, что нашла противоречие» и «во фрагменте оно
    действительно такое» — разные утверждения, и второе проверяется здесь без
    единого обращения к модели.
    """
    numbers = [
        float(value.replace(",", "."))
        for value in re.findall(r"\d+(?:[.,]\d+)?", fragment)
    ]
    return numbers


async def run_stage(
    *,
    job_dir: Path,
    version_dir: Path,
    result_dir: Path,
    project_id: str,
    document_code: Optional[str],
    job_id: str,
    attempt_id: str,
    timeout_sec: float = 300.0,
) -> dict[str, Any]:
    """Выполнить этап. Возвращает содержимое артефакта.

    Вызов модели идёт через `claude_runner._run_cli` — штатную точку конвейера,
    а не мимо неё. Это существенно: перехват провайдерского моста стоит именно
    там, и обходной вызов доказывал бы работу моста, но не работу конвейера
    через мост.
    """
    from audit_worker.providers import pipeline_bridge
    from backend.app.services.llm import claude_runner

    started = time.time()
    md_text = read_version_markdown(Path(version_dir), document_code)
    fragment = extract_fragment(md_text)
    prompt = build_prompt(fragment)

    exit_code, text, _cli = await claude_runner._run_cli(   # noqa: SLF001 — штатная точка
        prompt,
        tools="",
        timeout=int(timeout_sec),
        stage=STAGE_NAME,
        project_id=project_id,
    )

    outcome = pipeline_bridge.stored_outcome(
        stage=STAGE_NAME,
        prompt=prompt,
        required_result_fields=REQUIRED_RESULT_FIELDS,
        field_types=FIELD_TYPES,
        expected_semantics=EXPECTED_SEMANTICS,
        claim_task_id=str(job_id),
        claim_attempt_id=str(attempt_id),
    )
    if outcome is None:
        raise ProviderSelfcheckError(
            "результат вызова не найден в журнале попытки: этап не может "
            "подтвердить, что модель отвечала именно на этот запрос"
        )

    provider_result = outcome.provider_result
    payload = provider_result.result if isinstance(provider_result.result, dict) else {}
    # Независимая сверка чисел: значения из ответа обязаны встречаться во
    # фрагменте. Проверка не спрашивает модель — она считает по тексту.
    fragment_numbers = expected_values(fragment)
    reported = [
        float(value) for value in (payload.get("values") or [])
        if isinstance(value, (int, float))
    ]
    numbers_grounded = bool(reported) and all(
        any(abs(value - candidate) < 1e-6 for candidate in fragment_numbers)
        for value in reported
    )
    quotes_grounded = bool(payload.get("source_quotes")) and all(
        isinstance(quote, str) and quote.strip() and quote.strip() in fragment
        for quote in (payload.get("source_quotes") or [])
    )

    artifact = {
        "stage": STAGE_NAME,
        "job_id": str(job_id),
        "attempt_id": str(attempt_id),
        "project_id": str(project_id),
        "started_at": started,
        "finished_at": time.time(),
        "cli_exit_code": int(exit_code),
        "fragment_sha256": pipeline_bridge.sha256_text(fragment),
        "fragment_chars": len(fragment),
        "prompt_chars": len(prompt),
        "expected_marker": EXPECTED_MARKER,
        "grounding": {
            "numbers_grounded": numbers_grounded,
            "quotes_grounded": quotes_grounded,
            "fragment_numbers": fragment_numbers,
            "reported_values": reported,
        },
        **outcome.as_dict(),
    }
    result_dir = Path(result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)
    (result_dir / ARTIFACT_NAME).write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # Текст ответа в артефакт НЕ кладётся: отпечаток есть в `provider_result`,
    # а сырой ответ живёт только в журнале вызовов внутри каталога попытки.
    return artifact


def artifact_is_successful(artifact: dict[str, Any]) -> tuple[bool, str]:
    """Успех этапа = ответ модели + проверка контракта + сверка с фрагментом."""
    validation = artifact.get("validation") or {}
    grounding = artifact.get("grounding") or {}
    provider = artifact.get("provider_result") or {}
    problems: list[str] = []
    if provider.get("status") != "success":
        problems.append(f"provider_result.status={provider.get('status')!r}")
    if not validation.get("passed"):
        problems.append(f"проверка контракта: {validation.get('failed')}")
    if not grounding.get("numbers_grounded"):
        problems.append("числа ответа не найдены во фрагменте")
    if not grounding.get("quotes_grounded"):
        problems.append("цитаты ответа не найдены во фрагменте")
    return (not problems), "; ".join(problems)
