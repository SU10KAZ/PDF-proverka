"""Транспорт `block_analysis` через ProviderAdapter (этап 11F).

Чем этот путь отличается от прежней ветки Claude CLI — не «другим транспортом»,
а РАСПРЕДЕЛЕНИЕМ ОБЯЗАННОСТЕЙ, ровно как это уже сделано для `text_analysis`
(11D) и `findings_merge` (11E).

Было (`gemma_findings_only.call_claude_cli_for_block`):

    конвейер даёт модели ПУТЬ к PNG и ПУТЬ к временному файлу
      → модель сама читает картинку инструментом `Read`
      → модель сама пишет результат инструментом `Write`
      → конвейер читает файл

То есть ради одной картинки модель получала свободный доступ к файловой
системе воркера, а вызов шёл прямым `create_subprocess_exec` мимо
провайдерского слоя: без авторизации по режиму, без нейтрализации личного
контекста, без журнала вызовов и без сверки фактической модели.

Стало:

    конвейер сам читает PNG → строит промпт → отдаёт БАЙТЫ изображения
    провайдеру → модель возвращает structured JSON → конвейер пишет артефакт

Изображение уходит content-блоком `type=image` в теле запроса
(`--input-format stream-json`), инструментов у модели ноль, каталога вложений
не существует, путь к кропу модели не сообщается вовсе.

Промпт берётся у БОЕВОГО сборщика Stage 01 (`build_system_prompt` +
`build_single_block_user_text` из `gemma_findings_only`) — второй промпт ради
нового транспорта означал бы два расходящихся аудита. Добавляется ровно одно:
`SEVERITY_SEMANTICS`, та же константа, что закрыла дефект 11D.1 для текстовых
этапов. Определения шкалы важности нет ни в одном файле `prompts/`, и без неё
модель на воркере (где `CLAUDE.md` подавлен) оценивала бы тяжесть замечаний по
своему усмотрению.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from backend.app.pipeline.stages.text_analysis.provider_transport import (
    SEVERITY_SEMANTICS,
)

#: Идентификатор модели для журналов и провенанса, когда работает мост.
#:
#: Настоящую строку модели знает только локальная политика воркера, и до
#: `stage_models.json` ей дела нет. Ставить сюда `claude-opus-5` было бы
#: неправдой: конфигурация этап не выбирала. Значение намеренно НЕ похоже на
#: реальный id — так его нельзя перепутать с моделью в отчёте.
PROVIDER_BLOCK_MODEL_ID = "provider/worker-policy"

#: Поля, которые обязана вернуть модель. Совпадают с контрактом findings-only
#: схемы Stage 01: остальной конвейер читает именно `findings`.
REQUIRED_RESULT_FIELDS: tuple[str, ...] = ("findings",)

FIELD_TYPES: dict[str, Any] = {"findings": list}

#: MIME-тип кропов Stage 01. Кроп всегда PNG (`crop_blocks` рендерит именно его),
#: но тип передаётся явно: провайдер не обязан угадывать формат по расширению.
CROP_MEDIA_TYPE = "image/png"

#: Потолок размера одного кропа. Не «на всякий случай»: кроп уезжает в тело
#: запроса base64, то есть +33% к размеру, и блок на 20 МБ означал бы вызов,
#: который заведомо не пройдёт, но будет оплачен временем ожидания.
MAX_CROP_BYTES = 8 * 1024 * 1024


class BlockInputError(RuntimeError):
    """Вход блока непригоден. Вызова модели не будет."""


def read_crop(blocks_dir: Path, file_name: str) -> bytes:
    """Прочитать PNG кропа. Отсутствие файла — отказ, а не пустой вызов.

    Отдельная функция ради одного: путь собирается ЗДЕСЬ, из каталога кропов и
    имени файла из индекса, и никуда дальше не уезжает. Модель пути не видит.
    """
    path = (Path(blocks_dir) / str(file_name)).resolve()
    root = Path(blocks_dir).resolve()
    if root not in path.parents:
        raise BlockInputError(
            f"кроп {file_name!r} разрешается за пределы каталога блоков"
        )
    if not path.is_file():
        raise BlockInputError(f"кроп не найден: {path.name}")
    blob = path.read_bytes()
    if not blob:
        raise BlockInputError(f"кроп пуст: {path.name}")
    if len(blob) > MAX_CROP_BYTES:
        raise BlockInputError(
            f"кроп {path.name} — {len(blob)} байт, потолок {MAX_CROP_BYTES}"
        )
    return blob


def build_provider_prompt(
    *,
    system_prompt: str,
    user_text: str,
) -> dict[str, Any]:
    """Собрать промпт вызова и карту его состава.

    Возвращает и сам промпт, и `map` — что из чего сложилось. Карта уезжает в
    отчёт о прогоне: по ней видно, что инженерная часть не потерялась, при этом
    сам текст (замечания по документу заказчика) в отчёт не попадает.
    """
    system = str(system_prompt or "").strip()
    payload = str(user_text or "").strip()
    severity = SEVERITY_SEMANTICS.strip()
    instructions = "\n\n".join([
        system,
        severity,
        _OUTPUT_CONTRACT.strip(),
    ])
    prompt = instructions + "\n\n" + payload
    return {
        "prompt": prompt,
        "system_chars": len(instructions),
        "payload_chars": len(payload),
        "prompt_chars": len(prompt),
        "map": {
            "stage01_system_prompt_chars": len(system),
            "severity_semantics_chars": len(severity),
            "output_contract_chars": len(_OUTPUT_CONTRACT.strip()),
            "block_payload_chars": len(payload),
            "image_delivery": "content-block base64 в теле запроса (stdin stream-json)",
            "tools": 0,
        },
    }


#: Контракт ответа. Формулируется здесь, а не в общем промпте Stage 01, потому
#: что прежняя ветка просила модель ЗАПИСАТЬ файл, а здесь она обязана вернуть
#: объект. Это единственное содержательное расхождение с legacy-промптом, и оно
#: транспортное, а не смысловое.
_OUTPUT_CONTRACT = """
ФОРМАТ ОТВЕТА

Верни РОВНО ОДИН объект JSON и ничего кроме него — без пояснений до и после,
без обрамления markdown.

{"findings": [ ... ]}

Если замечаний по этому блоку нет — верни {"findings": []}. Пустой список это
законный ответ; выдумывать замечание, чтобы список не был пустым, запрещено.

Изображение блока приложено к этому сообщению. Никаких файлов читать не нужно
и нечем: инструментов у тебя нет.
"""


def result_findings(payload: Any) -> list[dict[str, Any]]:
    """Достать список замечаний из ответа модели. Чужая форма — пустой список."""
    if not isinstance(payload, dict):
        return []
    findings = payload.get("findings")
    if not isinstance(findings, list):
        return []
    return [f for f in findings if isinstance(f, dict)]


def soft_contract_report(payload: Any) -> dict[str, Any]:
    """Мягкие признаки качества ответа — для отчёта, не для отказа."""
    findings = result_findings(payload)
    with_severity = sum(1 for f in findings if str(f.get("severity") or "").strip())
    with_category = sum(1 for f in findings if str(f.get("category") or "").strip())
    with_value = sum(1 for f in findings if str(f.get("value_found") or "").strip())
    return {
        "findings": len(findings),
        "with_severity": with_severity,
        "with_category": with_category,
        "with_value_found": with_value,
    }


def provider_facts(result: Any) -> dict[str, Any]:
    """Публичные факты о вызове: всё, кроме содержимого ответа.

    Ответ модели — это замечания по документу заказчика. В отчёт о прогоне,
    который уезжает центру и разбирается руками, он не кладётся: там остаются
    отпечаток и счётчики.
    """
    facts = {k: v for k, v in result.as_dict().items() if k != "result"}
    payload = result.result if isinstance(result.result, dict) else {}
    facts["result_keys"] = sorted(payload.keys())
    facts["result_findings"] = len(result_findings(payload))
    return facts


def failure_detail(outcome: Any) -> str:
    """Почему вызов не принят — одной строкой, без содержимого ответа."""
    result = outcome.provider_result
    validation = outcome.validation.as_dict() if outcome.validation else None
    failed: Optional[Any] = (validation or {}).get("failed") if validation else None
    if not outcome.performed:
        return (
            "повтор невозможен: результат этого вызова уже записан в журнал "
            f"попытки и неуспешен (проверка: {failed}). Новая попытка требует "
            "нового attempt_id и новой единицы разрешения"
        )
    return (
        f"provider_result.status={result.status!r} "
        f"error_code={result.error_code!r} detail={result.detail!r} "
        f"validation_failed={failed}"
    )
