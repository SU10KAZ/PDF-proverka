"""Общий provider-транспорт для этапов «messages → один JSON-артефакт» (11F).

Зачем общий модуль, а не третья копия `_run_*_via_provider`.

`text_analysis` (11D) и `findings_merge` (11E) получили каждый свой транспорт —
и правильно: у первого вход это документ целиком с нарезкой и вычисткой путей,
у второго — сведение двух артефактов с проверкой полноты входа. У них разные
инварианты, и общий код их бы стёр.

А вот `optimization`, `optimization_critic` и `optimization_corrector` устроены
одинаково до неразличимости: боевой сборщик отдаёт `messages`, модель обязана
вернуть ОДИН объект JSON, конвейер пишет его в известный файл. Писать это
трижды значит трижды же ошибиться в одном и том же месте.

Что делает модуль и, главное, чего он НЕ делает:

  * НЕ строит промпт заново. Инженерная часть берётся у боевого сборщика
    `prompt_builder.build_*_messages` — того же, которым идёт HTTP-ветка;
  * НЕ меняет бизнес-логику этапа. Он транспорт: читает готовые messages,
    снимает с них файловые инструкции, добавляет шкалу severity и контракт
    ответа, зовёт мост, проверяет ответ, пишет артефакт;
  * НЕ имеет ни одной ветки обратно к прежнему транспорту. Мост активен только
    когда исполнитель воркера выписал привязку, УЖЕ списав разрешение
    оператора; уйти в этот момент на `claude -p` по PATH значило бы выполнить
    неавторизованный вызов из-под изолированного HOME и показать это как
    обычную ошибку этапа.

Почему файловые инструкции снимаются. Боевые шаблоны написаны под CLI с
инструментами: «READ via Read tool: …», «WRITE via Write tool:
{OUTPUT_PATH}/optimization.json». У вызова через провайдера инструментов ноль,
и такие строки для модели не задание, а описание невозможного — она либо
попытается их исполнить, либо решит, что от неё ждут описания действий вместо
результата.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, Optional

from backend.app.pipeline.stages.text_analysis.provider_transport import (
    SEVERITY_SEMANTICS,
)

#: Чем заменяется абсолютный путь в инструкциях.
#:
#: НЕ «(not available in this run)», как у text_analysis. Там плейсхолдер
#: честен: путь указывал на файл, которого у модели действительно нет. Здесь он
#: указывает на артефакт, который конвейер ВЛОЖИЛ в это же сообщение ниже, и
#: сказать модели «недоступно» значит соврать ей о собственном входе: критик
#: оптимизации, прочитав «document graph недоступен», обязан по своему же
#: критерию ставить `no_traceability` — то есть отказ, вызванный подсказкой.
INLINED_PLACEHOLDER = "(вложено в это сообщение ниже)"

#: Потолок вложений одного вызова: сколько картинок и сколько байт суммарно.
#:
#: `_get_plan_images` отбирает листы-планы БЕЗ какого-либо ограничения — на
#: версии с тридцатью планами это тридцать PNG по несколько сот килобайт, то
#: есть больше десяти мегабайт base64 в ОДНОЙ строке stdin. Дальше два исхода,
#: и оба необратимые: провайдер отвергает запрос (вызов оплачен, повтор
#: запрещён) либо принимает и считает по объёму. Отказ ДО заявки в журнале
#: дешевле обоих.
MAX_ATTACHED_IMAGES = 12
MAX_ATTACHED_BYTES = 6 * 1024 * 1024

#: Абсолютные пути в инструкциях. Тот же рубеж, что и у text_analysis: модель
#: не должна получать путь проекта — ни как задание, ни как справку.
_ABS_PATH_RE = re.compile(r"(?:^|[\s\"'`(\[])(/[A-Za-z0-9_][^\s\"'`)\]]{3,})")

#: Строки-инструкции файловой работы. Снимаются целиком, а не «переписываются»:
#: любая попытка переформулировать чужой шаблон на ходу — это правка промпта во
#: время прогона, что §16 задания 11F прямо запрещает.
_FILE_INSTRUCTION_MARKERS = (
    "via Read tool",
    "via Write tool",
    "Read tool:",
    "Write tool:",
    "READ via",
    "WRITE via",
    "Write JSON",
    "через инструмент Read",
    "через инструмент Write",
)


def strip_file_instructions(text: str) -> tuple[str, int]:
    """Убрать строки, требующие файловой работы. Возвращает (текст, сколько убрано)."""
    kept: list[str] = []
    removed = 0
    for line in str(text or "").splitlines():
        if any(marker in line for marker in _FILE_INSTRUCTION_MARKERS):
            removed += 1
            continue
        kept.append(line)
    return "\n".join(kept), removed


class ProviderStageRefusal(RuntimeError):
    """Этап не выполняется. Ни одна ветка отсюда не ведёт к прежнему транспорту."""


def count_absolute_paths(text: str) -> int:
    return len(_ABS_PATH_RE.findall(str(text or "")))


def _replace_paths(text: str) -> tuple[str, int]:
    """Заменить абсолютные пути на честный маркер. Возвращает (текст, сколько)."""
    replaced = 0

    def _sub(match: "re.Match[str]") -> str:
        nonlocal replaced
        replaced += 1
        return match.group(0).replace(match.group(1), INLINED_PLACEHOLDER)

    return _ABS_PATH_RE.sub(_sub, str(text or "")), replaced


def extract_images(messages: Iterable[dict]) -> list[tuple[str, bytes]]:
    """Достать изображения из боевых messages.

    Боевой сборщик `build_optimization_messages` кладёт в user-сообщение
    картинки листов-планов (`make_image_content` → `{"type":"image_url",
    "image_url":{"url":"data:image/png;base64,…"}}`). Отбросить их молча — ровно
    та потеря графики, ради запрета которой в общем пути моста стоит явный
    отказ: этап отчитался бы успехом, проанализировав чертежи, которых не видел.
    """
    import base64
    import binascii

    out: list[tuple[str, bytes]] = []
    for message in messages or []:
        content = (message or {}).get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") != "image_url":
                continue
            url = str(((part.get("image_url") or {}).get("url")) or "")
            if not url.startswith("data:"):
                raise ProviderStageRefusal(
                    "изображение задано ссылкой, а не данными: провайдерский "
                    "путь не ходит в сеть за содержимым промпта"
                )
            header, _, payload = url.partition(",")
            media_type = header[5:].split(";", 1)[0] or "image/png"
            try:
                out.append((media_type, base64.b64decode(payload, validate=True)))
            except (binascii.Error, ValueError) as exc:
                raise ProviderStageRefusal(
                    f"изображение не разбирается из data-URL: {exc}"
                ) from None
    if len(out) > MAX_ATTACHED_IMAGES:
        raise ProviderStageRefusal(
            f"вложений {len(out)} > потолка {MAX_ATTACHED_IMAGES}: отбрасывать "
            "часть чертежей молча нельзя, а слать всё — заведомо непроходной "
            "вызов. Нужен планировщик отбора, которого в provider-режиме нет"
        )
    total = sum(len(blob) for _mt, blob in out)
    if total > MAX_ATTACHED_BYTES:
        raise ProviderStageRefusal(
            f"вложения весят {total} байт > потолка {MAX_ATTACHED_BYTES} "
            "(в теле запроса это ещё +33% из-за base64)"
        )
    return out


def split_messages(messages: Iterable[dict]) -> tuple[str, str]:
    """Разложить боевые messages на инструкции и полезную нагрузку."""
    system_parts: list[str] = []
    user_parts: list[str] = []
    for message in messages or []:
        role = str((message or {}).get("role") or "")
        content = (message or {}).get("content")
        if isinstance(content, list):
            text = "\n".join(
                str(part.get("text") or "")
                for part in content
                if isinstance(part, dict) and part.get("type") == "text"
            )
        else:
            text = str(content or "")
        if role == "system":
            system_parts.append(text)
        else:
            user_parts.append(text)
    return "\n\n".join(p for p in system_parts if p), "\n\n".join(p for p in user_parts if p)


def output_contract(root_key: str) -> str:
    """Контракт ответа. Единственное содержательное отличие от CLI-промпта."""
    return f"""
ФОРМАТ ОТВЕТА

Верни РОВНО ОДИН объект JSON и ничего кроме него: ни пояснений до и после, ни
обрамления markdown, ни описания того, что ты собираешься сделать.

Объект обязан содержать ключ "{root_key}".

Никаких файлов читать и писать не нужно и нечем: инструментов у тебя нет, весь
необходимый материал уже вложен в это сообщение. Результат — твой ответ.
"""


def build_provider_prompt(
    messages: Iterable[dict],
    *,
    root_key: str,
    with_severity: bool = True,
) -> dict[str, Any]:
    """Собрать промпт вызова и карту его состава.

    Карта уезжает в отчёт о прогоне; сам промпт — нет. Промпт содержит
    замечания по документу заказчика, и отчёт стал бы вторым их экземпляром за
    пределами артефакта.
    """
    images = extract_images(messages)
    system_raw, payload = split_messages(messages)
    # Два разных снятия, и порядок содержателен. Сначала уходят СТРОКИ-задания
    # файловой работы целиком («WRITE via Write tool: …»), потом из оставшегося
    # текста — одиночные абсолютные пути, которые встречаются в описаниях
    # входных данных. Обратный порядок оставил бы в промпте инструкцию с
    # заглушкой вместо пути: «запиши результат в <путь удалён>».
    system_text, stripped = strip_file_instructions(system_raw)
    system_text, paths_stripped = _replace_paths(system_text)
    parts = [system_text.strip()]
    severity = SEVERITY_SEMANTICS.strip() if with_severity else ""
    if severity:
        parts.append(severity)
    contract = output_contract(root_key).strip()
    parts.append(contract)
    instructions = "\n\n".join(p for p in parts if p)
    prompt = instructions + "\n\n" + payload.strip()
    return {
        "prompt": prompt,
        "system_chars": len(instructions),
        "payload_chars": len(payload),
        "prompt_chars": len(prompt),
        "file_instructions_stripped": stripped,
        "filesystem_refs_stripped": paths_stripped,
        "absolute_paths_remaining_in_instructions": count_absolute_paths(instructions),
        "images": images,
        "map": {
            "images_attached": len(images),
            "source_system_chars": len(system_raw),
            "instructions_chars": len(instructions),
            "payload_chars": len(payload),
            "prompt_chars": len(prompt),
            "file_instructions_stripped": stripped,
            "filesystem_refs_stripped": paths_stripped,
            "severity_semantics_applied": bool(severity) and severity.splitlines()[0] in prompt,
            "output_contract_root_key": root_key,
            "absolute_paths_remaining_in_instructions": count_absolute_paths(instructions),
            "tools": 0,
        },
    }


def guard_problems(built: dict[str, Any], *, max_prompt_chars: int) -> list[str]:
    """Проверки промпта В РАНТАЙМЕ, а не только в тестах.

    Тест доказывает, что код УМЕЕТ собрать промпт правильно; артефакт прогона
    обязан доказать, что в ЭТОТ раз он собран правильно.
    """
    problems: list[str] = []
    if built["absolute_paths_remaining_in_instructions"]:
        problems.append(
            f"в инструкциях остались абсолютные пути "
            f"({built['absolute_paths_remaining_in_instructions']}): "
            "давать модели путь проекта запрещено"
        )
    if built["prompt_chars"] > max_prompt_chars:
        problems.append(
            f"промпт {built['prompt_chars']} симв. > потолка {max_prompt_chars}: "
            "планировщика нарезки в provider-режиме нет, а молчаливое усечение "
            "входа недопустимо"
        )
    if not str(built["prompt"]).strip():
        problems.append("промпт пуст")
    return problems


def run_report(
    *,
    stage: str,
    built: dict[str, Any],
    outcome: Any,
    root_key: str,
    prompt_sha256: str,
) -> dict[str, Any]:
    """Отчёт о прогоне. Без промпта и без ответа модели — только факты о вызове."""
    result = outcome.provider_result
    payload = result.result if isinstance(result.result, dict) else {}
    facts = {k: v for k, v in result.as_dict().items() if k != "result"}
    facts["result_keys"] = sorted(payload.keys())
    root_value = payload.get(root_key)
    facts["result_root_items"] = len(root_value) if isinstance(root_value, list) else None
    return {
        "stage": stage,
        "transport": "provider_adapter",
        "prompt_build": built["map"],
        "prompt_sha256": prompt_sha256,
        "performed_now": bool(outcome.performed),
        "provider_result": facts,
        "validation": outcome.validation.as_dict() if outcome.validation else None,
        "ledger": outcome.ledger.as_dict(),
        "content_excluded": (
            "Промпт и ответ модели в отчёт не кладутся: это данные по документу "
            "заказчика. Отпечаток промпта — prompt_sha256, отпечаток ответа — "
            "provider_result.raw_sha256."
        ),
    }


def failure_detail(outcome: Any) -> str:
    """Почему результат не принят — одной строкой, без содержимого ответа."""
    result = outcome.provider_result
    validation = outcome.validation.as_dict() if outcome.validation else None
    failed: Optional[Any] = (validation or {}).get("failed") if validation else None
    if outcome.performed:
        return (
            f"provider_result.status={result.status!r} "
            f"error_code={result.error_code!r} detail={result.detail!r} "
            f"validation_failed={failed}"
        )
    return (
        "повтор невозможен: результат этого вызова уже записан в журнал попытки "
        f"и неуспешен (проверка: {failed}). Новая попытка требует нового "
        "attempt_id и новой единицы разрешения"
    )


def write_artifact(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )
