"""
clause_binding.py
-----------------
Нормативная привязка замечаний: документ И номер пункта, подтверждённые базой.

Зачем отдельный этап. Ссылку на норму проставляет свод, попутно с десятком
других задач (слияние дублей, формулировки, листы, важность). Насколько
подробной выйдет ссылка, оказалось свойством модели, а не промпта: замер
06.08.2026 по семи проектам — claude-opus давал номер пункта у 71% ссылок,
codex/gpt-5.4 в шести прогонах из семи не давал вовсе, хотя в седьмом дал у
всех 22 замечаний, и все пункты оказались настоящими. То есть модель умеет и не
врёт — ей просто не до того на своде.

Метод, который выравнивает поведение любой модели, — не уговоры в промпте, а
цикл с обратной связью:

    узкая задача (только нормы) → ответ модели → сверка каждого пункта с
    индексом норм → ненайденные пункты возвращаются модели с указанием, что
    именно не найдено → повтор

Выдумать номер здесь бессмысленно: несуществующий пункт не пройдёт сверку и
вернётся на исправление, а после последнего раунда просто не будет записан.
Замечание в худшем случае остаётся с ссылкой на документ — как и было.

Модуль намеренно без ввода-вывода моделей: `build_messages` готовит запрос,
`parse_answer` разбирает ответ, `validate` сверяет с базой, `apply` пишет в
findings. Кто именно вызывает модель (Claude CLI, Codex CLI, локальная) —
решает вызывающая сторона, поэтому один и тот же метод работает для всех.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable

_PARA_RE = re.compile(r"п\.\s*(\d+(?:\.\d+)*)")
_CLAUSE_RE = re.compile(r"^\d+(?:\.\d+)*$")

MAX_FINDING_CHARS = 600
DEFAULT_ROUNDS = 2


SYSTEM_PROMPT = """Ты инженер-нормоконтролёр проектной документации РФ.

Задача одна: для каждого замечания назвать пункт норматива, который предъявляет \
нарушенное требование.

Ответ — ТОЛЬКО JSON-массив, без пояснений и markdown:
[{"id": "F-001", "doc": "ГОСТ Р 21.101-2020", "clause": "5.1.6"}]

Правила:
- Ответ обязателен для КАЖДОГО замечания из списка. Пропускать нельзя: пустой \
ответ — это не осторожность, а отказ от работы.
- `doc` — обозначение документа: ГОСТ, ГОСТ Р, СП, СНиП, ПУЭ. Без названия в \
кавычках и без статуса.
- `clause` — номер пункта ВНУТРИ этого документа: «5.1.6», «7.2», «10.3.1». \
Не диапазон, не раздел целиком, не «таблица 5».
- Каждый названный пункт автоматически сверяется с базой нормативов, и \
несуществующий вернётся тебе на исправление. Поэтому называй тот пункт, в \
номере которого ты уверен: лучше более общий пункт раздела, который точно \
существует, чем детальный подпункт наугад.
- Замечания об оформлении комплекта (обозначения, спецификации, ведомости, \
ссылки между листами) — это требования СПДС: ГОСТ Р 21.101, ГОСТ 21.501, \
ГОСТ 21.110 и подобные. У них тоже есть конкретные пункты."""


def select_targets(findings: Iterable[dict]) -> list[dict]:
    """Замечания, которым не хватает номера пункта.

    Берём и те, где нормы нет вовсе, и те, где назван только документ: во втором
    случае документ подсказывает модели направление, но не связывает её — если
    она считает уместным другой норматив, пусть назовёт другой.
    """
    targets = []
    for f in findings:
        norm = (f.get("norm") or "").strip()
        if not norm or not _PARA_RE.search(norm):
            targets.append(f)
    return targets


def build_messages(
    targets: list[dict],
    *,
    rejected: dict[str, str] | None = None,
) -> list[dict]:
    """Собрать запрос к модели.

    Args:
        targets: замечания без пункта.
        rejected: {finding_id: причина} с прошлого раунда — что база не нашла.
    """
    lines = []
    for f in targets:
        fid = str(f.get("id") or "")
        problem = str(f.get("problem") or "").strip()
        desc = str(f.get("description") or "").strip()
        text = (problem + " " + desc).strip()[:MAX_FINDING_CHARS]
        hint = (f.get("norm") or "").strip()
        hint_part = f" [свод предположил: {hint}]" if hint else ""
        lines.append(f"id={fid}{hint_part}\n{text}")

    user = "ЗАМЕЧАНИЯ:\n\n" + "\n\n".join(lines)

    if rejected:
        problems = "\n".join(f"- {fid}: {reason}" for fid, reason in rejected.items())
        user += (
            "\n\nПРЕДЫДУЩИЙ ОТВЕТ ЧАСТИЧНО ОТКЛОНЁН БАЗОЙ НОРМАТИВОВ:\n"
            f"{problems}\n\n"
            "Назови для этих замечаний другой пункт — существующий — либо не "
            "включай их в ответ вовсе."
        )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


def parse_answer(text: str) -> dict[str, dict]:
    """Разобрать ответ модели в {finding_id: {"doc", "clause"}}.

    Модели любят обрамлять JSON пояснениями и ```-заборами, поэтому берём первый
    массив в тексте, а не полагаемся на чистоту ответа.
    """
    if not text:
        return {}
    raw = text.strip()
    if "```" in raw:
        parts = [p for p in raw.split("```") if "[" in p]
        raw = parts[0] if parts else raw
        raw = re.sub(r"^\s*json\s*", "", raw, flags=re.I)
    start, end = raw.find("["), raw.rfind("]")
    if start == -1 or end == -1 or end < start:
        return {}
    try:
        items = json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return {}
    if not isinstance(items, list):
        return {}

    out: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        fid = str(item.get("id") or "").strip()
        doc = str(item.get("doc") or "").strip()
        clause = str(item.get("clause") or "").strip().lstrip("п. ").strip()
        if not fid or not doc or not clause:
            continue
        if not _CLAUSE_RE.match(clause):
            continue
        out[fid] = {"doc": doc, "clause": clause}
    return out


def validate(
    answers: dict[str, dict],
    norms_api: Any,
    resolve_code,
) -> tuple[dict[str, dict], dict[str, str]]:
    """Сверить каждый названный пункт с базой нормативов.

    Returns:
        (accepted, rejected) — accepted несёт canon-код, текст пункта и статус
        документа; rejected несёт причину, которую увидит модель в next round.
    """
    accepted: dict[str, dict] = {}
    rejected: dict[str, str] = {}

    for fid, ans in answers.items():
        doc, clause = ans["doc"], ans["clause"]
        canon = resolve_code(norms_api, doc)
        if not canon:
            rejected[fid] = f"документа «{doc}» нет в базе нормативов"
            continue
        try:
            res = norms_api.get_paragraph(canon, clause)
        except Exception as exc:  # noqa: BLE001 — сбой базы не должен ронять этап
            rejected[fid] = f"проверка «{doc}» п. {clause} не удалась ({exc})"
            continue
        text = (res.get("text") or "").strip()
        if not res.get("found") or not text:
            rejected[fid] = f"в «{doc}» нет пункта {clause}"
            continue
        try:
            status = norms_api.get_norm_status(canon)
        except Exception:  # noqa: BLE001
            status = {}
        accepted[fid] = {
            "doc": doc,
            "canon": canon,
            "clause": clause,
            "text": text,
            "status": status.get("status") or "",
        }
    return accepted, rejected


_STATUS_RU = {
    "active": "действует",
    "replaced": "заменён",
    "outdated_edition": "устаревшая редакция",
    "cancelled": "отменён",
}


def apply(findings: list[dict], accepted: dict[str, dict]) -> int:
    """Записать подтверждённые ссылки в замечания. Возвращает число изменённых."""
    by_id = {str(f.get("id") or ""): f for f in findings}
    changed = 0
    for fid, data in accepted.items():
        finding = by_id.get(fid)
        if finding is None:
            continue
        status_ru = _STATUS_RU.get(data["status"], data["status"])
        suffix = f" ({status_ru})" if status_ru else ""
        finding["norm"] = f"{data['doc']}{suffix}, п. {data['clause']}"
        if not (finding.get("norm_quote") or "").strip():
            finding["norm_quote"] = data["text"][:400]
            finding["norm_quote_source"] = "norms_index"
        finding["norm_paragraph_state"] = "paragraph_verified"
        finding["norm_binding"] = "clause_binding"
        changed += 1
    return changed
