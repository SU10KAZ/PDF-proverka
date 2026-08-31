"""Инвентаризация маршрутизации ИИ-слоя: кто уезжает модели и почему остальные — нет.

Зачем отдельный артефакт, если решение о маршруте можно принять на месте.

Потому что раньше такого решения не было вовсе. Слой получал ровно
``unified_synthesis.review_items`` — и это выглядело как «модели отдают всё
неразобранное», хотя на деле неразобранное живёт в четырёх разных артефактах:
несопоставленные строки таблиц нагрузок в ``electrical_table_changes``,
внутренние противоречия листа в ``document_inconsistencies``, находки с
неполными доказательствами внутри самих ``changes``, и только текстовые
свидетельства — в ``review_items``. Три класса из четырёх не маршрутизировались
никогда, и понять это по коду было нельзя: пропуск выглядел не как решение, а
как отсутствие строки.

Инвентаризация делает пропуск ЯВНЫМ. Каждый нерешённый инженерный элемент
получает решение маршрута с причиной, и решение это записывается в артефакт
независимо от режима: в «Быстро» инвентаризация тоже строится (0 обращений к
модели) и показывает, что именно ИИ мог бы взять на себя.

Три исхода, и они не взаимозаменяемы:

  AI_ELIGIBLE
      доказательства есть с обеих сторон, вопрос решаем разбором;
  AI_INELIGIBLE_INSUFFICIENT_EVIDENCE
      одной стороны физически нет в распознанном виде — модель не является
      заменой отсутствующего источника, и звать её значит платить за отказ;
  AI_INELIGIBLE_POLICY
      доказательства есть, но решение за человеком по устройству системы, а
      не по нехватке данных.

Отдельно про третий исход. «Недостаточно доказательств» и «не положено» —
разные утверждения, и склеивать их нельзя: первое чинится улучшением
распознавания, второе не чинится вообще, потому что чинить нечего.

ВАЖНО про инвариант проекта. Вердикт AI_INELIGIBLE_INSUFFICIENT_EVIDENCE НЕ
утверждает, что на листе чего-то нет. Он утверждает ровно одно: у слоя нет
прочитанного доказательства, на которое модель могла бы опереться. Элемент при
этом никуда не исчезает — он остаётся человеку с той же формулировкой, что и
раньше.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable, Mapping, Sequence

KIND = "stage_comparison_ai_routing_inventory"
SCHEMA_VERSION = "ai-routing-inventory.v1"
PRODUCER = "stage-comparison-ai-routing-v1"

# ── Решения маршрута ───────────────────────────────────────────────────────
ELIGIBLE = "AI_ELIGIBLE"
INELIGIBLE_EVIDENCE = "AI_INELIGIBLE_INSUFFICIENT_EVIDENCE"
INELIGIBLE_POLICY = "AI_INELIGIBLE_POLICY"
DECISIONS = (ELIGIBLE, INELIGIBLE_EVIDENCE, INELIGIBLE_POLICY)

# ── Внутренние виды нерешённого ────────────────────────────────────────────
KIND_TEXT_REVIEW = "TEXT_REVIEW"
KIND_TABLE_UNPROVEN = "TABLE_ROW_UNPROVEN"
KIND_TABLE_BLOCKED = "TABLE_ROW_BLOCKED"
KIND_CONSISTENCY_REVIEW = "CONSISTENCY_REVIEW"
KIND_CHANGE_REVIEW = "CHANGE_INCOMPLETE_EVIDENCE"
KINDS = (
    KIND_TEXT_REVIEW,
    KIND_TABLE_UNPROVEN,
    KIND_TABLE_BLOCKED,
    KIND_CONSISTENCY_REVIEW,
    KIND_CHANGE_REVIEW,
)

#: Как этот вид называется на языке инженера. Внутренние коды в отчёт не
#: попадают: инженер читает «строка таблицы без пары», а не TABLE_ROW_UNPROVEN.
HUMAN_CATEGORY = {
    KIND_TEXT_REVIEW: "Текст не сопоставлен между листами",
    KIND_TABLE_UNPROVEN: "Строка таблицы нагрузок без доказанной пары",
    KIND_TABLE_BLOCKED: "Сопоставление строк таблицы заблокировано",
    KIND_CONSISTENCY_REVIEW: "Внутреннее противоречие листа, требующее проверки",
    KIND_CHANGE_REVIEW: "Находка с неполными доказательствами",
}

#: Виды, у которых СЕГОДНЯ есть живой разбор. Допуск и наличие маршрута —
#: разные вещи, и склеивать их нельзя: элемент, допущенный к разбору, но
#: никем не разбираемый, в артефакте выглядел бы как «уехал модели» и тихо
#: терялся между решением и исполнением. Ровно так пропадали восемь элементов
#: на паре ГРЩ: инвентаризация писала routed_to_ai = true, а ни текстовый
#: проход, ни разбор тождества их не забирали.
ROUTE_IMPLEMENTED = "ROUTE_IMPLEMENTED"
ROUTE_NOT_IMPLEMENTED = "ROUTE_NOT_IMPLEMENTED"
ROUTE_NOT_ELIGIBLE = "ROUTE_NOT_ELIGIBLE"

#: Кто кого разбирает. Текстовый проход — свидетельства, разбор тождества —
#: строки таблиц. Внутренние противоречия и находки с неполными
#: доказательствами живого разбора пока не имеют.
ROUTED_KINDS = (
    KIND_TEXT_REVIEW,
    KIND_TABLE_UNPROVEN,
    KIND_TABLE_BLOCKED,
)

NOT_IMPLEMENTED_NOTE = (
    "разбор этого вида пока не реализован: элемент остаётся человеку, "
    "хотя доказательства для разбора есть"
)


# ── Коды причин маршрута ───────────────────────────────────────────────────
REASON_BOTH_SIDES_READ = "BOTH_SIDES_READABLE"
REASON_CANDIDATES_FOUND = "COUNTERPART_CANDIDATES_FOUND"
REASON_SIDE_NOT_RECOGNISED = "OPPOSITE_SIDE_NOT_RECOGNISED"
REASON_NO_CANDIDATES = "NO_COUNTERPART_CANDIDATES"
REASON_MODE_MISMATCH = "COMPARING_DIFFERENT_MODES_FORBIDDEN"
REASON_MATCHER_META = "MATCHER_QUALITY_META_FINDING"
REASON_ALREADY_PROVEN = "ALREADY_PROVEN_DETERMINISTICALLY"

REASON_TEXT = {
    REASON_BOTH_SIDES_READ: "обе стороны прочитаны — вопрос решается разбором",
    REASON_CANDIDATES_FOUND: "на другой стороне есть кандидаты для сопоставления",
    REASON_SIDE_NOT_RECOGNISED: (
        "на противоположном листе нет прочитанного текста, с которым можно "
        "сопоставить: модель не заменяет отсутствующий источник"
    ),
    REASON_NO_CANDIDATES: (
        "на другой стороне нет ни одного кандидата того же вида и раздела"
    ),
    REASON_MODE_MISMATCH: (
        "стороны приведены в разных режимах работы; сравнивать их напрямую "
        "запрещено устройством системы, а не нехваткой данных"
    ),
    REASON_MATCHER_META: (
        "находка о качестве самого сопоставления, а не об объекте проекта: "
        "оценивать матчер моделью не положено"
    ),
    REASON_ALREADY_PROVEN: "доказано детерминированно, разбор модели не нужен",
}

#: Какая доля значимых токенов искомой строки обязана найтись в кандидате.
#: Считается ОТ ИСКОМОЙ строки, а не от более короткой из двух: иначе левый
#: фрагмент «Проверил» из рамки штампа покрывает запрос «Проверил Бушмин
#: 02.26» на сто процентов и объявляет фамилию найденной, хотя её на листе
#: нет вовсе.
RETRIEVAL_MIN_COVERAGE = 0.5

#: Длина, начиная с которой СЛОВО считается содержательным. «Ip», «11» и
#: «ГРЩ» стоят на этом листе в каждой второй строке: совпадение по ним
#: связывает примечание с номиналом автомата.
STRONG_TOKEN_LENGTH = 4

#: Сколько строк противоположной стороны добирается адресно. Окно ограничено
#: намеренно: показать лист целиком значит вернуть модели ту самую задачу
#: поиска, ради снятия которой существует детерминированный слой.
RETRIEVAL_LIMIT = 6

#: Сколько кандидатов противоположной стороны показывается на один вопрос об
#: идентичности. Больше — и вопрос «эти две строки один объект» превращается
#: в «разберись в таблице сам».
CANDIDATE_LIMIT = 4

#: Слово вместе с приросшими к нему цифрами — один токен. Раздельная
#: разбивка делала «ВРУ1» и «ВРУ3» одинаковыми ({«вру»}, цифра короче двух
#: знаков и отбрасывалась), и сводная строка чужого потребителя попадала в
#: доказательства как своя.
_TOKEN_RE = re.compile(r"[a-zа-яё]+[0-9]*|[0-9]+(?:[.,][0-9]+)?", re.I)

#: Слова, которые есть в каждой второй строке чертежа и потому ничего не
#: доказывают. Без этого списка «ввод» и «схема» связывали бы любые две
#: строки листа.
_STOP_TOKENS = frozenset({
    "и", "в", "на", "с", "по", "не", "для", "из", "от", "до", "а", "или",
    "ввод", "вводы", "схема", "лист", "листа", "шт", "мм", "п",
})


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    return text.replace("ё", "е")


def tokens(value: Any) -> set[str]:
    """Значимые токены строки: слова и числа, без служебной шелухи."""
    return {
        token for token in _TOKEN_RE.findall(_normalize(value))
        if token not in _STOP_TOKENS and len(token) > 1
    }


def similarity(left: Any, right: Any) -> float:
    """Доля общих значимых токенов относительно более короткой строки.

    Именно относительно короткой, а не жаккар: строка примечания длиной в
    двадцать слов и её же обрывок в шесть слов — это одно место листа, и
    делить на объединение значило бы объявить их разными.
    """
    first, second = tokens(left), tokens(right)
    if not first or not second:
        return 0.0
    return len(first & second) / float(min(len(first), len(second)))


def strong_tokens(value: Any) -> set[str]:
    """Токены, совпадение по которым что-то доказывает.

    Только слова, и только длиной от четырёх букв. Числа сюда не входят
    намеренно, даже дробные: «P 1.1» и шифр «АА/БЭ-03-ДС3-ИОС1.1.ГЧ» делят
    ровно «1.1» — и по этому совпадению строка стадии объявлялась найденной
    на левом листе, где её нет. Число участвует в покрытии, но само по себе
    ничего не связывает: на однолинейной схеме их сотни.
    """
    return {
        token for token in tokens(value)
        if not token[0].isdigit() and len(token) >= STRONG_TOKEN_LENGTH
    }


def coverage(query: Any, candidate: Any) -> float:
    """Какая доля значимых токенов ЗАПРОСА нашлась в кандидате."""
    wanted = tokens(query)
    if not wanted:
        return 0.0
    return len(wanted & tokens(candidate)) / float(len(wanted))


def _evidence_enough(query: Any, candidate: Any) -> bool:
    """Кандидат действительно про то же место листа, а не про соседнее.

    Два независимых условия, и оба обязательны. Покрытие ловит обрывок,
    похожий только своим служебным началом; порог по содержательным словам
    ловит совпадение по «Ip» и «11», которых на схеме сотни. По отдельности
    ни одно из них не отсекает штамп: «Подп. и дата» покрывает «Изм. Кол.уч.
    Лист N°док. Подп. Дата» двумя сильными словами, но лишь на 29 %.
    """
    if coverage(query, candidate) < RETRIEVAL_MIN_COVERAGE:
        return False
    wanted = strong_tokens(query)
    shared = wanted & strong_tokens(candidate)
    if not wanted:
        # Содержательных слов нет вовсе — доказывать нечем ни в какую сторону.
        return False
    return len(shared) >= (1 if len(wanted) == 1 else 2)


def retrieve_lines(
    fragments: Iterable[Mapping[str, Any]],
    query: Any,
    *,
    limit: int = RETRIEVAL_LIMIT,
) -> list[dict[str, Any]]:
    """Адресно добрать строки стороны, у которой нет якорного фрагмента.

    Детерминированный поиск по токенам, а не обращение к модели: у элемента
    вида «добавлено» противоположная сторона не имеет координат вовсе, и окно
    вокруг несуществующего фрагмента честно пусто. Пустое окно модель читает
    как «доказательств не показали» — и отвечает EVIDENCE_TRUNCATED, хотя
    прочитанные строки этой стороны лежат в том же артефакте.

    Пустой результат НЕ означает, что на листе этого нет: он означает, что
    прочитанного доказательства нет. Разница принципиальная, и на ней стоит
    весь раздел полноты распознавания.
    """
    scored: list[tuple[float, int, dict[str, Any]]] = []
    for order, fragment in enumerate(fragments):
        if not isinstance(fragment, Mapping):
            continue
        text = str(fragment.get("text") or "")
        if not text.strip() or not _evidence_enough(query, text):
            continue
        scored.append((coverage(query, text), order, {
            "fragment_id": str(fragment.get("id") or ""),
            "text": text,
            "score": round(coverage(query, text), 3),
            "source": str(fragment.get("source") or ""),
            "page": fragment.get("pdf_page"),
        }))
    scored.sort(key=lambda entry: (-entry[0], entry[1]))
    return [entry[2] for entry in scored[:limit]]


# ── Строки таблиц нагрузок ─────────────────────────────────────────────────

def row_designations(row: Mapping[str, Any]) -> list[str]:
    """Все обозначения строки: собственное, соседние по ряду, фидерные.

    Ряд обозначений над колонкой — независимое свидетельство: именно из-за
    него правая «ВРУ-ХЦ» подписана «ШУ-ХЦ», и без него вопрос об идентичности
    задавать бессмысленно.
    """
    output: list[str] = []
    for value in row.get("own_designations") or ():
        if str(value or "").strip():
            output.append(str(value))
    for group in ("row_designations", "feeder_designations"):
        for value in row.get(group) or ():
            name = (
                value.get("designation") if isinstance(value, Mapping) else value
            )
            if str(name or "").strip():
                output.append(str(name))
    seen: set[str] = set()
    unique: list[str] = []
    for value in output:
        key = _normalize(value)
        if key and key not in seen:
            seen.add(key)
            unique.append(value)
    return unique


def row_identity_text(row: Mapping[str, Any]) -> str:
    """Строка, по которой считается похожесть двух строк таблицы."""
    parts = [str(row.get("consumer_label") or "")]
    parts += row_designations(row)
    parts.append(str(row.get("section_ref") or ""))
    return " | ".join(part for part in parts if part.strip())


def _row_scope(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("row_kind") or ""),
        _normalize(row.get("section_ref")),
    )


def matched_row_ids(table_changes: Mapping[str, Any] | None) -> set[str]:
    """Строки, у которых пара уже доказана детерминированно.

    Предлагать их модели нельзя. Детерминированный матчер занимает строку
    один раз (`used_left`/`used_right`), а разбор тождества, не знающий об
    этом, спокойно выдаёт вторую пару к той же строке — и в отчёте появляется
    «ХМ1: расчётная активная мощность увеличена с 157,5 до 335 кВт» дважды:
    как «найдено автоматически» и как «уточнено ИИ». Числа при этом верные,
    но инженеру предъявлена одна находка двумя строками.
    """
    output: set[str] = set()
    payload = table_changes or {}
    for record in list(payload.get("changes") or ()) + list(payload.get("unchanged") or ()):
        evidence = record.get("evidence") if isinstance(record, Mapping) else None
        if not isinstance(evidence, Mapping):
            continue
        for side in ("LEFT", "RIGHT"):
            value = (evidence.get(side) or {}).get("row_id")
            if value:
                output.add(str(value))
    return output


def counterpart_candidates(
    row: Mapping[str, Any],
    other_rows: Sequence[Mapping[str, Any]],
    *,
    limit: int = CANDIDATE_LIMIT,
    exclude: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    """Кандидаты противоположной стороны для одной несопоставленной строки.

    Отбор идёт по виду строки и разделу, а не по похожести подписи: подпись
    как раз и разъехалась — «ШУ-ХЦ» слева против «ВРУ-ХЦ» справа, — и
    требовать её совпадения значит не найти ровно то, ради чего вопрос
    задаётся. Похожесть решает только ПОРЯДОК показа.
    """
    kind, section = _row_scope(row)
    identity = row_identity_text(row)
    taken = {str(value) for value in exclude}
    scored: list[tuple[float, int, dict[str, Any]]] = []
    for order, other in enumerate(other_rows):
        if not isinstance(other, Mapping):
            continue
        if str(other.get("row_id") or "") in taken:
            continue
        other_kind, other_section = _row_scope(other)
        if kind and other_kind and kind != other_kind:
            continue
        # Раздел сравнивается только когда он известен обеим строкам: у
        # суммарных строк потребителя его нет ни слева, ни справа.
        if section and other_section and section != other_section:
            continue
        scored.append((
            similarity(identity, row_identity_text(other)),
            order,
            {
                "row_id": str(other.get("row_id") or ""),
                "score": round(similarity(identity, row_identity_text(other)), 3),
            },
        ))
    scored.sort(key=lambda entry: (-entry[0], entry[1]))
    return [entry[2] for entry in scored[:limit]]


def _rows_by_side(load_tables: Mapping[str, Any] | None) -> dict[str, list[dict]]:
    output: dict[str, list[dict]] = {"LEFT": [], "RIGHT": []}
    for side in ("LEFT", "RIGHT"):
        table = (load_tables or {}).get(side)
        rows = table.get("rows") if isinstance(table, Mapping) else None
        output[side] = [dict(row) for row in rows or () if isinstance(row, Mapping)]
    return output


def _other(side: str) -> str:
    return "RIGHT" if str(side).upper() == "LEFT" else "LEFT"


# ── Записи инвентаризации ──────────────────────────────────────────────────

def _entry(
    *,
    item_id: str,
    kind: str,
    decision: str,
    reason_code: str,
    available: Sequence[str],
    missing: Sequence[str],
    subject: Any = None,
    side: Any = None,
    summary: str = "",
    unresolved: bool = True,
    routing_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "kind": kind,
        "human_category": HUMAN_CATEGORY.get(kind, kind),
        "decision": decision,
        # Допуск сам по себе никуда элемент не отправляет: маршрут есть не у
        # каждого вида. Поле честно отвечает на вопрос «уехал ли он модели», а
        # не «разрешено ли было».
        "route_status": (
            ROUTE_NOT_ELIGIBLE if decision != ELIGIBLE
            else ROUTE_IMPLEMENTED if kind in ROUTED_KINDS
            else ROUTE_NOT_IMPLEMENTED
        ),
        "routed_to_ai": decision == ELIGIBLE and kind in ROUTED_KINDS,
        "reason_code": reason_code,
        "reason": REASON_TEXT.get(reason_code, reason_code),
        # Уже доказанная находка попадает в инвентаризацию не как задача, а
        # как след: «этот элемент модели не отдавали и не должны были».
        # Складывать её в один счётчик с нерешённым нельзя — тогда «сколько
        # снято с человека» считается от завышенной базы.
        "unresolved": bool(unresolved),
        "available_evidence": list(available),
        "missing_evidence": (
            list(missing)
            if decision != ELIGIBLE or kind in ROUTED_KINDS
            else [*missing, NOT_IMPLEMENTED_NOTE]
        ),
        "subject": subject,
        "side": side,
        "summary": summary,
        # Всё, что понадобится сборщику пакета: ссылки на строки-кандидаты,
        # добранные строки текста. Считается здесь один раз, чтобы сборщик
        # пакета и инвентаризация не разошлись в том, что именно доступно.
        "routing_payload": dict(routing_payload or {}),
    }


def _text_review_entries(
    review_items: Sequence[Mapping[str, Any]],
    preparation: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    fragments = ((preparation or {}).get("fragments") or {})
    by_side = {
        "LEFT": list(fragments.get("left") or ()),
        "RIGHT": list(fragments.get("right") or ()),
    }
    entries: list[dict[str, Any]] = []
    for item in review_items:
        if not isinstance(item, Mapping):
            continue
        before, after = item.get("before_value"), item.get("after_value")
        if after is not None and before is None:
            missing_side, query = "LEFT", after
        elif before is not None and after is None:
            missing_side, query = "RIGHT", before
        else:
            missing_side, query = "", after or before
        item_id = str(item.get("review_evidence_id") or "")
        summary = str(query or "")[:160]
        if not missing_side:
            entries.append(_entry(
                item_id=item_id, kind=KIND_TEXT_REVIEW, decision=ELIGIBLE,
                reason_code=REASON_BOTH_SIDES_READ,
                available=["обе стороны свидетельства прочитаны"],
                missing=[], summary=summary,
            ))
            continue
        found = retrieve_lines(by_side[missing_side], query)
        if found:
            entries.append(_entry(
                item_id=item_id, kind=KIND_TEXT_REVIEW, decision=ELIGIBLE,
                reason_code=REASON_BOTH_SIDES_READ,
                available=[
                    f"на стороне {missing_side} адресно найдено похожих строк:"
                    f" {len(found)}"
                ],
                missing=[],
                summary=summary,
                routing_payload={"retrieved": {missing_side: found}},
            ))
            continue
        entries.append(_entry(
            item_id=item_id, kind=KIND_TEXT_REVIEW,
            decision=INELIGIBLE_EVIDENCE,
            reason_code=REASON_SIDE_NOT_RECOGNISED,
            available=[
                f"сторона {_other(missing_side)}: текст прочитан",
            ],
            missing=[
                f"сторона {missing_side}: ни одной прочитанной строки, похожей"
                " на искомую (окно вокруг фрагмента пусто — фрагмента нет)"
            ],
            summary=summary,
        ))
    return entries


def _table_entries(
    table_changes: Mapping[str, Any] | None,
    load_tables: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    rows = _rows_by_side(load_tables)
    taken = matched_row_ids(table_changes)
    by_id = {
        str(row.get("row_id") or ""): row
        for side_rows in rows.values() for row in side_rows
    }
    entries: list[dict[str, Any]] = []

    for record in (table_changes or {}).get("unproven") or ():
        if not isinstance(record, Mapping):
            continue
        row_id = str(record.get("row_id") or "")
        side = str(record.get("side") or "").upper()
        row = by_id.get(row_id) or {}
        candidates = (
            counterpart_candidates(
                row, rows.get(_other(side)) or (), exclude=taken,
            )
            if row else []
        )
        summary = str(record.get("summary") or "")[:200]
        if candidates:
            entries.append(_entry(
                item_id=row_id or f"unproven:{summary[:40]}",
                kind=KIND_TABLE_UNPROVEN, decision=ELIGIBLE,
                reason_code=REASON_CANDIDATES_FOUND,
                available=[
                    f"строка стороны {side} прочитана целиком",
                    f"кандидатов на стороне {_other(side)}: {len(candidates)}",
                ],
                missing=["доказанная пара — её и предстоит выбрать"],
                subject=record.get("subject"), side=side, summary=summary,
                routing_payload={
                    "row_id": row_id,
                    "side": side,
                    "candidate_row_ids": [
                        value["row_id"] for value in candidates
                    ],
                },
            ))
            continue
        entries.append(_entry(
            item_id=row_id or f"unproven:{summary[:40]}",
            kind=KIND_TABLE_UNPROVEN, decision=INELIGIBLE_EVIDENCE,
            reason_code=REASON_NO_CANDIDATES,
            available=[f"строка стороны {side} прочитана"],
            missing=[
                f"на стороне {_other(side)} нет ни одной строки того же вида"
                " и раздела"
            ],
            subject=record.get("subject"), side=side, summary=summary,
        ))

    for record in (table_changes or {}).get("blocked") or ():
        if not isinstance(record, Mapping):
            continue
        reason = str(record.get("reason") or "")
        summary = str(record.get("summary") or "")[:200]
        item_id = str(record.get("match_id") or "") or f"blocked:{summary[:40]}"
        if reason == "ambiguous_row_match":
            left_ids = [str(value) for value in record.get("left_row_ids") or ()]
            right_ids = [str(value) for value in record.get("right_row_ids") or ()]
            entries.append(_entry(
                item_id=item_id, kind=KIND_TABLE_BLOCKED, decision=ELIGIBLE,
                reason_code=REASON_CANDIDATES_FOUND,
                available=[
                    f"кандидатов слева: {len(left_ids)}",
                    f"кандидатов справа: {len(right_ids)}",
                ],
                missing=["какая пара из перечисленных — один объект"],
                subject=record.get("subject") or (record.get("key") or [None])[0],
                summary=summary,
                routing_payload={
                    "left_row_ids": left_ids,
                    "right_row_ids": right_ids,
                },
            ))
            continue
        # Разные режимы работы. Отказ сравнивать их — осознанное решение
        # системы, а не пробел распознавания: показать «Рабочий/пожарн.» и
        # «без указания режима» как одно значение нельзя ни при какой
        # уверенности модели.
        entries.append(_entry(
            item_id=item_id, kind=KIND_TABLE_BLOCKED,
            decision=INELIGIBLE_POLICY, reason_code=REASON_MODE_MISMATCH,
            available=["обе строки прочитаны"],
            missing=["подтверждение, что сравнение режимов допустимо"],
            subject=record.get("subject"), summary=summary,
        ))
    return entries


def _consistency_entries(
    inconsistencies: Mapping[str, Any] | None,
    review_verdict: str = "REVIEW",
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for item in (inconsistencies or {}).get("items") or ():
        if not isinstance(item, Mapping):
            continue
        verdict = str(item.get("verdict") or "")
        item_id = str(item.get("inconsistency_id") or item.get("row_id") or "")
        summary = str(item.get("summary") or "")[:220]
        if verdict != review_verdict:
            # CONFIRMED и находки без вердикта доказаны на самом листе.
            entries.append(_entry(
                item_id=item_id, kind=KIND_CONSISTENCY_REVIEW,
                decision=INELIGIBLE_POLICY, reason_code=REASON_ALREADY_PROVEN,
                available=["противоречие доказано детерминированно"],
                missing=[], subject=item.get("subject"),
                side=item.get("side"), summary=summary, unresolved=False,
            ))
            continue
        entries.append(_entry(
            item_id=item_id, kind=KIND_CONSISTENCY_REVIEW, decision=ELIGIBLE,
            reason_code=REASON_BOTH_SIDES_READ,
            available=[
                "аппарат, секция по геометрии и подписи остальных линий"
                " секции прочитаны",
            ],
            missing=[
                "подтверждение, что необычная подпись — противоречие, а не"
                " осознанное исключение"
            ],
            subject=item.get("subject"), side=item.get("side"),
            summary=summary,
            routing_payload={"inconsistency_id": item_id},
        ))
    return entries


#: Находка, говорящая не об объекте проекта, а о качестве сопоставления.
_MATCHER_META_MARKERS = (
    "не удалось сопоставить",
    "сопоставить между редакциями",
)


def _change_entries(
    changes: Sequence[Mapping[str, Any]],
    is_review: Any,
    describe: Any,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for change in changes:
        if not isinstance(change, Mapping) or not is_review(change):
            continue
        change_id = str(change.get("change_id") or "")
        # Текст берётся тем же описателем, что и в предварительном отчёте:
        # у изменения нет собственного поля summary, и попытка прочитать его
        # молча давала пустую строку — а по пустой строке мета-находка о
        # качестве сопоставления неотличима от находки об объекте.
        summary = str(describe(change) or "")[:220]
        haystack = _normalize(summary)
        if any(marker in haystack for marker in _MATCHER_META_MARKERS):
            entries.append(_entry(
                item_id=change_id, kind=KIND_CHANGE_REVIEW,
                decision=INELIGIBLE_POLICY, reason_code=REASON_MATCHER_META,
                available=["диагностика сопоставления"],
                missing=[], summary=summary,
            ))
            continue
        entries.append(_entry(
            item_id=change_id, kind=KIND_CHANGE_REVIEW, decision=ELIGIBLE,
            reason_code=REASON_BOTH_SIDES_READ,
            available=["обе редакции узла прочитаны из вектор-слоя"],
            missing=["подтверждение, что различие не артефакт сопоставления"],
            subject=change.get("subject_ref") or change.get("subject"),
            summary=summary,
            routing_payload={"change_id": change_id},
        ))
    return entries


def build_inventory(
    *,
    synthesis: Mapping[str, Any] | None,
    preparation: Mapping[str, Any] | None = None,
    electrical_table_changes: Mapping[str, Any] | None = None,
    document_inconsistencies: Mapping[str, Any] | None = None,
    load_tables: Mapping[str, Any] | None = None,
    change_is_review: Any = None,
    change_describe: Any = None,
    pair_id: str = "",
    mode: str = "",
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Полная инвентаризация нерешённого: что уезжает модели и почему остальное — нет.

    Функция ничего не вызывает и ничего не решает про сам проект: она читает
    уже готовые артефакты и приписывает каждому нерешённому элементу маршрут.
    Ноль обращений к модели — поэтому её можно строить и в режиме «Быстро».
    """
    review_items = list((synthesis or {}).get("review_items") or ())
    changes = list((synthesis or {}).get("changes") or ())

    items: list[dict[str, Any]] = []
    items += _text_review_entries(review_items, preparation)
    items += _table_entries(electrical_table_changes, load_tables)
    items += _consistency_entries(document_inconsistencies)
    if change_is_review is not None:
        items += _change_entries(
            changes, change_is_review, change_describe or (lambda value: ""),
        )

    unresolved = [item for item in items if item["unresolved"]]
    counts: dict[str, int] = {
        "total": len(items),
        "unresolved_total": len(unresolved),
        # Сколько допущенных ДЕЙСТВИТЕЛЬНО уехало модели. Разница с
        # AI_ELIGIBLE — это ровно тот долг, который видно в артефакте, а не
        # только в коде.
        "routed_to_ai": sum(1 for item in unresolved if item["routed_to_ai"]),
        ROUTE_NOT_IMPLEMENTED: sum(
            1 for item in unresolved
            if item["route_status"] == ROUTE_NOT_IMPLEMENTED
        ),
    }
    for decision in DECISIONS:
        counts[decision] = sum(
            1 for item in unresolved if item["decision"] == decision
        )
    by_kind: dict[str, dict[str, int]] = {}
    for item in unresolved:
        bucket = by_kind.setdefault(
            item["kind"], {decision: 0 for decision in DECISIONS}
        )
        bucket[item["decision"]] += 1

    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "mode": mode,
        "generated_at": generated_at,
        "items": sorted(items, key=lambda value: (value["kind"], value["item_id"])),
        "counts": counts,
        "by_kind": by_kind,
        "decisions": list(DECISIONS),
        "constraints": {"uses_model": False, "is_deterministic": True},
        "provenance": {"producer": PRODUCER},
    }


def eligible_ids(inventory: Mapping[str, Any], kind: str | None = None) -> list[str]:
    """Идентификаторы элементов, которым разрешён разбор моделью."""
    return [
        str(item.get("item_id") or "")
        for item in inventory.get("items") or ()
        if isinstance(item, Mapping)
        and item.get("decision") == ELIGIBLE
        and item.get("routed_to_ai", True)
        and item.get("unresolved", True)
        and (kind is None or item.get("kind") == kind)
    ]


def entries_by_id(inventory: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(item.get("item_id") or ""): dict(item)
        for item in inventory.get("items") or ()
        if isinstance(item, Mapping)
    }


__all__ = [
    "CANDIDATE_LIMIT",
    "DECISIONS",
    "ELIGIBLE",
    "HUMAN_CATEGORY",
    "INELIGIBLE_EVIDENCE",
    "INELIGIBLE_POLICY",
    "KIND",
    "KINDS",
    "KIND_CHANGE_REVIEW",
    "KIND_CONSISTENCY_REVIEW",
    "KIND_TABLE_BLOCKED",
    "KIND_TABLE_UNPROVEN",
    "KIND_TEXT_REVIEW",
    "NOT_IMPLEMENTED_NOTE",
    "ROUTED_KINDS",
    "ROUTE_IMPLEMENTED",
    "ROUTE_NOT_ELIGIBLE",
    "ROUTE_NOT_IMPLEMENTED",
    "RETRIEVAL_LIMIT",
    "RETRIEVAL_MIN_COVERAGE",
    "STRONG_TOKEN_LENGTH",
    "SCHEMA_VERSION",
    "build_inventory",
    "counterpart_candidates",
    "coverage",
    "eligible_ids",
    "entries_by_id",
    "matched_row_ids",
    "retrieve_lines",
    "row_designations",
    "row_identity_text",
    "similarity",
    "strong_tokens",
    "tokens",
]
