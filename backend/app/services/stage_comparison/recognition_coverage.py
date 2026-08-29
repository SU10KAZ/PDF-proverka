"""Полнота распознавания: сколько текста листа система реально прочитала.

Боевой инвариант, ради которого написан этот модуль:

    ОТСУТСТВИЕ РАСПОЗНАННОГО ДОКАЗАТЕЛЬСТВА
    НЕ ЯВЛЯЕТСЯ ДОКАЗАТЕЛЬСТВОМ ОТСУТСТВИЯ.

Пропуск Markdown, ошибка распознавания, недочитанная таблица и неизвлечённый
фрагмент не имеют права сами по себе становиться «удалено», «добавлено»,
«изменено» и уж тем более «существенное изменение».

Как выглядит цена ошибки на реальной паре АР. Markdown правой редакции
содержал «З15.1» и «З15.2» — кириллическая «З» вместо цифры «3». В нативном
текстовом слое того же PDF на том же листе стояли правильные «315.1» и «315.2»
с теми же названиями и площадями 19.92 и 19.72. Система, сравнивавшая ТОЛЬКО
Markdown, честно не нашла строк слева справа и опубликовала четыре
«удалено» плюс четыре «добавлено» как существенные изменения проекта. Ни
одного изменения проекта там не было.

Отсюда конструкция: полнота считается по НЕЗАВИСИМЫМ друг от друга сигналам,
и ни один из них не является Markdown'ом, сравнение которого мы проверяем.

    A. подготовленный Markdown         — что система прочитала;
    B. нативный текстовый слой PDF     — что в документе есть на самом деле;
    C. привязка фрагмента к рамкам     — нашёлся ли фрагмент в слое вообще;
    D. структура таблицы               — доказан ли заголовок экспликации;
    E. ожидаемые идентификаторы группы — номера помещений, марки, площади;
    F. диагностика извлечения          — режим, выбранные страницы, объём.

Модель здесь не участвует ни на одном шаге: ни одного вызова, ни одного
токена. Это детерминированный замер, который можно перепроверить руками.

Уровни, на которых публикуется вердикт:

    документ → страница → группа сравнения → область доказательства

Статусы: SUFFICIENT / PARTIAL / INSUFFICIENT / UNKNOWN. «Достаточно» —
единственный, при котором расхождение имеет право стать содержательным
изменением; остальные три означают «покажи инженеру и не утверждай».

Чего этот модуль НЕ делает. Он не становится вторым источником фактов:
правило проекта — текст берётся только из Markdown, и нативный слой здесь
работает исключительно как контроль, а не как замена. Он не чинит
распознавание. И он не отменяет расхождение: элемент остаётся на месте, но
перестаёт называться доказанным.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable, Mapping, Sequence

CONTRACT_VERSION = "recognition-coverage.v1"

SUFFICIENT = "SUFFICIENT"
PARTIAL = "PARTIAL"
INSUFFICIENT = "INSUFFICIENT"
UNKNOWN = "UNKNOWN"
STATUSES = (SUFFICIENT, PARTIAL, INSUFFICIENT, UNKNOWN)

#: Порядок ухудшения: из нескольких вердиктов побеждает самый плохой, а не
#: самый частый. Полнота — это утверждение о худшем месте, а не среднее.
_SEVERITY = {INSUFFICIENT: 0, PARTIAL: 1, UNKNOWN: 2, SUFFICIENT: 3}

#: Ниже этого числа символов нативный слой страницы считается непригодным:
#: чертёж со шрифтами, переведёнными в кривые, или скан. Тогда независимого
#: сигнала нет вовсе, и честный ответ — UNKNOWN, а не «полнота достаточна».
MIN_NATIVE_CHARS = 40

#: Доли распознанного на странице, при которых страница считается прочитанной.
#: Пороги намеренно мягкие: жёсткий порог переводит в проверку весь корпус, а
#: решение по конкретному расхождению принимают прямые признаки ниже, а не эта
#: усреднённая величина.
PAGE_AGREEMENT_SUFFICIENT = 0.9
PAGE_AGREEMENT_PARTIAL = 0.6
PAGE_LOCATED_SUFFICIENT = 0.75

#: Предел размера индекса страницы. Лист с гигантской таблицей не должен
#: раздувать артефакт подготовки; усечение честно помечается.
MAX_TOKENS_PER_PAGE = 6000

# ── Причины ───────────────────────────────────────────────────────────────
REASON_NO_TEXT_LAYER = "native_text_layer_unusable"
REASON_NO_FRAGMENTS = "side_recognized_nothing_on_page"
REASON_OWN_SIDE_MISMATCH = "own_side_recognition_mismatch"
REASON_OPPOSITE_CONTAINS_VALUE = "opposite_side_native_text_contains_value"
REASON_OPPOSITE_CONTAINS_PART = "opposite_side_native_text_contains_part_of_value"
REASON_OPPOSITE_NOT_RECOGNIZED = "opposite_side_not_recognized"
REASON_PAGE_PARTIAL = "page_recognition_partial"
REASON_PAGE_INSUFFICIENT = "page_recognition_insufficient"
REASON_NO_SALIENT_TOKENS = "value_has_no_checkable_identifiers"
REASON_NO_INDEX = "recognition_index_absent"

#: Единая причина, по которой элемент не имеет права стать существенным.
REASON_COVERAGE_NOT_PROVEN = "recognition_coverage_not_proven"

_DASHES = "–—−‒―"


def normalize(value: Any) -> str:
    """Снять то, что не является различием распознавания.

    Регистр, пробелы, разделитель дробной части, типографские дефисы. Буквы
    НЕ сворачиваются в похожие по начертанию: кириллическая «З» и цифра «3» —
    это ровно тот сигнал, ради которого модуль написан, и склеить их значило
    бы ослепить проверку в единственном месте, где она нужна.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = text.replace(",", ".")
    for dash in _DASHES:
        text = text.replace(dash, "-")
    return re.sub(r"\s+", " ", text).strip()


_TOKEN_RE = re.compile(r"[\w./\-]+", re.UNICODE)
_HAS_DIGIT_RE = re.compile(r"\d")


def salient_tokens(value: Any) -> set[str]:
    """Токены, по которым вообще можно что-то проверить.

    Значимым считается токен, в котором есть цифра: номер помещения «315.1»,
    площадь «19.92», марка «ei60», отметка «-1.800». Слова без цифр в проверку
    не идут — они одинаковы у соседних строк таблицы и ничего не доказывают.
    """
    output: set[str] = set()
    for token in _TOKEN_RE.findall(normalize(value)):
        token = token.strip("./-")
        if len(token) >= 2 and _HAS_DIGIT_RE.search(token):
            output.add(token)
    return output


def checkable_tokens(value: Any) -> set[str]:
    """Токены, по которым можно судить о ПРИСУТСТВИИ значения на листе.

    От значимых отличаются тем, что из них убран шум, повторяющийся в каждой
    строке таблицы: единица «м2», одиночная цифра колонки, категория «в2».
    Требуются минимум две цифры и длина от трёх символов — этому отвечают
    номер помещения «315.1», площадь «19.92», отметка «-1.800», марка «ei60»,
    но не «м2», по которому иначе «совпадала» бы любая строка экспликации с
    любой другой.
    """
    return {
        token for token in salient_tokens(value)
        if len(token) >= 3 and len(_HAS_DIGIT_RE.findall(token)) >= 2
    }


def _compact(token: str) -> str:
    """Токен без разделителей: «315.1» → «3151».

    Нативный слой иногда разрывает число по словам, и строгое сравнение по
    токенам объявило бы отсутствующим то, что на листе есть. Сжатая форма
    используется ТОЛЬКО чтобы признать значение присутствующим — то есть
    всегда в сторону осторожности.
    """
    return re.sub(r"[./\-]", "", token)


def _presence_set(tokens: Iterable[str]) -> set[str]:
    values = set()
    for token in tokens:
        values.add(token)
        values.add(_compact(token))
    return values


# ── Индекс нативного слоя ─────────────────────────────────────────────────

def build_page_index(page_text: str) -> dict[str, Any]:
    """Компактный слепок нативного текста ОДНОЙ страницы.

    Хранится не сам текст, а то, что нужно проверке: объём (пригоден ли слой
    вообще) и множество значимых токенов. Полный текст листа удвоил бы
    артефакт подготовки, ничего не добавив к ответу на вопрос «есть ли на той
    стороне номер 315.1».
    """
    normalized = normalize(page_text)
    tokens = sorted(salient_tokens(normalized))
    truncated = len(tokens) > MAX_TOKENS_PER_PAGE
    return {
        "char_count": len(normalized),
        "salient_tokens": tokens[:MAX_TOKENS_PER_PAGE],
        "truncated": truncated,
        "has_text_layer": len(normalized) >= MIN_NATIVE_CHARS,
    }


def build_native_index(
    pdf_path: str,
    pages: Iterable[int],
    *,
    fitz: Any,
) -> dict[str, dict[str, Any]]:
    """Прочитать нативный текстовый слой выбранных страниц. Без модели и OCR."""
    wanted = sorted({int(page) for page in pages or ()})
    output: dict[str, dict[str, Any]] = {}
    if not wanted:
        return output
    with fitz.open(str(pdf_path)) as document:
        for number in wanted:
            if number < 1 or number > document.page_count:
                continue
            try:
                text = document[number - 1].get_text("text")
            except Exception:  # noqa: BLE001 — нечитаемая страница не роняет прогон
                text = ""
            output[str(number)] = build_page_index(text)
    return output


def _page_entry(
    index: Mapping[str, Any] | None, side: str, page: Any
) -> dict[str, Any] | None:
    if not isinstance(index, Mapping):
        return None
    pages = index.get(str(side).upper())
    if not isinstance(pages, Mapping):
        return None
    entry = pages.get(str(page))
    return entry if isinstance(entry, Mapping) else None


def native_tokens_for(
    index: Mapping[str, Any] | None,
    side: str,
    pages: Iterable[Any],
    *,
    compact: bool = True,
) -> tuple[set[str], bool]:
    """Значимые токены нативного слоя стороны и признак «слой пригоден».

    ``compact`` добавляет к строгим токенам их форму без разделителей, из-за
    чего «21.50» начинает совпадать с «2150». Это ослабление, и включать его
    можно только там, где «нашлось» ведёт к осторожности: при проверке
    ПРОТИВОПОЛОЖНОЙ стороны найденное значение отменяет утверждение об
    удалении. При проверке СВОЕЙ стороны всё наоборот — найденное значение
    подтверждает вывод, и там сравнение обязано быть строгим, иначе потерянная
    при распознавании запятая пройдёт как совпадение.
    """
    tokens: set[str] = set()
    usable = False
    for page in pages or ():
        entry = _page_entry(index, side, page)
        if entry is None:
            continue
        if entry.get("has_text_layer"):
            usable = True
        tokens.update(str(value) for value in entry.get("salient_tokens") or ())
    return (_presence_set(tokens) if compact else tokens), usable


# ── Уровень фрагмента ─────────────────────────────────────────────────────

def fragment_recognition(fragment: Mapping[str, Any]) -> dict[str, Any]:
    """Согласуется ли прочитанный Markdown с нативным текстом ПОД ТЕМИ ЖЕ рамками.

    ``pdf_canonical_text`` кладёт привязка фрагментов: это фактический текст
    PDF в границах, которые система назначила фрагменту. Расхождение значимых
    токенов здесь — прямое доказательство ошибки распознавания, а не гипотеза.
    """
    located = bool(fragment.get("bboxes"))
    native = fragment.get("pdf_canonical_text")
    expected = salient_tokens(fragment.get("canonical_text") or fragment.get("text"))
    if not located or not native:
        return {
            "located": located,
            "agreement": UNKNOWN,
            "missing_tokens": [],
        }
    present = _presence_set(salient_tokens(native))
    missing = sorted(token for token in expected if token not in present)
    return {
        "located": True,
        "agreement": SUFFICIENT if not missing else INSUFFICIENT,
        "missing_tokens": missing[:10],
    }


# ── Уровень страницы ──────────────────────────────────────────────────────

def page_coverage(
    fragments: Sequence[Mapping[str, Any]],
    native_entry: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Вердикт по одной странице одной стороны."""
    if native_entry is None:
        return {
            "status": UNKNOWN,
            "reason_codes": [REASON_NO_INDEX],
            "fragments": len(fragments),
            "located": 0,
            "agreeing": 0,
        }
    if not native_entry.get("has_text_layer"):
        # Скан или чертёжные шрифты в кривых: независимого сигнала нет.
        return {
            "status": UNKNOWN,
            "reason_codes": [REASON_NO_TEXT_LAYER],
            "fragments": len(fragments),
            "located": 0,
            "agreeing": 0,
            "native_chars": int(native_entry.get("char_count") or 0),
        }
    total = len(fragments)
    if total == 0:
        # В PDF текст есть, а система не прочитала ничего. Утверждать по такой
        # странице «этой строки здесь нет» нельзя ни при каких обстоятельствах.
        return {
            "status": INSUFFICIENT,
            "reason_codes": [REASON_NO_FRAGMENTS],
            "fragments": 0,
            "located": 0,
            "agreeing": 0,
            "native_chars": int(native_entry.get("char_count") or 0),
        }
    located = 0
    agreeing = 0
    checkable = 0
    for fragment in fragments:
        verdict = fragment_recognition(fragment)
        located += int(verdict["located"])
        if verdict["agreement"] == UNKNOWN:
            continue
        checkable += 1
        agreeing += int(verdict["agreement"] == SUFFICIENT)
    located_ratio = located / total
    agreement_ratio = (agreeing / checkable) if checkable else 0.0
    reasons: list[str] = []
    if checkable == 0:
        status = UNKNOWN
        reasons.append(REASON_NO_TEXT_LAYER)
    elif (
        agreement_ratio >= PAGE_AGREEMENT_SUFFICIENT
        and located_ratio >= PAGE_LOCATED_SUFFICIENT
    ):
        status = SUFFICIENT
    elif agreement_ratio >= PAGE_AGREEMENT_PARTIAL:
        status = PARTIAL
        reasons.append(REASON_PAGE_PARTIAL)
    else:
        status = INSUFFICIENT
        reasons.append(REASON_PAGE_INSUFFICIENT)
    return {
        "status": status,
        "reason_codes": reasons,
        "fragments": total,
        "located": located,
        "agreeing": agreeing,
        "checkable": checkable,
        "located_ratio": round(located_ratio, 4),
        "agreement_ratio": round(agreement_ratio, 4),
        "native_chars": int(native_entry.get("char_count") or 0),
    }


def worst(statuses: Iterable[str]) -> str:
    values = [status for status in statuses if status in _SEVERITY]
    if not values:
        return UNKNOWN
    return min(values, key=lambda status: _SEVERITY[status])


def aggregate(entries: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Свести вердикты нижнего уровня в один вердикт верхнего."""
    values = list(entries)
    if not values:
        return {"status": UNKNOWN, "reason_codes": [REASON_NO_INDEX], "counts": {}}
    counts: dict[str, int] = {}
    reasons: set[str] = set()
    for entry in values:
        status = str(entry.get("status") or UNKNOWN)
        counts[status] = counts.get(status, 0) + 1
        reasons.update(str(code) for code in entry.get("reason_codes") or ())
    return {
        "status": worst(counts),
        "reason_codes": sorted(reasons),
        "counts": dict(sorted(counts.items())),
    }


# ── Уровень области доказательства ────────────────────────────────────────

def _own_side_verdict(
    value: Any,
    side: str,
    pages: Sequence[Any],
    index: Mapping[str, Any] | None,
    page_status: str,
) -> tuple[str, list[str], dict[str, Any]]:
    """Есть ли то, что мы цитируем, в нативном слое СВОЕЙ стороны.

    Проверка применима не всегда. На листе, где чертёжные шрифты переведены в
    кривые, нативного текста нет вовсе или есть один штамп — тогда любая
    строка таблицы «отсутствует» в слое, и сравнение с ним доказывало бы не
    ошибку распознавания, а собственную неприменимость. Такой лист получает
    UNKNOWN: независимого сигнала нет, и делать вид, что он есть, нельзя.
    """
    native, usable = native_tokens_for(index, side, pages, compact=False)
    if not usable or page_status == UNKNOWN:
        return UNKNOWN, [REASON_NO_TEXT_LAYER], {"checked": False}
    expected = checkable_tokens(value)
    if not expected:
        # Проверить нечем — решает вердикт страницы.
        return page_status, (
            [REASON_NO_SALIENT_TOKENS] if page_status != SUFFICIENT else []
        ), {"checked": False}
    missing = sorted(token for token in expected if token not in native)
    if missing:
        return INSUFFICIENT, [REASON_OWN_SIDE_MISMATCH], {
            "checked": True, "missing_tokens": missing[:10],
        }
    return SUFFICIENT, [], {"checked": True, "missing_tokens": []}


def _opposite_side_verdict(
    value: Any,
    side: str,
    pages: Sequence[Any],
    index: Mapping[str, Any] | None,
    page_status: str,
    opposite_fragment_count: int,
) -> tuple[str, list[str], dict[str, Any]]:
    """Доказано ли, что этого значения на противоположной стороне НЕТ."""
    native, usable = native_tokens_for(index, side, pages)
    if not usable or page_status == UNKNOWN:
        return UNKNOWN, [REASON_NO_TEXT_LAYER], {"checked": False}
    expected = checkable_tokens(value)
    if expected:
        present = [token for token in sorted(expected) if token in native]
        if len(present) == len(expected):
            # То, что «исчезло», лежит в текстовом слое противоположного листа.
            # Разошлось распознавание, а не проект.
            return INSUFFICIENT, [REASON_OPPOSITE_CONTAINS_VALUE], {
                "checked": True, "present_tokens": present[:10],
            }
        if present:
            return PARTIAL, [REASON_OPPOSITE_CONTAINS_PART], {
                "checked": True, "present_tokens": present[:10],
            }
    if opposite_fragment_count == 0:
        return INSUFFICIENT, [REASON_OPPOSITE_NOT_RECOGNIZED], {"checked": True}
    if not expected:
        return page_status, (
            [REASON_NO_SALIENT_TOKENS] if page_status != SUFFICIENT else []
        ), {"checked": False}
    return page_status, (
        [] if page_status == SUFFICIENT else [REASON_PAGE_PARTIAL]
    ), {"checked": True, "present_tokens": []}


def evidence_coverage(
    *,
    bucket: str,
    item: Mapping[str, Any],
    group: Mapping[str, Any],
    index: Mapping[str, Any] | None,
    page_status: Mapping[str, Mapping[str, str]],
    fragment_counts: Mapping[str, Mapping[str, int]],
) -> dict[str, Any]:
    """Вердикт по одному расхождению Stage 3.

    ``bucket`` — корзина Stage 3: changed / removed / added.
    ``page_status`` — вердикты страниц вида {"LEFT": {"7": "SUFFICIENT"}}.
    """
    group_left = [int(page) for page in group.get("left_pages") or ()]
    group_right = [int(page) for page in group.get("right_pages") or ()]
    item_left = [int(page) for page in item.get("left_pages") or ()] or group_left
    item_right = [int(page) for page in item.get("right_pages") or ()] or group_right

    def status_of(side: str, pages: Sequence[int]) -> str:
        values = [
            str((page_status.get(side) or {}).get(str(page)) or UNKNOWN)
            for page in pages
        ]
        return worst(values)

    def fragments_on(side: str, pages: Sequence[int]) -> int:
        counts = fragment_counts.get(side) or {}
        return sum(int(counts.get(str(page)) or 0) for page in pages)

    verdicts: list[str] = []
    reasons: set[str] = set()
    signals: dict[str, Any] = {}

    if item.get("before") is not None:
        status, codes, detail = _own_side_verdict(
            item.get("before"), "LEFT", item_left, index,
            status_of("LEFT", item_left),
        )
        verdicts.append(status)
        reasons.update(codes)
        signals["left_own"] = {"status": status, **detail}
    if item.get("after") is not None:
        status, codes, detail = _own_side_verdict(
            item.get("after"), "RIGHT", item_right, index,
            status_of("RIGHT", item_right),
        )
        verdicts.append(status)
        reasons.update(codes)
        signals["right_own"] = {"status": status, **detail}

    if bucket == "removed":
        status, codes, detail = _opposite_side_verdict(
            item.get("before"), "RIGHT", group_right, index,
            status_of("RIGHT", group_right),
            fragments_on("RIGHT", group_right),
        )
        verdicts.append(status)
        reasons.update(codes)
        signals["opposite"] = {"side": "RIGHT", "status": status, **detail}
    elif bucket == "added":
        status, codes, detail = _opposite_side_verdict(
            item.get("after"), "LEFT", group_left, index,
            status_of("LEFT", group_left),
            fragments_on("LEFT", group_left),
        )
        verdicts.append(status)
        reasons.update(codes)
        signals["opposite"] = {"side": "LEFT", "status": status, **detail}

    status = worst(verdicts) if verdicts else UNKNOWN
    if status != SUFFICIENT:
        reasons.add(REASON_COVERAGE_NOT_PROVEN)
    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "reason_codes": sorted(reasons),
        "signals": signals,
    }


# ── Сборка артефакта ──────────────────────────────────────────────────────

def _fragments_by_side_page(
    preparation: Mapping[str, Any],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    output: dict[str, dict[str, list[dict[str, Any]]]] = {"LEFT": {}, "RIGHT": {}}
    raw = preparation.get("fragments")
    if not isinstance(raw, Mapping):
        return output
    for key, side in (("left", "LEFT"), ("right", "RIGHT")):
        for fragment in raw.get(key) or ():
            if not isinstance(fragment, Mapping):
                continue
            page = str(int(fragment.get("pdf_page") or 0))
            output[side].setdefault(page, []).append(dict(fragment))
    return output


def build_recognition_coverage(
    preparation: Mapping[str, Any],
    text_differences: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Полный вердикт полноты: документ, страница, группа, доказательство."""
    from .text_semantic_validation import (  # локальный импорт: без цикла
        STAGE3_SEMANTIC_BUCKETS,
        iter_stage3_evidence,
    )

    index = preparation.get("recognition_index")
    index = index if isinstance(index, Mapping) else None
    by_side_page = _fragments_by_side_page(preparation)

    pages: dict[str, dict[str, Any]] = {"LEFT": {}, "RIGHT": {}}
    fragment_counts: dict[str, dict[str, int]] = {"LEFT": {}, "RIGHT": {}}
    page_status: dict[str, dict[str, str]] = {"LEFT": {}, "RIGHT": {}}
    for side in ("LEFT", "RIGHT"):
        known = set(by_side_page[side])
        if isinstance(index, Mapping) and isinstance(index.get(side), Mapping):
            known |= set(index[side])
        for page in sorted(known, key=lambda value: int(value or 0)):
            fragments = by_side_page[side].get(page) or []
            verdict = page_coverage(fragments, _page_entry(index, side, page))
            pages[side][page] = verdict
            page_status[side][page] = verdict["status"]
            fragment_counts[side][page] = len(fragments)

    documents = {
        side: aggregate(pages[side].values()) for side in ("LEFT", "RIGHT")
    }

    groups: dict[str, Any] = {}
    for group in preparation.get("comparison_groups") or ():
        if not isinstance(group, Mapping):
            continue
        group_id = str(group.get("id") or "")
        sides = {}
        for side, key in (("LEFT", "left_pages"), ("RIGHT", "right_pages")):
            entries = [
                pages[side][str(int(page))]
                for page in group.get(key) or ()
                if str(int(page)) in pages[side]
            ]
            sides[side] = aggregate(entries)
        groups[group_id] = {
            "status": worst(value["status"] for value in sides.values()),
            "sides": sides,
        }

    evidence: dict[str, Any] = {}
    if isinstance(text_differences, Mapping):
        for source_ref, group, bucket, item in iter_stage3_evidence(
            text_differences, buckets=STAGE3_SEMANTIC_BUCKETS
        ):
            evidence[source_ref] = evidence_coverage(
                bucket=bucket,
                item=item,
                group=group,
                index=index,
                page_status=page_status,
                fragment_counts=fragment_counts,
            )

    status_counts: dict[str, int] = {}
    for value in evidence.values():
        code = str(value["status"])
        status_counts[code] = status_counts.get(code, 0) + 1
    return {
        "contract_version": CONTRACT_VERSION,
        "index_available": index is not None,
        "documents": documents,
        "pages": pages,
        "groups": groups,
        "by_evidence": evidence,
        "diagnostics": {
            "evidence_status_counts": dict(sorted(status_counts.items())),
            "pages_by_side": {
                side: len(pages[side]) for side in ("LEFT", "RIGHT")
            },
            "uses_model": False,
            "uses_ocr": False,
        },
    }


def coverage_of(
    text_differences: Mapping[str, Any] | None,
    source_evidence_ref: str,
) -> dict[str, Any]:
    """Вердикт по одному доказательству из опубликованного артефакта Stage 3.

    Отсутствие вердикта — это UNKNOWN, а не «достаточно»: артефакт, собранный
    сборкой без этой проверки, не имеет права молча получить зелёный свет.
    """
    coverage = (text_differences or {}).get("recognition_coverage")
    if isinstance(coverage, Mapping):
        value = (coverage.get("by_evidence") or {}).get(source_evidence_ref)
        if isinstance(value, Mapping):
            return dict(value)
    return {
        "contract_version": CONTRACT_VERSION,
        "status": UNKNOWN,
        "reason_codes": [REASON_NO_INDEX, REASON_COVERAGE_NOT_PROVEN],
        "signals": {},
    }


def is_sufficient(coverage: Mapping[str, Any] | None) -> bool:
    return bool(isinstance(coverage, Mapping) and coverage.get("status") == SUFFICIENT)


__all__ = [
    "CONTRACT_VERSION",
    "INSUFFICIENT",
    "MAX_TOKENS_PER_PAGE",
    "MIN_NATIVE_CHARS",
    "PARTIAL",
    "REASON_COVERAGE_NOT_PROVEN",
    "REASON_NO_FRAGMENTS",
    "REASON_NO_INDEX",
    "REASON_NO_SALIENT_TOKENS",
    "REASON_NO_TEXT_LAYER",
    "REASON_OPPOSITE_CONTAINS_PART",
    "REASON_OPPOSITE_CONTAINS_VALUE",
    "REASON_OPPOSITE_NOT_RECOGNIZED",
    "REASON_OWN_SIDE_MISMATCH",
    "REASON_PAGE_INSUFFICIENT",
    "REASON_PAGE_PARTIAL",
    "STATUSES",
    "SUFFICIENT",
    "UNKNOWN",
    "aggregate",
    "checkable_tokens",
    "build_native_index",
    "build_page_index",
    "build_recognition_coverage",
    "coverage_of",
    "evidence_coverage",
    "fragment_recognition",
    "is_sufficient",
    "native_tokens_for",
    "normalize",
    "page_coverage",
    "salient_tokens",
    "worst",
]
