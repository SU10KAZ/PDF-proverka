"""Разрешение ИДЕНТИЧНОСТИ строк таблиц нагрузок — и ничего кроме неё.

Почему модель спрашивают только про тождество, а не про изменение.

Потому что вопрос «что изменилось у этого объекта» отдаёт модели два решения
сразу: какие строки считать одним объектом и какие значения у него разошлись.
Второе система умеет считать сама, детерминированно и без единого обращения к
модели — этим занимается ``electrical_table_diff.compare_match``. Отдавая его
модели, мы платили бы за пересказ уже посчитанного и заодно открывали место
для выдуманного числа: «500А→3200А» из истории этого раздела появилось ровно
так.

Поэтому разделение жёсткое:

    модель  → «строка L2 и строка R1 — один объект» (или «нет», или «не знаю»)
    Python  → сверяет, что обе строки существуют, что каждая цитата лежит
              в названной строке дословно и что арифметика, на которую
              сослалась модель, сходится
    Python  → строит изменения параметров ТЕМ ЖЕ кодом, что и для пары,
              найденной детерминированным матчером

Поверхность для выдумки сжата до выбора из перечисленных вариантов. Модель не
может назвать строку, которой нет в пакете, не может сослаться на число,
которого нет в названной строке, и не может предъявить арифметику, которая не
сходится: всё это проверяется в Python, а провал проверки не публикуется
никогда.

Что модель НЕ имеет права делать даже при доказанном тождестве: менять подпись
на листе. «2ГРЩ-ВРУ3» с расчётной мощностью 183,9 кВт может оказаться линией
ВРУ1 — это доказывается суммой 181,8 + 183,9 = 365,7, совпадающей со сводной
строкой ВРУ1, — но исправлять чертёж система не будет. Она предложит пару, и
решение останется за инженером.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from ..production_artifacts import content_signature, stable_id
from . import routing

SCHEMA_VERSION = "stage-comparison-ai-identity.v1"
PROMPT_VERSION = "stage-comparison-ai-identity.v2"
VERIFIER_VERSION = "stage-comparison-ai-identity-verifier.v1"

#: Метод сопоставления, которым помечается пара, предложенная моделью. В
#: перечень методов детерминированного матчера он НЕ добавляется намеренно:
#: matcher про него ничего не знает и знать не должен, а `_confidence` там
#: уводит незнакомый метод в MEDIUM — ровно туда, где паре и место.
METHOD_AI_IDENTITY = "AI_IDENTITY"

VERDICT_SAME = "SAME_ENTITY"
VERDICT_DIFFERENT = "DIFFERENT_ENTITY"
VERDICT_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
VERDICT_NEED_EVIDENCE = "NEED_MORE_EVIDENCE"
VERDICTS = (
    VERDICT_SAME,
    VERDICT_DIFFERENT,
    VERDICT_INSUFFICIENT,
    VERDICT_NEED_EVIDENCE,
)

CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW", "UNKNOWN")

# ── Справочник запросов на доборы доказательств ────────────────────────────
# Перечень закрыт. Свободная строка здесь означала бы, что модель сама
# назначает себе следующий кусок листа, а бэкенд её исполняет: это ровно тот
# неограниченный доступ к документу, ради отказа от которого пакет и
# ограничивают.
NEED_SECTION_SUMMARY = "SECTION_SUMMARY_ROWS"
NEED_SAME_DESIGNATION = "SAME_DESIGNATION_ROWS"
NEED_NEIGHBOUR_ROWS = "NEIGHBOURING_ROWS"
NEED_CONTRADICTIONS = "ROW_CONTRADICTIONS"
NEEDED_EVIDENCE_TYPES = (
    NEED_SECTION_SUMMARY,
    NEED_SAME_DESIGNATION,
    NEED_NEIGHBOUR_ROWS,
    NEED_CONTRADICTIONS,
)
REQUESTED_SIDES = ("LEFT", "RIGHT", "BOTH")

#: Сколько строк добавляет ОДИН добор. Второго добора не бывает: если после
#: адресного расширения доказательств всё ещё не хватает, это ответ, а не
#: повод показать модели лист целиком.
EXPANSION_LIMIT = 8

#: Относительный допуск арифметической проверки. Расчётные величины на листе
#: округлены до десятых, поэтому точного равенства требовать нельзя; но и
#: широкий допуск превратил бы «сходится» в «примерно похоже».
ARITHMETIC_TOLERANCE = 0.005

_DASHES = "–—−"
_NUMBER_RE = re.compile(r"-?\d+(?:[.,]\d+)?")
#: Внутренние ссылки, которых в ответе модели быть не должно: их чеканит
#: бэкенд, и совпадение с настоящей было бы случайным.
_FORBIDDEN_REF_RE = re.compile(
    r"(etrow_|etm_|etchg_|ureview_|uchg_|tatom_|teva_|project_entity_)", re.I
)


def normalize(value: Any) -> str:
    """Снять форматирование, не трогая цифры и слова."""
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    text = text.replace(",", ".").replace("ё", "е")
    for dash in _DASHES:
        text = text.replace(dash, "-")
    return re.sub(r"\s+", " ", text)


def numbers_in(value: Any) -> list[float]:
    output: list[float] = []
    for raw in _NUMBER_RE.findall(str(value or "").replace(",", ".")):
        try:
            output.append(float(raw))
        except ValueError:
            continue
    return output


# ── Отрисовка строки таблицы ───────────────────────────────────────────────

_ROW_KIND_TITLE = {
    "CONSUMER_TOTAL": "суммарная строка потребителя",
    "FEEDER": "фидерная строка",
}


def render_row_line(row: Mapping[str, Any]) -> str:
    """Одна строка таблицы — одной текстовой строкой пакета.

    Верификатор потом ищет цитаты модели именно здесь, дословно. Поэтому
    отрисовка обязана быть детерминированной и включать всё, на что модель
    имеет право сослаться: подпись, обозначения, раздел, номер ввода, марку
    кабеля и сырую запись каждой величины ровно так, как она напечатана.
    """
    parts: list[str] = [f"«{row.get('consumer_label') or ''}»"]
    designations = routing.row_designations(row)
    if designations:
        parts.append("обозначения: " + ", ".join(designations))
    if row.get("section_ref"):
        parts.append(f"секция {row['section_ref']}")
    if row.get("input_number") is not None:
        parts.append(f"ввод {row['input_number']}")
    kind = str(row.get("row_kind") or "")
    if kind:
        parts.append(_ROW_KIND_TITLE.get(kind, kind))
    if row.get("mode_label"):
        parts.append(f"режим {row['mode_label']}")
    for cable in row.get("cables") or ():
        parts.append(f"кабель {cable}")
    for value in row.get("values") or ():
        raw = str(value.get("raw") or "").strip()
        if raw:
            parts.append(raw)
    return " | ".join(part for part in parts if str(part).strip())


def _row_view(ref: str, side: str, row: Mapping[str, Any], role: str) -> dict[str, Any]:
    return {
        "ref": ref,
        "side": side,
        "role": role,
        "text": render_row_line(row),
        "row_kind": row.get("row_kind"),
        "section": row.get("section_ref"),
        "input_number": row.get("input_number"),
        # Объявленные обозначения строки отдельным полем, а не только внутри
        # текста. Верификатор сверяет тождество именно с этим списком:
        # «ВРУ» — общая подстрока у ВРУ-А и ВРУ-АПТ, но обозначением не
        # является ни у одной из них, и связывать по ней две разные линии
        # значит доказывать тождество совпадением приставки.
        "designations": routing.row_designations(row),
    }


# ── Вопрос об идентичности ─────────────────────────────────────────────────

@dataclass
class IdentityQuestion:
    """Один вопрос: какая пара строк описывает один объект."""

    question_id: str
    kind: str
    subject: str
    section: str
    left: list[dict[str, Any]] = field(default_factory=list)
    right: list[dict[str, Any]] = field(default_factory=list)
    context: list[dict[str, Any]] = field(default_factory=list)
    contradictions: list[str] = field(default_factory=list)
    #: ref → row_id. Наружу к модели идентификаторы строк не уезжают: она
    #: отвечает адресами пакета, а бэкенд переводит их обратно сам.
    ref_to_row: dict[str, str] = field(default_factory=dict)
    source_item_id: str = ""

    def model_view(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "subject": self.subject,
            "section": self.section,
            "left_candidates": [dict(value) for value in self.left],
            "right_candidates": [dict(value) for value in self.right],
            "context_rows": [dict(value) for value in self.context],
            "known_contradictions": list(self.contradictions),
        }

    def lines(self) -> dict[str, dict[str, Any]]:
        output: dict[str, dict[str, Any]] = {}
        for line in [*self.left, *self.right, *self.context]:
            output[str(line["ref"])] = {
                **line,
                "normalized": normalize(line["text"]),
            }
        return output


@dataclass
class IdentityPackage:
    """Партия вопросов об идентичности, объединённых общим контекстом."""

    group_key: str
    questions: list[IdentityQuestion] = field(default_factory=list)

    def model_view(self) -> dict[str, Any]:
        """Партия для модели. Общий контекст печатается один раз.

        Опорный контекст у вопросов одной партии совпадает — это сводные
        строки листа. Повторять их в каждом вопросе значит платить за один и
        тот же текст столько раз, сколько в партии вопросов, и заодно топить
        сам вопрос в его окружении.
        """
        views = [question.model_view() for question in self.questions]
        shared: list[dict[str, Any]] = []
        first = views[0].get("context_rows") if views else []
        if views and all(view.get("context_rows") == first for view in views):
            shared = list(first or [])
            for view in views:
                view.pop("context_rows", None)
        return {
            "group": self.group_key,
            "context_rows": shared,
            "questions": views,
        }

    def digest(self) -> str:
        return content_signature(self.model_view())


def _rows_by_id(load_tables: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for side in ("LEFT", "RIGHT"):
        table = (load_tables or {}).get(side)
        for row in (table or {}).get("rows") or ():
            if isinstance(row, Mapping) and row.get("row_id"):
                output[str(row["row_id"])] = {**dict(row), "side": side}
    return output


def _totals_for(subject: Any, rows: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Сводные строки того же потребителя — опора для арифметики.

    Именно они делают вопрос про «2ГРЩ-ВРУ3» с 183,9 кВт решаемым: сумма
    фидеров обязана сойтись со сводной строкой своего потребителя, и это
    проверяемое утверждение, а не догадка по похожести подписи.
    """
    wanted = routing.tokens(subject)
    output: list[dict[str, Any]] = []
    for row in rows.values():
        if row.get("row_kind") != "CONSUMER_TOTAL":
            continue
        if wanted & routing.tokens(row.get("consumer_designation") or row.get("consumer_label")):
            output.append(row)
    return output


def _section_siblings(
    row: Mapping[str, Any],
    rows: Mapping[str, dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    section = normalize(row.get("section_ref"))
    side = row.get("side")
    output: list[dict[str, Any]] = []
    for other in rows.values():
        if other.get("side") != side or other.get("row_id") == row.get("row_id"):
            continue
        if not section or normalize(other.get("section_ref")) != section:
            continue
        output.append(other)
    output.sort(key=lambda value: str(value.get("row_id")))
    return output[:limit]


def _contradiction_texts(
    row_ids: Iterable[str],
    contradictions: Mapping[str, Any] | None,
) -> list[str]:
    wanted = {str(value) for value in row_ids}
    output: list[str] = []
    for side in ("LEFT", "RIGHT"):
        table = (contradictions or {}).get(side)
        for item in (table or {}).get("contradictions") or ():
            if not isinstance(item, Mapping):
                continue
            if str(item.get("row_id") or "") in wanted:
                summary = str(item.get("summary") or "").strip()
                if summary:
                    output.append(summary)
    return sorted(set(output))


def build_questions(
    *,
    inventory: Mapping[str, Any],
    load_tables: Mapping[str, Any] | None,
    context_limit: int = 6,
) -> list[IdentityQuestion]:
    """Вопросы об идентичности по элементам, которым разрешён разбор.

    Ровно те элементы, которым инвентаризация выдала AI_ELIGIBLE, и ровно тех
    видов, где вопрос сводится к выбору из перечисленного: строка без пары и
    заблокированное неоднозначное сопоставление.
    """
    rows = _rows_by_id(load_tables)
    questions: list[IdentityQuestion] = []
    for entry in inventory.get("items") or ():
        if not isinstance(entry, Mapping):
            continue
        if entry.get("decision") != routing.ELIGIBLE:
            continue
        kind = str(entry.get("kind") or "")
        payload = entry.get("routing_payload") or {}
        if kind == routing.KIND_TABLE_UNPROVEN:
            anchor = rows.get(str(payload.get("row_id") or ""))
            if anchor is None:
                continue
            candidate_ids = [
                str(value) for value in payload.get("candidate_row_ids") or ()
            ]
            side = str(anchor.get("side") or "LEFT")
            own = [anchor]
            others = [rows[value] for value in candidate_ids if value in rows]
            left_rows = own if side == "LEFT" else others
            right_rows = others if side == "LEFT" else own
        elif kind == routing.KIND_TABLE_BLOCKED:
            left_rows = [
                rows[str(value)] for value in payload.get("left_row_ids") or ()
                if str(value) in rows
            ]
            right_rows = [
                rows[str(value)] for value in payload.get("right_row_ids") or ()
                if str(value) in rows
            ]
            if not left_rows or not right_rows:
                continue
        else:
            continue

        question_id = stable_id(
            "idq",
            str(entry.get("item_id") or ""),
            *[str(row.get("row_id")) for row in [*left_rows, *right_rows]],
        )
        question = IdentityQuestion(
            question_id=question_id,
            kind=kind,
            subject=str(entry.get("subject") or ""),
            section=str(
                (left_rows or right_rows)[0].get("section_ref") or ""
            ),
            source_item_id=str(entry.get("item_id") or ""),
        )
        for index, row in enumerate(left_rows, start=1):
            ref = f"L{index}"
            question.left.append(_row_view(ref, "LEFT", row, "CANDIDATE"))
            question.ref_to_row[ref] = str(row.get("row_id"))
        for index, row in enumerate(right_rows, start=1):
            ref = f"R{index}"
            question.right.append(_row_view(ref, "RIGHT", row, "CANDIDATE"))
            question.ref_to_row[ref] = str(row.get("row_id"))
        questions.append(question)
    questions.sort(key=lambda value: value.question_id)
    return questions


#: Сколько сводных строк потребителей кладётся в опорный контекст. Их на
#: листе полтора десятка — это и есть весь набор возможных итогов, с
#: которыми обязана сойтись сумма фидеров. Предел стоит не ради экономии, а
#: чтобы лист с сотней потребителей не превратил пакет в сам лист.
BASE_CONTEXT_LIMIT = 20


def _designated_totals(rows: Mapping[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Сводные строки, у которых есть обозначение потребителя.

    Безымянные суммарные строки — это неразобранные полосы таблицы: сослаться
    на них нельзя, доказать ими ничего нельзя, а место в пакете они занимают.
    """
    output = [
        row for row in rows.values()
        if row.get("row_kind") == "CONSUMER_TOTAL"
        and str(row.get("consumer_designation") or "").strip()
    ]
    output.sort(key=lambda value: str(value.get("row_id")))
    return output


def attach_base_context(
    questions: Sequence[IdentityQuestion],
    *,
    load_tables: Mapping[str, Any] | None,
    contradictions: Mapping[str, Any] | None = None,
    context_limit: int = BASE_CONTEXT_LIMIT,
) -> None:
    """Опорный контекст вопроса: сводные строки потребителей и противоречия.

    Кладутся сводные строки ВСЕХ названных потребителей листа, а не только
    того, о ком вопрос. Иначе главный случай неразрешим: чтобы доказать, что
    линия, подписанная «2ГРЩ-ВРУ3» с Рр=183,9 кВт, на деле питает ВРУ1, нужна
    сводная строка ВРУ1 — то есть строка ЧУЖОГО потребителя. Своим набор
    ограничивался ровно до тех пор, пока вопрос считался вопросом о подписи, а
    не о том, куда сходится сумма.

    Соседи по секции сюда НЕ кладутся: их много, они похожи друг на друга, и
    именно они превращают вопрос «эти две строки один объект» в «разберись в
    таблице». Соседи добираются адресно и только по запросу модели.
    """
    rows = _rows_by_id(load_tables)
    totals = _designated_totals(rows)
    for question in questions:
        used = set(question.ref_to_row.values())
        own = routing.tokens(question.subject)
        ordered = sorted(
            totals,
            key=lambda row: (
                # Сводная строка своего потребителя идёт первой: с неё
                # начинается любая проверка суммы.
                0 if own & routing.tokens(row.get("consumer_designation")) else 1,
                str(row.get("row_id")),
            ),
        )
        index = 1
        for row in ordered[:context_limit]:
            if str(row.get("row_id")) in used:
                continue
            ref = f"C{index}"
            question.context.append(
                _row_view(ref, str(row.get("side")), row, "CONSUMER_TOTAL")
            )
            question.ref_to_row[ref] = str(row.get("row_id"))
            used.add(str(row.get("row_id")))
            index += 1
        question.contradictions = _contradiction_texts(
            question.ref_to_row.values(), contradictions
        )


def group_key_for(question: IdentityQuestion) -> str:
    """Чем объединяются вопросы в одну партию — разделом схемы.

    Раздел, а не семейство потребителя. Семейство давало партии по одному
    вопросу: на паре ГРЩ двадцать девять вопросов превращались в двадцать
    восемь обращений к модели при одном и том же опорном контексте в каждом.
    Строки одной секции объясняют друг друга, и сводные строки листа для них
    общие — платить за них по разу на вопрос незачем.
    """
    return question.section or "—"


def pack(
    questions: Sequence[IdentityQuestion],
    *,
    batch_size: int,
) -> list[IdentityPackage]:
    grouped: dict[str, list[IdentityQuestion]] = {}
    for question in questions:
        grouped.setdefault(group_key_for(question), []).append(question)
    packages: list[IdentityPackage] = []
    for key in sorted(grouped):
        items = sorted(grouped[key], key=lambda value: value.question_id)
        step = max(1, int(batch_size))
        for start in range(0, len(items), step):
            packages.append(IdentityPackage(key, items[start:start + step]))
    return packages


# ── Адресный добор доказательств ───────────────────────────────────────────

def expand(
    question: IdentityQuestion,
    request: Mapping[str, Any],
    *,
    load_tables: Mapping[str, Any] | None,
    contradictions: Mapping[str, Any] | None = None,
    limit: int = EXPANSION_LIMIT,
    exclude: Iterable[Any] = (),
) -> list[dict[str, Any]]:
    """Один добор доказательств по запросу модели — из закрытого справочника.

    Модель не читает файлы и не ищет сама: она называет ВИД недостающего
    доказательства из перечня, а какие именно строки этому виду отвечают,
    решает бэкенд. Запрос вне справочника исполняется как пустой — это не
    ошибка, а отказ.
    """
    kind = str(request.get("missing_evidence_type") or "")
    if kind not in NEEDED_EVIDENCE_TYPES:
        return []
    side = str(request.get("requested_side") or "BOTH").upper()
    if side not in REQUESTED_SIDES:
        side = "BOTH"
    rows = _rows_by_id(load_tables)
    # Строки, у которых пара уже доказана детерминированно, исключаются и
    # здесь. Отбор кандидатов их отсеивал, а добор — нет, и модель спокойно
    # выбирала пару к уже занятой строке: на паре ГРЩ так вернулись и ХМ1, и
    # ВРУ-А, из-за которых находка в отчёте удваивалась.
    known = set(question.ref_to_row.values()) | {str(value) for value in exclude}
    wanted = str(request.get("requested_entity") or question.subject)

    picked: list[dict[str, Any]] = []
    if kind == NEED_SECTION_SUMMARY:
        picked = _totals_for(wanted, rows)
    elif kind == NEED_SAME_DESIGNATION:
        tokens = routing.tokens(wanted)
        picked = [
            row for row in rows.values()
            if tokens & routing.tokens(
                " ".join(routing.row_designations(row))
                + " " + str(row.get("consumer_label") or "")
            )
        ]
    elif kind == NEED_NEIGHBOUR_ROWS:
        anchors = [
            rows[value] for value in question.ref_to_row.values() if value in rows
        ]
        for anchor in anchors:
            picked += _section_siblings(anchor, rows, limit=limit)
    elif kind == NEED_CONTRADICTIONS:
        # Противоречия — не строки: они уже уехали в вопросе списком. Добор
        # по ним расширяет перечень на строки, которые в них упомянуты.
        texts = _contradiction_texts(rows, contradictions)
        picked = [
            row for row in rows.values()
            if any(normalize(row.get("consumer_label")) in normalize(text)
                   for text in texts)
        ]

    if side in ("LEFT", "RIGHT"):
        picked = [row for row in picked if row.get("side") == side]
    picked = [row for row in picked if str(row.get("row_id")) not in known]
    picked.sort(key=lambda value: str(value.get("row_id")))

    added: list[dict[str, Any]] = []
    index = len(question.context) + 1
    for row in picked[:limit]:
        ref = f"C{index}"
        view = _row_view(ref, str(row.get("side")), row, f"EXPANDED:{kind}")
        question.context.append(view)
        question.ref_to_row[ref] = str(row.get("row_id"))
        added.append(view)
        index += 1
    return added


__all__ = [
    "ARITHMETIC_TOLERANCE",
    "BASE_CONTEXT_LIMIT",
    "EXPANSION_LIMIT",
    "IdentityPackage",
    "IdentityQuestion",
    "METHOD_AI_IDENTITY",
    "NEEDED_EVIDENCE_TYPES",
    "NEED_CONTRADICTIONS",
    "NEED_NEIGHBOUR_ROWS",
    "NEED_SAME_DESIGNATION",
    "NEED_SECTION_SUMMARY",
    "PROMPT_VERSION",
    "REQUESTED_SIDES",
    "SCHEMA_VERSION",
    "VERDICTS",
    "VERDICT_DIFFERENT",
    "VERDICT_INSUFFICIENT",
    "VERDICT_NEED_EVIDENCE",
    "VERDICT_SAME",
    "VERIFIER_VERSION",
    "attach_base_context",
    "build_questions",
    "expand",
    "group_key_for",
    "normalize",
    "numbers_in",
    "pack",
    "render_row_line",
]


# ── Схема ответа ───────────────────────────────────────────────────────────
# Схема — контракт, а не пожелание: обе CLI умеют принудительный
# структурированный вывод, поэтому поля, которого здесь нет, модель вернуть не
# может. Чего в схеме НЕТ намеренно: идентификаторов строк (модель отвечает
# адресами пакета L1/R2, а row_id подставляет бэкенд), координат, и любых
# значений параметров — их считает Python по проверенной паре.

_QUOTE = {
    "type": "object",
    "additionalProperties": False,
    "required": ["side", "row_ref", "quote"],
    "properties": {
        "side": {"type": "string", "enum": ["LEFT", "RIGHT"]},
        "row_ref": {
            "type": "string",
            "description": "Адрес строки пакета: L1, R2, C3. Копируется дословно.",
        },
        "quote": {
            "type": "string",
            "description": "Дословный кусок ИМЕННО этой строки пакета.",
        },
    },
}

_OPERAND = {
    "type": "object",
    "additionalProperties": False,
    "required": ["row_ref", "value"],
    "properties": {
        "row_ref": {"type": "string"},
        "value": {
            "type": "string",
            "description": "Число дословно так, как оно напечатано: «183,9».",
        },
    },
}

_NEED = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "missing_evidence_type", "requested_entity", "requested_side",
    ],
    "properties": {
        "missing_evidence_type": {
            "type": "string",
            "enum": list(NEEDED_EVIDENCE_TYPES),
            "description": "Вид недостающего доказательства из справочника.",
        },
        "requested_entity": {
            "type": "string",
            "description": "Обозначение потребителя, про который нужен добор.",
        },
        "requested_side": {"type": "string", "enum": list(REQUESTED_SIDES)},
    },
}

_IDENTITY = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "question_id", "verdict", "left_row_ref", "right_row_ref",
        "shared_identity", "arithmetic_total", "arithmetic_addends",
        "evidence_quotes", "confidence", "human_question",
        "engineering_summary", "need_more_evidence",
    ],
    "properties": {
        "question_id": {"type": "string"},
        "verdict": {"type": "string", "enum": list(VERDICTS)},
        "left_row_ref": {
            "type": ["string", "null"],
            "description": "Адрес выбранной строки ЛЕВОГО листа (L…).",
        },
        "right_row_ref": {
            "type": ["string", "null"],
            "description": "Адрес выбранной строки ПРАВОГО листа (R…).",
        },
        "shared_identity": {
            "type": ["string", "null"],
            "description": (
                "Дословная подстрока, которая есть В ОБЕИХ выбранных строках"
                " и доказывает тождество. Если тождество доказывается только"
                " арифметикой — null."
            ),
        },
        "arithmetic_total": {
            # Не anyOf: валидатор контракта поддерживает объявленное
            # подмножество JSON Schema и незнакомое ключевое слово считает
            # непроверяемым — то есть отклоняет ВЕСЬ ответ. Список типов
            # выражает то же самое и проверяется по-настоящему.
            **_OPERAND, "type": ["object", "null"],
            "description": (
                "Итог, с которым обязана сойтись сумма: величина сводной"
                " строки потребителя."
            ),
        },
        "arithmetic_addends": {
            "type": "array",
            "items": _OPERAND,
            "description": "Слагаемые. Каждое — дословно из названной строки.",
        },
        "evidence_quotes": {"type": "array", "items": _QUOTE},
        "confidence": {"type": "string", "enum": list(CONFIDENCE_LEVELS)},
        "human_question": {
            "type": ["string", "null"],
            "description": "Вопрос инженеру по-русски, если решать ему.",
        },
        "engineering_summary": {
            "type": "string",
            "description": "Одно-два предложения по-русски: что это значит.",
        },
        "need_more_evidence": {
            **_NEED, "type": ["object", "null"],
            "description": "Заполняется ТОЛЬКО при verdict = NEED_MORE_EVIDENCE.",
        },
    },
}

IDENTITY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["resolutions"],
    "properties": {
        "resolutions": {
            "type": "array",
            "description": "Ровно по одному ответу на каждый вопрос пакета.",
            "items": _IDENTITY,
        },
    },
}


IDENTITY_SYSTEM_PROMPT = (
    "Ты — инженер-электрик, читающий таблицы нагрузок однолинейной схемы. "
    "Ты отвечаешь на ОДИН вопрос: описывают ли две строки один и тот же "
    "объект. Ты не называешь, что у объекта изменилось, и не исправляешь "
    "подписи на листе."
)

_IDENTITY_RULES = """ПРАВИЛА

1. Твоя задача — ТОЛЬКО тождество. «Эта строка левого листа и эта строка
   правого описывают один инженерный объект» — да, нет или доказательств не
   хватает. Какие у объекта изменились мощность, ток и кабель, посчитает
   система сама по выбранной тобой паре. Не называй значений «до» и «после».
2. У каждой строки есть адрес: L1, L2… слева, R1, R2… справа, C1, C2… —
   контекст. Отвечай адресами, а не описаниями.
3. Любая цитата обязана лежать в НАЗВАННОЙ строке дословно. Цитата, которой в
   этой строке нет, отклоняет весь ответ.
4. Подпись — не доказательство сама по себе. «ШУ-ХЦ» слева и «ВРУ-ХЦ» справа
   могут быть одним шкафом, а «ДР2-ХМ2» и «ХМ2» — разными объектами. Доказывай
   одним из двух способов:
   а) shared_identity — обозначение, которое ЦЕЛИКОМ и ДОСЛОВНО значится в
      поле "designations" ОБЕИХ выбранных строк. Правый лист часто печатает
      старое обозначение в ряду над колонкой — тогда «ШУ-ХЦ» стоит в
      designations обеих строк, и это доказательство.
      Общий кусок разных обозначений доказательством НЕ является: «АПТ» из
      «ШУ-АПТ» и «ВРУ-АПТ», «ГВС» из «ЭБ-ГВС», «ВРУ» из «ВРУ-А» и «ВРУ-АПТ»
      будут отклонены. Похожесть обозначений — повод посчитать арифметику или
      ответить INSUFFICIENT_EVIDENCE, а не повод объявить тождество.
      Если обозначение есть в designations только ОДНОЙ строки, ставь
      shared_identity = null и доказывай суммой.
   б) арифметика: сумма расчётных мощностей фидеров обязана сойтись со
      сводной строкой потребителя. Заполни arithmetic_total и
      arithmetic_addends; каждое число копируй дословно из своей строки.
      Слагаемых нужно не меньше двух, и одно из них не может быть самим
      итогом. Система пересчитает сумму и отклонит ответ, если она не
      сходится.
   Если у выбранной стороны есть ВТОРОЙ кандидат с тем же обозначением,
   подпись не различает их вовсе — там годится только арифметика.
5. Строки РАЗНЫХ секций одним объектом не объявляй: секция — часть
   идентичности, а не оттенок подписи.
6. Разные режимы работы не сравниваются. Если у строк разные режимы, это не
   твой вопрос: verdict = INSUFFICIENT_EVIDENCE.
7. Если доказательств не хватает, но ты знаешь, ЧЕГО именно не хватает,
   отвечай NEED_MORE_EVIDENCE и заполни need_more_evidence из справочника.
   Добор бывает ровно один; после него ответ обязан быть окончательным.
8. Если две гипотезы остаются одинаково правдоподобными — INSUFFICIENT_EVIDENCE
   и вопрос инженеру в human_question. Отказ здесь полезнее уверенной догадки.
9. Внутренних идентификаторов (etrow_…, etm_…) в ответе быть не должно.
10. Отвечай ровно по одному разрешению на каждый вопрос пакета и копируй
    question_id дословно."""


def identity_prompt(package_view: Mapping[str, Any]) -> str:
    """Промпт одной партии вопросов об идентичности."""
    import json as _json

    return "\n\n".join([
        "ЗАДАЧА. Ниже строки таблиц нагрузок двух редакций одного листа, "
        "которые система не смогла сопоставить сама. По каждому вопросу реши, "
        "относятся ли названные строки к одному объекту.",
        _IDENTITY_RULES,
        "ВХОДНЫЕ ДАННЫЕ (JSON)",
        _json.dumps(package_view, ensure_ascii=False, indent=1),
        "Ответь строго по схеме ответа.",
    ])


# ── Детерминированный верификатор ──────────────────────────────────────────

@dataclass
class IdentityVerdict:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


_REQUIRED_FIELDS = (
    "question_id", "verdict", "left_row_ref", "right_row_ref",
    "shared_identity", "arithmetic_total", "arithmetic_addends",
    "evidence_quotes", "confidence", "human_question",
    "engineering_summary", "need_more_evidence",
)


def _free_text(resolution: Mapping[str, Any]) -> str:
    parts = [
        str(resolution.get(name) or "")
        for name in ("shared_identity", "human_question", "engineering_summary")
    ]
    for quote in resolution.get("evidence_quotes") or ():
        if isinstance(quote, Mapping):
            parts.append(str(quote.get("quote") or ""))
    return " ".join(parts)


def _grounded_number(
    operand: Any,
    lines: Mapping[str, Mapping[str, Any]],
    errors: list[str],
    where: str,
) -> float | None:
    """Число обязано быть напечатано в названной строке, а не вычислено."""
    if not isinstance(operand, Mapping):
        errors.append(f"арифметика: {where} задан не объектом")
        return None
    ref = str(operand.get("row_ref") or "")
    value = str(operand.get("value") or "")
    line = lines.get(ref)
    if line is None:
        errors.append(f"арифметика: {where} ссылается на строку {ref!r}, которой нет")
        return None
    if normalize(value) not in line["normalized"]:
        errors.append(
            f"арифметика: значения {value!r} нет в строке {ref} дословно"
        )
        return None
    parsed = numbers_in(value)
    if len(parsed) != 1:
        errors.append(
            f"арифметика: {where} = {value!r} — это не одно число"
        )
        return None
    return parsed[0]


def verify_identity(
    question: IdentityQuestion,
    resolution: Mapping[str, Any],
) -> IdentityVerdict:
    """Проверить один ответ об идентичности против его же пакета.

    Верификатор не смягчается ради процента разрешённого. Он проверяет ровно
    то, что модель обязана была доказать: что обе названные строки существуют
    и лежат на объявленных сторонах, что каждая цитата и каждое число взяты
    из названной строки дословно, и что арифметика, которой обоснован вывод,
    действительно сходится. Провал не публикуется никогда.
    """
    errors: list[str] = []
    warnings: list[str] = []

    for name in _REQUIRED_FIELDS:
        if name not in resolution:
            errors.append(f"схема: отсутствует поле {name}")
    if errors:
        return IdentityVerdict(False, errors, warnings)

    if str(resolution.get("question_id") or "") != question.question_id:
        errors.append("привязка: ответ относится к другому вопросу")
    verdict = str(resolution.get("verdict") or "")
    if verdict not in VERDICTS:
        errors.append(f"перечисление: verdict={verdict!r} вне допустимого множества")
    if str(resolution.get("confidence") or "") not in CONFIDENCE_LEVELS:
        errors.append("перечисление: confidence вне допустимого множества")
    if _FORBIDDEN_REF_RE.search(_free_text(resolution)):
        errors.append("ответ содержит внутренний идентификатор системы")

    lines = question.lines()

    for quote in resolution.get("evidence_quotes") or ():
        if not isinstance(quote, Mapping):
            errors.append("тип: цитата должна быть объектом")
            continue
        ref = str(quote.get("row_ref") or "")
        line = lines.get(ref)
        if line is None:
            errors.append(f"цитата: строки {ref!r} в пакете нет")
            continue
        side = str(quote.get("side") or "")
        if side and line["side"] != side:
            errors.append(
                f"стороны: строка {ref} относится к {line['side']},"
                f" а цитата объявлена как {side}"
            )
            continue
        text = normalize(quote.get("quote"))
        if text and text not in line["normalized"]:
            errors.append(f"цитата: {quote.get('quote')!r} нет в строке {ref}")

    if verdict == VERDICT_NEED_EVIDENCE:
        request = resolution.get("need_more_evidence")
        if not isinstance(request, Mapping):
            errors.append("добор: NEED_MORE_EVIDENCE без описания запроса")
        else:
            if str(request.get("missing_evidence_type") or "") not in NEEDED_EVIDENCE_TYPES:
                errors.append("добор: вид доказательства вне справочника")
            if str(request.get("requested_side") or "") not in REQUESTED_SIDES:
                errors.append("добор: сторона вне справочника")
        return IdentityVerdict(not errors, errors, warnings)

    if verdict != VERDICT_SAME:
        # «Разные объекты» и «не хватает доказательств» ничего не публикуют:
        # доказывать здесь нечего, проверять — тоже.
        return IdentityVerdict(not errors, errors, warnings)

    left_ref = str(resolution.get("left_row_ref") or "")
    right_ref = str(resolution.get("right_row_ref") or "")
    left_line, right_line = lines.get(left_ref), lines.get(right_ref)
    if left_line is None:
        errors.append(f"привязка: строки {left_ref!r} в пакете нет")
    elif left_line["side"] != "LEFT":
        errors.append(f"стороны: {left_ref} — не строка левого листа")
    if right_line is None:
        errors.append(f"привязка: строки {right_ref!r} в пакете нет")
    elif right_line["side"] != "RIGHT":
        errors.append(f"стороны: {right_ref} — не строка правого листа")
    if errors:
        return IdentityVerdict(False, errors, warnings)

    left_section = normalize(left_line.get("section"))
    right_section = normalize(right_line.get("section"))
    if left_section and right_section and left_section != right_section:
        errors.append(
            f"секции: {left_ref} относится к «{left_line.get('section')}»,"
            f" {right_ref} — к «{right_line.get('section')}»;"
            " строки разных секций одним объектом не объявляются"
        )

    shared = normalize(resolution.get("shared_identity"))

    def _declared(line: Mapping[str, Any]) -> set[str]:
        return {normalize(value) for value in line.get("designations") or ()}

    shared_ok = bool(
        shared
        and shared in _declared(left_line)
        and shared in _declared(right_line)
    )
    if resolution.get("shared_identity") and not shared_ok:
        errors.append(
            f"тождество: {resolution.get('shared_identity')!r} не значится"
            " обозначением обеих названных строк; общая подстрока"
            " обозначением не является"
        )
    # Доказательство обязано РАЗЛИЧАТЬ, а не просто быть верным. На левом
    # листе две линии подписаны «2ГРЩ-ВРУ3»: подстрока «ВРУ3» есть в обеих, и
    # ответ «эта строка — ВРУ3» одинаково проходит для любой из них. Такое
    # обоснование не выбирает пару, оно её угадывает, а разница между 72,7 и
    # 183,9 кВт — это разный проект. Здесь остаётся только арифметика.
    if shared_ok:
        for side, chosen, candidates in (
            ("LEFT", left_ref, question.left),
            ("RIGHT", right_ref, question.right),
        ):
            rivals = [
                str(line["ref"]) for line in candidates
                if shared in {
                    normalize(value) for value in line.get("designations") or ()
                }
                and str(line["ref"]) != chosen
            ]
            if rivals:
                # Не ошибка ответа, а негодность ЭТОГО обоснования: если
                # арифметика ниже сойдётся, пара доказана и без подписи.
                # Отклонять здесь значило бы наказывать за лишний, но верный
                # довод.
                shared_ok = False
                warnings.append(
                    f"подстрока {resolution.get('shared_identity')!r} есть и в"
                    f" строках {', '.join(sorted(rivals))} той же стороны"
                    f" {side}: выбранную строку она не отличает"
                )

    addends = resolution.get("arithmetic_addends") or []
    total_operand = resolution.get("arithmetic_total")
    arithmetic_ok = False
    if total_operand is not None or addends:
        total = _grounded_number(total_operand, lines, errors, "итог")
        values = [
            _grounded_number(operand, lines, errors, f"слагаемое {index}")
            for index, operand in enumerate(addends, start=1)
        ]
        if total is not None and len(values) >= 2 and all(
            value is not None for value in values
        ):
            summed = sum(value for value in values if value is not None)
            limit = max(abs(total), 1.0) * ARITHMETIC_TOLERANCE
            if abs(summed - total) <= limit:
                arithmetic_ok = True
            else:
                errors.append(
                    f"арифметика: сумма {summed:g} не сходится с итогом"
                    f" {total:g}"
                )
        elif total is not None and len(values) < 2:
            errors.append("арифметика: для доказательства нужно хотя бы два слагаемых")

    if not shared_ok and not arithmetic_ok:
        errors.append(
            "тождество ничем не доказано: нужна либо общая дословная подстрока"
            " в обеих строках, либо сходящаяся арифметика по сводной строке"
        )

    return IdentityVerdict(not errors, errors, warnings)


# ── Проверенное тождество → детерминированные изменения ────────────────────

def match_from(
    question: IdentityQuestion,
    resolution: Mapping[str, Any],
    rows_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Пара строк в том виде, в каком её ждёт детерминированный сравниватель.

    Обозначение берётся ИЗ СТРОК, а не из ответа модели: подпись на листе
    может врать (ровно поэтому вопрос и задавался), но выдумывать её взамен
    системе нельзя. Метод помечается как AI_IDENTITY — детерминированный
    сравниватель такого метода не знает и уводит уверенность в MEDIUM, что
    для пары, предложенной моделью, и есть правильный уровень.
    """
    left_id = question.ref_to_row.get(str(resolution.get("left_row_ref") or ""))
    right_id = question.ref_to_row.get(str(resolution.get("right_row_ref") or ""))
    left_row = rows_by_id.get(str(left_id or ""))
    right_row = rows_by_id.get(str(right_id or ""))
    if not left_row or not right_row:
        return None
    designation = str(
        right_row.get("consumer_designation")
        or left_row.get("consumer_designation")
        or question.subject
        or ""
    )
    return {
        "match_id": stable_id("etm", str(left_id), str(right_id)),
        "method": METHOD_AI_IDENTITY,
        "designation": designation,
        "left": dict(left_row),
        "right": dict(right_row),
        "question_id": question.question_id,
        "source_item_id": question.source_item_id,
    }


#: Оговорка детерминированного сравнивателя, которая для пары от модели
#: неверна: он приписывает её любому незнакомому методу. Заменяем ровно её,
#: не трогая сам сравниватель.
_DESIGNATION_NOTE = "Строки сопоставлены только по обозначению потребителя."
_AI_NOTE = (
    "Пара предложена ИИ и проверена правилами: обозначения и числа сверены "
    "по самим строкам. Подпись на листе при этом не исправлена."
)

#: Почему пара, тождество которой доказано, всё равно не даёт находки.
BLOCKED_MODE_UNPROVEN = "mode_provenance_unproven"

MODE_UNPROVEN_NOTE = (
    "Режим, к которому относятся величины, распознан не у обеих строк: "
    "сравнивать их значения нельзя, пока это не подтверждено."
)


def mode_comparable(
    left_row: Mapping[str, Any],
    right_row: Mapping[str, Any],
) -> bool:
    """Доказано ли, что величины обеих строк относятся к ОДНОМУ режиму.

    Почему для пары от модели этого мало — «оба режима не распознаны».
    На правом листе пары ГРЩ величины колонки ВРУ-ХЦ (37,5 кВт и 75,8 А)
    складываются ровно в блок «Авар. режим»: 37,5+37,5 = 75,0 и
    75,8+75,8 = 151,6 при напечатанных 75,0 и 151,5. Слева те же 13,7 и 66,2
    складываются в сводную строку режима «Рабочий» (27,5 кВт и 132 А) под
    заголовком «Расчетная мощность (в расчётном рабочем режиме ГРЩ)».

    То есть напечатанные числа верны с обеих сторон, объект один, а сравнение
    получается между рабочим режимом и аварийным. Существующий страж
    `mode_label_mismatch` этого не ловит: подпись «Авар. режим» напечатана над
    колонками, а не в строке, и связчик её к строке не привязывает — у обеих
    строк `mode_label` пуст, и страж видит «пусто против пусто».

    Для пары, найденной детерминированным матчером, риск берёт на себя
    доказанное тождество обозначения и секции. Для пары, предложенной
    моделью, такого запаса нет, поэтому здесь требуется положительное
    доказательство: у обеих строк режим назван и назван одинаково.
    """
    left = normalize(left_row.get("mode_label"))
    right = normalize(right_row.get("mode_label"))
    return bool(left) and left == right


def deterministic_changes(
    matches: Sequence[Mapping[str, Any]],
    compare_match: Any,
) -> dict[str, Any]:
    """Изменения по проверенным парам — ТЕМ ЖЕ кодом, что и для матчера.

    Модель сюда уже не участвует: она назвала пару, всё остальное считает
    существующий детерминированный сравниватель. Единственная правка — текст
    оговорки: сравниватель не знает про метод AI_IDENTITY и приписывает паре
    «сопоставлены только по обозначению», чего как раз не было.
    """
    changes: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for match in matches:
        if not mode_comparable(match["left"], match["right"]):
            # Тождество доказано, сравнение значений — нет. Пара уезжает
            # инженеру подсказкой к его собственной строке, а не находкой:
            # опубликовать её значило бы выдать сравнение рабочего режима с
            # аварийным за изменение проекта.
            blocked.append({
                "reason": BLOCKED_MODE_UNPROVEN,
                "subject": match.get("designation"),
                "match_id": match.get("match_id"),
                "question_id": match.get("question_id"),
                "source_item_id": match.get("source_item_id"),
                "left_row_id": match["left"].get("row_id"),
                "right_row_id": match["right"].get("row_id"),
                "left_label": match["left"].get("consumer_label"),
                "right_label": match["right"].get("consumer_label"),
                "summary": (
                    f"ИИ предлагает пару для «{match.get('designation')}»: "
                    f"{match['right'].get('consumer_label')}. "
                    + MODE_UNPROVEN_NOTE
                ),
                "resolved_by": "AI_IDENTITY",
            })
            continue
        result = compare_match(match)
        for bucket, target in (
            ("changes", changes), ("unchanged", unchanged),
        ):
            for record in result.get(bucket) or ():
                notes = [
                    note for note in record.get("notes") or ()
                    if note != _DESIGNATION_NOTE
                ]
                target.append({
                    **record,
                    "notes": [_AI_NOTE, *notes],
                    "resolved_by": "AI_IDENTITY",
                    "question_id": match.get("question_id"),
                    "source_item_id": match.get("source_item_id"),
                })
        blocked.extend(
            {
                **record,
                "subject": match.get("designation"),
                "match_id": match.get("match_id"),
                "resolved_by": "AI_IDENTITY",
            }
            for record in result.get("blocked") or ()
        )
    return {"changes": changes, "unchanged": unchanged, "blocked": blocked}


__all__ += [
    "BLOCKED_MODE_UNPROVEN",
    "IDENTITY_SCHEMA",
    "MODE_UNPROVEN_NOTE",
    "mode_comparable",
    "IDENTITY_SYSTEM_PROMPT",
    "IdentityVerdict",
    "deterministic_changes",
    "identity_prompt",
    "match_from",
    "verify_identity",
]
