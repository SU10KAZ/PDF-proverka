"""Пакет доказательств — единственный вход ИИ-аналитика.

Модель не видит PDF, не видит репозиторий и не ходит в файлы. Она видит ровно
то, что детерминированный слой уже установил про этот элемент: пару листов и
чем она доказана, значения из Stage 3, окно соседних строк вокруг фрагмента с
обеих сторон и то, что производитель фактов о нём знает.

Это не экономия токенов, а условие проверяемости: всё, что модель вернёт как
факт документа, обязано дословно найтись в пакете, и верификатор это
проверяет. Дать модели больше — значит потерять возможность её поймать.

Пакеты собираются в памяти прямо во время прогона, из тех же артефактов,
которые конвейер и так построил. Ничего с диска не дочитывается.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping

from .. import recognition_coverage
from ..production_artifacts import content_signature, stable_id
from . import schemas, settings

PACKAGE_VERSION = "stage-comparison-ai-evidence.v1"

#: Сколько строк документа показывать вокруг фрагмента с каждой стороны.
CONTEXT_WINDOW = 6
#: Предел длины одной строки контекста: длинное примечание не должно вытеснять
#: из пакета сам изменившийся ряд таблицы.
CONTEXT_CHAR_LIMIT = 400

#: Пометка, с которой наблюдение по чертежу попадает в пакет доказательств.
VISION_OBSERVATION_PREFIX = "по чертежу:"


def scope_refs_for_group(group: Mapping[str, Any]) -> list[str]:
    """Все ключи области, под которыми эта группа может встретиться.

    Ключ чеканят два производителя, и формулы у них исторически разные:
    производитель фактов хеширует позиционные аргументы, построитель атомов —
    именованный объект. Элемент без факта приходит со вторым ключом, элемент с
    фактом — с первым. Знать обе формулы здесь дешевле, чем потерять привязку
    к паре листов и отправить модель разбирать расхождение вслепую.
    """
    left = sorted(int(page) for page in group.get("left_pages") or [])
    right = sorted(int(page) for page in group.get("right_pages") or [])
    return [
        stable_id("text_scope_", group.get("id"), left, right),
        "text_scope_" + content_signature({
            "group_id": group.get("id"),
            "left_pages": left,
            "right_pages": right,
        })[:20],
    ]


def scope_ref_for_group(group: Mapping[str, Any]) -> str:
    return scope_refs_for_group(group)[0]


@dataclass
class EvidenceItem:
    """Один элемент, требующий разрешения."""

    item_id: str = ""
    atom_id: str = ""
    scope_ref: str = ""
    source: str = "TEXT"

    # что установил детерминированный Stage 3
    stage3_bucket: str = ""
    direction: str = ""
    before_value: str | None = None
    after_value: str | None = None

    # где это физически лежит
    left_pages: list[int] = field(default_factory=list)
    right_pages: list[int] = field(default_factory=list)
    locations: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    # страницы всей пары листов — нужны только визуальному резерву, когда у
    # элемента есть координаты лишь с одной стороны: чтобы ответить «строки
    # там действительно нет», надо посмотреть на противоположный лист целиком.
    # В `model_view()` это НЕ входит: текстовому аналитику лист целиком не
    # показывают, и отпечаток доказательств от этого поля не зависит.
    sheet_pages: dict[str, list[int]] = field(default_factory=dict)

    # Идентичность листа, доказанная по штампу из вектор-слоя. Нужна
    # визуальному резерву: текстовый штамп первичен, и увиденное на картинке
    # не имеет права молча его переопределить. В `model_view()` не входит —
    # аналитик видит ту же идентичность в описании пары листов.
    stamp_identity: dict[str, Any] = field(default_factory=dict)

    # окно соседних строк документа как адресуемые доказательства
    left_context: list[dict[str, Any]] = field(default_factory=list)
    right_context: list[dict[str, Any]] = field(default_factory=list)

    # что уже установил детерминированный слой про сам элемент
    deterministic_state: dict[str, Any] = field(default_factory=dict)

    # ссылки на доказательства, которыми модель обязана оперировать
    evidence_refs: list[dict[str, Any]] = field(default_factory=list)

    evidence_digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def model_view(self) -> dict[str, Any]:
        """Ровно то, что уходит в промпт: без внутренних ссылок и провенанса."""
        return {
            "item_id": self.item_id,
            "stage3_bucket": self.stage3_bucket,
            "deterministic_direction": self.direction,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "left_pages": self.left_pages,
            "right_pages": self.right_pages,
            "left_context": self.left_context,
            "right_context": self.right_context,
            "deterministic_state": self.deterministic_state,
        }


@dataclass
class EvidencePackage:
    """Партия элементов одной пары листов."""

    package_version: str = PACKAGE_VERSION
    schema_version: str = schemas.SCHEMA_VERSION
    relation_id: str = ""
    sheet_relation: dict[str, Any] = field(default_factory=dict)
    items: list[EvidenceItem] = field(default_factory=list)

    def digest(self) -> str:
        return content_signature({
            "package_version": self.package_version,
            "schema_version": self.schema_version,
            "sheet_relation": self.sheet_relation,
            "items": [item.model_view() for item in self.items],
        })

    def model_view(self) -> dict[str, Any]:
        return {
            "sheet_relation": self.sheet_relation,
            "items": [item.model_view() for item in self.items],
        }


# ── Сборка ────────────────────────────────────────────────────────────────

def _fragment_index(
    preparation: Mapping[str, Any],
) -> tuple[dict[str, tuple[str, int, int]], dict[tuple[str, int], list[dict[str, Any]]]]:
    """Фрагменты по страницам в порядке чтения и позиция каждого из них."""
    pages: dict[tuple[str, int], list[dict[str, Any]]] = {}
    raw = preparation.get("fragments")
    if not isinstance(raw, Mapping):
        return {}, {}
    for key, side in (("left", "LEFT"), ("right", "RIGHT")):
        for fragment in raw.get(key) or []:
            if not isinstance(fragment, Mapping):
                continue
            page = int(fragment.get("pdf_page") or 0)
            pages.setdefault((side, page), []).append(dict(fragment))
    for values in pages.values():
        values.sort(key=lambda value: (
            int(value.get("order") or 0), str(value.get("id") or "")
        ))
    position: dict[str, tuple[str, int, int]] = {}
    for (side, page), values in pages.items():
        for index, fragment in enumerate(values):
            position[str(fragment.get("id") or "")] = (side, page, index)
    return position, pages


def _context_lines(
    position: Mapping[str, tuple[str, int, int]],
    pages: Mapping[tuple[str, int], list[dict[str, Any]]],
    fragment_id: str,
    *,
    side_letter: str,
    start_number: int,
    window: int | None = None,
) -> list[dict[str, Any]]:
    """Окно соседних строк как АДРЕСУЕМЫЕ доказательства, а не как текст.

    Раньше контекст был списком строк, и проверить можно было только одно:
    встречается ли названное моделью значение где-нибудь на этой стороне.
    «Где-нибудь» — это ровно та формулировка, при которой площадь соседнего
    помещения проходит как площадь нужного: обе строки лежат в одном окне.

    У каждой строки теперь есть ссылка (L1, R3…), и модель обязана назвать,
    ИЗ КАКОЙ строки она взяла объект и из какой — значение. Верификатор после
    этого проверяет не присутствие подстроки в общем котле, а связку
    «объект + свойство + значение + сторона + место».
    """
    window = CONTEXT_WINDOW if window is None else window
    located = position.get(fragment_id)
    if located is None:
        return []
    side, page, index = located
    values = pages.get((side, page)) or []
    low = max(0, index - window)
    high = min(len(values), index + window + 1)
    output = []
    for offset, cursor in enumerate(range(low, high)):
        text = " ".join(str(values[cursor].get("text") or "").split())
        if len(text) > CONTEXT_CHAR_LIMIT:
            text = text[:CONTEXT_CHAR_LIMIT] + "…"
        output.append({
            "ref": f"{side_letter}{start_number + offset}",
            "side": side,
            "page": page,
            "text": text,
            # Строка, вокруг которой построено окно: именно её расхождение
            # разбирается. Ответ, не опирающийся ни на одну строку в фокусе,
            # относится к другому расхождению.
            "focus": cursor == index,
            "source": "TEXT",
        })
    return output


def _sheet_labels(sheet_relations: Mapping[str, Any]) -> dict[str, dict[int, str]]:
    raw = sheet_relations.get("sheet_labels")
    output: dict[str, dict[int, str]] = {"LEFT": {}, "RIGHT": {}}
    if not isinstance(raw, Mapping):
        return output
    for side in ("LEFT", "RIGHT"):
        values = raw.get(side)
        if not isinstance(values, Mapping):
            continue
        for page, label in values.items():
            try:
                output[side][int(page)] = str(label)
            except (TypeError, ValueError):
                continue
    return output


def _relation_view(
    relation: Mapping[str, Any],
    labels: Mapping[str, Mapping[int, str]],
) -> dict[str, Any]:
    """Пара листов так, как её должен прочитать аналитик: названия, не хеши."""
    left_pages = sorted({int(page) for page in relation.get("left_pages") or []})
    right_pages = sorted({int(page) for page in relation.get("right_pages") or []})
    stamp = relation.get("stamp_identity")
    return {
        "left_sheets": [
            {"page": page, "title": labels["LEFT"].get(page)} for page in left_pages
        ],
        "right_sheets": [
            {"page": page, "title": labels["RIGHT"].get(page)} for page in right_pages
        ],
        "relation_type": relation.get("relation_type"),
        "status": relation.get("status"),
        "confidence": relation.get("confidence"),
        "proved_by": relation.get("primary_source"),
        "reason_codes": sorted(
            str(value) for value in relation.get("reason_codes") or []
        ),
        "stamp_identity": dict(stamp) if isinstance(stamp, Mapping) else None,
    }


def _recognition_state(source_atom: Mapping[str, Any]) -> dict[str, Any]:
    """Вердикт полноты распознавания, доехавший из Stage 3 через провенанс."""
    value = source_atom.get("recognition_coverage")
    if not isinstance(value, Mapping):
        return {"status": recognition_coverage.UNKNOWN, "reason_codes": []}
    return {
        "status": str(value.get("status") or recognition_coverage.UNKNOWN),
        "reason_codes": sorted(
            str(code) for code in value.get("reason_codes") or ()
        ),
    }


def _locations(item: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    provenance = item.get("provenance")
    source_atom = (
        provenance.get("source_atom") if isinstance(provenance, Mapping) else None
    )
    raw = source_atom.get("locations") if isinstance(source_atom, Mapping) else None
    output: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    if not isinstance(raw, Mapping):
        return output
    for side in ("LEFT", "RIGHT"):
        values = raw.get(side)
        if isinstance(values, (list, tuple)):
            output[side] = [dict(value) for value in values if isinstance(value, Mapping)]
    return output


#: Провенанс строк, добранных адресно, а не окном вокруг якорного фрагмента.
#: Права у такого доказательства урезаны: оно подтверждает совпадение и не
#: имеет права утверждать изменение. Ограничение держится не честным словом
#: модели, а верификатором: при корзине Stage 3 «added» значение слева
#: отклоняется независимо от того, что модель увидела в окне.
RETRIEVED_SOURCE = "RETRIEVED_NATIVE_TEXT"


def _retrieved_lines(
    found: Iterable[Mapping[str, Any]],
    *,
    side: str,
    side_letter: str,
    start_number: int,
) -> list[dict[str, Any]]:
    """Адресно добранные строки стороны, у которой нет якорного фрагмента."""
    output: list[dict[str, Any]] = []
    for offset, line in enumerate(found):
        if not isinstance(line, Mapping):
            continue
        text = " ".join(str(line.get("text") or "").split())
        if len(text) > CONTEXT_CHAR_LIMIT:
            text = text[:CONTEXT_CHAR_LIMIT] + "…"
        output.append({
            "ref": f"{side_letter}{start_number + offset}",
            "side": side,
            "page": line.get("page"),
            "text": text,
            "focus": False,
            "source": RETRIEVED_SOURCE,
        })
    return output


def build_packages(
    *,
    review_items: Iterable[Mapping[str, Any]],
    preparation: Mapping[str, Any],
    sheet_relations: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]],
    batch_size: int,
    retrieved: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[EvidencePackage]:
    """Собрать пакеты, сгруппированные по паре листов.

    Партия внутри одной пары листов — это не только экономия вызовов: элементы
    одной таблицы объясняют друг друга, и модель, которая видит рядом «02.41
    Кладовая» и «02.42 Кладовая», перестаёт принимать соседнюю строку за то же
    помещение.
    """
    position, pages = _fragment_index(preparation)
    window = settings.context_window()
    labels = _sheet_labels(sheet_relations)
    relations_by_id = {
        str(relation.get("relation_id") or ""): relation
        for relation in sheet_relations.get("relations") or []
        if isinstance(relation, Mapping)
    }
    scope_to_relation = {
        scope_ref: str(group.get("id") or "")
        for group in comparison_groups
        for scope_ref in scope_refs_for_group(group)
    }

    grouped: dict[str, list[EvidenceItem]] = {}
    for item in review_items:
        if not isinstance(item, Mapping):
            continue
        locations = _locations(item)
        provenance = item.get("provenance")
        source_atom = (
            provenance.get("source_atom")
            if isinstance(provenance, Mapping)
            else {}
        )
        source_atom = source_atom if isinstance(source_atom, Mapping) else {}
        left_context: list[dict[str, Any]] = []
        for location in locations["LEFT"]:
            left_context += _context_lines(
                position, pages, str(location.get("fragment_id") or ""),
                side_letter="L", start_number=len(left_context) + 1,
                window=window,
            )
        right_context: list[dict[str, Any]] = []
        for location in locations["RIGHT"]:
            right_context += _context_lines(
                position, pages, str(location.get("fragment_id") or ""),
                side_letter="R", start_number=len(right_context) + 1,
                window=window,
            )
        # Сторона без якорного фрагмента получает адресно добранные строки.
        # Без них окно этой стороны честно пусто — не потому, что на листе
        # ничего нет, а потому, что окно строится вокруг фрагмента, которого у
        # элемента вида «добавлено» с этой стороны не бывает. Пустое окно
        # модель читает как «доказательств не показали» и отвечает
        # EVIDENCE_TRUNCATED, хотя прочитанные строки лежат в том же артефакте.
        found = (retrieved or {}).get(
            str(item.get("review_evidence_id") or "")
        ) or {}
        if not left_context:
            left_context += _retrieved_lines(
                found.get("LEFT") or (), side="LEFT", side_letter="L",
                start_number=1,
            )
        if not right_context:
            right_context += _retrieved_lines(
                found.get("RIGHT") or (), side="RIGHT", side_letter="R",
                start_number=1,
            )
        evidence = EvidenceItem(
            item_id=str(item.get("review_evidence_id") or ""),
            atom_id=str(item.get("atom_id") or ""),
            scope_ref=str(item.get("scope_ref") or ""),
            source=str(item.get("source") or "TEXT"),
            stage3_bucket=str(source_atom.get("stage3_bucket") or ""),
            direction=str(item.get("direction") or ""),
            before_value=item.get("before_value"),
            after_value=item.get("after_value"),
            left_pages=sorted({
                int(value["page"]) for value in locations["LEFT"]
                if isinstance(value.get("page"), int)
            }),
            right_pages=sorted({
                int(value["page"]) for value in locations["RIGHT"]
                if isinstance(value.get("page"), int)
            }),
            locations=locations,
            left_context=left_context,
            right_context=right_context,
            deterministic_state={
                "dimension": item.get("dimension"),
                # Свойство, которое детерминированный слой уже РАСПОЗНАЛ.
                # Без него верификатору нечем поймать подмену: «высота
                # потолка» на доказанной площади выглядит таким же свободным
                # текстом, как и «площадь».
                "facet_ref": item.get("facet_ref"),
                "outcome": item.get("outcome"),
                "reason_codes": sorted(
                    str(value) for value in item.get("reason_codes") or []
                ),
                "structured_fact": bool(source_atom.get("structured_fact")),
                "source_atom_outcome": (
                    provenance.get("source_atom_outcome")
                    if isinstance(provenance, Mapping)
                    else None
                ),
                # Полнота распознавания едет к модели вместе с элементом и
                # проверяется верификатором: разбирать значение, про которое
                # детерминированный слой сказал «прочитано ненадёжно», ИИ не
                # имеет права ни при какой уверенности.
                "recognition_coverage": _recognition_state(source_atom),
            },
            evidence_refs=[
                dict(value) for value in item.get("evidence_refs") or []
                if isinstance(value, Mapping)
            ],
        )
        evidence.evidence_digest = content_signature(evidence.model_view())
        relation_id = scope_to_relation.get(evidence.scope_ref, "")
        grouped.setdefault(relation_id, []).append(evidence)

    packages: list[EvidencePackage] = []
    for relation_id in sorted(grouped):
        relation = relations_by_id.get(relation_id) or {}
        view = _relation_view(relation, labels) if relation else {}
        items = sorted(grouped[relation_id], key=lambda value: value.item_id)
        relation_pages = {
            "LEFT": sorted({int(page) for page in relation.get("left_pages") or []}),
            "RIGHT": sorted({int(page) for page in relation.get("right_pages") or []}),
        }
        stamp = relation.get("stamp_identity")
        stamp = dict(stamp) if isinstance(stamp, Mapping) else {}
        for item_view in items:
            item_view.sheet_pages = {
                side: list(pages) for side, pages in relation_pages.items() if pages
            }
            item_view.stamp_identity = dict(stamp)
        for start in range(0, len(items), max(1, batch_size)):
            packages.append(EvidencePackage(
                relation_id=relation_id,
                sheet_relation=view,
                items=items[start:start + max(1, batch_size)],
            ))
    return packages


def vision_lines(
    item: EvidenceItem,
    payload: Mapping[str, Any],
    *,
    crops: Iterable[Mapping[str, Any]] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Наблюдение с чертежа — ОТДЕЛЬНОЕ типизированное доказательство.

    Раньше увиденное дописывалось в контекст обычной строкой, после чего
    аналитик цитировал его наравне с текстом документа, а верификатор не мог
    отличить «прочитано в PDF» от «показалось на картинке». Теперь у строки
    свой источник, своя сторона и свой отпечаток изображения: подменить
    сторону или подсунуть другой кроп с тем же ключом больше нельзя.
    """
    by_side = {"LEFT": [], "RIGHT": []}
    crops_by_side: dict[str, list[Mapping[str, Any]]] = {"LEFT": [], "RIGHT": []}
    for crop in crops or ():
        side = str(crop.get("side") or "").upper()
        if side in crops_by_side:
            crops_by_side[side].append(crop)
    for side, key, letter in (
        ("LEFT", "observed_left", "LV"), ("RIGHT", "observed_right", "RV"),
    ):
        text = " ".join(str(payload.get(key) or "").split())
        if not text:
            continue
        by_side[side].append({
            "ref": f"{letter}1",
            "side": side,
            "text": f"{VISION_OBSERVATION_PREFIX} {text}",
            "focus": True,
            "source": "VISION",
            "page_refs": [
                int(crop.get("page") or 0) for crop in crops_by_side[side]
            ],
            "crop_refs": [
                str(crop.get("crop_ref") or "") for crop in crops_by_side[side]
            ],
            "crop_digests": [
                str(crop.get("digest") or "") for crop in crops_by_side[side]
            ],
            "whole_sheet": any(
                bool(crop.get("whole_sheet")) for crop in crops_by_side[side]
            ),
            "model": str(payload.get("model") or ""),
        })
    return by_side


__all__ = [
    "CONTEXT_WINDOW",
    "RETRIEVED_SOURCE",
    "VISION_OBSERVATION_PREFIX",
    "vision_lines",
    "EvidenceItem",
    "EvidencePackage",
    "PACKAGE_VERSION",
    "build_packages",
    "scope_ref_for_group",
    "scope_refs_for_group",
]
