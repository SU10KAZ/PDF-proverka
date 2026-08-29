"""Детерминированная идентичность объекта проекта по его названию.

Один и тот же вопрос задают два места: верификатор ИИ-слоя (можно ли принять
название, которое дала модель) и чеканка внутренних ссылок (можно ли завести
объект под этим названием). Ответ обязан быть один, поэтому он живёт здесь.

Главное правило: совпадение ОДНОГО ЧИСЛА объектом не доказывает. В строке
«24.5 | Кладовая | 6,02» число 24.5 подтверждает идентификатор, но ничего не
говорит о том, что это за объект. «Помещение 24.5» и «вымышленный агрегат
24.5» отличаются не цифрой, а видом объекта, и вид обязан подтверждаться теми
же доказательствами — иначе рядом с настоящим помещением молча заводится
объект-двойник, поймать который потом нечем.

Поэтому название разбирается на две части:

* идентифицирующие токены — чем этот объект отличается от соседнего того же
  вида («24.5», «К5»). Сверяются ТОЧНО: 24.5 не равно 24.6;
* описательные слова — вид и название объекта («помещение», «кровля»).
  Сверяются с учётом русского словоизменения («помещение» ↔ «помещений»), но
  каждое обязано найтись в доказательствах. Слово, которого в доказательствах
  нет вовсе, — это предложенный моделью неизвестный вид объекта, и такой
  ответ уходит человеку, а не публикуется.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

#: Токен названия: слово, число, код с точками и дефисами.
TOKEN_RE = re.compile(r"[\w./\-]+", re.UNICODE)
_DIGIT_RE = re.compile(r"\d")

#: Служебные слова: связки и единицы, которые ничего не утверждают об объекте
#: и потому не требуют подтверждения. Список закрыт намеренно: всё, чего в нём
#: нет, считается содержательным словом и обязано найтись в доказательствах.
STRUCTURAL_WORDS = frozenset({
    "для", "или", "под", "над", "при", "без", "из", "изо", "около", "возле",
    "на", "в", "во", "с", "со", "и", "к", "ко", "у", "о", "об", "от", "до",
    "за", "по", "не", "шт", "штук", "мм", "см", "км", "м2", "м²", "кв",
    "the", "of", "for", "and", "with",
})

#: Минимальная длина содержательного слова. Более короткие («№», «п») ничего
#: не утверждают и подтверждения не требуют.
MIN_CONTENT_WORD = 3


def canonical(value: Any) -> str:
    """Регистр, «ё» и пробелы — не различие между объектами."""
    return " ".join(str(value or "").casefold().replace("ё", "е").split())


def tokens(value: Any) -> set[str]:
    """Токены строки как множество, без склейки в одну подстроку."""
    return {
        token.strip("./-")
        for token in TOKEN_RE.findall(canonical(value))
        if token.strip("./-")
    }


def identity_tokens(label: Any) -> set[str]:
    """Чем этот объект отличается от соседнего объекта того же вида.

    Для «помещение 24.5» это «24.5»: слово «помещение» стоит в каждой строке
    экспликации и не отличает ничего. Если различающих цифр нет вовсе
    («наружная стена»), берутся длинные слова названия.
    """
    found = tokens(label)
    numeric = {
        token for token in found
        if len(token) >= 2 and _DIGIT_RE.search(token)
    }
    if numeric:
        return numeric
    return {token for token in found if len(token) >= 4}


def descriptor_words(label: Any) -> set[str]:
    """Вид и название объекта: то, что обязано подтверждаться доказательствами.

    Числа сюда не попадают — их проверяет ``identity_tokens`` и проверяет
    точно. Здесь остаются именно слова: «помещение», «кровля», «агрегат».
    """
    return {
        token for token in tokens(label)
        if not _DIGIT_RE.search(token)
        and len(token) >= MIN_CONTENT_WORD
        and token not in STRUCTURAL_WORDS
    }


def _common_prefix(first: str, second: str) -> int:
    length = 0
    for left, right in zip(first, second):
        if left != right:
            break
        length += 1
    return length


def words_agree(word: str, other: str) -> bool:
    """Одно ли это слово с точностью до русского словоизменения.

    «помещение» и «помещений» — одно слово; «стена» и «степень» — разные.
    Проверка на окончание, а не на похожесть: общая часть обязана покрывать
    почти всё обоих слов, иначе совпадение случайно.
    """
    if word == other:
        return True
    shorter, longer = (word, other) if len(word) <= len(other) else (other, word)
    common = _common_prefix(shorter, longer)
    if len(shorter) < 4:
        # Короткое слово обязано входить в длинное целиком: «щит» — «щита».
        return common == len(shorter) and len(longer) - common <= 2
    return (
        common >= 4
        and common >= len(shorter) - 2
        and common >= len(longer) - 3
    )


def evidence_tokens(evidence: Iterable[Any]) -> set[str]:
    """Все токены, которые модель имела право видеть."""
    haystack: set[str] = set()
    for value in evidence or ():
        haystack |= tokens(value)
    return haystack


def grounding_problem(label: Any, evidence: Iterable[Any]) -> str | None:
    """Почему это название нельзя считать доказанным. ``None`` — можно.

    Проверок две, и обе обязательны. Идентификатор обязан найтись точно; вид
    объекта — обязан найтись как слово. Пропуск любой из них означает, что
    название объекта держится на совпадении одного числа.
    """
    if not canonical(label):
        return "название объекта пусто"
    haystack = evidence_tokens(evidence)
    if not haystack:
        return "доказательств, в которых объект мог бы быть назван, нет"

    identity = identity_tokens(label)
    if not identity:
        return f"в названии {str(label)!r} нет ничего, что отличало бы объект"
    if not (identity & haystack):
        return (
            f"идентификатор {'/'.join(sorted(identity))} не найден"
            " в доказательствах"
        )

    unsupported = sorted(
        word for word in descriptor_words(label)
        if not any(words_agree(word, other) for other in haystack)
    )
    if unsupported:
        return (
            f"вид объекта {'/'.join(unsupported)} в доказательствах не"
            " встречается: совпадения одного числа мало, чтобы считать это"
            " тем же объектом"
        )
    return None


def is_grounded(label: Any, evidence: Iterable[Any]) -> bool:
    return grounding_problem(label, evidence) is None


def supporting_evidence(label: Any, evidence: Iterable[Any]) -> list[str]:
    """Строки, которыми название объекта действительно подтверждается.

    Это не весь пакет, а именно доказательство: по одной строке на каждую
    часть названия. Оно едет вместе с машинным ответом, чтобы чеканка ссылок
    проверяла то же самое, что проверил верификатор, а не более узкий набор
    значений, в котором вид объекта не встречается никогда.
    """
    required = identity_tokens(label) | descriptor_words(label)
    if not required:
        return []
    support: list[str] = []
    remaining = set(required)
    for value in evidence or ():
        text = str(value or "").strip()
        if not text:
            continue
        found = tokens(text)
        covered = {
            word for word in remaining
            if word in found or any(words_agree(word, other) for other in found)
        }
        if covered:
            remaining -= covered
            support.append(text)
        if not remaining:
            break
    return support


__all__ = [
    "MIN_CONTENT_WORD",
    "STRUCTURAL_WORDS",
    "TOKEN_RE",
    "canonical",
    "descriptor_words",
    "evidence_tokens",
    "grounding_problem",
    "identity_tokens",
    "is_grounded",
    "supporting_evidence",
    "tokens",
    "words_agree",
]
