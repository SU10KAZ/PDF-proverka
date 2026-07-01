"""Дедуп соседних текстовых блоков против текст-слоя блока-схемы.

Идея: описание блока-схемы (полная разметка) уже несёт весь текстовый слой блока
(vector_text). Соседние `text`-блоки той же страницы (примечания, таблица ТТ и т.п.)
часто дублируют этот же текст — и если их отправлять в LLM отдельно, получается
задвоение и лишние токены.

Эта утилита решает, какие соседние блоки УЖЕ содержатся в текст-слое блока
(→ не отправлять повторно), а какие уникальны (→ отправлять). Разметку НЕ трогаем —
фильтруем только список соседей, поэтому риска срезать схемную часть нет.

Сравнение — по **биграммам** (пары соседних токенов). Это устойчиво к общему
словарю (имена панелей РП/ВП, числа), который при сравнении по отдельным словам
давал ложные срабатывания на схемной части (напр. строка дерева питания
«ГРЩ→ВП1→РП1» состоит из слов, встречающихся в таблицах-соседях, но как
последовательность там не встречается).
"""
from __future__ import annotations

import re
from typing import Any, Callable, Iterable

_TAG_RE = re.compile(r"<[^>]+>")
_ENT_RE = re.compile(r"&[a-z]+;")
_NONWORD_RE = re.compile(r"[^\w\s]")

# Порог bigram-containment, выше которого соседний блок считается дублем текст-слоя.
# На живых данных (13АВ-РД-ЭМ-К1): дубли ТТ/Примечания = 82–92%, уникум = 0%,
# прочие листы ≤22% → 0.80 даёт чистое разделение с запасом.
DEFAULT_THRESHOLD = 0.80


def _strip_html(s: str) -> str:
    s = _TAG_RE.sub(" ", s or "")
    s = _ENT_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(s: str) -> list[str]:
    s = _strip_html(s).lower().replace("ё", "е")
    return [t for t in _NONWORD_RE.sub(" ", s).split() if len(t) >= 2]


def _bigrams(tokens: list[str]) -> set[tuple[str, str]]:
    return set(zip(tokens, tokens[1:]))


def bigram_containment(candidate_text: str, reference_bigrams: set) -> float:
    """Доля биграмм кандидата, встречающихся в reference (0..1). <4 токенов → 0.0."""
    ct = _tokens(candidate_text)
    if len(ct) < 4:
        return 0.0
    cb = _bigrams(ct)
    if not cb:
        return 0.0
    return len(cb & reference_bigrams) / len(cb)


def filter_neighbor_blocks(
    text_layer: str,
    neighbor_blocks: Iterable[dict],
    *,
    threshold: float = DEFAULT_THRESHOLD,
    get_text: Callable[[dict], str] = lambda b: b.get("text", ""),
) -> tuple[list[dict], list[dict]]:
    """Разделить соседние текст-блоки на «слать» (уникальные) и «дубли» (в текст-слое).

    text_layer      : текстовый слой блока-схемы (vector_text / pdfplumber_text).
    neighbor_blocks : соседние текст-блоки ТОЙ ЖЕ страницы (скоуп по странице обязателен —
                      иначе ловятся одноимённые примечания с других листов).
    threshold       : порог bigram-containment для признания дублем.
    get_text        : как достать текст из элемента.

    → (send, dropped). Каждый элемент — копия исходного dict + поле
      ``bigram_in_text_layer`` (float); у dropped дополнительно ``reason="in_text_layer"``.
    """
    ref = _bigrams(_tokens(text_layer))
    send: list[dict] = []
    dropped: list[dict] = []
    for b in neighbor_blocks:
        cov = round(bigram_containment(get_text(b), ref), 3)
        item: dict[str, Any] = dict(b) if isinstance(b, dict) else {"text": str(b)}
        item["bigram_in_text_layer"] = cov
        if cov >= threshold:
            item["reason"] = "in_text_layer"
            dropped.append(item)
        else:
            send.append(item)
    return send, dropped
