"""Deterministic, language-level features of ProjectChange cards (no model, no labels).

Nothing here decides whether two cards are one engineering event.  The
features only rank which cards are worth showing TOGETHER to the AI
Consolidator.  No building name, page number, card id or technical value of
any particular project is encoded: similarity thresholds are percentiles of
the run's own pair distribution, designations are generic ``letters+digits``
tokens, location units are ``keyword + number`` in the generic Russian
construction vocabulary.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

HOMO = str.maketrans("ABCEHKMOPTXYaceopxy", "АВСЕНКМОРТХУасеорху")
STOP = set("""и в во на по с со к ко от до из за для при не без или а но что как это его ее их
также том той тот того при через под над между после перед все всех всего каждый каждой
предусмотрен предусмотрена предусмотрено предусмотрены выполнен выполнена выполнено указан указано
указана указаны показан показана показано показаны принят принято принята new old ранее теперь
изменение изменен изменена изменено изменены новый новая новое новые прежний прежняя""".split())
DESIG_RE = re.compile(r"(?<![0-9A-Za-zА-Яа-яЁё])(\d*[A-ZА-ЯЁ]{1,5}-?\d+(?:[.\-]\d+)*)(?![0-9A-Za-zА-Яа-яЁё])")
NUM_RE = re.compile(r"(?<![\d.,])\d+(?:[.,]\d+)?")
WORD_RE = re.compile(r"[a-zа-я]{4,}")
FRAGMENT_MIN_CHARS = 40
DOCUMENTARY_CUES = ("аннулир", "ведомост", "состав тома", "состав графическ", "условн обознач",
                    "условные обозначения", "легенд", "перечень листов", "штамп", "оформлен")
# Location units: keyword + number, optionally a list ("корпуса 3 и 3.1", "зданий 7, 9 и 11").
UNIT_KEYWORDS = ("корпус", "здани", "блок", "секци", "строени")
_UNIT_HEAD_RE = re.compile(r"(?<![а-яё])(" + "|".join(UNIT_KEYWORDS) + r")[а-яё]*\.?\s*(?:№\s*)?(\d+(?:[.,]\d+)?)")
_UNIT_NEXT_RE = re.compile(r"\s*(?:,|и|–|-)\s*(?:№\s*)?(\d+(?:[.,]\d+)?)")


def norm(text: str) -> str:
    text = (text or "").translate(HOMO).lower().replace("ё", "е")
    text = re.sub(r"[«»\"“”„()\[\]{}:;!?]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def designations(text: str) -> set[str]:
    return {m.group(1).upper() for m in DESIG_RE.finditer((text or "").translate(HOMO))}


def numbers(text: str) -> list[str]:
    return [m.group(0).replace(",", ".") for m in NUM_RE.finditer(text or "")]


def distinctive(num: str) -> bool:
    try:
        value = float(num)
    except ValueError:
        return False
    return "." in num or value >= 11


def stems(text: str) -> list[str]:
    return [w[:6] for w in WORD_RE.findall(norm(text)) if w not in STOP]


def ngrams(text: str, n: int = 4) -> set[str]:
    text = norm(text)
    return {text[i:i + n] for i in range(max(0, len(text) - n + 1))}


def jaccard(a: set, b: set) -> float:
    return len(a & b) / len(a | b) if (a or b) else 0.0


def location_units(text: str) -> set[str]:
    """Syntactic location units: 'здание 7 (16 эт.)' → {'здани 7'}; 'зданий 7 и 7.1' → both."""
    t = (text or "").lower().replace("ё", "е")
    units: set[str] = set()
    for m in _UNIT_HEAD_RE.finditer(t):
        kw = next(k for k in UNIT_KEYWORDS if m.group(1).startswith(k))
        units.add(f"{kw} {m.group(2).replace(',', '.')}")
        pos = m.end()
        while True:
            nxt = _UNIT_NEXT_RE.match(t, pos)
            if not nxt:
                break
            after = t[nxt.end():nxt.end() + 2]
            # a list element must not be a count followed by a word ("…, 16 этажей")
            if re.match(r"\s?[а-яa-z]", after) and not re.match(r"\s?и\b", t[nxt.end():nxt.end() + 3]):
                break
            units.add(f"{kw} {nxt.group(1).replace(',', '.')}")
            pos = nxt.end()
    return units


@dataclass
class CardFeatures:
    designations: set[str]
    stems: list[str]
    blocks: set[tuple[str, int, str]]
    old_pages: set[int]
    new_pages: set[int]
    fragments: list[tuple[str, set[str]]]
    transitions: set[tuple[str, str]]
    value_pairs: set[tuple[str, str]]
    distinctive_numbers: set[str]
    semantic_text: str


def card_features(card: dict[str, Any]) -> CardFeatures:
    params = card.get("changed_parameters") or []
    semantic = " ".join([card.get("engineering_subject") or "", card.get("change_summary") or "",
                         card.get("old_state") or "", card.get("new_state") or "",
                         " ".join(p.get("name") or "" for p in params)])
    transitions: set[tuple[str, str]] = set()
    pairs: set[tuple[str, str]] = set()
    for p in params:
        old_nums, new_nums = numbers(p.get("old_value") or ""), numbers(p.get("new_value") or "")
        pairs.add((norm(p.get("old_value") or ""), norm(p.get("new_value") or "")))
        for o in old_nums[:3]:
            for n in new_nums[:3]:
                if o != n and (distinctive(o) or distinctive(n)):
                    transitions.add((o, n))
    all_text = semantic + " " + " ".join((p.get("old_value") or "") + " " + (p.get("new_value") or "") for p in params)
    evidence = card.get("evidence_items") or []
    return CardFeatures(
        designations=designations(all_text), stems=stems(semantic),
        blocks={(e["side"], int(e["physical_page"]), e["block_id"]) for e in evidence},
        old_pages=set(card.get("old_pages") or []), new_pages=set(card.get("new_pages") or []),
        fragments=[(e["side"], ngrams(e.get("relevant_fragment") or "")) for e in evidence
                   if len(norm(e.get("relevant_fragment") or "")) >= FRAGMENT_MIN_CHARS],
        transitions=transitions,
        value_pairs={p for p in pairs if p[0] != p[1] and len(p[0]) + len(p[1]) >= 4},
        distinctive_numbers={n for n in numbers(all_text) if distinctive(n)},
        semantic_text=semantic)


def tfidf_vectors(features: list[CardFeatures]) -> list[dict[str, float]]:
    docs = [Counter(f.stems + [f"#{d}" for d in f.designations]) for f in features]
    n = len(docs)
    df: Counter = Counter()
    for d in docs:
        df.update(d.keys())
    vectors = []
    for d in docs:
        v = {t: (1 + math.log(c)) * math.log((n + 1) / (df[t] + 1)) for t, c in d.items()}
        length = math.sqrt(sum(x * x for x in v.values())) or 1.0
        vectors.append({t: x / length for t, x in v.items()})
    return vectors


def cosine(a: dict[str, float], b: dict[str, float]) -> float:
    if len(a) > len(b):
        a, b = b, a
    return sum(x * b.get(t, 0.0) for t, x in a.items())


def pair_features(features: list[CardFeatures]) -> dict[tuple[int, int], dict[str, Any]]:
    """Features of every unordered pair (i < j) by position in the result."""
    vectors = tfidf_vectors(features)
    out: dict[tuple[int, int], dict[str, Any]] = {}
    for i in range(len(features)):
        for j in range(i + 1, len(features)):
            fa, fb = features[i], features[j]
            frag = 0.0
            for sa, ga in fa.fragments:
                for sb, gb in fb.fragments:
                    if sa == sb:
                        frag = max(frag, jaccard(ga, gb))
            out[(i, j)] = {
                "shared_blocks": len(fa.blocks & fb.blocks),
                "frag_sim": round(frag, 3),
                "shared_transitions": len(fa.transitions & fb.transitions) + len(fa.value_pairs & fb.value_pairs),
                "shared_pages_both_sides": bool(fa.old_pages & fb.old_pages) and bool(fa.new_pages & fb.new_pages),
                "shared_designations": sorted(fa.designations & fb.designations)[:12],
                "shared_distinctive_numbers": len(fa.distinctive_numbers & fb.distinctive_numbers),
                "lex_cos": round(cosine(vectors[i], vectors[j]), 3),
            }
    return out


def documentary_cues(card: dict[str, Any]) -> list[str]:
    text = norm(" ".join([card.get("engineering_subject") or "", card.get("change_summary") or "",
                          card.get("old_state") or "", card.get("new_state") or ""]))
    return [cue for cue in DOCUMENTARY_CUES if cue in text]
