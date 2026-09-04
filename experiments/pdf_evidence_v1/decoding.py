"""CAD font decoding, audited per font instead of assumed globally.

AutoCAD writes Cyrillic in some ISOCPEUR/GOST subsets as a block of Latin
Extended-B codepoints displaced by a constant.  v3.0 applied one constant, 581,
wherever it happened to land in Cyrillic.  That works, and it is also exactly
the kind of rule that is right until it silently is not: the constant was never
proven *on the font*, so a stray glyph in an unrelated font was one accident
away from being rewritten into a plausible Russian word.

This module makes the repair a measured property of each font.

The first thing the measurement establishes is that **the shift cannot be found
by search**.  Maximizing the Cyrillic yield is the obvious objective and it is
wrong, because garbage is also Cyrillic: on ``IOS1.1/LEFT`` the yield-optimal
shift for ISOCPEUR is 565 and turns the title of a single-line diagram into
``ЎФЭЮЫШЭХЩЭРп аРбзХвЭРп беХЬР``, while the documented constant 581 turns the
same bytes into ``Однолинейная расчетная схема ВРУ-3``.  565 scores higher and
means nothing.  The only shift the recognized Markdown ever confirms is 581 —
nine times against zero for every alternative.

So the constant is fixed and external, and what each font is audited for is:

* **identifiability** — several distinct codepoints, because one codepoint
  repeated is no evidence about a displacement at all;
* **coverage** — how much of the font's block the constant places in Cyrillic;
* **independent confirmation** — whether a repaired string is then found
  verbatim in the recognized Markdown of its own page.

Either coverage or confirmation carries a font, and the reason both are allowed
is in the data: ``IOS2.1/LEFT`` places 42 of 64 block characters and would fail
a coverage gate, yet its repairs read ``Выпуск водопроводного ввода`` and
``Отметка оси трубы = -2.51`` and three of them are confirmed word for word by
the recognized layer.  The 22 that escape are one single codepoint (``Ⱦ``)
which the subset evidently maps outside this run; those characters are left
exactly as printed rather than guessed at.

Using the Markdown to validate a *codec* is not the Markdown vetoing a *fact*.
It may not veto one (decision item 3), and it is not being asked to: it is
being asked whether these bytes, decoded this way, ever produced a string an
independent reading also saw.

The yield-optimal shift is still computed, and reported as the diagnostic that
shows why searching is the wrong method.

Everything else keeps the characters exactly as the PDF wrote them, is marked
``DECODED_CAD_UNRESOLVED``, and may only support a fact another source
asserts.  Refusing to repair is not caution for its own sake: this corpus
contains ``Ʃ=60м`` — a cable length written with the mathematical sigma — whose
font's best-fitting shift would turn the sigma into the Cyrillic ``А``.  One
unreadable glyph is a smaller loss than one plausible wrong word, because the
plausible wrong word survives review.

The audit also counts how often a repaired string is confirmed verbatim by the
recognized Markdown of the same page — an independent check of the rule, not an
input to it.
"""
from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from .contract import (
    DECODED_CAD_REPAIRED,
    DECODED_CAD_UNRESOLVED,
    DECODED_NATIVE,
    UNDECODABLE,
)

#: Latin Extended-B block the AutoCAD subsets use for Cyrillic.
CAD_BLOCK = (0x0180, 0x024F)
#: The documented constant of the AutoCAD subset.  It is not a fitted value:
#: it is the only displacement any independent reading of this corpus confirms.
CORPUS_CAD_SHIFT = 581
#: Control codes in the same subsets are ASCII displaced by this much.
CAD_CONTROL_SHIFT = 31
CYRILLIC_BLOCK = (0x0400, 0x045F)

#: A font needs at least this many block characters before anything measured on
#: it means anything.
MIN_BLOCK_CHARS = 8
#: And they must be at least this many *distinct* codepoints.  One codepoint
#: repeated 59 times identifies nothing: every shift that carries it into
#: Cyrillic scores a perfect yield, and the argmax then picks by tie-break
#: rather than by evidence.  This corpus contains exactly that trap — ``Ʃ``
#: (U+01A9, the mathematical sigma of ``Ʃ=60м``, a cable length) is the only
#: block codepoint ArialMT uses, and its best-scoring shift would rewrite it
#: into the Cyrillic ``А``.
MIN_DISTINCT_BLOCK_CODES = 4
#: Share of the font's block characters the constant must carry into Cyrillic.
#: A partial fit is a different encoding wearing the same font name, not this
#: one, and half a codec is not a codec.
MIN_CYRILLIC_YIELD = 0.95

#: Codepoints that cannot be trusted as characters at all.
REPLACEMENT_CHAR = 0xFFFD
PRIVATE_USE = (0xE000, 0xF8FF)

_WORD_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]{2,}")
_FOLD_RE = re.compile(r"[^0-9a-zа-яё]+")


def _fold(value: str) -> str:
    """Same folding as ``textnorm.normalize``, kept local to avoid a cycle."""
    return _FOLD_RE.sub(" ", str(value).lower().replace("ё", "е")).strip()


def _in_block(code: int) -> bool:
    return CAD_BLOCK[0] <= code <= CAD_BLOCK[1]


def _is_cyrillic(code: int) -> bool:
    return CYRILLIC_BLOCK[0] <= code <= CYRILLIC_BLOCK[1]


def is_undecodable(text: str) -> bool:
    """Characters no repair can rescue: replacement, private use, unassigned."""
    for char in text:
        code = ord(char)
        if code == REPLACEMENT_CHAR:
            return True
        if PRIVATE_USE[0] <= code <= PRIVATE_USE[1]:
            return True
        if unicodedata.category(char) in {"Co", "Cn"}:
            return True
    return False


def apply_shift(text: str, shift: int) -> tuple[str, int]:
    """Apply a CAD shift.  Returns the text and how many characters moved.

    A character only moves when the shift lands it in Cyrillic.  That is the
    whole safety of the operation: a glyph outside the subset — ``Ʃ`` in
    ArialMT, which this corpus really contains — lands in Greek and is left
    exactly as it was.
    """
    moved = 0
    out: list[str] = []
    for char in text:
        code = ord(char)
        if _in_block(code) and _is_cyrillic(code + shift):
            out.append(chr(code + shift))
            moved += 1
        elif code < 0x20:
            out.append(chr(code + CAD_CONTROL_SHIFT))
        else:
            out.append(char)
    return "".join(out), moved


def best_shift(codes: Iterable[int]) -> tuple[int | None, float]:
    """The shift that carries the most block characters into Cyrillic.

    Searched rather than assumed, over every shift that could map the block
    into Cyrillic at all.  Ties go to the smaller shift so the result is
    deterministic.
    """
    block = [code for code in codes if _in_block(code)]
    if not block:
        return None, 0.0
    low = CYRILLIC_BLOCK[0] - CAD_BLOCK[1]
    high = CYRILLIC_BLOCK[1] - CAD_BLOCK[0]
    winner: int | None = None
    best = -1
    for shift in range(low, high + 1):
        hits = sum(1 for code in block if _is_cyrillic(code + shift))
        if hits > best:
            best, winner = hits, shift
    return winner, (best / len(block) if block else 0.0)


@dataclass
class FontProfile:
    """What one font does with the CAD block, measured on the document."""

    font: str
    spans: int = 0
    characters: int = 0
    block_characters: int = 0
    undecodable_spans: int = 0
    codes: Counter = field(default_factory=Counter)
    samples: list[tuple[int, str]] = field(default_factory=list)
    markdown_confirmations: int = 0
    markdown_checked: int = 0
    shift: int | None = None
    cyrillic_yield: float = 0.0
    argmax_shift: int | None = None
    argmax_yield: float = 0.0
    distinct_codes: int = 0
    proven: bool = False
    reason: str = "no_block_characters"

    def finalize(self, bodies: Mapping[int, str] | None = None) -> "FontProfile":
        self._confirm(bodies or {})
        argmax, argmax_yield = best_shift(self.codes.elements())
        self.argmax_shift = argmax
        self.argmax_yield = round(float(argmax_yield), 4)
        self.distinct_codes = len(self.codes)
        block = [code for code in self.codes.elements() if _in_block(code)]
        covered = sum(1 for code in block if _is_cyrillic(code + CORPUS_CAD_SHIFT))
        self.cyrillic_yield = round(covered / len(block), 4) if block else 0.0
        self.shift = CORPUS_CAD_SHIFT if block else None
        if not block:
            self.proven, self.reason = False, "no_block_characters"
        elif self.block_characters < MIN_BLOCK_CHARS:
            self.proven, self.reason = False, "too_few_block_characters"
        elif self.distinct_codes < MIN_DISTINCT_BLOCK_CODES:
            self.proven, self.reason = False, "one_codepoint_identifies_no_displacement"
        elif self.cyrillic_yield >= MIN_CYRILLIC_YIELD:
            self.proven, self.reason = True, "the_constant_covers_this_font"
        elif self.markdown_confirmations:
            self.proven, self.reason = True, "repairs_confirmed_word_for_word_by_the_recognized_layer"
        else:
            self.proven, self.reason = False, "neither_covered_nor_independently_confirmed"
        return self

    def _confirm(self, bodies: Mapping[int, str]) -> None:
        """How many repaired strings an independent reading also saw."""
        if not bodies:
            return
        seen: set[str] = set()
        for page, text in self.samples:
            repaired, moved = apply_shift(text, CORPUS_CAD_SHIFT)
            if not moved:
                continue
            folded = _fold(repaired)
            if len(folded) < 4 or folded in seen:
                continue
            seen.add(folded)
            self.markdown_checked += 1
            if folded in _fold(bodies.get(int(page), "")):
                self.markdown_confirmations += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "font": self.font,
            "spans": self.spans,
            "characters": self.characters,
            "block_characters": self.block_characters,
            "distinct_block_codepoints": self.distinct_codes,
            "undecodable_spans": self.undecodable_spans,
            "applied_shift": self.shift,
            "corpus_shift_coverage": self.cyrillic_yield,
            "yield_optimal_shift": self.argmax_shift,
            "yield_optimal_coverage": self.argmax_yield,
            "repairs_checked_against_markdown": self.markdown_checked,
            "repairs_confirmed_by_markdown": self.markdown_confirmations,
            "repair_proven": self.proven,
            "reason": self.reason,
        }


class DecodingProfile:
    """Per-font decoding decisions for one document."""

    def __init__(self, fonts: Mapping[str, FontProfile]) -> None:
        self.fonts = dict(fonts)

    def font(self, name: str) -> FontProfile | None:
        return self.fonts.get(name)

    def decode(self, text: str, font: str | None) -> tuple[str, str, int]:
        """Return ``(text, decoding_status, repaired_characters)``."""
        if not text:
            return text, DECODED_NATIVE, 0
        if is_undecodable(text):
            return text, UNDECODABLE, 0
        codes = [ord(char) for char in text]
        if not any(_in_block(code) for code in codes):
            return text, DECODED_NATIVE, 0
        profile = self.fonts.get(str(font or ""))
        if not (profile and profile.proven):
            # Includes the case where a *different* shift would fit better.
            # Fitting better is not being right (see the module docstring).
            # An unproven displacement is never applied.  Leaving the glyph as
            # the PDF wrote it costs one unreadable character; applying a shift
            # nobody proved costs a plausible wrong word, and a plausible wrong
            # word is the kind of evidence that survives review.
            return text, DECODED_CAD_UNRESOLVED, 0
        repaired, moved = apply_shift(text, CORPUS_CAD_SHIFT)
        if moved == 0:
            return text, DECODED_CAD_UNRESOLVED, 0
        return repaired, DECODED_CAD_REPAIRED, moved

    def to_dict(self) -> dict[str, Any]:
        return {
            "corpus_shift": CORPUS_CAD_SHIFT,
            "min_block_characters": MIN_BLOCK_CHARS,
            "min_distinct_block_codepoints": MIN_DISTINCT_BLOCK_CODES,
            "min_cyrillic_yield": MIN_CYRILLIC_YIELD,
            "fonts": [
                profile.to_dict()
                for profile in sorted(
                    self.fonts.values(), key=lambda item: (-item.block_characters, item.font)
                )
            ],
        }


#: Repaired strings kept per font for the confirmation check.  A cap, because
#: the check is a sample of the codec, not an inventory of the document.
MAX_SAMPLES_PER_FONT = 400


def build_profile(
    spans: Iterable[Mapping[str, Any]],
    bodies: Mapping[int, str] | None = None,
) -> DecodingProfile:
    """Measure every font of a document before a single fact is extracted.

    Two passes are unavoidable and cheap: the displacement of a font cannot be
    validated from the span that needs it.
    """
    fonts: dict[str, FontProfile] = {}
    for span in spans:
        name = str(span.get("font") or "")
        text = str(span.get("text") or "")
        profile = fonts.setdefault(name, FontProfile(font=name))
        profile.spans += 1
        profile.characters += len(text)
        if is_undecodable(text):
            profile.undecodable_spans += 1
        block = 0
        for char in text:
            code = ord(char)
            if _in_block(code):
                block += 1
                profile.codes[code] += 1
        profile.block_characters += block
        if block and len(profile.samples) < MAX_SAMPLES_PER_FONT:
            profile.samples.append((int(span.get("page") or 0), text))
    return DecodingProfile({
        name: profile.finalize(bodies) for name, profile in fonts.items()
    })


def confirmation_audit(
    repairs: Sequence[Mapping[str, Any]], bodies: Mapping[int, str], normalize
) -> dict[str, Any]:
    """Independent check of the repair rule against the recognized Markdown.

    This never gates a claim — under the asymmetric contract native text does
    not need the Markdown's permission to say that something is printed.  It is
    reported because a repair rule that no independent reading ever confirms
    would be a rule worth doubting.
    """
    per_font: dict[str, Counter] = {}
    for row in repairs:
        font = str(row.get("font") or "")
        counter = per_font.setdefault(font, Counter())
        counter["repaired_units"] += 1
        body = normalize(bodies.get(int(row["page"]), ""))
        words = [
            word for word in _WORD_RE.findall(str(row.get("text") or ""))
            if len(word) >= 4
        ]
        if not words:
            counter["not_checkable"] += 1
            continue
        hits = sum(1 for word in words if normalize(word) in body)
        counter["checkable_units"] += 1
        if hits == len(words):
            counter["confirmed_by_markdown"] += 1
        elif hits:
            counter["partially_confirmed"] += 1
    return {
        "fonts": {
            font: dict(sorted(counter.items())) for font, counter in sorted(per_font.items())
        }
    }


__all__ = [
    "CAD_BLOCK",
    "CORPUS_CAD_SHIFT",
    "CYRILLIC_BLOCK",
    "MIN_BLOCK_CHARS",
    "MIN_CYRILLIC_YIELD",
    "MIN_DISTINCT_BLOCK_CODES",
    "MAX_SAMPLES_PER_FONT",
    "DecodingProfile",
    "FontProfile",
    "apply_shift",
    "best_shift",
    "build_profile",
    "confirmation_audit",
    "is_undecodable",
]
