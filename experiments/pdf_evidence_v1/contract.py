"""The asymmetric evidence contract, in a form the code can enforce.

One rule decides the shape of everything else: **a producer of native PDF
evidence may say that something is printed, and may never say that something is
not**.  The two directions are not symmetric because the failure modes are not
symmetric.  A missed span costs a fact.  An invented absence costs a false
``REMOVED`` — the defect that produced 212 of them on a single sheet
(``docs/stage_comparison_parameter_diff.md``) and the reason the native layer
was given confirm-only rights in the first place.

This module therefore holds three closed vocabularies and the guards that keep
them closed:

* ``CLAIMS`` — what a unit is allowed to assert.  Two values, neither of which
  is an absence.  There is no third value to add later by accident: the guard
  ``assert_closed_claims`` refuses anything else, and
  ``assert_no_absence_vocabulary`` refuses the words themselves anywhere in a
  produced artifact.
* ``APPLICABILITY`` — *at what scope* the assertion holds.  A string printed in
  the title block is present on the sheet; that is a true positive presence at
  sheet scope and a lie at fragment scope.  Scope is therefore carried with the
  claim, never dropped.
* ``DECODING`` — how the characters were obtained.  Repaired text is marked as
  repaired forever; a reader that wants only untouched text can have it.

What the contract deliberately does *not* contain: confidence numbers,
thresholds on likelihood, and any notion of "probably present".  Every value
here is decided by a check that either holds or does not.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

SCHEMA_VERSION = "pdf-evidence.v1"

# ---------------------------------------------------------------------------
# closed vocabularies
# ---------------------------------------------------------------------------

#: Where the characters came from.  ``RECOGNIZED_MARKDOWN`` is listed because a
#: consumer joins the two layers and must be able to tell them apart; this
#: producer only ever emits the native values.
PROVENANCE = (
    "NATIVE_PDF_TEXT",
    "NATIVE_PDF_TEXT_CAD_REPAIRED",
    "NATIVE_PDF_ANNOTATION",
    "RECOGNIZED_MARKDOWN",
)
NATIVE_PROVENANCE = frozenset(PROVENANCE[:3])

#: The scope at which a claim holds.  ``UNKNOWN`` is a real answer, not a gap:
#: it says the layer could not prove a scope, which is different from proving
#: the sheet scope.
APPLICABILITY = ("FRAGMENT_LOCAL", "SHEET_SHARED", "DOCUMENT_SHARED", "UNKNOWN")

#: Claim semantics.  The whole asymmetry lives in this tuple's length.
POSITIVE_PRESENCE = "POSITIVE_PRESENCE"
SUPPORT_ONLY = "SUPPORT_ONLY"
CLAIMS = (POSITIVE_PRESENCE, SUPPORT_ONLY)

#: Words a producer artifact may never carry as a value.  They are checked as
#: strings rather than as types, because the danger is not a wrong enum member —
#: it is a well-meaning string slipping into a payload a consumer then reads.
FORBIDDEN_CLAIM_TERMS = (
    "ABSENT",
    "ABSENCE",
    "REMOVED",
    "DELETED",
    "MISSING",
    "NOT_PRESENT",
    "NOT_FOUND",
    "DISAPPEARED",
)

#: How the text of a unit was decoded.
DECODED_NATIVE = "DECODED_NATIVE"
DECODED_CAD_REPAIRED = "DECODED_CAD_REPAIRED"
DECODED_CAD_UNRESOLVED = "DECODED_CAD_UNRESOLVED"
UNDECODABLE = "UNDECODABLE"
DECODING = (
    DECODED_NATIVE,
    DECODED_CAD_REPAIRED,
    DECODED_CAD_UNRESOLVED,
    UNDECODABLE,
)
#: Decoding states whose characters are trustworthy enough to assert presence.
#: ``DECODED_CAD_REPAIRED`` is here and ``DECODED_CAD_UNRESOLVED`` is not.  The
#: difference is whether the displacement was proven on the font: an unproven
#: one is never applied at all, because this corpus shows what applying it
#: costs.  ``Ʃ`` in ArialMT is the mathematical sigma of ``Ʃ=60м`` — a cable
#: length — and the best-fitting shift for that font would rewrite it into the
#: Cyrillic ``А``, turning a total into an ampere.
RELIABLE_DECODING = frozenset({DECODED_NATIVE, DECODED_CAD_REPAIRED})

#: How a unit was attached to a region.  Every one of these is a drawn relation.
TABLE_CELL = "TABLE_CELL"
DIRECT_CONTAINMENT = "DIRECT_CONTAINMENT"
CONNECTED_CALLOUT = "CONNECTED_CALLOUT"
STAMP_ZONE = "STAMP_ZONE"
NO_OWNERSHIP = "NO_OWNERSHIP"
AMBIGUOUS_OWNERSHIP = "AMBIGUOUS_OWNERSHIP"
OWNERSHIP_CHANNELS = (
    TABLE_CELL,
    DIRECT_CONTAINMENT,
    CONNECTED_CALLOUT,
    STAMP_ZONE,
    AMBIGUOUS_OWNERSHIP,
    NO_OWNERSHIP,
)
#: The only channels that establish structural ownership.  Proximity is not
#: among them and cannot be added without changing this tuple, which the tests
#: assert against a frozen expectation.
STRUCTURAL_OWNERSHIP = frozenset({TABLE_CELL, DIRECT_CONTAINMENT, CONNECTED_CALLOUT})


class ContractViolation(AssertionError):
    """Raised when a produced payload breaks a rule of the contract."""


# ---------------------------------------------------------------------------
# the unit
# ---------------------------------------------------------------------------


@dataclass
class EvidenceUnit:
    """One piece of text that the PDF itself carries, with its geometry.

    ``claim`` is derived, never passed in: a caller cannot promote a unit by
    asserting a stronger value, only by supplying evidence that satisfies
    ``derive_claim``.
    """

    unit_id: str
    document: str
    page: int
    provenance: str
    decoding: str
    text: str
    bbox: tuple[float, float, float, float] | None
    applicability: str
    ownership: str
    region_id: str | None = None
    cell: tuple[int, int] | None = None
    font: str | None = None
    size: float | None = None
    vertical: bool = False
    source_spans: int = 1
    repaired_chars: int = 0
    notes: tuple[str, ...] = ()

    @property
    def claim(self) -> str:
        return derive_claim(
            decoding=self.decoding,
            bbox=self.bbox,
            page=self.page,
            applicability=self.applicability,
            ownership=self.ownership,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit_id": self.unit_id,
            "document": self.document,
            "page": self.page,
            "provenance": self.provenance,
            "decoding": self.decoding,
            "claim": self.claim,
            "applicability": self.applicability,
            "ownership": self.ownership,
            "region_id": self.region_id,
            "cell": list(self.cell) if self.cell else None,
            "text": self.text,
            "bbox": [round(float(value), 2) for value in self.bbox] if self.bbox else None,
            "font": self.font,
            "size": self.size,
            "vertical": bool(self.vertical),
            "source_spans": int(self.source_spans),
            "repaired_chars": int(self.repaired_chars),
            "notes": list(self.notes),
        }


def derive_claim(
    *,
    decoding: str,
    bbox: Sequence[float] | None,
    page: int | None,
    applicability: str,
    ownership: str,
) -> str:
    """The three preconditions of an independent positive presence claim.

    1. the characters are trustworthy — the decoding is native or a repair
       proven on the font;
    2. the provenance is exact — a page number and a rectangle, so a reader can
       go and look at the same place;
    3. if the claim is asserted as fragment-local, the ownership was proven
       structurally.  A sheet-scope fact never becomes a fragment-scope fact by
       being carried into a passport (decision item 5).

    Anything short of that is ``SUPPORT_ONLY``: it may corroborate what another
    source asserts and may never assert on its own.  Note what is *not* a
    downgrade — a unit whose scope is only sheet-wide still positively asserts
    presence at sheet scope.  Losing the scope is what would make it a lie, and
    the scope travels with the claim.
    """
    if decoding not in RELIABLE_DECODING:
        return SUPPORT_ONLY
    if bbox is None or page is None:
        return SUPPORT_ONLY
    if applicability == "FRAGMENT_LOCAL" and ownership not in STRUCTURAL_OWNERSHIP:
        return SUPPORT_ONLY
    return POSITIVE_PRESENCE


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------


def _walk(value: Any, path: str = "$") -> Iterator[tuple[str, Any]]:
    yield path, value
    if isinstance(value, Mapping):
        for key, item in value.items():
            yield from _walk(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from _walk(item, f"{path}[{index}]")


#: The one place a forbidden word legitimately appears in a produced payload:
#: the contract's own list of forbidden words.  A dictionary of banned terms is
#: not a claim, and the exclusion is a single explicit path rather than a rule
#: that would let the words back in anywhere else.
DECLARATION_PATHS = ("$.contract.vocabularies.forbidden_claim_terms",)


def absence_vocabulary_violations(
    payload: Any, *, ignore_paths: Sequence[str] = ()
) -> list[dict[str, str]]:
    """Every place a produced payload names an absence.

    The check is on string *values*, folded to upper case, compared to the
    forbidden terms exactly.  A key may describe recognition coverage
    (``pages_without_a_markdown_section``); a value may not be the word
    ``MISSING``, because a value is what a consumer reads as the answer.
    """
    violations: list[dict[str, str]] = []
    for path, item in _walk(payload):
        if not isinstance(item, str) or item.strip().upper() not in FORBIDDEN_CLAIM_TERMS:
            continue
        if any(path.startswith(prefix) for prefix in ignore_paths):
            continue
        violations.append({"path": path, "value": item})
    return violations


def assert_no_absence_vocabulary(payload: Any, *, ignore_paths: Sequence[str] = ()) -> None:
    violations = absence_vocabulary_violations(payload, ignore_paths=ignore_paths)
    if violations:
        raise ContractViolation(
            f"producer payload asserts an absence at {violations[0]['path']}: "
            f"{violations[0]['value']!r} ({len(violations)} place(s))"
        )


def claim_violations(payload: Any) -> list[dict[str, str]]:
    """Claims outside the closed vocabulary, wherever a ``claim`` key appears.

    Only string values are claims.  ``claim_semantics: [...]`` is a declaration
    of what the vocabulary *is*, and reading a schema as if it were an instance
    is how a guard ends up reporting itself.
    """
    violations: list[dict[str, str]] = []
    for path, item in _walk(payload):
        if not isinstance(item, Mapping):
            continue
        for key in ("claim", "claim_semantics"):
            value = item.get(key)
            if isinstance(value, str) and value not in CLAIMS:
                violations.append({"path": f"{path}.{key}", "value": value})
    return violations


def assert_closed_claims(payload: Any) -> None:
    violations = claim_violations(payload)
    if violations:
        raise ContractViolation(
            f"claim outside the closed vocabulary at {violations[0]['path']}: "
            f"{violations[0]['value']!r}"
        )


def assert_scope_discipline(units: Sequence[EvidenceUnit]) -> None:
    """Fragment scope requires structural ownership, everywhere, always."""
    for unit in units:
        if unit.applicability == "FRAGMENT_LOCAL" and unit.ownership not in STRUCTURAL_OWNERSHIP:
            raise ContractViolation(
                f"unit {unit.unit_id} claims fragment scope through {unit.ownership}"
            )
        if unit.applicability not in APPLICABILITY:
            raise ContractViolation(f"unit {unit.unit_id} has scope {unit.applicability!r}")
        if unit.provenance not in PROVENANCE:
            raise ContractViolation(f"unit {unit.unit_id} has provenance {unit.provenance!r}")
        if unit.decoding not in DECODING:
            raise ContractViolation(f"unit {unit.unit_id} has decoding {unit.decoding!r}")
        if unit.ownership not in OWNERSHIP_CHANNELS:
            raise ContractViolation(f"unit {unit.unit_id} has ownership {unit.ownership!r}")


def verify_producer_output(payload: Any, units: Sequence[EvidenceUnit] = ()) -> dict[str, Any]:
    """Run every guard and report, instead of only raising.

    Both forms exist on purpose: the pipeline wants the exception, the audit
    wants the number to print.
    """
    absence = absence_vocabulary_violations(payload, ignore_paths=DECLARATION_PATHS)
    claims = claim_violations(payload)
    scope: list[dict[str, str]] = []
    for unit in units:
        if unit.applicability == "FRAGMENT_LOCAL" and unit.ownership not in STRUCTURAL_OWNERSHIP:
            scope.append({"unit_id": unit.unit_id, "ownership": unit.ownership})
    return {
        "absence_vocabulary_violations": len(absence),
        "absence_vocabulary_examples": absence[:5],
        "claim_vocabulary_violations": len(claims),
        "scope_discipline_violations": len(scope),
        "scope_discipline_examples": scope[:5],
        "claims_allowed": list(CLAIMS),
        "structural_ownership_channels": sorted(STRUCTURAL_OWNERSHIP),
    }


# ---------------------------------------------------------------------------
# the contract, as data
# ---------------------------------------------------------------------------


def contract_document() -> dict[str, Any]:
    """The contract in machine-readable form, for the artifact."""
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "asymmetric_evidence_contract",
        "model_calls": 0,
        "vocabularies": {
            "provenance": list(PROVENANCE),
            "applicability": list(APPLICABILITY),
            "claim_semantics": list(CLAIMS),
            "decoding": list(DECODING),
            "ownership_channels": list(OWNERSHIP_CHANNELS),
            "structural_ownership_channels": sorted(STRUCTURAL_OWNERSHIP),
            "forbidden_claim_terms": list(FORBIDDEN_CLAIM_TERMS),
        },
        "rules": [
            {
                "id": "R1",
                "statement": (
                    "native PDF text may independently assert POSITIVE_PRESENCE when the "
                    "characters are reliably decoded, the provenance is exact (page and "
                    "rectangle), and — for a fragment-local assertion — region membership "
                    "was proven structurally"
                ),
            },
            {
                "id": "R2",
                "statement": (
                    "native PDF text may never assert an absence; the producer has no "
                    "vocabulary for one, and the guard refuses the words in any value"
                ),
            },
            {
                "id": "R3",
                "statement": (
                    "the absence of a fact in the recognized Markdown does not refute "
                    "positive native evidence"
                ),
            },
            {
                "id": "R4",
                "statement": (
                    "the absence of a fact in both the PDF and the Markdown proves "
                    "nothing about the document; the layer emits no unit at all"
                ),
            },
            {
                "id": "R5",
                "statement": (
                    "a sheet-level fact never enters a function passport as fragment-local "
                    "without proven structural ownership"
                ),
            },
            {
                "id": "R6",
                "statement": (
                    "a title-block value stays SHEET_SHARED (or DOCUMENT_SHARED when the "
                    "same value is printed in the title block of several sheets) unless a "
                    "narrower membership is proven"
                ),
            },
            {
                "id": "R7",
                "statement": "proximity never proves membership; only a drawn relation does",
            },
        ],
    }


__all__ = [
    "SCHEMA_VERSION",
    "PROVENANCE",
    "NATIVE_PROVENANCE",
    "APPLICABILITY",
    "CLAIMS",
    "POSITIVE_PRESENCE",
    "SUPPORT_ONLY",
    "DECODING",
    "RELIABLE_DECODING",
    "DECODED_NATIVE",
    "DECODED_CAD_REPAIRED",
    "DECODED_CAD_UNRESOLVED",
    "UNDECODABLE",
    "OWNERSHIP_CHANNELS",
    "STRUCTURAL_OWNERSHIP",
    "TABLE_CELL",
    "DIRECT_CONTAINMENT",
    "CONNECTED_CALLOUT",
    "STAMP_ZONE",
    "AMBIGUOUS_OWNERSHIP",
    "NO_OWNERSHIP",
    "FORBIDDEN_CLAIM_TERMS",
    "DECLARATION_PATHS",
    "ContractViolation",
    "EvidenceUnit",
    "derive_claim",
    "absence_vocabulary_violations",
    "assert_no_absence_vocabulary",
    "claim_violations",
    "assert_closed_claims",
    "assert_scope_discipline",
    "verify_producer_output",
    "contract_document",
]
