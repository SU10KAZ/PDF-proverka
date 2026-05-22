"""Object-signal detector (Phase 1 scaffolding, P0 safety layer).

Deterministic, regex/keyword-based detection of "object signals" in the
project's Markdown text. Object signals are coarse predicates about the
object (building, plant, system) that gate conditional checklist items:
e.g. EOM-22 «Молниезащита» is only valid if the MD signals that lightning
protection is required.

This module is NOT wired into any runtime. It exists so the future
`completeness_runner` can call `detect_object_signals(md_text)` and feed
the result into `checklist_gates.is_item_applicable(...)`.

The detection is intentionally conservative: false-negatives are preferred
over false-positives, because a missing signal blocks a finding (safe),
while a phantom signal allows a (potentially wrong) finding (unsafe).

Signal vocabulary is locked here in `KNOWN_SIGNALS`. Adding a new signal
requires:
  1. add to KNOWN_SIGNALS
  2. add a `SignalRule` to `_RULES`
  3. update tests

Pure stdlib, Python 3.11+. No LLM, no network.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


# Locked allow-list of signal names. Mirrors the list in /goal Stage 3 plus
# project-specific ones that the metadata uses.
KNOWN_SIGNALS: frozenset[str] = frozenset({
    "motors_present",
    "high_rise",
    "fire_system_present",
    "lightning_protection_required",
    "category_1_power",
    "smoke_ventilation_required",
    "underground_structure",
    "seismic_region",
    "residential_building",
    "public_building",
    "ventilation_system_present",
    "pumps_present",
    "facade_present",
    "roof_operated",
    "automation_present",
    "cable_lines_present",
    "wet_zone_present",
    "elevators_present",
    "generators_present",
})


@dataclass(frozen=True)
class SignalRule:
    """One signal -> list of compiled regex patterns; any match fires it."""

    name: str
    patterns: tuple[re.Pattern[str], ...]


def _compile(*sources: str, flags: int = re.IGNORECASE) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(s, flags) for s in sources)


# Per-signal regex bundles. Russian-first because production MDs are in
# Russian; English fallbacks added where Russian-only would miss obvious
# tokens.
_RULES: tuple[SignalRule, ...] = (
    SignalRule(
        "motors_present",
        _compile(
            r"\bэлектродвигател",
            r"\b(?:асинхронн[ыо]|синхронн[ыо])\b.{0,40}\bдвигател",
            r"\b(?:промышл[еённ]+\w*|производственн\w*)\s+нагрузк",
            r"\bcos\s*[φϕφ]\b",
        ),
    ),
    SignalRule(
        "high_rise",
        _compile(
            # "≥ 28 м", "≥ 75 м", "28 м и более", "высот... 75 м"
            r"(?:≥|>=|более|свыше|от)\s*(?:28|50|75|100)\s*м(?![а-я])",
            r"(?:28|50|75|100)\s*м\s+и\s+более",
            r"\bвысотн(?:ое|ого|ый|ых)\s+(?:здани|объект|сооружени)",
            r"\bмногоэтажн[аоы]+\s+(?:здани|жил|дом)",
            r"\b(?:количеств[ао]\s+этажей|этажност[ьи])\s*[:\-]?\s*(?:2[5-9]|[3-9]\d|\d{3})",
        ),
    ),
    SignalRule(
        "fire_system_present",
        _compile(
            r"\bА(?:П|У)С\b",  # АПС, АУС, АУПС
            r"\bАУПС\b",
            r"\bавтоматическ\w+\s+пожарн\w+\s+сигнализаци",
            r"\bпожарн\w+\s+сигнализаци",
            r"\bсистем\w+\s+(?:АПС|пожаротушени)",
            r"\bАУПТ\b",
            r"\bпожаротушени",
            r"\bСП\s*484\.1311500",
            r"\bСП\s*486\.1311500",
            r"\bФЗ[\s\-]*123",
        ),
    ),
    SignalRule(
        "lightning_protection_required",
        _compile(
            r"\bмолниезащит",
            r"\bСО[\s\-]*153[\s\-]*34\.21\.122",
            r"\bкатегори\w+\s+молниезащит",
            r"\bРД\s*34\.21\.122",
        ),
    ),
    SignalRule(
        "category_1_power",
        _compile(
            r"\bI\s+категори\w+\s+(?:надёжност|надежност|электроснабжен)",
            r"\bI\s*-\s*особ(?:ой|ая)\s+категори",
            r"\bпервой\s+категори\w+\s+(?:надёжност|надежност|электроснабжен)",
            r"\bкатегор\w+\s+(?:надёжност|надежност)\s*[:\-]?\s*I\b",
            r"\bАВР\b",  # automatic transfer switch — strong signal for cat 1
        ),
    ),
    SignalRule(
        "smoke_ventilation_required",
        _compile(
            r"\bпротиводымн\w+\s+вентиляц",
            r"\bПДВ\b",
            r"\bдымоудален",
            r"\bподпор\s+воздуха",
            r"\bСП\s*7\.13130",
        ),
    ),
    SignalRule(
        "underground_structure",
        _compile(
            r"\bподземн\w+\s+(?:часть|этаж|\w*стоянк|\w*парковк|сооружен|помещени)",
            r"\bподвал\w*\b",
            r"\bцокольн\w+\s+этаж",
            r"\bтехподполь\w*",
            r"\bстилобат",
            r"\bпаркинг\s+подземн",
            r"\bниже\s+(?:0\.000|нулев)",
        ),
    ),
    SignalRule(
        "seismic_region",
        _compile(
            r"\bсейсмич\w+\s+(?:район|воздействи|нагрузк)",
            r"\bсейсмост\w+",
            r"\b(?:сейсмо)\b",
            r"\bбалл\w*\s+по\s+ОСР",
            r"\bОСР[\s\-]*\d{4}",
            r"\bСП\s*14\.13330",
        ),
    ),
    SignalRule(
        "residential_building",
        _compile(
            r"\bМКД\b",
            r"\bмногоквартирн\w+\s+(?:жил|дом)",
            r"\bжил(?:ой|ое|ого|ого|ая|ых|ые|ом|ыми)\s+(?:дом|МКД|здани|корпус|комплекс|помещени|квартир|объект)",
            r"\bжилищн\w+\s+(?:фонд|строит)",
            r"\bапартамент",  # treat as residential-grade for inflation
            r"\bкласс\w*\s+Ф1",  # Ф1.x — жилые
        ),
    ),
    SignalRule(
        "public_building",
        _compile(
            r"\bобщественн\w+\s+(?:здани|сооружен|комплекс|помещени|назначени)",
            r"\bкласс\w*\s+Ф[234]",  # Ф2..Ф4 — общественные
            r"\bмассов\w+\s+пребыван",
            r"\bторгов\w+\s+(?:центр|комплекс|здани)",
            r"\bдетск(?:ий|ого|ому)\s+сад",
            r"\bшкол\w*\b",
            r"\bбольниц\w*",
            r"\bполиклиник",
            r"\bофисн\w+\s+(?:центр|здани|комплекс)",
        ),
    ),
    SignalRule(
        "ventilation_system_present",
        _compile(
            r"\bвентустановк",
            r"\bвоздуховод",
            r"\bПВУ\b",
            r"\bВУ\d+\b",
            r"\bП\d+\b",  # P1, P2 — приточные системы (paired w/ ventilation context)
            r"\bвентиляц",
            r"\bкратност\w+\s+воздухообмен",
            r"\bприточн\w+\s+(?:система|вентиляц)",
            r"\bвытяжн\w+\s+(?:система|вентиляц)",
        ),
    ),
    SignalRule(
        "pumps_present",
        _compile(
            r"\bнасос\w*\b",
            r"\bнасосн\w+\s+(?:станци|групп|оборудовани)",
            r"\bповысительн\w+\s+насос",
        ),
    ),
    SignalRule(
        "facade_present",
        _compile(
            r"\bвитраж",
            r"\bнавесн\w+\s+фасад",
            r"\bСПФ\b",
            r"\bструктурн\w+\s+остеклен",
            r"\bсветопрозрач\w+\s+(?:конструкц|констр)",
        ),
    ),
    SignalRule(
        "roof_operated",
        _compile(
            r"\bэксплуатируем\w+\s+кровл",
            r"\bкровл\w+\s+эксплуатируем",
            r"\bвыход\s+на\s+кровл",
            r"\bкровл\w+\s+с\s+выходом",
            r"\bпарапет\s+эксплуатируем",
        ),
    ),
    SignalRule(
        "automation_present",
        _compile(
            r"\bавтоматизаци",
            r"\bАСУ(?:Э|ТП|З|З\b)",
            r"\bдиспетчериз",
            r"\bАХЗ\b",
            r"\bИТП\b",
            r"\bтеплов\w+\s+пункт",
        ),
    ),
    SignalRule(
        "cable_lines_present",
        _compile(
            r"\bкабельн\w+\s+(?:журнал|трасс|лини|сет)",
            r"\bтрасс\w+\s+кабел",
            r"\bкабел[ьяи]+\s+ВВГ",
            r"\bкабел[ьяи]+\s+FRLS",
            r"\bкабел[ьяи]+\s+FRHF",
        ),
    ),
    SignalRule(
        "wet_zone_present",
        _compile(
            r"\bванн\w+\s+комнат",
            r"\bсанузе?л\w*",
            r"\bдушев\w+\s+(?:помещени|кабин)",
            r"\bвлажн\w+\s+помещени",
            r"\bрозеточн\w+\s+(?:групп|лини)",
            r"\bбассейн",
        ),
    ),
    SignalRule(
        "elevators_present",
        _compile(
            r"\bлифт\w*\b",
            r"\bподъёмник",
            r"\bподъемник",
            r"\bлифтов\w+\s+(?:шахт|холл|узел)",
        ),
    ),
    SignalRule(
        "generators_present",
        _compile(
            r"\bДГУ\b",
            r"\bдизел\w*\s+генератор",
            r"\bдизел\w*\s+электростанц",
            r"\bрезервн\w+\s+источник\w*\s+питан",
            r"\bИБП\b",
        ),
    ),
)


# Quick lookup by name (handy for tests / debug).
_RULES_BY_NAME: dict[str, SignalRule] = {r.name: r for r in _RULES}


def detect_object_signals(text: object) -> dict[str, bool]:
    """Run every signal rule against text; return {signal: bool}.

    Null-safe: None / non-str / empty input returns all signals False.
    All KNOWN_SIGNALS are always present in the result, so callers can do
    ``detected["high_rise"]`` without KeyError.
    """
    result: dict[str, bool] = {name: False for name in KNOWN_SIGNALS}
    if not isinstance(text, str) or not text:
        return result
    for rule in _RULES:
        for pat in rule.patterns:
            if pat.search(text):
                result[rule.name] = True
                break
    return result


def _required_signals_from_item(item_metadata: Mapping[str, object]) -> list[str]:
    raw = item_metadata.get("object_signals") if item_metadata else None
    if not isinstance(raw, (list, tuple)):
        return []
    out: list[str] = []
    for token in raw:
        if isinstance(token, str) and token in KNOWN_SIGNALS:
            out.append(token)
    return out


def has_required_signals(
    item_metadata: Mapping[str, object],
    detected_signals: Mapping[str, bool] | Iterable[str],
) -> bool:
    """True iff every signal required by the item is fired.

    `detected_signals` may be either the dict returned by
    ``detect_object_signals(text)`` *or* any iterable of signal names that
    are considered fired. An item with no required signals always passes.
    """
    required = _required_signals_from_item(item_metadata)
    if not required:
        return True

    fired: set[str]
    if isinstance(detected_signals, Mapping):
        fired = {k for k, v in detected_signals.items() if v}
    else:
        fired = {s for s in detected_signals if isinstance(s, str)}

    return all(sig in fired for sig in required)


def missing_required_signals(
    item_metadata: Mapping[str, object],
    detected_signals: Mapping[str, bool] | Iterable[str],
) -> list[str]:
    """Return required signals that did NOT fire (empty if none required)."""
    required = _required_signals_from_item(item_metadata)
    if not required:
        return []
    if isinstance(detected_signals, Mapping):
        fired = {k for k, v in detected_signals.items() if v}
    else:
        fired = {s for s in detected_signals if isinstance(s, str)}
    return [sig for sig in required if sig not in fired]


def known_signal_names() -> list[str]:
    """Sorted snapshot of KNOWN_SIGNALS — handy for tests / docs."""
    return sorted(KNOWN_SIGNALS)


def signal_rules_by_name() -> Mapping[str, SignalRule]:
    """Read-only view; convenient for introspection in tests."""
    return dict(_RULES_BY_NAME)


__all__ = [
    "KNOWN_SIGNALS",
    "SignalRule",
    "detect_object_signals",
    "has_required_signals",
    "missing_required_signals",
    "known_signal_names",
    "signal_rules_by_name",
]
