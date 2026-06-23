"""Large-sheet feeder/consumer matching (offline, default OFF).

Сопоставляет фидеры ГРЩ/ВРУ старой и новой стадии **по потребителю/нагрузке**,
а не по имени щита. Проблема: в новой стадии фидеры массово переименованы
(``ВРУ-2`` → ``ГРЩ1-РП2-1``), поэтому Opus, который матчит по имени щита,
сворачивает весь пофидерный diff в одно «структура переработана» и теряет
изменения нагрузок/сечений/вводов (GPT #2–8, #10–12, #15).

Модуль строит детерминированную таблицу ``OLD feeder ↔ NEW feeder ↔ что
изменилось`` на основе уже извлечённых ``page_enriched.json`` обеих сторон.
Сеть/Qwen/Opus не задействуются — это чистый разбор JSON.

Главный флаг — ``STAGE_COMPARISON_LARGE_SHEET_FEEDER_MATCHING_ENABLED`` (default
**false**). При false поведение rich-render идентично прежнему. При true в rich
MD добавляется секция «### Сопоставление фидеров по потребителю/нагрузке».

Первый этап (этот модуль): только offline dry-run + артефакты-отчёты. Live-врезка
в enriched MD остаётся за флагом и подключается отдельно после ревью отчёта.
"""

from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

__all__ = [
    "feeder_matching_enabled",
    "normalize_consumer",
    "extract_feeders",
    "score_pair",
    "match_feeders",
    "FeederMatchResult",
    "build_feeder_match_report",
    "render_feeder_match_md_section",
    "feeder_section_for_pair",
    "feeder_candidate_changes_enabled",
    "FeederCandidate",
    "build_feeder_candidate_changes",
    "render_feeder_candidate_changes_md_section",
    "feeder_md_for_pair",
    "run_offline_feeder_match",
]


# ─── env helpers (локальные, чтобы не тянуть зависимости) ───────────────────

def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return default


def feeder_matching_enabled() -> bool:
    """Главный флаг (default OFF → прежнее поведение rich-render)."""
    return _env_bool("STAGE_COMPARISON_LARGE_SHEET_FEEDER_MATCHING_ENABLED", False)


def cfg_high_threshold() -> float:
    return _env_float("STAGE_COMPARISON_LARGE_SHEET_FEEDER_HIGH", 0.78)


def cfg_medium_threshold() -> float:
    return _env_float("STAGE_COMPARISON_LARGE_SHEET_FEEDER_MEDIUM", 0.55)


def cfg_low_threshold() -> float:
    return _env_float("STAGE_COMPARISON_LARGE_SHEET_FEEDER_LOW", 0.34)


def cfg_ambiguous_margin() -> float:
    return _env_float("STAGE_COMPARISON_LARGE_SHEET_FEEDER_AMBIG_MARGIN", 0.06)


def feeder_candidate_changes_enabled() -> bool:
    """Default OFF. При true в rich MD после таблицы сопоставления добавляется
    секция «Кандидаты пофидерных изменений» — предвычисленные per-feeder
    candidate changes с detected_delta + рекомендованным заголовком finding'а
    + prompt-сигналом для Opus. Поведение при false идентично прежнему."""
    return _env_bool("STAGE_COMPARISON_LARGE_SHEET_FEEDER_CANDIDATE_CHANGES_ENABLED", False)


def cfg_candidate_delta_threshold() -> float:
    """Относительный порог «значимого» изменения мощности/тока (default 0.08 =
    8%). Ниже — считаем разницу пересчётом/округлением, не инженерной дельтой."""
    return _env_float("STAGE_COMPARISON_LARGE_SHEET_FEEDER_DELTA_THRESHOLD", 0.08)


# ─── нормализация потребителя ───────────────────────────────────────────────

# Латинские OCR-варианты board-токенов → кириллица (домен-специфично, короткие
# инженерные коды). Применяется по словным границам.
_TRANSLIT = [
    ("gzsh", "гзш"),
    ("yasn", "ясн"),
    ("aukrm", "аукрм"),
    ("vru", "вру"),
    ("grsch", "грщ"),
    ("grsh", "грщ"),
    ("gvs", "гвс"),
    ("itp", "итп"),
    ("xm", "хм"),
    ("dr", "др"),
    ("eb", "эб"),
    ("bus", "шина"),
]

_PUNCT_RE = re.compile(r"[^0-9a-zа-яё]+")
_WS_RE = re.compile(r"\s+")


def _norm_text(s: object) -> str:
    """NFKC + ё→е + lower + транслит латиницы + пунктуация→пробел."""
    if not s:
        return ""
    txt = unicodedata.normalize("NFKC", str(s)).replace("ё", "е").replace("Ё", "е")
    txt = txt.lower()
    for lat, cyr in _TRANSLIT:
        txt = re.sub(rf"\b{lat}\b", cyr, txt)
        txt = re.sub(rf"\b{lat}(?=[\d\-])", cyr, txt)
    txt = _PUNCT_RE.sub(" ", txt)
    return _WS_RE.sub(" ", txt).strip()


# Шумовые токены: обозначения щитов/вводов, которые НЕ должны быть главным
# ключом потребителя (ВРУ переименованы в ГРЩ-РП).
_DESIG_NOISE = {
    "вру", "грщ", "рп", "щу", "шу", "що", "щр", "ввод", "корпус", "корп",
    "к", "из", "1гр", "2гр", "1грщ", "2грщ", "помещения", "помещений",
    "встроенные", "а", "ст",
}


# Правила классификации системы. ПОРЯДОК ВАЖЕН (специфичное раньше общего).
# Каждое правило: (system_key, regex по нормализованному тексту).
_SYSTEM_RULES: list[tuple[str, re.Pattern]] = [
    ("earthing", re.compile(r"гзш|ре\s*шин|заземлит|металлоконстр|металлоконструк|метал\w*\s+ввод|дсуп|радиосет|теплосет")),
    ("busbar", re.compile(r"\bшина\b|шина\s*\d")),
    ("tp_input", re.compile(r"тп\s*т\d|ввод\s*\d?\s*к\s*тп|к\s*тп\d|шинопровод")),
    ("tp_own_needs", re.compile(r"собствен\w*\s+нужд|\bясн\b|сн\s*тп")),
    ("aukrm", re.compile(r"аукрм|\bку\d|компенсац|конденсатор")),
    ("cooling_center", re.compile(r"холодильн\w*\s+центр|хладоцентр|\bхц\b|шухц|шу\s*хц|вру\s*хц|насос\w*\s+хладоцентр")),
    # cooler РАНЬШЕ chiller: «Охладитель ДР2-ХМ2» содержит и «охладит», и «хм2» —
    # ведущий признак «охладитель» (это драйкулер), а не ХМ-чиллер.
    ("cooler", re.compile(r"охладит|драйкулер|\bдр\d|\bдр\b")),
    ("chiller", re.compile(r"чиллер|холодильн\w*\s+машин|\bхм\d|\bхм\b")),
    ("itp", re.compile(r"\bитп\b|теплов\w*\s+пункт|вру\s*итп")),
    ("apt", re.compile(r"\bапт\b|\bапп\b|пожаротуш|щу\s*апт|шу\s*апт|вру\s*апт")),
    ("water_pump", re.compile(r"\bхвс\b|\bхпв\b|\bнст\b|водоснабж|водопровод|насосн|хозпит|\bхп\b|шу\s*хп|шу\s*хвс|вру\s*нст")),
    ("gvs", re.compile(r"\bгвс\b|резервн\w*\s+бак|электробак|эб\s*гвс|\bбаки\b")),
    ("parking", re.compile(r"автостоянк|паркинг|вруа|вру\s*а\b")),
    ("lighting", re.compile(r"наружн\w*\s+освещ|\bщно\b|\bшно\b|щит\s+наруж")),
    ("reserve", re.compile(r"\bрезерв\b")),
    ("vru_input", re.compile(r"вру\s*[-.]?\s*(\d)")),
]

_UNIT_RULES: dict[str, re.Pattern] = {
    "chiller": re.compile(r"хм\s*(\d)"),
    "cooler": re.compile(r"др\s*(\d)"),
    # NFKC превращает «№» в «No» → «АУКРМ №1» становится «аукрм no1»: ловим и no.
    "aukrm": re.compile(r"(?:аукрм|ку)\s*(?:№\s*|no\s*)?(\d)|(?:№|no)\s*(\d)"),
    "vru_input": re.compile(r"вру\s*[-.]?\s*(\d)"),
}

_SYSTEM_LABEL = {
    "earthing": "Заземление/ГЗШ",
    "busbar": "Шины ГРЩ",
    "tp_input": "Ввод от ТП (шинопровод)",
    "tp_own_needs": "Собственные нужды ТП",
    "aukrm": "Компенсация реактивной мощности (АУКРМ/КУ)",
    "cooling_center": "Холодильный центр (ХЦ)",
    "chiller": "Холодильные машины (ХМ/чиллер)",
    "cooler": "Охладители (ДР)",
    "itp": "Индивидуальный тепловой пункт (ИТП)",
    "apt": "Насосная АПТ",
    "water_pump": "Насосная ХВС/НСТ/ХП",
    "gvs": "Резервные баки ГВС",
    "parking": "ВРУ автостоянки (ВРУа)",
    "lighting": "Наружное освещение (ЩНО)",
    "reserve": "Резерв",
    "vru_input": "ВРУ корпуса (ввод)",
    "other": "Прочее",
}


@dataclass
class NormalizedConsumer:
    raw: str
    clean: str
    system_key: str
    unit: Optional[int]
    input_no: Optional[int]
    subtag: Optional[str]
    consumer_key: str
    consumer_tokens: tuple[str, ...]


def _extract_input_no(norm: str) -> Optional[int]:
    m = re.search(r"ввод\s*(\d)", norm)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _extract_unit(system_key: str, norm: str) -> Optional[int]:
    rule = _UNIT_RULES.get(system_key)
    if not rule:
        return None
    m = rule.search(norm)
    if not m:
        return None
    for g in m.groups():
        if g:
            try:
                return int(g)
            except ValueError:
                continue
    return None


def _consumer_tokens(norm: str) -> tuple[str, ...]:
    toks = [t for t in norm.split() if t and t not in _DESIG_NOISE and not t.isdigit()]
    return tuple(dict.fromkeys(toks))  # preserve order, dedup


def normalize_consumer(
    load_name: object, designation: object = "", extra: object = ""
) -> NormalizedConsumer:
    """Классификация потребителя по нагрузке (load_name) с fallback на
    обозначение (designation/id) когда нагрузка пустая.

    Главный ключ — система потребителя (``system_key`` + ``unit``), НЕ имя щита.
    """
    raw_name = str(load_name or "").strip()
    raw_desig = str(designation or "").strip()
    # имя для потребителя: нагрузка приоритетна; если пусто — обозначение
    primary = raw_name if raw_name else raw_desig
    norm = _norm_text(f"{primary} {extra}".strip())
    norm_full = _norm_text(f"{primary} {raw_desig} {extra}".strip())

    system_key = "other"
    for key, rgx in _SYSTEM_RULES:
        if rgx.search(norm) or rgx.search(norm_full):
            system_key = key
            break

    unit = _extract_unit(system_key, norm_full)
    input_no = _extract_input_no(norm_full)
    subtag = "встроенные" if re.search(r"встроен", norm_full) else None

    if system_key == "other":
        # ключ потребителя — значимые токены (без designation-шума)
        toks = _consumer_tokens(norm)
        consumer_key = "other:" + ("_".join(toks[:3]) if toks else (norm[:24] or "n_a"))
    else:
        parts = [system_key]
        if unit is not None:
            parts.append(str(unit))
        if subtag:
            parts.append(subtag)
        consumer_key = "_".join(parts)

    return NormalizedConsumer(
        raw=primary,
        clean=norm,
        system_key=system_key,
        unit=unit,
        input_no=input_no,
        subtag=subtag,
        consumer_key=consumer_key,
        consumer_tokens=_consumer_tokens(norm),
    )


# ─── извлечение фидеров ─────────────────────────────────────────────────────

@dataclass
class Feeder:
    idx: int
    designation: str          # id / breaker (имя щита/фидера — переименовано)
    load_name: str            # потребитель/нагрузка
    breaker: Optional[str]
    breaker_params: Optional[str]
    cable: Optional[str]
    power_kw: Optional[float]
    current_a: Optional[float]
    phase: Optional[str]
    raw_text: str
    nc: NormalizedConsumer


def _to_float(v: object) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", ".")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


_CABLE_SECTION_RE = re.compile(r"(\d+)\s*[х×x]\s*\(?\s*(\d+)\s*[х×x]\s*(\d+(?:[.,]\d+)?)|(\d+)\s*[х×x]\s*(\d+(?:[.,]\d+)?)")


def _cable_section(text: object) -> Optional[str]:
    """Нормализованная запись сечения кабеля: '5x185', '3x(5x120)' → 'NxM'."""
    if not text:
        return None
    s = _norm_text(text)
    m = _CABLE_SECTION_RE.search(s)
    if not m:
        return None
    if m.group(1):  # AxBxC → берём ядро BxC, множитель отдельно
        return f"{m.group(1)}x{m.group(2)}x{m.group(3)}"
    return f"{m.group(4)}x{m.group(5)}"


def extract_feeders(page_enriched: dict) -> list[Feeder]:
    """Список фидеров из ``page_enriched.json`` (circuits[])."""
    out: list[Feeder] = []
    circuits = (page_enriched or {}).get("circuits") or []
    for i, c in enumerate(circuits):
        if not isinstance(c, dict):
            continue
        designation = str(c.get("id") or c.get("breaker") or "").strip()
        load_name = str(c.get("load_name") or c.get("consumer") or "").strip()
        breaker_params = c.get("breaker_params")
        cable = c.get("cable") or breaker_params
        # Классификацию ведём ТОЛЬКО по нагрузке (+designation fallback). raw_text
        # шумный и «течёт» с соседних фидеров (АПТ/ХМ рядом), ломая system_key —
        # поэтому в extra его НЕ передаём (cable извлекается отдельно из f.cable).
        nc = normalize_consumer(load_name, designation)
        out.append(
            Feeder(
                idx=i,
                designation=designation,
                load_name=load_name,
                breaker=(str(c.get("breaker")).strip() if c.get("breaker") else None),
                breaker_params=(str(breaker_params).strip() if breaker_params else None),
                cable=(str(cable).strip() if cable else None),
                power_kw=_to_float(c.get("calculated_power_kw")),
                current_a=_to_float(c.get("calculated_current_a")),
                phase=(str(c.get("phase")).strip() if c.get("phase") else None),
                raw_text=str(c.get("raw_text") or "").strip(),
                nc=nc,
            )
        )
    return out


# ─── скоринг ────────────────────────────────────────────────────────────────

def _token_overlap(a: tuple[str, ...], b: tuple[str, ...]) -> float:
    if not a or not b:
        return 0.0
    sa, sb = set(a), set(b)
    inter = len(sa & sb)
    if not inter:
        return 0.0
    return inter / len(sa | sb)


def _units_compatible(o: Feeder, n: Feeder) -> bool:
    """Совместимость единиц внутри системы: равны, либо хотя бы одна None
    (None = укрупнённый/обобщённый потребитель — wildcard)."""
    uo, un = o.nc.unit, n.nc.unit
    if uo is None or un is None:
        return True
    return uo == un


def _value_sim(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    if a == 0 and b == 0:
        return 1.0
    hi = max(abs(a), abs(b))
    if hi == 0:
        return 1.0
    return max(0.0, 1.0 - abs(a - b) / hi)


def score_pair(o: Feeder, n: Feeder) -> tuple[float, dict]:
    """Взвешенный скор пары OLD↔NEW. Возвращает (score, components).

    Веса: система+единица (0.40) > потребитель-текст (0.30) > мощность/ток
    (0.18) > кабель (0.07) > обозначение (0.05). Обозначение щита — низкий вес
    (ВРУ переименованы в ГРЩ-РП). Кросс-система → не кандидат (score=0)."""
    comp: dict = {}
    same_system = o.nc.system_key == n.nc.system_key and o.nc.system_key != "other"
    if not same_system:
        # other↔other по токенам потребителя (низкий потолок)
        if o.nc.system_key == "other" and n.nc.system_key == "other":
            sim = _token_overlap(o.nc.consumer_tokens, n.nc.consumer_tokens)
            comp = {"system_unit": 0.0, "consumer": round(sim, 3), "cross": True}
            return round(sim * 0.45, 4), comp
        return 0.0, {"cross_system": True}

    if not _units_compatible(o, n):
        return 0.0, {"unit_mismatch": True, "old_unit": o.nc.unit, "new_unit": n.nc.unit}

    # система + единица
    if o.nc.unit is not None and n.nc.unit is not None and o.nc.unit == n.nc.unit:
        su = 1.0
    elif o.nc.unit is None and n.nc.unit is None:
        su = 0.9
    else:
        su = 0.78  # одна сторона укрупнённая (None) — wildcard
    if o.nc.subtag and n.nc.subtag and o.nc.subtag == n.nc.subtag:
        su = min(1.0, su + 0.05)

    consumer = _token_overlap(o.nc.consumer_tokens, n.nc.consumer_tokens)
    # Для ИЗВЕСТНОЙ системы с совместимой единицей потребитель уже опознан по
    # system_key — синонимы («хладоцентр» ↔ «холодильный центр») делят мало
    # буквальных токенов, поэтому ставим пол. На «other» пол НЕ действует
    # (там опора только на токены — это сохраняет precision для неизвестных).
    consumer = max(consumer, 0.7)
    power = _value_sim(o.power_kw, n.power_kw)
    current = _value_sim(o.current_a, n.current_a)
    # из двух (P,I) берём лучшее доступное как «электрическую близость»
    pi = None
    avail = [x for x in (power, current) if x is not None]
    if avail:
        pi = max(avail)
    cab = None
    co, cn = _cable_section(o.cable), _cable_section(n.cable)
    if co and cn:
        cab = 1.0 if co == cn else 0.25
    desig = _token_overlap(o.nc.consumer_tokens, n.nc.consumer_tokens)  # placeholder
    desig_sim = 1.0 if _norm_text(o.designation) == _norm_text(n.designation) and o.designation else 0.0

    weights = {"system_unit": 0.40, "consumer": 0.30, "pi": 0.18, "cable": 0.07, "desig": 0.05}
    vals = {"system_unit": su, "consumer": consumer, "pi": pi, "cable": cab, "desig": desig_sim}
    num = 0.0
    den = 0.0
    for k, w in weights.items():
        v = vals[k]
        if v is None:
            continue
        num += w * v
        den += w
    score = (num / den) if den else 0.0
    comp = {
        "system_unit": round(su, 3),
        "consumer": round(consumer, 3),
        "pi": (round(pi, 3) if pi is not None else None),
        "cable": (cab if cab is not None else None),
        "desig": desig_sim,
        "score": round(score, 4),
    }
    return round(score, 4), comp


# ─── матчинг ────────────────────────────────────────────────────────────────

@dataclass
class FeederPair:
    consumer_key: str
    system_label: str
    status: str
    score: float
    old: Optional[Feeder]
    new: Optional[Feeder]
    components: dict = field(default_factory=dict)
    suspected_change: str = ""
    second_best: Optional[float] = None


@dataclass
class FeederMatchResult:
    pairs: list[FeederPair]
    summary: dict


def _status_from_score(score: float, ambiguous: bool) -> str:
    if ambiguous and score >= cfg_medium_threshold():
        return "ambiguous"
    if score >= cfg_high_threshold():
        return "matched_high_confidence"
    if score >= cfg_medium_threshold():
        return "matched_medium_confidence"
    if score >= cfg_low_threshold():
        return "low_confidence"
    return "no_match"


def _fmt_pi(f: Feeder) -> str:
    parts = []
    if f.power_kw is not None:
        parts.append(f"{f.power_kw:g} кВт")
    if f.current_a is not None:
        parts.append(f"{f.current_a:g} А")
    return " / ".join(parts) if parts else "—"


def _suspected_change(o: Optional[Feeder], n: Optional[Feeder]) -> str:
    if o is None and n is not None:
        return "новый фидер / добавлен ввод"
    if n is None and o is not None:
        return "фидер отсутствует в новой стадии"
    if o is None or n is None:
        return ""
    notes: list[str] = []
    if _norm_text(o.designation) != _norm_text(n.designation) and o.designation and n.designation:
        notes.append(f"переименование {o.designation}→{n.designation}")
    ps = _value_sim(o.power_kw, n.power_kw)
    cs = _value_sim(o.current_a, n.current_a)
    if (ps is not None and ps < 0.92) or (cs is not None and cs < 0.92):
        notes.append(f"нагрузка {_fmt_pi(o)} → {_fmt_pi(n)}")
    co, cn = _cable_section(o.cable), _cable_section(n.cable)
    if co and cn and co != cn:
        notes.append(f"кабель {co}→{cn}")
    if n.nc.input_no == 2 or (n.nc.input_no and n.nc.input_no >= 2):
        notes.append("2-й ввод (РП2)")
    return "; ".join(notes) if notes else "без значимых изменений"


def match_feeders(old_feeders: list[Feeder], new_feeders: list[Feeder]) -> FeederMatchResult:
    """Сопоставление по потребителю. Группировка по system_key, внутри —
    greedy по убыванию скора с гейтом совместимости единиц."""
    # bucket by system_key
    buckets: dict[str, dict[str, list[Feeder]]] = {}
    for f in old_feeders:
        buckets.setdefault(f.nc.system_key, {"old": [], "new": []})["old"].append(f)
    for f in new_feeders:
        buckets.setdefault(f.nc.system_key, {"old": [], "new": []})["new"].append(f)

    pairs: list[FeederPair] = []
    counts = {
        "matched_high_confidence": 0,
        "matched_medium_confidence": 0,
        "low_confidence": 0,
        "ambiguous": 0,
        "old_only": 0,
        "new_only": 0,
    }

    for system_key, grp in buckets.items():
        olds = list(grp["old"])
        news = list(grp["new"])
        label = _SYSTEM_LABEL.get(system_key, system_key)
        consumer_key_default = system_key

        # все кандидатные скоры внутри bucket
        cand: list[tuple[float, int, int, dict]] = []
        for oi, o in enumerate(olds):
            for ni, n in enumerate(news):
                sc, comp = score_pair(o, n)
                if sc > 0:
                    cand.append((sc, oi, ni, comp))
        cand.sort(key=lambda t: t[0], reverse=True)

        # кандидаты на каждый old/new с consumer_key контрагента — для ambiguous
        # детекции. Ambiguous = риск спутать РАЗНЫХ потребителей, поэтому
        # конкурент с ТЕМ ЖЕ consumer_key (та же нагрузка, другой ввод/дубль
        # OLD) ambiguity НЕ создаёт.
        cands_by_old: dict[int, list[tuple[float, str]]] = {}
        cands_by_new: dict[int, list[tuple[float, str]]] = {}
        for sc, oi, ni, _ in cand:
            cands_by_old.setdefault(oi, []).append((sc, news[ni].nc.consumer_key))
            cands_by_new.setdefault(ni, []).append((sc, olds[oi].nc.consumer_key))

        used_old: set[int] = set()
        used_new: set[int] = set()
        for sc, oi, ni, comp in cand:
            if oi in used_old or ni in used_new:
                continue
            o, n = olds[oi], news[ni]
            chosen_new_key = n.nc.consumer_key
            chosen_old_key = o.nc.consumer_key
            alt_o = max([s for s, k in cands_by_old.get(oi, []) if k != chosen_new_key] + [0.0])
            alt_n = max([s for s, k in cands_by_new.get(ni, []) if k != chosen_old_key] + [0.0])
            second = max(alt_o, alt_n)
            ambiguous = (second > 0 and (sc - second) < cfg_ambiguous_margin()
                         and sc >= cfg_medium_threshold())
            status = _status_from_score(sc, ambiguous)
            if status == "no_match":
                continue
            used_old.add(oi)
            used_new.add(ni)
            ck = o.nc.consumer_key if o.nc.consumer_key != "other" else n.nc.consumer_key
            pairs.append(FeederPair(
                consumer_key=ck or consumer_key_default,
                system_label=label,
                status=status,
                score=sc,
                old=o,
                new=n,
                components=comp,
                suspected_change=_suspected_change(o, n),
                second_best=round(second, 4) if second else None,
            ))
            counts[status] = counts.get(status, 0) + 1

        # leftovers
        for oi, o in enumerate(olds):
            if oi in used_old:
                continue
            pairs.append(FeederPair(
                consumer_key=o.nc.consumer_key, system_label=label,
                status="old_only", score=0.0, old=o, new=None,
                suspected_change=_suspected_change(o, None)))
            counts["old_only"] += 1
        for ni, n in enumerate(news):
            if ni in used_new:
                continue
            pairs.append(FeederPair(
                consumer_key=n.nc.consumer_key, system_label=label,
                status="new_only", score=0.0, old=None, new=n,
                suspected_change=_suspected_change(None, n)))
            counts["new_only"] += 1

    # порядок: по consumer_key, внутри matched раньше singleton
    status_rank = {
        "matched_high_confidence": 0, "matched_medium_confidence": 1,
        "ambiguous": 2, "low_confidence": 3, "old_only": 4, "new_only": 5,
    }
    pairs.sort(key=lambda p: (p.consumer_key, status_rank.get(p.status, 9), -p.score))

    summary = {
        "old_circuits": len(old_feeders),
        "new_circuits": len(new_feeders),
        "matched_high": counts["matched_high_confidence"],
        "matched_medium": counts["matched_medium_confidence"],
        "low_confidence": counts["low_confidence"],
        "ambiguous": counts["ambiguous"],
        "old_only": counts["old_only"],
        "new_only": counts["new_only"],
        "consumer_keys_matched": sorted({
            p.consumer_key for p in pairs
            if p.status in ("matched_high_confidence", "matched_medium_confidence", "ambiguous")
        }),
    }
    return FeederMatchResult(pairs=pairs, summary=summary)


# ─── отчёты / рендер ────────────────────────────────────────────────────────

def _pair_row(p: FeederPair) -> dict:
    o, n = p.old, p.new
    return {
        "consumer_key": p.consumer_key,
        "system": p.system_label,
        "status": p.status,
        "confidence": round(p.score, 3),
        "old_feeder": (o.designation if o else None),
        "new_feeder": (n.designation if n else None),
        "old_consumer": (o.load_name or o.designation if o else None),
        "new_consumer": (n.load_name or n.designation if n else None),
        "old_breaker": (o.breaker_params or o.breaker if o else None),
        "new_breaker": (n.breaker_params or n.breaker if n else None),
        "old_cable": (o.cable if o else None),
        "new_cable": (n.cable if n else None),
        "old_power_current": (_fmt_pi(o) if o else None),
        "new_power_current": (_fmt_pi(n) if n else None),
        "suspected_change": p.suspected_change,
        "second_best": p.second_best,
        "components": p.components,
    }


def build_feeder_match_report(result: FeederMatchResult, *, meta: Optional[dict] = None) -> dict:
    rows = [_pair_row(p) for p in result.pairs]
    return {
        "schema_version": 1,
        "meta": meta or {},
        "summary": result.summary,
        "rows": rows,
    }


def _md_cell(v: object) -> str:
    if v is None or v == "":
        return "—"
    return str(v).replace("|", "/").replace("\n", " ").strip()


_STATUS_LABEL = {
    "matched_high_confidence": "✅ высокая",
    "matched_medium_confidence": "🟡 средняя",
    "ambiguous": "⚠️ неоднозначно",
    "low_confidence": "🔸 низкая",
    "old_only": "◀ только OLD",
    "new_only": "▶ только NEW",
}


def render_feeder_match_md_section(result: FeederMatchResult, *, top: int = 0) -> str:
    """MD-секция для встраивания в rich enriched MD (вход Opus)."""
    s = result.summary
    lines: list[str] = []
    lines.append("### Сопоставление фидеров по потребителю/нагрузке")
    lines.append("")
    lines.append(
        f"Фидеры сопоставлены по ПОТРЕБИТЕЛЮ/нагрузке (не по имени щита — "
        f"в новой стадии ВРУ переименованы в ГРЩ-РП). OLD цепей: {s['old_circuits']}, "
        f"NEW цепей: {s['new_circuits']}. Высокая: {s['matched_high']}, средняя: "
        f"{s['matched_medium']}, низкая: {s['low_confidence']}, неоднозначно: "
        f"{s['ambiguous']}, только OLD: {s['old_only']}, только NEW: {s['new_only']}."
    )
    lines.append("")
    lines.append(
        "| Потребитель | OLD фидер | NEW фидер | OLD нагрузка | NEW нагрузка | "
        "OLD P/I | NEW P/I | OLD кабель | NEW кабель | Уверенность | Изменение |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    pairs = result.pairs
    if top and top > 0:
        pairs = pairs[:top]
    for p in pairs:
        o, n = p.old, p.new
        lines.append(
            "| " + " | ".join([
                _md_cell(p.consumer_key),
                _md_cell(o.designation if o else None),
                _md_cell(n.designation if n else None),
                _md_cell((o.load_name or o.designation) if o else None),
                _md_cell((n.load_name or n.designation) if n else None),
                _md_cell(_fmt_pi(o) if o else None),
                _md_cell(_fmt_pi(n) if n else None),
                _md_cell(o.cable if o else None),
                _md_cell(n.cable if n else None),
                _STATUS_LABEL.get(p.status, p.status),
                _md_cell(p.suspected_change),
            ]) + " |"
        )
    lines.append("")
    return "\n".join(lines)


# ─── candidate changes (per-feeder, default OFF) ────────────────────────────

# Только потребительские фидеры. Системные ключи (заземление, шины, ввод от ТП,
# СН ТП, резерв) покрываются core/system findings (топология/шинопровод/ТТ/ГЗШ)
# и НЕ должны порождать пофидерные дубли.
_CANDIDATE_SYSTEMS = {
    "vru_input", "parking", "itp", "apt", "water_pump", "gvs",
    "chiller", "cooler", "cooling_center", "aukrm", "lighting",
}

_CABLE_PARALLEL_RE = re.compile(r"^\s*(\d+)\s*[хx×]\s*(\(|[а-яёa-z])", re.IGNORECASE)


def _cable_parallel(text: object) -> Optional[int]:
    """Число параллельных линий: '2x(5x120)'→2, '3хППГнг…'→3, '5х185'→1."""
    if not text:
        return None
    s = unicodedata.normalize("NFKC", str(text)).strip().lower()
    m = _CABLE_PARALLEL_RE.match(s)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return 1 if _cable_section(text) else None


def _breaker_rating(f: Feeder) -> Optional[str]:
    """Номинал автомата (ток, А) — только из явного breaker-поля. Поля с
    маркерами расчётной мощности/тока (Py/Рр/Ip) игнорируются, чтобы расчётный
    ток не выдавался за смену номинала автомата (анти-false-positive)."""
    for src in (f.breaker_params, f.breaker):
        if not src:
            continue
        s = _norm_text(src)
        if any(t in s for t in ("py", "рр", "ip", "iрасч", "iр")):
            continue
        m = re.search(r"(\d{2,4})\s*а\b", s)
        if m:
            return m.group(1)
    return None


def _detect_feeder_delta(o: Feeder, n: Feeder) -> list[str]:
    """Список инженерных дельт между matched OLD↔NEW фидерами (пусто = нет)."""
    thr = cfg_candidate_delta_threshold()
    deltas: list[str] = []
    ps = _value_sim(o.power_kw, n.power_kw)
    if ps is not None and ps < (1.0 - thr):
        deltas.append("power_changed")
    cs = _value_sim(o.current_a, n.current_a)
    if cs is not None and cs < (1.0 - thr):
        deltas.append("current_changed")
    co, cn = _cable_section(o.cable), _cable_section(n.cable)
    if co and cn and co != cn:
        deltas.append("cable_changed")
    po, pn = _cable_parallel(o.cable), _cable_parallel(n.cable)
    if po and pn and po != pn:
        deltas.append("parallel_lines_changed")
    bo, bn = _breaker_rating(o), _breaker_rating(n)
    if bo and bn and bo != bn:
        deltas.append("breaker_changed")
    if o.nc.system_key != n.nc.system_key and "other" not in (o.nc.system_key, n.nc.system_key):
        deltas.append("consumer_type_changed")
    return deltas


@dataclass
class FeederCandidate:
    consumer_key: str
    system_label: str
    kind: str                 # matched | ambiguous | feeder_added | feeder_removed
    confidence: float
    old_feeder: Optional[str]
    new_feeder: Optional[str]
    old_consumer: Optional[str]
    new_consumer: Optional[str]
    old_power_current: Optional[str]
    new_power_current: Optional[str]
    old_cable: Optional[str]
    new_cable: Optional[str]
    old_breaker: Optional[str]
    new_breaker: Optional[str]
    detected_delta: list[str]
    recommended_finding_title: str
    requires_human_review: bool
    reason: str


def _consumer_label(p: FeederPair) -> str:
    o, n = p.old, p.new
    for f in (n, o):
        if f and (f.load_name or "").strip():
            return f.load_name.strip()
    return _SYSTEM_LABEL.get((n or o).nc.system_key, p.consumer_key)


def _recommend_title(p: FeederPair, deltas: list[str]) -> str:
    o, n = p.old, p.new
    name = _consumer_label(p)
    if "feeder_added" in deltas:
        return f"Добавлен фидер потребителя «{name}» ({n.designation if n else '—'})"
    if "feeder_removed" in deltas:
        return f"Удалён фидер потребителя «{name}» ({o.designation if o else '—'})"
    bits: list[str] = []
    if "power_changed" in deltas or "current_changed" in deltas:
        bits.append(f"нагрузка {_fmt_pi(o)} → {_fmt_pi(n)}")
    if "cable_changed" in deltas:
        bits.append(f"сечение {_cable_section(o.cable)}→{_cable_section(n.cable)}")
    if "parallel_lines_changed" in deltas:
        bits.append(f"парал. линий {_cable_parallel(o.cable)}→{_cable_parallel(n.cable)}")
    if "breaker_changed" in deltas:
        bits.append(f"номинал автомата {_breaker_rating(o)}→{_breaker_rating(n)} А")
    if "consumer_type_changed" in deltas:
        bits.append("тип потребителя изменён")
    head = f"Фидер «{name}» ({o.designation if o else '—'}→{n.designation if n else '—'})"
    return head + ": " + "; ".join(bits) if bits else head


def _candidate_reason(p: FeederPair, deltas: list[str]) -> str:
    base = {
        "matched_high_confidence": "высокая уверенность сопоставления",
        "matched_medium_confidence": "средняя уверенность сопоставления",
        "ambiguous": "неоднозначное сопоставление (несколько правдоподобных OLD)",
        "old_only": "фидер без пары на новой стадии",
        "new_only": "новый фидер без пары на старой стадии",
    }.get(p.status, p.status)
    return f"{base}; score={p.score:.2f}; дельты: {', '.join(deltas) or 'нет'}"


def _make_candidate(p: FeederPair, deltas: list[str], *, requires_human_review: bool, kind: str) -> FeederCandidate:
    o, n = p.old, p.new
    sys_key = (n or o).nc.system_key
    return FeederCandidate(
        consumer_key=p.consumer_key,
        system_label=_SYSTEM_LABEL.get(sys_key, sys_key),
        kind=kind,
        confidence=round(p.score, 3),
        old_feeder=(o.designation if o else None),
        new_feeder=(n.designation if n else None),
        old_consumer=((o.load_name or o.designation) if o else None),
        new_consumer=((n.load_name or n.designation) if n else None),
        old_power_current=(_fmt_pi(o) if o else None),
        new_power_current=(_fmt_pi(n) if n else None),
        old_cable=(o.cable if o else None),
        new_cable=(n.cable if n else None),
        old_breaker=((o.breaker_params or o.breaker) if o else None),
        new_breaker=((n.breaker_params or n.breaker) if n else None),
        detected_delta=deltas,
        recommended_finding_title=_recommend_title(p, deltas),
        requires_human_review=requires_human_review,
        reason=_candidate_reason(p, deltas),
    )


def build_feeder_candidate_changes(result: FeederMatchResult) -> list[FeederCandidate]:
    """Предвычисленные per-feeder candidate changes из результата сопоставления.

    Правила: high/medium с инженерной дельтой → твёрдый кандидат; ambiguous с
    дельтой → requires_human_review; old_only/new_only → feeder_removed/added,
    но ТОЛЬКО если consumer_key не покрыт matched-парой (не дубль укрупнённого
    представления OLD) и система — потребительская. Без дельты — не кандидат."""
    matched_keys = {
        p.consumer_key for p in result.pairs
        if p.status in ("matched_high_confidence", "matched_medium_confidence", "ambiguous")
    }
    out: list[FeederCandidate] = []
    seen_addremove: set[str] = set()
    for p in result.pairs:
        sys_key = (p.new or p.old).nc.system_key if (p.new or p.old) else "other"
        if sys_key not in _CANDIDATE_SYSTEMS:
            continue  # системные ключи покрыты core findings
        if p.status in ("matched_high_confidence", "matched_medium_confidence", "ambiguous"):
            deltas = _detect_feeder_delta(p.old, p.new)
            if not deltas:
                continue  # rule 3: нет инженерной дельты → нет кандидата
            out.append(_make_candidate(
                p, deltas,
                requires_human_review=(p.status == "ambiguous"),
                kind=("ambiguous" if p.status == "ambiguous" else "matched")))
        elif p.status in ("old_only", "new_only"):
            if p.consumer_key in matched_keys:
                continue  # rule 5: дубль укрупнённого представления потребителя
            if p.consumer_key in seen_addremove:
                continue  # один add/remove на consumer_key (не плодим дубли OLD-реп.)
            seen_addremove.add(p.consumer_key)
            delta = ["feeder_removed"] if p.status == "old_only" else ["feeder_added"]
            # add/remove из сопоставления — гипотеза (unit-None wildcard может
            # ошибиться), поэтому ВСЕГДА requires_human_review, не firm finding.
            out.append(_make_candidate(p, delta, requires_human_review=True, kind=p.status))
        # low_confidence → не кандидат
    return out


_FEEDER_CANDIDATE_PROMPT_SIGNAL = (
    "<!-- FEEDER_CANDIDATE_CHANGES -->\n"
    "Каждая строка ниже — ПРЕДВЫЧИСЛЕННЫЙ кандидат отдельного изменения по "
    "конкретному потребителю/фидеру (сопоставление по нагрузке, НЕ по имени щита). "
    "Инструкция для сравнения:\n"
    "- рассматривай КАЖДУЮ строку high/medium как потенциальное ОТДЕЛЬНОЕ изменение;\n"
    "- НЕ сворачивай числовые изменения разных matched-фидеров в один общий "
    "design_logic;\n"
    "- выпусти ОТДЕЛЬНЫЙ finding для каждого high/medium кандидата с инженерной "
    "дельтой (мощность/ток/кабель/номинал/параллельные линии/добавление/удаление "
    "фидера), процитировав ОБА значения OLD и NEW;\n"
    "- кандидаты из раздела «Неоднозначные» выпускай только как "
    "requires_human_review (или объедини в одну осторожную карточку), не как "
    "high-confidence;\n"
    "- НЕ дублируй уже выпущенные core/system findings (топология ГРЩ, шинопровод "
    "от ТП, учёт/ТТ, заземление/ДСУП) — это системные изменения, не пофидерные."
)

_CAND_HEADER = (
    "| consumer_key | old_feeder | new_feeder | confidence | old P/I | new P/I | "
    "old cable | new cable | old breaker | new breaker | detected_delta | "
    "recommended_finding_title |"
)
_CAND_SEP = "|---|---|---|---|---|---|---|---|---|---|---|---|"


def _cand_row(c: FeederCandidate) -> str:
    return "| " + " | ".join([
        _md_cell(c.consumer_key), _md_cell(c.old_feeder), _md_cell(c.new_feeder),
        f"{c.confidence:.2f}", _md_cell(c.old_power_current), _md_cell(c.new_power_current),
        _md_cell(c.old_cable), _md_cell(c.new_cable), _md_cell(c.old_breaker),
        _md_cell(c.new_breaker), _md_cell(", ".join(c.detected_delta)),
        _md_cell(c.recommended_finding_title),
    ]) + " |"


def render_feeder_candidate_changes_md_section(result: FeederMatchResult) -> str:
    """MD-секция «Кандидаты пофидерных изменений» (+ prompt-сигнал для Opus).
    Пусто, если кандидатов нет."""
    cands = build_feeder_candidate_changes(result)
    if not cands:
        return ""
    firm = [c for c in cands if not c.requires_human_review]
    review = [c for c in cands if c.requires_human_review]
    lines: list[str] = []
    lines.append("### Кандидаты пофидерных изменений из таблицы сопоставления")
    lines.append("")
    lines.append(_FEEDER_CANDIDATE_PROMPT_SIGNAL)
    lines.append("")
    lines.append(
        f"Твёрдых кандидатов (high/medium с инженерной дельтой): **{len(firm)}**; "
        f"неоднозначных (requires_human_review): **{len(review)}**."
    )
    lines.append("")
    lines.append(_CAND_HEADER)
    lines.append(_CAND_SEP)
    for c in firm:
        lines.append(_cand_row(c))
    lines.append("")
    if review:
        lines.append("#### Неоднозначные кандидаты (requires_human_review)")
        lines.append("")
        lines.append(_CAND_HEADER)
        lines.append(_CAND_SEP)
        for c in review:
            lines.append(_cand_row(c))
        lines.append("")
    return "\n".join(lines)


def feeder_md_for_pair(
    old_page_enriched: dict, new_page_enriched: dict, *, include_candidates: bool = False
) -> str:
    """Сборка feeder-секций rich MD за один match: таблица сопоставления + (опц.)
    секция кандидатов пофидерных изменений. "" если у стороны нет цепей."""
    olds = extract_feeders(old_page_enriched or {})
    news = extract_feeders(new_page_enriched or {})
    if not olds or not news:
        return ""
    result = match_feeders(olds, news)
    parts = [render_feeder_match_md_section(result)]
    if include_candidates:
        cand_md = render_feeder_candidate_changes_md_section(result)
        if cand_md:
            parts.append(cand_md)
    return "\n\n".join(parts)


def feeder_section_for_pair(old_page_enriched: dict, new_page_enriched: dict) -> str:
    """MD-секция сопоставления фидеров для встраивания в rich enriched MD.

    ``old`` = старая стадия (left), ``new`` = новая (right). Возвращает ""
    (никакой секции), если у любой стороны нет цепей — тогда rich MD остаётся
    без секции (fail-soft). Сеть/Qwen/Opus не задействуются."""
    olds = extract_feeders(old_page_enriched or {})
    news = extract_feeders(new_page_enriched or {})
    if not olds or not news:
        return ""
    result = match_feeders(olds, news)
    return render_feeder_match_md_section(result)


def run_offline_feeder_match(
    old_page_enriched: dict, new_page_enriched: dict, *, meta: Optional[dict] = None
) -> tuple[FeederMatchResult, dict, str]:
    """Полный offline-проход: extract → match → (report dict, md section)."""
    olds = extract_feeders(old_page_enriched)
    news = extract_feeders(new_page_enriched)
    result = match_feeders(olds, news)
    report = build_feeder_match_report(result, meta=meta)
    md = render_feeder_match_md_section(result)
    return result, report, md
