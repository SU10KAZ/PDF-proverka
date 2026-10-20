"""Сопоставление замечаний о нормативных ссылках для эксперимента по docnorm.

Одного обозначения нормы мало: одна и та же норма фигурирует в разных проблемах
(устаревшая редакция в ведомости, неверное применение в спецификации,
противоречие редакций в примечании). Поэтому пара считается одной проблемой,
только если совпадают все четыре признака:

  1. норма   — пересечение номеров норм (без года редакции: ГОСТ 6465-76 и
               ГОСТ 6465-2023 — одна норма в разных редакциях);
  2. суть    — основной тип нарушения (`essence`);
  3. предмет — пересечение изделий/материалов/видов работ (`subject`), обязателен;
  4. место   — пересечение мест в документе (`location`), если определено у обеих.

Предмет обязателен: если он не определяется у одной из сторон — связь «слабая»
и в метрики не идёт. Место сравнивается, только если определено у обеих
сторон (в архиве оно не определяется у половины замечаний). Дословно
совпадающий текст — одна проблема. Если кандидатов больше одного — «неоднозначно». Если норма
совпала, а суть другая или не определена — «неопределённо». Ложная связь хуже
пропущенной: всё неуверенное остаётся неопределённым и уходит на ручную проверку.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

from backend.app.pipeline.stages.prepare.codex_targeted_findings import (
    _NORM_CODE_RE,
    _norm_signature,
)

RULES_VERSION = "docnorm_match_v2"

STRONG = "strong"
WEAK = "weak"
AMBIGUOUS = "ambiguous"
UNDETERMINED = "undetermined"
UNMATCHED = "unmatched"

_EXTRA_NORM_RE = re.compile(
    r"(?:№\s*)?\d{2,3}\s*[-–]\s*Ф[ЗЭ]\b|\bФЗ\s*[-–№]?\s*\d{2,3}\b"
    r"|\b(?:ВСН|ППБ|СТО|ТР\s*ТС|ПУЭ)[\s-]*\d*(?:[.\-–]\d+)*",
    re.IGNORECASE,
)

# Порядок = приоритет основного типа.
ESSENCE_RULES: list[tuple[str, str]] = [
    # «статус/замена не подтверждены, требует проверки» — отдельная суть, а не «устарела»
    ("unconfirmed", r"не\s+подтвержд|требует\s+(проверки|уточнени|подтвержд|согласовани)|требуется\s+(проверить|уточнить)"),
    ("missing", r"отсутству\w*\s+(ссылк|норм|документ|базов)|нет\s+ссылки\s+на|не\s+указан\w*\s+(норм|сп\b|гост)"
                r"|неполн\w*\s+[^.]{0,40}(ведомост|ссылок|норм)|ведомост\w*\s+[^.]{0,60}неполн|не\s+включен\w*\s+част\w*\s+норм"),
    ("doc_identity", r"латиниц\w*\s+и\s+кириллиц|шифр|идентификатор\w*\s+(объект|част)|титульн\w+\s+данн|основани\w+\s+разработки"),
    ("conflict", r"разн\w+\s+редакци|противоречив\w+\s+(редакци|стандарт)|одновременно\s+(указан|использован|приведен|с\s+действующ)"
                 r"|при\s+действующ|несогласован\w+\s+(редакци|норматив)"),
    ("duplicate", r"дважды|дублир|две\s+одинаковые|повторен\w*\s+строк"),
    ("wrong_application", r"не\s+распространяется|некорректн\w+\s+применени|стандарт\w*\s+на\s+[^.]{0,60}(вместо|а\s+не)"
                          r"|по\s+стандарту\s+на\s+(кабел|друг)|регламентирует\s+[^.]{0,80}\s+(а\s+не|и\s+не)|не\s+относится\s+к\s+пожарн"
                          r"|указан\w*\s+как\s+стандарт\s+на|включен\w*\s+рекомендаци\w*\s+по"),
    ("title", r"(неверн|ошибочн|некорректн|неточн)\w*\s+(указан\w*\s+|приведен\w*\s+|записан\w*\s+)?(наименовани|названи)"
              r"|в\s+(названии|наименовании)\s+[^.]{0,60}(ошибочно|вместо|неверно|опечатк)|ошибочно\s+назван|опечатк\w*\s+в\s+названи"
              r"|(наименовани|названи)\w*\s+[^.]{0,60}не\s+(соответству|согласован)|разн\w+\s+названи|конфликт\s+обозначени\w*\s+и\s+наименовани"),
    ("designation", r"несуществующ|не\s+существует|(ошибочн|неверн|некорректн)\w*\s+(указан\w*\s+|записан\w*\s+|оформлен\w*\s+)?(обозначени|номер|индекс|ссылк|вид\w*\s+документ|дат)"
                    r"|опечатк\w*\s+(в\s+)?(обозначени|номер|индекс|нормативн)|искаж|вместо\s+[«\"]?(№|сп\b|гост|фз)"
                    r"|пропущен\w*\s+(буква|индекс)|сокращ\w+\s+год|ошибк\w+\s+(обозначени|записи)|ошибочно\s+указан\s+вид"),
    ("changes", r"перечень\s+изменений|перечн\w+\s+изменений|с\s+изменениями|изменени\w*\s+№"),
    ("not_profile", r"непрофильн|не\s+профильн|нерелевантн|не\s+относится\s+к\s+(раздел|дисциплин)"),
    ("status", r"отмен[её]н|отменн|замен[её]н|заменённ|устарев|утратив|утратил|недействующ|неактуальн|не\s+действует|прекращ"),
]
_ESSENCE_RX = [(name, re.compile(rx, re.IGNORECASE)) for name, rx in ESSENCE_RULES]

LOCATION_RULES: list[tuple[str, str]] = [
    ("reference_list", r"ведомост\w*\s+(ссылочн|прилагаем)|перечн\w*\s+(нормативн|действующ|ссылочн)|нормативн\w+\s+баз|ссылочн\w+\s+документ"),
    ("general_notes", r"общ\w+\s+указани|общих\s+данн|пояснительн|основн\w+\s+положени"),
    ("specification", r"спецификац|ведомост\w*\s+(материал|расход|отделк|решет|изделий)|граф\w*\s+[«\"]?(обозначени|тип)"),
    ("drawing_note", r"примечани|\bузл|узле|выноск|схем"),
]
_LOCATION_RX = [(name, re.compile(rx, re.IGNORECASE)) for name, rx in LOCATION_RULES]

SUBJECT_STEMS = (
    "труб", "короб", "кабел", "провод", "вру", "щит", "эмал", "краск", "окраск", "антикорроз",
    "раствор", "кирпич", "газобетон", "блок", "утеплит", "теплоизол", "плитк", "плит", "уголк",
    "профил", "решетк", "решётк", "лестниц", "двер", "окон", "окн", "витраж", "гидроизол", "кровл",
    "стал", "свар", "электрод", "заземл", "маркировк", "огнестойк", "пожарн", "мгн", "доступност",
    "перемычк", "сетк", "бетон", "арматур", "стекл", "мембран", "пенополистир", "минерал", "гипс",
    "перегородк", "облицовк", "фасад", "водосток", "люк", "огражд", "освещени", "молни", "спдс",
    "чертеж", "чертёж", "документаци", "устройств", "отделк", "пол", "потол",
)
_SUBJECT_RX = re.compile(r"\b(" + "|".join(sorted(SUBJECT_STEMS, key=len, reverse=True)) + r")\w*", re.IGNORECASE)
_CORE_RX = re.compile(r"(\d+(?:[._]\d+)*)")


def _text(item: dict) -> str:
    return f"{item.get('problem') or item.get('title') or ''} {item.get('description') or ''}"


def norm_cores(text: str) -> set[str]:
    """Номера норм без года редакции: «ГОСТ Р 6465-76» → «6465», «СП 10.13330.2017» → «10.13330»."""
    out: set[str] = set()
    for rx in (_NORM_CODE_RE, _EXTRA_NORM_RE):
        for m in rx.finditer(text or ""):
            raw = m.group(0)
            sig = _norm_signature(raw)
            if not sig:
                continue
            num = _CORE_RX.search(raw.replace(",", "."))
            if not num:
                out.add(sig)
                continue
            core = num.group(1).replace("_", ".")
            parts = core.split(".")
            # «13330.2017», «1311500.2021» — последний сегмент-год срезаем
            if len(parts) >= 3 and len(parts[-1]) == 4:
                core = ".".join(parts[:-1])
            kind = "ФЗ" if re.search(r"Ф[ЗЭ]", raw, re.IGNORECASE) else re.sub(r"[^A-ZА-Я]", "", raw.upper())[:4]
            out.add(f"{kind}:{core}")
    return out


def essence(text: str) -> str:
    for name, rx in _ESSENCE_RX:
        if rx.search(text or ""):
            return name
    return "other"


def locations(text: str) -> set[str]:
    return {name for name, rx in _LOCATION_RX if rx.search(text or "")}


def subjects(text: str) -> set[str]:
    return {m.group(1).lower()[:6] for m in _SUBJECT_RX.finditer(text or "")}


def features(item: dict) -> dict[str, Any]:
    """Норма и суть — из заголовка (`problem`), описание только как запасной источник:
    описания часто цитируют строку MD с несколькими нормами и чужими формулировками."""
    text = _text(item)
    head = str(item.get("problem") or item.get("title") or "")
    head_essence = essence(head)
    return {
        "norms": sorted(norm_cores(head) or norm_cores(text)),
        "essence": head_essence if head_essence != "other" else essence(text),
        "locations": sorted(locations(text)),
        "subjects": sorted(subjects(text)),
    }


def _norm_text(item: dict) -> str:
    return re.sub(r"\s+", " ", _text(item)).strip().lower()


def compare(a: dict, b: dict) -> str:
    """Уровень связи двух замечаний по их признакам (без учёта других кандидатов).

    v2: предмет обязателен (не определён у одной из сторон → weak); место
    сравнивается, только если определено у обеих; дословно совпадающий текст —
    одна проблема (предмет и суть совпадают тривиально).
    """
    fa, fb = features(a), features(b)
    if not set(fa["norms"]) & set(fb["norms"]):
        return UNMATCHED
    if _norm_text(a) and _norm_text(a) == _norm_text(b):
        return STRONG
    if fa["essence"] == "other" or fb["essence"] == "other" or fa["essence"] != fb["essence"]:
        return UNDETERMINED
    la, lb = set(fa["locations"]), set(fb["locations"])
    if la and lb and not la & lb:
        return UNDETERMINED
    sa, sb = set(fa["subjects"]), set(fb["subjects"])
    if not (sa and sb):
        return WEAK
    return STRONG if sa & sb else UNDETERMINED


def match(left: Iterable[dict], right: Iterable[dict]) -> dict[str, Any]:
    """Сопоставление «один к одному» внутри одной версии документа.

    Returns: {"pairs": [{"left","right","level"}], "left": {id: status}, "right": {id: status}}
    где status ∈ strong | weak | ambiguous | undetermined | unmatched.
    """
    L = {str(x["id"]): x for x in left if isinstance(x, dict) and x.get("id")}
    R = {str(x["id"]): x for x in right if isinstance(x, dict) and x.get("id")}
    levels = {(li, ri): compare(L[li], R[ri]) for li in L for ri in R}
    cand = {li: [ri for ri in R if levels[(li, ri)] in (STRONG, WEAK)] for li in L}
    rcand = {ri: [li for li in L if levels[(li, ri)] in (STRONG, WEAK)] for ri in R}

    pairs, lstat, rstat = [], {}, {}
    for li, rs in cand.items():
        if len(rs) == 1 and len(rcand[rs[0]]) == 1:
            ri = rs[0]
            level = levels[(li, ri)]
            pairs.append({"left": li, "right": ri, "level": level})
            lstat[li] = level
            rstat[ri] = level
    for li in L:
        if li in lstat:
            continue
        if len(cand[li]) > 1 or any(len(rcand[ri]) > 1 for ri in cand[li]):
            lstat[li] = AMBIGUOUS
        elif any(levels[(li, ri)] == UNDETERMINED for ri in R):
            lstat[li] = UNDETERMINED
        else:
            lstat[li] = UNMATCHED
    for ri in R:
        if ri in rstat:
            continue
        if len(rcand[ri]) > 1:
            rstat[ri] = AMBIGUOUS
        elif any(levels[(li, ri)] == UNDETERMINED for li in L):
            rstat[ri] = UNDETERMINED
        else:
            rstat[ri] = UNMATCHED
    return {"rules": RULES_VERSION, "pairs": pairs, "left": lstat, "right": rstat}
