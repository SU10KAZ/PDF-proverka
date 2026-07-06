"""«Умный страж отсутствия» (Stage 01 post-pass).

Исследование браков (03.07) показало: крупный класс ложных замечаний — «данные ЕСТЬ, ИИ
не увидел» (написал «не указано/отсутствует», а оно на другом листе/в тексте). A/B на
АР1.1-К7 вскрыл, что грубое правило «нет absence_checked → понизить» бьёт по ВЕРНЫМ
замечаниям об отсутствии (их 7 из 11, включая критическое). Поэтому логика двухтактная:

1. Детектор `_is_absence_claim` — дешёвый recall-пре-фильтр «похоже на утверждение об
   отсутствии». Его точность вторична: он лишь отбирает кандидатов на проверку.
2. Подтверждение присутствия — по ПОЛНОМУ тексту документа проверяем, есть ли заявленный
   как отсутствующий элемент где-либо ещё. Понижаем до «ПРОВЕРИТЬ ПО СМЕЖНЫМ» ТОЛЬКО
   подтверждённо-ложные (verdict=present). Верные отсутствия и не-absence — не трогаем.

Безопасный инвариант: без верификатора (или без текста документа) НЕ понижаем ничего —
лучше пропустить ложное, чем зарубить верное критическое. Ничего не удаляется («не reject»).
Работает только при PIPELINE_ABSENCE_GUARD_ENABLED. Fail-soft.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Optional

_VERIFY_SEVERITY = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"

# Маркеры замечания-об-отсутствии (утверждение, что чего-то НЕТ). Намеренно НЕ ловим
# «указано неверно», «не соответствует» и т.п. — это претензии к значению, не к отсутствию.
# Точность здесь не критична: детектор — пре-фильтр, финальное решение за подтверждением.
_ABSENCE_PATTERNS = [
    r"не\s+указ\w+",          # не указан/указано/указана/указаны
    r"отсутств\w+",           # отсутствует/отсутствуют/отсутствие
    r"не\s+привед\w+",        # не приведён/приведена/приведено
    r"не\s+показ\w+",         # не показан/показана/показано
    r"не\s+обознач\w+",       # не обозначен/обозначена
    r"не\s+задан\w*",         # не задан/задана/задано
    r"не\s+определ\w+",       # не определён/определена
    r"не\s+предусмотр\w+",    # не предусмотрен/предусмотрена
    r"не\s+прораб\w+",        # не проработан
    r"не\s+детализир\w*",     # не детализировано
    r"нет\s+(?:данн\w+|сведен\w+|информац\w+)",  # нет данных/сведений/информации
    r"не\s+хват\w+",          # не хватает
    r"недоста(?:ёт|ет)\w*",   # недостаёт (НЕ «недостаточно» — претензия к величине)
    # «пропущ…» намеренно НЕ ловим: «пропущена позиция» = отсутствие, но
    # «пропущенная цифра/буква» = опечатка (претензия к значению). Двусмысленно → пропуск.
]
_ABSENCE_RE = re.compile("|".join(_ABSENCE_PATTERNS), re.IGNORECASE)

# verifier: (md_text, candidates) -> {индекс_в_candidates: "present"|"absent"|"not_absence"}
Verifier = Callable[[str, list[dict]], dict]


def _is_absence_claim(finding: dict) -> bool:
    """Замечание похоже на утверждение об отсутствии (пре-фильтр)?"""
    text = " ".join(
        str(finding.get(k) or "")
        for k in ("finding", "problem", "description", "category")
    )
    return bool(_ABSENCE_RE.search(text))


def _has_absence_evidence(finding: dict) -> bool:
    """Модель уже указала конкретные проверенные места (`absence_checked` непуст)?"""
    checked = finding.get("absence_checked")
    if isinstance(checked, list):
        return any(str(x).strip() for x in checked)
    if isinstance(checked, str):
        return bool(checked.strip())
    return False


def enforce_absence_guard(
    findings: list[Any],
    *,
    md_text: Optional[str] = None,
    verifier: Optional[Verifier] = None,
) -> dict:
    """Понизить ПОДТВЕРЖДЁННО-ложные замечания-об-отсутствии до «ПРОВЕРИТЬ ПО СМЕЖНЫМ».

    Мутирует записи на месте. Ничего не удаляет (инвариант «не reject»).

    Кандидаты — замечания, которые (а) похожи на утверждение об отсутствии,
    (б) без непустого `absence_checked`, (в) ещё не «ПРОВЕРИТЬ ПО СМЕЖНЫМ».
    Понижаются ТОЛЬКО те, по которым верификатор подтвердил present (данные есть в
    документе). Без верификатора/текста — безопасный режим: не понижаем ничего.
    """
    scanned = 0
    absence_claims = 0
    candidates: list[tuple[int, dict]] = []

    for f in findings:
        if not isinstance(f, dict):
            continue
        scanned += 1
        if not _is_absence_claim(f):
            continue
        absence_claims += 1
        if str(f.get("severity") or "").strip() == _VERIFY_SEVERITY:
            continue  # уже мягкое
        if _has_absence_evidence(f):
            continue  # модель проверила и назвала места — доверяем
        candidates.append((len(candidates), f))  # локальный индекс → finding

    downgraded = 0
    verified = False
    if candidates and verifier is not None and md_text:
        try:
            verdicts = verifier(md_text, [f for _, f in candidates])
            verified = True
        except Exception:  # noqa: BLE001 — fail-soft: не удалось проверить → не трогаем
            verdicts = {}
            verified = False
        for idx, f in candidates:
            if verdicts.get(idx) == "present":  # подтверждённо-ложное отсутствие
                f["absence_guard_downgraded"] = True
                f["absence_guard_original_severity"] = str(f.get("severity") or "")
                f["absence_guard_evidence"] = str(
                    (verdicts.get(f"{idx}_evidence") if isinstance(verdicts, dict) else "") or ""
                )
                f["severity"] = _VERIFY_SEVERITY
                downgraded += 1

    return {
        "scanned": scanned,
        "absence_claims": absence_claims,
        "candidates": len(candidates),
        "verified": verified,
        "downgraded": downgraded,
    }


# ── Верификатор на claude -p (подтверждение присутствия по полному документу) ──

def build_verification_prompt(md_text: str, candidates: list[dict]) -> str:
    """Промпт: по полному MD решить для каждого «нет», есть ли элемент в документе."""
    flist = "\n".join(
        f"{i}) {str(f.get('finding') or '')[:400]}" for i, f in enumerate(candidates)
    )
    return (
        "Ты проверяешь замечания ИИ-аудита проектной документации. Ниже ПОЛНЫЙ текст "
        "документа (MD), затем список замечаний, каждое утверждает, что чего-то НЕТ / не "
        "указано / отсутствует.\n\n"
        "Для КАЖДОГО замечания сначала выдели КОНКРЕТНЫЙ проверяемый объект (нормативная "
        "ссылка, расчёт, параметр, оборудование, поле таблицы), затем найди его по ВСЕМУ "
        "документу (прямые упоминания и синонимы) и реши:\n"
        "- \"present\" — заявленный элемент/данные ФАКТИЧЕСКИ ЕСТЬ в документе (в другом "
        "разделе/листе/приложении/таблице) → замечание ЛОЖНОЕ.\n"
        "- \"absent\" — именно этого элемента нет во всём документе. ВАЖНО: похожая или "
        "ЧАСТИЧНАЯ информация НЕ считается подтверждением — если есть близкое, но не та "
        "деталь, ставь absent и в evidence пиши «есть X, но нет Y».\n"
        "- \"not_absence\" — замечание на самом деле НЕ об отсутствии, а о противоречии между "
        "разделами / неверном значении / ошибочном обозначении / дубликате.\n\n"
        "Не оценивай, правы ли нормы и требования сами по себе — проверяй ТОЛЬКО наличие/"
        "отсутствие в данном MD. evidence — одно короткое предложение по-русски.\n\n"
        "Верни ТОЛЬКО JSON-объект вида "
        "{\"verdicts\":[{\"i\":N,\"verdict\":\"present|absent|not_absence\",\"evidence\":\"кратко\"}]}.\n\n"
        f"=== ДОКУМЕНТ (MD) ===\n{md_text}\n\n=== ЗАМЕЧАНИЯ ===\n{flist}\n"
    )


def parse_verification_response(parsed: dict) -> dict:
    """Из JSON-ответа собрать {индекс: verdict} + {"<i>_evidence": ...}."""
    out: dict = {}
    if not isinstance(parsed, dict):
        return out
    for row in parsed.get("verdicts") or []:
        if not isinstance(row, dict) or "i" not in row:
            continue
        try:
            i = int(row["i"])
        except (TypeError, ValueError):
            continue
        v = str(row.get("verdict") or "").strip()
        if v in ("present", "absent", "not_absence"):
            out[i] = v
            out[f"{i}_evidence"] = str(row.get("evidence") or "")[:300]
    return out


def run_claude_verification(
    md_text: str, candidates: list[dict], *, timeout_sec: int = 180
) -> dict:
    """Верификатор присутствия на `claude -p` (подписка, не платный API).

    Один батч-вызов: полный MD + все кандидаты. Fail-soft: любой сбой → {} (никого не
    понижаем). Не выбрасывает — вызывается из enforce_absence_guard под try.
    """
    import json
    import os
    import subprocess

    from backend.app.core.config import get_claude_cli, get_claude_model

    cli = get_claude_cli()
    if not cli:
        return {}
    try:
        model = get_claude_model()
    except Exception:  # noqa: BLE001
        model = "claude-sonnet-4-6"

    prompt = build_verification_prompt(md_text, candidates)
    env = {k: v for k, v in os.environ.items() if not k.startswith("CLAUDE")}
    try:
        proc = subprocess.run(
            [cli, "-p", "--model", model, "--output-format", "json"],
            input=prompt, capture_output=True, text=True,
            timeout=timeout_sec, env=env, check=False,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return {}
    if proc.returncode != 0 and not proc.stdout:
        return {}
    try:
        cli_data = json.loads(proc.stdout)
        result_text = cli_data.get("result") or ""
    except (json.JSONDecodeError, KeyError, TypeError):
        result_text = proc.stdout
    m = re.search(r"\{.*\}", result_text or "", re.DOTALL)
    if not m:
        return {}
    try:
        return parse_verification_response(json.loads(m.group(0)))
    except json.JSONDecodeError:
        return {}
