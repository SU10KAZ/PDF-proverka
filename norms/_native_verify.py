"""Python-native верификация цитат норм без Claude CLI.

Заменяет Шаг 3 (LLM-чанки через MCP) в pipeline_service._verify_norms().
Читает paragraphs_to_verify, вызывает norms_api напрямую, пишет
norm_checks_llm.json в том же формате что Claude.

Скорость: ~5-15с на весь проект вместо 15-30 минут.
Fallback: при любой ошибке pipeline_service возвращается к Claude chunks.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

# Путь к Norms-main инструментам и его venv (нужен для sentence_transformers)
NORMS_TOOLS_PATH = Path("/home/coder/projects/Norms/tools")
NORMS_VENV_SITE = NORMS_TOOLS_PATH / "venv/lib/python3.12/site-packages"

# Jaccard threshold: при совпадении >= этого значения считаем пункт верным.
# Консервативное значение — неправильные пункты дают 0-10%, правильные >35%.
SIMILARITY_THRESHOLD = 0.30

# Минимальный score для semantic_search чтобы предложить кандидата
SEMANTIC_SCORE_MIN = 0.70


def _import_norms_api():
    """Импортировать norms_api из Norms-main (lazy, с path-инъекцией)."""
    venv = str(NORMS_VENV_SITE)
    tools = str(NORMS_TOOLS_PATH)
    if venv not in sys.path:
        sys.path.insert(0, venv)
    if tools not in sys.path:
        sys.path.insert(0, tools)
    import norms_api  # noqa: PLC0415
    return norms_api


def _jaccard(a: str, b: str) -> float:
    """Word-level Jaccard similarity для русского/английского текста."""
    if not a or not b:
        return 0.0
    wa = set(re.findall(r"[а-яёa-z]{3,}", a.lower()))
    wb = set(re.findall(r"[а-яёa-z]{3,}", b.lower()))
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _extract_paragraph_num(paragraph_key: str, norm_key: str, matched_code: str) -> str:
    """Извлечь номер пункта для конкретной нормы из paragraph_key.

    paragraph_key может содержать несколько норм:
    "СП 63.13330.2018, п. 5.1.1; ГОСТ Р 21.101-2020, п. 7.2"

    Ищем фрагмент относящийся к norm_key / matched_code.
    """
    # Нормализуем код для сравнения: убираем подчёркивания, лишние пробелы
    def _norm(s: str) -> str:
        return re.sub(r"[\s_]+", " ", s or "").lower().strip()

    candidates = [norm_key, matched_code]
    # Разбиваем paragraph_key на части по "; " или ","
    # и ищем ту часть где есть наш код
    parts = re.split(r";", paragraph_key or "")
    for part in parts:
        part = part.strip()
        for cand in candidates:
            if cand and _norm(cand) in _norm(part):
                m = re.search(r"п\.\s*([\d]+(?:[.\-][\d]+)*)", part)
                if m:
                    return m.group(1)
    # Fallback: первый пункт в paragraph_key
    m = re.search(r"п\.\s*([\d]+(?:[.\-][\d]+)*)", paragraph_key or "")
    return m.group(1) if m else ""


def _load_norm_quotes(findings_path: Path) -> dict[str, str]:
    """Загрузить {finding_id: norm_quote} из 03_findings.json."""
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
        return {
            f.get("id", ""): (f.get("norm_quote") or "")
            for f in data.get("findings", [])
        }
    except (json.JSONDecodeError, OSError):
        return {}


def verify_paragraphs_native(
    paragraphs_to_verify: list[dict],
    findings_path: Path,
    output_dir: Path,
) -> Path:
    """Верифицировать цитаты норм без Claude CLI.

    Args:
        paragraphs_to_verify: список из generate_deterministic_checks()
        findings_path: путь к 03_findings.json (для claimed_quote)
        output_dir: куда писать norm_checks_llm.json

    Returns:
        Path к записанному norm_checks_llm.json

    Raises:
        Exception: любая ошибка — pipeline_service поймает и уйдёт на fallback
    """
    norms_api = _import_norms_api()
    norm_quotes = _load_norm_quotes(findings_path)

    results: list[dict] = []

    for item in paragraphs_to_verify:
        finding_id = item.get("finding_id", "")
        norm_str = item.get("norm", "")
        norm_key = item.get("norm_key", "") or norm_str
        paragraph_key = item.get("paragraph_key", "") or norm_str
        matched_code = item.get("matched_code", "") or ""
        claimed_quote = norm_quotes.get(finding_id, "")
        paragraph_num = _extract_paragraph_num(paragraph_key, norm_key, matched_code)

        entry: dict = {
            "finding_id": finding_id,
            "norm": norm_str,
            "matched_code": matched_code,
            "claimed_quote": claimed_quote,
            "actual_quote": None,
            "paragraph_verified": False,
            "mismatch_details": "",
            "verified_via": "native_python",
        }

        if not paragraph_num:
            entry["mismatch_details"] = (
                f"Номер пункта не удалось извлечь из '{norm_str}'"
            )
            results.append(entry)
            continue

        # ── Шаг 1: получить реальный текст пункта ──
        para_result = norms_api.get_paragraph(matched_code, paragraph_num)
        found = para_result.get("found", False)
        has_text = para_result.get("has_text", False)
        actual_text = (para_result.get("text") or "").strip()

        if found and has_text and actual_text:
            entry["actual_quote"] = actual_text[:400]
            similarity = _jaccard(claimed_quote, actual_text)

            if similarity >= SIMILARITY_THRESHOLD:
                entry["paragraph_verified"] = True
                entry["mismatch_details"] = ""
            else:
                entry["paragraph_verified"] = False
                short_actual = actual_text[:200]
                entry["mismatch_details"] = (
                    f"п. {paragraph_num} содержит: «{short_actual}» — "
                    f"не соответствует цитате (similarity={similarity:.2f})."
                )
                # Попробуем найти правильный пункт
                _add_semantic_candidate(
                    entry, claimed_quote, matched_code, paragraph_num, norms_api
                )
        else:
            # Пункт не найден в базе
            resolution = para_result.get("resolution_reason", "paragraph_not_found")
            entry["mismatch_details"] = (
                f"п. {paragraph_num} не найден в {matched_code} ({resolution})."
            )
            _add_semantic_candidate(
                entry, claimed_quote, matched_code, paragraph_num, norms_api
            )

        results.append(entry)

    # ── Записать результат ──
    output = {
        "meta": {
            "method": "native_python",
            "verified_at": datetime.now().isoformat(),
            "total": len(results),
            "verified_true": sum(1 for r in results if r.get("paragraph_verified")),
            "verified_false": sum(1 for r in results if not r.get("paragraph_verified")),
        },
        "paragraph_checks": results,
    }

    out_path = output_dir / "norm_checks_llm.json"
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def _add_semantic_candidate(
    entry: dict,
    claimed_quote: str,
    matched_code: str,
    old_paragraph: str,
    norms_api,
) -> None:
    """Попробовать найти правильный пункт через semantic_search.

    Если найдён кандидат с score >= SEMANTIC_SCORE_MIN — добавляет его
    в mismatch_details в формате 'п. X.X.X', который _fix_paragraph_refs
    извлечёт regex'ом и подставит в findings.
    """
    if not claimed_quote or not claimed_quote.strip():
        return
    try:
        results = norms_api.semantic_search(
            claimed_quote[:300], top=3, code_filter=matched_code or None
        )
        if not results:
            return
        best = results[0]
        score = best.get("score", 0.0)
        if score < SEMANTIC_SCORE_MIN:
            return
        best_para = best.get("paragraph", "")
        best_text = (best.get("text") or "")[:150]
        if best_para and best_para != old_paragraph:
            entry["mismatch_details"] += (
                f" Лучший кандидат: п. {best_para} (score={score:.2f}): «{best_text}»"
            )
    except Exception:
        # semantic_search необязателен — не роняем весь прогон
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Шаг 7: Python-замена norm_requote (Claude CLI → прямой semantic_search)
# ─────────────────────────────────────────────────────────────────────────────

REQUOTE_SCORE_MIN = 0.80  # минимальный score для автоисправления пункта


def requote_norms_native(output_dir: Path) -> dict:
    """Уточнить цитаты норм для замечаний с флагом [Пункт нормы ... ручная сверка].

    Заменяет Claude CLI norm_requote (Шаг 7). Вызывает semantic_search
    напрямую через norms_api — тот же алгоритм что MCP, без посредника.

    Returns:
        {"resolved": N, "remaining": M, "total": K}
    """
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        return {"resolved": 0, "remaining": 0, "total": 0}

    try:
        fd = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"resolved": 0, "remaining": 0, "total": 0}

    findings = fd.get("findings", [])
    flagged = [f for f in findings if "[Пункт нормы" in (f.get("description") or "")]
    if not flagged:
        return {"resolved": 0, "remaining": 0, "total": 0}

    norms_api = _import_norms_api()
    resolved = 0

    for finding in flagged:
        norm_str = finding.get("norm", "") or ""
        norm_quote = finding.get("norm_quote", "") or ""
        query = norm_quote[:300] if norm_quote else ""
        if not query:
            continue

        # Чистый код нормы без скобок и статуса
        code = re.sub(r"\s*\([^)]*\)", "", norm_str).split(",")[0].strip()
        old_para_m = re.search(r"п\.\s*([\d.]+)", norm_str)
        old_para = old_para_m.group(1) if old_para_m else ""

        try:
            results = norms_api.semantic_search(query, top=3, code_filter=code or None)
        except Exception:
            continue

        best = results[0] if results else None
        if not best or best.get("score", 0) < REQUOTE_SCORE_MIN:
            continue

        new_para = best.get("paragraph", "")
        new_text = (best.get("text") or "")[:200]
        score = best["score"]

        if not new_para or new_para == old_para:
            # Пункт уже правильный — просто снимаем флаг и обновляем цитату
            _remove_manual_check_flag(finding, norm_str)
            finding["norm_quote"] = new_text
            resolved += 1
            continue

        # Исправляем номер пункта в поле norm
        if old_para:
            finding["norm"] = re.sub(
                r"п\.\s*" + re.escape(old_para), f"п. {new_para}", norm_str
            )
        # Обновляем цитату и снимаем флаг
        finding["norm_quote"] = new_text
        _remove_manual_check_flag(finding, norm_str)
        # Трассировка
        desc = finding.get("description", "") or ""
        finding["description"] = desc + f" [norm_requote: п.{old_para} → п.{new_para} score={score:.2f}]"
        resolved += 1

    remaining = sum(1 for f in findings if "[Пункт нормы" in (f.get("description") or ""))

    # Сохранить обновлённые findings
    import shutil
    backup = output_dir / "03_findings_pre_requote.json"
    shutil.copy2(findings_path, backup)
    findings_path.write_text(
        json.dumps(fd, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return {"resolved": resolved, "remaining": remaining, "total": len(flagged)}


def _remove_manual_check_flag(finding: dict, norm_str: str) -> None:
    """Убрать префикс [Пункт нормы ... требует ручной сверки] из description."""
    desc = finding.get("description", "") or ""
    flag = f"[Пункт нормы {norm_str} требует ручной сверки] "
    if flag in desc:
        finding["description"] = desc.replace(flag, "")
    else:
        # Убрать любой похожий флаг через regex
        finding["description"] = re.sub(
            r"\[Пункт нормы[^\]]+требует ручной сверки\]\s*", "", desc
        )
