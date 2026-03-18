#!/usr/bin/env python3
"""
Нормативная база: извлечение, верификация, обновление кеша норм.

Использование:
    python norms.py verify projects/<name>                # извлечь нормы из findings
    python norms.py verify projects/<name> --extract-only # только извлечь, без Claude

    python norms.py update projects/<name>    # обновить базу из одного проекта
    python norms.py update --all              # обновить из всех проектов
    python norms.py update --stats            # статистика базы
    python norms.py update --stale            # устаревшие нормы
"""
import json
import re
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).parent
NORMS_DB_PATH = BASE_DIR / "norms_db.json"
NORMS_PARAGRAPHS_PATH = BASE_DIR / "norms_paragraphs.json"
PROJECTS_DIR = BASE_DIR / "projects"


def _iter_project_dirs_pathlib(root: Path) -> list[Path]:
    """Рекурсивно найти все папки проектов (pathlib-версия)."""
    results = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir() or entry.name.startswith("_"):
            continue
        if (entry / "project_info.json").exists() or list(entry.glob("*.pdf")):
            results.append(entry)
        else:
            for sub in sorted(entry.iterdir()):
                if sub.is_dir() and not sub.name.startswith("_"):
                    results.append(sub)
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# VERIFY — извлечение нормативных ссылок из findings
# ═══════════════════════════════════════════════════════════════════════════════

NORM_PATTERNS = [
    r'СП\s+[\d\.]+\.\d{7}\.\d{4}',
    r'СП\s+\d+\.\d+\.\d{4}',
    r'ГОСТ\s+(?:Р\s+)?(?:IEC\s+)?(?:МЭК\s+)?[\d\.\-]+(?:\-\d{4})?',
    r'ПУЭ[\s\-]*[67]?',
    r'СНиП\s+[\d\.\-\*]+',
    r'ВСН\s+[\d\-]+',
    r'ФЗ[\s\-]*\d+',
    r'ПП\s+РФ\s+[№]?\s*\d+',
    r'СО\s+[\d\.\-]+',
]

NORM_REGEX = re.compile('|'.join(f'({p})' for p in NORM_PATTERNS), re.IGNORECASE)


def extract_norms_from_text(text: str) -> list[str]:
    """Извлечь нормативные ссылки из текста."""
    matches = NORM_REGEX.findall(text)
    norms = set()
    for match_tuple in matches:
        for m in match_tuple:
            if m.strip():
                norms.add(m.strip())
    return sorted(norms)


def extract_norms_from_findings(findings_path: Path) -> dict:
    """Прочитать 03_findings.json, извлечь все нормативные ссылки."""
    with open(findings_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    findings = data.get("findings", [])
    norms_map = {}

    for finding in findings:
        fid = finding.get("id", "?")
        norm_field = finding.get("norm") or ""
        problem_field = finding.get("finding") or finding.get("problem") or ""
        recommendation = finding.get("recommendation") or finding.get("solution") or ""

        found_norms = extract_norms_from_text(norm_field)
        found_norms += extract_norms_from_text(problem_field)
        found_norms += extract_norms_from_text(recommendation)

        for norm in found_norms:
            key = re.sub(r'\s+', ' ', norm).strip()

            if key not in norms_map:
                norms_map[key] = {
                    "cited_as": [],
                    "affected_findings": [],
                    "contexts": [],
                    "low_confidence_findings": [],
                    "finding_norms": {},
                }

            if norm_field and norm_field not in norms_map[key]["cited_as"]:
                norms_map[key]["cited_as"].append(norm_field)
            if fid not in norms_map[key]["affected_findings"]:
                norms_map[key]["affected_findings"].append(fid)

            # Сохраняем полную ссылку на норму из finding для paragraph cache
            if norm_field and fid not in norms_map[key].get("finding_norms", {}):
                norms_map[key].setdefault("finding_norms", {})[fid] = norm_field

            ctx = problem_field[:200] if problem_field else ""
            if ctx and ctx not in norms_map[key]["contexts"]:
                norms_map[key]["contexts"].append(ctx)

            confidence = finding.get("norm_confidence")
            if confidence is not None and confidence < 0.8:
                if fid not in norms_map[key]["low_confidence_findings"]:
                    norms_map[key]["low_confidence_findings"].append(fid)

    return {
        "norms": norms_map,
        "total_findings": len(findings),
        "total_unique_norms": len(norms_map),
    }


def format_norms_for_template(norms_data: dict) -> str:
    """Форматировать список норм для подстановки в шаблон Claude.

    Обогащает каждую норму данными из norms_db.json:
    - cached_status, edition_status, last_verified
    - force_websearch (True если кеш устарел или нормы нет в базе)

    Это превращает правило "проверять кеш старше 30 дней" из текста промпта
    в детерминированное поле входных данных.
    """
    # Загрузить кеш норм
    db = load_norms_db()
    db_norms = db.get("norms", {})
    now = datetime.now()
    stale_days = db.get("meta", {}).get("stale_after_days", 30)

    lines = []
    for i, (norm, info) in enumerate(norms_data["norms"].items(), 1):
        findings_str = ", ".join(info["affected_findings"])
        cited = info["cited_as"][0] if info["cited_as"] else norm

        # Поиск в кеше (с нормализацией ключа)
        norm_key = normalize_doc_number(norm)
        cached = db_norms.get(norm_key)

        if cached:
            cached_status = cached.get("status", "?")
            edition_st = cached.get("edition_status", "")
            last_ver = cached.get("last_verified", "")
            # Вычислить stale
            is_stale = True
            if last_ver:
                try:
                    ver_date = datetime.fromisoformat(last_ver)
                    is_stale = (now - ver_date) > timedelta(days=stale_days)
                except (ValueError, TypeError):
                    is_stale = True
            force_ws = is_stale
            cache_line = (
                f"   - **Кеш:** status=`{cached_status}`"
                + (f", edition=`{edition_st}`" if edition_st else "")
                + f", last_verified=`{last_ver[:10] if last_ver else '?'}`"
                + f", **force_websearch={force_ws}**"
            )
        else:
            force_ws = True
            cache_line = "   - **Кеш:** не найдена, **force_websearch=True**"

        entry = (
            f"{i}. **{norm}**\n"
            f"   - Как указано в проекте: `{cited}`\n"
            f"   - Затронутые замечания: {findings_str}\n"
            f"{cache_line}"
        )
        low_conf = info.get("low_confidence_findings", [])
        if low_conf:
            entry += f"\n   - Требуют проверки цитат: {', '.join(low_conf)}"
        lines.append(entry)
    return "\n".join(lines)


def format_findings_to_fix(norm_checks_path: Path, findings_path: Path) -> str:
    """Определить какие замечания нужно пересмотреть после верификации норм."""
    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks = json.load(f)
    with open(findings_path, "r", encoding="utf-8") as f:
        findings_data = json.load(f)

    findings_map = {f["id"]: f for f in findings_data.get("findings", [])}
    lines = []

    revision_fids = set()
    for check in checks.get("checks", []):
        if not check.get("needs_revision", False):
            continue
        for fid in check.get("affected_findings", []):
            finding = findings_map.get(fid)
            if not finding:
                continue
            revision_fids.add(fid)
            lines.append(
                f"### {fid}\n"
                f"- **Текущая норма:** `{finding.get('norm', '?')}`\n"
                f"- **Проблема:** {check.get('status', '?')} — {check.get('details', '')}\n"
                f"- **Актуальный документ:** `{check.get('current_version', '?')}`\n"
                f"- **Замена:** `{check.get('replacement_doc') or 'нет'}`\n"
            )

    for pc in checks.get("paragraph_checks", []):
        if pc.get("paragraph_verified", True):
            continue
        fid = pc.get("finding_id", "")
        if fid in revision_fids:
            continue
        finding = findings_map.get(fid)
        if not finding:
            continue
        revision_fids.add(fid)
        lines.append(
            f"### {fid}\n"
            f"- **Текущая норма:** `{finding.get('norm', '?')}`\n"
            f"- **Проблема:** Цитата пункта не подтверждена\n"
            f"- **Заявленная цитата:** `{pc.get('claimed_quote', '?')}`\n"
            f"- **Реальный текст:** `{pc.get('actual_quote') or 'не найден'}`\n"
            f"- **Расхождение:** {pc.get('mismatch_details', '?')}\n"
        )

    if not lines:
        return "Все нормы актуальны. Пересмотр не требуется."
    return "\n".join(lines)


def generate_deterministic_checks(norms_data: dict, project_id: str = "") -> dict:
    """Детерминированная проверка статуса документов из norms_db.json.

    Python гарантирует актуальность документа (железобетон).
    Не использует LLM — только реестр + TTL-контроль.

    Args:
        norms_data: извлечённые нормы из findings.
        project_id: идентификатор проекта (для трассировки в meta).

    Returns:
        {
            "checks": [...],           # статусы всех норм (из БД или unknown)
            "unknown_norms": [...],     # нормы, требующие WebSearch (нет в БД или stale)
            "paragraphs_to_verify": [...],  # цитаты с low confidence
            "meta": {...},
        }
    """
    db = load_norms_db()
    db_norms = db.get("norms", {})
    replacements = db.get("replacements", {})
    now = datetime.now()
    stale_days = db.get("meta", {}).get("stale_after_days", 30)

    # Загрузить параграфы для быстрой проверки
    pdb = load_norms_paragraphs()
    known_paragraphs = pdb.get("paragraphs", {})

    checks = []
    unknown_norms = []
    paragraphs_to_verify = []

    stats = {
        "total": 0,
        "from_db": 0,
        "unknown": 0,
        "active": 0,
        "outdated_edition": 0,
        "replaced": 0,
        "cancelled": 0,
    }

    for norm_raw, info in norms_data.get("norms", {}).items():
        stats["total"] += 1
        norm_key = normalize_doc_number(norm_raw)
        cited_as = info["cited_as"][0] if info.get("cited_as") else norm_raw
        affected = info.get("affected_findings", [])

        cached = db_norms.get(norm_key)

        if cached:
            last_ver = cached.get("last_verified", "")
            is_stale = True
            if last_ver:
                try:
                    ver_date = datetime.fromisoformat(last_ver)
                    is_stale = (now - ver_date) > timedelta(days=stale_days)
                except (ValueError, TypeError):
                    is_stale = True

            if is_stale:
                # В базе есть, но устарел — нужен WebSearch для подтверждения
                unknown_norms.append({
                    "norm": norm_raw,
                    "norm_key": norm_key,
                    "cited_as": cited_as,
                    "affected_findings": affected,
                    "reason": "stale_cache",
                    "cached_status": cached.get("status"),
                    "last_verified": last_ver,
                })
                # Пока ставим статус из кеша (с пометкой)
                check_entry = _build_check_from_cache(
                    norm_raw, norm_key, cached, cited_as, affected,
                    verified_via="cache_stale",
                )
                checks.append(check_entry)
                stats["unknown"] += 1
            else:
                # Свежий кеш — детерминированный статус
                check_entry = _build_check_from_cache(
                    norm_raw, norm_key, cached, cited_as, affected,
                    verified_via="deterministic",
                )
                checks.append(check_entry)
                stats["from_db"] += 1
                s = check_entry["status"]
                if s in stats:
                    stats[s] += 1
        else:
            # Нормы нет в базе — нужен WebSearch
            unknown_norms.append({
                "norm": norm_raw,
                "norm_key": norm_key,
                "cited_as": cited_as,
                "affected_findings": affected,
                "reason": "not_in_db",
            })
            checks.append({
                "norm_as_cited": cited_as,
                "doc_number": norm_key,
                "status": "unknown",
                "edition_status": "unknown",
                "current_version": None,
                "replacement_doc": None,
                "source_url": None,
                "details": "Норма не найдена в norms_db.json — требуется WebSearch",
                "affected_findings": affected,
                "needs_revision": False,  # Пока неизвестно
                "verified_via": "pending_websearch",
            })
            stats["unknown"] += 1

        # Идентификация цитат для проверки LLM
        for fid in affected:
            low_conf = info.get("low_confidence_findings", [])
            if fid in low_conf:
                # Paragraph cache: ключ — нормализованный текст ссылки на норму
                finding_norm = info.get("finding_norms", {}).get(fid, "")
                paragraph_key = normalize_paragraph_key(
                    finding_norm.strip() if finding_norm else norm_key
                )
                if paragraph_key not in known_paragraphs:
                    paragraphs_to_verify.append({
                        "finding_id": fid,
                        "norm": norm_raw,
                        "norm_key": norm_key,
                        "paragraph_key": paragraph_key,
                    })

    # Лимит на параграфы (как раньше — max 10)
    paragraphs_to_verify = paragraphs_to_verify[:10]

    meta = {
        "project_id": project_id,
        "check_date": now.isoformat(),
        "total_checked": stats["total"],
        "from_db": stats["from_db"],
        "unknown_need_websearch": stats["unknown"],
        "policy_violations": [],
        "results": {
            "active": stats["active"],
            "outdated_edition": stats["outdated_edition"],
            "replaced": stats["replaced"],
            "cancelled": stats["cancelled"],
            "unknown": stats["unknown"],
        },
    }

    return {
        "checks": checks,
        "unknown_norms": unknown_norms,
        "paragraphs_to_verify": paragraphs_to_verify,
        "meta": meta,
    }


def _build_check_from_cache(
    norm_raw: str,
    norm_key: str,
    cached: dict,
    cited_as: str,
    affected: list[str],
    verified_via: str,
) -> dict:
    """Построить запись check из кешированных данных."""
    db_status = cached.get("status", "active")
    edition_status = cached.get("edition_status")

    # Определить финальный статус для замечания
    if edition_status == "outdated":
        display_status = "outdated_edition"
    else:
        display_status = db_status

    # needs_revision: детерминированно
    needs_revision = display_status in ("replaced", "cancelled", "outdated_edition")

    # edition_status: явное значение для каждого check
    # active — текущая редакция, outdated_edition — есть новая,
    # replaced/cancelled — документ заменён/отменён, unknown — неизвестно
    if display_status == "outdated_edition":
        resolved_edition_status = "outdated_edition"
    elif display_status in ("replaced", "cancelled"):
        resolved_edition_status = display_status
    elif display_status == "active":
        resolved_edition_status = "active"
    else:
        resolved_edition_status = "unknown"

    return {
        "norm_as_cited": cited_as,
        "doc_number": norm_key,
        "status": display_status,
        "edition_status": resolved_edition_status,
        "current_version": cached.get("current_version"),
        "replacement_doc": cached.get("replacement_doc"),
        "source_url": cached.get("source_url"),
        "details": cached.get("notes", ""),
        "affected_findings": affected,
        "needs_revision": needs_revision,
        "verified_via": verified_via,
    }


def merge_llm_norm_results(
    deterministic_path: Path,
    llm_results_path: Path,
) -> dict:
    """Слить результаты LLM (WebSearch) с детерминированными проверками.

    LLM обновляет:
    1. checks[] для unknown_norms (заполняет реальный статус)
    2. paragraph_checks[] (верификация цитат)

    Returns: статистика слияния.
    """
    with open(deterministic_path, "r", encoding="utf-8") as f:
        det_data = json.load(f)
    with open(llm_results_path, "r", encoding="utf-8") as f:
        llm_data = json.load(f)

    # Индексировать детерминированные checks по doc_number
    det_checks = {c["doc_number"]: c for c in det_data.get("checks", [])}

    # Слить LLM checks (обновить unknown → реальный статус)
    updated = 0
    for llm_check in llm_data.get("checks", []):
        doc = llm_check.get("doc_number", "")
        if not doc:
            continue
        doc_key = normalize_doc_number(doc)
        if doc_key in det_checks:
            old = det_checks[doc_key]
            if old.get("status") == "unknown" or old.get("verified_via") in (
                "pending_websearch", "cache_stale",
            ):
                # Обновить из LLM
                old["status"] = llm_check.get("status", old["status"])
                old["current_version"] = llm_check.get("current_version") or old.get("current_version")
                old["replacement_doc"] = llm_check.get("replacement_doc") or old.get("replacement_doc")
                old["source_url"] = llm_check.get("source_url") or old.get("source_url")
                old["details"] = llm_check.get("details") or old.get("details", "")
                old["verified_via"] = llm_check.get("verified_via", "websearch")
                # Пересчитать needs_revision
                old["needs_revision"] = old["status"] in (
                    "replaced", "cancelled", "outdated_edition",
                )
                updated += 1
        else:
            # Новая норма от LLM (не было в детерминированном списке)
            det_checks[doc_key] = llm_check
            updated += 1

    # Собрать финальный norm_checks.json
    final_checks = list(det_checks.values())

    # paragraph_checks берём целиком от LLM
    paragraph_checks = llm_data.get("paragraph_checks", [])

    # Пересчитать meta
    meta = det_data.get("meta", {})
    meta["from_websearch"] = updated
    by_status = {}
    for c in final_checks:
        s = c.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1
    meta["results"] = by_status

    final = {
        "meta": meta,
        "checks": final_checks,
        "paragraph_checks": paragraph_checks,
    }

    # Записать финальный norm_checks.json (в то же место что deterministic)
    with open(deterministic_path, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)

    # Обновить norms_db.json из LLM-результатов
    db = load_norms_db()
    db_updated = 0
    for llm_check in llm_data.get("checks", []):
        result = merge_norm_check(db, llm_check, meta.get("project_id", "unknown"))
        if result in ("added", "updated"):
            db_updated += 1
    if db_updated > 0:
        save_norms_db(db)

    return {
        "checks_updated_from_llm": updated,
        "paragraph_checks": len(paragraph_checks),
        "norms_db_updated": db_updated,
    }


def merge_chunked_llm_results(chunk_paths: list[Path], merged_path: Path) -> dict:
    """Слить несколько norm_checks_llm_*.json в один norm_checks_llm.json.

    Используется при параллельной верификации норм (chunked mode).
    """
    all_checks = []
    all_paragraphs = []
    for cp in chunk_paths:
        if not cp.exists():
            continue
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
            all_checks.extend(data.get("checks", []))
            all_paragraphs.extend(data.get("paragraph_checks", []))
        except (json.JSONDecodeError, OSError):
            continue

    merged = {
        "meta": {
            "merged_from": len(chunk_paths),
            "merge_date": datetime.now().isoformat(),
        },
        "checks": all_checks,
        "paragraph_checks": all_paragraphs,
    }
    merged_path.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return {"chunks_merged": len(chunk_paths), "checks": len(all_checks), "paragraphs": len(all_paragraphs)}


def format_llm_work_for_template(
    unknown_norms: list[dict],
    paragraphs_to_verify: list[dict],
    findings_path: Path | None = None,
) -> str:
    """Форматировать только LLM-работу для шаблона norm_verify.

    Включает:
    1. Нормы с unknown статусом — нужен WebSearch для определения статуса
    2. Цитаты с low confidence — нужен WebSearch для верификации текста пункта
    """
    lines = []

    if unknown_norms:
        lines.append("## Часть 1: Определение статуса документов (WebSearch)\n")
        lines.append("Для каждой нормы ниже выполни WebSearch и определи актуальный статус:\n")
        for i, norm in enumerate(unknown_norms, 1):
            reason = "не в базе" if norm.get("reason") == "not_in_db" else f"кеш устарел (проверен: {norm.get('last_verified', '?')[:10]})"
            findings_str = ", ".join(norm.get("affected_findings", []))
            cached_status = norm.get("cached_status")
            cached_hint = f", предыдущий статус: `{cached_status}`" if cached_status else ""
            lines.append(
                f"{i}. **{norm['norm']}** ({reason}{cached_hint})\n"
                f"   - Затронутые замечания: {findings_str}\n"
            )

    if paragraphs_to_verify:
        lines.append("\n## Часть 2: Верификация цитат пунктов (WebSearch)\n")
        lines.append("Для каждого замечания ниже проверь точный текст пункта нормы:\n")
        for i, pv in enumerate(paragraphs_to_verify, 1):
            lines.append(
                f"{i}. Замечание **{pv['finding_id']}**: норма `{pv['norm']}`\n"
            )

    if not lines:
        return ""

    return "\n".join(lines)


def validate_norm_checks(norm_checks_path: Path) -> dict:
    """Пост-валидация norm_checks.json — программный слой контроля.

    Проверяет:
    1. needs_revision=True для replaced/cancelled/outdated_edition
    2. force_websearch нарушения (verified_via="cache" при force_websearch=True)

    Возвращает dict с результатами и списком исправлений.
    """
    if not norm_checks_path.exists():
        return {"valid": False, "error": "norm_checks.json не найден"}

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    checks = checks_data.get("checks", [])
    fixes = []
    violations = []

    for check in checks:
        doc = check.get("doc_number", "?")
        status = check.get("status", "")

        # Правило 1: replaced/cancelled/outdated_edition → needs_revision=True
        if status in ("replaced", "cancelled", "outdated_edition"):
            if not check.get("needs_revision", False):
                check["needs_revision"] = True
                fixes.append(
                    f"{doc}: needs_revision принудительно=True (status={status})"
                )

        # Правило 2: stale кеш + verified_via="cache" = policy_violation
        if check.get("verified_via") in ("cache", "cache_stale"):
            db = load_norms_db()
            cached = db.get("norms", {}).get(normalize_doc_number(doc))
            if cached:
                last_ver = cached.get("last_verified", "")
                stale_days = db.get("meta", {}).get("stale_after_days", 30)
                if last_ver:
                    try:
                        ver_date = datetime.fromisoformat(last_ver)
                        is_stale = (datetime.now() - ver_date) > timedelta(days=stale_days)
                        if is_stale:
                            violations.append(
                                f"{doc}: verified_via='{check['verified_via']}' но кеш устарел "
                                f"(last_verified={last_ver[:10]}, stale_days={stale_days})"
                            )
                            check["verified_via"] = "cache_stale"
                            check["_policy_violation"] = "stale_cache_used"
                    except (ValueError, TypeError):
                        pass

        # Правило 3: outdated_edition не должен схлопываться в active
        if status == "outdated_edition" and not check.get("needs_revision", False):
            check["needs_revision"] = True
            fixes.append(f"{doc}: outdated_edition принудительно needs_revision=True")

    # Записать policy_violations в meta
    meta = checks_data.get("meta", {})
    meta["policy_violations"] = violations
    checks_data["meta"] = meta

    # Записать исправления обратно
    if fixes or violations:
        with open(norm_checks_path, "w", encoding="utf-8") as f:
            json.dump(checks_data, f, ensure_ascii=False, indent=2)

    return {
        "valid": len(violations) == 0,
        "total_checks": len(checks),
        "policy_violations": violations,
        "fixes_applied": fixes,
        "violations": violations,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# UPDATE — обновление централизованной базы норм
# ═══════════════════════════════════════════════════════════════════════════════

def load_norms_db() -> dict:
    """Загрузить базу норм."""
    if not NORMS_DB_PATH.exists():
        return {
            "meta": {
                "description": "Централизованная база нормативных документов с автообновлением",
                "last_updated": datetime.now().isoformat(),
                "total_norms": 0,
                "stale_after_days": 30,
                "update_history": [],
            },
            "norms": {},
            "replacements": {},
        }
    with open(NORMS_DB_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_norms_db(db: dict):
    """Сохранить базу норм."""
    db["meta"]["total_norms"] = len(db["norms"])
    db["meta"]["last_updated"] = datetime.now().isoformat()
    with open(NORMS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def load_norms_paragraphs() -> dict:
    """Загрузить справочник проверенных параграфов."""
    if not NORMS_PARAGRAPHS_PATH.exists():
        return {
            "meta": {
                "description": "Проверенные цитаты конкретных пунктов нормативных документов",
                "last_updated": None,
                "total_paragraphs": 0,
            },
            "paragraphs": {},
        }
    with open(NORMS_PARAGRAPHS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_norms_paragraphs(pdb: dict):
    """Сохранить справочник параграфов."""
    pdb["meta"]["total_paragraphs"] = len(pdb["paragraphs"])
    pdb["meta"]["last_updated"] = datetime.now().isoformat()
    with open(NORMS_PARAGRAPHS_PATH, "w", encoding="utf-8") as f:
        json.dump(pdb, f, ensure_ascii=False, indent=2)


def update_paragraphs_from_project(pdb: dict, project_path: Path) -> int:
    """Обновить справочник параграфов из paragraph_checks."""
    norm_checks_path = project_path / "_output" / "norm_checks.json"
    if not norm_checks_path.exists():
        return 0

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    paragraph_checks = checks_data.get("paragraph_checks", [])
    if not paragraph_checks:
        return 0

    count = 0
    for pc in paragraph_checks:
        if not pc.get("paragraph_verified"):
            continue
        norm = pc.get("norm", "")
        actual_quote = pc.get("actual_quote")
        if not norm or not actual_quote:
            continue

        key = normalize_paragraph_key(norm.strip())
        existing = pdb["paragraphs"].get(key)
        if existing and existing.get("quote") == actual_quote:
            continue

        pdb["paragraphs"][key] = {
            "norm": key,
            "quote": actual_quote,
            "verified_at": datetime.now().isoformat(),
            "source_project": project_path.name,
        }
        count += 1

    return count


# ─── Paragraph Cache: helper-функции ──────────────────────────────────────────

def normalize_paragraph_key(raw_key: str) -> str:
    """Нормализовать ключ параграфа для стабильного lookup.

    Убирает статусные хвосты (действует..., ред..., изм...),
    нормализует пробелы. Ключ стабилен независимо от формата ссылки.

    Примеры:
        "СП 256.1325800.2016 (действует), п. 15.3" → "СП 256.1325800.2016, п. 15.3"
        "СП 256.1325800.2016, п. 15.3"              → "СП 256.1325800.2016, п. 15.3"
    """
    key = raw_key.strip()
    if not key:
        return key
    # Убрать хвосты в скобках: (действует...), (ред. ...), (изм. ...)
    key = re.sub(
        r'\s*\((?:действу|ред\.|изм\.|с изм|введ|утв|актуал|в ред)[^)]*\)',
        '', key, flags=re.IGNORECASE,
    )
    # Убрать markdown
    key = key.replace("**", "").replace("*", "")
    # Нормализовать пробелы
    key = re.sub(r'\s+', ' ', key).strip()
    return key


def get_paragraph(paragraph_key: str) -> dict | None:
    """Прочитать одну запись из paragraph cache.

    Ищет сначала по точному ключу, потом по нормализованному.

    Returns: dict с полями {norm, quote, verified_at, ...} или None.
    """
    pdb = load_norms_paragraphs()
    paragraphs = pdb.get("paragraphs", {})
    key = paragraph_key.strip()
    # Точный lookup
    result = paragraphs.get(key)
    if result:
        return result
    # Нормализованный lookup (для ключей со статусом)
    norm_key = normalize_paragraph_key(key)
    if norm_key != key:
        result = paragraphs.get(norm_key)
        if result:
            return result
    # Обратный поиск: нормализуем все ключи в cache
    for cached_key, val in paragraphs.items():
        if normalize_paragraph_key(cached_key) == norm_key:
            return val
    return None


def upsert_paragraph(
    paragraph_key: str,
    quote: str,
    norm_key: str = "",
    verified_via: str = "manual",
    confidence: float = 1.0,
    project_id: str = "",
    verified: bool = True,
) -> str:
    """Добавить или обновить запись в paragraph cache.

    Не записывает неподтверждённые цитаты как подтверждённые.
    Не перетирает active verified quote устаревшей.

    Returns: "added" | "updated" | "skipped"
    """
    if not verified:
        return "skipped"

    paragraph_key = normalize_paragraph_key(paragraph_key.strip())
    if not paragraph_key or not quote:
        return "skipped"

    pdb = load_norms_paragraphs()
    existing = pdb["paragraphs"].get(paragraph_key)

    if existing:
        # Не перетираем, если та же цитата
        if existing.get("quote") == quote:
            return "skipped"
        # Не понижаем confidence
        if existing.get("confidence", 0) > confidence:
            return "skipped"

    pdb["paragraphs"][paragraph_key] = {
        "norm": norm_key or paragraph_key,
        "quote": quote,
        "verified_at": datetime.now().isoformat(),
        "verified_via": verified_via,
        "confidence": confidence,
        "source_project": project_id,
    }
    save_norms_paragraphs(pdb)
    return "added" if not existing else "updated"


def merge_paragraph_checks(paragraph_checks: list[dict], project_id: str = "") -> dict:
    """Массовое слияние paragraph_checks в paragraph cache.

    Args:
        paragraph_checks: список из norm_checks.json / norm_checks_llm.json
        project_id: идентификатор проекта

    Returns: {added, updated, skipped}
    """
    stats = {"added": 0, "updated": 0, "skipped": 0}
    pdb = load_norms_paragraphs()

    for pc in paragraph_checks:
        if not pc.get("paragraph_verified"):
            stats["skipped"] += 1
            continue

        quote = pc.get("actual_quote") or ""
        norm = pc.get("norm", "")
        paragraph_key = normalize_paragraph_key(
            pc.get("paragraph_key") or norm.strip()
        )

        if not paragraph_key or not quote:
            stats["skipped"] += 1
            continue

        existing = pdb["paragraphs"].get(paragraph_key)
        if existing and existing.get("quote") == quote:
            stats["skipped"] += 1
            continue

        pdb["paragraphs"][paragraph_key] = {
            "norm": norm,
            "quote": quote,
            "verified_at": datetime.now().isoformat(),
            "verified_via": pc.get("verified_via", "websearch"),
            "confidence": pc.get("confidence", 0.9),
            "source_project": project_id,
            "finding_id": pc.get("finding_id", ""),
        }
        stats["added" if not existing else "updated"] += 1

    if stats["added"] + stats["updated"] > 0:
        save_norms_paragraphs(pdb)

    return stats


def paragraph_cache_stats() -> dict:
    """Статистика paragraph cache."""
    pdb = load_norms_paragraphs()
    paragraphs = pdb.get("paragraphs", {})
    total = len(paragraphs)
    empty_quote = sum(1 for p in paragraphs.values() if not p.get("quote"))
    by_source = {}
    for p in paragraphs.values():
        via = p.get("verified_via", "unknown")
        by_source[via] = by_source.get(via, 0) + 1
    return {
        "total": total,
        "empty_quote": empty_quote,
        "by_verified_via": by_source,
        "last_updated": pdb.get("meta", {}).get("last_updated"),
    }


def _guess_category(doc_number: str) -> str:
    """Определить категорию по номеру документа."""
    dn = doc_number.upper()
    if dn.startswith("ФЗ"):
        return "federal_law"
    if dn.startswith("ПП РФ"):
        return "government_decree"
    if dn.startswith("ПУЭ"):
        return "pue"
    if "13130" in dn or "1311500" in dn:
        return "sp_fire"
    if dn.startswith("СП"):
        return "sp"
    if dn.startswith("ГОСТ"):
        return "gost"
    if dn.startswith("СО ") or dn.startswith("ВСН"):
        return "other"
    return "other"


def normalize_doc_number(raw: str) -> str:
    """Нормализовать номер документа для использования как ключ.

    Правила:
    1. Убрать markdown-жирный (**), лишние пробелы
    2. Убрать хвосты: (действует...), (ред...), (изм...), (с изменениями...)
    3. Унифицировать пробелы и дефисы
    4. НЕ убирать год — он часть ключа (СП 54.13330.2022 ≠ СП 54.13330.2016)
    5. НЕ схлопывать "ГОСТ Р" в "ГОСТ" — это разные документы
    """
    doc = raw.strip()
    # Убрать markdown
    doc = doc.replace("**", "").replace("*", "")
    # Убрать хвосты в скобках: (действует...), (ред. ...), (изм. ...), (с изменениями...)
    doc = re.sub(
        r'\s*\((?:действу|ред\.|изм\.|с изм|введ|утв|актуал|в ред)[^)]*\)',
        '', doc, flags=re.IGNORECASE,
    )
    # Убрать хвосты без скобок: "с Изменениями №1-3", "с Изменением №1"
    doc = re.sub(
        r'\s+с\s+(?:[Ии]зменениями?|[Ии]зменением)\s*(?:№\s*[\d,\s\-–]+)?',
        '', doc,
    )
    # Убрать "ред. DD.MM.YYYY" без скобок
    doc = re.sub(r'\s*ред\.\s*\d{2}\.\d{2}\.\d{4}', '', doc)
    # Унифицировать пробелы
    doc = re.sub(r'\s+', ' ', doc).strip()
    # Убрать trailing точку/запятую
    doc = doc.rstrip('.,;: ')
    return doc


def merge_norm_check(db: dict, check: dict, project_id: str) -> str:
    """Слить одну проверку нормы в базу."""
    doc_number = normalize_doc_number(check.get("doc_number", ""))
    if not doc_number:
        return "skipped"

    status = check.get("status", "not_found")
    if status == "not_found":
        return "skipped"

    now = datetime.now().isoformat()

    # Статус документа: active/replaced/cancelled
    # edition_status: ok/outdated/unknown — отдельно от статуса документа
    status_map = {
        "active": "active",
        "outdated_edition": "active",
        "replaced": "replaced",
        "cancelled": "cancelled",
    }
    db_status = status_map.get(status, status)

    # Детальный статус редакции — НЕ схлопываем
    if status == "outdated_edition":
        edition_status = "outdated"
    elif status == "active":
        edition_status = "ok"
    else:
        edition_status = None  # для replaced/cancelled не применимо

    existing = db.get("norms", {}).get(doc_number)

    if existing:
        changed = False
        if existing.get("status") != db_status:
            existing["status"] = db_status
            changed = True
        # Сохраняем детальный статус редакции
        if edition_status is not None:
            if existing.get("edition_status") != edition_status:
                existing["edition_status"] = edition_status
                changed = True
        elif "edition_status" in existing and db_status in ("replaced", "cancelled"):
            # Для заменённых/отменённых — убираем edition_status (неприменимо)
            del existing["edition_status"]
            changed = True
        new_version = check.get("current_version")
        if new_version and new_version != existing.get("current_version"):
            existing["current_version"] = new_version
            changed = True
        replacement = check.get("replacement_doc")
        if replacement and replacement != existing.get("replacement_doc"):
            existing["replacement_doc"] = replacement
            changed = True
            if "replacements" not in db:
                db["replacements"] = {}
            db["replacements"][doc_number] = replacement
        source_url = check.get("source_url")
        if source_url and source_url != existing.get("source_url"):
            existing["source_url"] = source_url
            changed = True
        details = check.get("details")
        if details:
            existing["notes"] = details
        existing["last_verified"] = now
        existing["verified_by"] = f"websearch:{project_id}"
        return "updated" if changed else "unchanged"
    else:
        new_entry = {
            "doc_number": doc_number,
            "title": check.get("norm_as_cited", doc_number),
            "status": db_status,
            "current_version": check.get("current_version"),
            "replacement_doc": check.get("replacement_doc"),
            "category": _guess_category(doc_number),
            "notes": check.get("details", ""),
            "source_url": check.get("source_url"),
            "last_verified": now,
            "verified_by": f"websearch:{project_id}",
        }
        # Сохраняем детальный статус редакции
        if edition_status is not None:
            new_entry["edition_status"] = edition_status
        db["norms"][doc_number] = new_entry
        replacement = check.get("replacement_doc")
        if replacement and db_status in ("replaced", "cancelled"):
            if "replacements" not in db:
                db["replacements"] = {}
            db["replacements"][doc_number] = replacement
        return "added"


def update_from_project(db: dict, project_path: Path) -> dict:
    """Обновить базу из norm_checks.json одного проекта."""
    norm_checks_path = project_path / "_output" / "norm_checks.json"
    if not norm_checks_path.exists():
        return {"error": f"norm_checks.json не найден в {project_path}"}

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    checks = checks_data.get("checks", [])
    project_id = project_path.name

    stats = {"added": 0, "updated": 0, "unchanged": 0, "skipped": 0}
    for check in checks:
        result = merge_norm_check(db, check, project_id)
        stats[result] = stats.get(result, 0) + 1
    return stats


def get_stale_norms(db: dict) -> list:
    """Получить список норм, которые давно не проверялись."""
    stale_days = db.get("meta", {}).get("stale_after_days", 30)
    threshold = datetime.now() - timedelta(days=stale_days)
    stale = []
    for doc_number, norm in db.get("norms", {}).items():
        last_verified = norm.get("last_verified", "")
        if not last_verified:
            stale.append(doc_number)
            continue
        try:
            verified_dt = datetime.fromisoformat(last_verified)
            if verified_dt < threshold:
                stale.append(doc_number)
        except (ValueError, TypeError):
            stale.append(doc_number)
    return stale


def print_stats(db: dict):
    """Вывести статистику базы."""
    norms = db.get("norms", {})
    total = len(norms)

    by_status = {}
    by_category = {}
    for norm in norms.values():
        status = norm.get("status", "unknown")
        category = norm.get("category", "other")
        by_status[status] = by_status.get(status, 0) + 1
        by_category[category] = by_category.get(category, 0) + 1

    stale = get_stale_norms(db)
    replacements = db.get("replacements", {})
    pdb = load_norms_paragraphs()
    total_paragraphs = len(pdb.get("paragraphs", {}))

    print(f"\n{'='*60}")
    print(f"  БАЗА НОРМАТИВНЫХ ДОКУМЕНТОВ — СТАТИСТИКА")
    print(f"{'='*60}")
    print(f"  Всего норм:         {total}")
    print(f"  Проверенных цитат:  {total_paragraphs}")
    print(f"  Таблица замен:      {len(replacements)} записей")
    print(f"  Устаревших (>{db['meta'].get('stale_after_days', 30)} дн): {len(stale)}")
    print(f"  Последнее обновление: {db['meta'].get('last_updated', 'N/A')}")
    print()
    print("  По статусу:")
    for status, count in sorted(by_status.items()):
        icon = {"active": "+", "replaced": "!", "cancelled": "X", "limited": "~", "voluntary": "?"}.get(status, " ")
        print(f"    [{icon}] {status}: {count}")
    print()
    print("  По категории:")
    for cat, count in sorted(by_category.items()):
        print(f"    {cat}: {count}")

    if stale:
        print(f"\n  Нормы, требующие повторной проверки ({len(stale)}):")
        for doc in stale[:10]:
            norm = norms[doc]
            print(f"    - {doc} (проверена: {norm.get('last_verified', 'никогда')[:10]})")
        if len(stale) > 10:
            print(f"    ... и ещё {len(stale) - 10}")

    print(f"{'='*60}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI — точка входа с подкомандами
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python norms.py verify projects/<name>              # извлечь нормы")
        print("  python norms.py verify projects/<name> --extract-only")
        print("  python norms.py update projects/<name>              # обновить из проекта")
        print("  python norms.py update --all                       # обновить из всех")
        print("  python norms.py update --stats                     # статистика")
        print("  python norms.py update --stale                     # устаревшие")
        sys.exit(1)

    command = sys.argv[1]

    if command == "verify":
        if len(sys.argv) < 3:
            print("Использование: python norms.py verify projects/<name> [--extract-only]")
            sys.exit(1)

        project_dir = Path(sys.argv[2])
        extract_only = "--extract-only" in sys.argv

        if not project_dir.is_absolute():
            project_dir = Path.cwd() / project_dir

        output_dir = project_dir / "_output"
        findings_path = output_dir / "03_findings.json"

        if not findings_path.exists():
            print(f"ОШИБКА: Файл {findings_path} не найден. Сначала выполните аудит (этап 03).")
            sys.exit(1)

        print(f"Извлечение нормативных ссылок из {findings_path.name}...")
        norms_data = extract_norms_from_findings(findings_path)

        print(f"Найдено замечаний: {norms_data['total_findings']}")
        print(f"Уникальных нормативных ссылок: {norms_data['total_unique_norms']}")

        for norm, info in norms_data["norms"].items():
            findings_str = ", ".join(info["affected_findings"])
            print(f"  - {norm} (в замечаниях: {findings_str})")

        norms_extracted_path = output_dir / "norms_extracted.json"
        with open(norms_extracted_path, "w", encoding="utf-8") as f:
            json.dump({
                "project_dir": str(project_dir),
                "extracted_at": datetime.now().isoformat(),
                **norms_data,
            }, f, ensure_ascii=False, indent=2)

        print(f"Сохранено: {norms_extracted_path}")

        if extract_only:
            print("Режим --extract-only: Claude CLI не запускается.")
            return

        norms_list_text = format_norms_for_template(norms_data)
        print(f"\nСписок норм для верификации:\n{norms_list_text}")
        print(f"\nДля запуска верификации через Claude CLI используйте webapp или pipeline.")

    elif command == "update":
        if len(sys.argv) < 3:
            print("Использование: python norms.py update [projects/<name> | --all | --stats | --stale]")
            sys.exit(1)

        db = load_norms_db()
        arg = sys.argv[2]

        if arg == "--stats":
            print_stats(db)
            return

        if arg == "--stale":
            stale = get_stale_norms(db)
            if stale:
                print(f"Нормы, требующие проверки ({len(stale)}):")
                for doc in stale:
                    norm = db["norms"].get(doc, {})
                    print(f"  {doc} — проверена: {norm.get('last_verified', 'никогда')[:10]}")
            else:
                print("Все нормы актуальны.")
            return

        if arg == "--all":
            if not PROJECTS_DIR.is_dir():
                print(f"Папка проектов не найдена: {PROJECTS_DIR}")
                sys.exit(1)

            total_stats = {"added": 0, "updated": 0, "unchanged": 0, "skipped": 0}
            processed = 0
            pdb = load_norms_paragraphs()
            total_paragraphs = 0

            for project_dir in _iter_project_dirs_pathlib(PROJECTS_DIR):
                norm_checks = project_dir / "_output" / "norm_checks.json"
                if not norm_checks.exists():
                    continue

                stats = update_from_project(db, project_dir)
                if "error" in stats:
                    print(f"  [{project_dir.name}] {stats['error']}")
                    continue

                processed += 1
                for key in total_stats:
                    total_stats[key] += stats.get(key, 0)

                p_count = update_paragraphs_from_project(pdb, project_dir)
                total_paragraphs += p_count

                print(
                    f"  [{project_dir.name}] +"
                    f"{stats['added']} добавлено, "
                    f"{stats['updated']} обновлено, "
                    f"{stats['unchanged']} без изменений, "
                    f"{stats['skipped']} пропущено"
                    + (f", {p_count} параграфов" if p_count else "")
                )

            db["meta"]["update_history"] = db["meta"].get("update_history", [])[-9:]
            db["meta"]["update_history"].append({
                "date": datetime.now().isoformat(),
                "source": "all_projects",
                "projects_processed": processed,
                "stats": total_stats,
            })

            save_norms_db(db)
            if total_paragraphs > 0:
                save_norms_paragraphs(pdb)
            print(f"\nИтого из {processed} проектов: "
                  f"+{total_stats['added']} добавлено, "
                  f"{total_stats['updated']} обновлено, "
                  f"{total_stats['unchanged']} без изменений")
            if total_paragraphs > 0:
                print(f"Параграфов добавлено: {total_paragraphs} (всего в базе: {len(pdb['paragraphs'])})")
            print(f"База сохранена: {NORMS_DB_PATH}")
            return

        # Один проект
        project_path = Path(arg)
        if not project_path.is_absolute():
            project_path = BASE_DIR / project_path

        if not project_path.is_dir():
            print(f"Проект не найден: {project_path}")
            sys.exit(1)

        stats = update_from_project(db, project_path)
        if "error" in stats:
            print(stats["error"])
            sys.exit(1)

        pdb = load_norms_paragraphs()
        p_count = update_paragraphs_from_project(pdb, project_path)
        if p_count > 0:
            save_norms_paragraphs(pdb)

        db["meta"]["update_history"] = db["meta"].get("update_history", [])[-9:]
        db["meta"]["update_history"].append({
            "date": datetime.now().isoformat(),
            "source": project_path.name,
            "stats": stats,
        })

        save_norms_db(db)
        print(
            f"[{project_path.name}] +"
            f"{stats['added']} добавлено, "
            f"{stats['updated']} обновлено, "
            f"{stats['unchanged']} без изменений, "
            f"{stats['skipped']} пропущено"
            + (f", {p_count} параграфов" if p_count else "")
        )
        print(f"База сохранена: {NORMS_DB_PATH}")

    else:
        print(f"Неизвестная команда: {command}")
        print("Доступные команды: verify, update")
        sys.exit(1)


if __name__ == "__main__":
    main()
