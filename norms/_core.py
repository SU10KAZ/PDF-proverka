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


NORMS_DIR = Path(__file__).parent
BASE_DIR = NORMS_DIR.parent
NORMS_DB_PATH = NORMS_DIR / "norms_db.json"
NORMS_PARAGRAPHS_PATH = NORMS_DIR / "norms_paragraphs.json"
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
    stale_days = db.get("meta", {}).get("stale_after_days", 180)

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
    """Детерминированная проверка статусов норм через Norms-main.

    Единственный источник истины — внешний status_index.json проекта Norms.
    WebSearch и локальный norms_db.json НЕ используются: если нормы нет в
    индексе, она честно помечается как missing и уходит в очередь на ручное
    добавление.

    В paragraphs_to_verify попадают ТОЛЬКО те цитаты, для которых все три
    условия выполнены:
      * check.verified_via == "norms_authoritative"
      * check.has_text is True (в Norms-main есть текст документа)
      * нет trusted-записи в paragraph cache (trusted = provenance из нового
        MCP-флоу, не legacy websearch)

    Args:
        norms_data: извлечённые нормы из findings.
        project_id: идентификатор проекта (для трассировки в meta).

    Returns:
        {
            "checks": [...],                 # статусы всех норм
            "missing_norms": [...],          # supported-семейства без записи
            "unsupported_norms": [...],      # семейство не распознано
            "paragraphs_to_verify": [...],   # цитаты для LLM
            "meta": {...},
        }
    """
    from norms.external_provider import resolve_norm_status

    pdb = load_norms_paragraphs()
    known_paragraphs = pdb.get("paragraphs", {})
    trusted_skipped = 0
    legacy_ignored = 0

    now = datetime.now()

    checks: list[dict] = []
    missing_norms: list[dict] = []
    unsupported_norms: list[dict] = []
    paragraphs_to_verify: list[dict] = []

    stats = {
        "total": 0,
        "authoritative": 0,
        "missing": 0,
        "unsupported": 0,
        "active": 0,
        "outdated_edition": 0,
        "replaced": 0,
        "cancelled": 0,
        "not_found": 0,
        "unknown": 0,
    }

    for norm_raw, info in norms_data.get("norms", {}).items():
        stats["total"] += 1
        norm_key = normalize_doc_number(norm_raw)
        cited_as = info["cited_as"][0] if info.get("cited_as") else norm_raw
        affected = info.get("affected_findings", [])

        resolved = resolve_norm_status(norm_raw)
        check_entry = _build_check_from_resolved(
            norm_key=norm_key,
            cited_as=cited_as,
            affected=affected,
            resolved=resolved,
        )
        checks.append(check_entry)
        status = check_entry["status"]
        if status in stats:
            stats[status] += 1

        via = check_entry["verified_via"]
        if via == "norms_authoritative":
            stats["authoritative"] += 1
        elif via == "norms_missing":
            stats["missing"] += 1
            missing_norms.append({
                "norm": norm_raw,
                "norm_key": norm_key,
                "cited_as": cited_as,
                "affected_findings": affected,
                "detected_family": resolved.get("detected_family"),
                "supported_family": bool(resolved.get("supported_family")),
                "resolution_reason": resolved.get("resolution_reason"),
                "action": "add_document_to_vault",
            })
        elif via == "norms_unsupported":
            stats["unsupported"] += 1
            unsupported_norms.append({
                "norm": norm_raw,
                "norm_key": norm_key,
                "cited_as": cited_as,
                "affected_findings": affected,
                "detected_family": resolved.get("detected_family"),
                "supported_family": False,
                "resolution_reason": resolved.get("resolution_reason"),
                "action": "review_family_support",
            })

        # Цитаты: жёсткий фильтр. Отправляем LLM на проверку только если
        #   (1) norma authoritative в Norms-main;
        #   (2) в Norms-main есть текст документа (иначе MCP не вернёт цитату);
        #   (3) trusted cache ещё не знает ответа.
        # Legacy-записи paragraph cache (source=websearch+webfetch и т.п.)
        # считаются недоверенными — их присутствие НЕ skip'ает LLM-проверку.
        if via != "norms_authoritative":
            continue
        if not check_entry.get("has_text"):
            continue
        for fid in affected:
            finding_norm = info.get("finding_norms", {}).get(fid, "")
            paragraph_key = normalize_paragraph_key(
                finding_norm.strip() if finding_norm else norm_key
            )
            cached_entry = known_paragraphs.get(paragraph_key)
            if cached_entry is not None and _is_trusted_paragraph_entry(cached_entry):
                trusted_skipped += 1
                continue
            if cached_entry is not None:
                # legacy-запись есть, но доверять ей не можем — пере-проверяем.
                legacy_ignored += 1
            paragraphs_to_verify.append({
                "finding_id": fid,
                "norm": norm_raw,
                "norm_key": norm_key,
                "paragraph_key": paragraph_key,
                "matched_code": resolved.get("matched_code"),
                "has_text": bool(resolved.get("has_text")),
            })

    meta = {
        "project_id": project_id,
        "check_date": now.isoformat(),
        "total_checked": stats["total"],
        "authoritative": stats["authoritative"],
        "missing": stats["missing"],
        "unsupported": stats["unsupported"],
        "paragraphs_trusted_skipped": trusted_skipped,
        "paragraphs_legacy_ignored": legacy_ignored,
        "policy_violations": [],
        "source": "norms_main_status_index",
        "results": {
            "active": stats["active"],
            "outdated_edition": stats["outdated_edition"],
            "replaced": stats["replaced"],
            "cancelled": stats["cancelled"],
            "not_found": stats["not_found"],
            "unknown": stats["unknown"],
        },
    }

    return {
        "checks": checks,
        "missing_norms": missing_norms,
        "unsupported_norms": unsupported_norms,
        "paragraphs_to_verify": paragraphs_to_verify,
        "meta": meta,
    }


# ─── Trusted paragraph cache ──────────────────────────────────────────────
# Легаси-кеш norms_paragraphs.json был наполнен WebSearch+WebFetch ответами,
# которые мы больше не считаем authoritative. Запись считается доверенной
# (= её можно использовать для пропуска LLM-верификации) ТОЛЬКО если у неё
# есть явный признак провенанса из нового MCP-флоу.

_TRUSTED_VERIFIED_VIA_PREFIXES = (
    "norms_mcp",            # из нового norm_verify промпта (verified_via="norms_mcp_paragraph")
    "norms_main_mcp",       # будущие вариации
    "norms_authoritative",  # теоретически — если писали checks
)
_TRUSTED_SOURCE_VALUES = {"norms_main_mcp", "norms_main"}


def _is_trusted_paragraph_entry(entry: dict) -> bool:
    """True, если запись paragraph cache получена через новый MCP-флоу.

    Легаси-записи (source="websearch+webfetch", verified_via="websearch" и
    подобные) считаются недоверенными — их наличие не должно пропускать
    LLM-верификацию. Это защита от дрейфа: старые кешированные цитаты могут
    быть неточными относительно текущего текста нормы в Norms-main.
    """
    if not isinstance(entry, dict):
        return False
    via = (entry.get("verified_via") or "").strip().lower()
    if via and any(via.startswith(p) for p in _TRUSTED_VERIFIED_VIA_PREFIXES):
        return True
    src = (entry.get("source") or "").strip().lower()
    if src in _TRUSTED_SOURCE_VALUES:
        return True
    return False


def _status_from_resolved(resolved: dict) -> str:
    """Свести результат resolve_norm_status к статусу замечания.

    Mapping согласно ТЗ:
        active            → active
        outdated_edition  → outdated_edition
        replaced          → replaced
        cancelled         → cancelled
        not_in_index      → not_found
        unsupported       → unknown
        прочее            → unknown
    """
    if resolved.get("found"):
        return resolved.get("status", "unknown")
    reason = resolved.get("resolution_reason")
    if reason == "not_in_index":
        return "not_found"
    if reason == "unsupported_family":
        return "unknown"
    return "unknown"


def _verified_via_from_resolved(resolved: dict) -> str:
    """Метка провайдера для check.verified_via."""
    if resolved.get("found"):
        return "norms_authoritative"
    if resolved.get("resolution_reason") == "not_in_index":
        return "norms_missing"
    if resolved.get("resolution_reason") == "unsupported_family":
        return "norms_unsupported"
    return "norms_missing"


def _build_check_from_resolved(
    norm_key: str,
    cited_as: str,
    affected: list[str],
    resolved: dict,
) -> dict:
    """Собрать запись check из результата resolve_norm_status()."""
    status = _status_from_resolved(resolved)
    via = _verified_via_from_resolved(resolved)

    # needs_revision: replaced/cancelled/outdated_edition обязательно.
    needs_revision = status in ("replaced", "cancelled", "outdated_edition")

    # edition_status — явное значение для каждой check-записи.
    # Правило: edition_status описывает редакцию документа. Для not_found
    # документа вообще нет в индексе, поэтому про его редакцию ничего не знаем
    # → "unknown" (а не "not_found", это путает валидатор).
    if status == "outdated_edition":
        edition_status = "outdated_edition"
    elif status in ("replaced", "cancelled"):
        edition_status = status
    elif status == "active":
        edition_status = "active"
    else:
        # not_found, unknown и прочее → редакция неизвестна
        edition_status = "unknown"

    if via == "norms_missing":
        details = (
            "Норма не найдена в индексе Norms-main — "
            "добавлена в missing_norms_queue для ручной обработки."
        )
    elif via == "norms_unsupported":
        details = (
            "Не удалось определить семейство нормы — требуется ревизия "
            "поддержки в Norms-main."
        )
    else:
        details = resolved.get("details") or ""

    return {
        "norm_as_cited": cited_as,
        "doc_number": resolved.get("matched_code") or norm_key,
        "query": resolved.get("query", cited_as),
        "status": status,
        "doc_status": resolved.get("doc_status"),
        "edition_status": edition_status,
        "current_version": resolved.get("current_version"),
        "replacement_doc": resolved.get("replacement_doc"),
        "source_url": resolved.get("source_url"),
        "details": details,
        "affected_findings": affected,
        "needs_revision": needs_revision,
        "verified_via": via,
        "authoritative": bool(resolved.get("authoritative")),
        "resolution_reason": resolved.get("resolution_reason"),
        "detected_family": resolved.get("detected_family"),
        "supported_family": bool(resolved.get("supported_family")),
        "has_text": bool(resolved.get("has_text")),
        "norms_title": resolved.get("title"),
        "norms_file": resolved.get("file"),
    }


# ─── Missing norms queue ──────────────────────────────────────────────────

def build_missing_norms_queue(
    det_result: dict,
    project_id: str = "",
) -> dict:
    """Собрать структуру очереди пропусков для записи в _output.

    Args:
        det_result: результат generate_deterministic_checks().
        project_id: идентификатор проекта для трассировки.

    Returns:
        {
            "meta": {...},
            "queue": [{norm, reason, action, affected_findings, ...}],
            "report": {authoritative, missing, unsupported, total},
        }
    """
    now_iso = datetime.now().isoformat()
    missing = det_result.get("missing_norms", [])
    unsupported = det_result.get("unsupported_norms", [])
    meta = det_result.get("meta", {}) or {}

    queue = []
    for item in missing:
        queue.append({
            "norm": item.get("norm"),
            "norm_key": item.get("norm_key"),
            "cited_as": item.get("cited_as"),
            "detected_family": item.get("detected_family"),
            "supported_family": True,
            "resolution_reason": item.get("resolution_reason") or "not_in_index",
            "action": item.get("action") or "add_document_to_vault",
            "affected_findings": item.get("affected_findings") or [],
        })
    for item in unsupported:
        queue.append({
            "norm": item.get("norm"),
            "norm_key": item.get("norm_key"),
            "cited_as": item.get("cited_as"),
            "detected_family": item.get("detected_family"),
            "supported_family": False,
            "resolution_reason": item.get("resolution_reason") or "unsupported_family",
            "action": item.get("action") or "review_family_support",
            "affected_findings": item.get("affected_findings") or [],
        })

    report = {
        "total_checked": meta.get("total_checked", 0),
        "authoritative": meta.get("authoritative", 0),
        "missing": len(missing),
        "unsupported": len(unsupported),
        "queue_size": len(queue),
    }

    return {
        "meta": {
            "project_id": project_id,
            "generated_at": now_iso,
            "source": "norms_main_status_index",
        },
        "queue": queue,
        "report": report,
    }


def render_missing_norms_queue_md(queue_data: dict) -> str:
    """Сформировать markdown-сводку очереди пропусков."""
    meta = queue_data.get("meta", {})
    report = queue_data.get("report", {})
    queue = queue_data.get("queue", [])

    lines = [
        "# Missing norms queue",
        "",
        f"- Проект: `{meta.get('project_id', '?')}`",
        f"- Сгенерировано: `{meta.get('generated_at', '?')}`",
        f"- Источник: `{meta.get('source', '?')}`",
        "",
        "## Сводка",
        f"- Всего проверено: {report.get('total_checked', 0)}",
        f"- Authoritative (есть в Norms): {report.get('authoritative', 0)}",
        f"- Missing (нужно добавить в vault): {report.get('missing', 0)}",
        f"- Unsupported (семейство не поддержано): {report.get('unsupported', 0)}",
        "",
    ]

    if not queue:
        lines.append("Очередь пуста — все нормы покрыты Norms-main.")
        return "\n".join(lines)

    lines.append("## Позиции")
    for i, item in enumerate(queue, 1):
        lines.append(
            f"{i}. **{item.get('norm')}** "
            f"(семейство: {item.get('detected_family') or '—'}, "
            f"reason: `{item.get('resolution_reason')}`)"
        )
        lines.append(f"   - Action: `{item.get('action')}`")
        if item.get("affected_findings"):
            lines.append(
                f"   - Затронутые замечания: {', '.join(item['affected_findings'])}"
            )
    return "\n".join(lines)


def write_missing_norms_queue(
    output_dir: Path,
    det_result: dict,
    project_id: str = "",
) -> dict:
    """Записать очередь пропусков в _output проекта.

    Пишет три файла рядом:
        missing_norms_queue.json  — машиночитаемая очередь
        missing_norms_report.json — краткий сводный отчёт
        missing_norms_queue.md    — человекочитаемая сводка

    Файлы создаются всегда — даже если очередь пуста, чтобы трейсинг был
    полным. Возвращает краткую статистику для логгирования.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    queue_data = build_missing_norms_queue(det_result, project_id=project_id)

    (output_dir / "missing_norms_queue.json").write_text(
        json.dumps(queue_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "missing_norms_report.json").write_text(
        json.dumps({
            "meta": queue_data["meta"],
            "report": queue_data["report"],
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (output_dir / "missing_norms_queue.md").write_text(
        render_missing_norms_queue_md(queue_data),
        encoding="utf-8",
    )

    return queue_data["report"]


def merge_llm_norm_results(
    deterministic_path: Path,
    llm_results_path: Path,
) -> dict:
    """Слить paragraph_checks от LLM в детерминированный norm_checks.json.

    После миграции на Norms-main LLM НЕ МОЖЕТ менять статус документа —
    статусы authoritative из status_index.json. Любые checks[] из LLM-файла
    игнорируются (записываем их количество в meta для трассировки). Берём
    только paragraph_checks[] — верификацию цитат пунктов через MCP.

    Returns: статистика слияния.
    """
    with open(deterministic_path, "r", encoding="utf-8") as f:
        det_data = json.load(f)
    with open(llm_results_path, "r", encoding="utf-8") as f:
        llm_data = json.load(f)

    # Статусы не перезаписываются — только фиксируем сколько пришло.
    ignored_status_attempts = len(llm_data.get("checks", []) or [])

    paragraph_checks = llm_data.get("paragraph_checks", []) or []

    final_checks = list(det_data.get("checks", []))
    meta = det_data.get("meta", {}) or {}
    meta["paragraph_checks_from_llm"] = len(paragraph_checks)
    meta["ignored_llm_status_attempts"] = ignored_status_attempts
    meta["llm_may_change_status"] = False
    # Пересчитать results — статусы не поменялись, но сохраняем инвариант.
    by_status: dict[str, int] = {}
    for c in final_checks:
        s = c.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1
    meta["results"] = by_status

    final = {
        "meta": meta,
        "checks": final_checks,
        "paragraph_checks": paragraph_checks,
    }

    with open(deterministic_path, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)

    # norms_db.json больше не authoritative — Norms-main это делает.
    # Но paragraph cache пополняем verified-цитатами.
    merge_stats = merge_paragraph_checks(
        paragraph_checks, project_id=meta.get("project_id", "unknown"),
    )

    return {
        "checks_updated_from_llm": 0,
        "ignored_llm_status_attempts": ignored_status_attempts,
        "paragraph_checks": len(paragraph_checks),
        "paragraph_cache_added": merge_stats.get("added", 0),
        "paragraph_cache_updated": merge_stats.get("updated", 0),
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
    paragraphs_to_verify: list[dict],
    findings_path: Path | None = None,
    *_legacy_positional,
) -> str:
    """Форматировать LLM-работу для шаблона norm_verify.

    После миграции на Norms-main LLM отвечает ТОЛЬКО за верификацию цитат
    конкретных пунктов через MCP (`mcp__norms__get_paragraph_json`). Статусы
    документов уже определены Python детерминированно, WebSearch запрещён.

    Для совместимости со старым вызовом (unknown_norms, paragraphs_to_verify,
    findings_path) первые позиционные аргументы-списки норм игнорируются —
    если первым аргументом пришёл список dict'ов без ключа "finding_id" и
    передан второй список, считаем, что вызов legacy.
    """
    # Backward-compat shim: старый контракт был
    # (unknown_norms, paragraphs_to_verify, findings_path).
    if _legacy_positional and isinstance(_legacy_positional[0], list):
        # сигнатура старая: paragraphs_to_verify лежит во втором аргументе
        paragraphs_to_verify = _legacy_positional[0]
        if len(_legacy_positional) > 1:
            findings_path = _legacy_positional[1]
    elif (
        isinstance(paragraphs_to_verify, list)
        and paragraphs_to_verify
        and isinstance(paragraphs_to_verify[0], dict)
        and "finding_id" not in paragraphs_to_verify[0]
        and isinstance(findings_path, list)
    ):
        paragraphs_to_verify = findings_path
        findings_path = None

    if not paragraphs_to_verify:
        return ""

    lines = [
        "## Верификация цитат пунктов (только MCP norms, без WebSearch)",
        "",
        "Статусы документов уже детерминированы Python из Norms-main. "
        "Для каждой позиции ниже проверь точный текст пункта ТОЛЬКО через "
        "MCP-инструменты: `mcp__norms__get_paragraph_json` для точного",
        "запроса по коду и номеру пункта, `mcp__norms__semantic_search_json` "
        "для поиска по смыслу. Интернет запрещён.",
        "",
    ]
    for i, pv in enumerate(paragraphs_to_verify, 1):
        matched = pv.get("matched_code") or pv.get("norm_key") or pv.get("norm")
        has_text = pv.get("has_text")
        has_text_hint = "" if has_text is None else (
            "  (текст документа доступен)"
            if has_text else "  (текст документа недоступен)"
        )
        lines.append(
            f"{i}. Замечание **{pv.get('finding_id')}**: норма "
            f"`{pv.get('norm')}` → matched_code `{matched}`{has_text_hint}"
        )
    return "\n".join(lines)


def validate_norm_checks(norm_checks_path: Path) -> dict:
    """Пост-валидация norm_checks.json — программный слой контроля.

    После миграции на Norms-main статусы authoritative, поэтому проверка
    stale-кеша больше не применима. Контракты, которые всё ещё имеют смысл:

    1. needs_revision=True для replaced/cancelled/outdated_edition.
    2. verified_via ∈ {norms_authoritative, norms_missing, norms_unsupported}.
       Любые legacy-значения (cache, cache_stale, pending_websearch, websearch)
       считаем нарушением политики и нормализуем.
    """
    if not norm_checks_path.exists():
        return {"valid": False, "error": "norm_checks.json не найден"}

    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks_data = json.load(f)

    checks = checks_data.get("checks", [])
    fixes: list[str] = []
    violations: list[str] = []

    legacy_via_map = {
        "cache": "norms_authoritative",
        "cache_stale": "norms_missing",
        "pending_websearch": "norms_missing",
        "websearch": "norms_missing",
        "deterministic": "norms_authoritative",
    }

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

        # Правило 2: verified_via должен быть из разрешённого набора.
        via = check.get("verified_via", "")
        if via in legacy_via_map:
            new_via = legacy_via_map[via]
            violations.append(
                f"{doc}: legacy verified_via='{via}' заменён на '{new_via}'"
            )
            check["verified_via"] = new_via
            check["_policy_violation"] = "legacy_verified_via"

        # Правило 3: outdated_edition не должен схлопываться в active
        if status == "outdated_edition" and not check.get("needs_revision", False):
            check["needs_revision"] = True
            fixes.append(f"{doc}: outdated_edition принудительно needs_revision=True")

    meta = checks_data.get("meta", {})
    meta["policy_violations"] = violations
    checks_data["meta"] = meta

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
                "stale_after_days": 180,
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
        norm = pc.get("norm", "")
        if not norm:
            continue

        key = normalize_paragraph_key(norm.strip())
        verified = pc.get("paragraph_verified", False)
        actual_quote = pc.get("actual_quote")

        if verified and actual_quote:
            # Positive result — сохраняем/обновляем цитату
            existing = pdb["paragraphs"].get(key)
            if existing and existing.get("quote") == actual_quote:
                continue
            pdb["paragraphs"][key] = {
                "norm": key,
                "quote": actual_quote,
                "verified": True,
                "verified_at": datetime.now().isoformat(),
                "source_project": project_path.name,
            }
            count += 1
        elif not verified:
            # Negative result — кешируем чтобы не перепроверять
            existing = pdb["paragraphs"].get(key)
            # Не перезаписываем положительный результат отрицательным
            if existing and existing.get("verified"):
                continue
            pdb["paragraphs"][key] = {
                "norm": key,
                "quote": actual_quote,
                "verified": False,
                "mismatch_details": pc.get("mismatch_details", ""),
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
    stale_days = db.get("meta", {}).get("stale_after_days", 180)
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
    print(f"  Устаревших (>{db['meta'].get('stale_after_days', 180)} дн): {len(stale)}")
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


# ═══════════════════════════════════════════════════════════════════════════════
# NORM CONTRACT — классификация, обогащение и нормализация norm-полей в findings
# (бывший norm_contract.py)
# ═══════════════════════════════════════════════════════════════════════════════

import logging as _logging

_nc_logger = _logging.getLogger("norm_contract")


# ─── Norm status classification ────────────────────────────────────────────

def classify_norm_status(finding: dict) -> str:
    """Классифицировать статус нормативной ссылки finding.

    Returns:
        "exact_quote"          — есть норма + точная цитата + высокая уверенность
        "paraphrased"          — есть норма + приблизительная цитата
        "norm_detected_no_quote" — есть норма, но нет цитаты
        "no_norm_cited"        — нет нормативной ссылки
        "not_found"            — норма указана, но не найдена при верификации
        "invalid_reference"    — норма заменена/отменена
    """
    norm = (finding.get("norm") or "").strip()
    norm_quote = finding.get("norm_quote")
    # Поля от norm verification (если обогащены)
    verification = finding.get("norm_verification", {})
    check_status = verification.get("status", "")

    if not norm:
        return "no_norm_cited"

    if check_status in ("replaced", "cancelled"):
        return "invalid_reference"
    if check_status == "not_found":
        return "not_found"

    if norm_quote and isinstance(norm_quote, str) and len(norm_quote) > 10:
        if verification.get("paragraph_verified"):
            return "exact_quote"
        return "paraphrased"

    return "norm_detected_no_quote"


def classify_norm_quote_status(finding: dict) -> str:
    """Классифицировать статус цитаты.

    Returns:
        "exact"       — цитата подтверждена верификацией (paragraph_verified)
        "approximate" — цитата есть, но не подтверждена
        "missing"     — цитата отсутствует
    """
    norm_quote = finding.get("norm_quote")
    verification = finding.get("norm_verification", {})

    if not norm_quote or not isinstance(norm_quote, str) or len(norm_quote) < 5:
        return "missing"
    if verification.get("paragraph_verified"):
        return "exact"
    return "approximate"


def compute_norm_confidence(finding: dict) -> float:
    """Deprecated: norm_confidence больше не используется как критерий решения.

    Оставлена для backward-compatibility. Возвращает 1.0 для всех findings с нормой.
    """
    norm = (finding.get("norm") or "").strip()
    if not norm:
        return 0.0
    return 1.0


# ─── Enrichment: findings <- norm_checks ──────────────────────────────────

def enrich_findings_from_norm_checks(
    findings: list[dict],
    norm_checks: dict,
) -> dict:
    """Обогатить findings данными из norm_checks.json.

    Добавляет/обновляет в каждом finding:
    - norm_verification: {status, edition_status, verified_via, ...}
    - norm_status: classification
    - norm_quote_status: classification
    - norm_quote: actual_quote если найдена и лучше текущей

    Returns: статистика обогащения.
    """
    checks = norm_checks.get("checks", [])
    paragraph_checks = norm_checks.get("paragraph_checks", [])

    # Индекс: doc_number → check
    check_index = {}
    for check in checks:
        doc = check.get("doc_number", "")
        if doc:
            check_index[doc] = check
        # Также индексируем по cited_as для fuzzy matching
        cited = check.get("norm_as_cited", "")
        if cited and cited != doc:
            check_index[cited] = check

    # Индекс: finding_id → paragraph_check
    para_index = {}
    for pc in paragraph_checks:
        fid = pc.get("finding_id", "")
        if fid:
            if fid not in para_index:
                para_index[fid] = []
            para_index[fid].append(pc)

    stats = {
        "total": len(findings),
        "enriched_verification": 0,
        "enriched_quote": 0,
        "status_upgrade": 0,
    }

    for finding in findings:
        fid = finding.get("id", "")
        norm_raw = (finding.get("norm") or "").strip()

        # Найти matching check
        matched_check = None
        if norm_raw:
            # Пробуем по doc_number (нормализованному)
            for doc_key, check in check_index.items():
                if doc_key in norm_raw or norm_raw.startswith(doc_key):
                    matched_check = check
                    break
            # Fallback: по affected_findings
            if not matched_check:
                for check in checks:
                    if fid in check.get("affected_findings", []):
                        matched_check = check
                        break

        # Обогащаем norm_verification
        if matched_check:
            verification = {
                "status": matched_check.get("status", "unknown"),
                "edition_status": matched_check.get("edition_status", "unknown"),
                "verified_via": matched_check.get("verified_via", "unknown"),
                "needs_revision": matched_check.get("needs_revision", False),
                "current_version": matched_check.get("current_version"),
                "replacement_doc": matched_check.get("replacement_doc"),
            }
            finding["norm_verification"] = verification
            stats["enriched_verification"] += 1

        # Обогащаем paragraph data
        pcs = para_index.get(fid, [])
        for pc in pcs:
            actual_quote = pc.get("actual_quote")
            verified = pc.get("paragraph_verified", False)

            if actual_quote and isinstance(actual_quote, str) and len(actual_quote) > 10:
                current_quote = finding.get("norm_quote")
                # Обновляем если текущая цитата отсутствует или хуже
                if not current_quote or not isinstance(current_quote, str):
                    finding["norm_quote"] = actual_quote
                    finding["norm_source"] = "paragraph_check"
                    stats["enriched_quote"] += 1

            if verified:
                finding.setdefault("norm_verification", {})["paragraph_verified"] = True

        # Классифицируем
        finding["norm_status"] = classify_norm_status(finding)
        finding["norm_quote_status"] = classify_norm_quote_status(finding)
        finding["norm_policy_class"] = compute_norm_policy_class(finding)

    return stats


# ─── Selective Critic calibration ──────────────────────────────────────────

# Deprecated: NORM_CONFIDENCE_THRESHOLDS больше не используются.
# Оставлены для backward-compatibility импортов.
NORM_CONFIDENCE_THRESHOLDS = {}

DEFAULT_NORM_CONFIDENCE_THRESHOLD = 0.0


# ─── Norm policy class ────────────────────────────────────────────────────

def compute_norm_policy_class(finding: dict) -> str:
    """Вычислить norm_policy_class по severity.

    Returns:
        "required"    — КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ: норма обязательна
        "recommended" — ЭКСПЛУАТАЦИОННОЕ: норма желательна
        "optional"    — РЕКОМЕНДАТЕЛЬНОЕ/ПРОВЕРИТЬ ПО СМЕЖНЫМ: допустимо без нормы
    """
    severity = finding.get("severity", "")
    if severity in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"):
        return "required"
    if severity == "ЭКСПЛУАТАЦИОННОЕ":
        return "recommended"
    return "optional"


def should_review_norm(finding: dict) -> bool:
    """Определить, нужна ли проверка нормативной ссылки finding.

    Учитывает norm_status и norm_policy_class. Все цитаты проверяются
    на этапе 04 независимо от самооценки LLM.
    """
    norm_status = finding.get("norm_status") or classify_norm_status(finding)

    # Нет нормы → решение зависит от policy class
    if norm_status == "no_norm_cited":
        policy = finding.get("norm_policy_class") or compute_norm_policy_class(finding)
        return policy == "required"

    # Невалидная ссылка → обязательно проверять
    if norm_status in ("not_found", "invalid_reference"):
        return True

    # Непроверенные цитаты → отправить на проверку
    if norm_status in ("paraphrased", "norm_detected_no_quote"):
        return True

    return False
