"""
verify_norms.py — Извлечение нормативных ссылок из 03_findings.json.
Подготовка данных для верификации через Claude CLI + WebSearch.
Может вызываться как standalone или из webapp pipeline.

Использование:
    python verify_norms.py projects/<name>
    python verify_norms.py projects/<name> --extract-only   # только извлечь, не запускать Claude
"""
import json
import re
import sys
from pathlib import Path
from datetime import datetime


# ─── Паттерны для извлечения нормативных ссылок ───

NORM_PATTERNS = [
    # СП 256.1325800.2016 (с вариациями)
    r'СП\s+[\d\.]+\.\d{7}\.\d{4}',
    # СП короткие: СП 6.13130.2021
    r'СП\s+\d+\.\d+\.\d{4}',
    # ГОСТ Р 50345-2010, ГОСТ 31996-2012, ГОСТ IEC 61008-1-2020
    r'ГОСТ\s+(?:Р\s+)?(?:IEC\s+)?(?:МЭК\s+)?[\d\.\-]+(?:\-\d{4})?',
    # ПУЭ-7, ПУЭ-6
    r'ПУЭ[\s\-]*[67]?',
    # СНиП
    r'СНиП\s+[\d\.\-\*]+',
    # ВСН
    r'ВСН\s+[\d\-]+',
    # Федеральные законы: ФЗ-384, ФЗ-123
    r'ФЗ[\s\-]*\d+',
    # ПП РФ №815
    r'ПП\s+РФ\s+[№]?\s*\d+',
    # СО 153-34.21.122-2003
    r'СО\s+[\d\.\-]+',
]

# Объединяем в один паттерн
NORM_REGEX = re.compile('|'.join(f'({p})' for p in NORM_PATTERNS), re.IGNORECASE)


def extract_norms_from_text(text: str) -> list[str]:
    """Извлечь нормативные ссылки из текста."""
    matches = NORM_REGEX.findall(text)
    # findall с группами возвращает кортежи — берём непустые
    norms = set()
    for match_tuple in matches:
        for m in match_tuple:
            if m.strip():
                norms.add(m.strip())
    return sorted(norms)


def extract_norms_from_findings(findings_path: Path) -> dict:
    """
    Прочитать 03_findings.json, извлечь все нормативные ссылки.

    Returns:
        {
            "norms": {
                "СП 256.1325800.2016": {
                    "cited_as": ["СП 256.1325800.2016, п.14.9", ...],
                    "affected_findings": ["F-001", "F-003"],
                    "contexts": ["ТТ ВП-ППУ: ...", "СП 256 указан в редакции..."]
                }
            },
            "total_findings": N,
            "total_unique_norms": N
        }
    """
    with open(findings_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    findings = data.get("findings", [])
    norms_map = {}

    for finding in findings:
        fid = finding.get("id", "?")
        norm_field = finding.get("norm") or ""
        problem_field = finding.get("finding") or finding.get("problem") or ""
        recommendation = finding.get("recommendation") or finding.get("solution") or ""

        # Извлекаем нормы из поля norm
        found_norms = extract_norms_from_text(norm_field)

        # Также ищем в тексте замечания и рекомендации
        found_norms += extract_norms_from_text(problem_field)
        found_norms += extract_norms_from_text(recommendation)

        for norm in found_norms:
            # Нормализуем ключ (убираем лишние пробелы)
            key = re.sub(r'\s+', ' ', norm).strip()

            if key not in norms_map:
                norms_map[key] = {
                    "cited_as": [],
                    "affected_findings": [],
                    "contexts": [],
                }

            if norm_field and norm_field not in norms_map[key]["cited_as"]:
                norms_map[key]["cited_as"].append(norm_field)

            if fid not in norms_map[key]["affected_findings"]:
                norms_map[key]["affected_findings"].append(fid)

            ctx = problem_field[:200] if problem_field else ""
            if ctx and ctx not in norms_map[key]["contexts"]:
                norms_map[key]["contexts"].append(ctx)

    return {
        "norms": norms_map,
        "total_findings": len(findings),
        "total_unique_norms": len(norms_map),
    }


def format_norms_for_template(norms_data: dict) -> str:
    """Форматировать список норм для подстановки в шаблон Claude."""
    lines = []
    for i, (norm, info) in enumerate(norms_data["norms"].items(), 1):
        findings_str = ", ".join(info["affected_findings"])
        cited = info["cited_as"][0] if info["cited_as"] else norm
        lines.append(
            f"{i}. **{norm}**\n"
            f"   - Как указано в проекте: `{cited}`\n"
            f"   - Затронутые замечания: {findings_str}"
        )
    return "\n".join(lines)


def format_findings_to_fix(norm_checks_path: Path, findings_path: Path) -> str:
    """
    Прочитать norm_checks.json, определить какие замечания нужно пересмотреть.
    Вернуть текст для подстановки в norm_fix_task.md.
    """
    with open(norm_checks_path, "r", encoding="utf-8") as f:
        checks = json.load(f)

    with open(findings_path, "r", encoding="utf-8") as f:
        findings_data = json.load(f)

    findings_map = {f["id"]: f for f in findings_data.get("findings", [])}
    lines = []

    for check in checks.get("checks", []):
        if not check.get("needs_revision", False):
            continue

        for fid in check.get("affected_findings", []):
            finding = findings_map.get(fid)
            if not finding:
                continue

            lines.append(
                f"### {fid}\n"
                f"- **Текущая норма:** `{finding.get('norm', '?')}`\n"
                f"- **Проблема:** {check.get('status', '?')} — {check.get('details', '')}\n"
                f"- **Актуальный документ:** `{check.get('current_version', '?')}`\n"
                f"- **Замена:** `{check.get('replacement_doc') or 'нет'}`\n"
            )

    if not lines:
        return "Все нормы актуальны. Пересмотр не требуется."

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Использование: python verify_norms.py projects/<name> [--extract-only]")
        sys.exit(1)

    project_dir = Path(sys.argv[1])
    extract_only = "--extract-only" in sys.argv

    if not project_dir.is_absolute():
        project_dir = Path.cwd() / project_dir

    output_dir = project_dir / "_output"
    findings_path = output_dir / "03_findings.json"

    if not findings_path.exists():
        print(f"ОШИБКА: Файл {findings_path} не найден. Сначала выполните аудит (этап 03).")
        sys.exit(1)

    # Извлекаем нормы
    print(f"Извлечение нормативных ссылок из {findings_path.name}...")
    norms_data = extract_norms_from_findings(findings_path)

    print(f"Найдено замечаний: {norms_data['total_findings']}")
    print(f"Уникальных нормативных ссылок: {norms_data['total_unique_norms']}")

    for norm, info in norms_data["norms"].items():
        findings_str = ", ".join(info["affected_findings"])
        print(f"  - {norm} (в замечаниях: {findings_str})")

    # Сохраняем извлечённые нормы
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

    # Формируем текст для шаблона
    norms_list_text = format_norms_for_template(norms_data)
    print(f"\nСписок норм для верификации:\n{norms_list_text}")
    print(f"\nДля запуска верификации через Claude CLI используйте webapp или pipeline.")


if __name__ == "__main__":
    main()
