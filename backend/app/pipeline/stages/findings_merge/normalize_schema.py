"""Нормализация схемы 03_findings.json сразу после свода.

Зачем. Свод — агентный вызов модели, и она изредка переименовывает поля против
схемы промпта. Реальный случай (ЭО1-3, 04.08.2026): все 40 замечаний вышли с
`norm_reference` вместо `norm`. Формально файл валиден, конвейер отчитался «OK»,
но дальше:

  • этап верификации норм ищет `norm` → не увидел НИ ОДНОЙ ссылки и прошёл
    мимо, не создав даже `03a_norms_verified.json`;
  • в отчёте и БЗ у проекта нормативные ссылки «пропали», хотя модель их дала.

Молчаливая потеря целого слоя данных — хуже явной ошибки, поэтому переименования
чиним детерминированно и о каждом факте сообщаем в лог.

Список псевдонимов намеренно узкий: только те, что реально встречались или
однозначно означают то же самое. Неизвестные лишние поля не трогаем — они
безвредны, а угадывание смысла чужого поля опаснее его игнорирования.
"""
from __future__ import annotations

import json
from pathlib import Path

# псевдоним → каноническое имя поля из схемы промпта
FIELD_ALIASES: dict[str, str] = {
    "norm_reference": "norm",
    "normative_reference": "norm",
    "norm_ref": "norm",
    "norm_citation": "norm_quote",
    "quote": "norm_quote",
    "source_findings": "source_finding_ids",
    "source_ids": "source_finding_ids",
    "related_blocks": "related_block_ids",
    "source_blocks": "source_block_ids",
}


def normalize_findings_schema(output_dir: str | Path) -> dict:
    """Привести имена полей замечаний к канону. Возвращает отчёт о правках.

    Правило разрешения конфликта: каноническое поле, уже заполненное моделью,
    приоритетнее псевдонима — псевдоним тогда просто отбрасывается. Иначе
    «norm»: null рядом с «norm_reference»: "СП 60..." затирал бы живое значение.
    """
    path = Path(output_dir) / "03_findings.json"
    report: dict = {"renamed": {}, "findings_changed": 0, "ok": True}
    if not path.exists():
        report["ok"] = False
        return report

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 — нормализация не должна ронять merge
        report["ok"] = False
        report["error"] = str(exc)
        return report

    findings = data.get("findings")
    if not isinstance(findings, list):
        return report

    changed_findings = 0
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        touched = False
        for alias, canonical in FIELD_ALIASES.items():
            if alias not in finding:
                continue
            value = finding.pop(alias)
            if finding.get(canonical) in (None, "", [], {}):
                finding[canonical] = value
                report["renamed"][alias] = report["renamed"].get(alias, 0) + 1
                touched = True
        if touched:
            changed_findings += 1

    if changed_findings:
        report["findings_changed"] = changed_findings
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(path)
    return report
