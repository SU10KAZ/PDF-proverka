"""Виды кандидатов и проверяемые основания новой гипотезы."""

REPLICATION_KIND = "replicate_accepted_optimization"
DISCOVERY_KIND = "type_size_reduction_opportunity"
SUPPORTED_KINDS = (REPLICATION_KIND, DISCOVERY_KIND)


def discovery_evidence_problems(dossier: dict, assessment: dict) -> list[str]:
    """Гипотеза требует конкретного действия и адресуемого сравнения вариантов."""
    problems = []
    if not str(assessment.get("proposed_action") or "").strip():
        problems.append("Не указано конкретное изменение для целевого проекта.")
    rows = {
        str(row.get("row_id")): (target.get("project_id"), row)
        for target in dossier.get("targets") or []
        for row in target.get("rows") or [] if row.get("row_id")
    }
    refs = set(str(ref) for ref in assessment.get("comparison_row_ids") or [])
    if not refs or not refs.issubset(rows):
        problems.append("Нет допустимых ссылок на сравниваемые строки досье.")
    compared = [rows[ref] for ref in refs if ref in rows]
    variants = {str(row.get("type_mark") or row.get("designation") or row.get("name") or "").strip()
                for _, row in compared} - {""}
    if len(variants) < 2 or len({pid for pid, _ in compared}) < 2:
        problems.append("Не приведено сравнение разных исполнений из разных проектов.")
    if not any(pid == assessment.get("project_id") for pid, _ in compared):
        problems.append("Сравнение не включает целевой проект.")
    return problems
