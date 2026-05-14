"""
REST API для контроля ранее согласованных замечаний (migrated findings).

Эндпоинты ручные: auto-run в pipeline по умолчанию НЕ включён, чтобы не
менять существующий полный аудит.

POST /api/projects/{project_id}/versions/{version_id}/migrated-findings/check
GET  /api/projects/{project_id}/versions/{version_id}/migrated-findings/report
"""
from fastapi import APIRouter, HTTPException

from backend.app.services.common import project_service, version_service
from backend.app.services.findings import migrated_findings_service as svc

router = APIRouter(
    prefix="/api/projects",
    tags=["migrated_findings"],
)


def _validate_project_and_version(project_id: str, version_id: str):
    """Проверить, что проект существует и version_id валиден."""
    proj_dir = project_service.resolve_project_dir(project_id)
    if not proj_dir.exists():
        raise HTTPException(404, f"Проект '{project_id}' не найден")
    try:
        version_service.get_version_entry(proj_dir, project_id, version_id)
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))


@router.post("/{project_id:path}/versions/{version_id}/migrated-findings/check")
async def check_migrated_findings(project_id: str, version_id: str):
    """Запустить deterministic recheck замечаний из предыдущей проверенной версии.

    - Только для V2+ (для V1 нет более ранней версии).
    - Читает accepted findings из `expert_review.json` старой версии.
    - Сравнивает с уже найденными V_current findings (norms / title / page).
    - Записывает `_versions/{version_id}/_output/migrated_findings_report.json`.
    - Добавляет в `03_findings.json` только `still_relevant`; для
      `duplicate_of_new_finding` обогащает существующий finding origin-метаданными.
    """
    _validate_project_and_version(project_id, version_id)
    try:
        result = svc.run_migrated_findings_check(project_id, version_id)
    except svc.MigratedFindingsError as e:
        raise HTTPException(400, str(e))
    except version_service.VersionNotFoundError as e:
        raise HTTPException(404, str(e))
    return result


@router.get("/{project_id:path}/versions/{version_id}/migrated-findings/report")
async def get_migrated_findings_report(project_id: str, version_id: str):
    """Прочитать сохранённый отчёт. Возвращает 200 + `exists: false`, если
    отчёт ещё не создан (а не 404 — отчёт ещё нечего показывать)."""
    _validate_project_and_version(project_id, version_id)
    report = svc.read_migrated_findings_report(project_id, version_id)
    if report is None:
        return {
            "project_id": project_id,
            "version_id": version_id,
            "exists": False,
            "report": None,
        }
    return {
        "project_id": project_id,
        "version_id": version_id,
        "exists": True,
        "report": report,
    }
