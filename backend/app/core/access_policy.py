"""Таблица политик маршрутов портала (план доступа, раздел 4–5).

Каждый маршрут приложения покрыт явным правилом:

* ``ALLOW`` — явный перечень (метод, шаблон пути) → политика для viewer:
  ``PUBLIC``, ``SELF``, ``COMMON``, ``VIEW:<раздел>``, ``DOWNLOAD:<раздел>``;
* ``ADMIN_PREFIXES`` — всё остальное в перечисленных префиксах — только admin.

``IMPORT_DECISIONS`` разрешает единственный POST импорта Excel активному
аккаунту с просмотром результатов аудита.

Маршрут вне обоих перечней тоже трактуется как admin (fail closed), а тест
``tests/test_access_policy_coverage.py`` падает, если у маршрута нет явного
правила — так новый роутер не может появиться «молча открытым».

Классифицировано по фактическому поведению, а не по методу: GET, который
пишет или запускает обработку (``matrix``, ``/sessions``, ``live-status``,
``/ai/settings`` …), остаётся admin; для viewer есть read-only двойники.

Проверка применяется только в режиме ``accounts`` (роль viewer существует
только там). В ``env_users`` и ``emergency_owner`` вошедший — сотрудник с
прежними полномочиями / владелец; при выключенной авторизации проверок нет.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException
from starlette.requests import HTTPConnection

from backend.app.core.access import SECTIONS, request_account

PUBLIC = "PUBLIC"
MACHINE = "MACHINE"
SELF = "SELF"
COMMON = "COMMON"
ADMIN = "ADMIN"
VIEW = "VIEW"
DOWNLOAD = "DOWNLOAD"
IMPORT_DECISIONS = "IMPORT_DECISIONS"
UPLOAD_PROJECTS = "UPLOAD_PROJECTS"


@dataclass(frozen=True)
class Policy:
    kind: str
    section: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.kind}:{self.section}" if self.section else self.kind


def _p(spec: str) -> Policy:
    kind, _, section = spec.partition(":")
    if section and section not in SECTIONS:
        raise ValueError(f"неизвестный раздел {section}")
    return Policy(kind, section or None)


_AR, _VC, _SC, _KB, _SCH = ("audit_results", "version_comparison", "stage_comparison",
                            "knowledge_base", "schedule")
VC = "/api/version-comparison/objects/{object_id}"
SCS = "/api/stage-comparison/sessions/{session_id}"
PP = SCS + "/pairs/{pair_id}"
CONS = PP + "/consolidated/{source_run_id}/{consolidator_run_id}"
HM = "/api/human-mapping/objects/{object_id}"
PCP = "/api/project-change-preview/objects/{object_id}"

_ALLOW_SPEC: dict[str, list[str]] = {
    IMPORT_DECISIONS: ["POST /api/knowledge-base/upload-excel"],
    UPLOAD_PROJECTS: ["POST /api/projects/upload-folder", "POST /api/projects/upload-folder/precheck"],
    PUBLIC: [
        "GET /login", "GET /register",
        "POST /api/auth/login", "POST /api/auth/register", "POST /api/auth/logout",
        "GET /api/auth/me", "GET /api/info", "GET /favicon.ico",
    ],
    SELF: [
        "GET /change-password", "POST /api/auth/password", "POST /api/auth/sessions/revoke",
    ],
    COMMON: [
        "GET /", "GET /human-mapping", "GET /human-mapping/",
        "GET /api/objects", "GET /api/objects/stats",
    ],
    f"{VIEW}:{_AR}": [
        "GET /api/projects", "GET /api/projects/disciplines", "GET /api/audit/disciplines",
        "GET /api/projects/{project_id:path}", "GET /api/projects/{project_id:path}/versions",
        "GET /api/projects/{project_id:path}/versions/{version_id}/files",
        "GET /api/projects/{project_id:path}/versions/{version_id}/migrated-findings/report",
        "GET /api/project-groups",
        "GET /api/findings/summary", "GET /api/findings/{project_id:path}",
        "GET /api/findings/{project_id:path}/finding/{finding_id}",
        "GET /api/findings/{project_id:path}/block-map",
        "GET /api/findings/{project_id:path}/textlayer-highlights-shadow",
        "GET /api/findings/{project_id:path}/kb-validation",
        "GET /api/tiles/{project_id:path}/blocks", "GET /api/tiles/{project_id:path}/blocks/analysis",
        "GET /api/tiles/{project_id:path}/blocks/image/{block_id}",
        "GET /api/tiles/{project_id:path}/blocks/region-image/{block_id}",
        "GET /api/document/{project_id:path}/pages", "GET /api/document/{project_id:path}/page/{page_num}",
        "GET /api/document/{project_id:path}/pdf",
        "GET /api/optimization/summary/all", "GET /api/optimization/{project_id:path}",
        "GET /api/optimization/{project_id:path}/block-map", "GET /api/optimization/{project_id:path}/status",
        "GET /api/optimization/section/{section_code:path}",
        "GET /api/optimization/section/{section_code}/replications",
        "GET /api/optimization/section/{section_code}/replications/{replication_id}",
        "GET /api/knowledge-base/expert-review/{project_id:path}",
    ],
    f"{DOWNLOAD}:{_AR}": [
        "GET /api/export/audit-package/{project_id:path}",
        "GET /api/discussions/{project_id:path}/resolved/excel",
    ],
    f"{VIEW}:{_VC}": [
        "GET /api/version-comparison/objects", f"GET {VC}/catalog", f"GET {VC}/overview",
        f"GET {VC}/projects/{{project_id}}/matrix-view",
        f"GET {VC}/sources/{{sha}}/pages/{{page}}/thumb", f"GET {VC}/sources/{{sha}}/pages/{{page}}/canvas",
        f"GET {VC}/sources/{{sha}}/pages/{{page}}/tile",
        f"GET {VC}/projects/{{project_id}}/runs", f"GET {VC}/runs/{{run_id}}", f"GET {VC}/runs/{{run_id}}/permit",
        f"GET {VC}/runs/{{run_id}}/reports", f"GET {VC}/runs/{{run_id}}/spec",
        f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}", f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}/preview.png",
        f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}/tile",
        f"GET {VC}/projects/{{project_id}}/series", f"GET {VC}/series/{{series_id}}",
        f"GET {VC}/runs/{{run_id}}/ai/jobs", f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}",
        f"GET {VC}/runs/{{run_id}}/ai/latest", f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/view",
        f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/sheets/{{spid}}",
        f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/sheets/{{spid}}/focus",
        f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/sheets/{{spid}}/ref-focus",
        f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}/changes", f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}/changes/focus",
        f"GET {VC}/series/{{series_id}}/ai-sheet-changes",
        f"GET {VC}/series/{{series_id}}/whole-specifications/status",
        f"GET {VC}/md-reports", f"GET {VC}/md-reports/{{report_id}}",
    ],
    f"{DOWNLOAD}:{_VC}": [
        f"GET {VC}/runs/{{run_id}}/reports/{{name}}", f"GET {VC}/series/{{series_id}}/report.xlsx",
        f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/report", f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/report.md",
        f"GET {VC}/runs/{{run_id}}/ai/jobs/{{job_id}}/view.md", f"GET {VC}/runs/{{run_id}}/sheets/{{spid}}/changes.md",
        f"GET {VC}/series/{{series_id}}/ai-sheet-changes/summary.md",
        f"GET {VC}/md-reports/{{report_id}}/report.pdf", f"GET {VC}/md-reports/{{report_id}}/report.json",
    ],
    f"{VIEW}:{_SC}": [
        "GET /api/stage-comparison/objects", "GET /api/stage-comparison/objects/{object_id}/document-groups",
        "GET /api/stage-comparison/objects/{object_id}/session",
        "GET /api/stage-comparison/objects/{object_id}/sessions/{session_id}/pair",
        f"GET {SCS}", f"GET {PP}",
        f"GET {PP}/production/state", f"GET {PP}/production/stages/{{stage_id}}/result",
        f"GET {PP}/production/text-evidence", f"GET {PP}/runs/{{run_id}}/project-changes",
        f"GET {PP}/consolidated", f"GET {CONS}", f"GET {CONS}/it-analysis", f"GET {CONS}/it-analysis/model-run",
        f"GET {PP}/consolidated/{{source_run_id}}/evidence/{{evidence_id}}/crop",
        f"GET {PP}/production/changes", f"GET {PP}/production/questions",
        f"GET {PP}/production/questions/{{domain_key}}", f"GET {PP}/production/preliminary-report",
        f"GET {PP}/production/human-review", f"GET {PP}/production/final-report",
        f"GET {PP}/production/changes/{{target_id}}/evidence",
        f"GET {PP}/sheet-matches", f"GET {PP}/sheet-link-repairs", f"GET {PP}/text-comparison",
        f"GET {PP}/text-exclusions", f"GET {PP}/graphic-comparison", f"GET {PP}/text-differences",
        f"GET {PP}/text-ai-review", f"GET {PP}/text-final-comparison", f"GET {PP}/text-change-summary",
        f"GET {PP}/high-level-project-changes", f"GET {PP}/text-entities",
        f"GET {PP}/page-thumb", f"GET {PP}/page-info", f"GET {PP}/text-search", f"GET {PP}/page-preview",
        f"GET {PP}/page-tile", f"GET {PP}/page-svg",
        f"GET {PP}/block-mapping/status", f"GET {PP}/block-mapping/runs/{{run_id}}/region-index",
        f"GET {PP}/block-mapping/runs/{{run_id}}/page-blocks",
        f"GET {PP}/block-mapping/runs/{{run_id}}/blocks/{{side}}/{{block_id}}",
        f"GET {PP}/block-mapping/source/page-blocks", f"GET {PP}/block-mapping/source/blocks/{{side}}/{{block_id}}",
        f"GET {PP}/block-mapping/runs/{{run_id}}/prelink-reconciliation", f"GET {PP}/prelinks",
        "GET /api/stage-comparison/objects/{object_id}/project-changes",
        "GET /api/stage-comparison/objects/{object_id}/project-changes/evidence/{evidence_id}/crop",
        "GET /api/stage-comparison/objects/{object_id}/project-changes/evidence/{evidence_id}/page-view",
        "GET /api/stage-comparison/objects/{object_id}/project-changes/evidence/{evidence_id}/page-image",
        f"GET {PCP}", f"GET {PCP}/manifest", f"GET {PCP}/report", f"GET {PCP}/decisions/{{decision_key}}",
        f"GET {PCP}/evidence/{{evidence_id}}/crop", f"GET {PCP}/viewer/pairs/{{pair_id}}",
        f"GET {PCP}/viewer/pairs/{{pair_id}}/page-info", f"GET {PCP}/viewer/pairs/{{pair_id}}/page-thumb",
        f"GET {PCP}/viewer/pairs/{{pair_id}}/page-preview", f"GET {PCP}/viewer/pairs/{{pair_id}}/page-tile",
        "GET /api/project-comparison/catalog", "GET /api/project-comparison/catalog/entries/{entry_id}",
        f"GET {HM}/comparisons/{{pair}}/ui-data", f"GET {HM}/pairs/{{pair}}/ui-data",
        f"GET {HM}/comparisons/{{pair}}/assets/{{asset_path:path}}", f"GET {HM}/pairs/{{pair}}/assets/{{asset_path:path}}",
        f"GET {HM}/assets/{{asset_path:path}}",
        f"GET {HM}/comparisons/{{pair}}/reviews", f"GET {HM}/pairs/{{pair}}/reviews",
        f"GET {HM}/comparisons/{{pair}}/block-links", f"GET {HM}/pairs/{{pair}}/block-links",
    ],
    f"{DOWNLOAD}:{_SC}": [
        f"GET {CONS}/report.xlsx", f"GET {CONS}/report.zip",
    ],
    f"{VIEW}:{_KB}": [
        "GET /api/knowledge-base/entries", "GET /api/knowledge-base/stats",
        "GET /api/knowledge-base/patterns", "GET /api/knowledge-base/missing-norms",
    ],
    f"{VIEW}:{_SCH}": [
        "GET /api/schedule", "GET /api/schedule/plan", "GET /api/schedule/reserve",
    ],
}

#: Префиксы, всё остальное в которых — только admin. Явный перечень: маршрут
#: вне ALLOW и вне этих префиксов роняет тест покрытия.
ADMIN_PREFIXES: tuple[str, ...] = (
    "/api/audit/", "/api/projects", "/api/project-groups", "/api/findings/", "/api/tiles/",
    "/api/document/", "/api/optimization/", "/api/discussions/", "/api/knowledge-base/",
    "/api/export/", "/api/usage/", "/api/objects", "/api/users", "/api/schedule/",
    "/api/critic-v2/", "/api/external-register/", "/api/stage-comparison/",
    "/api/version-comparison/", "/api/project-change-preview/", "/api/project-comparison/",
    "/api/human-mapping/", "/api/projects-v2-shadow/", "/api/action-log", "/api/workers",
    "/api/admin/", "/audit-workers", "/admin/", "/docs", "/redoc", "/openapi.json",
)

#: Свой контур аутентификации (bearer-токен исполнителя).
MACHINE_PREFIXES: tuple[str, ...] = ("/api/v1/worker/",)


def _build() -> dict[tuple[str, str], Policy]:
    out: dict[tuple[str, str], Policy] = {}
    for spec, entries in _ALLOW_SPEC.items():
        policy = _p(spec)
        for entry in entries:
            method, _, path = entry.partition(" ")
            key = (method, path)
            if key in out:
                raise ValueError(f"маршрут дважды в таблице политик: {entry}")
            out[key] = policy
    return out


ALLOW: dict[tuple[str, str], Policy] = _build()


def policy_for(method: str, path_template: str) -> Policy:
    explicit = ALLOW.get((method.upper(), path_template))
    if explicit is not None:
        return explicit
    if method.upper() == "HEAD":
        explicit = ALLOW.get(("GET", path_template))
        if explicit is not None:
            return explicit
    if path_template.startswith(MACHINE_PREFIXES):
        return Policy(MACHINE)
    return Policy(ADMIN)


def has_explicit_rule(method: str, path_template: str) -> bool:
    return ((method.upper(), path_template) in ALLOW
            or path_template.startswith(ADMIN_PREFIXES)
            or path_template.startswith(MACHINE_PREFIXES))


def _deny(code: str, message: str, **extra) -> HTTPException:
    return HTTPException(403, {"code": code, "message": message, **extra})


async def enforce_route_policy(conn: HTTPConnection) -> None:
    """Глобальная зависимость FastAPI: право на маршрут для вошедшего."""
    if conn.scope.get("type") != "http":
        return  # WebSocket проверяет свой обработчик (main._ws_account)
    route = conn.scope.get("route")
    template = getattr(route, "path", None) or conn.url.path
    method = conn.scope.get("method", "GET")
    policy = policy_for(method, template)
    if policy.kind in (PUBLIC, MACHINE):
        return
    ctx = request_account(conn)  # уже проверен PortalAuthMiddleware
    if ctx is None:
        raise HTTPException(401, {"code": "not_authenticated", "message": "Требуется вход."})
    _check_role(ctx, policy)
    # Объект проверяется ПОСЛЕ прав: иначе по 404/403 можно было бы узнавать,
    # какие объекты существуют, не имея доступа к разделу.
    from backend.app.core.object_scope import enforce_object_scope

    enforce_object_scope(conn)


def _check_role(ctx, policy: Policy) -> None:
    if not ctx.enforces_roles or ctx.is_admin:
        return
    if policy.kind in (SELF, COMMON):
        return
    if policy.kind == VIEW:
        if ctx.can_view(policy.section):
            return
        raise _deny("forbidden_section", "Нет доступа к разделу.", section=policy.section)
    if policy.kind == DOWNLOAD:
        if ctx.can_download(policy.section):
            return
        raise _deny("forbidden_download", "Нет права скачивания в разделе.", section=policy.section)
    if policy.kind == IMPORT_DECISIONS:
        if ctx.can_import_decisions:
            return
        raise _deny("forbidden_decision_import",
                    "Для загрузки решений нужен доступ к результатам аудита.")
    if policy.kind == UPLOAD_PROJECTS:
        if ctx.can_upload_projects:
            return
        raise _deny("forbidden_project_upload", "Администратор не разрешил загрузку проектов.")
    raise _deny("admin_only", "Действие доступно только администратору.")


__all__ = ["ADMIN_PREFIXES", "ALLOW", "MACHINE_PREFIXES", "Policy", "enforce_route_policy",
           "has_explicit_rule", "policy_for"]
