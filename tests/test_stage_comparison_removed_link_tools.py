"""Контракт: удалённые инструменты сравнения не доступны через API."""

from backend.app.api.routers.stage_comparison import router


def _methods_by_path() -> dict[str, set[str]]:
    return {
        route.path: set(route.methods or set())
        for route in router.routes
        if hasattr(route, "path")
    }


def test_removed_link_and_sheet_tools_have_no_routes():
    routes = _methods_by_path()
    removed_paths = {
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/auto-link",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/save-template",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/template-status",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/apply-template",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/clear-template",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/analysis-mode",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-alignment/suggest",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-alignment/suggest-by-stamp",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/page-alignment/auto-match-apply",
        "/api/stage-comparison/sessions/{session_id}/page-alignment/auto-match",
        "/api/stage-comparison/pipeline-v2/{session_id}/block-link-preview",
        "/api/stage-comparison/pipeline-v2/{session_id}/entity-alignment-preview",
        # Pipeline V2 удалён целиком (UI + endpoints + backend-модули).
        "/api/stage-comparison/pipeline-v2/{session_id}/ui-payload",
        "/api/stage-comparison/pipeline-v2/{session_id}/pairs/{pair_id}/run",
        # Диагностические этапы новой цепочки убраны вместе с кнопками.
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/sheet-identity",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/sheet-alignment",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/change-regions-pilot",
        "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/change-detection",
    }
    assert removed_paths.isdisjoint(routes)

    removed_fragments = {
        "auto-match",
        "suggest-by-stamp",
        "analysis-mode",
        "template-status",
        "save-template",
        "apply-template",
        "clear-template",
        "block-link-preview",
        "entity-alignment-preview",
        "entity-mapping-overrides",
        "link-validation",
        "exclusion-preview",
        "exclusion-review-overrides",
        "controlled-enforce",
        "enrichment-selection-observe",
        "pipeline-v2",
        "sheet-identity",
        "sheet-alignment",
        "change-regions-pilot",
        "change-detection",
    }
    assert not {
        path
        for path in routes
        if any(fragment in path for fragment in removed_fragments)
    }

    links = "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/links"
    assert "POST" not in routes.get(links, set())


def test_md_enrichment_remains_available():
    routes = _methods_by_path()
    md = "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/md-enrichment"
    assert "GET" in routes[md]
