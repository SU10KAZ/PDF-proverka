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
    }
    assert not {
        path
        for path in routes
        if any(fragment in path for fragment in removed_fragments)
    }

    links = "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/links"
    assert "POST" not in routes.get(links, set())


def test_md_enrichment_and_pipeline_main_screen_remain_available():
    routes = _methods_by_path()
    md = "/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/md-enrichment"
    payload = "/api/stage-comparison/pipeline-v2/{session_id}/ui-payload"
    run = "/api/stage-comparison/pipeline-v2/{session_id}/pairs/{pair_id}/run"
    assert "GET" in routes[md]
    assert "GET" in routes[payload]
    assert "POST" in routes[run]
