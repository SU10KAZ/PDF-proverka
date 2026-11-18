"""Read-only catalog of accepted, frozen Project Comparison results (0 model calls)."""
import re

from fastapi import APIRouter, HTTPException, Query

from backend.app.services.project_change_catalog import catalog

router = APIRouter(prefix='/api/project-comparison/catalog', tags=['Project Comparison catalog'])
_ENTRY_ID = re.compile(r'pcc1_[0-9a-f]{32}')
# Версия сравнения стадий объекта; с object_id без версии — Версия 1, как до
# появления версий (вкладки, открытые до выкатки, видят прежний список).
_LEGACY_VERSION = 'cv001'


def _version(object_id: str | None, comparison_version: str | None) -> str | None:
    if not object_id:
        return None
    return comparison_version or _LEGACY_VERSION


@router.get('')
def list_catalog(diagnostics: bool = Query(default=False),
                 object_id: str | None = Query(default=None, min_length=1, max_length=128,
                                             pattern=r'^[A-Za-z0-9_-]+$'),
                 comparison_version: str | None = Query(default=None, pattern=r'^cv\d{3}$')):
    # Diagnostics (failed/partial runs, no cards) are returned only on request.
    return catalog.cached_catalog(include_diagnostics=diagnostics, object_id=object_id,
                                  comparison_version=_version(object_id, comparison_version))


@router.get('/entries/{entry_id}')
def catalog_entry(entry_id: str):
    if not _ENTRY_ID.fullmatch(entry_id):
        raise HTTPException(400, 'Invalid catalog entry id')
    for entry in catalog.cached_catalog()['entries']:
        if entry['catalog_entry_id'] == entry_id:
            return entry
    raise HTTPException(404, 'Catalog entry not found')


@router.get('/progress')
def catalog_progress(object_id: str = Query(min_length=1, max_length=128,
                                           pattern=r'^[A-Za-z0-9_-]+$'),
                     comparison_version: str | None = Query(default=None, pattern=r'^cv\d{3}$')):
    return catalog.progress_for_object(object_id, _version(object_id, comparison_version))
