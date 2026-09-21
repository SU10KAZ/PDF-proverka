"""Read-only catalog of accepted, frozen Project Comparison results (0 model calls)."""
import re

from fastapi import APIRouter, HTTPException, Query

from backend.app.services.project_change_catalog import catalog

router = APIRouter(prefix='/api/project-comparison/catalog', tags=['Project Comparison catalog'])
_ENTRY_ID = re.compile(r'pcc1_[0-9a-f]{32}')


@router.get('')
def list_catalog(diagnostics: bool = Query(default=False)):
    # Diagnostics (failed/partial runs, no cards) are returned only on request.
    return catalog.cached_catalog(include_diagnostics=diagnostics)


@router.get('/entries/{entry_id}')
def catalog_entry(entry_id: str):
    if not _ENTRY_ID.fullmatch(entry_id):
        raise HTTPException(400, 'Invalid catalog entry id')
    for entry in catalog.cached_catalog()['entries']:
        if entry['catalog_entry_id'] == entry_id:
            return entry
    raise HTTPException(404, 'Catalog entry not found')
