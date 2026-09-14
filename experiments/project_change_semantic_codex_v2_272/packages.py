"""Versioned source-only packages, with explicit diagnostic scope scaffolding."""
import json
from pathlib import Path
import fitz

from experiments.project_change_272.inventory import ROOT, read, sha, immutable
from experiments.project_change_semantic_272.access import prepared_pairs
from experiments.project_change_semantic_272.history import document_history, pages_from_markdown
from experiments.project_change_semantic_272.packets import digest
from .contracts import escalate

BASE = ROOT / 'semantic_codex_v2_diagnostic'
V1 = ROOT / 'semantic_codex_v1'
PACKAGE_VERSION = 'V2_DIAGNOSTIC_PACKAGE_V1'


def save(path, value):
    if path.exists():
        if read(path) != value:
            raise ValueError('Immutable artifact drift: ' + str(path))
    else:
        immutable(path, value)


def validate_package(p, pairs, exclusions):
    if (p['version'] != PACKAGE_VERSION or p['object_id'] != 272 or
        p['partition'] != 'DEV' or p['old'] != 'stage_1' or p['new'] != 'stage_2' or
        p['logical_baseline'] != 'v002' or p['split_sha256'] != sha(ROOT / 'SPLIT.json')):
        raise PermissionError('Wrong diagnostic corpus/direction/version')
    if p['package_hash'] != digest({k: v for k, v in p.items() if k != 'package_hash'}):
        raise ValueError('Package content drift')
    ids = set()
    for e in p['sources']:
        i, side, page = e['pair_index'], e['side'], e['page']
        if i not in pairs or side not in ['old', 'new'] or page in exclusions[(i, side)]:
            raise PermissionError('Non-DEV or embargo/history source')
        doc = pairs[i][side]
        if e['document_version'] != doc['document_version'] or e['source_receipt'] != doc['artifacts']['pdf']:
            raise PermissionError('Physical version/source drift')
        if e['evidence_id'] in ids:
            raise ValueError('Duplicate evidence ID')
        ids.add(e['evidence_id'])
        if [i, side, page] not in p['allowed_page_scopes']:
            raise PermissionError('Evidence outside declared scope')
        if i != p['pair_index'] and e['role'] != 'EXPLICIT_LINKED_DOCUMENT_CONTEXT':
            raise PermissionError('Uncertified document boundary crossing')
        if e.get('raster'):
            r = e['raster']
            if not Path(r['path']).resolve().is_relative_to(BASE / 'rasters') or sha(r['path']) != r['sha256']:
                raise ValueError('Raster provenance/hash drift')
    if sum(bool(e.get('raster')) for e in p['sources']) > 8:
        raise ValueError('More than eight raster inputs')


def prepare():
    manifest = read(BASE / 'DIAGNOSTIC_MANIFEST.json')
    if sha(BASE / 'DIAGNOSTIC_MANIFEST.json') != (BASE / 'DIAGNOSTIC_MANIFEST.sha256').read_text().strip():
        raise ValueError('Diagnostic selection drift')
    pairs = {p['index']: p for p in prepared_pairs('DEV')}
    exclusions = {(i, s): set(p['embargo_pages'][s]) | set(document_history(p[s], p['embargo_pages'][s]))
                  for i, p in pairs.items() for s in ['old', 'new']}
    registry = {r['case_id']: r for r in read(BASE / 'SOURCE_SCOPE_REGISTRY.json')['scopes']}
    changes = {c['project_change_id']: c for c in read(V1 / 'reports/codex_v1_source_audited/PROJECT_CHANGES_SOURCE_AUDITED.json')['project_changes']}
    original_coverage = {}
    for entry in read(V1 / 'audit/PACKAGES.json')['packets']:
        path = Path(entry['path'])
        if sha(path) != entry['sha256']:
            raise ValueError('V1 primary packet drift')
        packet = read(path)
        for side in ['old', 'new']:
            original_coverage.setdefault((packet['pair_index'], side), set()).update(e['page'] for e in packet['evidence'][side])
    cache, markdown, results, gaps = {}, {}, [], []
    for case in manifest['cases']:
        cid = case['case_id']; scope = registry[cid]; original = changes.get(cid)
        claims, crop_requests = [], []
        if original:
            for bi, bundle in enumerate(original['state_bundles']):
                source = read(bundle['source_packet']['path'])
                for fi, fact in enumerate(bundle['event']['facts']):
                    claims.append(dict(claim_id=f'b{bi}f{fi}', property=fact['property'],
                        old_value=fact['old_value'], new_value=fact['new_value']))
                    for side in ['old', 'new']:
                        for w in fact[side + '_witnesses']:
                            e = next(e for e in source['evidence'][side] if e['evidence_id'] == w['evidence_id'])
                            if e.get('raster'):
                                crop_requests.append((case['pair_index'], side, e['page'], e['bbox']))
        rows = []
        for page_scope in scope['pages']:
            i, side, number = page_scope['pair_index'], page_scope['side'], page_scope['page']
            if i not in pairs or number in exclusions[(i, side)]:
                raise PermissionError(f'Forbidden diagnostic source: {i}/{side}/{number}')
            key = (i, side); doc = pairs[i][side]
            if key not in cache:
                cache[key] = fitz.open(doc['artifacts']['pdf']['path'])
                markdown[key] = pages_from_markdown(Path(doc['artifacts']['work_md']['path']).read_text())
            if not 1 <= number <= len(cache[key]):
                raise ValueError('Page outside source')
            page = cache[key][number-1]
            native = page.get_text()
            ocr = markdown[key].get(number, '')
            text = 'PDF NATIVE (unrotated extraction):\n' + native + '\nOCR (fallible; raster resolves glyphs):\n' + ocr
            row = dict(**page_scope, document_version=doc['document_version'], source_receipt=doc['artifacts']['pdf'],
                evidence_id=f'p{i}_{side}_{number}', text=text, text_complete=True,
                native_sha256=digest(native), ocr_sha256=digest(ocr), raster=None,
                coordinate_space='DISPLAY_PDF_POINTS', bbox=list(page.rect))
            rows.append(row)
        # Original detailed witness crops before full-page context, always newly receipted.
        requests = []
        for i, side, page, box in crop_requests:
            if not any((r['pair_index'], r['side'], r['page']) == (i, side, page) for r in rows):
                raise PermissionError('Original crop outside explicit registry')
            request = (i, side, page, tuple(box))
            if request not in requests:
                requests.append(request)
        # Full context for NEW/OLD and newly admitted counter-pages takes priority
        # over redundant crops. Budget limitation is recorded, never concealed.
        full = [(r['pair_index'], r['side'], r['page'], tuple(r['bbox'])) for r in rows]
        selected = list(dict.fromkeys(requests[:2] + full))[:8]
        for index, (i, side, number, box) in enumerate(selected):
            page = cache[(i, side)][number-1]
            base = next(r for r in rows if (r['pair_index'], r['side'], r['page']) == (i, side, number))
            # get_pixmap clips in DISPLAY coordinates, including rotated PDFs.
            # Native text boxes would be derotated; raster clip boxes must not be.
            display_box = fitz.Rect(box)
            image_key = digest(dict(source=base['source_receipt'], page=number, bbox=box,
                renderer=fitz.VersionBind, coordinate_contract='DISPLAY_CLIP_V1', max_edge=2600))
            path = BASE / 'rasters' / (image_key + '.png'); path.parent.mkdir(parents=True, exist_ok=True)
            scale = min(2.5, 2600 / max(fitz.Rect(box).width, fitz.Rect(box).height))
            if not path.exists():
                page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=display_box).save(path)
            raster = dict(path=str(path), sha256=sha(path), source_bbox_display=list(box),
                          source_pdf_sha256=base['source_receipt']['sha256'], page=number)
            if tuple(base['bbox']) == box and not base['raster']:
                base['raster'] = raster
            else:
                rows.append(dict(base, evidence_id=base['evidence_id'] + f'_crop{index}',
                    bbox=list(box), raster=raster, level=3, text='', text_complete=False))
        # A bounded package does not silently claim to contain truncated source text.
        char_budget = 42000
        per_page = min(10000, char_budget // max(1, sum(bool(r['text']) for r in rows)))
        for r in rows:
            if len(r['text']) > per_page:
                r['text'] = r['text'][:per_page]
                r['text_complete'] = False
        p = dict(version=PACKAGE_VERSION, object_id=272, logical_baseline='v002',
            partition='DEV', old='stage_1', new='stage_2', split_sha256=sha(ROOT/'SPLIT.json'),
            case_id=cid, pair_index=case['pair_index'], engineering_subject=case['subject'],
            untrusted_candidate_summary=original['summary_ru'] if original else '',
            candidate_claims=claims, sources=rows,
            allowed_page_scopes=[[r['pair_index'],r['side'],r['page']] for r in scope['pages']],
            scope_provenance=dict(registry_sha256=sha(BASE/'SOURCE_SCOPE_REGISTRY.json'),
                origin=scope['scope_origin'], autonomous_scope_discovery_proven=False),
            coverage_complete=False, raster_budget=8, text_truncation_explicit=True)
        same_pair = [r for r in rows if r['pair_index'] == case['pair_index']]
        p['old_retrieval'] = escalate(dict(pair_index=case['pair_index'],
            old_version=pairs[case['pair_index']]['old']['document_version'],
            allowed_page_ids=[r['evidence_id'] for r in same_pair]), case['subject'], same_pair)
        p['package_hash'] = digest(p)
        validate_package(p, pairs, exclusions)
        path = BASE/'packages'/(cid+'.json'); save(path, p)
        results.append(dict(case_id=cid, path=str(path), sha256=sha(path), package_hash=p['package_hash']))
        gaps.append(dict(case_id=cid, pages=[dict(**r,
            present_v1_primary=r['page'] in original_coverage.get((r['pair_index'], r['side']), set()),
            present_v2_text=any(e['pair_index']==r['pair_index'] and e['side']==r['side'] and e['page']==r['page'] and e['text'] for e in rows),
            present_v2_raster=any(e['pair_index']==r['pair_index'] and e['side']==r['side'] and e['page']==r['page'] and e['raster'] for e in rows))
            for r in scope['pages']], claim='PAGE_COVERAGE_ONLY_NOT_RECALL'))
    for doc in cache.values():
        doc.close()
    save(BASE/'PACKAGE_INDEX.json', dict(version=PACKAGE_VERSION, packages=results,
        manifest_sha256=sha(BASE/'DIAGNOSTIC_MANIFEST.json'), registry_sha256=sha(BASE/'SOURCE_SCOPE_REGISTRY.json')))
    save(BASE/'PACKAGE_COVERAGE.json', gaps)
    print('Prepared', len(results), 'versioned diagnostic packages', flush=True)


if __name__ == '__main__':
    prepare()
