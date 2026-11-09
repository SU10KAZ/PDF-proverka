"""Inventory actual source bytes without treating their presence as full evidence coverage."""
import hashlib
import json
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCES = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/sources')


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(path.read_text())


def main():
    projects = {}
    for discipline in ('AK', 'ITP', 'AR', 'KM', 'VK', 'EOM'):
        folder = SOURCES / discipline
        blocks = read(folder / 'blocks.json')
        files = []
        for name, role in [('document.md', 'authoritative_TEXT'),
                           ('document.pdf', 'GRAPHIC_source_not_TEXT_override'),
                           ('blocks.json', 'block_geometry_and_identity')]:
            path = folder / name
            files.append({'path': str(path), 'sha256': digest(path),
                          'bytes': path.stat().st_size, 'role': role})
        projects[discipline] = {
            'document_id': blocks['document_id'],
            'document_name': blocks['document_name'],
            'document_path_metadata': blocks.get('document_path'),
            'page_count_from_block_registry': len(blocks['pages']),
            'files': files,
        }
    checked = 0
    for path in sorted((HERE / 'decisions').glob('*.json')):
        decision = read(path)
        source = decision.get('source_md')
        if isinstance(source, dict) and source.get('sha256') and source.get('path'):
            candidate = Path(source['path'])
            if candidate.parent.parent == SOURCES:
                expected = projects[candidate.parent.name]['files'][0]
                assert str(candidate) == expected['path']
                assert source['sha256'] == expected['sha256'], path
                checked += 1
    records = read(HERE / 'consolidated/records.json')
    by_id = {r['candidate_id']: r for r in records}
    targets = read(HERE / 'consolidated/evaluation_targets.json')['targets']
    assignments = []
    for target in targets:
        cids = sorted({cid for ctx in target['contexts'] for cid in ctx['candidate_ids']})
        disciplines = sorted({by_id[cid]['discipline'] for cid in cids})
        assert len(disciplines) == 1, (target['target_id'], disciplines)
        assignments.append({
            'target_id': target['target_id'], 'discipline': disciplines[0],
            'route': target['route'], 'candidate_ids': cids,
            'baseline_candidate_ids': [cid for cid in cids if cid.startswith('B-')],
            'historical_candidate_ids': [cid for cid in cids if cid.startswith('H-')],
            'primary_input_files_present': True,
            'full_evidence_availability': 'not_inferred_from_primary_files',
            'eligible_for_run_scoring': False,
        })
    counts = {d: dict(Counter(x['route'] for x in assignments if x['discipline'] == d))
              for d in projects}
    result = {
        'purpose': 'Evaluator-only input inventory for the six known regression projects.',
        'summary': {'projects': len(projects), 'files_hashed': 18,
                    'direct_decision_MD_hash_matches': checked,
                    'targets_assigned': len(assignments), 'targets_by_discipline': counts},
        'projects': projects, 'targets': assignments,
        'policy': {
            'TEXT': 'Prepared MD is authoritative; never re-extract or override TEXT from PDF raster.',
            'GRAPHIC': 'PDF and block geometry support graphic review; file presence does not prove each required graphic has been read.',
            'questions': 'Separate questions and clarifications section, as explicitly chosen by the user.',
            'external_sources': 'Manufacturer and normative evidence are not proven available merely because primary MD/PDF exist. Declare permitted sources before scoring those targets.',
            'blindness': 'This inventory contains target IDs and is evaluator-only. Discovery receives only separately prepared source files and approved instructions.',
            'same_input': 'Use identical source hashes and permitted supplementary sources for baseline and candidate.',
            'exposure': 'All six projects are previously known regression cases, never new blind validation.',
        },
        'benchmark_frozen': False, 'stage_1_complete': False,
    }
    (HERE / 'consolidated/input_inventory.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False))


if __name__ == '__main__':
    main()
