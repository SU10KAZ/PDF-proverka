"""Preserve archived external-source records without treating URLs as available files."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FIELDS = ('external_evidence', 'external_sources', 'primary_source')


def read(path):
    return json.loads(path.read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    entries = []
    theme_refs = {}
    for decision_path in sorted((HERE / 'decisions').glob('*.json')):
        decision = read(decision_path)
        sources = [(decision_path, decision)]
        linked = decision.get('primary_adjudication')
        if linked:
            path = Path(linked['path'])
            if not path.is_absolute():
                path = ROOT / path
            assert digest(path) == linked['sha256']
            sources.append((path, read(path)))
        for path, document in sources:
            for field in FIELDS:
                if field not in document:
                    continue
                original = document[field]
                records = original if isinstance(original, list) else [original]
                for index, record in enumerate(records):
                    key = f'{decision_path.stem}:{field}:{index}:{path.stem}'
                    local = None
                    if isinstance(record, dict) and record.get('path'):
                        candidate = Path(record['path'])
                        if not candidate.is_absolute():
                            candidate = ROOT / candidate
                        if candidate.is_file():
                            sha = digest(candidate)
                            if record.get('sha256'):
                                assert sha == record['sha256'], candidate
                            local = {'path': str(candidate), 'sha256': sha}
                    entries.append({
                        'reference_id': key, 'theme': decision_path.stem,
                        'source': {'path': str(path.relative_to(ROOT)),
                                   'sha256': digest(path),
                                   'json_pointer': '/' + field + ('/' + str(index) if isinstance(original, list) else '')},
                        'archived_reference': record,
                        'verified_local_copy': local,
                        'availability': 'verified_local_path' if local else 'no_verified_local_path_in_record',
                        'current_network_access': 'not_requested',
                    })
                    theme_refs.setdefault(decision_path.stem, []).append(key)
    target_links = []
    for target in read(HERE / 'consolidated/evaluation_targets.json')['targets']:
        refs = sorted({key for ctx in target['contexts'] for key in theme_refs.get(ctx['theme'], [])})
        if refs:
            target_links.append({'target_id': target['target_id'], 'route': target['route'],
                                 'context_reference_ids': refs,
                                 'dependency_status': 'theme_context_not_proven_required_for_every_subclaim',
                                 'automatic_exclusion_from_denominator': False})
    result = {
        'purpose': 'Evaluator-only inventory of saved supplementary references, not a new verification of their truth or current availability.',
        'summary': {'archived_references': len(entries), 'themes_with_explicit_references': len(theme_refs),
                    'targets_with_referenced_context': len(target_links),
                    'verified_local_copies': sum(e['verified_local_copy'] is not None for e in entries)},
        'rules': [
            'Retain original access dates, failed access, quotation limits and uncertainty verbatim.',
            'A URL or a recorded SHA without a local path is not a verified available file; this does not prove no copy exists elsewhere.',
            'Absence of these explicit fields does not establish that a theme needs no supplementary evidence.',
            'Do not exclude all targets of a theme because one supplementary source is absent; local document conflicts can have independent support.',
            'Discovery must receive the same general source-access capabilities in both variants, not this answer-bearing list of targeted URLs.',
            'If a required external source fails during a run, record availability separately from search failure; do not silently remove the target after seeing results.',
        ],
        'entries': entries, 'target_context_links': target_links,
        'benchmark_frozen': False, 'production_promotion_allowed': False,
    }
    (HERE / 'consolidated/supplementary_inventory.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['summary'], ensure_ascii=False))


if __name__ == '__main__':
    main()
