"""Pre-implementation grouping experiment on explicitly constructed event truth.

Not corpus truth or an extraction benchmark. Each record is a fact occurrence;
truth defines the engineering event independently of the grouping algorithms.
"""
from collections import defaultdict
from pathlib import Path
from experiments.text_comparison_v1.common import write

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1')


def cases():
    rows = []

    def add(entity, family, event, passage, scope='building_a', replacement=False, n=1):
        for i in range(n):
            rows.append(dict(entity=entity, family=family, event=event, passage=passage,
                             scope=scope, replacement=replacement, fact=f'{event}_{i}'))

    add('Н7', 'model', 'replace_n7', 'p1', replacement=True)
    add('Н7', 'parameter', 'replace_n7', 'p1', replacement=True, n=20)
    add('Н7', 'model', 'replace_n7', 'p2', replacement=True)
    add('Н7', 'parameter', 'replace_n7', 'p2', replacement=True, n=20)
    add('Н8', 'capacity', 'capacity_n8', 'p1')  # same page/passage, different entity
    add('Н8', 'mode', 'mode_n8', 'p3')  # same entity, different engineering event
    add('Н7', 'capacity', 'capacity_other_building', 'p4', scope='building_b')
    add(None, 'requirement', 'anonymous_req_a', 'p5')
    add(None, 'requirement', 'anonymous_req_b', 'p6')
    return rows


def group(rows, approach):
    groups = defaultdict(list)
    for row in rows:
        if approach == 'entity_first':
            key = (row['scope'], row['entity'])
        elif approach == 'event_first':
            key = (row['scope'], row['entity'], row['passage'],
                   'replacement' if row['replacement'] else row['family'])
        elif approach == 'guarded_hybrid':
            # Explicit replacement relation absorbs parameters; otherwise an
            # independent family remains independent. Anonymous identities stay local.
            key = (row['scope'], row['entity'] or row['passage'],
                   'replacement' if row['replacement'] else row['family'])
        else:
            raise ValueError(approach)
        groups[key].append(row)
    return list(groups.values())


def run(root=ROOT):
    rows = cases()
    results = []
    for name in ('entity_first', 'event_first', 'guarded_hybrid'):
        groups = group(rows, name)
        over = sum(len({r['event'] for r in g}) > 1 for g in groups)
        appearances = defaultdict(set)
        for i, g in enumerate(groups):
            for r in g:
                appearances[r['event']].add(i)
        under = sum(len(v) > 1 for v in appearances.values())
        results.append(dict(approach=name, groups=len(groups), over_grouped=over,
                            under_grouped=under, expected_events=len(appearances)))
    write(root/'reports/APPROACH_EXPERIMENT.json', dict(facts=rows, results=results,
          basis='Constructed grouping DEV, before route implementation; no historical examples'))
    (root/'reports/APPROACHES_COMPARED.md').write_text(
        '# Approaches compared before implementation\n\n'
        'Executable DEV experiment: 47 fact occurrences, six intended events. '
        'These are constructed labels, not human corpus truth.\n\n'
        '| Approach | Groups | Over-grouped groups | Under-grouped events |\n'
        '|---|---:|---:|---:|\n' + ''.join(
            f"| {r['approach']} | {r['groups']} | {r['over_grouped']} | {r['under_grouped']} |\n"
            for r in results) + '\nChosen: guarded hybrid. Entity-first merges independent mode/capacity '
        'events and anonymous subjects. Local event-first handles replacement hierarchy but duplicates '
        'repeated descriptions. Hybrid requires identity + event/state compatibility and explicit '
        'replacement support. Page co-location is never an identity key.\n\n'
        'Local LLM grouping was considered but not executed: it adds a provider dependency and still '
        'needs deterministic evidence validation. V1 uses bounded text templates and abstention. '
        'The comparison tests grouping only; extraction/alignment get separate adversarial tests.\n')
    print(results)


if __name__ == '__main__':
    run()
