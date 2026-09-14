"""Fresh, whole-document DEV replay with source isolation and full coverage counts."""
import argparse
from collections import Counter
from copy import deepcopy
from pathlib import Path
import re
import time

from .inventory import ROOT, REPO, read, write, immutable, sha, now
from .policy import admitted_pairs


def isolated_document(document, embargo, directory):
    """Blank embargoed pages in derived OCR/geometry; preserve line/page ordinals."""
    doc = deepcopy(document)
    source = doc['artifacts']
    target = directory / doc['document_version']
    if (target / 'DOCUMENT.json').exists():
        return read(target / 'DOCUMENT.json')
    target.mkdir(parents=True, exist_ok=True)
    lines = Path(source['work_md']['path']).read_text().splitlines()
    output = []
    page = None
    for line in lines:
        match = re.fullmatch(r'## Page (\d+)\s*', line)
        if match:
            page = int(match[1])
        output.append(line if match or page not in embargo else '')
    md = target / 'document.md'
    md.write_text('\n'.join(output) + '\n')
    blocks = read(source['blocks']['path'])
    blocks['blocks'] = [b for b in blocks['blocks'] if int(b['page_index']) + 1 not in embargo]
    bp = target / 'blocks.json'
    immutable(bp, blocks)
    doc['source_lineage'] = dict(original_sources=source, embargo_pages=embargo,
                                 line_ordinals_preserved=True,
                                 nonembargo_content_changed=False)
    doc['artifacts'] = dict(pdf=source['pdf'], work_md=dict(path=str(md), sha256=sha(md)),
                            blocks=dict(path=str(bp), sha256=sha(bp)))
    immutable(target / 'DOCUMENT.json', doc)
    return doc


def prepare(partition='DEV', candidate=None):
    from experiments.project_change_text_v1.narrative import build
    from experiments.table_materialization_v3.run import materialize_document
    from experiments.engineering_subject_resolver_v1 import source as subjects
    pairs = admitted_pairs(partition, candidate)
    base = ROOT / 'sources' / partition
    subjects.V3 = base / 'tables'
    output = []
    for pair in pairs:
        for side in ['old', 'new']:
            doc = isolated_document(pair[side], pair['embargo_pages'][side], base / 'isolated')
            pair[side] = doc
            version = doc['document_version']
            target = base / 'tables' / version
            if not (target / 'tables.json').exists():
                result = materialize_document(doc)
                for name in ['ledger', 'tables', 'semantics', 'decisions']:
                    immutable(target / (name + '.json'), result[name])
            if not (base / 'narrative' / (version + '.json')).exists():
                immutable(base / 'narrative' / (version + '.json'), build(doc))
            pool_path = base / 'pools' / (version + '.json')
            if not pool_path.exists():
                pool, coverage = subjects.table_pool(doc, pair['pair_key'], side)
                immutable(pool_path, dict(pool=pool, coverage=coverage))
            print('Prepared', pair['index'], side, doc['document_code'], flush=True)
        output.append(pair)
    immutable(base / 'PAIRS.json', output)
    previous = read(ROOT / 'STATE.json') if (ROOT / 'STATE.json').exists() else {}
    write(ROOT / 'STATE.json', dict(status='RUNNING_' + partition, updated_at=now(),
                                    split_sha256=sha(ROOT / 'SPLIT.json'),
                                    next_action='Whole-document DEV baseline and source-only coverage audit',
                                    final_holdout_opened=previous.get('final_holdout_opened', False) or partition == 'FINAL_HOLDOUT',
                                    validation_opened=previous.get('validation_opened', False) or partition == 'VALIDATION'))


def run(name, partition='DEV', candidate=None):
    from experiments.project_change_text_v1.engine import compare
    from experiments.engineering_subject_resolver_v1.core import rank, deterministic, reconcile
    from experiments.engineering_subject_resolver_v1.source import packet
    from experiments.evidence_scope_binding_v1.source import table_nodes
    from experiments.evidence_scope_binding_v1.core import bind
    from experiments.evidence_scope_binding_v1.run import enrich
    from experiments.project_change_master.state import apply
    from experiments.project_change_master.groups import compare_groups
    admitted_pairs(partition, candidate)  # Must precede derived input access.
    base = ROOT / 'sources' / partition
    out = ROOT / 'cycles' / name
    if out.exists():
        raise ValueError('Immutable cycle exists')
    out.mkdir(parents=True)
    immutable(out / 'MANIFEST.json', dict(started_at=now(), partition=partition,
        split_sha256=sha(ROOT / 'SPLIT.json'), inputs_sha256=sha(base / 'PAIRS.json'),
        algorithm='inherited_scope_subject_typed_state_fresh_272_sources',
        model_calls=0, historical_answers_used=False, source_selected_queries='all admitted table subjects; complete narrative',
        code={str(p.relative_to(REPO)): sha(p) for p in (REPO / 'experiments/project_change_272').glob('*.py')}))
    summaries = []
    all_changes = []
    started = time.monotonic()
    for pair in read(base / 'PAIRS.json'):
        tick = time.monotonic()
        pools = {}
        units = {}
        coverage = {}
        for side in ['old', 'new']:
            version = pair[side]['document_version']
            source = read(base / 'pools' / (version + '.json'))
            pools[side] = source['pool']
            coverage[side] = source['coverage']
            units[side] = read(base / 'narrative' / (version + '.json'))
        text_result = compare(pair['pair_key'], units['old']['units'], units['new']['units'])
        # Bind all admitted subjects, retaining unresolved states in coverage.
        bindings = {}
        for side in ['old', 'new']:
            enriched = []
            for subject in pools[side]:
                fragment, nodes = table_nodes(subject)
                binding = bind(fragment, nodes)
                bindings[subject['subject_id']] = binding
                enriched.append(enrich(subject, binding))
            pools[side] = enriched
        packets = [packet('q_' + q['subject_id'], q, rank(q, pools['old'])) for q in pools['new']]
        relations = reconcile([deterministic(p) for p in packets])
        changes = list(text_result['changes'])
        results = []
        for p, relation in zip(packets, relations):
            state = apply(p, relation)
            groups = compare_groups(p, relation)
            changes += state['project_changes'] + groups['project_changes']
            results.append(dict(packet=p, relation=relation, state=state, groups=groups))
        summary = dict(index=pair['index'], cipher=pair['new']['document_code'],
            old_subjects=len(pools['old']), new_subjects=len(pools['new']),
            table_admission=coverage, table_relations=dict(Counter(r['relation'] for r in relations)),
            text=text_result['metrics'], text_quality={s: units[s]['quality'] for s in ['old', 'new']},
            candidates=len(changes), accepted=sum(c['status'] == 'PROVEN' for c in changes),
            review=sum(c['status'] == 'REVIEW' for c in changes),
            seconds=round(time.monotonic() - tick, 2))
        immutable(out / 'pairs' / (str(pair['index']) + '.json'), dict(
            pair=pair, summary=summary, text=text_result, table=results, bindings=bindings, project_changes=changes))
        summaries.append(summary)
        all_changes += [dict(pair_index=pair['index'], change=c) for c in changes]
        print({k: v for k, v in summary.items() if k not in ['text_quality', 'table_admission']}, flush=True)
    immutable(out / 'RESULTS.json', dict(pairs=summaries, project_changes=all_changes,
        seconds=time.monotonic() - started, adjudication='NOT_ADJUDICATED',
        project_change_precision=None, high_value_coverage=None,
        GRAPHIC='NOT_IMPLEMENTED_IN_THIS_BASELINE', split_sha256=sha(ROOT / 'SPLIT.json')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['prepare', 'run'])
    parser.add_argument('--name', default='01_inherited_baseline')
    args = parser.parse_args()
    prepare() if args.action == 'prepare' else run(args.name)
