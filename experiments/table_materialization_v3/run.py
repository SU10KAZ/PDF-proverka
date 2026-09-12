"""Immutable offline runs; the three Foundation artifacts remain byte-identical."""
from pathlib import Path
import argparse
import json
import resource
import time

from experiments.semantic_foundation_v3 import frozen_v1 as v1
from experiments.semantic_foundation_v3.materialize import materialize_document as foundation
from experiments.semantic_foundation_v3.run import read, write, receipt
from experiments.semantic_foundation_v3.scorer import AnchorResolver, abstain
from .model import assemble

ARTIFACTS = ('ledger', 'semantics', 'decisions', 'tables')
POLICY = Path(__file__).with_name('policy.json')


def materialize_document(document, promoted=None):
    if promoted is None:
        promoted = read(POLICY)['promoted_rules']
    result = foundation(document)
    raw = Path(document['artifacts']['work_md']['path']).read_text(errors='replace').splitlines()
    result['tables'] = assemble(result, raw, promoted)
    return result


class TableResolver(AnchorResolver):
    def __init__(self, result):
        super().__init__(result)
        self.segment_tables = {s: n for n, table in enumerate(result['tables']['tables']) for s in table['segments']}

    def predict(self, case):
        if case['document_version'] != self.result['ledger']['document_version']:
            return abstain('DOCUMENT_VERSION_MISMATCH')
        a, b = [self.resolve(x) for x in case['anchors']]
        if a is None or b is None:
            return abstain('MISSING_SOURCE_ANCHOR')
        if a > b:
            return abstain('REVERSED_ANCHORS')
        if case['kind'] != 'TABLE':
            return super().predict(case)
        owners = [self.columns['owner'][i] for i in (a, b)]
        if any(o not in self.segment_tables for o in owners):
            return abstain('WRONG_ANCHOR_KIND')
        if owners[0] == owners[1]:
            return {'decision': 'SAME', 'basis': 'PROVEN', 'evidence_codes': ['WITHIN_ONE_SOURCE_SEGMENT']}
        edges = [d for d in self.result['tables']['boundaries'] if a < d['right_anchor'] <= b]
        if not edges or any(d['decision'] == 'REVIEW' for d in edges):
            return {**abstain('REVIEW_EDGE_ON_PATH' if edges else 'NO_PUBLISHED_EDGE'),
                    'edges': edges}
        return {'decision': 'NEW' if any(d['decision'] == 'NEW' for d in edges) else 'SAME',
                'basis': 'PROVEN', 'evidence_codes': sorted({s for d in edges for s in d['join_evidence'] + d['split_evidence']}),
                'edges': edges}


def run(documents, output, promoted):
    output = Path(output)
    if output.exists():
        raise ValueError('Runs are immutable; choose a new output directory')
    index, started = [], time.perf_counter()
    for document in documents:
        tick = time.perf_counter()
        for name in ('work_md', 'blocks'):
            src = document['artifacts'][name]
            if v1.file_sha(Path(src['path'])) != src['sha256']:
                raise ValueError('Pinned source changed')
        result = materialize_document(document, promoted)
        row = {'document_version': document['document_version'], 'artifacts': {}, 'quality': result['quality']}
        for name in ARTIFACTS:
            path = output / document['document_version'] / (name + '.json')
            write(path, result[name])
            row['artifacts'][name] = receipt(path)
        row['seconds'] = time.perf_counter() - tick
        index.append(row)
        del result
    perf = {'seconds': time.perf_counter() - started, 'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'artifact_bytes': sum(a['bytes'] for r in index for a in r['artifacts'].values()), 'documents': len(documents)}
    write(output / 'index.json', index)
    write(output / 'performance.json', perf)
    write(output / 'producer.json', {'files': [receipt(p) for p in sorted(Path(__file__).parent.glob('*.py'))],
          'documents_sha256': v1.digest(documents), 'promoted_rules': sorted(promoted)})
    print(json.dumps(perf))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--documents', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--policy', type=Path, default=POLICY)
    group.add_argument('--observe', action='store_true', help='Collect all candidate evidence without promotion')
    args = parser.parse_args()
    run(read(args.documents), args.output, () if args.observe else read(args.policy)['promoted_rules'])
