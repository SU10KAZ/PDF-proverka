"""Deterministic artifacts and the fixed, source-only DEV access boundary."""
import json
from pathlib import Path
import sys

from experiments.project_change_272.inventory import ROOT, REPO, OBJECT, sha
from experiments.project_change_contracts_v3_272.evidence import fingerprint

FROZEN = {2: ('ad0a31a342a666082f2ef66a', 45, 24),
          8: ('caea6d2810c334ec0368de8e', 108, 188)}
OUT = ROOT / 'fresh_dev_sample_f5_pipeline'


def write(path, value):
    path = Path(path)
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + '\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text() != payload:
            raise ValueError('Frozen artifact drift: ' + str(path))
    else:
        path.write_text(payload)


def admit(partition='DEV', indices=(2, 8)):
    # Reject before invoking even the metadata/source-hash guard.
    if partition != 'DEV' or set(indices) != set(FROZEN) or len(indices) != 2:
        raise PermissionError('Only the two frozen DEV pairs are authorized')
    from experiments.project_change_semantic_272.access import prepared_pairs
    pairs = prepared_pairs(partition='DEV', indices=list(indices))
    for pair in pairs:
        key, old, new = FROZEN[pair['index']]
        if (pair['pair_key'], pair['old']['structure']['pages'], pair['new']['structure']['pages']) != (key, old, new):
            raise PermissionError('Frozen pair identity/page count drift')
        if pair['project'] != OBJECT:
            raise PermissionError('Foreign project')
    return sorted(pairs, key=lambda p: p['index'])


class AccessAudit:
    """Process-local deny-by-default reads of corpus/project data; no network.

    Install after the existing guard verifies metadata and source hashes. Only
    these four source documents and newly produced artifacts can then be read.
    Python audit hooks cannot be removed; tests call check() without installing.
    """
    def __init__(self, pairs, output):
        self.output = Path(output).resolve()
        self.allowed = {Path(a['path']).resolve() for p in pairs for s in ('old', 'new')
                        for a in p[s]['artifacts'].values()}
        self.reads, self.denied = set(), []

    def check(self, event, args):
        if event in {'socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'}:
            self.denied.append(event)
            raise PermissionError('F5 forbids network, subprocesses and inference')
        if event != 'open' or not isinstance(args[0], (str, bytes, Path)):
            return
        path = Path(args[0]).resolve()
        sensitive = (path.is_relative_to(ROOT.parent) or path.is_relative_to(REPO / 'projects_v2')
                     or (path.is_relative_to(REPO) and path.suffix not in {'.py', '.pyc', '.so'}))
        if sensitive and path not in self.allowed and not path.is_relative_to(self.output):
            self.denied.append(str(path))
            raise PermissionError('F5 source allowlist rejected: ' + str(path))
        if sensitive:
            self.reads.add(str(path))

    def install(self):
        sys.addaudithook(self.check)


def code_hashes():
    roots = ('project_change_f5_272', 'project_change_contracts_272',
             'project_change_contracts_v3_272', 'project_change_f2_binding_v4_272',
             'semantic_foundation_v3', 'project_change_semantic_272', 'project_change_272')
    return {str(p.relative_to(REPO)): sha(p) for root in roots
            for p in sorted((REPO / 'experiments' / root).glob('*.py'))}
