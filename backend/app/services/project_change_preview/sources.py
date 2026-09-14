"""Pinned DEV-only source admission. No comparison or grouping is executed."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from backend.app.services.stage_comparison.domain_keys import canonical, signature

OBJECT = '272_Sadovnicheskaya_76_Balchug_Esteyt'
REPO = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = REPO / 'backend/app/data/project_change_bridge_sources.json'


class SourceUnavailable(ValueError):
    pass


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


class FrozenSources:
    def __init__(self, manifest_path=DEFAULT_MANIFEST):
        # Import the offline access guard only after preview is explicitly
        # enabled; production startup never imports research packages.
        from experiments.project_change_272.policy import admitted_pairs
        self.manifest_path = Path(manifest_path).resolve()
        self.manifest = read(self.manifest_path)
        m = self.manifest
        if (m.get('schema_version') != 'project-change-bridge-sources/1' or m.get('object_id') != OBJECT
                or m.get('partition') != 'DEV' or m.get('stage_relation') != ['stage_1', 'stage_2']):
            raise SourceUnavailable('Only frozen object 272 DEV sources are supported')
        self.root = Path(m['research_root']).resolve()
        self.receipts = {self.manifest_path: sha(self.manifest_path)}
        for relative, digest in m['policy_receipts'].items():
            self.receipts[self._path(relative)] = digest
        self._verify_receipts()
        self.pairs = {p['pair_key']: p for p in admitted_pairs('DEV', root=self.root)}
        index = {p['index']: p for p in self.pairs.values()}
        self.events = []
        for artifact in m['artifacts']:
            p = index.get(artifact['pair_index'])
            if not p or artifact['pair_key'] != p['pair_key']:
                raise SourceUnavailable('Artifact not admitted by the frozen DEV split')
            path = self._path(artifact['artifact'])
            if path.parent != self.root / 'runs' / m['source_run_id'] / 'pairs':
                raise SourceUnavailable('Contradictory run selection')
            self.receipts[path] = artifact['sha256']
            if sha(path) != artifact['sha256']:
                raise SourceUnavailable('Research artifact drift')
            data = read(path)
            if data['pair']['pair_key'] != p['pair_key']:
                raise SourceUnavailable('Wrong pair provenance')
            for change in data['project_changes']:
                if change['comparison_scope'] != p['pair_key'] or change['status'] not in {'PROVEN', 'REVIEW'}:
                    raise SourceUnavailable('Invalid ProjectChange scope/status')
                self.events.append((copy.deepcopy(change), p))
        if len({c['project_change_id'] for c, _ in self.events}) != len(self.events):
            raise SourceUnavailable('Duplicate runtime ProjectChange ids')
        self.documents = {}
        for p in self.pairs.values():
            for side in ['old', 'new']:
                d = p[side]
                path = Path(d['artifacts']['pdf']['path']).resolve()
                self.documents[(p['pair_key'], side)] = path
                for receipt in d['artifacts'].values():
                    self.receipts[Path(receipt['path']).resolve()] = receipt['sha256']
        for change, pair in self.events:
            for side in ['old', 'new']:
                for evidence in change['evidence_'+side]:
                    self.evidence_binding(pair, side, evidence)
                    for receipt in evidence['source_receipts'].values():
                        self.receipts[Path(receipt['path']).resolve()] = receipt['sha256']
        self.source_revision = signature({'manifest': sha(self.manifest_path), 'adapter': 'pcbridge-view/1'})
        self._stats = {}
        self.assert_current()

    def _path(self, relative):
        path = (self.root / relative).resolve()
        if not path.is_relative_to(self.root):
            raise SourceUnavailable('Source manifest path outside the corpus')
        return path

    def _verify_receipts(self):
        for path, digest in self.receipts.items():
            if sha(path) != digest:
                raise SourceUnavailable('Pinned source changed: ' + path.name)

    def assert_current(self):
        # Pin bytes at load, and recheck any source whose file identity changed.
        # The selected research artifacts are never automatically advanced.
        for path, digest in self.receipts.items():
            stat = path.stat()
            stamp = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            if self._stats.get(path) != stamp:
                if sha(path) != digest:
                    raise SourceUnavailable('Pinned source changed; preview stopped: ' + path.name)
                self._stats[path] = stamp

    def evidence_binding(self, pair, side, e):
        doc = pair[side]
        receipt = e['source_receipts']['pdf']
        path = Path(receipt['path']).resolve()
        if (path != self.documents[(pair['pair_key'], side)]
                or receipt['sha256'] != doc['artifacts']['pdf']['sha256']
                or e['document_version'] != doc['document_version']
                or e['document_code'] != doc['document_code']):
            raise SourceUnavailable('Evidence is not from the admitted document/version')
        for kind, receipt in e['source_receipts'].items():
            original = doc['artifacts'].get(kind)
            if not original:
                raise SourceUnavailable('Unknown source receipt')
            receipt_path = Path(receipt['path']).resolve()
            original_path = Path(original['path']).resolve()
            isolated = self.root/'sources'/'DEV'/'isolated'/doc['document_version']/original_path.name
            if receipt_path not in {original_path, isolated.resolve()}:
                raise SourceUnavailable('Evidence receipt outside the admitted DEV version')
        locator = e.get('locator') or {}
        pages = {r['page'] for r in e.get('source_refs', [])}
        if locator.get('page'):
            pages.add(locator['page'])
        if not pages or any(type(p) is not int or p < 1 or p in pair['embargo_pages'][side] for p in pages):
            raise SourceUnavailable('Evidence page missing or embargoed')
        if e['route'] not in {'TEXT', 'TABLE', 'GRAPHIC'}:
            raise SourceUnavailable('Unsupported evidence route')
        return path, sorted(pages)


def event_identity(change, pair, evidence_keys, candidate_version):
    """Stable event identity plus a stricter reuse receipt, never display text alone.

    Scope/subject IDs and fact IDs from research are audit-only. Raw source
    content receipts disambiguate local/unnamed subjects. A broad lineage key
    can detect stale history but can NEVER grant authority.
    """
    subject = change['engineering_subject']
    semantic = {k: subject.get(k) for k in ['mark', 'equipment_class', 'system', 'engineering_function',
        'room', 'floor', 'semantic_subject']}
    family = {'schema': 'pcbridge-decision/1', 'object': OBJECT, 'stages': ['stage_1', 'stage_2'],
        'documents': {s: pair[s]['document_code'] for s in ['old', 'new']},
        'subject': semantic, 'event_type': change['change_type'], 'scope': change['scope'],
        'properties': sorted({f['property'] for f in change.get('supporting_fact_changes', [])})}
    payload = {**family, 'source_evidence': sorted(evidence_keys, key=canonical)}
    facts = [{k: f.get(k) for k in ['property', 'old', 'new']} for f in change.get('supporting_fact_changes', [])]
    binding = {'identity': payload, 'candidate_version': candidate_version,
        'old_state': change.get('old_state'), 'new_state': change.get('new_state'),
        'facts': sorted(facts, key=canonical), 'resolution': subject.get('resolution'),
        'identity_basis': sorted(subject.get('identity_basis', [])), 'research_status': change['status'],
        'review_reasons': sorted(change.get('review_reasons', [])), 'conflicts': change.get('conflicts', [])}
    reusable = bool(evidence_keys and subject.get('resolution') in {'EXPLICIT', 'LOCAL'}
                    and not any('AMBIGUOUS' in reason for reason in change.get('review_reasons', [])))
    return {'decision_key': 'pcbridge1_' + signature(payload), 'lineage_key': 'pclineage1_' + signature(family),
        'binding_signature': signature(binding), 'identity_reusable': reusable,
        'identity_payload': payload, 'evidence_snapshot': binding}
