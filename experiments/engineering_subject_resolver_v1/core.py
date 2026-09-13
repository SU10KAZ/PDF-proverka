"""Shared evidence contract and conservative identity decisions.

Observation IDs are version-local. A relation, not a global guessed key,
establishes cross-version identity. Model and numeric state never anchor it.
"""
from collections import Counter
from difflib import SequenceMatcher
import re

from experiments.text_comparison_v1.common import digest
from experiments.text_old_scope_recovery_v1.retrieval import tokens

RELATIONS = ['SAME_SUBJECT', 'RELATED_SUBJECT', 'DIFFERENT_SUBJECT',
             'AMBIGUOUS', 'NOT_FOUND_UNPROVEN']
FIELDS = ['discipline', 'system', 'engineering_function', 'equipment_class',
          'mark', 'position', 'room', 'floor', 'served_zone', 'connection',
          'local_heading', 'explicit_references']
ANCHORS = ['mark', 'system', 'room', 'served_zone', 'engineering_function']


def norm(s):
    return re.sub(r'\s+', ' ', (s or '').casefold().replace('ё', 'е').replace('*', '')).strip()


def features(text, heading='', discipline=''):
    """Literal clues, not inferred facts. Unparsed fields stay empty."""
    s = norm(text)
    out = {k: [] for k in FIELDS}
    out['discipline'] = [discipline] if discipline else []
    out['local_heading'] = [heading] if heading else []
    out['mark'] = sorted(set(re.findall(r'(?<!\w)(?:[ПВТК]\d+(?:\.\d+)?[а-я]{0,3}|(?:ДУ|ПД)\d+(?:\.\d+)?[а-я]{0,3})(?!\w)', text)))
    out['room'] = re.findall(r'помещени[ея]\s*(?:№\s*)?(\d+(?:\.\d+)?)', s)
    out['floor'] = re.findall(r'(-?\d+)\s*(?:-?м|-?ом|-?й)?\s*этаж', s)
    out['served_zone'] = re.findall(r'(?:корпус|секция|пожарный отсек|зона)\s*№?\s*\d+', s)
    for name, pat in [('насос', r'насос'), ('теплообменник', r'теплообменник'),
                      ('вентилятор', r'вентилятор'), ('клапан', r'клапан'),
                      ('фанкойл', r'фанкойл'), ('чиллер', r'чиллер')]:
        if re.search(pat, s): out['equipment_class'].append(name)
    return out


def make_subject(*, source_type, side, version, scope, text, evidence, clues=None,
                 context=None, purity='PROVEN', local_id='', **extra):
    s = dict(schema='engineering-subject.v1', subject_id='es_' + digest(
        [source_type, side, version, scope, local_id, text])[:24],
        source_type=source_type, side=side, document_version=version,
        comparison_scope=scope, text=text, clues=clues or features(text),
        context=context or [], evidence=evidence, purity=purity,
        state={'model_is_property': True}, **extra)
    return s


def compatible(a, b):
    return (a['comparison_scope'] == b['comparison_scope'] and
            a['source_type'] == b['source_type'] and a['side'] != b['side'] and
            a['document_version'] != b['document_version'])


def conflicts(a, b):
    # Changed mark may be a renamed slot: it is not itself DIFFERENT_SUBJECT.
    return [k for k in ['discipline', 'room', 'floor', 'served_zone']
            if a['clues'][k] and b['clues'][k] and
            not set(a['clues'][k]) & set(b['clues'][k])]


def rank(query, pool, k=6):
    """Retrieval-only similarity: property/model/position matches prove nothing."""
    qt = set(tokens(query['text']))
    out = []
    for s in pool:
        if not compatible(query, s) or s['purity'] != 'PROVEN': continue
        st = set(tokens(s['text']))
        overlap = len(qt & st) / max(1, len(qt | st))
        lexical = SequenceMatcher(None, norm(query['text']), norm(s['text']), autojunk=False).ratio()
        anchors = sum(bool(set(query['clues'][f]) & set(s['clues'][f])) for f in ANCHORS)
        heading = bool(set(query['clues']['local_heading']) & set(s['clues']['local_heading']))
        score = .55 * overlap + .35 * lexical + .15 * anchors + .1 * heading
        if overlap > 0 or anchors: out.append(dict(subject=s, score=round(score, 6), conflicts=conflicts(query, s)))
    return sorted(out, key=lambda x: (-x['score'], x['subject']['subject_id']))[:k]


def decision(packet, relation='NOT_FOUND_UNPROVEN', reason='No proven counterpart', old_ids=None,
             new_ids=None, method='deterministic', confidence='LOW', witnesses=None):
    return dict(schema='engineering-subject-relation.v1', candidate_id=packet['candidate_id'],
                packet_hash=packet['packet_hash'], source_type=packet['source_type'],
                relation=relation, old_subject_ids=old_ids or [],
                new_subject_ids=new_ids or [packet['new']['subject_id']],
                confidence=confidence, method=method, reason=reason,
                witnesses=witnesses or [], state_relation='UNASSESSED',
                review_required=relation not in ['SAME_SUBJECT', 'RELATED_SUBJECT'],
                absence_proven=False)


def deterministic(packet):
    q = packet['new']
    if q['purity'] != 'PROVEN':
        return decision(packet, 'AMBIGUOUS', 'SOURCE_TYPE_NOT_PURE')
    hits = []
    for row in packet['old_candidates']:
        s = row['subject']
        if not compatible(q, s) or conflicts(q, s): continue
        common = {f for f in ANCHORS if set(q['clues'][f]) & set(s['clues'][f])}
        if ('mark' in common and len(common - {'mark'}) >= 1 and
                q['clues']['equipment_class'] and
                q['clues']['equipment_class'] == s['clues']['equipment_class']):
            hits.append(s)
    if len(hits) == 1:
        s = hits[0]
        return decision(packet, 'SAME_SUBJECT', 'Unique explicit mark plus independent scoped anchor and class',
                        [s['subject_id']], confidence='HIGH',
                        witnesses=[dict(old_subject_id=s['subject_id'], old_quote=s['text'],
                                        new_subject_id=q['subject_id'], new_quote=q['text'])])
    return decision(packet, 'AMBIGUOUS' if packet['old_candidates'] else 'NOT_FOUND_UNPROVEN',
                    'Structured evidence insufficient or nonunique')


def similarity_only(packet):
    rows = packet['old_candidates']
    if packet['new']['purity'] != 'PROVEN': return decision(packet, 'AMBIGUOUS', 'SOURCE_TYPE_NOT_PURE')
    if not rows: return decision(packet)
    top = rows[0]
    if (top['score'] >= .65 and not top['conflicts'] and
            (len(rows) == 1 or top['score'] - rows[1]['score'] >= .12)):
        s = top['subject']; q = packet['new']
        return decision(packet, 'SAME_SUBJECT', 'Similarity threshold/margin proposal; not chosen for production',
                        [s['subject_id']], method='similarity_only', confidence='HIGH',
                        witnesses=[dict(old_subject_id=s['subject_id'], old_quote=s['text'],
                                        new_subject_id=q['subject_id'], new_quote=q['text'])])
    return decision(packet, 'AMBIGUOUS', 'Similarity cannot separate alternatives', method='similarity_only')


def validate_ai(packet, proposal):
    """Reject invalid IDs/quotes/cardinality/routes, uncertain identity and missing witnesses."""
    try:
        relation = proposal['relation']; confidence = proposal['confidence']
        assert relation in RELATIONS and confidence in ['HIGH', 'MEDIUM', 'LOW']
        old = {r['subject']['subject_id']: r['subject'] for r in packet['old_candidates']}
        new = {s['subject_id']: s for s in [packet['new']] + packet.get('new_neighbors', [])}
        oi, ni = proposal['old_subject_ids'], proposal['new_subject_ids']
        assert len(oi) == len(set(oi)) and len(ni) == len(set(ni))
        assert set(oi) <= old.keys() and set(ni) <= new.keys()
        if relation in ['SAME_SUBJECT', 'RELATED_SUBJECT', 'DIFFERENT_SUBJECT']:
            assert oi and ni and packet['new']['subject_id'] in ni
            assert all(compatible(old[o], new[n]) for o in oi for n in ni)
            assert all(s['purity'] == 'PROVEN' for s in [old[i] for i in oi] + [new[i] for i in ni])
            if relation == 'SAME_SUBJECT': assert len(oi) == len(ni) == 1
            if relation == 'RELATED_SUBJECT':
                assert (len(oi) == 1 and len(ni) > 1) or (len(oi) > 1 and len(ni) == 1)
                assert proposal.get('restructuring_basis') and proposal.get('scope_conserved') is True
            witnesses = proposal['witnesses']
            assert {w['old_subject_id'] for w in witnesses} == set(oi)
            assert {w['new_subject_id'] for w in witnesses} == set(ni)
            for w in witnesses:
                a, b = old[w['old_subject_id']], new[w['new_subject_id']]
                assert len(w['old_quote']) >= 8 and len(w['new_quote']) >= 8
                assert w['old_quote'] in a['text'] and w['new_quote'] in b['text']
            assert proposal.get('alternatives_reason') and proposal.get('identity_basis')
        if relation in ['SAME_SUBJECT', 'RELATED_SUBJECT'] and confidence != 'HIGH':
            return decision(packet, 'AMBIGUOUS', 'AI_IDENTITY_CONFIDENCE_INSUFFICIENT', method='hybrid_ai')
        result = decision(packet, relation, proposal['reason'], oi, ni, 'hybrid_ai', confidence, proposal.get('witnesses', []))
        result['identity_basis'] = proposal.get('identity_basis', '')
        result['alternatives_reason'] = proposal.get('alternatives_reason', '')
        result['restructuring_basis'] = proposal.get('restructuring_basis', '')
        return result
    except (KeyError, AssertionError, TypeError, ValueError):
        return decision(packet, 'AMBIGUOUS', 'AI_PROPOSAL_VALIDATION_FAILED', method='hybrid_ai')


def reconcile(results):
    """No independent 1:1 decisions may silently use one OLD object twice."""
    counts = Counter((r['source_type'], o) for r in results if r['relation'] == 'SAME_SUBJECT' for o in r['old_subject_ids'])
    for r in results:
        if r['relation'] == 'SAME_SUBJECT' and any(counts[r['source_type'], o] > 1 for o in r['old_subject_ids']):
            r.update(relation='AMBIGUOUS', confidence='LOW', review_required=True,
                     reason='MULTIPLE_NEW_SUBJECTS_SHARE_OLD_WITHOUT_RESTRUCTURING_CERTIFICATE')
    return results
