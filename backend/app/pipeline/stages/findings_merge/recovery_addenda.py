"""Publish recovered subclaims without silently replacing the original finding."""
from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import json
from .candidate_binding import bind_candidate_decisions
from .recovery_links import target_signature


def render_description(original, addenda):
    texts = list(dict.fromkeys(a['claim'] + '\n' + a['reason'] for a in addenda))
    return 'Первоначальное описание:\n' + original + '\n\nДополнительная проверка:\n' + '\n\n'.join(texts)


def prepare_addenda(packet, decisions):
    candidates = {(c['origin'], c['pointer']): c for c in packet.get('candidate_group', [packet['candidate']])}
    targets = defaultdict(list)
    for finding in packet['existing_findings']:
        if isinstance(finding, dict) and isinstance(finding.get('id'), str):
            targets[finding['id']].append(finding)
    rows = bind_candidate_decisions({'candidates': list(candidates.values())}, decisions)['decisions']
    counts = Counter((r.get('origin'), r.get('pointer')) for r in rows if isinstance(r, dict)
                     and all(isinstance(r.get(k), str) for k in ('origin', 'pointer')))
    updates, pending = [], []
    for receipt in rows:
        if not isinstance(receipt, dict) or not all(isinstance(receipt.get(k), str) for k in ('origin', 'pointer')):
            continue
        key = (receipt['origin'], receipt['pointer'])
        candidate = candidates.get(key)
        if candidate is None or counts[key] != 1:
            continue
        obligations = {o['claim']['id'] for o in candidate.get('pending_subclaims', [])
                       if isinstance(o, dict) and isinstance(o.get('claim'), dict) and isinstance(o['claim'].get('id'), str)}
        claims = receipt.get('subclaims', [])
        if not isinstance(claims, list):
            continue
        ids = Counter(c.get('id') for c in claims if isinstance(c, dict) and isinstance(c.get('id'), str))
        for claim in claims:
            if not isinstance(claim, dict) or not isinstance(claim.get('id'), str) or claim['id'] not in obligations:
                continue
            if claim.get('decision') not in ('retained', 'duplicate'):
                continue
            refs, destinations = claim.get('source_refs'), claim.get('target_finding_ids')
            valid = (ids[claim['id']] == 1 and all(isinstance(claim.get(k), str) and claim[k].strip() for k in ('claim', 'reason'))
                     and isinstance(refs, list) and refs and all(isinstance(r, str) and r in packet['allowed_source_refs'] for r in refs)
                     and isinstance(destinations, list) and destinations and all(isinstance(t, str) and len(targets[t]) == 1 for t in destinations))
            if not valid:
                pending.append({'reason': 'invalid_recovered_subclaim_publication', 'claim': deepcopy(claim), 'origin': key[0], 'pointer': key[1]})
                continue
            for target in dict.fromkeys(destinations):
                update = {'origin': key[0], 'pointer': key[1], 'source_id': candidate['source_id'],
                          'claim_id': claim['id'], 'claim': claim['claim'], 'reason': claim['reason'],
                          'source_refs': list(refs),
                          'source_types': {r: 'graphic' if r in packet.get('attached_image_block_ids_in_order', []) else 'text' for r in refs},
                          'target_id': target,
                          'target_signature': target_signature(targets[target][0]), 'semantic_verified': False}
                update['id'] = hashlib.sha256(json.dumps(update, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
                updates.append(update)
    return updates, pending


def apply_addenda(findings, updates):
    index = defaultdict(list)
    for finding in findings:
        index[finding.get('id')].append(finding)
    applied, pending = [], []
    for update in updates:
        matches = index.get(update['target_id'], [])
        if len(matches) != 1:
            pending.append({'update': update, 'reason': 'ambiguous_addendum_target'})
            continue
        target = matches[0]
        original = target.get('recovery_original_description', target.get('description', ''))
        existing = target.get('recovery_addenda', [])
        valid_existing = isinstance(existing, list) and all(isinstance(a, dict)
            and all(isinstance(a.get(k), str) for k in ('claim', 'reason')) for a in existing)
        baseline = {**target, 'description': original}
        baseline_signature = target_signature(baseline)
        snapshots = target.get('recovery_addendum_snapshot_signatures', [])
        snapshots = snapshots if isinstance(snapshots, list) else []
        intact = valid_existing and (not existing or target.get('description') == render_description(original, existing))
        unchanged_base = target.get('recovery_addendum_base_signature', baseline_signature) == baseline_signature
        accepted_signatures = [baseline_signature, target_signature(target), *snapshots]
        if not isinstance(original, str) or not intact or not unchanged_base or update['target_signature'] not in accepted_signatures:
            pending.append({'update': update, 'reason': 'addendum_target_changed'})
            continue
        sources = target.get('source_finding_ids', [])
        blocks = target.get('source_block_ids', [])
        evidence = target.get('evidence', [])
        if (not isinstance(sources, list) or not all(isinstance(x, str) for x in sources)
                or not isinstance(blocks, list) or not all(isinstance(x, str) for x in blocks)
                or not isinstance(evidence, list)):
            pending.append({'update': update, 'reason': 'invalid_addendum_target_sources'})
            continue
        target['source_finding_ids'] = list(dict.fromkeys([*sources, update['source_id']]))
        target['source_block_ids'] = list(dict.fromkeys([*blocks, *update['source_refs']]))
        present = {e.get('block_id') for e in evidence if isinstance(e, dict) and isinstance(e.get('block_id'), str)}
        target['evidence'] = [*evidence, *[
            {'block_id': ref, 'type': update['source_types'][ref], 'origin': 'recovery_addendum',
             'addendum_id': update['id'], 'semantic_verified': False}
            for ref in update['source_refs'] if ref not in present]]
        target['recovery_addendum_base_signature'] = baseline_signature
        target['recovery_addendum_snapshot_signatures'] = list(dict.fromkeys([*snapshots, target_signature(target)]))
        if update not in existing:
            existing = [*existing, deepcopy(update)]
        target['recovery_original_description'] = original
        target['recovery_addenda'] = existing
        target['description'] = render_description(original, existing)
        applied.append(update)
    return {'applied': applied, 'pending': pending, 'semantic_verified': False}


def subclaim_is_emitted(final, key, claim):
    targets = claim.get('target_finding_ids', [])
    if not isinstance(targets, list) or not targets:
        return False
    for fid in targets:
        matches = [f for f in final.get('findings', []) if isinstance(f, dict) and f.get('id') == fid]
        if len(matches) != 1:
            return False
        finding = matches[0]
        addenda = finding.get('recovery_addenda', [])
        original = finding.get('recovery_original_description')
        if not isinstance(addenda, list) or not isinstance(original, str) or not addenda or not all(
                isinstance(a, dict) and all(isinstance(a.get(k), str) for k in ('claim', 'reason')) for a in addenda):
            return False
        if finding.get('description') != render_description(original, addenda):
            return False
        if not any((a.get('origin'), a.get('pointer')) == key and a.get('claim_id') == claim.get('id')
                   and a.get('claim') == claim.get('claim') and a.get('reason') == claim.get('reason') for a in addenda):
            return False
    return True


def publish_new_finding_addenda(findings, updates, local_targets, originals):
    """Bind a local publication to its unchanged normalized finding, then emit it."""
    bound, pending = [], []
    for update in updates:
        local_id = update['target_id']
        mapped = local_targets.get(local_id, [])
        source = [f for f in originals if f.get('id') == local_id]
        target = [f for f in findings if len(mapped) == 1 and f.get('id') == mapped[0]]
        if len(source) != 1 or len(target) != 1 or target_signature(source[0]) != update['target_signature']:
            pending.append({'update': update, 'reason': 'ambiguous_new_addendum_target'})
            continue
        # Normalization changes only ID and may fill problem from finding.
        restored = {**target[0], 'id': local_id, 'problem': source[0].get('problem')}
        if target_signature(restored) != update['target_signature']:
            pending.append({'update': update, 'reason': 'new_addendum_target_changed'})
            continue
        bound.append({**update, 'reported_target_id': local_id,
                      'reported_target_signature': update['target_signature'],
                      'target_id': target[0]['id'], 'target_signature': target_signature(target[0])})
    result = apply_addenda(findings, bound)
    result['pending'].extend(pending)
    return result
