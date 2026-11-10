"""Apply source-bound, explicitly reviewed decisions after verifier phases.

Raw model verdicts never constitute admission decisions. The optional local inbox
contains separate semantic adjudication, including scope and defect-basis roles.
Unreviewed findings stay unchanged; deferred/suppressed originals remain in the
same document for traceability. No provider calls occur here.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from .source_review_results import validate_source_review
from .source_review_evidence import evidence_digest


def _check_decision(response, decision):
    if decision.get('response_sha256') != evidence_digest(response):
        raise ValueError('stale_semantic_decision')
    if decision.get('origin') != 'explicit_source_review':
        raise ValueError('explicit_semantic_review_required')
    for key in ('reviewer', 'reason'):
        if not isinstance(decision.get(key), str) or not decision[key].strip():
            raise ValueError('review_provenance_required')
    claims = {c['claim_id']:c for c in response['claims']}
    reviews = decision.get('claim_reviews', [])
    if not isinstance(reviews,list) or len(reviews)!=len(claims):
        raise ValueError('incomplete_claim_review')
    seen=set(); bases=[]
    for r in reviews:
        cid=r.get('claim_id')
        if cid not in claims or cid in seen:
            raise ValueError('ambiguous_claim_review')
        seen.add(cid)
        if r.get('role') not in ('defect_basis','context','recommendation'):
            raise ValueError('claim_role_required')
        if r.get('verdict') not in ('supported','refuted','needs_data','not_checked'):
            raise ValueError('invalid_reviewed_verdict')
        if not isinstance(r.get('reason'),str) or not r['reason'].strip():
            raise ValueError('claim_review_reason_required')
        if r['verdict'] in ('supported','refuted'):
            if r.get('same_scope_verified') is not True or r.get('evidence_sufficiency_verified') is not True:
                raise ValueError('claim_scope_or_evidence_not_verified')
            if not claims[cid].get('evidence'):
                # Conditional consequences/recommendations can be reasoned from
                # directly evidenced premises. Require explicit one-hop links;
                # this verifies traceability, not the truth of the inference.
                refs = r.get('supporting_claim_ids', [])
                if (r['verdict'] != 'supported' or r['role'] == 'defect_basis'
                        or not isinstance(refs, list) or not refs
                        or any(not isinstance(ref, str) for ref in refs)
                        or len(set(refs)) != len(refs)):
                    raise ValueError('conclusive_claim_without_evidence')
                for ref in refs:
                    matches = [p for p in reviews if p.get('claim_id') == ref]
                    if (ref == cid or ref not in claims or len(matches) != 1
                            or matches[0].get('verdict') != 'supported'
                            or matches[0].get('same_scope_verified') is not True
                            or matches[0].get('evidence_sufficiency_verified') is not True
                            or not claims[ref].get('evidence')):
                        raise ValueError('unverified_derived_claim_premise')
        if r['role']=='defect_basis':bases.append(r)
    if not bases:raise ValueError('defect_basis_required')
    action=decision.get('action')
    if action not in ('retain','revise','suppress','defer','question'):
        raise ValueError('invalid_action')
    if action=='suppress' and not all(r['verdict']=='refuted' for r in bases):
        raise ValueError('unrefuted_defect_basis_cannot_be_suppressed')
    if action in ('retain','revise') and not any(r['verdict']=='supported' for r in bases):
        raise ValueError('supported_defect_basis_required')
    if action=='retain' and any(r['verdict']!='supported' for r in reviews):
        raise ValueError('mixed_basis_requires_revision')
    if action in ('revise','question'):
        fields=decision.get('approved_fields')
        if not isinstance(fields,dict) or set(fields)!= {'problem','description','solution'} or any(not isinstance(v,str) or not v.strip() for v in fields.values()):
            raise ValueError('explicit_final_wording_required')
        if decision.get('final_wording_verified') is not True:
            raise ValueError('final_wording_not_verified')
    if action=='defer' and any(r['verdict']=='supported' for r in bases):
        raise ValueError('supported_core_must_be_preserved')
    if action=='question':
        if any(r['verdict']=='supported' for r in bases):
            if (decision.get('question_basis') not in ('minor_confirmed','outside_profile')
                    or not isinstance(decision.get('profile_reason'),str)
                    or not decision['profile_reason'].strip()):
                raise ValueError('supported_core_requires_explicit_profile_decision')
        elif decision.get('context_search_completed') is not True or decision.get('engineer_action_required') is not True:
            raise ValueError('internal_context_gap_is_not_engineer_question')
    return action


DISPLAY_REVIEW_FIELDS = ('risk', 'consequence', 'severity', 'norm', 'norm_quote')


def _review_display_fields(original, decision, action):
    """Require explicit consistency review of ancillary report text before applying."""
    if action not in ('retain', 'revise', 'question'):
        return {}
    required = {k for k in DISPLAY_REVIEW_FIELDS if original.get(k) not in (None, '')}
    reviews = decision.get('display_field_reviews', {})
    if isinstance(reviews, list):
        # Some providers emit entries as an array. Bind only by the exact,
        # unique original-value hash; never guess by order or revised wording.
        bound = {}
        for row in reviews:
            if not isinstance(row, dict):
                raise ValueError('ambiguous_display_field_review')
            matches = [k for k in required if evidence_digest(original[k]) == row.get('original_value_sha256')]
            if len(matches) != 1 or matches[0] in bound:
                raise ValueError('ambiguous_display_field_review')
            bound[matches[0]] = row
        reviews = bound
    if not isinstance(reviews, dict) or set(reviews) != required:
        raise ValueError('incomplete_display_field_review')
    updates = {}
    for key in required:
        row = reviews[key]
        if not isinstance(row, dict) or row.get('original_value_sha256') != evidence_digest(original[key]):
            raise ValueError('stale_display_field_review')
        if (row.get('consistent_with_final_claim') is not True
                or not isinstance(row.get('reason'), str) or not row['reason'].strip()):
            raise ValueError('display_field_consistency_unverified')
        value = row.get('value')
        if 'value' not in row or not isinstance(value, str):
            raise ValueError('explicit_display_field_value_required')
        if key == 'severity' and not value.strip():
            raise ValueError('severity_cannot_be_empty')
        if action == 'retain' and value != original[key]:
            raise ValueError('changed_display_field_requires_revision')
        updates[key] = value
    return updates


def apply_review_admissions(document, entries, md):
    """Return a new document and per-entry receipts, preserving all originals."""
    if not isinstance(document,dict) or not isinstance(document.get('findings'),list):
        raise ValueError('unsupported_findings_container')
    if not isinstance(entries,list):raise ValueError('invalid_review_inbox')
    for key in ('source_review_history','suppressed_findings','pending_source_findings'):
        if key in document and not isinstance(document[key],list):
            raise ValueError('invalid_archive_container')
    result=deepcopy(document); receipts=[]
    counts={}
    for entry in entries:
        fid=entry.get('response',{}).get('finding_id') if isinstance(entry,dict) else None
        if isinstance(fid,str):counts[fid]=counts.get(fid,0)+1
    for entry in entries:
        try:
            if not isinstance(entry,dict):raise ValueError('invalid_review_entry')
            response=entry['response'];fid=response['finding_id'];decision=entry['decision']
            if counts.get(fid)!=1:raise ValueError('duplicate_finding_decisions')
            matches=[f for f in result['findings'] if f.get('id')==fid]
            if len(matches)!=1:raise ValueError('finding_missing_or_ambiguous')
            original=matches[0]
            validation=validate_source_review(response,entry['queue'],[original],md)
            if validation['errors']:raise ValueError('invalid_source_receipt: '+','.join(validation['errors']))
            action=_check_decision(response,decision)
            display_updates=_review_display_fields(original,decision,action)
            history={'finding_id':fid,'original_finding':deepcopy(original),'decision':deepcopy(decision),
                     'response':deepcopy(response),'source_md_sha256':hashlib.sha256(md.encode()).hexdigest()}
            # Only mutate once all checks have passed for this entry.
            result.setdefault('source_review_history',[]).append(history)
            if action in ('suppress','defer'):
                result['findings']=[f for f in result['findings'] if f is not original]
                archive='suppressed_findings' if action=='suppress' else 'pending_source_findings'
                result.setdefault(archive,[]).append(deepcopy(original))
            else:
                if action in ('revise','question'):original.update(decision['approved_fields'])
                original.update(display_updates)
                original['report_section']='questions' if action=='question' else 'technical_findings'
                original['source_evidence_review_status']='reviewed_question' if action=='question' else 'reviewed_supported'
                original['source_review_response_sha256']=evidence_digest(response)
            receipts.append({'finding_id':fid,'status':'applied','action':action})
        except (KeyError,ValueError,TypeError,AttributeError) as exc:
            receipts.append({'finding_id':entry.get('response',{}).get('finding_id') if isinstance(entry,dict) and isinstance(entry.get('response'),dict) else None,
                             'status':'not_applied','reason':str(exc)})
    return result,receipts


def run_review_admission_phase(output_dir, md):
    directory=Path(output_dir);inbox=directory/'03_source_review_admissions.json'
    if not inbox.exists():return {'applied':0,'not_applied':0,'skipped':True}
    entries=json.loads(inbox.read_text())
    path=directory/'03_findings.json';before=json.loads(path.read_text())
    after,receipts=apply_review_admissions(before,entries,md)
    if after!=before:
        temporary=path.with_suffix('.source_review.tmp')
        temporary.write_text(json.dumps(after,ensure_ascii=False,indent=2))
        temporary.replace(path)
    (directory/'03_source_review_admission_results.json').write_text(json.dumps(receipts,ensure_ascii=False,indent=2))
    return {'applied':sum(r['status']=='applied' for r in receipts),
            'not_applied':sum(r['status']=='not_applied' for r in receipts),'skipped':False}
