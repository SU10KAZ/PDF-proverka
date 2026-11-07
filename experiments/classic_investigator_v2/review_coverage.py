"""Full-record delivery and honest review coverage; never edits audit findings."""
import copy
import hashlib
import json


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def payload(findings, authoritative_text):
    ids = [r.get('id') for r in findings]
    if any(not isinstance(i, str) or not i for i in ids) or len(ids) != len(set(ids)):
        raise ValueError('Unique finding IDs required')
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in authoritative_text.items()):
        raise ValueError('TEXT must map block IDs to exact source text')
    return {'findings': copy.deepcopy(findings),
            'finding_hashes': {r['id']: digest(r) for r in findings},
            'authoritative_text': copy.deepcopy(authoritative_text),
            'graphics_delivered': False,
            'limits': 'Complete finding records, no truncation. No graphics. Model absence_checked '
                      'is only an assertion, not proof of an executed source operation.'}


def assess(delivered, response):
    originals = delivered['findings']
    expected = {r['id']: r for r in originals}
    if {k: digest(v) for k, v in expected.items()} != delivered['finding_hashes']:
        raise ValueError('Delivered finding changed')
    reviews = response.get('reviews', []) if isinstance(response, dict) else []
    if not isinstance(reviews, list):
        raise ValueError('Reviews must be an array')
    indexed = {}
    for review in reviews:
        fid = review.get('finding_id') if isinstance(review, dict) else None
        if fid not in expected or fid in indexed:
            raise ValueError('Unknown or duplicate finding review')
        indexed[fid] = review
    rows = []
    for fid, original in expected.items():
        review = indexed.get(fid)
        issues = []
        if review is None:
            status = 'not_reviewed'
        else:
            verdict = review.get('verdict')
            if verdict not in {'supported', 'refuted', 'mixed', 'needs_data'}:
                issues.append('missing_or_invalid_verdict')
            if not isinstance(review.get('reason'), str) or not review['reason'].strip():
                issues.append('missing_reason')
            evidence = review.get('evidence', [])
            if not isinstance(evidence, list):
                issues.append('invalid_evidence_array')
                evidence = []
            for item in evidence:
                if not isinstance(item, dict):
                    issues.append('invalid_evidence_item')
                    continue
                text = delivered['authoritative_text'].get(item.get('block_id'))
                quote = item.get('quote')
                if item.get('kind') != 'TEXT' or text is None or not isinstance(quote, str) or not quote or quote not in text:
                    issues.append('unbound_source_quote')
            if verdict in {'supported', 'refuted', 'mixed'} and not evidence:
                issues.append('missing_source_evidence')
            if issues:
                status = 'invalid_review'
            elif verdict == 'needs_data':
                status = 'needs_source_followup'
            else:
                status = 'source_quoted_requires_semantic_adjudication'
        rows.append({'finding_id': fid, 'original_sha256': digest(original),
                     'status': status, 'issues': issues, 'review': copy.deepcopy(review),
                     'technical_validity_verified': False, 'may_downgrade': False})
    return {'rows': rows, 'all_findings_addressed': len(indexed) == len(expected),
            'all_reviews_have_source_quotes': bool(rows) and all(
                r['status'] == 'source_quoted_requires_semantic_adjudication' for r in rows),
            'technical_review_complete': False,
            'limits': 'Literal quote binding does not establish object identity, interpretation, '
                      'whole-document absence or correctness. Never apply whole-row deletion '
                      'or downgrade from this coverage report.'}
