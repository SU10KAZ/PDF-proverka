"""Offline display candidate; does not approve grouping or resolve inherited issues."""
from copy import deepcopy
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def expose_group_scope_notes(finding):
    """Keep reviewed inherited limitations visible, retaining all original member text.

    Call only on a source-reviewed grouping preview. This is presentation, not
    evidence validation or an admission decision. Invalid/stale previews fail closed.
    """
    provenance = finding.get('grouping_provenance', {})
    decision = provenance.get('decision', {})
    proposal = provenance.get('proposal', {})
    members = finding.get('grouping_original_findings', [])
    review = decision.get('source_review', {})
    if (not members or decision.get('action') != 'group' or review.get('action') != 'group'
        or decision.get('members_sha256') != evidence_digest(members)
        or decision.get('proposal_sha256') != evidence_digest(proposal)):
        raise ValueError('unbound_group_preview')
    binding = review.get('binding', {})
    if any(binding.get(k) != decision.get(k) for k in ('members_sha256','proposal_sha256','source_md_sha256')):
        raise ValueError('stale_group_source_review')
    issues = review.get('inherited_member_issues', [])
    if not isinstance(issues, list):
        raise ValueError('invalid_group_scope_notes')
    ids = {m['id'] for m in members}
    if len(ids) != len(members):
        raise ValueError('ambiguous_group_members')
    lines = []
    for issue in issues:
        if (not isinstance(issue, dict) or not isinstance(issue.get('member_ids'), list)
            or not issue['member_ids'] or any(i not in ids for i in issue['member_ids'])
            or not isinstance(issue.get('issue'), str) or not issue['issue'].strip()
            or not isinstance(issue.get('evidence'), list) or not issue['evidence']):
            raise ValueError('unbound_group_scope_note')
        lines.append('[' + ', '.join(issue['member_ids']) + '] ' + issue['issue'])
    if not lines:
        return deepcopy(finding)
    prefix = 'Ограничения проверки отдельных замечаний:\n' + '\n'.join(lines) + '\n\n'
    before = finding.get('description')
    if not isinstance(before, str) or not before:
        raise ValueError('missing_member_descriptions')
    result = deepcopy(finding)
    if not before.startswith(prefix):
        result['description'] = prefix + before
    # Existing report concatenates description and norm; do not silently truncate.
    if len(result['description']) + len(str(result.get('norm', ''))) + 8 > 32767:
        raise ValueError('visible_group_notes_exceed_excel_limit')
    return result
