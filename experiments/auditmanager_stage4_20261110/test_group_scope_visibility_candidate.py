import copy
import json
from pathlib import Path
import sys
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from experiments.auditmanager_stage4_20261110.group_scope_visibility_candidate import expose_group_scope_notes

BASE = Path(__file__).parent/'grouping_scope_pair_evidence'

@pytest.fixture(params=['AK_00','EOM_01'])
def grouped(request):
    data = json.loads((BASE/request.param/'output_preview.json').read_text())
    return next(f for f in data['findings'] if f.get('grouping_original_findings'))


def test_real_inherited_limits_visible_and_members_unchanged(grouped):
    original = copy.deepcopy(grouped)
    revised = expose_group_scope_notes(grouped)
    assert grouped == original
    assert revised['description'].endswith(original['description'])
    issues = grouped['grouping_provenance']['decision']['source_review']['inherited_member_issues']
    for issue in issues:
        assert issue['issue'] in revised['description']
    assert {k:v for k,v in revised.items() if k != 'description'} == {k:v for k,v in original.items() if k != 'description'}
    assert expose_group_scope_notes(revised) == revised


def test_changed_member_cannot_reuse_review(grouped):
    grouped['grouping_original_findings'][0]['description'] += ' changed'
    with pytest.raises(ValueError, match='unbound_group_preview'):
        expose_group_scope_notes(grouped)


def test_foreign_member_issue_not_inserted(grouped):
    grouped['grouping_provenance']['decision']['source_review']['inherited_member_issues'][0]['member_ids'] = ['foreign-id']
    with pytest.raises(ValueError, match='unbound_group_scope_note'):
        expose_group_scope_notes(grouped)


def test_excel_limit_does_not_hide_members(grouped):
    grouped['description'] = 'x' * 32760
    with pytest.raises(ValueError, match='visible_group_notes_exceed_excel_limit'):
        expose_group_scope_notes(grouped)
