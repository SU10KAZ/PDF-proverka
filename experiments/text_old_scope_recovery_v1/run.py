"""Read-only input preparation and freeze for OLD scope recovery."""
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
import argparse
import shutil
import subprocess
from experiments.text_comparison_v1.common import read,write,file_hash,digest
from experiments.project_change_text_v1.run import verify as verify_base
from .retrieval import packet,priority
from .source_pool import build

BASE=Path('/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1')
ROOT=Path('/home/coder/auditmanager/corpus-audits/20260913_text_old_scope_recovery_v1')
REPO=Path(__file__).resolve().parents[2]


def prepare(root=ROOT):
    verify_base(BASE)
    project=read(BASE/'reports/PROJECT_TEXT_CHANGES.json');pairs={p['pair_key']:p for p in read(BASE/'FROZEN_PROJECT_INPUTS.json')['pairs']}
    inventory=[];packets=[];documents={};expanded={}
    for c in project['project_changes']:
        if c['status']!='REVIEW':continue
        level,reasons=priority(c)
        inventory.append(dict(project_change_id=c['project_change_id'],priority=level,priority_reasons=reasons,
                              new_statement=c['new_state'],comparison_scope=c['comparison_scope'],initial_review_reasons=c['review_reasons']))
        if level!='HIGH':continue
        pair=pairs[c['comparison_scope']]
        for side in ('old','new'):
            d=pair[side];v=d['document_version']
            if v not in documents:documents[v]=read(BASE/'run1/documents'/v/'narrative.json')
            if v not in expanded:
                expanded[v]=build(d);write(root/'documents'/v/'scope_pool.json',expanded[v])
        p=packet(c,expanded[pair['old']['document_version']]['units'],documents[pair['new']['document_version']]['units'],expanded[pair['new']['document_version']]['units'])
        write(root/'packets'/(c['project_change_id']+'.json'),p);packets.append(p)
    write(root/'reports/PRIORITY_INVENTORY.json',inventory)
    write(root/'reports/PREPARE_METRICS.json',dict(initial_review=len(inventory),priorities=dict(Counter(x['priority'] for x in inventory)),
          packets=len(packets),local_text_characters=sum(p['local_text_characters'] for p in packets),max_local_text_characters=max(p['local_text_characters'] for p in packets),
          candidates=dict(Counter(len(p['old_candidates']) for p in packets))))
    rows=['# REVIEW priority inventory','','Priority assigned from NEW content only, before OLD retrieval decisions. HIGH is the execution cohort; MEDIUM/LOW remain unchanged.','',
          '| Candidate | Priority | Reason | NEW statement |','|---|---|---|---|']
    for r in sorted(inventory,key=lambda r:({'HIGH':0,'MEDIUM':1,'LOW':2}[r['priority']],r['project_change_id'])):
        rows.append(f"| {r['project_change_id']} | {r['priority']} | {', '.join(r['priority_reasons'])} | {r['new_statement'].replace('|','/')} |")
    (root/'reports/REVIEW_PRIORITY_INVENTORY.md').write_text('\n'.join(rows)+'\n')
    print(read(root/'reports/PREPARE_METRICS.json'))


def freeze(root=ROOT):
    target=root/'reports/CANDIDATE_MANIFEST.json'
    if target.exists():raise ValueError('Already frozen')
    base=verify_base(BASE)
    m=dict(schema='old-scope-recovery-candidate.v1',architecture_iteration=2,maximum_architecture_iterations=3,
           frozen_at=datetime.now(timezone.utc).isoformat(),base_candidate_hash=base['candidate_content_hash'],
           base_project_sha256=file_hash(BASE/'reports/PROJECT_TEXT_CHANGES.json'),
           code_files={str(p.relative_to(REPO)):file_hash(p) for p in sorted(Path(__file__).parent.glob('*.py'))},
           protected_base_files=base['code_files'],inventory_sha256=file_hash(root/'reports/PRIORITY_INVENTORY.json'),
           packet_hashes={p.name:file_hash(p) for p in sorted((root/'packets').glob('*.json'))},
           scope_pool_hashes={str(p.relative_to(root)):file_hash(p) for p in sorted((root/'documents').glob('*/scope_pool.json'))},
           candidate_commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
           projectchange_schema_changed=False,grouping_changed=False,table_compared=False,graphic_compared=False)
    m['candidate_hash']=digest(m);write(target,m);print(m['candidate_hash'])


def verify(root=ROOT):
    m=read(root/'reports/CANDIDATE_MANIFEST.json');verify_base(BASE)
    assert file_hash(BASE/'reports/PROJECT_TEXT_CHANGES.json')==m['base_project_sha256']
    for p,h in m['code_files'].items():assert file_hash(REPO/p)==h,p
    for p,h in m['packet_hashes'].items():assert file_hash(root/'packets'/p)==h,p
    for p,h in m['scope_pool_hashes'].items():assert file_hash(root/p)==h,p
    assert file_hash(root/'reports/PRIORITY_INVENTORY.json')==m['inventory_sha256']
    return m


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['prepare','freeze','verify']);p.add_argument('--root',type=Path,default=ROOT)
    a=p.parse_args();globals()[a.command](a.root)
