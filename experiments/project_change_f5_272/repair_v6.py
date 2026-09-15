"""Offline immutable F5 V6: saved regression nine gate, then frozen 79."""
import argparse
from collections import Counter
from pathlib import Path
import json
import sys

from .common import ROOT, AccessAudit, admit, code_hashes, fingerprint, write
from .repair_v2 import file_hashes
from .repair_v4 import OUT as V4
from .diagnose_v5 import OUT as V5
from .package_v6 import one_package
from experiments.project_change_272.inventory import sha

OUT = ROOT / 'fresh_dev_sample_f5_pipeline_v6'
PREVIOUS = (V4, V5)


def read(path):
    return json.loads(Path(path).read_text())


def offline(event, args):
    if event in {'socket.connect','socket.getaddrinfo','subprocess.Popen','os.system'}:
        raise PermissionError('V6 forbids model calls, network and subprocesses')
    if event == 'open' and isinstance(args[0], (str, bytes, Path)):
        path = Path(args[0]).resolve()
        mode, flags = args[1:3]
        writing = (isinstance(mode,str) and any(c in mode for c in 'wax+')) or bool(flags & 3)
        if writing and any(path.is_relative_to(p) for p in PREVIOUS):
            raise PermissionError('V4/V5 artifacts are immutable')


def snapshot(output):
    if output.exists():
        raise FileExistsError('V6 output must be new')
    write(output/'PREVIOUS_FILE_HASHES.json',{str(p):file_hashes(p) for p in PREVIOUS})
    manifest = read(V5/'REGRESSION_9_MANIFEST.json')
    write(output/'REGRESSION_9_MANIFEST.json',manifest)
    write(output/'INPUT_BINDINGS.json',dict(manifest_sha256=sha(V5/'REGRESSION_9_MANIFEST.json'),
        roster_sha256=sha(V4/'ROSTER.json'),prior_exposure='Previously audited DEV; not newly blind'))
    write(output/'ROSTER.json',read(V4/'ROSTER.json'))
    # Frozen mechanical baseline only. It is never an input to source_build.
    index = read(V4/'PACKAGE_INDEX.json')
    for row in index:
        write(output/'baseline'/row['package'], read(V4/row['package']))
    write(output/'baseline/PACKAGE_INDEX.json',index)
    for path in sorted((V4/'_inventory_cache').glob('pair_*/DOCUMENT_INVENTORY_*.json')):
        write(output/'_inventory_cache'/path.relative_to(V4/'_inventory_cache'),read(path))
    write(output/'INVENTORY_BINDINGS.json',{str(p.relative_to(output)):sha(p)
        for p in sorted((output/'_inventory_cache').glob('pair_*/*.json'))})


class BuildAccess(AccessAudit):
    def check(self,event,args):
        if event=='open' and isinstance(args[0],(str,bytes,Path)):
            p=Path(args[0]).resolve()
            if p.is_relative_to(self.output/'baseline') or p.name in {'REGRESSION_ROOT_CAUSE.json','SOURCE_IDENTITY_REVIEW.json'}:
                self.denied.append(str(p))
                raise PermissionError('Baseline/diagnostic content cannot inform the builder')
        super().check(event,args)


def source_build(output, all_packages=False):
    from .subject_discovery import discover
    from .subject_correspondence import correspond
    from .subject_v4 import refine_subjects
    from .boundary_sources import Document
    code=code_hashes()
    for relative, digest in read(output/'INVENTORY_BINDINGS.json').items():
        if sha(output/relative)!=digest:
            raise ValueError('Frozen source inventory content drift')
    if all_packages:
        if read(output/'REGRESSION_9_REPLAY.json')['status']!='PASS':
            raise PermissionError('Regression nine must pass before all 79')
        if read(output/'ALGORITHM_FREEZE.json')['code']!=code:
            raise ValueError('Code changed after regression freeze')
    target=output if all_packages else output/'regression_9'
    if (target/'PACKAGES_FREEZE.json').exists():
        raise FileExistsError('Build already frozen')
    # Read only saved IDs before installing the source-only build guard.
    selected={r['package_id'] for r in read(output/'REGRESSION_9_MANIFEST.json')['records']}
    roster=read(output/'ROSTER.json')
    pairs=admit()
    access=BuildAccess(pairs,output);access.install()
    prepared=[]
    for pair in pairs:
        invs={s:read(output/f'_inventory_cache/pair_{pair["index"]}/DOCUMENT_INVENTORY_{s.upper()}.json') for s in ('old','new')}
        for side,inv in invs.items():
            if inv['document_version']!=pair[side]['document_version'] or inv['source']!=pair[side]['artifacts']:
                raise ValueError('Frozen source inventory binding drift')
        indices={s:discover(inv) for s,inv in invs.items()}
        prepared.append(dict(pair=pair,inventories=invs,indices=indices,
            subjects={x['subject_id']:x for index in indices.values() for x in index['subjects']},
            correspondence=correspond(indices['old']['subjects'],indices['new']['subjects'])))
    actual={c['candidate_id'] for p in prepared for c in p['correspondence']['candidates']}
    if len(actual)>80:
        write(output/'STOP.json',dict(status='PACKAGE_EXPLOSION',packages=len(actual)))
        raise SystemExit('PACKAGE_EXPLOSION')
    if actual!={r['package_id'] for r in roster['all']}:
        raise ValueError('Frozen package roster changed')
    packages={};guards_all=[];canonical_all={}
    for p in prepared:
        docs={s:Document(inv) for s,inv in p['inventories'].items()}
        canonical,_,guards=refine_subjects(p,docs)
        canonical_all.update(canonical);guards_all.extend(guards)
        for side,inv in p['inventories'].items():
            write(target/f'pair_{p["pair"]["index"]}/DOCUMENT_INVENTORY_{side.upper()}.json',inv)
        for candidate in p['correspondence']['candidates']:
            if not all_packages and candidate['candidate_id'] not in selected:
                continue
            one=one_package(p,docs,canonical,guards,candidate,target)
            two=one_package(p,docs,canonical,guards,candidate,target)
            if one!=two:
                raise ValueError('Nondeterministic rebuild')
            rel=f'pair_{p["pair"]["index"]}/packages/{candidate["candidate_id"]}.json'
            write(target/rel,one);packages[rel]=one
            print(json.dumps(dict(package=rel,completeness=one['completeness']),ensure_ascii=False),flush=True)
    if code!=code_hashes():
        raise ValueError('Code changed during build')
    hashes={rel:p['package_hash'] for rel,p in packages.items()}
    if all_packages and any(hashes.get(rel)!=h for rel,h in read(output/'regression_9/PACKAGES_FREEZE.json')['package_hashes'].items()):
        raise ValueError('Regression packages differ in full rebuild')
    write(target/'PACKAGES_FREEZE.json',dict(package_hashes=hashes,aggregate_hash=fingerprint(hashes),passes=2))
    write(target/'PACKAGE_INDEX.json',[dict(package=rel,candidate_id=p['candidate_subject']['candidate_id'],
        confidence=p['candidate_subject']['confidence'],completeness=p['completeness'],package_hash=p['package_hash']) for rel,p in packages.items()])
    write(target/'SUBJECT_INDEX.json',canonical_all)
    write(target/'WRONG_DOCUMENT_AUDIT.json',dict(pages=guards_all,mismatches=[g for g in guards_all if g['usable']=='NO']))
    write(target/'BUILD_ACCESS.json',dict(reads=sorted(access.reads),denied=access.denied,model_calls=0,
        source_truth_read=False,validation='NOT OPENED',final_holdout='NOT OPENED',other_projects='NO',
        source_inventory='Immutable V4 inventory, hash-bound to admitted DEV sources; subject refinement rerun'))
    write(target/'HASH_STABILITY.json',dict(status='PASS',passes=2,packages=len(packages),aggregate_hash=fingerprint(hashes)))
    if not all_packages:
        write(output/'ALGORITHM_FREEZE.json',dict(code=code))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('snapshot','regression-build','regression-audit','all-build','audit'))
    parser.add_argument('--output',type=Path,default=OUT)
    args=parser.parse_args();output=args.output.resolve()
    if any(output==p or output.is_relative_to(p) for p in PREVIOUS):
        raise PermissionError('Prior artifacts are immutable')
    sys.addaudithook(offline)
    if args.action=='snapshot':snapshot(output)
    elif args.action in {'regression-build','all-build'}:source_build(output,args.action=='all-build')
    else:
        from .audit_v6 import regression_audit,final_audit
        (regression_audit if args.action=='regression-audit' else final_audit)(output)


if __name__=='__main__':main()
