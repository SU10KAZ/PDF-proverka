"""Prepare evaluator-only, unscored ledgers after experimental output sealing."""
import argparse
import json
from pathlib import Path
from .score_atomic_review import prepare, sha
from .verify_output_review_history import encode
from .verify_experimental_seals import verify


def materialize(root, destination):
    root=Path(root).resolve();destination=Path(destination).resolve()
    # Never place benchmark-bearing evaluation files within model work directories.
    if destination==root or destination.is_relative_to(root):
        raise ValueError('Evaluator output must be outside discovery root')
    trial=json.loads((root/'experimental_collection_trial.json').read_text())
    evaluator=Path(trial['evaluator']['path'])
    if sha(evaluator)!=trial['evaluator']['sha256']:raise ValueError('Evaluator changed')
    seals=verify(root);prepared=[]
    for sealed in seals['verified_runs']:
        key=f"{sealed['discipline']}_r{sealed['repeat']}_{sealed['variant']}"
        target=destination/key
        ledger_path=target/'coverage_ledger.json';receipt_path=target/'coverage_initial_receipt.json'
        if ledger_path.exists() or receipt_path.exists():
            if not ledger_path.is_file() or not receipt_path.is_file():raise ValueError('Incomplete existing evaluation: '+key)
            receipt=json.loads(receipt_path.read_text());ledger=json.loads(ledger_path.read_text())
            if any(receipt[k]!=sealed[k] for k in ['final_sha256','variant','discipline','repeat']):raise ValueError('Existing evaluation bound to another run')
            if receipt['evaluator_sha256']!=sha(evaluator) or ledger['evaluator_sha256']!=sha(evaluator) or ledger['findings_sha256']!=sealed['final_sha256']:raise ValueError('Existing evaluation binding changed')
            prepared.append({'run':key,'ledger_path':str(ledger_path),'status':'preserved_existing_review'})
            continue
        ledger=prepare(evaluator,sealed['final_path'],sealed['discipline'])
        ledger.update(repeat=sealed['repeat'],variant=sealed['variant'])
        assert all(t['coverage']=='unchecked' and not t['finding_ids'] for t in ledger['targets'])
        import hashlib
        receipt={**sealed,'evaluator_sha256':sha(evaluator),'trial_sha256':seals['trial_sha256'],'initial_ledger_sha256':hashlib.sha256(encode(ledger)).hexdigest(),'production_acceptance':False}
        target.mkdir(parents=True,exist_ok=True)
        with ledger_path.open('xb') as f:f.write(encode(ledger))
        with receipt_path.open('xb') as f:f.write(encode(receipt))
        prepared.append({'run':key,'ledger_path':str(ledger_path),'status':'prepared_unreviewed','targets':len(ledger['targets'])})
    return {'status':'evaluation_preparation_only','prepared':prepared,'sealed_runs':len(seals['verified_runs']),'pending_records':seals['pending_records'],'not_dispatched':seals['not_dispatched'],'model_calls':0,'production_acceptance':False}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--destination',type=Path,required=True);a=p.parse_args()
    print(json.dumps(materialize(a.root,a.destination),ensure_ascii=False))
