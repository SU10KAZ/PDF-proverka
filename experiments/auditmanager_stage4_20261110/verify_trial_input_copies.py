"""Check all prepared copies and per-arm configuration without model dispatch."""
import argparse
import json
from pathlib import Path
from .materialize_trial_inputs import sha


def verify(root):
    layout=json.loads((root/'layout.json').read_text());seen=set();sources={};count=0;receipts=[];preflights={}
    for variant in layout['arms']:
        path=root/(variant+'_input_copy_receipt.json')
        receipt=json.loads(path.read_text());assert len(receipt['runs'])==18
        receipts.append({'path':str(path),'sha256':sha(path)})
        p=root/f'ak_r1_{variant}'/'transport_preflight.json'
        report=json.loads(p.read_text())
        assert report['status']=='shared_transport_preflight_passed' and report['model_calls']==0
        assert report['cross_arm_imports']=={} and report['variant']==variant
        preflights[variant]=report
    baseline=preflights['classic']
    for report in preflights.values():
        for field in ('stage_models','reasoning_effort','max_request_attempts','retry_delay_seconds'):
            assert report[field]==baseline[field],field
    expected_callbacks={'classic':set(),'classic_current':set(),'inventory':{'inventory_invoke'},
        'source_review':{'source_review_invoke','source_final_check_invoke'},
        'candidate':{'inventory_invoke','source_review_invoke','source_final_check_invoke'}}
    for variant,r in preflights.items():
        assert {k for k in r['context_option_keys'] if k.endswith('_invoke')}==expected_callbacks[variant]
    for run in layout['runs']:
        work=Path(run['work']);assert not (work/'classic_control/protocol.json').exists()
        manifest=json.loads((work/'classic_control/input_manifest.json').read_text())[0]
        version=Path(manifest['version_dir']);assert str(version)==run['proposed_version_dir']
        actual={str(p.relative_to(version)) for p in version.rglob('*') if p.is_file()}
        assert actual==set(manifest['files'])
        assert not any(p.is_symlink() for p in version.rglob('*'))
        source=layout['sources_observed_now_not_yet_trial_frozen'][run['discipline']]
        expected={n.replace('03_analysis/runs/candidate/','03_analysis/runs/comparison/',1):h for n,h in source['files'].items()}
        assert manifest['files']==expected
        for name,digest in expected.items():
            p=version/name;assert sha(p)==digest
            key=(p.stat().st_dev,p.stat().st_ino);assert key not in seen;seen.add(key)
            original=Path(source['root'])/name.replace('03_analysis/runs/comparison/','03_analysis/runs/candidate/',1)
            assert key!=(original.stat().st_dev,original.stat().st_ino)
            count+=1
        assert sources.setdefault(run['discipline'],expected)==expected
    return {'status':'input_copies_and_arm_configuration_verified_not_trial_ready',
        'runs':len(layout['runs']),'rehash_count':count,'distinct_file_inodes':len(seen),
        'equal_inputs_across_arms_and_repeats':True,'shared_source_hardlinks':False,
        'copy_receipts':receipts,'arm_preflights':preflights,
        'model_calls':0,'protocols_created':False,'stage4_complete':False,
        'limits':['No independent audit result or engineer assessment exists for these prepared slots.',
                  'Current implementation is not frozen; subsequent edits require new preflight and freeze.',
                  'Source copy isolation is not proof of every filesystem permission boundary in a future executor.']}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);a=p.parse_args()
    result=verify(a.root)
    Path(__file__).with_name('all_trial_input_copies_check.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ['status','runs','rehash_count','distinct_file_inodes','model_calls']}))
