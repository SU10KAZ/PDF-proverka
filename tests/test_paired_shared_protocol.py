import importlib.util
import json
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('paired_protocol',Path(__file__).resolve().parents[1]/'experiments/auditmanager_stage4_20261110/paired_protocol.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)


def fixture(tmp_path):
    repo=tmp_path/'repo';source=tmp_path/'source';work=tmp_path/'work'
    for name in module.SHARED_FILES:
        path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(name)
    path=work/'classic_control/protocol.json';path.parent.mkdir(parents=True)
    contract={'variant':'classic','source_root':str(source.resolve()),'source_files':module.shared_manifest(repo)}
    path.write_text(json.dumps({'shared_execution':contract}))
    return repo,source,work,path,contract


def test_identical_shared_harness_passes_without_backend_import(tmp_path):
    repo,source,work,path,contract=fixture(tmp_path)
    assert module.verify_shared_protocol(repo,source,work,'classic')==contract

@pytest.mark.parametrize('name',module.SHARED_FILES)
def test_each_changed_shared_dependency_blocks_execution(tmp_path,name):
    repo,source,work,path,contract=fixture(tmp_path)
    (repo/name).write_text('changed')
    with pytest.raises(ValueError,match='Shared harness changed'):
        module.verify_shared_protocol(repo,source,work,'classic')

@pytest.mark.parametrize('bad',['missing','incomplete','variant','root'])
def test_incomplete_or_wrong_arm_contract_rejected(tmp_path,bad):
    repo,source,work,path,contract=fixture(tmp_path)
    if bad=='incomplete':contract['source_files'].pop(module.SHARED_FILES[0])
    if bad=='variant':contract['variant']='candidate'
    if bad=='root':contract['source_root']=str(repo)
    path.write_text(json.dumps({} if bad=='missing' else {'shared_execution':contract}))
    with pytest.raises(ValueError):module.verify_shared_protocol(repo,source,work,'classic')


def trial_fixture(tmp_path):
    import hashlib
    repo,source,work,path,contract=fixture(tmp_path)
    manifest=work/'classic_control/input_manifest.json';manifest.write_text('[]')
    evaluator=tmp_path/'evaluator.json';evaluator.write_text('{"answers":"evaluator only"}')
    acceptance=tmp_path/'acceptance.json';acceptance.write_text('{"criteria":"frozen"}')
    digest=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    trial={'status':'frozen_comparative_execution',
        'evaluator':{'path':str(evaluator),'sha256':digest(evaluator)},
        'acceptance_protocol':{'path':str(acceptance),'sha256':digest(acceptance)},
        'runs':[{'work':str(work.resolve()),'variant':'classic','discipline':'AK','repeat':1,'input_manifest_sha256':digest(manifest)}]}
    trial_path=tmp_path/'trial.json';trial_path.write_text(json.dumps(trial))
    protocol={'shared_execution':contract,'input_manifest_sha256':digest(manifest),
              'comparison_trial':{'path':str(trial_path),'sha256':digest(trial_path)}}
    path.write_text(json.dumps(protocol))
    return work,path,trial_path,trial,protocol,evaluator,acceptance,manifest,digest


def test_trial_verification_returns_no_evaluator_answers(tmp_path):
    work,*_=trial_fixture(tmp_path)
    result=module.verify_trial_binding(work,'classic','AK')
    assert set(result)=={'variant','discipline','repeat','trial_sha256'}


@pytest.mark.parametrize('fault',['evaluator','acceptance','trial','input','duplicate','discipline','repeat','unfrozen'])
def test_changed_or_unregistered_trial_cannot_run(tmp_path,fault):
    work,path,trial_path,trial,protocol,evaluator,acceptance,manifest,digest=trial_fixture(tmp_path)
    if fault in ('evaluator','acceptance','trial','input'):
        {'evaluator':evaluator,'acceptance':acceptance,'trial':trial_path,'input':manifest}[fault].write_text('changed')
    else:
        if fault=='duplicate':trial['runs'].append(dict(trial['runs'][0]))
        elif fault=='discipline':trial['runs'][0]['discipline']='KM'
        elif fault=='repeat':trial['runs'][0]['repeat']=True
        else:trial['status']='draft'
        trial_path.write_text(json.dumps(trial))
        protocol['comparison_trial']['sha256']=digest(trial_path)
        path.write_text(json.dumps(protocol))
    with pytest.raises(ValueError):module.verify_trial_binding(work,'classic','AK')
