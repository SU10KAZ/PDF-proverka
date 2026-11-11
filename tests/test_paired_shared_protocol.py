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
