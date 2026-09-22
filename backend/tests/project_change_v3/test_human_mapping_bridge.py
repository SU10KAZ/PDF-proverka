"""Synthetic-only bridge contract and zero-provider-call receipts."""
import copy
import json
import os
from pathlib import Path

import pytest
from backend.app.services.project_change_v3 import human_mapping_bridge as b, run_storage as rs
from backend.app.services.project_change_v3.provider import FakeProvider
from backend.app.services.project_change_v3.hm_builder import build_human_mapping_ui_data
from backend.app.services.stage_comparison import paths


def receipt(name, **details):
    if os.getenv('BRIDGE_RECEIPTS'):
        rs.atomic(Path(os.environ['BRIDGE_RECEIPTS']) / (name + '.json'),
                  {'status': 'PASS', 'model_calls': 0, **details})


def region(rid, olds, news):
    return {'region_id': rid, 'old_pages': [1], 'new_pages': [7], 'engineering_domain': 'synthetic',
            'scope': 'synthetic', 'locations': [], 'reason_for_correspondence': 'synthetic',
            'important_text_blocks': [{'side': side, 'physical_page': page, 'block_id': bid,
                'block_type': 'TEXT', 'relevance': 'synthetic'}
                for side, page, ids in [('OLD', 1, olds), ('NEW', 7, news)] for bid in ids],
            'important_table_blocks': [], 'important_graphic_blocks': [], 'confidence': 0.5}


def mapping(edges):
    return {'pair': 'pair', 'regions': [region('M' + str(i), [o], [n]) for i, (o, n) in enumerate(edges)],
            'unmatched_old': [] if edges else [1], 'unmatched_new': [] if edges else [7], 'coverage_notes': []}


@pytest.fixture
def env(tmp_path, monkeypatch, request):
    from backend.app.services.project_change_v3 import scope, engine
    monkeypatch.setenv('COMPARISON_ROOT', str(tmp_path / 'comparison'))
    monkeypatch.setenv('PROJECT_COMPARISON_V3_ALLOW_INFERENCE', '0')
    monkeypatch.setenv('PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE', '1')
    monkeypatch.setattr(scope, 'object_id_for_session', lambda sid: 'object')
    from backend.app.services.common import object_service
    monkeypatch.setattr(object_service, '_load_objects', lambda: {'objects': [{'id': 'object'}]})
    old, new = tmp_path / 'old.pdf', tmp_path / 'new.pdf'
    old.write_bytes(b'synthetic old'); new.write_bytes(b'synthetic new')
    monkeypatch.setattr(engine, '_resolve_pair_paths', lambda sid, pid: ({'pdf': old}, {'pdf': new}, {}))
    rs.atomic(paths.session_json_path('comparison'), {'id': 'comparison'})
    rs.atomic(paths.pair_json_path('comparison', 'pair'), {'id': 'pair'})
    args = dict(object_id='object', comparison_id='comparison', pair_id='pair', source_run_id='source')
    directory = rs.create('comparison', 'pair', 'source', 'object')
    structure = []
    for side, page, prefix in [('OLD', 1, 'A'), ('NEW', 7, 'B')]:
        blocks = [{'block_id': prefix + str(i), 'modality': 'TEXT', 'bbox': [0, 0, 1, 1],
                   'structured_md': 'synthetic', 'tables': [], 'graphic_crop_ref': ''} for i in range(52)]
        structure.append({'side': side, 'physical_page': page, 'blocks': blocks})
        rs.atomic(directory / 'project_change_v3/source' / side.lower() / f'p{page:03d}/page.json', {'blocks': blocks})
    sf = directory / 'project_change_v3/DOCUMENT_STRUCTURE.json'
    rs.atomic(sf, structure)
    sm = {'old_pdf_sha256': rs.sha(old), 'new_pdf_sha256': rs.sha(new), 'structure_sha256': rs.sha(sf)}
    source_map = mapping([])
    source_olds, source_news = getattr(request, 'param', (['A0', 'A1'], ['B0', 'B1']))
    source_map.update(regions=[region('R', source_olds, source_news)], unmatched_old=[], unmatched_new=[])
    hm = build_human_mapping_ui_data(pair_id='pair', object_id='object', semantic_map=source_map, work_dir=directory / 'project_change_v3')
    hm.update(run_id='source', session_id='comparison')
    state = {'run_id': 'source', 'status': 'COMPLETED', 'reason_code': 'v3_completed'}
    rs.atomic(directory / 'project_change_v3_result.json', {'run_id': 'source', 'session_id': 'comparison',
        'pair_id': 'pair', 'object_id': 'object', 'source_manifest': sm, 'projectchanges': [],
        'unresolved_hints': [], 'provenance': {}})
    rs.atomic(directory / 'project_change_v3_semantic_map.json', source_map)
    rs.atomic(directory / 'human_mapping/ui_data.json', hm)
    rs.atomic(directory / 'state.json', state)
    rs.finalize('comparison', 'pair', 'source', state)
    return dict(args=args, directory=directory, structure=structure, new_pdf=new)


def edit(env, o, n, *, rid='R', event_type='ADD_BLOCK_LINK', link_id=None, previous=None, timestamp='2026-01-01T00:00:00Z'):
    path = env['directory'] / 'human_mapping/human_block_link_edits.jsonl'
    rows = b.storage.read_jsonl(path)
    event = {'event_id': f'e{len(rows)}', 'event_type': event_type, 'region_id': rid,
             'object_id': 'object', 'comparison_id': 'pair', 'pair_key': 'pair', 'run_id': 'source',
             'link_id': link_id or f'human:{len(rows)}', 'old_block_id': o, 'new_block_id': n,
             'previous_link_id': previous, 'timestamp': timestamp}
    b.storage.append_jsonl(path, event)
    return event


def review(env, *, status='HUMAN_CONFIRMED', olds=None, news=None, rid='R'):
    path = env['directory'] / 'human_mapping/reviews.jsonl'
    rows = b.storage.read_jsonl(path)
    event = {'review_id': f'r{len(rows)}', 'region_id': rid, 'object_id': 'object',
             'comparison_id': 'pair', 'pair_key': 'pair', 'run_id': 'source',
             'old_block_ids': olds or ['A0', 'A1'], 'new_block_ids': news or ['B0', 'B1'],
             'status': status, 'timestamp': f'2026-01-02T00:00:{len(rows):02d}Z'}
    b.storage.append_jsonl(path, event)
    return event


def snapshot(env):
    return b.build_snapshot(**env['args'], created_at='2026-01-03T00:00:00Z')


def run(env, snap, edges, **overrides):
    provider = FakeProvider(handlers={'MAPPING': lambda **kw: mapping(edges)})
    args = dict(snapshot=snap, expected_sha256=snap.sha256, **env['args'], new_run_id='future', provider=provider)
    args.update(overrides)
    return b.fake_mapper_run(**args), provider


@pytest.mark.parametrize('name,edges', [
    ('BRIDGE_1_TO_1_TEST', [('A0', 'B0')]),
    ('BRIDGE_1_TO_N_TEST', [('A0', 'B0'), ('A0', 'B1')]),
    ('BRIDGE_N_TO_1_TEST', [('A0', 'B0'), ('A1', 'B0')]),
    ('BRIDGE_N_TO_N_TEST', [('A0', 'B0'), ('A1', 'B1')]),
])
def test_cardinalities(env, name, edges):
    for o, n in edges:
        edit(env, o, n)
    review(env)
    snap = snapshot(env)
    anchors = snap.value()['confirmed_anchors']
    assert {(a['old_block_id'], a['new_block_id']) for a in anchors} == set(edges)
    assert all(a['source'] == 'HUMAN_MANUAL' for a in anchors)
    result, provider = run(env, snap, edges)
    assert result['semantic_map'] == mapping(edges)
    assert result['provenance']['confirmed_anchor_count'] == len(edges)
    with pytest.raises(b.BridgeError, match='BRIDGE_MAPPING_REJECTED') as exc:
        run(env, snap, [])
    assert exc.value.details['missing_anchors'] == sorted(edges)
    receipt(name, anchors=anchors, omitted_response='REJECTED', preserved_edges=edges, cartesian_expansion=0)


def test_rejection(env):
    edit(env, 'A0', 'B0'); review(env, status='HUMAN_REJECTED')
    snap = snapshot(env)
    with pytest.raises(b.BridgeError, match='BRIDGE_MAPPING_REJECTED') as exc:
        run(env, snap, [('A0', 'B0'), ('A0', 'B1')])
    assert exc.value.details['rejected_exact_links'] == [('A0', 'B0')]
    accepted, _ = run(env, snap, [('A0', 'B1'), ('A1', 'B0')])
    assert len(accepted['semantic_map']['regions']) == 2
    receipt('BRIDGE_REJECT_TEST', enforcement='REJECT_RESPONSE', forbidden=[['A0', 'B0']],
            allowed=[['A0', 'B1'], ['A1', 'B0']], block_blacklist=False)


def test_sparse_uncertain_unreviewed(env):
    assert snapshot(env).value()['confirmed_anchors'] == []
    edit(env, 'A0', 'B0'); review(env, olds=['A0'], news=['B0'])
    snap = snapshot(env)
    assert len(snap.value()['confirmed_anchors']) == 1
    assert sum(map(len, snap.value()['unconstrained']['block_ids'].values())) == 102
    result, _ = run(env, snap, [('A0', 'B0'), ('A51', 'B51')])
    review(env, status='HUMAN_UNCERTAIN')
    assert snapshot(env).value()['confirmed_anchors'] == []
    receipt('BRIDGE_SPARSE_TEST', anchors=1, unconstrained_blocks=102, uncertain='UNCONSTRAINED', unreviewed='UNCONSTRAINED')
    receipt('BRIDGE_DRY_RUN', snapshot=snap.value(), sha256=snap.sha256, conflicts=[])


def test_conflict(env):
    source = b.load_source(**env['args'])
    source['hm']['regions'].append({**copy.deepcopy(source['hm']['regions'][0]), 'id': 'R2'})
    edit(env, 'A0', 'B0'); edit(env, 'A0', 'B0', rid='R2')
    review(env); review(env, status='HUMAN_REJECTED', rid='R2')
    reviews, edits = b._history(source)
    with pytest.raises(b.BridgeError, match='CONFIRMED_AND_REJECTED_EXACT_EDGE'):
        b._build(source, reviews, edits, '2026-01-03T00:00:00Z')
    receipt('BRIDGE_CONFLICT_TEST', error='BRIDGE_CONFLICT_REVIEW_REQUIRED', mapper_calls=0)


def test_stale(env):
    snap = snapshot(env)
    env['new_pdf'].write_bytes(b'changed')
    fake = FakeProvider()
    with pytest.raises(b.BridgeError, match='STALE_SOURCE_PDF'):
        run(env, snap, [], provider=fake)
    assert fake.calls == []
    receipt('BRIDGE_STALE_SOURCE_TEST', stale_source='REJECTED', mapper_calls=0)


def test_immutability_and_identity(env, tmp_path):
    edit(env, 'A0', 'B0'); review(env)
    snap = snapshot(env)
    path = tmp_path / 'snapshot.json'
    snap.write(path)
    original = path.read_bytes()
    detached = snap.value(); detached['confirmed_anchors'].clear()
    review(env, status='HUMAN_REJECTED')
    assert snapshot(env).sha256 != snap.sha256 and path.read_bytes() == original
    # Frozen history remains valid even after newer decisions.
    before = {str(p): rs.sha(p) for p in env['directory'].rglob('*') if p.is_file()}
    pointer = paths.production_dir('comparison', 'pair') / 'current_run.json'
    pointer_hash = rs.sha(pointer)
    result, _ = run(env, snap, [('A0', 'B0')])
    assert before == {str(p): rs.sha(p) for p in env['directory'].rglob('*') if p.is_file()}
    assert rs.sha(pointer) == pointer_hash
    assert result['run_id'] != result['source_run_id']
    with pytest.raises(FileExistsError): snap.write(path)
    with pytest.raises(b.BridgeError, match='NEW_RUN_ID_REQUIRED'): run(env, snap, [], new_run_id='source')
    receipt('BRIDGE_IMMUTABILITY_TEST', snapshot_sha256=snap.sha256, later_sha256=snapshot(env).sha256, immutable=True)
    receipt('VERSIONED_RUN_BRIDGE_TEST', baseline_run_mutated=False, pointer_mutated=False, new_run_id_required=True,
            future_provenance=result['provenance'])


def test_cross_run(env):
    import shutil
    edit(env, 'A0', 'B0'); review(env)
    directory = env['directory']
    other = rs.create('comparison', 'pair', 'other', 'object')
    for file in directory.iterdir():
        if file.name == 'run_manifest.json': continue
        if file.is_dir(): shutil.copytree(file, other / file.name)
        else: shutil.copy2(file, other / file.name)
    for name in ['reviews.jsonl', 'human_block_link_edits.jsonl']:
        (other / 'human_mapping' / name).unlink()
    for name in ['project_change_v3_result.json', 'state.json', 'human_mapping/ui_data.json']:
        rs.atomic(other / name, {**rs.read(other / name), 'run_id': 'other'})
    rs.finalize('comparison', 'pair', 'other', rs.read(other / 'state.json'))
    args = {**env['args'], 'source_run_id': 'other'}
    assert b.build_snapshot(**args).value()['confirmed_anchors'] == []
    # Physically misplaced history is an error, never silently imported.
    shutil.copy2(directory / 'human_mapping/reviews.jsonl', other / 'human_mapping/reviews.jsonl')
    with pytest.raises(b.BridgeError, match='HUMAN_EVENT_SCOPE_MISMATCH'):
        b.build_snapshot(**args)
    receipt('BRIDGE_CROSS_RUN_ISOLATION_TEST', other_run_anchor_count=0, cross_run_bleed=0, contamination='REJECTED')


def test_deleted_reassigned_and_late_manual(env):
    e = edit(env, 'A0', 'B0'); review(env)
    edit(env, 'A0', 'B1', event_type='REASSIGN_BLOCK_LINK', previous=e['link_id'], timestamp='2026-01-03T00:00:00Z')
    assert snapshot(env).value()['confirmed_anchors'] == []
    # Explicit later confirmation required (append order within reviews is authoritative).
    path = env['directory'] / 'human_mapping/reviews.jsonl'
    row = review(env)
    row.update(review_id='later', timestamp='2026-01-04T00:00:00Z')
    b.storage.append_jsonl(path, row)
    assert snapshot(env).value()['confirmed_anchors'][0]['new_block_id'] == 'B1'
    edit(env, 'A0', 'B1', event_type='DELETE_BLOCK_LINK', link_id='human:1', timestamp='2026-01-05T00:00:00Z')
    assert snapshot(env).value()['confirmed_anchors'] == []


@pytest.mark.parametrize('mutation', ['run', 'object', 'pair', 'region', 'side', 'block', 'state', 'hash'])
def test_bad_input_no_calls(env, mutation):
    edit(env, 'A0', 'B0'); review(env)
    path = env['directory'] / 'human_mapping/reviews.jsonl'
    row = json.loads(path.read_text())
    if mutation in {'run', 'object', 'pair', 'region'}:
        row[{'run':'run_id','object':'object_id','pair':'pair_key','region':'region_id'}[mutation]] = 'wrong'
    elif mutation in {'side', 'block'}:
        row['old_block_ids'] = ['B0' if mutation == 'side' else 'unknown']
    else:
        mf = env['directory'] / 'run_manifest.json'
        data = rs.read(mf); data['state' if mutation == 'state' else 'old_pdf_sha256'] = 'wrong'; rs.atomic(mf, data)
    path.write_text(json.dumps(row) + '\n')
    with pytest.raises(b.BridgeError): snapshot(env)


def test_baseline_package(env):
    from backend.app.services.project_change_v3.contracts import MAPPER_PROMPT, MAP_SCHEMA
    from backend.app.services.project_change_v3.source_prep import mapping_images
    package = b.mapper_package('pair', env['structure'])
    assert package == dict(stage='MAPPING', call_id='pair_SEMANTIC_MAPPING', pair_id='pair',
        prompt=MAPPER_PROMPT, data={'pair':'pair', 'pages':env['structure']}, schema=MAP_SCHEMA,
        images=mapping_images(env['structure']))
    with pytest.raises(b.BridgeError, match='BRIDGE_SNAPSHOT_REQUIRED'):
        b.mapper_package('pair', env['structure'], mapping_mode='human_anchored')
    snap = snapshot(env)
    with pytest.raises(b.BridgeError, match='SNAPSHOT_HASH_MISMATCH'):
        run(env, snap, [], expected_sha256='bad')
    receipt('BASELINE_REGRESSION', default_v3='UNCHANGED', prompt_unchanged=True, schema_unchanged=True)


def test_cli_and_schema(env, tmp_path, monkeypatch, capsys):
    from scripts.human_mapping_bridge_dry_run import main
    import jsonschema
    out = tmp_path / 'bridge.json'
    monkeypatch.setattr('sys.argv', ['bridge', '--object-id', 'object', '--comparison-id', 'comparison',
        '--pair-id', 'pair', '--source-run-id', 'source', '--created-at', '2026-01-03T00:00:00Z', '--output', str(out)])
    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['bridge_snapshot_sha256'] == snapshot(env).sha256
    schema = json.loads((Path(__file__).resolve().parents[3] / 'docs/BRIDGE_SNAPSHOT_SCHEMA.json').read_text())
    jsonschema.validate(json.loads(out.read_bytes()), schema)
    assert main() == 2
    capsys.readouterr()


from backend.tests.project_change_v3.test_generic_production_boundary_e2e import generic_env, _run


def test_existing_full_fake_production_source(generic_env, monkeypatch):
    from backend.tests.project_change_v3 import generic_fixture as gf
    from backend.app.services.common import object_service
    monkeypatch.setattr(object_service, '_load_objects', lambda: {'objects': [{'id': gf.OBJECT_ID}]})
    state = _run(generic_env)
    args = dict(object_id=gf.OBJECT_ID, comparison_id=generic_env['session_id'], pair_id=gf.PAIR_ID,
                source_run_id=state['run_id'])
    snap = b.build_snapshot(**args)
    source = b.load_source(**args)
    r = source['hm']['regions'][0]
    o, n = r['old_blocks'][0]['id'], r['new_blocks'][0]['id']
    common = {'object_id':gf.OBJECT_ID, 'comparison_id':gf.PAIR_ID, 'pair_key':gf.PAIR_ID,
              'run_id':state['run_id'], 'region_id':r['id']}
    b.storage.append_jsonl(source['directory'] / 'human_mapping/human_block_link_edits.jsonl',
        {**common, 'event_id':'e1', 'event_type':'ADD_BLOCK_LINK', 'link_id':'human:manual',
         'old_block_id':o, 'new_block_id':n, 'timestamp':'2026-01-01T00:00:00Z'})
    b.storage.append_jsonl(source['directory'] / 'human_mapping/reviews.jsonl',
        {**common, 'review_id':'r1', 'old_block_ids':[o], 'new_block_ids':[n],
         'status':'HUMAN_CONFIRMED', 'timestamp':'2026-01-02T00:00:00Z'})
    snap = b.build_snapshot(**args)
    assert len(snap.value()['confirmed_anchors']) == 1
    response = rs.read(source['directory'] / 'project_change_v3_semantic_map.json')
    extra = copy.deepcopy(response['regions'][0]); extra['region_id'] = 'HUMAN-EXACT'
    for key in b.REF_KEYS:
        extra[key] = [ref for ref in extra[key] if (ref['side'], ref['block_id']) in {('OLD',o), ('NEW',n)}]
    response['regions'].append(extra)
    fake = FakeProvider(handlers={'MAPPING': lambda **kw: response})
    result = b.fake_mapper_run(snapshot=snap, expected_sha256=snap.sha256, **args, new_run_id='anchored-future', provider=fake)
    assert result['provenance']['confirmed_anchor_count'] == 1 and len(fake.calls) == 1
    receipt('BRIDGE_FULL_FAKE_FLOW', versioned_source=True, real_source_prep=True, human_manual=True,
            snapshot_validated=True, fake_mapper_calls=1, model_calls_count=0)


@pytest.mark.parametrize('env', [(['A0'], ['B0']), (['A0'], ['B0', 'B1']), (['A0', 'A1'], ['B0'])], indirect=True)
def test_ai_proposed_confirmed(env):
    source = b.load_source(**env['args'])
    r = source['hm']['regions'][0]
    review(env, olds=[x['id'] for x in r['old_blocks']], news=[x['id'] for x in r['new_blocks']])
    snap = snapshot(env)
    assert all(a['source'] == 'HUMAN_CONFIRMED' for a in snap.value()['confirmed_anchors'])
    links = b.validation.base_proposed_links(r)
    result, _ = run(env, snap, [(a['old_block_id'], a['new_block_id']) for a in links])
    assert result['provenance']['confirmed_anchor_count'] == len(links)


def test_snapshot_tamper_and_non_fake_provider(env):
    snap = snapshot(env)
    value = snap.value(); value['rejected_links'].append({'old_block_id':'A0','new_block_id':'B0'})
    changed = b.Snapshot(b.canonical(value))
    fake = FakeProvider()
    with pytest.raises(b.BridgeError, match='SNAPSHOT_CONTENT_MISMATCH'):
        run(env, changed, [], provider=fake)
    assert fake.calls == []
    with pytest.raises(b.BridgeError, match='FAKE_PROVIDER_REQUIRED'):
        run(env, snap, [], provider=object())


def test_source_creation_and_future_provenance_persistence(env):
    snap = snapshot(env)
    result, _ = run(env, snap, [])
    pointer = rs.current('comparison', 'pair')
    # Isolated persistence proof only: normal run storage accepts the exact
    # frozen input/provenance without publishing or rewriting the baseline.
    directory = rs.create('comparison', 'pair', result['run_id'], 'object')
    snap.write(directory / 'human_mapping_bridge_snapshot.json')
    with rs.selected('comparison', 'pair', result['run_id']):
        rs.save('comparison', 'pair', 'human_anchored_mapper_dry_run', result)
    assert rs.read(directory / 'human_anchored_mapper_dry_run.json')['provenance']['bridge_snapshot_sha256'] == snap.sha256
    assert rs.current('comparison', 'pair') == pointer
    with pytest.raises(b.BridgeError, match='NEW_RUN_ID_REQUIRED'):
        run(env, snap, [])


def test_forged_history_and_invalid_snapshot_fail_before_mapper(env):
    edit(env, 'A0', 'B0'); review(env)
    source = b.load_source(**env['args'])
    reviews, edits = b._history(source)
    reviews[0]['status'] = 'HUMAN_REJECTED'
    forged = b._build(source, reviews, edits, '2026-01-03T00:00:00Z')
    fake = FakeProvider()
    with pytest.raises(b.BridgeError, match='SNAPSHOT_HISTORY_NOT_SOURCE_PREFIX'):
        run(env, forged, [], provider=fake)
    with pytest.raises(b.BridgeError, match='INVALID_SNAPSHOT_JSON'):
        run(env, b.Snapshot(b'not json'), [], provider=fake)
    assert fake.calls == []


def test_cli_subprocess_json_error_and_no_registry_creation(tmp_path):
    import subprocess
    import sys
    root = Path(__file__).resolve().parents[3]
    data = tmp_path / 'app_data'; data.mkdir()
    # Exercise the legacy configuration's stdout diagnostics in a fresh process.
    (data / 'stage_models.json').write_text('{}')
    (data / 'stage_batch_modes.json').write_text('{}')
    proc = subprocess.run([sys.executable, str(root / 'scripts/human_mapping_bridge_dry_run.py'),
        '--object-id','missing','--comparison-id','missing','--pair-id','missing','--source-run-id','missing'],
        env={**os.environ, 'AUDIT_DISABLE_DOTENV':'1', 'AUDIT_APP_DATA_DIR':str(data),
             'COMPARISON_ROOT':str(tmp_path / 'comparison')}, capture_output=True, text=True)
    assert proc.returncode == 2
    assert json.loads(proc.stdout)['conflicts'][0]['reason'] == 'COMPARISON_NOT_FOUND'
    assert not (data / 'objects.json').exists() and not (tmp_path / 'comparison').exists()
