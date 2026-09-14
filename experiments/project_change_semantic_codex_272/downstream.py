"""Codex-only closure and ownership wiring; existing engineering rules retained."""
import argparse
import asyncio
from pathlib import Path

from experiments.project_change_272.inventory import immutable, read, sha, now
from experiments.project_change_semantic_272.closure import prepare
from collections import defaultdict
from experiments.project_change_semantic_272.ownership import load_members, member_view, check_partition, GROUP, AUDIT
from experiments.project_change_semantic_272.packets import digest
from experiments.project_change_semantic_272.report import export
from .provider import CodexProvider
from .run import BASE
from .schema import obj, arr, STRING, BOOL

GROUP_SCHEMA = obj(dict(groups=arr(obj(dict(group_id=STRING, member_ids=arr(STRING),
    engineering_subject=STRING, summary_ru=STRING,
    relation=dict(type='string', enum=['SAME_CHANGE_EVIDENCE', 'LINKED_CONFIGURATION', 'AGGREGATE_COMPONENT', 'SINGLETON']),
    identity_reason=STRING))), review=arr(obj(dict(member_id=STRING, reason=STRING)))))
GROUP_AUDIT = obj(dict(decisions=arr(obj(dict(group_id=STRING,
    verdict=dict(type='string', enum=['ACCEPT', 'REVIEW']), reason=STRING,
    one_owner=BOOL, summary_entailed=BOOL, no_duplicate_or_split=BOOL, conditions_preserved=BOOL)))))


class LocalCodexCalls:
    def __init__(self, out):
        self.out = out
        self.provider = CodexProvider(out)

    async def call(self, key, system, data):
        return await self.provider.call(key, system, data,
            GROUP_AUDIT if key.endswith('_audit') else GROUP_SCHEMA)

    async def close(self):
        immutable(self.out / 'CODEX_USAGE.json', dict(provider='codex_chatgpt',
            invocations=self.provider.invocations, cache_hits=self.provider.cache_hits,
            provider_cost_usd=None, at=now()))


def primary_packets(name):
    root = BASE / 'runs' / name
    manifest = read(root / 'MANIFEST.json')
    receipt = read(root / 'RUN_RECEIPT.json')
    if (manifest['partition'] != 'DEV' or manifest['smoke'] or manifest['config']['provider'] != 'codex_chatgpt'
            or receipt['completed_packets'] != 230 or not receipt['coverage_complete']):
        raise PermissionError('All 230 clean Codex packets required before closure')
    packets = BASE / (name + '_packets')
    if packets.exists():
        if read(packets / 'MANIFEST.json')['source_manifest_sha256'] != sha(root / 'MANIFEST.json'):
            raise ValueError('Source primary run drift')
        for source, expected in manifest['packets'].items():
            if sha(packets / 'packets' / Path(source).name) != expected:
                raise ValueError('Copied frozen packet drift')
        return packets.name
    immutable(packets / 'MANIFEST.json', dict(partition='DEV', source_manifest_sha256=sha(root / 'MANIFEST.json')))
    for path, expected in manifest['packets'].items():
        path = Path(path)
        if sha(path) != expected:
            raise ValueError('Primary packet drift')
        target = packets / 'packets' / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(path.read_bytes())
    immutable(packets / 'COUNTS.json', dict(packets=230))
    return packets.name


def prepare_closure(primary_name):
    packets_name = primary_packets(primary_name)
    return prepare(primary_name + '_closure_packets', primary_name, packets_name, artifact_base=BASE)


async def ownership(primary_name):
    members, reviews = load_members(primary_name + '_closure', primary_name + '_closure_packets',
                                   'DEV', None, artifact_base=BASE)
    out = BASE / 'ownership' / (primary_name + '_ownership')
    provider = CodexProvider(out)
    manifest = dict(partition='DEV', source_run=primary_name + '_closure', config=provider.config,
        source_receipt_sha256=sha(BASE / 'runs' / (primary_name + '_closure') / 'RUN_RECEIPT.json'))
    def save(path, value):
        if path.exists():
            if read(path) != value:
                raise ValueError('Ownership resume artifact drift')
        else:
            immutable(path, value)
    save(out / 'MANIFEST.json', manifest)
    save(out / 'MEMBERS.json', members)
    save(out / 'INPUT_REVIEWS.json', reviews)
    by_pair = defaultdict(list)
    for m in members:
        by_pair[m['pair_index']].append(m)
    changes = []
    needs_review = []
    for index, rows in sorted(by_pair.items()):
        data = [member_view(m) for m in rows]
        proposal = await provider.call(str(index) + '_group', GROUP, dict(members=data), GROUP_SCHEMA)
        errors = check_partition(rows, proposal)
        audit = await provider.call(str(index) + '_audit', AUDIT, dict(members=data, proposal=proposal), GROUP_AUDIT) if not errors else {}
        decisions = audit.get('decisions', [])
        save(out / 'pairs' / (str(index) + '.json'), dict(proposal=proposal, audit=audit, errors=errors))
        if errors:
            needs_review.extend(dict(member_id=m['member_id'], reason=errors) for m in rows)
            continue
        needs_review += proposal['review']
        for g in proposal['groups']:
            ds = [d for d in decisions if d.get('group_id') == g['group_id']]
            ok = len(ds) == 1 and ds[0].get('verdict') == 'ACCEPT' and all(ds[0].get(k) is True for k in
                ['one_owner', 'summary_entailed', 'no_duplicate_or_split', 'conditions_preserved'])
            changes.append(dict(**g, pair_index=index,
                project_change_id=digest([index, sorted(g['member_ids'])])[:24],
                status='ACCEPTED_CANDIDATE' if ok else 'REVIEW', ownership_audit=ds,
                state_bundles=[m for m in rows if m['member_id'] in g['member_ids']],
                source_adjudication='NOT_INDEPENDENTLY_ADJUDICATED'))
        print(index, 'groups', len(proposal['groups']), flush=True)
    save(out / 'PROJECT_CHANGES.json', dict(project_changes=changes, review_members=needs_review,
        accepted=sum(c['status'] == 'ACCEPTED_CANDIDATE' for c in changes), input_members=len(members),
        pair_count=len(by_pair), adjudication='NOT_SOURCE_ADJUDICATED'))
    return out


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['prepare-closure', 'ownership', 'report'])
    parser.add_argument('--name', default='codex_v1')
    args = parser.parse_args()
    if not args.name.startswith('codex_') or '/' in args.name or '..' in args.name:
        raise ValueError('Separate Codex name required')
    if args.action == 'prepare-closure':
        prepare_closure(args.name)
    elif args.action == 'ownership':
        asyncio.run(ownership(args.name))
    else:
        export(args.name, args.name + '_ownership', artifact_base=BASE)
