import asyncio
import importlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from .packets import digest

runner=importlib.import_module('experiments.project_change_semantic_272.run')


class NoNetworkClient:
    def __init__(self):self.chat=SimpleNamespace(completions=self)
    async def create(self,**kwargs):
        return SimpleNamespace(id='test-response',usage=SimpleNamespace(model_dump=lambda:{'prompt_tokens':1,'completion_tokens':1,'cost':0}),
            choices=[SimpleNamespace(finish_reason='stop',message=SimpleNamespace(content='{"events": [], "unknowns": []}'))])
    async def close(self):pass


class CompletePacketRun(unittest.TestCase):
    def exercise(self, client, count=1, expected_error=None, reservation_error=None):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'SPLIT.json').write_text('{}')
            directory=root/'input';(directory/'packets').mkdir(parents=True)
            (directory/'MANIFEST.json').write_text('{"partition":"DEV"}')
            (directory/'COUNTS.json').write_text(json.dumps({'packets':count}))
            pair=dict(index=5,pair_key='pair5',embargo_pages={'old':[],'new':[]})
            p=dict(pair_index=5,pair_key='pair5',partition='DEV',source_versions={},evidence={},
                proposal_kind='TEST_PROTOCOL_ONLY',proposal_query='Test protocol fixture',coverage_complete=False)
            for side in ['old','new']:
                receipt={'path':str(root/(side+'.pdf')),'sha256':side+'hash'}
                pair[side]=dict(document_version=side+'v',artifacts={'pdf':receipt})
                p['source_versions'][side]=side+'v'
                p['evidence'][side]=[dict(evidence_id=side+'_1',side=side,document_version=side+'v',page=1,
                    source_receipt=receipt,source_kind='PDF_NATIVE_TEXT',route='TEXT',bbox=[1,2,3,4],quote='A synthetic protocol fixture, not quality evidence.')]
            for i in range(count):
                p.pop('packet_id',None);p['proposal_query']=f'Synthetic protocol fixture {i}'
                p['packet_id']=digest(p)[:24]
                (directory/'packets'/(p['packet_id']+'.json')).write_text(json.dumps(p))
            with patch.object(runner,'ROOT',root),patch.object(runner,'BASE',root),patch.object(runner,'authorize',return_value=[pair]),patch.object(runner,'document_history',return_value={}),patch('openai.AsyncOpenAI',return_value=client),patch('backend.app.services.llm.paid_api_guard.reserve_paid_api',return_value=None,side_effect=reservation_error):
                if expected_error:
                    with self.assertRaises(expected_error):asyncio.run(runner.run('test',directory,count))
                else:asyncio.run(runner.run('test',directory,count))
            receipt=json.loads((root/'runs/test/RUN_RECEIPT.json').read_text())
            return receipt

    def test_admitted_packet_reaches_model_adapter_without_real_network(self):
        receipt=self.exercise(NoNetworkClient())
        self.assertEqual(receipt['completed_packets'],1)
        self.assertEqual(receipt['calls'],1)
        self.assertEqual(receipt['status'],'COMPLETE')

    def test_provider_denial_stops_queued_packets_but_keeps_inflight_results(self):
        class Denial(Exception):status_code=403
        class DenyingClient(NoNetworkClient):
            attempted=0
            async def create(self,**kwargs):
                self.attempted+=1;attempt=self.attempted
                await asyncio.sleep(0)
                if attempt==1:raise Denial('sensitive provider account URL')
                return await super().create(**kwargs)
        client=DenyingClient()
        r=self.exercise(client,12,runner.RunAuthorizationBlocked)
        self.assertEqual(client.attempted,3)
        self.assertEqual(r['completed_packets'],2)
        self.assertEqual(len(r['failed_packets']),1)
        self.assertEqual(len(r['skipped_packets']),9)
        self.assertEqual(r['status'],'AUTHORIZATION_BLOCKED')
        self.assertEqual(r['authorization_stop']['http_status'],403)
        self.assertNotIn('sensitive',json.dumps(r))

    def test_local_paid_guard_stops_before_network(self):
        from backend.app.services.llm.paid_api_guard import PaidApiBlockedError
        r=self.exercise(NoNetworkClient(),12,runner.RunAuthorizationBlocked,PaidApiBlockedError('daily_limit'))
        self.assertEqual(r['calls'],0)
        self.assertEqual(len(r['skipped_packets']),11)
        self.assertEqual(r['authorization_stop']['error_type'],'PaidApiBlockedError')

    def test_transient_provider_failure_is_not_authorization_denial(self):
        class Transient(Exception):status_code=429
        self.assertFalse(runner.authorization_failure(Transient()))


if __name__=='__main__':unittest.main()
