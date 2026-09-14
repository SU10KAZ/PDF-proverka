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
    def test_admitted_packet_reaches_model_adapter_without_real_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'SPLIT.json').write_text('{}')
            directory=root/'input';(directory/'packets').mkdir(parents=True)
            (directory/'MANIFEST.json').write_text('{"partition":"DEV"}')
            (directory/'COUNTS.json').write_text('{"packets":1}')
            pair=dict(index=5,pair_key='pair5',embargo_pages={'old':[],'new':[]})
            p=dict(pair_index=5,pair_key='pair5',partition='DEV',source_versions={},evidence={},
                proposal_kind='TEST_PROTOCOL_ONLY',proposal_query='Test protocol fixture',coverage_complete=False)
            for side in ['old','new']:
                receipt={'path':str(root/(side+'.pdf')),'sha256':side+'hash'}
                pair[side]=dict(document_version=side+'v',artifacts={'pdf':receipt})
                p['source_versions'][side]=side+'v'
                p['evidence'][side]=[dict(evidence_id=side+'_1',side=side,document_version=side+'v',page=1,
                    source_receipt=receipt,source_kind='PDF_NATIVE_TEXT',route='TEXT',bbox=[1,2,3,4],quote='A synthetic protocol fixture, not quality evidence.')]
            p['packet_id']=digest(p)[:24]
            (directory/'packets'/(p['packet_id']+'.json')).write_text(json.dumps(p))
            with patch.object(runner,'ROOT',root),patch.object(runner,'BASE',root),patch.object(runner,'authorize',return_value=[pair]),patch('openai.AsyncOpenAI',return_value=NoNetworkClient()),patch('backend.app.services.llm.paid_api_guard.reserve_paid_api',return_value=None):
                asyncio.run(runner.run('test',directory,1))
            receipt=json.loads((root/'runs/test/RUN_RECEIPT.json').read_text())
            self.assertEqual(receipt['completed_packets'],1)
            self.assertEqual(receipt['calls'],1)


if __name__=='__main__':unittest.main()
