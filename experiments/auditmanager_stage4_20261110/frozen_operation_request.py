"""Load a sealed research request without rebuilding it from changed runtime code."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def load_frozen_operation(root, md_path):
    root = Path(root)
    wire = json.loads((root/'request.json').read_text())
    binding = json.loads((root/'binding.json').read_text())
    assert evidence_digest(wire) == binding['request_sha256']
    source_binding = binding.get('source_binding')
    if source_binding is None:
        assert hashlib.sha256(Path(md_path).read_bytes()).hexdigest() == binding['md_sha256']
        assert len(wire['image_paths']) == len(binding['images'])
        for path, image in zip(wire['image_paths'], binding['images'], strict=True):
            assert Path(path).resolve() == Path(image['path']).resolve()
            assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == image['sha256']
    packet = json.loads(wire['messages'][-1]['content'])
    stage = 'object_operations_' + hashlib.sha256(packet['object']['id'].encode()).hexdigest()[:16]
    if packet.get('context_revision'):
        stage += '_context_' + str(packet['context_revision'])
    request = CodexTargetedPass(stage, '03_'+stage+'.json', wire['messages'],
                               tuple(Path(p) for p in wire['image_paths']), continuation_source=source_binding)
    assert verify_operation_source_binding(request) is None
    return root, request, binding
