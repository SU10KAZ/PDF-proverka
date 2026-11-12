"""Prepare opt-in DEV controls for audit-input provenance and dependent prose."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_merge.publication_reconciliation import PURPOSE_AWARE_INSTRUCTIONS
from .prepare_publication_purpose_trial import prepare as prepare_purpose
from .run_primary_text_runtime_probe import sha, write
from .run_runtime_probe_recovery import verify_packet

INSTRUCTIONS = PURPOSE_AWARE_INSTRUCTIONS + '''
Distinguish the project sources from the auditor's input wording. A typo or stale
identifier in an audit claim is not a defect in the project. If attached source
evidence resolves that typo, use the source-supported identifier in the retained
claim and account for the corrected input wording in its spans/reason or a
history-only processing note. Do not ask the engineer to correct the audit text.
Do not use this rule to hide a discrepancy that exists between project sources.
A conditional consequence of a retained issue belongs with that issue as a
conditional risk, not as an additional question asking whether that risk might
occur. Likewise, an instruction to reconcile the same documents belongs with the
issue. Combine their input spans when evidence supports that relationship; do
not discard the risk or recommendation. Preserve unresolved limits without
claiming an actual failure. Retain a separate question only if it requires a
distinct source-grounded project decision, not merely confirmation of our wording
or of a possible consequence. An unresolved signal purpose can still be a useful
question; never infer its direction from a label alone.
'''


def prepare(destination):
    selected=['F-062','F-053','F-061','F-057','F-054']
    manifest=prepare_purpose(destination,selected)
    for row in manifest['requests']:
        path=Path(row['path']);request=verify_packet(row);before=deepcopy(request)
        request['messages'][0]['content']=INSTRUCTIONS
        assert request['messages'][1:]==before['messages'][1:]
        assert request['image_hashes']==before['image_hashes']
        write(path,request);row['sha256']=sha(path);verify_packet(row)
    manifest.update(selection='DEV selected controls for audit typo versus project discrepancy, dependent risk versus actionable question; identical v3 packets, changed system instructions only. No reference answers.',
                    instruction_variant='editorial_provenance_v1',candidate_code_sha256=sha(Path(__file__)))
    write(destination/'preparation.json',manifest)
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('destination',type=Path)
    print(json.dumps(prepare(parser.parse_args().destination.resolve()),ensure_ascii=False))
