"""Paths and fixed diagnostic case manifest. No truth loading at import time."""
from pathlib import Path
import hashlib
import json
import subprocess

CODE = Path(__file__).resolve().parents[2]
ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272')
PREVIOUS = ROOT / 'controlled_inference_f1_f4_f2_v2'
OUT = ROOT / 'controlled_inference_f1_f4_f2_v3'
CASES = [(5, c) for c in ('C01', 'C07', 'C15', 'C18', 'C22', 'C05')] + [
    (7, c) for c in ('R01', 'R13', 'R19', 'Q04', 'S_FP01', 'R21')]
# Claim types are diagnostic metadata, not expected outcomes or semantic evidence.
PROFILES = dict(C01='TOPOLOGY_DECLARATION', C07='AGGREGATE_CALCULATED_RESULT',
    C15='TOPOLOGY_DECLARATION', C18='NOVEL_SYSTEM', C22='INPUT_CRITERION',
    C05='TOPOLOGY', R01='TOPOLOGY_DECLARATION', R13='TOPOLOGY', R19='INPUT_CRITERION',
    Q04='AGGREGATE_CALCULATED_RESULT', S_FP01='ROUTING', R21='SELECTED_EQUIPMENT')


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def ref(path):
    return dict(path=str(path), sha256=sha(path))


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
    if path.exists():
        if path.read_text() != content:
            raise ValueError('Immutable artifact drift: ' + str(path))
    else:
        path.write_text(content)


def git_commit():
    return subprocess.check_output(['git', '-C', str(CODE), 'rev-parse', 'HEAD'], text=True).strip()


def previous_hashes():
    return {str(p.relative_to(ROOT)): sha(p) for version in ('v1', 'v2')
            for p in sorted((ROOT / ('controlled_inference_f1_f4_f2_' + version)).rglob('*')) if p.is_file()}
