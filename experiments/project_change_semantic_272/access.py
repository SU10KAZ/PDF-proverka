"""Reserve access requires a freeze of this algorithm, not another candidate."""
from pathlib import Path

from experiments.project_change_272.inventory import ROOT,REPO,read,sha
from experiments.project_change_272.policy import admitted_pairs,verify_candidate


def authorize(partition='DEV',candidate=None):
    pairs=admitted_pairs(partition,candidate)
    if partition!='DEV':
        manifest=verify_candidate(candidate,partition)
        required={str(p.relative_to(REPO)) for p in Path(__file__).parent.glob('*.py')}
        if not required<=set(manifest['code']):
            raise PermissionError('Reserved evidence requires a complete semantic candidate freeze')
    return pairs


def prepared_pairs(partition='DEV',candidate=None):
    allowed={p['index']:p for p in authorize(partition,candidate)}
    pairs=read(ROOT/'sources'/partition/'PAIRS.json')
    if len(pairs)!=len(allowed) or {p['index'] for p in pairs}!=set(allowed):
        raise PermissionError('Prepared documents differ from the frozen complete-pair allocation')
    for pair in pairs:
        source=allowed[pair['index']]
        for field in ['partition','pair_key','project','embargo_pages']:
            if pair[field]!=source[field]:raise PermissionError('Prepared pair scope drift')
        for side in ['old','new']:
            doc=pair[side];expected=source[side]
            if doc['document_version']!=expected['document_version']:
                raise PermissionError('Prepared document version drift')
            pdf=doc['artifacts']['pdf'];original=expected['artifacts']['pdf']
            if pdf['sha256']!=original['sha256'] or Path(pdf['path']).resolve()!=Path(original['path']).resolve():
                raise PermissionError('Prepared PDF is not an admitted source')
            for kind,receipt in doc['artifacts'].items():
                path=Path(receipt['path'])
                if sha(path)!=receipt['sha256']:raise PermissionError('Prepared source artifact drift')
                if kind!='pdf' and not path.resolve().is_relative_to((ROOT/'sources'/partition/'isolated'/doc['document_version']).resolve()):
                    raise PermissionError('Derived text/blocks outside the isolated document scope')
    return pairs
