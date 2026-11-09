import json
from backend.app.pipeline.stages.findings_merge.runner import merge_similar_findings
from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
from backend.app.services.findings.dedup.class_dedup import derive_class_key
from backend.app.services.findings.dedup.fuzzy_dedup import fuzzy_dedup


def pair():
    base={'severity':'РЕКОМЕНДАТЕЛЬНОЕ','category':'documentation','problem':'Проверить состав комплекта'}
    return [{**base,'id':'F-001','report_section':'technical_findings'},
            {**base,'id':'F-002','report_section':'questions'}]


def test_same_words_in_different_sections_survive_all_dedup_routes(tmp_path):
    findings=pair()
    assert derive_class_key(findings[0]) != derive_class_key(findings[1])
    kept,_=fuzzy_dedup(findings)
    assert len(kept)==2
    path=tmp_path/'03_findings.json';path.write_text(json.dumps({'findings':findings}))
    merge_similar_findings('test',output_dir=tmp_path)
    assert len(json.loads(path.read_text())['findings'])==2
    result=combine_findings_with_targeted({'findings':[findings[0]]},[('targeted',{'findings':[findings[1]]})])
    assert len(result['findings'])==2


def test_same_section_duplicates_still_merge():
    findings=pair();findings[1]['report_section']='technical_findings'
    kept,_=fuzzy_dedup(findings)
    assert len(kept)==1
    result=combine_findings_with_targeted({'findings':[findings[0]]},[('targeted',{'findings':[findings[1]]})])
    assert len(result['findings'])==1
