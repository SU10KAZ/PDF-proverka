"""Check all completed source-only packets locally without interpreting graphics."""
from collections import Counter
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry


def assess(discipline='KM'):
    root=Path('/tmp/auditmanager_whole_project_inventory_20261110')/discipline
    manifest=json.loads((root/'manifest.json').read_text())
    rows=[];unresolved_graphics=[];issues=[];quote_count=0;matches=0;status_counts=Counter()
    for entry,digest in zip(manifest['packets'],manifest['request_digests'],strict=True):
        path=root/'execution'/entry['stage']/'result.json'
        if not path.exists():break
        request=json.loads((root/(entry['stage']+'.json')).read_text())
        assert evidence_digest(request)==digest
        source=json.loads(request['messages'][-1]['content'])
        blocks={b['block_id']:b for b in source['source_blocks']}
        attached={b['block_id'] for b in source['images_in_attachment_order']}
        result=json.loads(path.read_text())
        assert not result['is_error']
        data=result['json_data'];coverage=reconcile_object_registry(data,set(blocks))
        for obj in coverage['objects']:
            for i,param in enumerate(obj.get('observed_parameters',[])):
                if not isinstance(param,dict):
                    issues.append({'packet':entry['stage'],'object_id':obj['id'],'parameter_index':i,'reason':'malformed_parameter'});continue
                bid=param.get('source_ref');block=blocks.get(bid)
                reason=None
                if block is None:reason='source_not_supplied'
                elif block['kind'] in ('TEXT','TABLE'):
                    quote=param.get('quote')
                    if not isinstance(quote,str) or not quote.strip():reason='missing_exact_text_quote'
                    else:
                        quote_count+=1
                        if quote in block['content']:matches+=1
                        else:reason='quote_not_exact_substring'
                elif bid not in attached:reason='graphic_not_attached'
                if reason:
                    issues.append({'packet':entry['stage'],'object_id':obj['id'],'parameter_index':i,'source_ref':bid,'reason':reason})
        for rec in coverage['source_inventory']:
            if rec['status'] in ('needs_data','not_checked') and blocks[rec['block_id']]['kind'] in ('IMAGE','GRAPHIC'):
                unresolved_graphics.append({'packet':entry['stage'],**rec})
        status_counts.update(row['status'] for row in coverage['operations'])
        rows.append({'packet':entry['stage'],'result_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'objects':len(coverage['objects']),'invalid_objects':len(coverage['invalid_objects']),
            'findings':len(data.get('text_findings',[])),'unresolved_operations':coverage['unresolved_operations'],
            'unresolved_source_blocks':coverage['unresolved_source_blocks'],
            'duration_ms':result['duration_ms'],'input_tokens':result.get('input_tokens'),'output_tokens':result.get('output_tokens')})
    report={'discipline':discipline,'source_md_sha256':manifest['source_md_sha256'],
        'completed_packets':len(rows),'total_packets':len(manifest['packets']),'packets':rows,
        'packet_local_objects':sum(r['objects'] for r in rows),'raw_findings':sum(r['findings'] for r in rows),
        'operation_status_counts':dict(status_counts),'quote_count':quote_count,'exact_quote_matches':matches,
        'parameter_evidence_issues':issues,'unresolved_graphics':unresolved_graphics,
        'duration_ms_sum':sum(r['duration_ms'] for r in rows),
        'input_tokens_sum':sum(r['input_tokens'] or 0 for r in rows),'output_tokens_sum':sum(r['output_tokens'] or 0 for r in rows),
        'semantic_verified':False,'whole_project_coverage_verified':False,
        'limits':['Object records may represent views or repeat across packets; not unique physical objects or defects.',
            'Exact quote match verifies text provenance only, not values, interpretation, comparison or usefulness.',
            'Graphic claims require image inspection. This script does not inspect pixels.',
            'Unresolved operations and sources remain unresolved; completed packet does not imply completed audit.']}
    Path(__file__).with_name(discipline+'_inventory_progress_assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({k:report[k] for k in ('completed_packets','total_packets','packet_local_objects','raw_findings','quote_count','exact_quote_matches','operation_status_counts')}))
    print('parameter_evidence_issues',len(issues),'unresolved_graphics',len(unresolved_graphics))
    return report


if __name__=='__main__':assess()
