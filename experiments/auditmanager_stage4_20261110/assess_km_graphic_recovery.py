"""Assess source-reading recovery; never equate reported coverage with truth."""
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry
from .run_km_graphic_recovery import ROOT


def main():
    manifest=json.loads((ROOT/'manifest.json').read_text())
    baseline_path=Path('/tmp/auditmanager_whole_project_inventory_20261110/KM/execution/aggregate_coverage.json')
    baseline=json.loads(baseline_path.read_text());original={r['block_id']:r for r in baseline['source_inventory']}
    rows=[];pending=[]
    for packet in manifest['packets']:
        folder=ROOT/f"packet_{packet['packet']:02d}";output=folder/'result.json'
        if not output.exists():pending.append(packet['packet']);continue
        request=json.loads((folder/'request.json').read_text());result=json.loads(output.read_text())
        assert evidence_digest(request)==packet['request_sha256']
        assert json.loads((folder/'result_binding.json').read_text())=={'request':packet['request_sha256'],'result':evidence_digest(result)}
        if result['is_error']:
            rows.append({'packet':packet['packet'],'status':'provider_error','error_message':result.get('error_message')});continue
        data=result['json_data'];coverage=reconcile_object_registry(data,set(packet['block_ids']))
        parameter_issues=[];parameters=0
        for obj in coverage['objects']:
            for i,p in enumerate(obj.get('observed_parameters',[])):
                parameters+=1
                if not isinstance(p,dict) or p.get('source_ref') not in obj['source_refs'] or not isinstance(p.get('graphic_observation'),str) or not p['graphic_observation'].strip():
                    parameter_issues.append({'object_id':obj['id'],'parameter_index':i,'parameter':p})
        rows.append({'packet':packet['packet'],'status':'response_assessed_structurally',
            'result_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
            'duration_ms':result['duration_ms'],'input_tokens':result['input_tokens'],'output_tokens':result['output_tokens'],
            'objects':len(coverage['objects']),'invalid_objects':coverage['invalid_objects'],
            'observed_parameters':parameters,'parameter_provenance_issues':parameter_issues,
            'unexpected_operation_receipts':data.get('object_operation_results',[]),
            'source_changes':[{'block_id':r['block_id'],'before':original[r['block_id']], 'after':r} for r in coverage['source_inventory']],
            'raw_findings':data.get('text_findings',[]),'semantic_verified':False})
    report={'manifest_sha256':evidence_digest(manifest),'baseline_coverage_sha256':hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
        'completed_packets':len(rows),'total_packets':len(manifest['packets']),'pending_packets':pending,'packets':rows,
        'source_blocks_reported_inventoried':sum(x['after']['status']=='reported_inventoried' for r in rows for x in r.get('source_changes',[])),
        'source_blocks_still_unresolved':sum(x['after']['status'] in ('needs_data','not_checked') for r in rows for x in r.get('source_changes',[])),
        'baseline_artifacts_changed':False,'semantic_coverage_complete':False,'new_useful_defects_confirmed':0,
        'limits':['Readability recovery on known selected source blocks; not independent unseen-project evaluation.',
            'Image scale and prompt both changed; no isolated causal attribution or measured engineering recall improvement.',
            'Object counts are extraction records, not physical item counts; no automatic cross-pass identity merge.',
            'Reported inventoried status and graphic_observation text require visual/source checking.',
            'Whole-project coverage and technical operations remain incomplete.']}
    Path(__file__).with_name('KM_graphic_recovery_assessment.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('completed_packets','pending_packets','source_blocks_reported_inventoried','source_blocks_still_unresolved')}))

if __name__=='__main__':main()
