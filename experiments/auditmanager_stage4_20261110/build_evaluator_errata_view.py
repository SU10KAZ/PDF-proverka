"""Explicit working view over frozen targets; never silently rewrite the release."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

RELEASE=Path('experiments/auditmanager_stage1_20261109/benchmark_release_v1.json')
ERRATA=Path(__file__).with_name('benchmark_errata.json')


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def build_view():
    release=json.loads(RELEASE.read_text());errata=json.loads(ERRATA.read_text())
    assert release['status']=='frozen_evaluator_dataset'
    for entry in release['files']:
        assert sha(Path(entry['path']))==entry['sha256'],entry['path']
    original=release['denominators'];working=deepcopy(original)
    ids={t for values in original.values() for t in values};holds=[]
    for entry in errata['entries']:
        targets=entry.get('target_ids') or [entry['target_id']]
        assert all(t in ids for t in targets),targets
        status=entry['status']
        if status=='disputed_prior_refutation_basis_contradicted':
            dimensions=['truth_scoring','filter_acceptance']
            for route in working:working[route]=[t for t in working[route] if t not in targets]
        elif status=='disputed_output_section_only':
            dimensions=entry['excluded_metric_dimensions']
            assert dimensions==['report_section_accuracy']
        else:raise ValueError('Unknown errata status requires explicit handling: '+status)
        holds.append({'target_ids':targets,'status':status,'held_dimensions':dimensions,
            'evaluation_action':entry['evaluation_action'],'resolved':False})
    report={'status':'working_errata_overlay_not_comparison_freeze',
        'original_release':{'path':str(RELEASE),'sha256':sha(RELEASE),'version':release['version']},
        'errata':{'path':str(ERRATA),'sha256':sha(ERRATA)},'verified_release_files':len(release['files']),
        'original_denominators':original,'working_truth_denominators':working,'holds':holds,
        'original_counts':{k:len(v) for k,v in original.items()},'working_counts':{k:len(v) for k,v in working.items()},
        'rules':['Always retain full-set counts and excluded target-level reasons in comparison reports.',
            'EOM F032 report-section hold does not remove its factual target or unsupported-consequence checks.',
            'KM F014 is unresolved, not a passing negative filter control.',
            'No engineer decision or resolution is fabricated by applying a hold.',
            'Freeze the chosen evaluator version before comparative results are revealed; this working view is evaluator-only.'],
        'observed_quality_metrics':None,'production_acceptance':False}
    return report


def main():
    report=build_view()
    Path(__file__).with_name('evaluator_errata_working_view.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ['verified_release_files','original_counts','working_counts']},ensure_ascii=False))


if __name__=='__main__':main()
