"""Account once for each recent EOM/AR/KM/AK request, including failed attempts.

This bounded ledger is not the complete six-project cost comparison. Subscription
counters are not money and missing counters are never interpreted as free usage.
"""
import hashlib
import json
from pathlib import Path


def summarize():
    directories = [
        ('EOM_instance_inventory', Path('/tmp/auditmanager_eom_instance_inventory_control_20261110/EOM/execution/object_inventory_000')),
        ('EOM_instance_graphic', Path('/tmp/auditmanager_eom_instance_graphic_control_20261110')),
        ('EOM_instance_identity', Path('/tmp/auditmanager_eom_instance_operations_20261110/hvac_32')),
        *[(f'EOM_cable_graphic_{i}', Path('/tmp/auditmanager_eom_cable_graphic_control_20261110') / f'block_{i:03d}') for i in (0, 1)],
        ('EOM_cable_inventory', Path('/tmp/auditmanager_eom_cable_inventory_control_20261110/EOM/execution/object_inventory_000')),
        ('EOM_initial', Path('/tmp/auditmanager_eom_balance_inventory_control_20261110/EOM/execution/object_inventory_000')),
        ('EOM_extraction_only', Path('/tmp/auditmanager_eom_balance_inventory_control_20261110_extraction_only/EOM/execution/object_inventory_000')),
        *[(f'EOM_split_{i}', Path('/tmp/auditmanager_eom_balance_inventory_control_20261110_split/EOM/execution') / f'object_inventory_{i:03d}') for i in range(5)],
        ('EOM_graphic_strip', Path('/tmp/auditmanager_eom_graphic_recovery_20261110')),
        ('EOM_graphic_grid', Path('/tmp/auditmanager_eom_graphic_recovery_20261110_grid')),
        ('EOM_graphic_grid_claude', Path('/tmp/auditmanager_eom_graphic_grid_claude_control_20261110')),
        ('EOM_prescan', Path('/tmp/auditmanager_eom_prescan_recovery_control_20261110')),
        ('EOM_prescan_followup', Path('/tmp/auditmanager_eom_prescan_recovery_control_20261110/followup')),
        *[(f'AR_recovery_{i}', Path('/tmp/auditmanager_ar_deferred_control_20261110') / f'pass_{i}') for i in range(2)],
        ('AR_unlabelled_inventory', Path('/tmp/auditmanager_ar_unlabelled_inventory_control_20261110/AR/execution/object_inventory_000')),
        ('AR_unlabelled_inventory_v2', Path('/tmp/auditmanager_ar_unlabelled_inventory_control_20261110_regions_v2/AR/execution/object_inventory_000')),
        *[(f'AR_region_{i:02d}', Path('/tmp/auditmanager_ar_region_operations_20261110') / f'region_{i:02d}') for i in (5, 7, 8, 9, 10, 11)],
        *[(f'AR_region05_context_{i}', Path('/tmp/auditmanager_ar_region_operations_20261110/region_05') / f'context_{i}') for i in (1, 2)],
        *[(f'KM_recovery_{i}', Path('/tmp/auditmanager_km_deferred_control_20261110') / f'pass_{i}') for i in range(2)],
        ('KM_node_navigation', Path('/tmp/auditmanager_km_node_navigation_control_20261110')),
        ('KM_LK6_quantity', Path('/tmp/auditmanager_km_quantity_operations_20261110/obj_09')),
        *[(f'KM_LK6_quantity_context_{i}', Path('/tmp/auditmanager_km_quantity_operations_20261110/obj_09') / f'context_{i}') for i in (1, 2, 3)],
        ('KM_OGM20_geometry', Path('/tmp/auditmanager_km_geometry_operations_20261110/E7')),
        ('KM_adjacent_flights', Path('/tmp/auditmanager_km_geometry_operations_20261110/mark_ogm_6')),
        *[(f'AK_inventory_{i}', Path('/tmp/auditmanager_ak_cabinet_inventory_control_20261110/AK/execution') / f'object_inventory_{i:03d}') for i in range(5)],
        ('AK_identity_0_v1', Path('/tmp/auditmanager_ak_cabinet_operations_20261110/packet_000')),
        *[(f'AK_identity_{i}_v2', Path('/tmp/auditmanager_ak_cabinet_operations_20261110/local_claim_v2') / f'packet_{i:03d}') for i in range(5)],
        *[(f'AK_identity_{i}_v3', Path('/tmp/auditmanager_ak_cabinet_operations_20261110/page_context_v3') / f'packet_{i:03d}') for i in range(5)],
        ('AK_identity_1_v4', Path('/tmp/auditmanager_ak_cabinet_operations_20261110/graphic_context_v4/packet_001')),
        ('AK_identity_2_interrupted', Path('/tmp/auditmanager_ak_cabinet_operations_20261110/local_claim_v2/packet_002/interrupted_01')),
        ('AK_graphic_interrupted', Path('/tmp/auditmanager_ak_terminal_graphic_control_20261110/interrupted_01')),
        ('AK_inventory_4_interrupted', Path('/tmp/auditmanager_ak_cabinet_inventory_control_20261110/AK/execution/object_inventory_004/interrupted_01')),
        ('AK_graphic_interrupted_2', Path('/tmp/auditmanager_ak_terminal_graphic_control_20261110/interrupted_02')),
        ('AK_terminal_graphic', Path('/tmp/auditmanager_ak_terminal_graphic_control_20261110')),
    ]
    rows = []
    seen = set()
    for label, folder in directories:
        assert folder.resolve() not in seen
        seen.add(folder.resolve())
        attempt = folder / 'attempt.json'; output = folder / 'result.json'
        row = {'control': label, 'directory': str(folder), 'status': 'not_started'}
        rows.append(row)
        if not attempt.exists():
            continue
        row['attempt_receipt'] = json.loads(attempt.read_text())
        if row['attempt_receipt'].get('status') == 'interrupted':
            row.update(status='interrupted',token_counters_incomplete=True)
            continue
        row['status'] = 'result_pending'
        row['token_counters_incomplete'] = True
        if not output.exists():
            continue
        raw = output.read_bytes(); result = json.loads(raw)
        row.update(status='failed' if result.get('is_error') else 'response_received',
                   result_sha256=hashlib.sha256(raw).hexdigest(),
                   model=result.get('model'), duration_ms=result.get('duration_ms'),
                   input_tokens=result.get('input_tokens'), output_tokens=result.get('output_tokens'),
                   cost_source=result.get('cost_source'), error=result.get('error_message'))
        row['token_counters_incomplete'] = any(
            result.get(key) is None for key in ('input_tokens', 'output_tokens')) or bool(
            result.get('is_error') and not (result.get('input_tokens') or result.get('output_tokens')))
    completed = [r for r in rows if 'result_sha256' in r]
    return {'scope': 'Recent EOM extraction/recovery, AR/KM deferred, AR unlabelled inventory and AK cabinet controls only; not all stage4 work.',
            'requests': rows, 'completed_requests': len(completed),
            'pending_results': sum(r['status'] == 'result_pending' for r in rows),
            'failed_requests': sum(r['status'] == 'failed' for r in rows),
            'interrupted_requests': sum(r['status'] == 'interrupted' for r in rows),
            'reported_input_tokens': sum(r.get('input_tokens') or 0 for r in completed),
            'reported_output_tokens': sum(r.get('output_tokens') or 0 for r in completed),
            'token_accounting_complete': not any(r.get('token_counters_incomplete',False) for r in rows),
            'completed_request_minutes_sum': round(sum(r.get('duration_ms') or 0 for r in completed)/60000, 3),
            'wall_clock_elapsed_minutes': None, 'monetary_cost_known': False,
            'boundaries': ['Parallel request durations must not be summed as wall-clock elapsed time.',
                           'Subscription token counts do not establish remaining quota or monetary cost.',
                           'Replays from sealed results incur no new model request and are not recounted.',
                           'Response received does not establish semantic acceptance.']}


if __name__ == '__main__':
    report = summarize()
    Path(__file__).with_name('recent_control_usage.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k != 'requests'}, ensure_ascii=False))
