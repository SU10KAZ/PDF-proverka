# Pro Diagnostic — variant comparison

| Metric | v1_control_baseline | v2_thinking_low_heal_on | v3_thinking_low_heal_off |
| --- | --- | --- | --- |
| Label | control: baseline Pro (reuse, healing=ON, parallelism=2, no thinking override) | Pro + reasoning.effort=low, healing=ON, parallelism=2 | Pro + reasoning.effort=low, healing=OFF, parallelism=2 |
| thinking_low | False | True | True |
| response_healing_initial | True | True | False |
| parallelism | 2 | 2 | 2 |
| total_blocks | 17 | 17 | 17 |
| coverage_pct | 88.24% | 100.00% | 100.00% |
| missing | 2 | 0 | 0 |
| duplicate | 0 | 0 | 0 |
| extra | 0 | 0 | 0 |
| missing_block_ids | 6DRC-7KQL-9TJ, 4MQJ-6NXP-4YH | — | — |
| improved | 10 | 9 | 8 |
| unchanged | 1 | 2 | 3 |
| degraded | 4 | 6 | 6 |
| degraded_block_ids | 4UTW-PPGP-VEN, 9J9X-DXHT-6GJ, 7NAU-TNME-3AR, 6PCX-NFHU-6KW | 4UTW-PPGP-VEN, 6DRC-7KQL-9TJ, 9J9X-DXHT-6GJ, 7NAU-TNME-3AR, 9LPD-VX9H-YHK, 6PCX-NFHU-6KW | 4UTW-PPGP-VEN, 6DRC-7KQL-9TJ, 9J9X-DXHT-6GJ, 7NAU-TNME-3AR, 9LPD-VX9H-YHK, 6PCX-NFHU-6KW |
| additional findings vs Flash | 21 | -7 | -8 |
| blocks_with_findings | 14 | 14 | 12 |
| total_findings (engine) | 29 | 14 | 13 |
| total_key_values (engine) | 179 | 309 | 300 |
| median_key_values | 11.0 | 12.0 | 11.0 |
| elapsed_s | 458.3 | 93.9 | 96.3 |
| avg / median / p95 dur (s) | 52.4 / 30.8 / 152.8 | 10.7 / 10.1 / 24.8 | 11.0 / 9.7 / 19.8 |
| prompt tok | 32049 | 35867 | 35865 |
| output tok | 68374 | 9106 | 8901 |
| reasoning tok | 56388 | 0 | 0 |
| cost USD | $0.8846 | $0.1810 | $0.1785 |
| cost / valid block | $0.05897 | $0.01065 | $0.01050 |
| cost source actual/est | 15/2 | 17/0 | 17/0 |
| healed responses | n/a | 17 | 0 |
| empty_response_count | n/a | 0 | 0 |
| empty_response_after_retry_count | n/a | 0 | 0 |
| empty_response_block_ids | — | — | — |
| empty_after_retry_block_ids | — | — | — |
