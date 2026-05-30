# Comparison vs Claude

| Engine | Coverage | Missing/Dup/Extra | Strict S/F | Improved | Added findings | Degraded | Add blocks_with_findings | Add KV | Summary specificity | Retry trig/rec | Cost USD | Elapsed s | Reuse |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| claude_reused | 100.00% | 0/0/0 | 17/0 | 15 | +46 | 1 | +11 | +21 | 12 | n/a | $0.5446 | 264.2 | reused |
| pro_high_p2 | 100.00% | 0/0/0 | 17/0 | 11 | +22 | 4 | +10 | +3 | 1 | 0/0 | $0.9487 | 351.2 | ran_now |
| pro_high_p1 | 100.00% | 0/0/0 | 17/0 | 11 | +19 | 5 | +10 | +7 | 2 | 2/2 | $0.8089 | 839.9 | ran_now |
| pro_low_p2 | 100.00% | 0/0/0 | 17/0 | 10 | +10 | 6 | +10 | +45 | 1 | 0/0 | $0.1839 | 91.2 | ran_now |
