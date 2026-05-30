# Crop stats by profile (resolution A/B)

| profile | blocks | long p50/p95/max | size_kb p50/p95/max | >=1000/1200/1500/2000 | risk h/n/l |
|---|---|---|---|---|---|
| r800 | 215 | 800.0/988.3/2629 | 39.5/69.4/605.6 | 10/5/4/2 | 11/151/53 |
| r1000 | 215 | 1000.0/1000.0/2629 | 50.2/91.6/605.6 | 215/5/4/2 | 11/151/53 |
| r1200 | 215 | 1200.0/1200.0/2629 | 64.2/110.6/605.6 | 215/215/4/2 | 11/151/53 |

## Predicted batch plan (baseline_p3 production batching)

| profile | total_batches | avg_batch | max_batch | max_heavy_in_batch | median_batch_kb |
|---|---|---|---|---|---|
| r800 | 33 | 6.52 | 8 | 2 | 271.6 |
| r1000 | 33 | 6.52 | 8 | 2 | 354.0 |
| r1200 | 33 | 6.52 | 8 | 2 | 428.3 |
