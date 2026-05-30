# Winner Recommendation

1. `b8` survived: NO
2. `b10` launched: NO
3. `b10` survived: NO
4. Degradation starts at: `b8`
5. Practical upper bound now: `b6`
6. Go higher now: NO

## Why
- `b6` remains the safest practical ceiling for this stress set.
- `b8`: launched=True, survived=False, coverage=100.00%, degraded=5.
- `b10`: launched=False, survived=False, coverage=0.00%, degraded=0.

## Budget
- Cap: $2.00
- Additional spend: $0.1775

## Recommendation
- Keep `b6`; higher batch sizes are not justified by this experiment.
