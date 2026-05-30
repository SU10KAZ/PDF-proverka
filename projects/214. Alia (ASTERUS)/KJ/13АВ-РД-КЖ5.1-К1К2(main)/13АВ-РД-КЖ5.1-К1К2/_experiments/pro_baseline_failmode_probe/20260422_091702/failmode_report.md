# Pro baseline fail-mode probe — summary

Confirmatory probe of the 2 baseline-failing blocks (`6DRC-7KQL-9TJ`, `4MQJ-6NXP-4YH`) under high reasoning.

## Mode totals

| Mode | Label | Calls | Success | Success% | EmptyResp | EmptyAfterRetry | MultiAnalyses | WrongBlockId | APIError | RetryRecovered |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A | high+heal_on,par=2 | 10 | 10 | 100% | 0 | 0 | 0 | 0 | 0 | 0 |
| B | high+heal_off,par=2 | 10 | 10 | 100% | 0 | 0 | 0 | 0 | 0 | 0 |

## A — failure mode distribution
```
{
  "success": 10
}
```
Primary finish_reason distribution:
```
{
  "stop": 10
}
```
### Per-block (A)
- `6DRC-7KQL-9TJ` n=5 success=5/5 empty=0 empty_after_retry=0 multi=0 wrong_id=0 api_err=0 retry_recovered=0 median_dur=38.6s median_out_tok=2577 median_reason_tok=1985 max_raw_len=2707
  - failure_modes: {"success": 5}
- `4MQJ-6NXP-4YH` n=5 success=5/5 empty=0 empty_after_retry=0 multi=0 wrong_id=0 api_err=0 retry_recovered=0 median_dur=33.6s median_out_tok=3788 median_reason_tok=2962 max_raw_len=2820
  - failure_modes: {"success": 5}

## B — failure mode distribution
```
{
  "success": 10
}
```
Primary finish_reason distribution:
```
{
  "stop": 10
}
```
### Per-block (B)
- `6DRC-7KQL-9TJ` n=5 success=5/5 empty=0 empty_after_retry=0 multi=0 wrong_id=0 api_err=0 retry_recovered=0 median_dur=40.4s median_out_tok=2672 median_reason_tok=1915 max_raw_len=2294
  - failure_modes: {"success": 5}
- `4MQJ-6NXP-4YH` n=5 success=5/5 empty=0 empty_after_retry=0 multi=0 wrong_id=0 api_err=0 retry_recovered=0 median_dur=38.2s median_out_tok=4360 median_reason_tok=3433 max_raw_len=2916
  - failure_modes: {"success": 5}

## Diagnosis (rule-based)

- Mode A: NO failures observed in this probe — baseline 2/17 missing not reproduced under N=5 (transient/rate-dependent?).
- Mode B success ≈ Mode A → healing is NOT the differentiator.
