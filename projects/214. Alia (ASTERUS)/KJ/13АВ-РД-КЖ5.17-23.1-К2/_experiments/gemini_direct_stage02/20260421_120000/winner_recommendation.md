# Winner Recommendation

## Final Answers

| Question | Answer |
|----------|--------|
| Mainline model | **gemini-3.1-pro-preview** |
| Fallback/Escalation model | **gemini-3.1-pro-preview** |
| Batch profile | **b10** |
| Parallelism | **3** |
| Flex recommendation | Not tested |

## Phase A Quality Summary

| Model | Coverage | Findings | KV median | Cost/block |
|-------|----------|----------|-----------|------------|
| gemini-2.5-flash | 0.0% | 0 | 0.0 | $0.00000 |
| gemini-3.1-pro-preview   | 0.0%   | 0   | 0.0   | $0.00000   |

## Cost/Quality Trade-off

- **Flash** is cheaper than **Pro**
- Flash did not meet quality gate — Pro recommended
- Standard tier winner for normal pipeline; Flex as optional bulk mode

## Batch Profile Winner: b10
Not tested

## Parallelism Winner: 3
Not tested

## What did NOT pass
- Production defaults remain UNCHANGED — no global switch made
- Direct Gemini path requires explicit GEMINI_DIRECT_API_KEY
- No Flex as default — standard tier remains mainline for latency-sensitive runs
