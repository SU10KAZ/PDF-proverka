# Phase E — Pro Selective Fallback ROI

Sample: 15 blocks (weakest from Flash full-doc run)

## Pro vs Flash on SAME blocks

| Metric | Flash (from full run) | Pro (this phase) | Delta |
|--------|-----------------------|------------------|-------|
| Returned | 15/15 | 15/15 | — |
| Unreadable | 0 | 0 | 0 |
| Empty KV | 1 | 0 | -1 |
| Zero findings | 15 | 1 | — |
| Total findings | 0 | 26 | **+26** |
| Total KV | 9867 | 304 | **-9563** |

## Cost

- Pro total cost: **$1.1479**
- Pro cost per sample block: **$0.07653**
- Pro cost per ADDED finding: **$0.04415**

## Recommendation

- **RECOMMENDED** selective Pro escalation for weak-heuristic blocks.
- Improved-block proxy: 16 of 15.
- Additional findings: +26 (2600.0% vs Flash on same blocks).
- Extra cost per improved block: ~$0.07174.


