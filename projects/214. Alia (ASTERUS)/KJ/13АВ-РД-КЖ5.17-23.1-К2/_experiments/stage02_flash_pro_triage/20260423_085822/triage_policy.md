# Stage 02 Flash -> Pro Triage Policy

## Practical algorithm

1. Run `google/gemini-2.5-flash` on every block in single-block mode.
2. Build a Pro escalation set from Flash output and block metadata.
3. Run `google/gemini-3.1-pro-preview` only on selected blocks, still single-block.
4. Merge final stage 02 output by replacing escalated Flash analyses with successful Pro analyses.

## Default Pro escalation rules

- Complex/risky block with one or more Flash findings.
- Complex/risky block where Flash failed, returned no usable analysis, or marked the block unreadable.
- Any Flash finding with weak/uncertain extraction: inferred block id, weak summary, very low KV, unreadable.
- Any high-value Flash finding severity: critical, operational, economic, or cross-section check.

## Cost guardrail

Simple/light Flash-positive blocks stay Flash-only by default. Use
`--include-simple-findings` only when recall matters more than cost.

## Non-goals

- No Pro multi-block batching.
- No Claude comparison.
- No Flash production default changes.
- No recrop or block rebuild.
- No stage 03+ changes.
