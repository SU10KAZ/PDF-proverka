# Phase A Quality Gate Result

## Phase A Quality Gate: google/gemini-2.5-flash vs google/gemini-3.1-pro-preview

- PASS coverage=100% (flash=100.0%)
- PASS missing=0 (flash=0)
- PASS duplicate=0 (flash=0)
- PASS extra=0 (flash=0)
- PASS unreadable flash<=pro (0<=0)
- FAIL blocks_with_findings flash>=95% of pro (24.5%)
- FAIL total_findings flash>=95% of pro (41.3%)
- PASS median_kv flash>=90% of pro (158.3%)
- PASS cost/valid_block flash substantially < pro (flash=$0.00263 pro=$0.06450)

**GATE RESULT: Flash FAILED** (2 checks failed)
-> Mainline candidate: **google/gemini-3.1-pro-preview**
-> Fallback: N/A (pro is mainline)

## Recommendation
- **Mainline**: google/gemini-3.1-pro-preview
- **Fallback/Escalation**: google/gemini-3.1-pro-preview
