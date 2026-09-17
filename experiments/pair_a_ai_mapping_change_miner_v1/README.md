# Pair A AI mapping + change miner generalization test

This is a no-tuning replication of the Pair B two-pass architecture on frozen
DEV Pair A (АР1), pair index 2. Preparation uses only admitted v002 PDFs,
structured source blocks, PDF-derived graphic crops, and page rasters. It does
not open the Pair A source audit, prior packages, prior ProjectChanges, or
historical findings.

The frozen split embargoes OLD physical pages 6, 8, 9, 11, 12, and 20. They are
never exposed to either model pass. Reports must therefore distinguish mapped
accessible pages (maximum 39) from the 45 physical pages in the source PDF.

Inference uses fresh tools-disabled `gpt-6-astra` / `xhigh` Codex/ChatGPT
contexts, one mapping call and one mining call per frozen group. OpenRouter is
not used. There are no retries or semantic repairs.

```sh
python -m experiments.pair_a_ai_mapping_change_miner_v1.run prepare
python -m experiments.pair_a_ai_mapping_change_miner_v1.run map
python -m experiments.pair_a_ai_mapping_change_miner_v1.run mine
```

Only a verified `CHANGE_MINER_FREEZE.json` permits opening Pair A evaluation
truth and creating the requested source-first audits and report.
