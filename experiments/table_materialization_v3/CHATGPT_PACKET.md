# ChatGPT holdout package

`chatgpt_packet.py` exports the existing frozen 27-case / 27-document holdout.
It preserves the original case IDs, document versions, line anchors and source
segment spans. Cases are ordered by case ID, independently of selection groups.
The exporter does not run Table V3 or read DEV/EVAL answer files.

The package contains `manifest.json`, Russian `CHATGPT_INSTRUCTIONS.md`, and
`cases/case_01` through `cases/case_27`. Each case contains the two source crops,
the exact OCR fragments, adjacent recognized text, full-page PNGs for the target
pages and two neighbors on either side, and a PDF/Markdown source context. The
PDF covers the surrounding uninterrupted run of pages with literal OCR table
rows, two pages on each side, and the document's first page. This rule only
expands source coverage; it does not determine the adjudication answer.

Explicit crop coordinates were checked against the source rasters to distinguish
tables sharing one OCR block. Full source pages remain available. Visible PDF
annotations are flattened into the extracted PDF; interactive notes, fields,
external PDF links and document metadata are not exported. OCR image-analysis
blocks are excluded from the short and extended text context. The selected
source segments and anchors are preserved verbatim, including OCR imperfections.

Only `YES`, `NO`, `UNSURE`, and `BROKEN` are allowed as adjudicator responses.
The package contains no prefilled responses or selector/rule/model results.

Build, inspect the images, then finalize the ZIP:

```bash
python -m experiments.table_materialization_v3.chatgpt_packet
python -m experiments.table_materialization_v3.chatgpt_packet --publish
```

The default artifact directory is
`/home/coder/auditmanager/corpus-audits/20260913_table_materialization_v3/`.
Builds use `CHATGPT_TABLE_HOLDOUT_27.building/` and refuse to overwrite existing
work. Publication produces `CHATGPT_TABLE_HOLDOUT_27/`,
`CHATGPT_TABLE_HOLDOUT_27.zip`, a SHA-256 sidecar and an external verification
report. The report and exporter code are not included in the blind ZIP.

Publication checks all 54 anchors against both the frozen blind packet and
the source ledger, source file hashes, exact crop/full-page pixels, all image
decodes, extracted PDF page mappings and native text, near-identical PDF rasters,
the file inventory, forbidden payload fields and labels, and ZIP contents/CRC.
Small PDF color rounding differences are tolerated (at most 12/255 per channel
and 0.3 mean per channel); PNG comparisons are exact. The source selection
receipt is checked against the existing frozen independence report.

The delivered package has 27/27 cases, 200 PNGs and 490 context PDF pages across
27 PDFs. All 54 target crops were visually inspected. Existing holdout sources,
candidate code and policy remain unchanged.
