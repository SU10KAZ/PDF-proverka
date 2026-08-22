# Vision vs VectorDescription + deterministic diff

The same `gpt-5.6-sol` model compared the same five pairs in two isolated calls. The Vision call received only ten raster crops. The vector call received only Level 3 descriptions and deterministic diffs. Both ran in `/tmp`, read-only, without repository context.

| Pair | Vision | Vector + diff | Better arm |
|---|---|---|---|
| ss_scheme_text_changed | Correct and visually specific | Correct, specific and directly traceable | Tie |
| ss_table_graphic | Correctly reports no major change | False-positive “added position 1” from crop/span fragmentation | Vision |
| ar_plan | Essentially correct, but tiny text is unreadable | Quantifies near identity and the one-command difference | Vector |
| vk_nodes | Finds added notes/−0.034, but overstates the class | Verifies geometry, misses annotations because text is undecodable | Vision |
| eom_singleline_changed | Correct four-branch semantic explanation | Equally correct with deterministic evidence | Tie |

Scores (five 1–5 judgements per criterion, maximum 25):

| Arm | Accuracy | Completeness | Verifiability | Compactness | Total / 100 |
|---|---:|---:|---:|---:|---:|
| Vision | 23 | 24 | 19 | 15 | 81 |
| Vector + diff | 21 | 19 | 24 | 15 | 79 |

Compactness is mixed. The vector input was 137,896 characters (about 145 KB on disk), versus 3,095,180 bytes of PNGs, but the invocations reported 70,631 and 38,069 total tokens respectively. Level 3 therefore needs another reduction before it is economically preferable as an LLM input.

Conclusion: Vision was slightly more accurate and complete; vector evidence was distinctly more auditable. The result supports a gated hybrid, not replacement of either source.
