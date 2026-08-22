# p12_hybrid — adversarial verification

Verifier: independent Track B agent. Goal: refute, not agree. Everything below is either a
command I ran or a file I read. Model calls used the same binary/model as the probe
(`/home/coder/.vscode-server/extensions/openai.chatgpt-26.818.41705-linux-x64/bin/linux-x86_64/codex`,
`gpt-5.6-sol`). `tiktoken` was installed in a throw-away venv (it is absent from every
interpreter on this box, `/opt/py312` included).

| claim | verdict |
|---|---|
| 1 — bytes are not the billing unit; Cyrillic JSON 2.73 B/tok vs PNG 158.9 B/tok, 58.2× | **WEAKENED** |
| 2 — 75.0 % of the vector bill is its prompt; Vision images 51.2 %, harness 36.5 % | **WEAKENED** |
| 3 — L3.texts = 49.7 %; 39.9 % of payload provably non-informative | **WEAKENED** |
| 4 — minimal payload 3,940 vs 53,299 (13.5×); 20,483 end-to-end at same coarse accuracy | **WEAKENED** |
| 5 — 127×/143× where nothing meaningful changed, plateau 2.9× on the real change | **REFUTED (as stated)** |

---

## 0. What I re-ran / re-measured

Deterministic probes reproduce **byte-identically** (so no fabrication anywhere):

```
$V -m ...probes.hybrid_p1_prompt_composition   -> hybrid_prompt_composition.json   identical
$V -m ...probes.hybrid_p4_wasted_tokens        -> hybrid_wasted_tokens.json        identical
$V -m ...probes.hybrid_p2_minimal_payload      -> hybrid_minimal_payload_sizes.json identical
```

New **live measurements** I made (the probe estimated these; I measured them):

| what | probe's number | I measured | how |
|---|---|---|---|
| codex harness baseline, no schema | 13,892 | **13,892** | 1 zero-image call, `--json` `usage.input_tokens` |
| codex harness baseline **with `--output-schema`** (what Track A actually used) | not measured | **14,010** (= 14,003 + 7-tok prompt) | same, `--output-schema ai_output_schema.json` |
| vector arm total input (archived `vector_prompt.txt`, schema, stdin) | 13,892+52,983 = 66,875 assumed | **66,986** | 1 call |
| → vector prompt tokens | 52,983 (o200k) | **52,983** (66,986 − 14,003) | exact match — the tokenizer risk H1 names is closed |
| vision arm total input (archived `vision_prompt.txt` + the 10 crops + schema) | 13,892+192+19,479 = 33,563 assumed | **33,105** | 1 call |
| image tokens, 10 benchmark crops | 19,479 (formula) | **18,817** (32,709 − 13,892, twice, identical) and **18,910** (33,105 − 14,003 − 192) | 3 calls |

---

## CLAIM 1 — WEAKENED

**What survives.** 144,669 B → 52,983 o200k tokens is right, and I confirmed it against the
live API, not just against tiktoken: `66,986 − 14,003 = 52,983` exactly. 2.73 B/token is
correct arithmetic. So the falsification route the probe itself named for H1 ("tokenize with
the provider's real tokenizer and get a materially different count") is closed — good.

**"Cyrillic" is the wrong cause — REFUTED.** I decomposed the archived prompt:

```
chars 137,896  bytes 144,669  tokens 52,983
Cyrillic chars      6,125  =  4.4 % of chars, 8.5 % of bytes
ASCII chars       131,192  = 95.1 %
digits             29,057  = 21.1 %
ascii-only substream:    131,192 B -> 49,769 tok = 2.64 B/tok
non-ascii substream:      13,477 B ->  3,295 tok = 4.09 B/tok
```

The payload is **95 % ASCII**, and the ASCII part is the part that tokenizes badly
(2.64 B/tok vs 4.09). Cyrillic contributes 6.2 % of the prompt's tokens; delete every Russian
character and the bill drops 6 %. The real driver is coordinate/hash/id soup (21 % of
characters are digits). Calling it "minified **Cyrillic** JSON" mis-attributes the cause.

**58.2× is a property of the PNG encoder, not of billing — REFUTED as a measurement.**
The token cost of an image depends on pixel area (capped), not on file bytes, so "bytes per
token" for an image is whatever the compressor happened to do. I re-encoded the same ten
crops (identical pixels ⇒ identical token cost):

```
orig PNG        3,095,180 B -> 158.9 B/tok -> 58.2x     (probe's number)
PNG optimize=1  3,767,406 B -> 193.4 B/tok -> 70.8x
JPEG q85        2,344,853 B -> 120.4 B/tok -> 44.1x
```

Same billing, ratio swings 44×–71×. And with my **measured** 18,910 image tokens the probe's
own configuration gives 163.7 B/tok → **59.9×**, not 58.2×.

**The framing is inverted.** In the billing unit the vector payload *was* larger: 52,983
tokens of prompt against 19,102 tokens of vision content (18,910 images + 192 prompt) —
**2.77×**. "Cost more not because its payload was large" is rhetoric; the honest sentence is
"a token-dense text payload of 53 k tokens costs more than 19 k tokens of images".

**The reconciliation is looser than advertised.** With measured images the "gap explained"
becomes 52,983 − 18,910 = **34,073** against a gap of 32,562 (miss 1,511, not 942). The gap
is exactly `Δinput − Δoutput` = 33,881 − 1,319 = 32,562; "prompt minus images" is a
near-coincidence that silently drops the 192-token vision prompt and the 1,319-token output
difference.

## CLAIM 2 — WEAKENED

**75.0 % is right to the decimal.** 52,983 / 70,631 = 75.01 %, and 52,983 is now a measured
number, not an estimate. Harness baseline 13,892 reproduced exactly on a fresh call.

**But the quantity being decomposed was never identified.** codex's `tokens used` line (the
source of Track A's 70,631 / 38,069) is **not** input+output. Three *identical* zero-image
calls printed `tokens used` = **841**, **1 865**, **3 913**. Matching those against `--json`
usage from the same configuration:

```
input 13,892, cached 13,056, output 5 -> 13,892-13,056+5 =   841
input 13,892, cached 12,032, output 5 -> 13,892-12,032+5 = 1,865
input 13,892, cached  9,984, output 5 -> 13,892- 9,984+5 = 3,913
```

`tokens used = input_tokens − cached_input_tokens + output_tokens`. This is **exactly the
falsification condition H2 states for itself** ("show `tokens used` in codex excludes cached
input, which would change the residual") and it is met. The whole reconciliation
(`base + prompt + output + residual = 70,631`) is valid only if `cached_input_tokens = 0` in
Track A's two archived runs — which nobody checked and which cannot be checked from
`invocation_metadata.json` (it stores only the stderr tail). The arithmetic happens to close
(66,986 measured input + 3,645 ⇒ 70,631), so cache was probably 0; but the probe did not know
what it was decomposing, and the same number on a warm cache would have been ~50 k.

**"Images are 51.2 %" — REFUTED by direct measurement.** The 19,479 comes from the probe's
own fitted formula for 7 of 10 crops, three of them pinned at the saturation ceiling where
the formula is known to miss badly (its own validation row `ss_table_extra_contour_z4`:
predicted 3,051, measured 1,279 — a 58 % error). Measured directly, twice, stable:

```
10 crops + tiny prompt: input 32,709 - 13,892 = 18,817 image tokens
archived vision prompt: input 33,105 - 14,003 - 192 = 18,910 image tokens
=> image share of the 38,069 bill = 49.4 % .. 49.7 %, NOT 51.2 %
```

The qualitative statement flips: images are just **under** half the Vision bill.

**"Harness prompt alone is 36.5 % (13,892, measured over 3 zero-image calls)" — off-config.**
Those three calls did not pass `--output-schema`, which Track A's runs did. With the schema
the fixed overhead is **14,003 + 7**, i.e. 118 tokens more: the true share is **36.8 %**. Small,
but it means the "measured baseline" was not measured on the invocation being decomposed.

**"Reasoning only 2,285" is a residual, not a measurement.** Measured: vector output =
70,631 − 66,986 = **3,645** tokens, of which the archived JSON answer is 1,471, so reasoning
≈ 2,174. Same shape for Vision: output = 4,964, reasoning ≈ 4,029 (probe said 3,571). The
residual absorbs tokenizer error, schema tokens and cache effects; here it lands within ~5 %,
but nothing in the method guarantees that.

## CLAIM 3 — WEAKENED

**The headline half is CONFIRMED, and by a cleaner route than the probe's.** The probe measured
a *rebuilt* payload (`prompt_rebuild_matches_artifact: false`). I parsed the five JSON blocks
out of the **archived** `vector_prompt.txt` — the bytes that were actually billed — and got:

```
per-pair payload sum          52,865 tok   (probe's rebuild: 53,299)
L3.texts (both sides, 5 pairs) 26,471 tok = 50.07 %
```

Identical absolute number, slightly *higher* share. "Half the vector prompt is one field" stands.

**39.9 % "provably non-informative" is inflated three ways.**

1. *Phantom field.* I diffed rebuild vs archive: the only difference is `hatch_candidates`,
   added to L3 **after** the AI run (5 blocks, +989 B, +434 tok). The probe's W3 counts
   `hatch_candidates` (384 tok, 20 % of W3) as waste in a payload that never contained it.
2. *One block carries it.* W1 undecodable = 13,232 tok, of which **13,212 (99.8 %) is
   `vk_nodes`**; the other four pairs score 5 tokens each — literally the cost of an empty
   JSON array. Drop that single pair and the corpus waste is **23.0 %**
   ((21,251 − 14,093 − 302) / (52,865 − 23,107)), not 39.9 %. The claim reports 60.7 % on
   vk_nodes as a detail; in fact vk_nodes *is* the finding.
3. *Wrong provenance.* "text the extractor itself flagged UNDECODABLE" is doubly wrong:
   the flag lives in **`comparator.py:282-284`**, not the extractor, and it is a **layer-level**
   verdict (`suspicious >= 5 or ratio >= 0.02` over the whole stream). The 13,232-token figure
   comes from the probe's own per-span rule (`hybrid_p4_wasted_tokens.garbage`: ≥1 control
   char). Directionally fine for vk_nodes, but it is the probe's ground truth, not the tool's.
4. *"sha256/pattern ids" is 21 % hashes.* Recomputed on the archived prompt:
   sha256 signature strings **1,315**, `L3.patterns` **2,535**, `diff.repeated_patterns`
   **2,255** = 6,105. Two thirds of that is pattern lists that also carry *counts*
   (`["pattern_72facc24ae3d",15]`), and `hybrid_pattern_stability.json` shows ids do match
   across sides (ar_plan Jaccard 0.817, ss_plan_dense 0.471). Useless-to-an-LLM is a
   reasonable judgement; **"provably"** is not earned.

## CLAIM 4 — WEAKENED

Reproduces exactly (`hybrid_minimal_payload_sizes.json` byte-identical on re-run; 3,940 =
604+273+118+162+2,783; usage input 18,197 + output 2,286 = 20,483). Same model, same schema,
same `model_reasoning_effort="xhigh"` — that part of the design is sound.

**"At the same coarse accuracy" is not supported by the probe's own table.**
`hybrid_arm_comparison.json` gives vector **4/5**, vision **3/5**, minimal **3/5**. So the
minimal arm is *worse than the arm it replaces*. Worse still on content precision, by the
probe's own column: `false_major_changes_on_quiet_pairs` — vision 0, trackA_vector 1,
**minimal_base 2**. And the benchmark is the known-broken one (O1/O2): I re-verified O1
myself — `ar_plan` left and right are the **same file**, sha256 `3d7242cd5e72b326…`, page 8
both sides. Strip the degenerate pair and the unreadable-text pair and score on the three
pairs that actually test anything:

```
                 ss_scheme   ss_table   eom      informative score
human            SSVC        NEAR_ID    SC
trackA_vector    SSVC ok     NEAR_ID ok SC ok    3/3
trackA_vision    SSVC ok     NEAR_ID ok SC ok    3/3
minimal_base     SSVC ok     SSVC  MISS SC ok    2/3
```

n = 3 informative pairs. That is not "the same coarse accuracy"; it is one arm being wrong
where both others are right, on a sample too small to distinguish either way.

**13.5× is inflated by the two pairs that carry nothing** (see claim 5). On the three pairs
where the minimal payload actually transmits content: 14,821 → 3,660 = **4.05×**.

**One correction in the claim's favour:** at the bill level the three arms share a 14,003-token
fixed harness cost. Net of it, minimal is 6,480 vs vector 56,628 vs vision 24,066 — **8.7×**
and **3.7×**, not 3.45×/1.86×. The end-to-end comparison is fair in unit (all three totals are
`uncached input + output`) *provided* cache was 0 in all three; the minimal run records
`cached_input_tokens: 0`, Track A's two runs record nothing.

## CLAIM 5 — REFUTED (as stated)

Arithmetic reproduces (ar_plan 15,035→118, vk_nodes 23,199→162, eom 8,151→2,783, 32 clusters).
The *characterisation* does not survive.

**1. The 127×/143× payloads contain no changes at all.** From
`hybrid_minimal_payloads.json`, both are `"changes": []` plus an uncertainty stub:

```
ar_plan  118 tok: changes [], uncertainty [GEOMETRY_TOO_DENSE_TO_LOCALISE (18,080/18,069 segments)]
vk_nodes 162 tok: changes [], uncertainty [TEXT_LAYER_UNDECODABLE, GEOMETRY_TOO_DENSE_TO_LOCALISE]
```

They are produced by two hard bail-outs in `hybrid_p2_minimal_payload.py`:
`text_undecodable → pass` (emit no events) and `DENSE_SEGMENT_LIMIT = 6000` (skip
localisation). A 143× "reduction" achieved by declining to answer is not a reduction on
comparable content — a 0-token payload would score ∞×.

**2. The threshold is set on the evaluation set.** `DENSE_SEGMENT_LIMIT = 6000` splits these
very five pairs cleanly (eom 704 primitives → localised; ar_plan 18 k and vk_nodes 20 k
segments → bail). `eps=0.06`, `min_size=8`, pairing distance `0.04` are likewise hand-set with
no held-out pair. This is the "filter tuned on the data it is evaluated on" trap.

**3. "Nothing meaningful changed" is false for vk_nodes.** Track A's crop-based human
validation (`human_validation.json`) records `vk_nodes: STRUCTURE_SAME_VALUES_CHANGED` —
"RIGHT includes an additional notes area and a −0.034 annotation". The Vision arm found both.
The minimal payload transmits neither, and the model duly answered **INSUFFICIENT_DATA** — a
miss, recorded in the probe's own `hybrid_arm_comparison.json`. So one of the two "127×/143×"
pairs is a pair where the representation **lost a real change**, which is precisely the
falsification condition H5 lists for itself.

**4. The other one is O1.** `ar_plan` compares a PDF to itself (verified: identical sha256).
"Nothing meaningful changed" there is a property of the benchmark, not of the payload.

**What does survive:** the 2.9× plateau on `eom_singleline_changed` (8,151 → 2,783, 32
disjoint clusters) is real, reproduces, and is the only one of the three numbers measured on a
pair with genuine engineering change. As an argument that a span-level diff cannot compress a
real structural change it is sound. As "the measured boundary", it rests on a single pair.

---

## Traps checked

| trap | result |
|---|---|
| claim resting on 1–2 blocks | **hit** — claim 3's 39.9 % is 99.8 % one block (vk_nodes); claim 5's headline is 2 blocks, one of them a PDF vs itself; claim 4/5's plateau is 1 block |
| ground truth the probe assigned itself | **not hit** — labels come from Track A `human_validation.json`, method line says each crop was inspected visually; the probe did not invent them |
| precision/recall without a denominator | **partial** — "3/5 vs 4/5 vs 3/5" states its denominator, but the informative denominator is 3, and `false_major_changes_on_quiet_pairs` (where minimal is worst) is omitted from the claim |
| filter tuned on the evaluated data | **hit** — `DENSE_SEGMENT_LIMIT=6000`, eps/min_size/0.04, and the undecodable bail-out, all set against these five pairs |
| win only on the pairs where nothing changed | **hit** — 13.5× → 4.05× once the two empty-stub pairs are removed |
| contaminated by O1/O2 | **hit** — 1 of 5 AI pairs is O1 (same file, verified), and it supplies the 127× headline |

## Reproduction

```bash
V=<venv-with-tiktoken>/bin/python
C=/home/coder/.vscode-server/extensions/openai.chatgpt-26.818.41705-linux-x64/bin/linux-x86_64/codex
S=experiments/stage_comparison_vector_blocks/ai_output_schema.json
A=experiments/stage_comparison_vector_blocks/artifacts/ai_experiment

# harness baseline with and without the schema Track A used
$C exec --ephemeral --skip-git-repo-check --ignore-rules --sandbox read-only \
   --model gpt-5.6-sol -c 'model_reasoning_effort="low"' -C /tmp --json "Ответь одним словом: ок"
$C exec ... --output-schema $S ... "Ответь одним словом: ок"

# real input tokens of the two archived Track A prompts
$C exec ... --output-schema $S -C /tmp --json -            < $A/vector_prompt.txt
$C exec ... --output-schema $S $(printf -- '--image=%s ' <10 crops>) --json - < $A/vision_prompt.txt

# 'tokens used' semantics: run the SAME call 3x WITHOUT --json and compare with --json usage
$C exec --ephemeral --skip-git-repo-check --ignore-rules --sandbox read-only \
   --model gpt-5.6-sol -c 'model_reasoning_effort="low"' -C /tmp "Ответь одним словом: ок"

# determinism + independent recomputation
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p1_prompt_composition
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p4_wasted_tokens
$V -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p2_minimal_payload
python3 -c "import json,hashlib; ..."   # sha256 of block_pairs.json left/right PDFs -> O1
```
