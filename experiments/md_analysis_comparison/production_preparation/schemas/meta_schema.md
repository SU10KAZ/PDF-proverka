# Meta block — v2

Top-level `meta` block on `01_text_analysis.json` and `03_findings.json`
that carries pipeline / routing observability. New in v2.

Schema-version contract: when `meta.schema_version == 2` is present, the
file follows the v2 conventions described here and in `finding_schema_v2.md`.
Files without `meta` (or with `meta.schema_version` absent or `1`) are
treated as v1.

---

## Fields

| Field | Type | Required | Semantics |
|---|---|---|---|
| `schema_version` | int | YES (in v2) | `2` for v2. v1 either lacks `meta` entirely or sets this to `1`. |
| `document_type` | string | YES | One of: `full_rd`, `audit_comparison`, `tz_vs_rd`, `specification_only`. The value used to drive completeness routing. |
| `document_type_confidence` | number | YES | 0.0–1.0. Detector confidence from `detect_document_type`. |
| `dedup_report` | object | optional | See "Dedup report" subsection. Present whenever class_dedup or fuzzy_dedup ran. |
| `completeness_applied` | bool | YES | Whether the completeness lens (Sonnet) ran for this project. False for `full_rd` (per Phase 1 policy). |
| `completeness_lens_version` | string \| null | optional | The version of the completeness prompt used (e.g. `"v2"`). `null` when `completeness_applied=false`. |
| `lens_duration_sec` | object | optional | Per-leg wall-clock. Keys: `current_method`, `completeness`. Values: floats in seconds. |
| `fallback_used` | bool | YES | `true` if the pipeline had to fall back to A0 (current_method only) because the completeness lens failed. Important — this means completeness_applied may be true while the actual contribution was zero. |

## Dedup report

Sub-object of `meta`. Aggregates whichever dedup steps ran. Shape:

```json
{
  "dedup_report": {
    "total_in": 24,
    "total_out": 18,
    "clusters": 18,
    "same_class_drops": 6,
    "same_class_drops_by_key": {
      "outdated_norm_reference|вру 1||": 2,
      "internal_contradiction|нагрузка|||": 4
    },
    "critical_collapsed_count": 0,
    "sim_threshold": 0.7,
    "methods_seen": ["current_method", "completeness"]
  }
}
```

| Sub-field | Type | Semantics |
|---|---|---|
| `total_in` | int | Findings count before dedup. |
| `total_out` | int | Findings count after dedup (canonicals only). Always ≤ `total_in`. |
| `clusters` | int | Number of clusters; equal to `total_out` for `collapse_to_canonical`. |
| `same_class_drops` | int | Number of findings collapsed into canonicals. |
| `same_class_drops_by_key` | object | Per-class-key drop count. Keys are `class_key` strings (or 60-char signature prefixes for fuzzy_dedup). |
| `critical_collapsed_count` | int | Number of times the КРИТИЧЕСКОЕ-protect rule fired. Production-monitored — if > 0 spikes suddenly, something is upstream of dedup is mis-classifying findings as КРИТ. |
| `sim_threshold` | number | Threshold used (fuzzy_dedup only). |
| `methods_seen` | array<string> | Method/agent names that contributed (merge_across_methods only). |

When both `class_dedup` and `fuzzy_dedup` ran in sequence, only the
**second** report is persisted (the file is overwritten on each pass).
Production callers SHOULD log the first report separately if both are
relevant.

---

## Worked example: 01_text_analysis.json with v2 meta

```json
{
  "stage": "01_text_analysis",
  "project_id": "MULTI/cross_01",
  "text_source": "md",
  "timestamp": "2026-05-20T12:00:00Z",
  "project_params": {
    "object_type": "МКД",
    "total_load_kw": 380
  },
  "normative_refs_found": [
    {"ref": "СП 256.1325800.2016", "status": "actual", "edition": "29.01.2024"}
  ],
  "text_findings": [
    { "id": "T-001", "...": "...full v2 finding..." }
  ],
  "meta": {
    "schema_version": 2,
    "document_type": "audit_comparison",
    "document_type_confidence": 0.85,
    "dedup_report": {
      "total_in": 13,
      "total_out": 11,
      "clusters": 11,
      "same_class_drops": 2,
      "same_class_drops_by_key": {
        "internal_contradiction|нагрузка|||": 2
      },
      "critical_collapsed_count": 0,
      "sim_threshold": 0.7,
      "methods_seen": ["current_method", "completeness"]
    },
    "completeness_applied": true,
    "completeness_lens_version": "v2",
    "lens_duration_sec": {
      "current_method": 209.0,
      "completeness": 147.0
    },
    "fallback_used": false
  }
}
```

## Worked example: 03_findings.json with v2 meta (post-dedup)

Same shape but on the merged finding list:

```json
{
  "stage": "03_findings",
  "project_id": "MULTI/cross_01",
  "findings": [
    { "id": "F-001", "...": "...canonical finding..." }
  ],
  "meta": {
    "schema_version": 2,
    "document_type": "audit_comparison",
    "document_type_confidence": 0.85,
    "dedup_report": { "total_in": 13, "total_out": 11, "clusters": 11, "same_class_drops": 2, "critical_collapsed_count": 0, "sim_threshold": 0.7, "methods_seen": ["current_method", "completeness"], "same_class_drops_by_key": {} },
    "completeness_applied": true,
    "completeness_lens_version": "v2",
    "lens_duration_sec": { "current_method": 209.0, "completeness": 147.0 },
    "fallback_used": false
  }
}
```

---

## Backwards compatibility

- Files without `meta` are v1 — readers MUST tolerate the absence and
  default to `document_type="full_rd"`, `completeness_applied=false`,
  `schema_version=1`.
- Readers MUST NOT reject unknown sub-fields under `meta` — the block is
  extensible and future fields will land here.
- Writers (production pipeline) MUST overwrite `meta` atomically, never
  partially. If a step decides to update `meta.dedup_report`, it should
  read the existing `meta`, merge, and write the whole block back.

## Validation rules (recommended)

| Rule | Why |
|---|---|
| `meta.document_type ∈ ALLOWED` | Prevents typos breaking the prompt substitution. |
| `0.0 ≤ meta.document_type_confidence ≤ 1.0` | Sanity. |
| `meta.dedup_report.total_out ≤ meta.dedup_report.total_in` | Dedup never adds findings. |
| `meta.dedup_report.critical_collapsed_count == 0` (alert if > 0) | КРИТ-protect should rarely fire; if it spikes, investigate. |
| `meta.completeness_applied implies meta.completeness_lens_version != null` | Sanity. |
| `meta.fallback_used implies meta.completeness_applied` | Fallback only meaningful when completeness was attempted. |
