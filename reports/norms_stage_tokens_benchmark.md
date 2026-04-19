# norm_verify token usage benchmark — new (MCP) vs old (WebSearch)

Generated: 2026-04-18T08:21:59.203831+00:00
Source: `~/.claude/projects/-home-coder-projects-PDF-proverka/*.jsonl`

## Data availability

- Projects scanned: **3**
- Projects with NEW token data: **3**
- Projects with OLD token data: **1**
- Projects with RELIABLE comparison: **1**

> Claude CLI session JSONL files in ~/.claude/projects/ are pruned after a retention period; earliest remaining file is dated 2026-04-15. webapp/data/usage_data.json keeps records ~30 days — earliest is 2026-03-25. Historical runs older than these dates cannot be matched to token usage.

## Per-project summary

| project_id | old_total | new_total | Δtotal | Δ% | old_wall | new_wall | reliable |
|-|-|-|-|-|-|-|-|
| AI/133-23-ГК-АИ2 | — | 165,617 | — | — | 802.99s | 924.48s | — |
| AR/13АВ-РД-АР1.1-К4 (Изм.2) | 86,573 | 36,350 | -50,223 | -58.01% | 1549.40s | 232.06s | ✓ |
| OV/133_23-ГК-ОВ1.2 | — | 120,671 | — | — | 716.05s | 544.55s | — |

## New run — detailed tool breakdown

### AI/133-23-ГК-АИ2

- sessions: **4**
- models: claude-opus-4-6
- input_tokens: **267**
- output_tokens: **165,350**
- total_tokens: **165,617**
- cache_creation_input_tokens: 1,139,557
- cache_read_input_tokens: 6,464,422
- assistant_turns: 155
- tool_counts:
  - `Bash`: 4
  - `Grep`: 5
  - `Read`: 18
  - `ToolSearch`: 4
  - `Write`: 4
  - `mcp__norms__get_paragraph_json`: 48
  - `mcp__norms__semantic_search_json`: 28

**OLD**:
_Token usage for the old run is NOT recoverable:_
- OLD session JSONL not found for 2026-03-16T13:10:18. Claude CLI sessions dir retains only 2026-04-15+; webapp/data/usage_data.json — only 2026-03-25+. Token usage for this historical run is unrecoverable.

### AR/13АВ-РД-АР1.1-К4 (Изм.2)

- sessions: **1**
- models: claude-opus-4-6
- input_tokens: **44**
- output_tokens: **36,306**
- total_tokens: **36,350**
- cache_creation_input_tokens: 265,275
- cache_read_input_tokens: 1,485,005
- assistant_turns: 36
- tool_counts:
  - `Grep`: 1
  - `Read`: 6
  - `ToolSearch`: 1
  - `Write`: 1
  - `mcp__norms__get_paragraph_json`: 10
  - `mcp__norms__semantic_search_json`: 7

**OLD**:
- sessions: **3**
- models: claude-opus-4-6
- input_tokens: **76**
- output_tokens: **86,497**
- total_tokens: **86,573**
- cache_creation_input_tokens: 469,757
- cache_read_input_tokens: 2,514,879
- assistant_turns: 64
- tool_counts:
  - `Bash`: 8
  - `Glob`: 1
  - `Grep`: 3
  - `Read`: 21
  - `Write`: 6

### OV/133_23-ГК-ОВ1.2

- sessions: **2**
- models: claude-opus-4-6
- input_tokens: **131**
- output_tokens: **120,540**
- total_tokens: **120,671**
- cache_creation_input_tokens: 654,618
- cache_read_input_tokens: 5,955,202
- assistant_turns: 93
- tool_counts:
  - `Grep`: 1
  - `Read`: 23
  - `ToolSearch`: 2
  - `Write`: 2
  - `mcp__norms__get_paragraph_json`: 24
  - `mcp__norms__semantic_search_json`: 15

**OLD**:
_Token usage for the old run is NOT recoverable:_
- OLD session JSONL not found for 2026-03-21T23:17:08. Claude CLI sessions dir retains only 2026-04-15+; webapp/data/usage_data.json — only 2026-03-25+. Token usage for this historical run is unrecoverable.

## Aggregate (reliable projects only)

- Comparable projects count: **1**
- Uncomparable (old tokens missing): **2**
- old_mean_total_tokens: **86,573**
- new_mean_total_tokens: **36,350**
- mean_delta_tokens: **-50,223** (-58.01%)
- old_median_total_tokens: **86,573**
- new_median_total_tokens: **36,350**
- median_delta_tokens: **-50,223**

## Token direction (new vs old)

- NEW tokens **higher** than OLD: 0 project(s)
- NEW tokens **lower** than OLD: 1 project(s)
  - AR/13АВ-РД-АР1.1-К4 (Изм.2): -50,223 (-58.01%)
- OLD tokens **NOT recovered**: 2 project(s) (old wall-clock есть, но Claude JSONL/usage_data уже обрезаны по ретенции)
  - AI/133-23-ГК-АИ2: NEW=165,617, OLD=—
  - OV/133_23-ГК-ОВ1.2: NEW=120,671, OLD=—

## Wall-clock vs tokens

- **AR/13АВ-РД-АР1.1-К4 (Изм.2)**: wall ↓ -85.02% (-1317.34s), tokens ↓ -58.01% (-50,223) → нет trade-off: обе метрики улучшились одновременно

## Wall-clock only (для проектов без старых token-данных)

| project_id | old_wall | new_wall | Δwall | old_tokens | new_tokens |
|-|-|-|-|-|-|
| AI/133-23-ГК-АИ2 | 802.99s | 924.48s | +15.13% | NOT RECOVERED | 165,617 |
| AR/13АВ-РД-АР1.1-К4 (Изм.2) | 1549.40s | 232.06s | -85.02% | 86,573 | 36,350 |
| OV/133_23-ГК-ОВ1.2 | 716.05s | 544.55s | -23.95% | NOT RECOVERED | 120,671 |

