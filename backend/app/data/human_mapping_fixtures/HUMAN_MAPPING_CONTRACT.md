# Human Mapping Contract

This UI is research-only. It does not modify Semantic Mapping V3, ProjectChanges, evidence, prompts, mining, dedupe, source files, production, validation, or final holdout.

- `HUMAN_CONFIRMED`: in a future pipeline, exactly these OLD and NEW blocks are a hard-linked direct-comparison area.
- `HUMAN_REJECTED`: only this exact OLD↔NEW set is prohibited; every member block remains available for other semantic matches.
- `HUMAN_UNCERTAIN` and `UNREVIEWED`: no restriction; ordinary AI semantic mapping applies.

Every review is append-only in `human_mapping_reviews/reviews.jsonl`. A later decision carries `supersedes_review_id`; it never overwrites prior history. The current UI does not consume reviews into any pipeline.
