The large re-extracted descriptions (`recut/<pair>/<side>/vector_block.json` for
ar_wall_sections / vk_node_plan / vk_nodes, and `derot/vk_nodes`) were deleted after
measurement because they are 48–217 MB each. Every number derived from them is stored in
`../ptn_recut_diff.json`. Regenerate with:

    python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut ar_wall_sections 60000
    python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut vk_node_plan 60000
    python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut vk_nodes 200000
    PTN_GRID=16 PTN_JACCARD=0.5 python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut_diff ar_wall_sections vk_node_plan vk_nodes

`derot/eom_singleline_changed/` (968 KB) is kept — it is the corrected extraction behind P15–P17.
