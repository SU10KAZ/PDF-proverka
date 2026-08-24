"""Named, versioned policy for production graphic Mode 1.

The values are not universal design constants.  They are the stable G1 copy
of the thresholds calibrated by research commit ``b37e9f20`` on the 56-pair
benchmark.  A ledger records the policy id, so a later policy can change
without migrating old artifacts.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class GraphicMode1Policy:
    policy_id: str = "graphic_mode1_policy_v1"
    version: str = "EXPERIMENTALLY_CALIBRATED_V1"
    provenance: str = (
        "research commit b37e9f20; benchmark_pairs.json (56 real revision "
        "pairs); thresholds calibrated and evaluated on the same corpus"
    )
    extractor_version: str = "visual_vector_extractor_v1"
    registration_version: str = "physical_similarity_registration_v1"
    diff_version: str = "local_visible_ink_diff_v1"

    cell_pt: float = 0.6
    quality_cell_pt: float = 0.8
    tolerance_pt: float = 1.2
    merge_pt: float = 2.4
    border_pt: float = 3.0
    border_probe_pt: float = 24.0
    min_region_ink_pt: float = 8.0
    text_overlap_drop: float = 0.65

    min_ink_pt: float = 200.0
    min_symmetric_coverage: float = 0.80
    max_changed_ink_fraction: float = 0.25
    max_published_regions: int = 40

    # Extraction honesty gates.  Recall was >= .978 on the calibration
    # corpus.  Precision is measured with the conservative 245 gray threshold,
    # where the corpus minimum was .9221.
    min_extraction_precision: float = 0.90
    min_extraction_recall: float = 0.95
    render_dark_threshold: int = 245
    raster_backed_area_fraction: float = 0.50
    text_as_curves_min_segments: int = 2000
    text_as_curves_peer_spans: int = 20

    max_rotation_deg: float = 3.0
    rotation_min_coverage_gain: float = 0.02
    max_scale_delta_from_hypothesis: float = 0.15
    min_registration_primitives: int = 5
    registration_success_floor: float = 0.50

    max_diagnostic_filtered_regions: int = 50

    def public_dict(self) -> dict[str, Any]:
        values = asdict(self)
        return {
            "id": values.pop("policy_id"),
            "version": values.pop("version"),
            "provenance": values.pop("provenance"),
            "parameters": values,
        }


EXPERIMENTALLY_CALIBRATED_V1 = GraphicMode1Policy()


__all__ = ["GraphicMode1Policy", "EXPERIMENTALLY_CALIBRATED_V1"]
