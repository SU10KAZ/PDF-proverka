# Budget Timeline

| Step | Label | Predicted USD | Actual USD | Approved | Remaining before | Remaining after | Note |
|------|-------|---------------|------------|----------|------------------|-----------------|------|
| reference_fill | Missing Pro single-block reference fill (r800) | $0.0000 | $0.0000 | False | $5.0000 | $5.0000 | all requested reference outputs reused |
| phaseA_b2 | Batch screening b2 | $0.4000 | $0.4213 | True | $5.0000 | $4.5788 | calls=8 elapsed=154.7s |
| phaseA_b4 | Batch screening b4 | $0.4000 | $0.3067 | True | $4.5788 | $4.2721 | calls=4 elapsed=119.7s |
| phaseA_b6 | Batch screening b6 | $0.4000 | $0.2293 | True | $4.2721 | $4.0428 | calls=3 elapsed=91.6s |
| phaseB_r1000 | Resolution screening r1000 vs r800 reference | $0.4680 | $0.3019 | True | $4.0428 | $3.7408 | crop_cache=False elapsed=120.9s |
| phaseC_confirmatory | Confirmatory full run batch b6 r800 | $1.2500 | $0.6585 | True | $3.7408 | $3.0823 | calls=7 elapsed=367.1s |

- Cap: $5.00 | Spent: $1.9177 | Remaining: $3.0823
