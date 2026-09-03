# Function Lineage v2.4.2 — IOS2.1 isolated scoped AI smoke

Frozen scoped transport `edcaea0b997330b744f2c479783b9c3ced5e29ae`; model `gpt-5.6-sol/low`.
Three independent cold repeats, Pass A/B, no majority override. `FUNCTION_REMOVED` was excluded by the frozen smoke contract.

| Task | Eligible | Cold 1 A/B | Cold 2 A/B | Cold 3 A/B | Distribution | Stable | Disagree | NME | Parser | Verifier | Capacity |
|---|---:|---|---|---|---|---:|---:|---:|---|---|---|
| LEFT17 | 10 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | lcand_cd6c87ed7f043a937b27 / lcand_cd6c87ed7f043a937b27 | `{"lcand_cd6c87ed7f043a937b27": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT18 | 4 | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | lcand_d9f1abdb7469869363ad / lcand_d9f1abdb7469869363ad | `{"lcand_d9f1abdb7469869363ad": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT19 | 11 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | lcand_26bcd544f168ff9ccea5 / lcand_26bcd544f168ff9ccea5 | `{"lcand_26bcd544f168ff9ccea5": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT20 DOMESTIC | 9 | lcand_1d1f175a30c34b88c6e0 / lcand_1d1f175a30c34b88c6e0 | lcand_1d1f175a30c34b88c6e0 / lcand_1d1f175a30c34b88c6e0 | lcand_1d1f175a30c34b88c6e0 / lcand_1d1f175a30c34b88c6e0 | `{"lcand_1d1f175a30c34b88c6e0": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT20 FIRE | 9 | lcand_ebafe4012323c47ac349 / lcand_ebafe4012323c47ac349 | lcand_ebafe4012323c47ac349 / lcand_ebafe4012323c47ac349 | lcand_ebafe4012323c47ac349 / lcand_ebafe4012323c47ac349 | `{"lcand_ebafe4012323c47ac349": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT20 METERING | 9 | lcand_3e5e047c8b378f731c6b / lcand_3e5e047c8b378f731c6b | lcand_3e5e047c8b378f731c6b / lcand_3e5e047c8b378f731c6b | lcand_3e5e047c8b378f731c6b / lcand_3e5e047c8b378f731c6b | `{"lcand_3e5e047c8b378f731c6b": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |
| LEFT20 PARENT | 3 | lcand_9c617494b14c2b922d3f / lcand_9c617494b14c2b922d3f | lcand_9c617494b14c2b922d3f / lcand_9c617494b14c2b922d3f | lcand_9c617494b14c2b922d3f / lcand_9c617494b14c2b922d3f | `{"lcand_9c617494b14c2b922d3f": 6}` | 3/3 | 0 | 0 | PASS | PASS | PASS |

## Eligible candidate inventories

### LEFT17

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_cd6c87ed7f043a937b27` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[27]` | `['func_287ad40449effc04fdbe']` | `['frag_619741a406872bfa1864']` | 37 refs; `6fe1523afa61ac17d50291627dfbb07a57c03a13f1d4b49c2a88cc4cf9084da8` |
| 2 | `lcand_3aaf4b980ebdaf1cbcc7` | EXACT_SCOPE | SPLIT_1_TO_N | `[26, 27]` | `['func_affc945e95116ff52caf', 'func_287ad40449effc04fdbe']` | `['frag_619741a406872bfa1864', 'frag_a79ca701f464324b4362']` | 54 refs; `8ddddae4c9bf8969a34808ebe2bd76f0379e41d3585c341f0b54bce3ed68e2d2` |
| 3 | `lcand_52a01bb786e915732fce` | EXACT_SCOPE | SPLIT_1_TO_N | `[26, 27]` | `['func_affc945e95116ff52caf', 'func_0b12de35e986a0d1ee09']` | `['frag_2bb086e931f063a6a1f7', 'frag_a79ca701f464324b4362']` | 54 refs; `fb54340977dfacb0fb3a975cc0816d85b3d521a67f4245640d176f95990de754` |
| 4 | `lcand_7982d4b46ef834f35a82` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[27]` | `['func_0b12de35e986a0d1ee09']` | `['frag_2bb086e931f063a6a1f7']` | 37 refs; `c64cd991b2c61cdbc9c3150f36226e11e953874f6dd8e16a75c8556cceba6a1f` |
| 5 | `lcand_bd2d05c8c17ea4a31d57` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[44]` | `['func_98308b436856e8f8469a']` | `['frag_32be4ac28d9b781ff9a4']` | 32 refs; `599d73f4de52a7c2e242d7a9777d57aa147ff4859e4d00caaa449bd7b5efbcad` |
| 6 | `lcand_d90d7c4a0ba3aaba8156` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[40]` | `['func_d19cab63ec23d75478d8']` | `['frag_180d1b0f445d5e6b4cd4']` | 32 refs; `a206900533f38b1d276247497fd7356da5dd484766f15aa842d05112616197d1` |
| 7 | `lcand_4d414db693038a89cffe` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_affc945e95116ff52caf']` | `['frag_a79ca701f464324b4362']` | 35 refs; `d76d09e74acbb3427f7a29d5da0cdf72428400e8c34c76079e0985725c2b7dcd` |
| 8 | `lcand_49eb4c0d65f4ca6eaf73` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_56125ba29972d3363de4']` | `['frag_7b437fe2dc0771230584']` | 33 refs; `8b3726eb58d13883ebe11aa74f6bde9cadf7f3f125f640d41640fa759b48683c` |
| 9 | `lcand_3bd538c47d400ad30128` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[53]` | `['func_cb1e6ee9f5dc7e6c9998']` | `['frag_be106212bacaddd28fc4']` | 31 refs; `8b946d2d25ed6840e35712eca05643f659533d996f13f54171f277c8f1ee8105` |
| 10 | `lcand_4d7b1f9ac9228ce7e2ec` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[63]` | `['func_dfa7166db7b544022b83']` | `['frag_b54fce2ae0f47c54f0e6']` | 31 refs; `b7be39f92f67910be0cfb935387f500a83b6e3535d2b10414c6370d139fd33ce` |

### LEFT18

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_d9f1abdb7469869363ad` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[24]` | `['func_d619e153e6ba0589fddc']` | `['frag_72283b6a91dce0f0b36a']` | 36 refs; `f8d2cd135f3da32a950df564c07387692b2f31ea59e963cde7a9b1bf4bac018f` |
| 2 | `lcand_5cd788591a72c7f72efb` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_60b9dd50b8d81ed8c23c']` | `['frag_9521275c54d339e45cec']` | 33 refs; `b30c474c1eaf8e94dd4baf1054c7b43add5869f877274baaa83ab74cc7f51387` |
| 3 | `lcand_93a1d841cbdc4d34b135` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[53]` | `['func_42f6d99005154db20ea5']` | `['frag_859ee0f7729b45ea6bd2']` | 31 refs; `3f13adffe246509e69af8164fa42bfa9745acbaa0741a5dd95f67e774f3dcd9b` |
| 4 | `lcand_ebe22cea05c4fd45c18e` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_f1d8d521aa0b649e0b09']` | `['frag_85df66f19ac87cb93212']` | 33 refs; `7a30f3fd6940ebc21e13d21fbc5165e66a9d8abd8a3349d8e9c4ceac77791e88` |

### LEFT19

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_26bcd544f168ff9ccea5` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[30]` | `['func_238a1403e6f63796566d']` | `['frag_cf1509d7dde095ee3958']` | 35 refs; `062db955144326de611d2ad734a9152fa72082a6a2620653eff03a483e957691` |
| 2 | `lcand_c725393a11cb3b17ed2d` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[25]` | `['func_4cc9b29e75e6cf3966b8']` | `['frag_8f2cfda62adadad1a01d']` | 36 refs; `6cb9b25bfce4ac3798082524e45efc0699a68083e8ba8cbc734c3df4dde30cf8` |
| 3 | `lcand_483ebda563a5ac1ef19e` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[30]` | `['func_74bfdef46f5f7beff825']` | `['frag_e5bc3b0e86aaa8a4bb34']` | 35 refs; `409113d040cc49d5b027110fdae0df82734d72998589ed1330f2573b1439942b` |
| 4 | `lcand_f6c7cb5c5eea35c57b89` | EXACT_SCOPE | SPLIT_1_TO_N | `[25, 26]` | `['func_4cc9b29e75e6cf3966b8', 'func_55fa700f4a2ebcb6f79b']` | `['frag_6724c6c375f99e8ed353', 'frag_8f2cfda62adadad1a01d']` | 53 refs; `188d9246c077e593066614d55da2299810782520559cbed0d6e27b33d3da3a5b` |
| 5 | `lcand_d8a337a39d4a0a87da7b` | EXACT_SCOPE | SPLIT_1_TO_N | `[29, 30]` | `['func_56125ba29972d3363de4', 'func_238a1403e6f63796566d']` | `['frag_7b437fe2dc0771230584', 'frag_cf1509d7dde095ee3958']` | 50 refs; `725f5521dd788f7140b182d927ef262eb17e5b13dcedd4fa4203cddabea716ab` |
| 6 | `lcand_ff6f5c138b7b89d1fe2d` | EXACT_SCOPE | SPLIT_1_TO_N | `[25, 26]` | `['func_505c567c2a80e806818c', 'func_55fa700f4a2ebcb6f79b']` | `['frag_6724c6c375f99e8ed353', 'frag_e974a135e2f2cb21e682']` | 53 refs; `2f0d33679bb795847b0fcbcbf609ea473f9bc4037732dd037806963727b94ea7` |
| 7 | `lcand_03f655edda73c8442470` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[25]` | `['func_505c567c2a80e806818c']` | `['frag_e974a135e2f2cb21e682']` | 36 refs; `c6e82e0becafea5bc7647b43987977884fdd6e06201b16c8d0a24af44ff2c7e4` |
| 8 | `lcand_75917131c82d7381de0e` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_55fa700f4a2ebcb6f79b']` | `['frag_6724c6c375f99e8ed353']` | 35 refs; `cf6f4b7b72b4b22862e0a6625e621e2c3199f84425c5352504658b0491034081` |
| 9 | `lcand_91fdd78a29c5bab93b28` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_affc945e95116ff52caf']` | `['frag_a79ca701f464324b4362']` | 35 refs; `97181edd19bd7cc3b13ab8b33477be5a8f4da2732dd370b2bb5eccc79e4e0e6d` |
| 10 | `lcand_b0c7c89b3a9870b36f9c` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_56125ba29972d3363de4']` | `['frag_7b437fe2dc0771230584']` | 33 refs; `ebeff3561a928de90b693a442c510cb43b31df2f519d00cbb662a056dea82e36` |
| 11 | `lcand_95e6c3ca4ad0c8567d10` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[63]` | `['func_dfa7166db7b544022b83']` | `['frag_b54fce2ae0f47c54f0e6']` | 31 refs; `2cdabcb3876c61aca80736da15582bab5b7aecf0989e4c212412dd1168149b96` |

### LEFT20 DOMESTIC

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_8dd3901e468fc831af4e` | EXACT_SCOPE | SPLIT_1_TO_N | `[24, 25, 26]` | `['func_2f17121477205f7cca17', 'func_505c567c2a80e806818c', 'func_d7f66f9e67cecffa855f']` | `['frag_35a415251b304e088a23', 'frag_c1e2de111d4d31073cdc', 'frag_e974a135e2f2cb21e682']` | 67 refs; `477da67ce4e7c16cfce03a916aeac3cbcaafb316df2b0ff2d82e2d9254b46587` |
| 2 | `lcand_1d1f175a30c34b88c6e0` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_d7f66f9e67cecffa855f']` | `['frag_c1e2de111d4d31073cdc']` | 31 refs; `49231ce71a2d752ad26edf960af7a113c360cc3cedcb9f39b7701c38a4ee3c63` |
| 3 | `lcand_e14357276511421901de` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[47]` | `['func_6cfd84f203e3c24d8198']` | `['frag_32c9267326e8332879fe']` | 20 refs; `ad11949f55a6e3512469cfd29070c4d3d586c1fd4f157ac05084f5b5e10459fe` |
| 4 | `lcand_4f3e97990344eed51c55` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_affc945e95116ff52caf']` | `['frag_a79ca701f464324b4362']` | 31 refs; `2cc69461a7a2008dbf6883008039b979500a90e51ec4deb0ff24827947530d0e` |
| 5 | `lcand_4957e9865bfd379479fa` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_57cdb20d4b5249d51d05']` | `['frag_2dfce063f33caeeee357']` | 31 refs; `0e0d9e8e3f37f4110ce1e91a05b2400f940742d78415ea0e80c261e246ab819c` |
| 6 | `lcand_38157d382693b6e44757` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[24]` | `['func_2f17121477205f7cca17']` | `['frag_35a415251b304e088a23']` | 32 refs; `60bbc1ce9e404b682fe276c73638b21bebc9c237f91a216b2022f869094ecaa6` |
| 7 | `lcand_6c2e7132dc6952e328cd` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[27]` | `['func_0b12de35e986a0d1ee09']` | `['frag_2bb086e931f063a6a1f7']` | 33 refs; `2c8cf18a0925f8b5c35d9199ee0b2c426687e581c9b1c1e328ff4abdf1acbbe5` |
| 8 | `lcand_bb2e91baa704ac74259a` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_56125ba29972d3363de4']` | `['frag_7b437fe2dc0771230584']` | 29 refs; `57ea125bddc56a40ad212f072cc97e7a85640b3060ca05c8baad718426760d71` |
| 9 | `lcand_2e6d094a49daa2a10a00` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[25]` | `['func_505c567c2a80e806818c']` | `['frag_e974a135e2f2cb21e682']` | 32 refs; `53aee9573c37a64e92ed30c39af9779029b120d00cfbd7660f25e4e7e21406cd` |

### LEFT20 FIRE

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_701d664a2c9395559b56` | EXACT_SCOPE | SPLIT_1_TO_N | `[26, 27, 28]` | `['func_affc945e95116ff52caf', 'func_0b12de35e986a0d1ee09', 'func_011ba53858207da5c1a5']` | `['frag_2bb086e931f063a6a1f7', 'frag_7a19d07a14974eefda68', 'frag_a79ca701f464324b4362']` | 69 refs; `639a701ea89c24e15442f7576f22697cdbd34bf9a21af6666ff383de401feedb` |
| 2 | `lcand_ebafe4012323c47ac349` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[28]` | `['func_011ba53858207da5c1a5']` | `['frag_7a19d07a14974eefda68']` | 33 refs; `688fd934aa77b26d01bd50c2b774fe0685a467dd4b1bf5d6788b702ff37cccc8` |
| 3 | `lcand_7b22e485cd53f929073f` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[40]` | `['func_aa17614f57f2337a05c9']` | `['frag_8782511ad43bd4d4b3dc']` | 28 refs; `258f38b6527aabd3226e6edf3385a33bf22ec38a5206c89b188a1e0a33626c10` |
| 4 | `lcand_97079aeefd65714f57d7` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[44]` | `['func_eef21e09b4659da4e77d']` | `['frag_c03888621de8b911dc93']` | 28 refs; `f201097c75813ff16f14733b100cd09afdf5856524dbefa3220380f72ad4ff8c` |
| 5 | `lcand_dfbd6e5f0341f1b4c89b` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_affc945e95116ff52caf']` | `['frag_a79ca701f464324b4362']` | 31 refs; `1b0631cdf42527f3a649bb64804c149bca6ded469fa4cc667caf9ec512065cfa` |
| 6 | `lcand_e4155c5ecfa49d066436` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_57cdb20d4b5249d51d05']` | `['frag_2dfce063f33caeeee357']` | 31 refs; `d8e52de46871302481668c123446a6a1458d8e6699424973673968bb7ae37795` |
| 7 | `lcand_a2b28caea897ce9c5fc7` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_56125ba29972d3363de4']` | `['frag_7b437fe2dc0771230584']` | 29 refs; `238baeec85cfd4c85dbb92f9c0aaa3395cbb6f8f0e0acd8bb5626d4ca9f509e9` |
| 8 | `lcand_b37dcf069b969ae7dc87` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[24]` | `['func_2f17121477205f7cca17']` | `['frag_35a415251b304e088a23']` | 32 refs; `4adc5069ae07fea97c4550080b9c9f5b453dcf0f195f1d28fe25cd045e20bda0` |
| 9 | `lcand_c6118cc5e076b7723e5b` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[27]` | `['func_0b12de35e986a0d1ee09']` | `['frag_2bb086e931f063a6a1f7']` | 33 refs; `2f9ca1082a42f4147249991948b8f57b1f5f82664f0063ede2c2fc823b1debfd` |

### LEFT20 METERING

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_51ca1f895cef97227887` | EXACT_SCOPE | SPLIT_1_TO_N | `[29, 30]` | `['func_f1d8d521aa0b649e0b09', 'func_aa79b91574f40b68dfc4']` | `['frag_85df66f19ac87cb93212', 'frag_db946f7dd6839703e922']` | 46 refs; `a035f32c62eb3407074c64da11ddfccebff67f2ef7c181737c4cff6c5458197b` |
| 2 | `lcand_3e5e047c8b378f731c6b` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_f1d8d521aa0b649e0b09']` | `['frag_85df66f19ac87cb93212']` | 29 refs; `da408294b85651ac09254b52c38d1b49cea8115625fffe5d424761bd5ab0fabc` |
| 3 | `lcand_b66e65c78fbef35b6183` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[25]` | `['func_2435c15ccc7c50a8b8f3']` | `['frag_bb87072730057206ffe9']` | 32 refs; `c0a2cbd259bf84df9519ad76aaf2f01da005fd0cf0577db6e26d445372494d80` |
| 4 | `lcand_b6e7a128af9a882de0e5` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[30]` | `['func_aa79b91574f40b68dfc4']` | `['frag_db946f7dd6839703e922']` | 31 refs; `06b10b367f4621d85b6aa1269a0af50a0cbd2c2fd7b377bbf70e93666d8eb6aa` |
| 5 | `lcand_de8e9d6ee522ef5afac6` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[26]` | `['func_affc945e95116ff52caf']` | `['frag_a79ca701f464324b4362']` | 31 refs; `023f537a67c18ebda19c7941ab7a6ef79ee65f1d38295b4502158f7191e1ed4f` |
| 6 | `lcand_e3d57a3116233c0b0c7d` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_60b9dd50b8d81ed8c23c']` | `['frag_9521275c54d339e45cec']` | 29 refs; `826ad9808d93378ca4f75ed96999917ad2db5812bb403c87b80c99786fcbceca` |
| 7 | `lcand_8d946bf4d67789f0b5d6` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[29]` | `['func_56125ba29972d3363de4']` | `['frag_7b437fe2dc0771230584']` | 29 refs; `4fe23ccc6dcd04501c1942e7d65031b7a84bb550e11cb7f9957e2ead52a2302f` |
| 8 | `lcand_7711f45ce4ec767469f6` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[25]` | `['func_df9e63596aef951247e8']` | `['frag_2290ae31ef653c531da5']` | 32 refs; `868bdab724649407ddfdef54242f446ccd2a302ae2fb269d2bdf1a91efe16b6b` |
| 9 | `lcand_f807f1b2b5b3494725f2` | EXACT_SCOPE | CONTINUED_1_TO_1 | `[24]` | `['func_2f17121477205f7cca17']` | `['frag_35a415251b304e088a23']` | 32 refs; `e628273882e7c6979ff8f084d2ebd4e057013ae65c0608a013587e8aec8541e3` |

### LEFT20 PARENT

| Rank | Candidate | Scope relation | Relation | RIGHT pages | RIGHT functions | RIGHT fragments | Evidence |
|---:|---|---|---|---|---|---|---|
| 1 | `lcand_9c617494b14c2b922d3f` | EXACT_SCOPE | FUNCTION_DISTRIBUTED | `[26, 28, 29]` | `['func_d7f66f9e67cecffa855f', 'func_011ba53858207da5c1a5', 'func_f1d8d521aa0b649e0b09']` | `['frag_7a19d07a14974eefda68', 'frag_85df66f19ac87cb93212', 'frag_c1e2de111d4d31073cdc']` | 79 refs; `bf9f6a7ee574795da1c30bf54dcfa1a5a20a920183e575e12ffb31ccccd2d0f3` |
| 2 | `lcand_b3eef05dda4a6df387c0` | EXACT_SCOPE | FUNCTION_DISTRIBUTED | `[26, 29, 40]` | `['func_d7f66f9e67cecffa855f', 'func_f1d8d521aa0b649e0b09', 'func_aa17614f57f2337a05c9']` | `['frag_85df66f19ac87cb93212', 'frag_8782511ad43bd4d4b3dc', 'frag_c1e2de111d4d31073cdc']` | 74 refs; `025bddf6603db8675422ebf453eebb47b89ece4a23ea48dbe7fd64f24f424f5e` |
| 3 | `lcand_4457a40a1fdceace6f4b` | EXACT_SCOPE | FUNCTION_DISTRIBUTED | `[26, 29, 44]` | `['func_d7f66f9e67cecffa855f', 'func_f1d8d521aa0b649e0b09', 'func_eef21e09b4659da4e77d']` | `['frag_85df66f19ac87cb93212', 'frag_c03888621de8b911dc93', 'frag_c1e2de111d4d31073cdc']` | 74 refs; `35eb1e747346b03704635a97d6dfc42081d4d701be97f48ff2bdd1bc645e2db6` |

## Controls and safety

LEFT19 independent distribution: `{"lcand_26bcd544f168ff9ccea5": 6}`. A repeated R30 preference, if present, is only a stable model preference and does not prove R25 deterministically wrong.

LEFT20 child union == parent distributed candidate: **YES** — stable child union equals frozen distributed parent candidate. Model bypass: `NO`.

Cross-granularity selectable competition `0`; unsupported accepted `0`; RIGHT_MAP_CONFLICT `0`; FUNCTION_FRAGMENT_CONFLICT `0`.

Capacity was checked in separate atomic-child and composite-parent scenarios; nested parent and child results were never treated as simultaneous materialization.

## Cost

Request attempts `24`; successful inference requests `24`; input/output/total tokens `0/0/0`.

Model runtime `202536 ms`; wall time `63177 ms`; telemetry defect `True` — Successful inference returned usage={} / zero tokens; token telemetry is defective.

Production runs `0`; deploy `NO`; shadow `OFF`; materialization `NO`; Vision `NO`.

## Verdict

**A — inference completed; seven scoped critical tasks are stable and safe.**

Even with verdict A: **NO DEPLOY. NO SHADOW. NO MATERIALIZATION.**
