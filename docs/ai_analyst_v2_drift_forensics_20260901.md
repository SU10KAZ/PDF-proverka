# AI Analyst v2: drift forensics и reproducibility gate

Дата расследования: 2026-09-01. Acceptance-пара: `p11c797af90` из session `7cccec69bb0b4327`. Исходный HEAD до любых изменений: `c8b4124660f78cf9c6793549c37b56b4587625d2`.

## Итог

Вердикт reproducibility gate: **B — контур безопасен, но AI outcome слишком нестабилен для production**. Во всех трёх новых cache-off runs получено `unsupported=0`, по три успешные model sessions без timeout/failure и шесть действий Human Review Orchestrator. Однако при побайтово одинаковых входах и prompts продуктовый результат менялся от Stage 7 `77→78` до `77→75`, а pairwise overlap materialized product падал до `0.25`.

Историческое расхождение полностью объяснено двумя отдельными факторами:

1. «Успешный acceptance» был не cold run, а warm replay трёх сохранённых ответов для старого `whole-document.v1` prompt. Cache integrity не нарушена, но этот replay ошибочно использовался как acceptance для сравнения с новым `compact-context.v3` prompt.
2. Между этими prompts содержательный model input изменился. На шести задачах новый ответ потерял либо сознательно не выбрал требуемую ссылку `FAST:CHANGE:<task_id>`; неизменившийся verifier правильно не пропустил формально неподтверждённый результат. Три новых независимых runs дополнительно доказали существенную недетерминированность модели даже на абсолютно одинаковом новом payload.

Verifier safety, FAST, нормативный контур, Human Review UX и Engineer Approval flow не ослаблялись и не менялись.

## 1. HEAD before и commits

- HEAD before: `c8b4124660f78cf9c6793549c37b56b4587625d2`.
- `45b7c17e` — `Add AI analyst reproducibility gate`: только provenance/audit fields, exact payload capture, frozen FAST manifest и формальный three-run gate с tests.
- Настоящий документ фиксируется отдельным documentation commit.
- Push, deploy и release не выполнялись.

## 2. Два замороженных исторических run

| Run | Путь | Доказательство | Результат |
|---|---|---|---|
| A: последний успешный acceptance | `comparison/ai_analyst_v2/20260901_grsh_human_review_orchestrator` | `source_run.json` SHA-256 `72b560f045a781202fda0a37e970db7509ccc0199189778f0a36565becb42cb9`; materialization `2682af0fe22b9ab27e5a63c846c0aa8f434beba13de61bfcf770d67c6362845e`; report `ce2bffe3273321db5b93497aa73d3e2ceab806d9ad5c5806ecc9e97b5f2ed449` | 17 AI verified; 3 verifier rejects; 5 findings; Stage 7 `77→74`; report AI=4; pre-HRO=38; HRO=6; unsupported=0 |
| B: production-readiness cold | `comparison/ai_analyst_v2/20260901_grsh_production_readiness_cold` | `low/cold_run.json` SHA-256 `1e1cbe8e26d732f9bca530ddce23af87888cda07648aedbe07a630119cea8f5c`; materialization `1b05530afd499aa5fd372a0a1fe0b75895d8a4ad96c840c9acccc12b2b9d0699`; report `f3491a21927f4f7b8168a161061095cb134eb19e466f15e239a9761197d6d12f` | 12 AI verified; 7 verifier rejects; 2 findings; Stage 7 `77→77`; report AI=1; pre-HRO=44; HRO=6; unsupported=0; 507689 ms |

Файлы production source в `comparison/sessions/7cccec69bb0b4327/pairs/p11c797af90/production` имеют timestamps раньше обоих AI runs. Они не перегенерировались во время расследования. Их прежний `fast_baseline.json` имеет одинаковый SHA-256 `06059513eddc8844ac60c6ef5a53a8fe38ab5734477d8efde646bbbc0835bb46` во всех сравнениях.

## 3. FAST artifact diff

Для сравнения применена одинаковая canonical projection к замороженному production source. Полная frozen FAST input signature: `890198adde16fe622b8ccd6c1b0b395faab0a1cf3baed38032045b741e4e83b7`.

| Artifact | Old digest | New digest | Records | Same |
|---|---|---|---:|---|
| selected pages | `f4823f63f78dff041f28e300ded152a862e23b0e860fd6404079c4f96f8fd954` | `f4823f63f78dff041f28e300ded152a862e23b0e860fd6404079c4f96f8fd954` | 2 | YES |
| prepared blocks | `50690d6cd59aaf0b1a529789bb5895838e69bccbdd643b7051ced0d007a8341e` | `50690d6cd59aaf0b1a529789bb5895838e69bccbdd643b7051ced0d007a8341e` | 2 | YES |
| native text | `83f7d87c286d16d2579501a5fc61c02bbb8b6259773098daea258d7b5d35550d` | `83f7d87c286d16d2579501a5fc61c02bbb8b6259773098daea258d7b5d35550d` | 3 | YES |
| graph LEFT | `74020c1f79030b45ef373e00dc6c4edc6f69b99d42e1eb65ce703c3ac09fca08` | `74020c1f79030b45ef373e00dc6c4edc6f69b99d42e1eb65ce703c3ac09fca08` | 13 | YES |
| graph RIGHT | `b009d12c01f268af634a435b92e3577b2f01f811cbee2594aedbd8b40b1de555` | `b009d12c01f268af634a435b92e3577b2f01f811cbee2594aedbd8b40b1de555` | 13 | YES |
| electrical tables | `2d869be198e30b20630bfc34907b23830be395cbcd8ba2e586c08b415045a714` | `2d869be198e30b20630bfc34907b23830be395cbcd8ba2e586c08b415045a714` | 2 | YES |
| unified synthesis | `5a021d8043d2493df8c4c0d3b0d29bf0b4b65b1c77b62c61bbd279c495102203` | `5a021d8043d2493df8c4c0d3b0d29bf0b4b65b1c77b62c61bbd279c495102203` | 54 | YES |
| Stage-7 targets | `121c108e7076bdb7dcc9a74eff9846e3c20ae882103d9f4591723014034c542b` | `121c108e7076bdb7dcc9a74eff9846e3c20ae882103d9f4591723014034c542b` | 77 | YES |
| document inconsistencies | `a4bef10bc788973955a25344aca9590d5a3bd224e9d5ef52b24da90968999730` | `a4bef10bc788973955a25344aca9590d5a3bd224e9d5ef52b24da90968999730` | 13 | YES |
| recognition coverage | `00efc8a260e07fdcc8ae4eecdde965c280da9ad05afe1bd8417480c5daf152b9` | `00efc8a260e07fdcc8ae4eecdde965c280da9ad05afe1bd8417480c5daf152b9` | 3 | YES |
| unresolved inventory source | `aeb00ec2f535ac7ae0306b7fb004a754773845f92ed69ead75530584d00c20c1` | `aeb00ec2f535ac7ae0306b7fb004a754773845f92ed69ead75530584d00c20c1` | 85 | YES |

Вывод: `INPUT_DRIFT=0`. Фактический FAST input не является причиной расхождения.

## 4. Происхождение pre-HRO `38→44`

Current-HEAD counterfactual replay доказал причинность: старые model responses дают ровно прежние 5 findings, `77→74`, pre-HRO=38 и report AI=4; cold responses на тех же FAST/context дают 2 findings, `77→77`, pre-HRO=44 и report AI=1. Дополнительные шесть review items — это те же task IDs, которые перестали сниматься AI-решением:

| Old/New ID | Source | Почему появился в новых 44 | FAST change | Materialization change | Projection change | Run-generation difference |
|---|---|---|---|---|---|---|
| `uchg_1bf23921e9dbc1df9612` | `CHANGE_INCOMPLETE_EVIDENCE`, ВРУ1, кабели 1→3 | Old `FORMATTING_ONLY/PASS/NO_CHANGE`; new ответ не сослался на `FAST:CHANGE:<id>`, verifier reject | NO | consequence only | NO | YES, model response |
| `uchg_79d5e6aa07c1e8df4ffe` | `CHANGE_INCOMPLETE_EVIDENCE`, ВРУ3, кабели 1→2 | Old `FORMATTING_ONLY/PASS/NO_CHANGE`; new ответ потерял исходную FAST-ссылку | NO | consequence only | NO | YES, model response |
| `uchg_86dd32aa72abda4b7af5` | `CHANGE_INCOMPLETE_EVIDENCE`, ВРУ4, кабели 1→3 | Old verified finding; new ответ потерял исходную FAST-ссылку | NO | finding no longer materializes | NO | YES, model response |
| `uchg_882a967353baf40a13d1` | `CHANGE_INCOMPLETE_EVIDENCE`, ВРУ3, кабели 1→2 | Old formatting-only; new evidence показывает неоднозначные left rows с количеством 2 и 3, model вернула need-more; тип ответа не совпал с task contract | NO | consequence only | NO | YES, safer model response |
| `uchg_d1d843ad9e789eeabb13` | `CHANGE_INCOMPLETE_EVIDENCE`, ВРУ4, кабели 1→3 | Old verified finding; new ответ потерял исходную FAST-ссылку | NO | finding no longer materializes | NO | YES, model response |
| `uchg_dc6ba52a9e78520fd415` | `CHANGE_INCOMPLETE_EVIDENCE`, ХМ1, кабели 1→3 | Old verified finding; new ответ потерял исходную FAST-ссылку | NO | finding no longer materializes | NO | YES, model response |

У всех шести old ID равен new ID, source одинаков, и исходные review targets уже присутствуют в frozen FAST. Это не генерация новых FAST items и не изменение HRO projection.

## 5. Analysis context diff

Producer version обозначен code revision, создавшей artifact: old `ai_v2.context@eb90d6ca`, new `ai_v2.context@c8b41246`.

| Context | Old schema / digest / records / bytes | New schema / digest / records / bytes | Task IDs |
|---|---|---|---|
| Sheet Context | `stage-comparison-ai-v2-sheet-context.v1`; `4c03b81c3f1109578aed976aeab684b1acf66cc9c323f4fc4a445c0151ee2c31`; 581; 422478 | та же schema, digest, 581, 422478 | source catalog, route list не хранит; содержимое byte-identical |
| Focused Evidence | legacy map, effective v1 (в artifact нет embedded `schema_version`); `6c51515ead6951be154dc40f841c46e2d514d321775e7499d1eff8cd1198de9a`; 80; 71605 | та же effective schema; `f8223ce8251529288be5ac632f87a58e0cfdac2d4442b5cad0c1a25d3a53c975`; 80; 72399 | одинаковые 80 IDs; new добавил related full-table-row refs для 10 cable/graph tasks |
| Evidence Catalog | legacy map, effective v1 (нет embedded `schema_version`); `d4f1010686d5922d9b24e59e993e3b227638e847b0cb773af401c841534e3a0e`; 604; 493326 | тот же digest, 604, 493326 | evidence refs, не routed task list; byte-identical |
| Unresolved Inventory | `stage-comparison-ai-v2-inventory.v1`; `5a9a2f856d9e1a72624d5fb76542a5b9cb45985203e062cc97b63fdf90045a44`; 92; 101426 | та же schema; `cc62ce990c096382a54322b1f0aafc3a1697ff798c97d349f396f8c21a8f404c`; 92; 101477 | одинаковые 92 item IDs и одинаковый task input signature `f23b9f14cd43fc56e6964b1d5d62069cefcd36085dc446caeace08c529944203` |

Inventory diff ограничен `generated_at` и новым explicit root constraint `human_review_classified_before_routing:false`. Counts, items, task IDs и routing decisions совпадают. Общая context signature изменилась с `ae40062911fe3dc440ec2efd5bef82eba8bab74da6ce34fd582a3fc3b1794690` на `560f9871ed8f87956cd854660c3fdc7ba283135c09cc286e128b4dac677c91f3`; model-context signature — с `d69d4f…` (315611 bytes) на `9fb93d2ed81bf09fa54f1095391872c9d252d1d054683f9c79572bc2120eb303` (110003 bytes).

Фактически модели в обоих runs переданы **одни и те же 45 задач**, не 42: 27 table identity и 18 engineering/general, в одинаковом порядке `11 + 16 + 18`. Inventory: total unresolved=80, routed=45, not routed=35. `ROUTING_DRIFT=0`.

## 6. Task-level diff всех 45 routed AI-eligible tasks

`Y/Y` означает old routed=YES и new routed=YES. `PASS` для `NEED_MORE_EVIDENCE`/`INSUFFICIENT_EVIDENCE` означает, что verifier принял корректный отказ модели; это не `AI_RESOLVED_VERIFIED`.

| # | task_id | Routed old/new | Old verdict | New verdict | Old verifier | New verifier | Old materialization | New materialization |
|---:|---|:---:|---|---|---|---|---|---|
| 1 | `aiv2_graph093c833b3e8b812dcb5d` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 2 | `aiv2_graph1b05743a7c96fb825137` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 3 | `aiv2_graph77a770f96b64a5d03bc3` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 4 | `aiv2_graph9019a3788047821b99ce` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 5 | `aiv2_graphae3477b1f6eb3330176c` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | MATERIALIZED_FINDING | MATERIALIZED_FINDING |
| 6 | `aiv2_graphb09ca51e196d3a20d94b` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 7 | `aiv2_graphdc0cc536557acf3d4c34` | Y/Y | SAME_ENTITY | SAME_ENTITY | PASS | PASS | IDENTITY_NO_EFFECT | IDENTITY_NO_EFFECT |
| 8 | `aiv2_task689d6857de251eca1a5f` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 9 | `aiv2_task9645987432ce6645f2d6` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 10 | `blocked:Обозначению «ВРУ3» на одном из листов от` | Y/Y | INSUFFICIENT | INSUFFICIENT | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 11 | `dinc_4aa7caeb4bbc` | Y/Y | DOCUMENT_ERROR | DOCUMENT_ERROR | PASS | PASS | MATERIALIZED_FINDING | MATERIALIZED_FINDING |
| 12 | `etrow_01345b5fe243` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 13 | `etrow_103c355354f0` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 14 | `etrow_1267e9006710` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 15 | `etrow_13bd274d31d2` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 16 | `etrow_1d1d66b3c758` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 17 | `etrow_49377705ce36` | Y/Y | DIFFERENT_ENTITY | DIFFERENT_ENTITY | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 18 | `etrow_4b86e6145bdb` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 19 | `etrow_5b74c4b42f14` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 20 | `etrow_5f3fb7c84741` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 21 | `etrow_6a0bb3d86984` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 22 | `etrow_714f2149a121` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 23 | `etrow_73e5b5173ec6` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 24 | `etrow_7ba58b8584d0` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 25 | `etrow_8ffba02ee64d` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 26 | `etrow_b0629f42e109` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 27 | `etrow_b186dc9e425c` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 28 | `etrow_c175665a6807` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 29 | `etrow_d187ee42a365` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 30 | `etrow_d361638bc684` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 31 | `etrow_d46eb7193d2b` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 32 | `etrow_d95c2ba77ee1` | Y/Y | NEED_MORE | NEED_MORE | REJECT | PASS | REJECTED_VERIFIER | HUMAN_REQUIRED |
| 33 | `etrow_df171fd4a2c5` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 34 | `etrow_ee94b1350e4d` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 35 | `etrow_ef9e2f621358` | Y/Y | NEED_MORE | NEED_MORE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |
| 36 | `uchg_1bf23921e9dbc1df9612` | Y/Y | FORMATTING_ONLY | FORMATTING_ONLY | PASS | **REJECT** | NO_CHANGE | REJECTED_VERIFIER |
| 37 | `uchg_79d5e6aa07c1e8df4ffe` | Y/Y | FORMATTING_ONLY | FORMATTING_ONLY | PASS | **REJECT** | NO_CHANGE | REJECTED_VERIFIER |
| 38 | `uchg_8543fbd998ba9547286d` | Y/Y | SUPPORTED_CHANGE | SUPPORTED_CHANGE | REJECT | REJECT | REJECTED_VERIFIER | REJECTED_VERIFIER |
| 39 | `uchg_86dd32aa72abda4b7af5` | Y/Y | SUPPORTED_CHANGE | SUPPORTED_CHANGE | PASS | **REJECT** | **MATERIALIZED_FINDING** | REJECTED_VERIFIER |
| 40 | `uchg_882a967353baf40a13d1` | Y/Y | FORMATTING_ONLY | NEED_MORE | PASS | **REJECT** | NO_CHANGE | REJECTED_VERIFIER |
| 41 | `uchg_d1d843ad9e789eeabb13` | Y/Y | SUPPORTED_CHANGE | SUPPORTED_CHANGE | PASS | **REJECT** | **MATERIALIZED_FINDING** | REJECTED_VERIFIER |
| 42 | `uchg_dc6ba52a9e78520fd415` | Y/Y | SUPPORTED_CHANGE | SUPPORTED_CHANGE | PASS | **REJECT** | **MATERIALIZED_FINDING** | REJECTED_VERIFIER |
| 43 | `ureview_1319176d91e6cf2528cc` | Y/Y | FORMATTING_ONLY | FORMATTING_ONLY | REJECT | PASS | REJECTED_VERIFIER | HUMAN_REQUIRED |
| 44 | `ureview_8368aeb30c79a505d739` | Y/Y | FORMATTING_ONLY | FORMATTING_ONLY | PASS | PASS | NO_CHANGE | NO_CHANGE |
| 45 | `ureview_9abc613b67e0e1758299` | Y/Y | SUPPORTED_CHANGE | SUPPORTED_CHANGE | PASS | PASS | HUMAN_REQUIRED | HUMAN_REQUIRED |

## 7. Фактически отправленный prompt: три sessions

Старые prompts восстановлены checkout’ом prompt builder из `eb90d6ca` и применением его к замороженным old context/inventory. Для каждой session полученный cache key побайтово совпал с сохранённым cache record; это доказывает фактический сериализованный payload, а не только template.

| Session | Old prompt | New prompt | Tasks/order | Old cache key | New cold cache key |
|---|---|---|---|---|---|
| 1 table identity | `whole-document.v1`; actual output schema `stage-comparison-ai-identity.v1`; 551143 B; prompt digest `38e97f…`; evidence `5db3…` | `compact-context.v3`; identity v1; 266092 B; digest `a936806f…`; evidence `dbc7677b…` | те же 11, тот же порядок | `5745b01efaf0480c6267335d13fad8f1b279a0d74ac4fd0cc5bd8a785a9ff0bd` | `97560714abcb6bc71642765394a0df3a…` |
| 2 table identity | `whole-document.v1`; identity v1; 586576 B; digest `144e47…`; evidence `7d61e4…` | `compact-context.v3`; identity v1; 301525 B; digest `d6a295ff…`; evidence `625e8956…` | те же 16, тот же порядок | `70ca38d238761967a4d14978e8a51fa1c047dfbf5be8209fdb8459dc21d94d9c` | `fa7c711c5abf11cefab5d573fcb86336…` |
| 3 analyst | `whole-document.v1`; `stage-comparison-ai-analyst-v2.response.v1`; 553422 B; digest `67d9ac…`; evidence `d1e63b…` | `compact-context.v3`; analyst response v1; 246956 B; digest `d4ce4dc0…`; evidence `e207ecee…` | те же 18, тот же порядок | `9ea1dd1a3e084b6e6b0d52e96f5b105845f704cebc3981e8eeaab185a8ac9722` | `ff21b15bc136f3f1de37393e4eff3ef2…` |

Old total transmitted bytes: 1691141; new: 814573. System prompt не менялся. Old context signature во всех sessions `ae400629…`, new `560f9871…`.

Содержательный diff:

- compact model context исключил повторяющиеся `source_tokens`, `nearby`, raw `text` и полный ambiguous list, сохранив compact indexes и task-specific evidence refs;
- Focused Evidence одновременно добавил related full-table-row refs для 10 cable/graph tasks;
- new task framing требует strict proof для `FORMATTING_ONLY` и явную исходную `FAST:CHANGE:<task_id>`/`FOCUS:*` ссылку;
- task IDs и ordering не менялись; omitted/added evidence относится к представлению context, а не к FAST source.

Это доказанный `PROMPT_DRIFT`, а не routing/input drift. Исторический table cache metadata называл schema generic analyst v1, хотя schema digest был digest фактической identity schema; минимальный fix теперь пишет фактическую identity schema version. Коллизии не было, потому что digest отличался.

## 8. Gateway/model execution environment diff

| Setting | A | B | Вывод |
|---|---|---|---|
| model / effort | `gpt-5.6-sol` / LOW | то же | same |
| actual sessions/calls | 3 cached responses / 0 calls | 3 sessions / 3 calls | A был warm replay, B cold |
| configured max sessions | 4 | 3 | actual batching одинаково 3 |
| timeout | 600 s | 600 s | same |
| CLI version | исторически не сохранялась, потому доказать точное значение нельзя | исторически не сохранялась | provenance gap закрыт; новые runs записали `codex-cli 0.151.0-alpha.7.2` |
| gateway implementation | git blob `1b5ced…` | тот же blob | same |
| cache/settings implementation | blobs `97e94…` / `32ac…` | те же | same |
| runtime check | SHA-256 `7efddc0735144d785ff688e9dfb1eeeb0d54663f2afa6b55c6f96d7cf9f5f535` | тот же SHA | byte-identical |
| max output | explicit token cap отсутствует; JSON output schema ограничивает форму | то же | same |
| retry | один retry только для transient process failure | то же | no verifier retry |

Одинакова semantic launch configuration: `codex exec -m`, read-only sandbox, skip git check, ephemeral session, ignore user config/rules, isolated temporary `CODEX_HOME`, output file, disabled tools/plugins/apps/multi-agent, LOW reasoning, JSON output schema, prompt через stdin. Environment allowlist и binary path совпали; runtime payload не содержит secrets. Различия environment не объясняют A/B. Историческую CLI version нельзя честно восстановить из существующих artifacts.

## 9. Cache audit

A содержит `model_calls=0`, `sessions=0`, `cache_hits=3`: это warm replay low-effort responses, созданных ранее для `whole-document.v1`. Для каждой из трёх sessions пересчитанный full key точно совпал с сохранённым key выше. Key уже включал evidence/context, model, effort, prompt version+digest, schema version+digest и role. Low/medium artifacts имеют разные keys.

- stale context reuse: 0;
- response from another prompt/schema: 0;
- incomplete key collision: 0;
- `CACHE_ERROR`: 0;
- methodological error: 1 — warm replay старого prompt был принят как финальная acceptance-точка против cold run нового prompt.

Следовательно, cache не был загрязнён. Он «выглядел лучше», потому что воспроизвёл старые model responses; он не доказывал воспроизводимость current prompt.

## 10. Verifier diff

`verifier.py` old/new git blob одинаков: `02a96804…`; identity verifier `8fb003…`; response contract `57874…`. `VERIFIER_HARDENING=0`.

| Task(s) | OLD | NEW | Класс |
|---|---|---|---|
| `uchg_1bf…`, `uchg_79d…` | formatting verdict включал исходный FAST candidate, PASS | verdict тот же, но `selected_candidate_refs` не содержит уже переданный `FAST:CHANGE:<id>`; «нет ссылки на исходную FAST-находку» | C: model output/formal quality worsened |
| `uchg_86dd…`, `uchg_d1d…`, `uchg_dc6…` | supported-change включал исходный FAST candidate, PASS | та же substantive позиция, но исходная FAST-ссылка пропущена; REJECT | C |
| `uchg_882…` | FORMATTING_ONLY, PASS | модель увидела две left rows ВРУ3 с count 2 и 3 и безопасно запросила evidence; contract task type не совпал, REJECT | A: старое решение нельзя считать безопасной истиной |
| `ureview_131…` | claim value не был точной строкой evidence, REJECT | две точные строки evidence, PASS | улучшение model output |
| `etrow_d95…` | выдуманная quote «Насосная АПТ», REJECT | корректный insufficient/need-more, PASS | улучшение model output |
| `uchg_854…` | одна сторона не имеет positive object evidence, REJECT | то же | стабильный безопасный reject |

Из 45 задач model verdict label изменился только у `uchg_882…`, но verifier outcome изменился у восьми из-за структуры claims/refs. Опция B «новый verifier ошибочно слишком строг» имеет count 0. Ослаблять проверку ради возврата 17 нельзя.

## 11. Materialization diff

Current-HEAD counterfactual исключает materializer/projection как первопричину: одни и те же нынешние materializer и HRO projection на old responses воспроизводят старый headline, а на cold responses — новый.

- Три cable findings `uchg_86dd…`, `uchg_d1d…`, `uchg_dc6…`: `MATERIALIZED_FINDING→REJECTED_VERIFIER`, root cause — model omitted required FAST ref.
- Formatting-only: `uchg_1bf…`, `uchg_79d…`, `uchg_882…` переходят `NO_CHANGE→REJECTED_VERIFIER`; `ureview_836…` остаётся `NO_CHANGE`; `ureview_131…` проходит verifier в B, но остаётся human-priority и не создаёт finding.
- Graph identities: все семь проходят в обоих runs; `aiv2_graphae…` materializes finding, остальные шесть — `IDENTITY_CONFIRMED_NO_DECISION_EFFECT`.
- TS1/TS2 document error `dinc_4aa…`: PASS и materialized finding в обоих runs.
- Text/layout `ureview_9abc…`: PASS в обоих, но по policy остаётся `HUMAN_REQUIRED`.
- Table identity: один `DIFFERENT_ENTITY` за run может меняться между независимыми runs, но materializer оставляет его human-required.

Исторические materializer/HRO source blobs менялись, однако counterfactual reproduction доказывает `MATERIALIZATION_CHANGE=0` и `PROJECTION_CHANGE=0` для исследуемого headline.

## 12. Root-cause classification

Counts считаются по наблюдаемым decision transitions A→B, а не по числу строк кода, которых коснулся новый prompt.

| Category | Count | Основание |
|---|---:|---|
| INPUT_DRIFT | 0 | все frozen FAST projections identical |
| ROUTING_DRIFT | 0 | те же 45 IDs и order |
| PROMPT_DRIFT | 8 | восемь verifier outcome transitions на изменённом serialized prompt/context |
| MODEL_NONDETERMINISM | 0 для причинного A→B сравнения; **10** unstable resolution-membership tasks в three-run gate | A и B имели разные prompts, поэтому их нельзя использовать как controlled nondeterminism test; три новых payload-identical runs дают union 18, all-run intersection 8 |
| VERIFIER_HARDENING | 0 | verifier blobs identical |
| MATERIALIZATION_CHANGE | 0 | current-head counterfactual reproduces оба headline |
| PROJECTION_CHANGE | 0 | то же |
| CACHE_ERROR | 0 | все три historical keys valid |
| OTHER | 1 | warm replay ошибочно принят за cross-version acceptance |

На product-support уровне в трёх новых runs нестабильны 6 из union 8 supported/materialized task outcomes; стабильны только graph finding и TS1/TS2 document error.

## 13. Минимальный fix

Модель, prompt, threshold и verifier не менялись. Реализован минимальный reproducibility/provenance gate:

- сохраняются full frozen FAST input manifest/signature и его product projections;
- сохраняются model-context, inventory, full prompt manifest/signature, exact serialized payload, task order, output contract schema, cache key и CLI version;
- run `input_signature` теперь покрывает FAST, context, model context, inventory, prompt, model и effort;
- table identity cache metadata пишет фактическую identity schema version;
- gate требует ровно три cache-disabled runs, по три model calls/sessions, identical input/payload/task signatures, exact accounting и считает resolution/supported/product overlap;
- gate выдаёт A/B/C и запрещает rollout для B/C.

Verifier-aware retry не добавлен: root cause не является единичным детерминированным transport defect, а новые runs показывают широкую model nondeterminism. Retry мог бы изменить coverage, но не доказал бы reproducibility; safety verifier не ослаблялся.

## 14. Три полных cold runs

Все runs использовали frozen FAST signature `890198adde16fe622b8ccd6c1b0b395faab0a1cf3baed38032045b741e4e83b7`, context `560f9871ed8f87956cd854660c3fdc7ba283135c09cc286e128b4dac677c91f3`, model context `9fb93d2ed81bf09fa54f1095391872c9d252d1d054683f9c79572bc2120eb303`, inventory `f23b9f14cd43fc56e6964b1d5d62069cefcd36085dc446caeace08c529944203`, общий run input signature `0faeb731cf4da0ddd338cc731e88c936f4520999b67c2870e0b0dedba33c0ffa`, prompt signature `7b7f2a0937bec60daac1fd6cc1365b93f270a37c57e1bdb4f1cede28dafd9ffa`, модель `gpt-5.6-sol`, LOW, три sessions, cache disabled. Exact payload signatures в каждой тройке одинаковы: `a9dd7fec2120b89cc8d8843e1a4fa80485ba10a10595f46f7ecea95ac045d835`, `2a5689dedafdd46b0a0ce76a31876f2f3c8ef8a46c7fd33e9c7613951dc8c30d`, `ef5a2bfe64f5115f0e24ab6423b8f06240bb77514ef09bf39dfa2daa325e0fab`.

| Metric | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| AI verifier pass / AI_RESOLVED_VERIFIED | 12 | 9 | 17 |
| AI verifier reject | 5 | 7 | 2 |
| materialized findings | 2 | 2 | 5 |
| Stage 7 before→after | 77→77 | 77→78 | 77→75 |
| Stage-7 removed | 0 | 0 (добавлен 1) | 2 |
| AI verified in Preliminary Report | 1 | 1 | 4 |
| Human Review interactions | 6 | 6 | 6 |
| unsupported | 0 | 0 | 0 |
| duration | 587416 ms | 368002 ms | 566158 ms |
| model calls / sessions | 3 / 3 | 3 / 3 | 3 / 3 |
| cache hits/writes | 0 / 0 | 0 / 0 | 0 / 0 |

Run directories: `comparison/ai_analyst_v2/20260901_grsh_repro_gate_run1`, `...run2`, `...run3`. Ни один не имел model failure или timeout.

## 15. Overlap и stability

| Pair | Resolution overlap | Supported materialized overlap | Materialized product overlap |
|---|---:|---:|---:|
| Run 1↔Run 2 | 8/13 = `0.615385` | 2/3 = `0.666667` | 2/3 = `0.666667` |
| Run 1↔Run 3 | 11/18 = `0.611111` | 3/8 = `0.375` | 3/8 = `0.375` |
| Run 2↔Run 3 | 9/17 = `0.529412` | 2/8 = `0.25` | 2/8 = `0.25` |

Под «resolution» gate считает множество `AI_RESOLVED_VERIFIED` task IDs; под supported — task outcomes, реально поддержанные verifier/materializer; product overlap сравнивает canonical product decision fingerprints, а не model prose. Поэтому расхождение нельзя списать на разный текст ответа.

Стабильное ядро supported product: graph identity finding `aiv2_graphae3477b1f6eb3330176c` и document error `dinc_4aa7caeb4bbc`. Formatting-only `ureview_836…` есть в Run 1 и 3, но отсутствует в Run 2. Пять cable tasks, отклонённые в historical cold B, внезапно проходят в Run 3. Это и есть controlled proof `MODEL_NONDETERMINISM`.

Machine-readable gate: `comparison/ai_analyst_v2/20260901_grsh_reproducibility_gate.json`; verdict `B`, `recommend_rollout=false`, structural `problems=[]`.

## 16. Unsupported и human interactions

Во всех трёх независимых runs:

- `unsupported_published=0` и `unsupported_materialized=0`;
- Human Review Orchestrator сформировал 6 действий;
- verifier/materializer safety barrier сохранился;
- различается coverage и продуктовый набор, а не safety count.

Это исключает verdict C «системный safety defect», но не удовлетворяет rollout acceptance: `0 unsupported` необходимо, но недостаточно при product overlap `0.25`.

## 17. Tests

Добавлены regression tests для:

- frozen FAST input digest и old FAST provenance unchanged;
- prompt/context/model-context/run signatures;
- cache-key integrity и фактической contract schema;
- deterministic task ordering;
- exact routed-task accounting;
- exact serialized payload capture;
- gate requirements: три cold runs, cache disabled, calls/sessions, unsupported, overlaps, A/B/C.

Verifier retry tests не добавлялись, потому retry не реализован. Проверка релевантного набора:

```text
python -m pytest -q tests/test_stage_comparison_ai_v2.py \
  tests/test_stage_comparison_ai_v2_reproducibility.py \
  tests/test_stage_comparison_ai_v2_materialization.py \
  tests/test_stage_comparison_ai_layer.py
144 passed in 0.94s
```

Дополнительно вызван полный `python -m pytest -q`. Он остановился на collection до запуска tests: в текущем окружении отсутствуют optional gateway dependencies `grpc` и `google.protobuf`, из-за чего не импортируются пять unrelated gateway/PKI/transport modules. Требуемые версии объявлены в `requirements-gateway.txt`/`requirements-worker-grpc.txt`; зависимости в рамках этого forensic scope не устанавливались. Это environment collection blocker, не failure AI Analyst tests.

## 18. Final verdict и git status

Acceptance: **B — безопасно, но AI outcome слишком нестабилен**. Rollout recommendation: **NO**. Latency не оптимизировалась. FAST, нормативный контур, HRO UX и Engineer Approval flow не менялись. Push/deploy/release не выполнялись.

После documentation commit ожидаемый status: branch `main` содержит два локальных task commits сверх исходного HEAD; рабочее дерево чисто по task-файлам. Существующий до начала работы unrelated untracked `backend/app/data/objects.json.corrupt_20260831T093914` сохранён без изменений и не включён ни в один commit.

«Почему старый AI Analyst давал лучший результат, а новый cold run — хуже»: старый результат был warm replay ответов для более раннего полного prompt, тогда как новый cold run получил содержательно другой compact prompt; одинаковый safety verifier отклонил ответы, потерявшие обязательные FAST-ссылки, а controlled повторение нового prompt дополнительно показало сильную недетерминированность модели.

«Стабилен ли теперь AI Analyst v2 на трёх независимых запусках: НЕТ».

«Можно ли после этого выкатывать его в production: НЕТ».
