# WB-29 collection identity batch — independent source review

12 October 2026. **SOURCE PASS for the five-function 2155 batch only.** No essential defect found in the reviewed bytes. This is neither whole-WB-29 completion nor compile, installation, first-use, queue or app acceptance. No SQL/RPC, business data, fixtures or tests were executed by this reviewer. Root alone owns product changes and any later authorised checks.

## Scope and exact source

Read the complete owning WB-29 card (`02_WORKBENCH.md:1073–1137`), its exact working checklist D01–D24 and BD155 (`design/WB-29-resolution-owner-design.md:29–43`). Compare each entire new function with the actual parent recorded in `design/WB-29-collection-successor-provenance.json`; inspect the canonical reader in `2135_banking_pay_next_treatment_native_publication_v1.sql:5–34` and the native publication interval in `0016_banking_pay_next_stale_snooze_publication_v1.sql:152–219,299–337`. Paths below are relative to this backend worktree; dated SQL files are under `supabase/repeatable`.

| Reviewed function/signature | Exact parent | Source finding |
| --- | --- | --- |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` | `12102026_1822_banking_pay_next_classified_work_collection_v1.sql` | 2155:13–69 adds same-Work canonical Source aliases and substitutes only original/current Source-event authority. Observed Position revision/A/R, actual original approved-line identity, PAYE/source-target conservation, component/rule and whole-Case balance checks remain intact. |
| `private.bpay_next_case_allocation_plan_v1(jsonb)` | same 1822 | 2155:192–250 makes the equivalent alias change in the existing managed-origin lookup. The complete remaining DERIVE/ALLOCATE body, accepted Rule B rule/control cut, live balance operands, floor, ordering, headroom and allocation formulas are unchanged. The inherited explicit Source-comparison refusal at 185–189 remains; this batch does not claim that later dependency is implemented. |
| `private.bpay_next_work_collection_guard_v1()` | same 1822 | 2155:666–681 qualifies the original paid Source authority via the canonical alias. Immutable original effect/run-line/revision/line/transfer, exact realised-hold and outcome proof, current leased job/epoch/phase, actual observed Position and segment/manual observation assertions remain unchanged (623–665,682–783). |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | `12102026_1849_banking_pay_next_manual_collection_retained_approval_v1.sql` | 2155:906–910,1009–1031 map original Source authority only. 1058–1085 separately qualify original binding publication and actual treatment publication. Actual paid/captured/Position/current/job operands, first paid-anchor fallback, same-job replay and all principal/R/W/H/effect/projection calculations remain unchanged. |
| `private.bpay_next_work_collection_observation_v1(uuid,text,uuid)` | `12102026_1934_banking_pay_next_removed_source_collection_observation_v1.sql` | 2155:1207–1210,1240–1314 use canonical Source authority and independent actual publication. Retained known MANUAL withdrawal, actual current/Position eligibility, exact REMOVED classification/absence and authorised bundle/head/receipt proof, SHIFT→FIXED exception and full source/target segment conservation remain unchanged. |

The complete function comparison contains only the stated new alias declarations/point joins, Source-authority operand replacements and split-publication qualification. It contains no altered money expression, stored paid identity, mutation/counter/event statement, release scope, fallback, new producer or command. Function signatures, volatility/security/search-path declarations and owner/revokes retain each predecessor. All five remain private; the batch adds no public SECURITY DEFINER membership or grants.

## Publication interval, strict eligibility and NULL handling

The native 0016 owner sets each Position's `applied_revision_id` to the new publication before calling reconciliation (180–200). Its publication can still be QUEUED in that transaction; only later does it set APPLYING (212–214), and Work's aggregate applied pointer advances at final SNOOZE completion (299–313). Therefore original Source-book publication and current operational publication must be distinct facts.

2155 implements that split correctly: the binding's approval revision is the qualified canonical Source revision (1074); its original publication must already be APPLIED/DONE when different from the actual Position revision (1081). The actual publication uses the actual Position revision and the current POSITION_APPLY command, with original QUEUED/APPLYING eligibility (1070–1071,1082–1083). Settlement/release paths retain actual APPLIED/DONE eligibility (1084). It does not wrongly require aggregate `Work.applied_revision_id` to have advanced during NEW/REMOVED. The strict actual `Work.current_revision_id = Position.applied_revision_id` check remains at 1044–1046 and 1080; the original leased Candidate/job/order checks remain at 821–840. Observation likewise retains exact actual current/Position eligibility (1192–1205) before its split publication joins (1263–1289,1302–1314).

The canonical helper is called only for a non-NULL native revision with both a seal and a non-NULL treatment root link; CASE otherwise keeps that exact revision ID (17–18,196–197,668,906,1009–1012,1207–1210). Missing/unsealed actual observation still returns incomplete through the original path (1202–1205). A claimed sealed treatment with invalid immutable root/publication/predecessor/command fails closed through 2135:19–31; no unqualified history fallback is introduced. Original and current line/component IDs remain actual native IDs, including the retained REMOVED classification line (1254–1256,1276–1284).

This is an original-approval alias, not accepted-offer eligibility. It does not replace accepted current/expected/captured revision guards or revive an earlier accepted selection after later publication. Owner E2/Rule B still requires changed accepted Work to be held back/system-released without repricing; the later capture/hold and accepted-choice integration remains outside this five-function receipt. No principal, policy, tax, allocation or balance freeze is added.

## Frozen review hashes

SHA-256 of saved disk bytes at this review:

| File | SHA-256 |
| --- | --- |
| `12102026_2155_banking_pay_next_treatment_collection_identity_v1.sql` | `9fa243c98655e950e670bcb65eb73ef554883cf00cb2e1824982a5ed407c8eee` |
| `design/WB-29-collection-successor-provenance.json` | `a0e820e3151ab5ea682ea5da3a03880e7c3866626740ddde445e9a608d5dc078` |
| `design/WB-29-resolution-owner-design.md` | `72d10d9b2219058ff072a4768bc2424edd0acfa5e94d1a6abbdb0a1ab30fb5e6` |
| `12102026_2135_banking_pay_next_treatment_native_publication_v1.sql` | `56d91e37ede376018c72245ee02fd1d30237f987c656754b2fd0ec668582ddc1` |
| `12102026_1822_banking_pay_next_classified_work_collection_v1.sql` | `b9650e9da89a3dca3b59bc33f73efc8acf132038dae163df826d1f18730990fd` |
| `12102026_1849_banking_pay_next_manual_collection_retained_approval_v1.sql` | `de67314666b76892ef6b4ac25554fa44e7f7a89a96068eabe6b7ea9a3b79a830` |
| `12102026_1934_banking_pay_next_removed_source_collection_observation_v1.sql` | `c52b03c259630bfed375cd3e21e6e25ee0668b5a1a64e73b42979198787c3020` |

Provenance parent hashes match the actual saved parent files. Source verdict applies only to the exact reviewed batch above. Root's later compile/catalogue/source-gate results, if supplied, require their own attributed evidence; no such results are inferred here. **Whole WB-29 remains open; all joined business/app acceptance is NOT_RUN.**
