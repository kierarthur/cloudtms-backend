# F-25 Materialise call completion review

10 October 2026. **PASS for the finite0129 Materialise save/INSERT guard/retained assertion foundation. All three findings below are corrected in fresh reviewed bytes; no remaining actionable source defect found. Whole owner remains pending.** This is source review, not installation or runtime evidence.

Source-only review in BUILD-BE `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`, baseline `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. Full0129,0111/0128,0121/0122/0125, full original2203 MAT and1534 submissionComplete, component immutability owner, both design notes, BD018 and registry inspected. Applied schema status is parent evidence; this reviewer read saved source only. No SQL, tests, runtime, fixtures, product edits, hosted/network or secret operations. Only this review artifact was written.

## Findings closed in fresh reviewed source

1. **Actual immutable source-signature lineage/content — corrected.** Initial0129 asserted copy workflow/result generation/Timesheet/kind/role/content but did not link the actual request source signature to that copy. Current save lines153-164 selects the genuine source UUID from transient original request, requires its immutable workflow/expected generation/kind/role, real source/copy immutable timestamps, exact `copy.source_component_id=coalesce(source.source_component_id,source.id)` and exact copy/source/certificate contentSHA equality. This matches original2203 source selection2406-2415 and copy2676-2678. Replay checks frozen fields only, never today's source state or workflow generation. The original valid source hash is nonnull32 bytes; NULL equality cannot silently certify missing content. Retained copy assertion also requires its real nonnull frozen source_component_id.
2. **Fresh ContractWeek/Timesheet owner links — corrected.** Fresh guard lines97-102 binds actual ContractWeek(id,Contract,week) and actual Timesheet(candidate_workflow_id,candidate_workflow_generation) equal certificate workflow/result generation, alongside ID/version/Contract/week/hash. This follows original2203 validation2380-2384 and write2632/2646-2647. These are plain preclock FRESH identity checks; neither replay nor later retained assertion consults them as currentness.
3. **Exact single-membership submission-complete tail — corrected.** Fresh SUBMIT now requires request_scope_id=submission_membership_id, comparison result SHA equals actual child result, completion object and original finite scalars ok:true/replay:false/completed_scope_count:1/issue_scope_count=(child.outcome='ISSUES'?1:0), and actual original manager_message_intent_ids array. Actual submitted membership Timesheet/revision/hash remain exact; membership state is now the exact MATCHED versus ISSUES result, not either submitted state. This matches original1534 return635-638 and update514-518 for original2203's one supplied completion. The full actual completion JSON is retained in its original business cache/result digest and is not rewritten by this protocol; no fresh completion condition is imposed on replay.

## Current owner and INSERT timing

The shared0125 owner guard requires actual current xid/epoch, BUSINESS_OPEN, OPEN input transaction, NEXT epoch, cause/stack identity, no completion witness kind/ID/SHA, no metadata command and no already inserted root adjunct.0129 exact single private table trigger further requires mode STANDALONE_QUERY and one complete stack: DIRECT `{MAT}`, SELF `{SELF,MAT}`, APP `{APP,MAT}`. Caller kind is actual root-derived DIRECT/SELF/APP. Cause UUID text must equal request key; original Materialise SHA equals direct/SELF root hash, while actual APP root hash is intentionally separate. Exact supplied xid/epoch, transaction timestamp, named-composite SHA and actual current input end ordinal are checked. All schema/FK/NOT NULL/variant restrictions remain.

No new Source/module/business lock, agency command, producer leave or finish is introduced. The genuine future outer owner must own early admission and original workflow/Timesheet/ContractWeek/component locks. This helper stores a call certificate at the real preclock business tail, not a new business receipt.

## Full transient request and actual unchanged reply

Save recomputes exact original `weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_CANDIDATE_CHECK_MATERIALISATION_V1',request-'request_idempotency_key')`. It binds actual parsed account/Candidate/workflow/request UUID/expected generation/kind/generation/publication/request-scope fields and original `_candidate_sha256_jsonb_v1(immutable_submission)`. The direct casts match original2203 parsing2265-2276; there is no invented optional empty-string normalization. Full transient request is hashed and not retained.

Full original result must be an object, ok true, with exact variant replay flag, workflow/result generation/Timesheet/revision/hash/copy signature/kind/materialisation request hash scalar equality. Actual input/result Timesheet hashes use original lowercase32-byte parser. Save hashes complete original return JSON, including actual comparison/completion result, without substituting a smaller object. Insert guard binds it to the exact scoped workflow last key/hash/cache. Cached result scalars are parsed again; NULL identity/hash or mismatch fails DISTINCT/STRICT/schema checks. Named NEW casts match0111 table composites and request/result SQL UUID/integer/bytea/text types.

## Fresh and original replay separation

FRESH has actual original WORKER_SUBMITTED workflow/result generation, signature copy and immutable-submission SHA, exactly expected_generation+1, current actual Timesheet/hash and real comparison/submission completion where SUBMIT_TIMESHEET. The certificate's no-comparison schema/retained proof preserves actual CHECK_HOURS/SELF query-empty behavior.

EXACT_LAST_REPLAY uses the same original locked workflow account/Candidate/key/hash and exact cached return with original `idempotent_replay=true` change. It deliberately runs before the FRESH guard. It never requires today's WORKER_SUBMITTED state, current workflow generation, current Timesheet version/hash, current membership or current publication. Original request immutable-submission SHA comes from exact submitted request whose hash matched cache, not today's workflow snapshot. Required Contract/ContractWeek/week/environment scalars are replay context only; they are not historical fresh submission terms. This agrees with saved guard design/replay refinement and BD018 companion schema review.

Replay takes no prior comparison into this transaction:0111 requires no comparison reference and zero new stages; retained assertion requires physically no same-producer compare child. A pure pre-entry original replay returns without any producer/certificate. A genuine already-owned locked-race/inherited replay can save its actual zero-stage certificate for its parent's returned result. Full owner implementation must preserve that distinction.

BD018/0128's APP_EXACT_REPLY instead describes a new zero-work APP transaction's actual old FINAL_SUBMIT receipt; it has no MAT/compare/response child work. This MAT foundation does not reinterpret that branch or pretend an old APP receipt means the new transaction materialised.

## Retained assertion and complete actual range

Retained MAT assertion reads actual immutable certificate and producer, checks triple/root/caller/cause/request/selfSHA, actual raw range and all same-producer comparison children. No child permits no stages and physically zero child headers. FRESH SUBMIT requires exactly one real ordinal1 MATERIALISE child, same producer, generation/publication/membership/Timesheet/revision/hash/Candidate/Contract/week and identical actual begin/end range, with exact child SHA;0122 complete evidence/stage/witness assertion is invoked. Thus the MAT slice cannot contain stages outside its actual comparator slice or import a child from another transaction. Other canonical post-comparison callbacks must be classified/proved or truthfully refused by future owner integration.

Original submissionComplete changes candidate_action/manager_action and delivery/audit state; current incident canonical hook includes identity/state/current comparison only. Those ordinary updates do not alone add Query stages. No material canonical change is silently ignored by the range assertion.

Retained signature copy assertion checks immutable workflow/generation/Timesheet/kind/role/content and nonnull immutable_at; it intentionally ignores present component state.07082026_2310 fixes those immutable fields, source lineage/content and prevents deletion after immutable transition. Retained MAT proof does not consult current workflow, Timesheet hash, membership, module activity or publication currentness. Replay context remains contextual; no later assertion upgrades it to former fresh owner authority.

All derived current xid/epoch, whole original result SHA, actual raw range count/SHA, transaction timestamp and final typed selfSHA are assembled before a sole immutable header INSERT, followed by full retained assertion. No placeholder UPDATE, deferred business FK, callback second clock, audit side-effect journal or raw financial/submission snapshot is added.

## Declarations, ACL, registry and completion boundary

Three new private SECURITY INVOKER functions have explicit paths: retained assert STABLE; trigger/save default VOLATILE. Logical owner postgres; PUBLIC/anon/authenticated/service_role execute revoked. One exact BEFORE INSERT table trigger protects MAT certificates. Registry exact assert(uuid),trigger(),save(materialise_completion,jsonb,jsonb) signatures use0129 as sole new final root/physical/closure/allowedPhysical with no predecessor. Original2203 public MAT remains unchanged by this foundation.

No whole-owner PASS is made. Original full MAT/SELF/APP body seams must still capture actual request/result/context/scope/signature and actual range start/end, save at final cache tail, leave nested children and finish true roots once. Whole APP response/comparison union, root adjunct and Source prefinal completion remain separate.

## Raw saved checkpoint

| Path relative to BUILD-BE | Bytes | Raw SHA-256 |
| --- | ---: | --- |
| `supabase/repeatable/10102026_0129_banking_pay_next_candidate_materialise_completion_v1.sql` | 17292 | `2ae3bbd068bc9f5e68eab42963d6f8ad1354477a25041ee327085f446db91edd` |
| `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` | 19298 | `2f6c039ad7ac175e0d6aa486baa888dbe9d18f0292b80d577ac4a4925ad5df8a` |
| `supabase/migrations/10102026_0128_banking_pay_next_candidate_exact_app_reply_v1.sql` | 1979 | `c1e32e41a2cd8fefd095927f822c2394ef2cfcfe02c226cfb7d7b16670e9c53c` |
| `supabase/repeatable/10102026_0121_banking_pay_next_candidate_completion_digests_v1.sql` | 15740 | `ed7afd09a2ef94717dc403e4686d764c35d152fc9f83c540bd7a5eda5177d5b7` |
| `supabase/repeatable/10102026_0122_banking_pay_next_candidate_compare_evidence_v1.sql` | 22197 | `9ec7a01ea541e69f4d81f9d22dd4b21b50e96b6cfe45fa8f8e293efd90b0ae51` |
| `supabase/repeatable/10102026_0125_banking_pay_next_candidate_compare_completion_v1.sql` | 18063 | `5f8b6839c72d470706982c2f5531f266c6161ff980cb6c55057b4b91941feaec` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 149249 | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | 268078 | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `supabase/repeatable/07082026_2310_candidate_manager_review_helpers_v1.sql` | 27036 | `42a3b830a53ea3ffad232dbe414d06d38771e4d6c4736a16518053a14df811ed` |
| `docs/bpay-next/design/F-25-candidate-certificate-guard-build.md` | 18930 | `e16c33679a1aeafc03950d8b12d4b1eae8c10ba9f6d9ed42c5378e42e3853cb8` |
| `docs/bpay-next/design/F-25-candidate-protocol-schema-build.md` | 23517 | `2b6279e1a8d057fbd175d27e68e0967d866ecf46c95144a38cc4d195ef011473` |
| `docs/bpay-next/BUILD_LOG.md` | 40015 | `4adc36d340f3dde6e32ad44792ba12e024566d6e444666f81951cb1a02465931` |
| `docs/bpay-next/final-definer-closures.json` | 763598 | `6405f5048455c38fd5c14493f93a3ad59fd01d0a202b2af13c1c81f6892a0840` |
