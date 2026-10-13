# F-25 Candidate materialise and comparison completion build

10 October 2026. Independent source-only design for BUILD-BE. No product edit, SQL, runtime, test, fixture or hosted action was performed. Root alone writes successors. This bounded design does not complete F25, SourceUpload/Final/Correct Final or the current07102026_2215 Rows owner.

Use the existing CANDIDATE_MATERIALISE, CANDIDATE_SELF_SUBMIT and QUERY_SYNC kinds. A true app submit already has its immutable FINAL_SUBMIT receipt in current0017. Direct materialisation and self-submit lack an immutable whole-owner receipt and need a small private append-only **protocol completion**, anchored to actual original results/evidence at the true tail. Their mutable workflow/draft/membership is valid original business state while the owner holds it, but must not be reconstructed later as durable completion authority. Private compare and prefinal are child work; neither gets a clock or finishes an outer producer.

## Exact saved bodies, callers and seams

All four bodies below are still sole saved definitions in `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql`. Preserve full current bodies and actual signatures/security/ACLs in a new successor; add exact F-04 final-root entries. Current APP submit body is instead0017:892–1157 and must remain the predecessor for that signature.

| Function | Earliest original lock / pure work | True tail / ownership |
|---|---|---|
| `public.weekly_source_candidate_check_materialise_atomic_v1(jsonb,timestamptz)`,2203:2188 | Service/shape/typed parsing/request kind/hours-only :2245–2285; original hash `WEEKLY_SOURCE_CANDIDATE_CHECK_MATERIALISATION_V1` of request minus request key :2287–2290; operation advisory :2291; workflow UPDATE lock :2295–2296; original **last-only mutable** replay :2301–2309. | After original Timesheet/workflow/signature-copy writes; optional compare :2721; full submission-complete child :2725; actual result :2738–2746; final workflow response UPDATE :2747–2748; finish/leave before return :2749. APP0017:1083 and self2203:3047 are the only saved SQL callers. |
| `public.weekly_source_candidate_self_submit_atomic_v1(uuid,text,jsonb,timestamptz)`,2203:3002 | Service :3019; **true session context lock :3020–3021 precedes materialise**; body/typed/workflow validation :3027–3045 currently follows it. | One nested materialise call :3047–3059; true root completes before :3060 return. No immutable self business receipt is written. |
| `private.weekly_source_candidate_submission_compare_sync_v1(uuid,uuid,uuid,uuid,bytea,timestamptz)`,2203:1224 | Service/typed required fields :1278–1281; no admission/producer today; null-generation publication/cycle/Timesheet locks :1290–1301 or generation/cycle locks :1312–1319. | Exact issue loop, new/changed comparisons, omission resolution/outreach/notifications through :1893, final open count :1895–1905, return :1907–1911. Only materialise:2721 and prefinal:2018 call it. Child only under those real parents. |
| `private.weekly_source_candidate_prefinal_publish_recheck_v1(uuid,uuid)`,2203:1918 | Service, publication/cycle/upload/profile/coverage and Office RECHECK_SOURCE :1942–1967 are plain reads/authority. It delegates locking compare inside signed-week loop :2018. | All actual signed weeks :1972–2031, then all distinct cohort restarts :2032–2040, return :2042–2045. Sole saved caller is real `public.weekly_source_projection_publish_atomic_v1(jsonb)` at upload_publication:3101. That parent still runs import outreach :3106 and returns :3117. |

The current2357 edges already support APP_SUBMIT→MATERIALISE, SELF_SUBMIT→MATERIALISE, MATERIALISE→QUERY_SYNC and SOURCE_UPLOAD→QUERY_SYNC. There is no need for a new prefinal owner enum or QUERY_SYNC→QUERY_SYNC edge. Do not treat the existing generic SOURCE_ROWS_APPLY→QUERY_SYNC edge as proof a real Rows caller exists; current2215 does not call either private compare/prefinal in the bounded saved census.

**Actual SELF_SUBMIT branch is query-empty unless another real canonical callback changes a tuple.** Materialise invokes compare/submission completion only when `v_kind='SUBMIT_TIMESHEET'` :2709. SELF_SUBMIT and CHECK_HOURS do not enter that branch. They still mutate actual Candidate Timesheet/workflow/signature state, whose ordinary columns are outside19 PAY_QUERY relations. Preserve that condition. Do not add a new self comparison, version every Timesheet save or claim self creates CLEAR evidence. A valid zero-stage root finishes its owned transaction with no metadata command.

## 1. Materialise entry, replay and exit

After original service/shape/typed/hash work and **before advisory :2291**, perform only a plain workflow probe scoped by exact workflow/account/Candidate. The existing retained last tuple can return the original reply only when `last_mutation_idempotency_key` equals the exact key and stored response's materialisation hash matches the original request hash. Missing response/hash is not a completed replay. Preserve original replay conflict and last-only limitation; there is no historical search or fabricated reply for an older overwritten key. Retain the locked original probe after entry as race recheck.

A new direct call enters CANDIDATE_MATERIALISE with the existing request key UUID text and original materialisation hash, STANDALONE_QUERY, before the first advisory/row/FK lock. A nested app/self call uses the same existing edge and receives `is_outer=false`; it must not fresh-admit, take a second module decision, replace parent request hash or finish a command. Pure app original receipt still wins in current0017 before entry. The materialise child hash differs from the app root hash; keep that distinction in typed child/root evidence.

Preserve all original original account/workflow/generation/publication projection, scope/currentness, Contract-week/signature/policy, week-start/active-request/day-off/schedule/overlap and financial-state-exists refusal checks. Preserve lifecycle context/deferred pay-summary GUC handling exactly. No financial row, summary, invoice, authorisation or payment capture is added.

Direct materialise finishes only after final workflow response :2748 and a genuine current-xid protocol completion inserted at that point. Nested materialise records whatever finite child comparison completion is needed, then leaves MATERIALISE before returning to its parent. APP still runs optional Candidate response, draft retirement, final projection and its immutable receipt :1145–1150; parent finish stays :1151–1154. No child finish occurs at comparison return or workflow generation update.

Locked race replay after entry has no new business mutation. If outer, insert a **zero-stage exact-last-replay** protocol completion which records the genuine locked retained response/hash/key and no child certificate; then finish without a metadata command. This does not claim this later transaction generated earlier comparison evidence. If nested, simply leave. A pure pre-entry exact reply returns without producer/completion/version. Unknown/overwritten old keys retain the original stale/replay behavior.

## 2. Self-submit enters before true session lock

Use `_candidate_session_context_v1(session,environment,NULL,now,false)` first, as current0017 APP does. Exact helper07082026_2059:633–638 shows false path is plain SELECT. Preserve its original active environment/session/account/expiry/rotation/selected-Candidate ownership/eligibility checks; do not store session UUID, context JSON/email or credentials in protocol evidence.

Perform original body/UUID/integer/timestamp/hour-shape parsing and plain workflow/account/Candidate match before entry. Construct exactly the original server materialise request :3048–3057 from that authenticated context and body. Compute the existing materialisation hash over this exact normalized request minus request key, retaining the same domain; it already binds account/Candidate/workflow/expected generation/signature/signed time/immutable submission. SELF_SUBMIT has no original independent hash, so using this actual delegated-request hash for its protocol cause does not alter a business reply hash. Keep a distinct SELF_SUBMIT kind and genuine key UUID text.

If exact currently retained materialise replay is available, return that original reply before fresh locking. Otherwise enter SELF_SUBMIT before `_candidate_session_context_v1(...,true)`, then retain its original true context and workflow validations. Recheck selected Candidate and account equal pure context, matching current0017:986–989. Delegate to materialise, which inherits/pushes MATERIALISE and leaves after its original tail. Self inserts its root completion and finishes only after the returned child result, immediately before :3060. Same-XID parent/child digests and result identity must match; no mutable future workflow reread supplies the witness.

## 3. Minimal finite protocol completion shape

Current0020 header allows only SYSTEM_HOURS_ACCEPT/QUERY_SYNC and records public delivery-specific cycle/publication selection. It is **not** a valid Candidate materialise header unchanged. Author a separate additive Candidate completion type or an explicit later schema extension; do not reuse QUERY_SYNC kind to conceal a materialise/self root. Retain2352 owner enums.

Suggested exact private root header, keyed by actual producer UUID/composite xid+epoch:

```text
owner_kind CANDIDATE_MATERIALISE | CANDIDATE_SELF_SUBMIT
completion_variant FRESH_MATERIALISE | EXACT_LAST_REPLAY
root_request_sha256 bytea32; materialise_request_sha256 bytea32
request_key uuid; account_id uuid; candidate_id uuid; environment finite original environment
workflow_id uuid; expected_generation positive; result_generation positive
request_kind CHECK_HOURS | SUBMIT_TIMESHEET | SELF_SUBMIT
contract_id uuid; contract_week_id uuid; week_ending_date date
candidate_generation_id null|uuid; publication_id null|uuid; membership_id null|uuid
actual_timesheet_id uuid; actual_timesheet_version positive; original_timesheet_hash bytea32
signature_copy_id null|uuid; signature_content_sha256 null|bytea32
immutable_submission_sha256 bytea32; original_result_sha256 bytea32
comparison_child_count finite; ordered_child_certificates_sha256 bytea32
actual_stage_count 0..1000; first/last ordinal exact NULL-pair or positive bounded range
raw_stages_sha256 bytea32; completion_sha256 bytea32; completion timestamp
```

No full request, immutable submission/schedule blob, mutable workflow/draft JSON, result JSON, contact/bank/money/counter snapshot is stored. Original semantic hash and typed result SHA bind those original owner operands. Actual typed references/digests/generation transitions/result are captured and validated **at the owned true tail**, before clock; immutable header then retains that proof. Later assertion never requires workflow to remain WORKER_SUBMITTED or current generation to equal the old result, never recalculates Timesheet hash from today's mutable row and never reprojects the current Candidate request.

The signature copy has actual immutable facts:07082026_2310 component guard refuses DELETE when immutable_at nonnull :432–433 and fixes workflow/generation/no/kind/role/source lineage/content SHA/immutable time :456–473. Its state may later change, so do not require today's state='IMMUTABLE' as historical completion proof. Validate original same-owner signature copy lineage/content facts at fresh tail, retain only UUID/SHA; no storage-key/contact data. Workflow/Timesheet are mutable and their original terms require this new append-only header rather than relying on their references alone.

Same-xid insertion helper validates OPEN/BUSINESS_OPEN, exact actual root/top stack and original request; complete actual result/hash and selected scope; every child certificate; exact whole material-stage raw digest/census. Header/witnesses use private RLS/revoked service/browser direct writes/execute and existing append-only UPDATE/DELETE/TRUNCATE guards. Exact scalar checks must include nonnull positive ordinal endpoints and nullable tuple branches explicitly. This root protocol completion is metadata only, never an advertised business idempotency reply.

For nested APP, its existing immutable FINAL_SUBMIT receipt remains root completion authority; do not create a second Candidate root header or clock. Associate child evidence by same actual producer ID and extend the existing APP witness validator to verify applicable child certificate census without replacing its receipt/candidate/request/response proof. Current0017 APP hash/witness branch :87–94 remains the predecessor.

## 4. Private comparison inherits one parent and retains actual evidence

Before its first cycle/generation/Timesheet lock, require the actual OPEN preclock parent. Known actual caller top kinds are MATERIALISE or SOURCE_UPLOAD (prefinal is transparent child orchestration). Enter QUERY_SYNC using bounded private call identity/hash binding actual generation/publication/membership/Timesheet/retained Timesheet hash; this pushes only the existing allowed edge. For a null token under stable other module ownership preserve original shared Source behavior. Under NEXT, a direct unsupported private invocation with no proven outer owner refuses with a finite named owner-not-installed error before its business locks; do not manufacture a standalone public QuerySync receipt.

Preserve the entire original comparator: current publication/profile/policy; confirmed-prefinal coverage; candidate schedule matching and nearest source tie refusal; historical exact same-profile durable-work-event reuse :1521–1571; actual SOURCE_ABSENT_ZERO/FULL_NEGATIVE/ZERO distinctions; original work-event identity INSERT/conflict checks; source-only unmatched issue generation; effective query policy; resolved-decision matching; new/changed/unchanged cases; exact omission scope; final original open count/outcome. No new comparison calculator or broad omission predicate is allowed.

Add a private append-only **child comparison completion** per actual call, attached to producer/xid/epoch with finite call ordinal/UUID. It is a subordinate witness, not a producer or financial command. Record actual input typed scope/retained hash, complete generated issue work-event ID set+digest, exact seen set+digest, result new/changed/unchanged/resolved/open counts/outcome, omission incident set+digest, immutable comparison/event witness set+digest and the exact child material-stage ordinal range/raw digest. Full generated issue set is computed by original code, not by a later current draft/source reread. Validate `new+changed+unchanged=generated issue count` and exact seen equality.

For new/changed comparison capture actual immutable comparison ID and its full original existing comparison fingerprint. Capture the actual SOURCE_RECHECKED event by exact unique `(event_kind,idempotency_key)` after the original ON CONFLICT DO NOTHING INSERT; require its incident/episode/publication/fingerprint and payload comparison ID/origin exactly match. Do not require a fresh RETURNING row or fabricate a new event. Unchanged OPEN comparison retains its exact immutable comparison ID/fingerprint, with no invented SOURCE_RECHECKED event. An unchanged comparison or genuine retained resolved decision may belong to an earlier publication: the original fingerprint compares business facts, so requiring that retained comparison's publication ID to equal the new input publication would break the original same-facts branch. Bind the current call's publication separately from its genuine historical immutable comparison identity.

Original resolved-decision skip :1741 uses helperdelivery:4663–4686: actual latest incident RESOLVED with OFFICE_ACCEPTED_SYSTEM_HOURS/MANAGER_CONFIRMED_SYSTEM_HOURS/CANDIDATE_CORRECTED and exact comparison material terms. It returns a boolean, not an immutable certificate. Capture the exact immutable comparison and genuine matching resolution event ID/digest **while executing that original skip**, plus original resolved decision kind. Validate the actual immutable evidence for that precise incident episode; do not later rerun latest mutable incident to reconstruct why it skipped, and do not count a missing witness as generated new evidence. An inherited unknown historical decision requires honest unsupported/UNAVAILABLE proof handling in NEXT, not a fabricated resolution event.

Omission scope is exactly2203:1848–1865: same group/cycle/Candidate/client; comparison Contract; work date in actual membership week; prefinal confirmed coverage when applicable; OPEN and work event absent from exact seen set; incident UUID lock order. Preserve all predicates. Capture its complete actual resolved incident set and exact `SOURCE_MATCHED:<incident UUID>:<publication UUID>` RESOLVED SYSTEM events with origin CANDIDATE_TIMESHEET_SUBMISSION. Metadata counts alone cannot prove omission closure. Final open count :1895–1905 intentionally uses the original whole-week predicate; do not silently add the prefinal coverage filter there.

Append the child completion only after the full original omission/outreach/notification work and final count, then leave QUERY_SYNC before returning. No child finish/clock. Bounded witness cap is atomic refusal, never truncation. Its raw stage digest excludes completion SHA (existing event digest includes root witness and would be circular). Root's final command event digest may bind completed root witness normally afterward.

## 5. Canonical comparison INSERT staging and honest NULL root

Add the narrow canonical comparison hook separately from current0017 incident hook. Only immutable INSERT is a new gate event; existing comparison UPDATE/DELETE append-only rules remain. Canonical tuple is exactly existing row `incident_id,revision_number,contract_id,issue_family,material_comparison_fingerprint` (plus immutable row identity owner_row_id); original fingerprints already bind actual comparison inputs. Do not snapshot whole comparison/prose/schedule/economic rows.

Comparison AFTER INSERT may read its **actual referenced parent incident** plainly to obtain Candidate/client/group/cycle/work-event scope. The genuine inserting owner must have already locked that parent FOR UPDATE or just created it in this same transaction (private compare1755 or existing1749; public QuerySync analogous real parent). Prehold exact Candidate KEY SHARE once the real scope is resolved and before its first material incident/comparison hook. New own parent incident and its original RI handling are exact; an unrelated guessed incident or trigger-time locking discovery is not permitted. No SELECT FOR UPDATE, fresh admission/module acquisition or clock occurs in the trigger.

Retain known `candidate_id` and **NULL `root_timesheet_id`**. Incident has no canonical-root column (current0017:867–869 already does this). Comparison's `candidate_timesheet_id` is signed Candidate evidence, not proof of a canonical financial Source root; do not pass it as root or query today's canonical Timesheet. A genuine preheld canonical-root proof can be used only if actual owner possesses it and its immutable applicability is separately established. Unknown root remains truthful; original canonical scope reader stays authoritative and acceptance must not discard Candidate-scoped events merely because the optional root is NULL.

Staging uses same parent producer, module epoch/current xid/actual ordinal through2357, old absent `{}`/new present finite tuple. Existing incident INSERT followed by comparison INSERT followed by incident pointer UPDATE are distinct meaningful ordered events; retain all three, including temporary null pointer, rather than coalescing away intermediate state. Exact duplicate tuples/no-op operational state changes create no event. Compare generation/request/auth/workflow/Timesheet writes outside19 remain unjournalled.

The comparison hook's finite applicable top owner list must cover actual registered public QuerySync and this private QUERY_SYNC child plus proven Source Final/Correct/Rows writer scopes when their real successors are built. Do not attach an unrestricted grant/list merely because those enum strings exist. An unsupported raw material NEXT writer remains the current honest finite refusal.

## 6. Prefinal child and remaining Source boundaries

Prefinal does not enter a second QUERY_SYNC frame; it validates an already-owned SOURCE_UPLOAD parent at entry, then each actual compare call pushes/leaves QUERY_SYNC under that parent. Retain original pure Office/profile/publication/coverage checks and exact signed-week census :1972–2016, including historical mapped coverage or actual signed schedule/effective policy. Retain signed Timesheet UUID order, same retained Timesheet hash, each child outcome and all distinct Candidate/client cohort restarts :2032–2040. Add only a bounded child-set scope digest/census if needed to prove the root's whole expected signed-week set; don't synthesize a root business receipt from these aggregate counts.

SOURCE_UPLOAD must itself enter before original admission/advisory :2823–2824 and cycle/report/publication locks, retain current/correction READY/frozen/replay semantics, and finish only after prefinal plus import outreach :3106 and genuine publication-fingerprint tail :3117–3127. Its current-publication rows are STATEFUL/currentness evidence, not automatically an immutable whole-root reply; a separate finite SourceUpload root completion type/source-member classification is still needed. Therefore prefinal hook support cannot by itself claim SourceUpload closure. Until a registered root exists, this private NEXT branch has truthful owner-not-installed protection, not a callback clock.

Final/Correct Final remain their actual existing kinds and real final CAS/manual-resolution/retained completion authority; no materialise root witness substitutes for them. Current0009 private manual_reviews_finalised requires their OPEN root; parent must close all true downstream reprojects/receipts/audit before finishing. Current07102026_2215 Rows is the final physical successor preserving same-file recovery; use that full body, not historical1534 projection-build. Its different resolved/source economic/expense/Contract callbacks and mutable same-file replay need separate true-tail witness design. No Source current/FROZEN/READY branch is promoted to CURRENT/CLEAR by this Candidate slice.

## Ordered next build

1. Add finite Candidate root/child completion schema, private immutable ACL/guards and genuine references before helpers; do not rewrite locked2153/2352/0020 or expand existing owner enums unnecessarily.
2. Add exact Candidate/child raw-census/completion digest and witness validators. Full successor of latest0017 witness assertion preserves manual/response/APP branches; add MAT/SELF root evidence and exact child checks. Current0002 finish/command/deferred assertion dispatch through that validator, so no alternate clock is added.
3. Add comparison INSERT stage helper/attachment and exact parent/Candidate scope protection in its real writers. Review all current comparison INSERT callers before broadening the attachment; unknown canonical root remains NULL.
4. Install full current materialise/self/privatecompare successors at their exact early-entry/replay/tail seams. Current0017 APP unchanged root still supplies immutable FINAL_SUBMIT witness and one command; nested materialise/compare only leave.
5. Install prefinal child only alongside a separately proved SOURCE_UPLOAD root, with full expected signed-week/child scope evidence. Keep other Source root paths explicitly pending or named unsupported before mutation, preserving stable non-NEXT behavior.
6. Update exact new signatures/final roots/physical closures/ACLs; source-review exact successor bytes and actual writer census. Root chooses/runs its own permitted checks. This lane has executed none.

## Source hashes

| Saved file | SHA256 |
|---|---|
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` |2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6|
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` |08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270|
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` |996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf|
| `supabase/repeatable/10102026_0002_banking_pay_next_query_metadata_owner_v1.sql` |a253e956f0f55ed3fc0e03dafbe97c690d800a05bf532f402c51c8e81641b759|
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` |d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1|
| `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql` |bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf|
| `supabase/repeatable/07102026_2215_weekly_source_same_file_recheck_recovery_v1.sql` |91972ea67ab5c4898ff513a970a4cb63fa8a333a599ebcee4634977662f58511|

Other inspected originals:15092026_1534 query_delivery complete-submission388–639 and resolved-decision4663–4686; upload_publication true parent2739–3127;07082026_2059 pure/locked session609–691;07082026_2310 component immutable guard430–503. These are source evidence, not executed behavioral proof.
