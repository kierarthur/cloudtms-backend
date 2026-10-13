# F-25 Candidate typed protocol completion schema build

10 October 2026. Concrete source-only refinement of `F-25-candidate-materialise-completion-build.md`. No product/schema edit, SQL, runtime, test, fixture or hosted action was performed. Root alone implements. Suggested new names below are design APIs, not claims that objects exist.

Use five append-only private tables: one actual Materialise-call certificate, one true-root adjunct, one per-private-comparison-call header, and comparison input/omission certificates. Do not reuse0020's standalone public QuerySync header: its delivery shape and owner-kind constraints do not describe these owners. No new producer enum, clock, business receipt, universal command UUID or raw request/financial snapshot is needed. Keep the actual existing APP FINAL_SUBMIT receipt as its business authority.

## 1. `private.bpay_next_candidate_materialise_completion`

One actual Materialise invocation exists in each current direct MAT/SELF/APP original body. Key its certificate by `producer_id uuid primary key`, not an invented historical request receipt. This finite singleton is proved by the real current callers; do not generalize it to arbitrary multiple Materialise invocations without another explicit call scope. Fresh direct MAT gets this same certificate plus its root adjunct; nested MAT gets only this certificate and leaves. The certificate may represent an actual owned exact-last-replay branch without pretending that transaction generated the older comparison evidence.

Concrete columns:

```text
producer_id uuid PK; preparing_transaction_id xid8; module_epoch bigint >0
FK(producer_id, preparing_transaction_id, module_epoch) -> query_producer
caller_kind DIRECT | CANDIDATE_SELF_SUBMIT | CANDIDATE_APP_SUBMIT
variant FRESH | EXACT_LAST_REPLAY
request_key uuid; materialise_request_sha256 bytea32
account_id uuid -> candidate_app_accounts(id)
candidate_id uuid -> candidates(id); environment TEST | LIVE (existing type only)
workflow_id uuid -> candidate_submission_workflows(id)
expected_generation integer >0; result_generation integer >0
request_kind CHECK_HOURS | SUBMIT_TIMESHEET | SELF_SUBMIT
contract_id uuid -> contracts(id); contract_week_id uuid -> contract_weeks(id)
week_ending_date date
candidate_generation_id nullable uuid -> weekly_candidate_outreach_generations(id)
projection_publication_id nullable uuid -> weekly_source_projection_publications(id)
request_scope_id nullable uuid
submission_membership_id nullable uuid -> weekly_timesheet_submission_request_memberships(id)
timesheet_id uuid -> timesheets(timesheet_id); timesheet_revision integer >0
timesheet_hash bytea32; signature_copy_id uuid -> candidate_submission_components(id)
signature_content_sha256 nullable bytea32; immutable_submission_sha256 bytea32
result_sha256 bytea32
comparison_completion_id nullable uuid -> candidate_compare_completion(id)
comparison_completion_sha256 nullable bytea32
begin_event_ordinal integer 0..1000; end_event_ordinal integer 0..1000
actual_stage_count integer 0..1000; raw_stages_sha256 bytea32
completion_sha256 bytea32; completed_at_utc default transaction_timestamp()
```

`request_scope_id` is the original opaque request-scope UUID. **It is not universally a submission membership FK**: CHECK_HOURS uses a Candidate query scope; only SUBMIT_TIMESHEET obtains actual `v_membership.id` at2203:2717–2720. Keep the separate nullable actual membership reference. Compare ID/SHA are a strict null pair and may be present only for FRESH SUBMIT_TIMESHEET. FRESH SUBMIT_TIMESHEET requires exactly one current actual comparison certificate; FRESH CHECK_HOURS/SELF_SUBMIT and EXACT_LAST_REPLAY require none. SELF_SUBMIT's generation/publication/request scope/membership tuple is null under the original server-built request. Scope legality for other branches follows the real owner's original validations, not a guessed stronger generic check.

FRESH result generation is original `v_next_generation` and the actual result must equal the full original `v_result`; verify at the live owned tail, then retain scalar IDs/digests. Do not require `result_generation=expected_generation+1` in the EXACT_LAST_REPLAY branch: that exact key has already advanced the workflow. The replay returned JSON differs only by the original idempotent_replay change; hash the actual returned reply. No unique constraint on `(workflow_id,request_key)` here: a later owned race replay is a different protocol transaction, and this metadata is not a new historical business reply cache.

Capture existing `v_request_hash` unchanged from `WEEKLY_SOURCE_CANDIDATE_CHECK_MATERIALISATION_V1` at2287–2290. Capture immutable-submission SHA from the actual `_candidate_sha256_jsonb_v1` owner and signature content SHA from the real signature copy. Preserve nullable content if the real original owner allows it; never manufacture a hash. Source component immutable lineage/content checks may be revalidated later; today's component state or workflow/Timesheet hash/currentness must not become historical authority. Every business FK is inserted before the agency clock.

## 2. `private.bpay_next_candidate_root_completion`

Key `producer_id uuid primary key` with the same composite producer/xid/epoch FK. This is one true-root completion adjunct, including an APP receipt attachment; an attachment is not a second root/command.

```text
producer_id, preparing_transaction_id, module_epoch
owner_kind CANDIDATE_MATERIALISE | CANDIDATE_SELF_SUBMIT | CANDIDATE_APP_SUBMIT
variant DIRECT_MATERIALISE | SELF_SUBMIT | APP_RECEIPT_ATTACH
request_key uuid; root_request_sha256 bytea32; candidate_id uuid
materialise_completion_id nullable uuid -> candidate_materialise_completion(producer_id)
materialise_completion_sha256 nullable bytea32
app_receipt_id nullable uuid -> weekly_candidate_app_mutation_receipts(id)
app_submission_kind nullable RESPONSES_ONLY | WHOLE_WEEK_REVISION
app_response_request_key nullable uuid
app_response_item_count integer >=0; app_response_set_sha256 bytea32
comparison_child_count integer 0..1000; comparison_children_sha256 bytea32
actual_stage_count integer 0..1000; first_stage_ordinal nullable integer 1..1000
last_stage_ordinal nullable integer 1..1000; raw_stages_sha256 bytea32
result_sha256 bytea32; completion_sha256 bytea32
completed_at_utc default transaction_timestamp()
```

Owner/variant are exact pairs. DIRECT and SELF require their actual singleton Materialise certificate for the same producer, no APP receipt/response fields, and exact root/child request-key/hash equality using the real direct or server-built SELF delegated request. SELF also requires original request_kind SELF_SUBMIT, no compare certificate and zero actual Query stages; retain original Timesheet/workflow writes normally. DIRECT CHECK_HOURS likewise has no private comparison; SUBMIT_TIMESHEET binds its actual one child. Any genuinely unexpected canonical callback must be classified/proved or refused; do not silently make SELF create comparison/CLEAR evidence.

APP requires the actual FINAL_SUBMIT receipt, its exact UUID request key/root request hash/Candidate and original full response SHA. WHOLE_WEEK_REVISION has exactly the one original MAT invocation; RESPONSES_ONLY has none. The actual original APP CHECK_HOURS branch invokes CandidateResponse after MAT at0017:1113–1119, using the same real request UUID. Preserve that original edge. Capture real response-set count/SHA from those immutable original response events and conditional resolution pairs using current0017's exact request metadata; do not classify all APP stages as comparator stages. No response body, contact, credential or original request JSON is retained. Empty actual response work gets an explicit domain empty-set digest.

Bind the new adjunct cryptographically without changing the existing business receipt or result: proposed private APP completion SHA is `SHA(domain, producerUUID, receiptUUID, originalResponseSHA, adjunctSHA)`. Keep the actual receipt UUID as `p_witness_id`; in a full successor of the current witness assertion retain the existing receipt kind/key/request/Candidate proof, compare its responseSHA with the adjunct's actual resultSHA, then require this new private composite witness SHA. Full APP owner calls finish with that composite only at its real final receipt tail. This changes protocol metadata, not the API/result/receipt request hash. Do not silently accept a missing adjunct for newly authored owners; any historical protocol-version compatibility needs an explicit retained version proof rather than a permissive fallback.

If root prefers retaining the prior private APP response-only witness SHA, it must still explicitly certify the same immutable adjunct and complete child/stage census; a mere same-Candidate check is insufficient to prove those new children. The composite form gives one unambiguous new protocol authority.

## 3. `private.bpay_next_candidate_compare_completion`

This is a subordinate immutable certificate, not a QUERY_SYNC root or clock. Header columns:

```text
id uuid PK (private call identity); producer_id, preparing_transaction_id, module_epoch
composite producer FK; unique(producer_id,call_ordinal); unique(id,producer_id)
call_ordinal integer 1..1000; caller_kind MATERIALISE | PREFINAL
call_request_sha256 bytea32
candidate_generation_id nullable uuid; publication_id uuid; membership_id nullable uuid
timesheet_id uuid; timesheet_revision integer >0; timesheet_hash bytea32
call_now_utc nullable timestamptz (actual existing argument)
candidate_id uuid; client_id uuid; contract_id uuid; week_ending_date date
source_group_id uuid; source_cycle_id uuid; profile_code text (actual original profile)
generated_issue_count integer 0..1000; generated_work_ids uuid[]
generated_issues_sha256 bytea32
omitted_count integer 0..1000; omitted_incident_ids uuid[]
new_count,changed_count,unchanged_count,resolved_count integer 0..1000
open_issue_count integer >=0; outcome MATCHED | ISSUES; result_sha256 bytea32
input_completions_sha256 bytea32; omission_completions_sha256 bytea32
original_witness_count integer 0..4000; original_witnesses_sha256 bytea32
begin_event_ordinal integer 0..1000; end_event_ordinal integer 0..1000
actual_stage_count integer 0..1000; raw_stages_sha256 bytea32
completion_sha256 bytea32; completed_at_utc default transaction_timestamp()
```

Use genuine existing FK targets for the typed business IDs; all FK checks occur before clock. Generation/membership are both nonnull for the original Materialise SUBMIT_TIMESHEET call and both null for the original prefinal null-generation call. The latter constructs local generation/membership scope values inside the original body; retain actual Candidate/client/Contract/week scalars, without claiming invented persisted generation/membership IDs.

`new+changed+unchanged=generated_issue_count`, `resolved=omitted_count`, MATCHED iff open_issue_count0. Do not cap open_issue_count at1000: original1895–1905 counts the real whole-week open population, not merely this generated set or prefinal coverage. Typed arrays must be one-dimensional/lower-bound1 when nonempty, nonnull, native-UUID sorted unique, exact cardinalities; empty sets have explicit domain digests. The actual generated IDs/issue SHAs and original `v_seen_work_events` must match at true insertion. Retain only one proved equal set, not redundant copied raw issues. The actual original omission selector's captured pre-update expected set must equal the inserted omission rows. A hash by itself does not prove that live selector ran.

`call_request_sha256` binds domain, actual six original call operands (including supplied now), actual caller kind and call ordinal. Root/caller request SHA remains distinct. Header resultSHA binds the full unchanged private comparison return; counts/outcome are checked against that exact original return at the true child tail. Private comparison result does not gain a certificate field.

Current MATERIALISE calls compare at most once, so its exact per-producer call ordinal is1. The caller binds its child by `(producer_id,call_ordinal=1)` plus exact original generation/publication/membership/Timesheet/hash/result; no latest-row search. Private compare allocates its private UUID locally and inserts this exact ordinal at completion. More than one certificate for this current MAT scope is a named refusal. For future Source prefinal, the real signed-week loop assigns ordinal from its own explicit counter and passes it through a typed private scoped overload; do not discover it with MAX/latest. Preserve the original six-argument private interface/unchanged return; add a finite internal overload/core carrying `p_protocol_call_ordinal integer` only when that actual Source root is built. Unregistered direct NEXT private invocation refuses before its first business lock.

## 4–5. Typed comparison input and omission certificates

`private.bpay_next_candidate_compare_input_completion` mirrors the complete current0036 input shape, replacing root completion FK with `(compare_completion_id,producer_id)` FK to the compare header:

```text
compare_completion_id uuid; producer_id uuid; work_event_id uuid
outcome NEW | CHANGED | UNCHANGED_OPEN | UNCHANGED_RESOLVED
incident_id uuid; issue_episode integer >=1; comparison_revision_id uuid
prior_comparison_revision_id nullable uuid; recheck_event_id nullable uuid
resolved_decision_event_id nullable uuid
generated_issue_sha256 bytea32; matched_facts_sha256 nullable bytea32
evidence_sha256 bytea32
PK(compare_completion_id,work_event_id); unique(compare_completion_id,incident_id)
```

Retain0036's exact four branch null/presence conditions and real comparison/event/work/incident FKs. Generated issues at2203:1695–1712 use the same15 business keys accepted by0038; missing optional expected fingerprint hashes as null. Reuse0038's complete typed issue digest; do not substitute a material comparison fingerprint for complete generated issue identity. Historical resolved requires the narrow matched-facts SHA recomputed from the actual old immutable comparison and genuine0039-qualified historical decision. Today's current incident pointer/lifecycle is not reread later.

`private.bpay_next_candidate_compare_omission_completion` mirrors current0036 omission columns/FKs: compare ID+producer, incident, work, episode, actual comparison, actual resolved event, evidenceSHA; PK(compare,incident), unique(compare,work), unique(compare,resolvedEvent). Omitted work cannot overlap generated work.

No separate physical original-witness table is required for this child slice: all actual IDs already have FKs in the two certificate tables. Build exact0051-style UNION DISTINCT of comparisons, CHANGED priors, real rechecks, historical decisions and omission events; retain its count/ordered SHA in the child header and recompute it later. Every recheck and omission must additionally bind original `origin=CANDIDATE_TIMESHEET_SUBMISSION` from2203:1841/1888. Preserve genuine historical/unchanged comparison publication; new child recheck/omission events bind actual current call publication.0030's existing event digest omits origin, so add a narrow Candidate event digest suffix over the actual immutable origin field, rather than silently claiming0030 hashes it. No invented event/RETURNING fallback.

## Concrete helper/API and insertion order

Suggested private helpers (SECURITY INVOKER, explicit paths, revoked public/anon/authenticated/service_role execute):

```sql
bpay_next_query_raw_stages_range_v1(p_producer uuid,p_begin integer,p_end integer)
  returns table(actual_stage_count bigint,first_ordinal bigint,last_ordinal bigint,raw_sha256 bytea);
bpay_next_candidate_compare_save_v1(
 p_header private.bpay_next_candidate_compare_completion,
 p_seen_work_ids uuid[],
 p_outcomes text[],p_work_ids uuid[],p_issue_sha256 bytea[],
 p_incident_ids uuid[],p_episodes integer[],p_comparison_ids uuid[],
 p_prior_ids uuid[],p_recheck_ids uuid[],p_decision_ids uuid[],p_matched_sha256 bytea[],
 p_expected_omitted_ids uuid[],p_omitted_ids uuid[],p_omitted_work_ids uuid[],
 p_omitted_episodes integer[],p_omitted_comparison_ids uuid[],p_omitted_event_ids uuid[],
 p_original_result jsonb) returns bytea;
bpay_next_candidate_materialise_save_v1(
 p_certificate private.bpay_next_candidate_materialise_completion,
 p_original_result jsonb) returns bytea;
bpay_next_candidate_root_save_v1(
 p_header private.bpay_next_candidate_root_completion) returns bytea;
bpay_next_candidate_compare_evidence_assert_v1(p_compare_id uuid) returns void;
bpay_next_candidate_compare_expected_stages_v1(p_compare_id uuid)
 returns table(operation text,owner_relation text,owner_row_id uuid,work_event_id uuid,
               comparison_id uuid,prior_comparison_id uuid,stage_rank integer);
bpay_next_candidate_compare_expected_witnesses_v1(p_compare_id uuid)
 returns table(witness_ordinal bigint,witness_kind text,comparison_revision_id uuid,
               discrepancy_event_id uuid,evidence_sha256 bytea);
bpay_next_candidate_root_assert_v1(p_producer uuid,p_witness_id uuid,p_witness_sha256 bytea) returns void;
```

Composite parameters are finite typed operands; JSON in the two save helpers is the actual transient existing return, parsed/hashed only and never persisted. Add pure typed row/set/root digests analogous to0043. Array inputs require exact aligned cardinalities/branch-null rules; sort paired rows by work UUID, never independent arrays.

Insert immutable compare header **first**, using local complete header/expected child digests computed from the actual aligned locals; then its typed input/omission children; then assert the completed exact set before leaving QUERY_SYNC. This makes every immediate child FK real without deferred RI work. Do not insert placeholders and UPDATE immutable headers. The current-xid insertion guard allows child rows only for that genuine OPEN, BUSINESS_OPEN header/protocol scope; before returning, the helper verifies the complete set/SHAs and actual child result. Parent MAT inserts later and immediately references the already-existing child. Root/APP adjunct is inserted last and references already-existing MAT/APP business receipt. No reverse child-to-not-yet-existing-MAT FK, no deferred constraint trick and no unproved late FK discovery.

Record `begin_event_ordinal` from the actual owned input transaction's next_event_ordinal at child entry before its first business lock; record end at the actual final1905 count/result tail. Exact range is `(begin,end]`; empty has begin=end, count0, null first/last and domain empty rawSHA. Use0030-style raw event digests excluding completion witness SHA, so no circular hash. Child stages are the genuine incident INSERT, comparison INSERT, incident pointer UPDATE and omission resolution; verify0047-style exact bijection/formula `3*NEW+2*CHANGED+omissions`, adapted to this actual range/origin. Whole-root raw proof includes all actual owned stages and no gaps/foreign producer rows.

Full MAT source retains pure authority/shape/hash and exact scoped last-only replay before entry, then early entry before2291 advisory; original locked replay remains. Fresh cert is inserted only after final workflow response UPDATE2747–2748. For an outer pure replay, return original reply with no new producer/certificate. For an already-owned nested replay or locked-race replay, retain a zero-stage actual-call certificate for the true parent's returned result, no compare certificate, and leave; do not fabricate earlier stage generation. This refines the prior design's simple nested-leave branch so SELF/APP can bind the actual replay operand without a mutable future cache reread.

Full SELF first uses pure session context, actual normalized delegated request/hash and replay; enters before true session lock, revalidates actual account/Candidate, calls original MAT, inserts SELF adjunct after its actual child result and finishes once. Full APP remains at original receipt tail; child MAT/compare only leave. Preserve signature/workflow/result/deferred pay-summary behavior, request hashes, original comparison algorithm, exact omission/outreach/notification tail and unchanged original response JSON.

## Final assertion, finite boundaries and real blockers

The full successor of latest completion_witness_assert must retain manual/response/System/publicSync/APP predecessors in their actual master order. Add MAT/SELF root dispatch to the private root assertion; augment APP with actual receipt+adjunct proof. Recompute root/MAT/child/input/omission/result/witness digests, exact child census and whole raw-stage census. Child ranges cannot overlap or leave material comparator stages unclassified. APP's real CandidateResponse stages use its original immutable request metadata/conditional resolution evidence separately; preserve that owner proof and classify the union exactly rather than accepting arbitrary same-Candidate events.

Fresh insertion guards require current xid, same epoch, input OPEN, producer BUSINESS_OPEN, actual legal root/top stack and no root completion witness/clock. Completion rows are logical owner postgres, private RLS/revoked direct writes; existing append-only UPDATE/DELETE/TRUNCATE guards. Later retained-evidence assertion verifies immutable IDs/certificates and producer linkage, without requiring current module ACTIVE, current workflow generation/state, current Timesheet hash, current membership/publication or re-running historical decision/omission predicates.

There is no intrinsic blocker to Candidate direct MAT/SELF plus actual APP/MAT child certification. Source prefinal has a real dependency: its true SOURCE_UPLOAD completion is not built by this Candidate slice. A Source prefinal child set cannot certify that parent or finish a clock. Keep that NEXT branch named owner-not-installed before locks until the actual Source root, signed-week expected-set/call-ordinal census and full import-outreach tail exist. Final/Correct Final/current2215 Rows remain separate scopes. New metadata definitions are design proposals requiring source review and root's permitted compiler checks; this note asserts none passed.

## Saved source hashes

| Source | Raw SHA-256 |
|---|---|
| `docs/bpay-next/design/F-25-candidate-materialise-completion-build.md` | `b896c4af1c8c55a822b2d6ef07ca28a6684dd494c47d8600364fb71a187eb7cf` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| `supabase/repeatable/10102026_0002_banking_pay_next_query_metadata_owner_v1.sql` | `a253e956f0f55ed3fc0e03dafbe97c690d800a05bf532f402c51c8e81641b759` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb` |
| `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql` | `f5847e3e3fb28a8745d3ee8c29d0dd7dc09740d7c23627b380f358f762c985af` |
| `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql` | `bd9920f0440406a7710f713391db25646ebc475fa906d7faac6fad03aa133768` |
