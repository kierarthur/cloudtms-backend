# F-25 QuerySync completion guards review

10 October 2026. **PASS — bounded source-only guards and immutable assertions.** Reviewed fresh saved `supabase/repeatable/10102026_0058_banking_pay_next_query_sync_completion_guards_v1.sql`, 16,977 bytes, raw SHA-256 `8027ed7810e0345b4370a897551ddca3fae0868119728253ab9fa4abe5b09ce8`. No product edits, SQL/runtime/tests/fixtures, hosted calls or business execution. No installation/behavioral pass is claimed. Actual full QuerySync writer/private save helper remain pending separate successors; these primitives do not yet certify a complete real owner execution.

No actionable defect identified in fresh0058 within that boundary. Reviewed full four functions and both trigger attachments against saved0032/0036/0043/0047/0051/0057, original1534 QuerySync and current final-definer registry. Explicit NEW named-composite casts are present for both digest paths.

## Same-xid preclock typed admission

`private.bpay_next_query_sync_certificate_insert_guard_v1()`5–48 is the shared BEFORE INSERT callback for exact input and omission private tables182–186. It obtains actual producer by NEW.completion_id and actual immutable header.14–22 requires:

*actual pg_current_xact_id(), producer BUSINESS_OPEN/mode STANDALONE_QUERY/root owner QUERY_SYNC/exact stack ARRAY['QUERY_SYNC']; no previously assigned completion witness;
*header xid/epoch/kind/variant/requestSHA exactly equal original producer using row IS DISTINCT FROM;
*exact input transaction epoch and phase OPEN;
*plain current module NEXT/epoch matching stable originally owned epoch.

Missing header/producer is INTO STRICT failure. This is not a service RPC; private schemas/table/function denies prevent external clients from authoring certificates. Generic trigger callback is attached only BEFORE INSERT to the two intended tables; original0036 immutable UPDATE/DELETE/TRUNCATE guards remain intact.

25–32 plain preclock business scope checks bind actual source-cycle group, actual incident workUUID/issue episode/current comparison/group and actual comparison incidentUUID to NEW record, plus genuine work first-source-group/Candidate/Client consistency. These are source-only SELECTs, not explicit row/advisory/module locks. Real true owner must already hold/create original rows; record and original FKs remain actual preclock key-share work. No late clock or root discovery occurs in this admission callback.

Input33–38 requires header input membership, actual state OPEN for NEW/CHANGED/UNCHANGED_OPEN or RESOLVED for UNCHANGED_RESOLVED, correct typed digest and append count below exact header input count. `NEW::private.bpay_next_query_sync_input_completion` is explicit, so rowtype-specific digest receives correct named composite. Omission39–44 requires exact header omitted membership, work excluded from input set, actual state RESOLVED/resolution_kind SOURCE_MATCHED, correct typed omission digest via explicit NEW named-composite cast, and count below header omission count.

0036 schema supplies required UUID/episode32-byte hashes, finite outcome enum, explicit branch-null shape, unique completion/work and completion/incident, retained actual original IDs, input/omission FKs, RLS/browser/service deny and append-only guards. Header0020 provides nonnull finite arrays with no NULL entries and<=1000 counts. Guard membership tests therefore do not admit a nullable empty success: empty sets reject an attempted insertion; required null row IDs either fail strict actual lookup or row IS DISTINCT FROM before admission. Header count0 correctly permits zero child insertions, not an arbitrary child.

The callback verifies admission/current scope and typed digest; it does not replace the original parsed issue predicate or whole final evidence assertions. Real writer/save still must check original latest-match selection, input full/narrow hashes, original expected/actual omissions and genuine full true tail. No assumption that an arbitrary digest reconstructs raw input JSON is introduced.

## QuerySync historical immutable whole closure

`private.bpay_next_query_sync_root_completion_assert_v1(uuid)`50–82 checks header↔producer xid/epoch/QUERY_SYNC kind+variant/requestSHA; standalone mode/exact root stack; exact causeKind/key `QUERY_SYNC:<original request SHA hex>`; child_actionSHA NULL; exact existing input/omitted identity digest domains and revised0043 completionSHA. It calls full0051 certificate evidence assertion and0047 raw stage closure before accepting actual witness set.

Unlike INSERT callback, historical assertion deliberately does not require today's module NEXT, producer current xid/BUSINESS_OPEN, or current Source/work/incident/timesheet/publication lifecycle. It reads private retained completion/producer/certificates/stages and genuine immutable original comparisons/events. This permits valid old-epoch FINISHED completion revalidation. Typed epoch/key/digests remain bound to original producer; it does not reinterpret old evidence as today's active business state.

0051 binds sorted native workUUID+issueSHA input set, exact omitted incident set and outcome counts to header; recomputes typed child/row/requestSHA; excludes input/omission overlap; binds comparison UUID to actual incident; NEW revision1, actual CHANGED prior/new immutable comparison identity/fingerprint/order; genuine SOURCE_RECHECKED exact episode/pub/fingerprint/SYSTEM/key/payload; UNCHANGED_RESOLVED matched-facts SHA and0039 real original decision/pair; genuine SOURCE_MATCHED resolution episode/pub/key/NULL fingerprint/SYSTEM.0039 does not invent an event or force historical event publication to today's Sync/comparison publication.

0047 verifies full raw ownership census/hash/range, exact3NEW/2CHANGED/0unchanged/1omission operation matrix, two-way count1 bijection, knownCandidate/NULLroot, absent{}, NEW insert→pointer OLD continuity, Candidate equality across actual comparison and incident stages, strict per-work operation ordinal order. It excludes unrelated non-query/epoch/owner events and Source original-command stages. Noncircular0030 raw digest excludes completion witness fields;0043 child digests and query-only completion wrapper bind actual durable typed records.

No lifecycle fields used by25–44 preclock admission are reread by0058 historical assert or0051/0047. A later incident comparison/head/lifecycle change does not invalidate a genuine old immutable completion merely because today's current state differs. Recorded workUUID/episode/scope were certified at preclock admission; later assertion checks retained immutable relationships.

## Exact complete witness set, bounded4000

70–80 independently aggregates actual header witness children and exact0051 expected UNION. Both aggregate original `(ordinal,kind,comparisonUUID,eventUUID,evidenceSHA)` sorted by ordinal, with canonical[] for empty set. Exact counts/digests must equal each other and header original_witness_count/SHA; every actual child evidenceSHA must match current0030 original-evidence digest. Expected set comprises every input comparison, actual CHANGED prior, omission comparison, real rechecks, genuine historical resolved decisions and actual omission resolutions. UNION deduplicates only the exact kind/originalUUID identity; row_number gives deterministic finite kind/nativeUUID order and contiguous ordinals.

Thus omitted/extra/reordered/arbitrary-event witnesses cannot be hidden by plausible counts or a publication-wide union. It does not replace actual immutable parent response with generic JSON. Original evidence relationships are qualified before expected manifest assembly.

Expected count<=4000 matches additive schema `10102026_0057_banking_pay_next_query_witness_envelope_v1.sql`: original witness count0..4000 and ordinal1..4000. Historical-resolved1000 inputs use2000 comparison/decision witnesses;1000 distinct omissions use2000 comparison/resolution witnesses and1000 actual stages. Both input/omission<=1000 and canonical-stage<=1000 remain unchanged; this is evidence capacity only, no extra selected work/stage/business operation or truncation. System Hours exact2*N remains schema-required and unchanged.

## Dispatch preservation verified by exact source diff

`private.bpay_next_query_root_completion_assert_v1(uuid)`84–124 was compared function-declaration/body text against current0032 counterpart. The **only** difference is inserted92–93 QUERY_SYNC→new typed assertion/RETURN. Existing System Hours branch entire body/config/declaration is unchanged: original raw stages/header/request/child action/variant/cause/selection/digests/exact Office pair witness validation retained.0043 root digest intentionally returns unchanged base digest for System owner and wraps query child digests only for QUERY_SYNC;0036 optional query header fields are schema-NULL for System. New query certificate triggers attach only new query child tables and do not intercept System path.

`private.bpay_next_query_completion_witness_assert_v1(uuid,uuid,bytea)`127–171 was compared against full0032 current dispatch. The **only** difference is System branch selector changed from owned.owner_kind='SYSTEM_HOURS_ACCEPT' to IN('SYSTEM_HOURS_ACCEPT','QUERY_SYNC'). Entire original manual, manager/Candidate response and Candidate App branches are text-identical, including config/signature:

*manual immutable command UUID/request/kind/resultSHA and actual manual stage scope;
*response exact original request key and original typed response completion digest;
*App immutable FINAL_SUBMIT receipt UUID/idempotency/request/resultSHA and actual Candidate scope;
*existing unsupported-owner refusal retained.

New query dispatch requires witnessUUID=actual producerUUID and an actual immutable header with matching completionSHA, then routes to complete root assertion. It does not widen manager/Candidate business reply, recreate command UUIDs, change accepted transport response, introduce a second agency clock, or mark all Source owner kinds installed. Shared error text `BPAY_NEXT_SYSTEM_COMPLETION_WITNESS_UNBOUND` remains unchanged; its name is not a semantic dispatch defect.

## Private declarations, triggers and physical registry

All four function declarations retain fixed explicit search_path and SECURITY INVOKER; historical assertions STABLE, trigger function normal PLPGSQL trigger volatility. They contain no agency receive/finalize/clock function call. Certificate INSERT trigger itself returns typed NEW only; it does not write original business rows/audit/view state. It performs counts and reads while input OPEN, before clock.

173–180 exact logical owner postgres and EXECUTE revoke public/anon/authenticated/service_role. Both private child tables already deny direct clients via0036 RLS/ACL. Two task-named BEFORE INSERT triggers are replaced/attached only on intended input/omission relations; append-only source triggers preserved. Notification is release SQL declaration, not executed by this review.

Current final-definer registry maps all four exact signatures to0058 finalRoot/finalPhysical/allowedPhysical/finalClosures. New callback/query assertion have no predecessor; generic root assertion carries0032 predecessor; witness dispatcher carries0002→0017→0032→0058 complete chain. Historical definitions remain source predecessors, not selected final bodies. No duplicate new function physical definition found.

## Pending real owner obligations

PASS is finite completion admission/immutable assertion and preservation of prior dispatch only. Private actual save helper and full public QuerySync successor must still own true entry before original first lock, full unchanged auth/shape/live checks, original resolved-match decision order, full issue/outcome collection, exact unchanged original omitted selector, genuine new/changed/unchanged/resolve IDs, noncircular digest precompute and one immutable header insert, all certificate/witness FK work before clock, original complete restart/outreach/audit tail, final assertion, root finish or nested leave, and original API response/no-event behavior.0020 root header insertion is not itself a genuine whole-owner completion; true source helper must require correct same-xid preclock root and reach original5116 tail. Unsupported broader Source publication roots remain pending.

Only full writer can certify that input_issueSHA and narrower matched-factsSHA were computed from actual parsed original issue and original nine-fact equality predicate. Historical assertion validates durable digests/original evidence, not reconstruction of omitted raw JSON or current financial snapshot. Genuine zero-stage full action can retain private completion but must use existing0002 finish empty branch before agency command receive;0058 alone does not execute that action.

## Saved source provenance

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0058_banking_pay_next_query_sync_completion_guards_v1.sql` | `8027ed7810e0345b4370a897551ddca3fae0868119728253ab9fa4abe5b09ce8` |
| `supabase/repeatable/10102026_0032_banking_pay_next_query_system_hours_owners_v1.sql` | `ab774f951ba157ad7705f31e461db145d4a9ff2ca4d6155e58a87853814a44f1` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb` |
| `supabase/repeatable/10102026_0043_banking_pay_next_query_sync_completion_digests_v1.sql` | `79e3787728d2550a423f9552cd3b4e3b670c292e4715607b0a792ee9ff9f77eb` |
| `supabase/repeatable/10102026_0047_banking_pay_next_query_sync_stage_closure_v1.sql` | `d8f0e2f689eeb22ba480f039318a01b90187dbec5fbe098190f12428155b059e` |
| `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql` | `bd9920f0440406a7710f713391db25646ebc475fa906d7faac6fad03aa133768` |
| `supabase/migrations/10102026_0057_banking_pay_next_query_witness_envelope_v1.sql` | `121eafa88611f7bd79c24ee5d2325f1077b6afa43d7e4ceec57a92323695b958` |
| `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql` | `f5847e3e3fb28a8745d3ee8c29d0dd7dc09740d7c23627b380f358f762c985af` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
