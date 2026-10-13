# F-25 QuerySync exact stage closure review

10 October 2026. **PASS — bounded source-only stage closure.** Fresh saved `supabase/repeatable/10102026_0047_banking_pay_next_query_sync_stage_closure_v1.sql`, 11,695 bytes; raw SHA-256 `d8f0e2f689eeb22ba480f039318a01b90187dbec5fbe098190f12428155b059e`. No product edits, SQL/runtime/tests/fixtures, hosted calls or business execution. Root reports separate source gate305/833; this review did not execute or independently claim that test result.

No actionable source defect identified in these three functions. PASS covers the exact certified-operation/raw-stage relation. Full0036 certificate semantic validation/insert guards and genuine whole QuerySync owner remain pending; this helper does not make those complete. No installation or behavioral pass is claimed.

## Expected matrix and operation matcher

`private.bpay_next_query_sync_expected_stages_v1(uuid)`5–20 derives finite actual operations from typed immutable per-input/omission certificates:

| Certified branch | Operation matrix / rank |
|---|---|
|NEW|INSERT_INCIDENT rank1, INSERT_COMPARISON rank2, SET_COMPARISON rank3|
|CHANGED|INSERT_COMPARISON rank2, SET_COMPARISON rank3|
|UNCHANGED_OPEN / UNCHANGED_RESOLVED|No expected stage|
|omission|RESOLVE_OMISSION rank1|

Counts exactly3*NEW+2*CHANGED+omissions. It uses UNION ALL intentionally: each certified operation is a distinct required obligation; duplicates cannot be hidden by union deduplication. Row identity is actual certificate incident/comparison UUID, work identity actual retained work UUID. Omission prior comparison equals actual retained current comparison. NEW prior comparison is NULL; CHANGED prior UUID is retained. Certificate constraints/semantics must independently validate those branch inputs.

`private.bpay_next_query_sync_stage_matches_v1(uuid,text,uuid,uuid,uuid)`22–51 requires known Candidate/noncanonical-root NULL and the actual raw event+q record, returns COALESCE(...,false) so missing event, missing comparison and nullable predicate failures cannot become acceptance:

*INSERT_INCIDENT: absent→present, OLD={} exactly, actual workUUID, NEW state OPEN and explicit JSON null currentcomparison key, NEW candidate text=known q.Candidate.
*INSERT_COMPARISON: absent→present/OLD={}, exact complete five-key NEW tuple reconstructed from genuine immutable comparison fields: incident/revision/Contract/family/material-fingerprint hex. SQL subselect missing comparison gives NULL then false. Contract nullable JSON null is retained. No current work/incident-state read.
*SET_COMPARISON: present→present, original work/OPEN→OPEN, OLD comparison matches nullable prior using IS NOT DISTINCT FROM, NEW comparison exact actual comparisonUUID. Removing only currentcomparison makes whole OLD/NEW canonical tuples equal. Candidate text equals q.Candidate; NEW candidate identity therefore equal via whole tuple comparison.
*RESOLVE_OMISSION: present→present, original work/OPEN→RESOLVED, retained comparison unchanged, removing only state makes entire OLD/NEW tuple equal, knownCandidate matches canonical tuple.
*Unknown operation returnsfalse.

The matcher itself does not take expected relation/row as arguments;84–91 surrounding bijection constrains actual e.owner_relation/owner_row_id to each expected relation/row before calling it. It is a private predicate rather than a standalone claim that a free event matches any caller-selected operation. Existing0017/0044 exact row hooks supply canonical raw tuples; later certificate-semantic checks bind actual comparison to certified incident/work/episode. This stage helper does not replace that different validation.

## Complete raw census and noncircular header

`private.bpay_next_query_sync_stages_assert_v1(uuid)`53–124 selects actual QUERY_SYNC completion and actual producer ID, obtains current0030 raw-stage digest, builds expected count and validates62–80:

*Actual count equals expected count and exact3/2/1 header formula, <=1000; header count/min/max/rawSHA exactly equals actual raw helper record using row IS DISTINCT FROM (NULL min/max on empty set handled correctly).
*Owner association total count equals raw count. LEFT JOIN exposes missing e/q instead of allowing inner-join digest to silently drop an association.
*Every owned stage is PAY_QUERY, exact producer xid/epoch, original_command_idNULL for this standalone metadata path, matching e/q relation and row, allowed incident/comparison relation, knownCandidate and root NULL.
*Recomputed OLD/NEW typed tupleSHA equals immutable raw stageSHA. Absent OLD/NEW must be{}, not arbitrary object. Required schema hashes/presence/tuples are nonnull; NULL bypass is not available.
*Every event in actual producer xid belongs to this producer and exactPAY_QUERY/epoch, excluding foreign/non-query/unowned stage outside the inner joined set. A q row has an event FK; a stranded original event is caught by this complete xid census.

Raw helper0030 explicitly excludes completion-witnessSHA from its domain/body, so computing/querying raw_stageSHA is noncircular. Existing event-command digest may bind the finished completion later. This helper reads immutable metadata and immutable original comparisons only; no Source admission/module/clock, row locks, current business-state requalification, writes, callback finish or audit/view side effect. It may be invoked later after FINISHED; it correctly does not demand current producer BUSINESS_OPEN/current module ACTIVE or absence of later metadata receipts.

`09102026_2244_banking_pay_next_input_transaction_v1.sql:33–34` requires transaction_ordinal1..1000 and UNIQUE(preparing_xid,ordinal), so distinctness of actual ordinals is schema-enforced. Same-xid epoch owner census makes that sufficient for these owned stages. Header min/max bind actual range; operation order is checked separately.

## Bijection, tuple continuity, Candidate equality and order

84–92 checks both directions with exact count=1:

1. Every certified expected operation has exactly one actual producer-owned event matching relation/row and operation facts.
2. Every actual producer-owned event has exactly one certified expected operation with relation/row/facts.

Thus plausible totals, duplicate/subset coverage and unrelated same-count mutations cannot pass. No entire raw stage is discarded. With zero expected/raw stages both directions are vacuous, while complete census/header empty-range/digest still must match; full owner no-event rules remain separate.

95–103 explicitly compares each NEW incident INSERT's exact NEW tuple with the same actual incident pointer UPDATE's OLD tuple using IS DISTINCT FROM. Bijection already uniquely qualifies insert/pointer; the join cannot silently avoid a missing operation because that failed beforehand. A NEW incident scope change between INSERT and pointer is rejected even if each stage would separately look plausible.

104–112 requires actual inserted comparison stage Candidate equal all incident stage Candidates for that certified NEW/CHANGED incident. Combined with per-incident candidate-text matches and NEW continuity, comparison insertion and incident mutations share actual knownCandidate scope. Root NULL is required throughout the complete census. This is original knownCandidate, not inferred from comparison's candidate_timesheet_id.

113–122 matches operation events and requires strict increasing transaction ordinals for every increasing-rank pair sharing workUUID. NEW insertion<comparison<pointer; CHANGED comparison<pointer. Comparison-before-incident or pointer-before-comparison cannot pass. There is no invented global ordering across unrelated works or native-sorted replacement of original textual work lock order. Omission has one stage, no artificial comparison insertion/order requirement. Cross-input/omission work disjointness and unique certificate work identity are0036 semantic/schema responsibilities; they remain pending and must be asserted in the final whole completion path before claiming business completion.

## Source semantics and supporting closure

Original public QuerySync remains full `15092026_1534_weekly_source_query_delivery_v1.sql:4691–5123`; original counters/new INSERT/comparison/pointer/omission loops justify exactly this matrix. The stage closure does not alter original body, resolved-decision predicate, omission selector, fingerprint, authentication/lock order, restart/outreach/audit or original result shape. Current0017 incident callback captures insert/pointer/state transitions, current0044 comparison AFTER INSERT captures one actual immutable new comparison. Actual final whole owner successor is pending.

Current0044 header comment was corrected to acknowledge stageCandidate FK's real preclock key-share work; original function body/declaration/trigger/ACL remain unchanged. Earlier0044 PASS remains bounded. Raw gate/stage creation itself occurs before clock under actual root/inheritedOPEN lifetime, not inside0047 assertion. This review does not widen SourceFinal/Correct/Rows/Upload owner admission or claim their true-root closure complete.

Full completion must separately require QUERY_SYNC producer/header/request/certificate digests, real branch comparison/incident/work/episode evidence, exact immutable original recheck/resolve decisions, complete input/omission sets/counters, actual private same-xid append-only insert guard, full original true-tail invocation and no-event finish. Validated stage mapping alone cannot certify that a forged/misbound certificate is a genuine completed original owner; do not expose these helpers as service RPC or substitute them for0036/full004x owner.

## ACL and final-definer registry

All three functions STABLE SECURITY INVOKER with fixed explicit search_path; expected/assert use pg_catalog/private, matcher additionally public for exact immutable comparison. Logical owner postgres and explicit execute revoke public/anon/authenticated/service_role125–130. None is exposed as service/browser SQL RPC or grants new table permissions.

Registry has exactly these three new signatures, all selecting `supabase/repeatable/10102026_0047_banking_pay_next_query_sync_stage_closure_v1.sql` as finalRoot/finalPhysical/allowedPhysical/finalClosures, no predecessor; owner F-25 exact QuerySync canonical operation-stage bijection:

*private.bpay_next_query_sync_expected_stages_v1(uuid)
*private.bpay_next_query_sync_stage_matches_v1(uuid,text,uuid,uuid,uuid)
*private.bpay_next_query_sync_stages_assert_v1(uuid)

No conflicting definition found in bounded current source. This is source registry consistency only; no gate/compile execution.

## Saved source provenance

| Source | Raw SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0047_banking_pay_next_query_sync_stage_closure_v1.sql` | `d8f0e2f689eeb22ba480f039318a01b90187dbec5fbe098190f12428155b059e` |
| `supabase/repeatable/10102026_0044_banking_pay_next_query_comparison_stage_v1.sql` | `b04ba86aeee9cf21900d02eb71f71bf1afc8864306f354ef32deddfbf8f4c989` |
| `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql` | `4be599c83fe18bb3b8761fecebb3e0a9785d2d69a716b83225787f8cc431dd54` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `supabase/migrations/09102026_2244_banking_pay_next_input_transaction_v1.sql` | `4fe014fe836d3474c18d77670788814754384a6fab7b889b557a6af55e0e2191` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `docs/bpay-next/design/F-25-query-sync-completion-build.md` | `e78e73c208b29aef2e12ae4531959e70d74e38846c92c6b1221ac42128a0714c` |
