# F-25 QuerySync finite true-tail completion build

10 October 2026. Read-only source design. Root alone writes product; no SQL/runtime/tests/fixtures or hosted calls were executed. Status: complete bounded source design; typed durable per-input/omission certificate refinement and exact SQL fragments included.

## Concrete current source and bounded owner

Preserve the entire original `public.weekly_source_query_sync_atomic_v1(jsonb)` declaration/body at `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql:4691–5123`, including service auth, exact parsing/errors, publication/profile/report scope, both Office permission calls, cycle/work/incident locks, fingerprint owner, signed timesheet/source link/policy checks, counters, omission predicate, cohort restart/outreach/audit and unchanged response. The original has no root request UUID or immutable whole-Sync reply; use actual private producer UUID only as protocol completion identity. It has no existing `semanticHash` to replace. Preserve `private.weekly_source_query_comparison_fingerprint_v1` and the original manager/Candidate semantic hashes; add a separate domain-qualified QuerySync producer request hash over complete parsed input.

Current supporting schema is `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql`. Current helpers are `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql`: raw-stages `(uuid)` at7, original-evidence `(text,uuid,uuid)` at23, typed completion-row digest at63. The current completion assertion remains response/manual-only in `10102026_0017_banking_pay_next_query_response_owners_v1.sql`; root must add the finite QUERY_SYNC branch in an ordered successor, retaining the complete current assertion body.

## Exact integration seams

1. Pure header/publication/profile/report/Office authority remain4737–4770. Before producer entry, parse the entire issue array using the existing allowed-key list4785–4790 and exact existing casts/kind/hash errors, reject duplicate/non-UUID/null work IDs, and retain typed sorted input IDs. Do not move any live signed timesheet, link, policy or incident check out of its original loop. The producer request digest binds actor/cycle/publication and every allowed issue field; no token, prose, contact or money fields. UUID ordering for retained sets is native `ORDER BY uuid`, irrespective of the original body loop's unchanged `ORDER BY value->>'work_event_id'` text lock order.
2. Replace original single admission4771 with actual `producer_enter_v1('QUERY_SYNC','QUERY_SYNC', 'QUERY_SYNC:'||encode(request_sha,'hex'),request_sha,'STANDALONE_QUERY')`. It owns original Source admission/module SHARE before cycle lock4772. Keep locked publication/auth rechecks4773–4775 exactly. A null token means unchanged non-NEXT original behavior. Do not take another late Source/module admission inside an inherited producer.
3. Keep each original loop statement. Append typed completion identities to private local arrays at the original branch seams described below. Neither comparison/event identity discovery nor row hooks take a clock.
4. Omission selector5006–5064 must remain literally unchanged. At each selected row, append `incident.id` to expected omitted IDs before its existing UPDATE5066. After exact original RESOLVED insert5081–5089, select the real event by unique `(event_kind,idempotency_key)` and verify it; append actual omitted incident/event/comparison IDs. Do not rerun the predicate after changing state to RESOLVED: it would return no longer-OPEN rows and conceal the original expected set.
5. Finish only after full5093–5116 restart/outreach/audit tail. At this point, while producer BUSINESS_OPEN and input OPEN, insert the typed completion header/witnesses, all original evidence FKs and digest. Then root `finish_standalone_v1(producer,'QUERY_SYNC',producer,completionSHA)`. Nested real owners only `leave_v1(producer,'QUERY_SYNC')`; do not insert a root completion whose root kind is not QUERY_SYNC. No saved SQL caller of this public Sync exists in the bounded census; generic allowed stack edges do not prove a different private Candidate comparison helper is this function.
6. Preserve original5117 response and result counter values exactly. Original zero material-event finish creates no agency command, no financial job and no input receipt; the private completion can still certify the actual no-event tail.

## Typed local arrays and helper signature

Use parallel arrays, never a replacement generic caller JSON object. All arrays are private locals assembled by the genuine original owner; arrays are checked one-dimensional/lower-bound1, no null mandatory entries, native-UUID sorted distinct sets and exact equal lengths. Outcome fields allow only NEW / CHANGED / UNCHANGED_OPEN / UNCHANGED_RESOLVED. Nullable parallel witness IDs have explicit branch-specific constraints.

```sql
private.bpay_next_query_sync_completion_save_v1(
 p_producer_id uuid,
 p_actor_user_id uuid, p_source_cycle_id uuid, p_projection_publication_id uuid,
 p_input_work_event_ids uuid[],       -- pure complete sorted requested set
 p_seen_work_event_ids uuid[],        -- exact original v_seen_work_events, sorted in helper
 p_outcomes text[],                  -- aligned with p_seen work IDs, not independently sorted
 p_incident_ids uuid[], p_comparison_ids uuid[],
 p_prior_comparison_ids uuid[],      -- CHANGED only; actual old v_comparison.id
 p_recheck_event_ids uuid[],         -- NEW/CHANGED only, actual unique event
 p_resolved_decision_event_ids uuid[],-- UNCHANGED_RESOLVED only, actual historical RESOLVED
 p_expected_omitted_incident_ids uuid[],
 p_actual_omitted_incident_ids uuid[],
 p_omitted_comparison_ids uuid[], p_omitted_resolve_event_ids uuid[],
 p_new_incidents integer, p_changed_comparisons integer,
 p_unchanged_incidents integer, p_resolved_incidents integer
) returns bytea
```

Proposed helper is private `VOLATILE SECURITY INVOKER` with explicit search_path and revoke execute from public/anon/authenticated/service_role; public original security-definer owner calls it. It asserts current `pg_current_xact_id()` producer/epoch/input phase, owner_kind=cause_kind=QUERY_SYNC, mode=STANDALONE_QUERY, phase=BUSINESS_OPEN, owner_stack=ARRAY['QUERY_SYNC'], cause key/request hash exact, and completion absent. It may lock only its already-owned private producer/input rows. Every business read/FK insertion is before finish/agency clock; later witness assertion only rechecks immutable retained rows and private stages.

Per-input branch data:

| Original seam | Outcome / exact evidence | Required counter |
|---|---|---|
|4882–4884|UNCHANGED_RESOLVED: latest incident under exact4668–4686 predicate; matched immutable comparison ID plus actual historical RESOLVED event ID; no fabricated SOURCE_RECHECKED.|unchanged++|
|4891–4916 then4968–5002|NEW: real inserted incident and comparison plus exact SOURCE_RECHECKED event. Prior comparison NULL.|new++|
|4945–4950 then4968–5002|CHANGED: real old comparison before replacement, real new comparison, exact SOURCE_RECHECKED. Old/new fingerprints differ.|changed++|
|4961–4963|UNCHANGED_OPEN: existing real current comparison, equal original material fingerprint. Recheck/resolved/prior IDs NULL. No fresh event is required.|unchanged++|

Capture old comparison ID before4968 overwrites `v_comparison`; do not infer CHANGED from today's revision_number alone. For each NEW/CHANGED SOURCE_RECHECKED insert ON CONFLICT DO NOTHING, select actual immutable row by original key `SOURCE_RECHECKED:<incident UUID>:<fingerprint hex>`; verify incident/episode/publication/fingerprint/SYSTEM actor, payload comparison ID/issue family/presence. A missing RETURNING row means possible original existing event, not permission to fabricate or silently omit evidence.

Tail equations: sorted unique seen IDs equals pure input IDs; each input has exactly one outcome/comparison; outcome counts equal all four original counters (UNCHANGED_OPEN+UNCHANGED_RESOLVED is unchanged); new+changed+unchanged=input count; sorted distinct actual omitted IDs equals the exact locked expected selector IDs; resolved=omitted cardinality; omitted work IDs cannot overlap input. Bound input/omitted sets<=1000, total actual stages<=1000, deduplicated original witnesses<=4000. Refuse excess atomically under NEXT rather than truncate. Existing metadata bound often caps a NEW action below1000 because NEW yields incident insertion, comparison insertion and incident pointer stages.

## Historical resolved-decision proof is real, not SOURCE_RECHECKED

Original4668–4686 chooses the latest episode for group/work (`ORDER BY episode_number DESC,id DESC LIMIT1`), requires state=RESOLVED, resolution_kind in OFFICE_ACCEPTED_SYSTEM_HOURS / MANAGER_CONFIRMED_SYSTEM_HOURS / CANDIDATE_CORRECTED, and compares **only** contract, issue family/presence and six local time/break fields using IS NOT DISTINCT FROM. It does not check current publication/source-row/timesheet fields, expected fingerprint input, candidate shift, or a recomputed current-upload fingerprint. Keep exactly that predicate, including historical publication validity; do not call normal current-issue fingerprint equality here or force historical comparison publication to equal current Sync publication.

At original4882 branch under already-locked work identity, select exact latest incident and matched comparison, then require one genuine immutable RESOLVED event from the corresponding original owner:

* OFFICE: event_kind=RESOLVED, actor_kind=OFFICE, payload resolution OFFICE_ACCEPTED_SYSTEM_HOURS, key `OFFICE_ACCEPTED_RESOLVED:<incident>:<matched comparison fingerprint>`. Verify the matching OFFICE_ACCEPTED pair by its original key and matching incident/episode/publication/fingerprint/actor UUID.
* MANAGER: event_kind=RESOLVED, actor_kind=MANAGER, payload resolution MANAGER_CONFIRMED_SYSTEM_HOURS, key `MANAGER_CONFIRMED:<actual MANAGER_RESPONDED event UUID>`. Verify real immutable parent response, same incident/episode/publication/fingerprint, response_kind SYSTEM_CORRECT or CANDIDATE_DID_NOT_WORK, original request UUID/hash/count/fingerprint identity.
* CANDIDATE: event_kind=RESOLVED, actor_kind=CANDIDATE, payload resolution CANDIDATE_CORRECTED, key `CANDIDATE_CORRECTED:<actual CANDIDATE_RESPONDED event UUID>`. Verify real parent response same incident/episode/publication/fingerprint, choice CANDIDATE_WRONG and original request UUID/hash/count/fingerprint identity.

All three resolution events must match latest incident ID/episode and matched comparison fingerprint; historical event publication is its actual original publication, not today's Sync publication. Missing/conflicting historical event is a named bounded refusal under NEXT; do not manufacture a witness or substitute mutable incident.resolved_at/state as immutable proof. `RESOLVED_DECISION` child references the genuine RESOLVED UUID, `COMPARISON` child its matched comparison UUID. Underlying pair/response may be revalidated transitively by the immutable original idempotency key without a new child witness kind; 0020 does not admit MANAGER_RESPONDED/CANDIDATE_RESPONDED witness kind.

## Exact omission predicate and expected selection

The original selector is quoted below verbatim by local file extraction in the appended fragment. It covers source group/OPEN, actual guard client, unseen work, no-future origin week, actual CYCLE or NHSP_REPORT_SCOPE client, omission meaning OR NHSP exact current resolved positive/provisional row matched to candidate times, plus no current resolved mapping collision. It is **not** every absent work ID; it has no added date-range or upload-wide client shortcut. Capture expected rows inside its existing sorted `FOR UPDATE OF incident` loop before changing them. The completion record certifies that this exact owner consumed that complete original selector; later assertion rechecks retained immutable comparisons/events and recorded sets, never attempts to recalculate today's OPEN omission predicate.

## Existing schema mapping and raw noncircular digest

0020 header stores producerID/xid/epoch/root kind+variant/request SHA, actor/cycle/publication, typed input work UUIDs and omitted incident UUIDs with counts/digests, exact original result counts, complete material stage count/min/max/raw SHA, complete original witness count/SHA, completion SHA. QUERY_SYNC has child_action_sha=NULL. Publication UUID is actual immutable scope anchor; current0020 has no separate report_scope_id/scope-kind fields. Do not falsely report those columns exist.

0020 child stores `(completion_id,witness_ordinal,witness_kind,comparison_revision_id,discrepancy_event_id,evidence_sha256)`. COMPARISON uses real comparisonUUID; SOURCE_RECHECKED/SOURCE_MATCHED/RESOLVED_DECISION use real eventUUID. Deterministic order: explicit finite witness-kind rank, then native comparisonUUID/eventUUID, deduplicated by `(kind,UUID)`; contiguous ordinals1..count. Include CHANGED prior comparison if validating old/new distinction from retained originals. It is legitimate to retain the same comparison once even if referenced for multiple validation purposes; arrays maintain per-input alignment until verified insertion.

Use actual `bpay_next_query_raw_stages_v1(producer)` once after complete business tail. Its domain `BPAY_NEXT_QUERY_RAW_STAGES_V1` hashes producer ID and ordered raw e.id/xid/epoch/ordinal/relation/row/presence/OLD+NEW SHA/Candidate/root/canonical tuples. It **excludes** producer completion witness fields. Verify joins/ownership/scope before trusting its count: every producer event must join exact PAY_QUERY e and q, xid/epoch/owner relation/row, tuple hashes/presence, all stage ordinals distinct; no stage may disappear in inner joins.

Do not use `bpay_next_query_event_digest_v1` to construct completion SHA:0002 includes completion witness kind/ID/SHA, so that would be circular. Once completion is immutable and set on producer, original metadata command digest can bind it normally.

## Additive durable per-input and omission certificates (recommended)

Root's identified0020 gap is valid: a union of comparisons/events plus input ID/counts does not durably say which exact parsed input produced which outcome, especially UNCHANGED_RESOLVED ignores current input timesheet/source/link/expected-hash fields. Retaining per-input alignment only in private arrays is insufficient for later exact assertion. Add bounded private append-only typed child relations. No raw request JSON, finance data, contacts or mutable counters. The original body remains financial/query business authority; these rows certify only its actual complete root.

Recommended source schema below is a design fragment, not executed/product-authored. Use an additive db:new migration and explicit ACL/immutable guards. The header needs two nullable digest columns with QUERY_SYNC-only required values, included by an ordered successor to0030 completion digest; SYSTEM_HOURS records keep NULL. Do not silently change `original_witnesses_sha256` meaning to cover unrelated certificate bodies.

```sql
alter table private.bpay_next_query_root_completion
 add column query_input_completions_sha256 bytea,
 add column query_omission_completions_sha256 bytea,
 add constraint bpay_next_query_root_completion_query_children_ck check (
   (owner_kind='QUERY_SYNC'
    and octet_length(query_input_completions_sha256)=32
    and octet_length(query_omission_completions_sha256)=32
    and query_input_completions_sha256 is not null
    and query_omission_completions_sha256 is not null)
   or (owner_kind<>'QUERY_SYNC'
    and query_input_completions_sha256 is null
    and query_omission_completions_sha256 is null));

create table private.bpay_next_query_sync_input_completion (
 completion_id uuid not null references private.bpay_next_query_root_completion(id) on delete restrict,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 issue_sha256 bytea not null check(octet_length(issue_sha256)=32),
 outcome text not null check(outcome in ('NEW','CHANGED','UNCHANGED_OPEN','UNCHANGED_RESOLVED')),
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 prior_comparison_revision_id uuid references public.weekly_issue_comparison_revisions(id) on delete restrict,
 recheck_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 resolved_decision_event_id uuid references public.weekly_discrepancy_events(id) on delete restrict,
 matched_facts_sha256 bytea check(octet_length(matched_facts_sha256)=32),
 evidence_sha256 bytea not null check(octet_length(evidence_sha256)=32),
 primary key(completion_id,work_event_id),
 unique(completion_id,incident_id),
 check((outcome='CHANGED')=(prior_comparison_revision_id is not null)),
 check(prior_comparison_revision_id is null or prior_comparison_revision_id<>comparison_revision_id),
 check((outcome in ('NEW','CHANGED'))=(recheck_event_id is not null)),
 check((outcome='UNCHANGED_RESOLVED')=(resolved_decision_event_id is not null)),
 check((outcome='UNCHANGED_RESOLVED')=(matched_facts_sha256 is not null))
);

create table private.bpay_next_query_sync_omission_completion (
 completion_id uuid not null references private.bpay_next_query_root_completion(id) on delete restrict,
 incident_id uuid not null references public.weekly_discrepancy_incidents(id) on delete restrict,
 work_event_id uuid not null references public.weekly_work_events(id) on delete restrict,
 issue_episode integer not null check(issue_episode>=1),
 comparison_revision_id uuid not null references public.weekly_issue_comparison_revisions(id) on delete restrict,
 resolve_event_id uuid not null references public.weekly_discrepancy_events(id) on delete restrict,
 evidence_sha256 bytea not null check(octet_length(evidence_sha256)=32),
 primary key(completion_id,incident_id),
 unique(completion_id,work_event_id), unique(completion_id,resolve_event_id)
);
```

`issue_episode` is needed because original comparison row has incident/revision but no issue_episode field; later resolve/recheck qualification must bind captured original episode without consulting future mutable incident lifecycle. Work UUID FK is real preclock business key-share work; original inputwork FOR UPDATE is already held. Omitted incident/work scope discovery and all new certificate FKs must finish before the clock; do not assume an AFTER insert is lock-free.

Before-INSERT private guard on each certificate requires header owner_kind/root_variant=QUERY_SYNC, header.preparing_xid=current xid, exact producer header UUID/epoch, producer BUSINESS_OPEN, input OPEN, actual owner_stack=ARRAY['QUERY_SYNC'], and no existing completion for a different lifetime. Actual true-tail helper caps each child set by header counts and <=1000. Revoke all table privileges from public/anon/authenticated/service_role, enable RLS, owner postgres, immutable BEFORE UPDATE/DELETE and BEFORE TRUNCATE guards. No browser/service INSERT route or generic certificate writer. An immutable header must be fully computed before INSERT; compute child row digests from typed arrays and original evidence, aggregate ahead, insert header once, then children/witnesses, verify exact equality, finish. Never insert placeholder header hashes then UPDATE.

The helper signature above gains aligned `p_issue_sha256 bytea[]`, `p_issue_episodes integer[]`, `p_matched_facts_sha256 bytea[]`, and `p_omitted_work_event_ids uuid[]`, `p_omitted_issue_episodes integer[]`. Alternatively one new SQL composite for these exact columns is finite, but arrays reuse existing shape without a generic JSON record. Every mandatory/null branch is checked before use.

### Digests and authentic original input

Pure issue digest domain `BPAY_NEXT_QUERY_SYNC_ISSUE_V1` is a typed fixed array in this order:

```sql
jsonb_build_array(
 'BPAY_NEXT_QUERY_SYNC_ISSUE_V1',
 (issue->>'work_event_id')::uuid,
 nullif(issue->>'candidate_timesheet_id','')::uuid,
 nullif(issue->>'candidate_timesheet_revision','')::integer,
 lower(nullif(issue->>'candidate_shift_fingerprint','')),
 nullif(issue->>'source_row_id','')::uuid,
 nullif(issue->>'source_work_event_link_id','')::uuid,
 nullif(issue->>'contract_id','')::uuid,
 upper(btrim(coalesce(issue->>'issue_family',''))),
 upper(btrim(coalesce(issue->>'source_presence',''))),
 nullif(issue->>'candidate_start_at_local','')::timestamp,
 nullif(issue->>'candidate_end_at_local','')::timestamp,
 nullif(issue->>'candidate_break_minutes','')::integer,
 nullif(issue->>'system_start_at_local','')::timestamp,
 nullif(issue->>'system_end_at_local','')::timestamp,
 nullif(issue->>'system_break_minutes','')::integer,
 lower(nullif(issue->>'expected_material_comparison_fingerprint',''))
)
```

SHA256(convert_to(array::text,'UTF8')). Parse/validate actual hash32 fields using original hex32 owner/errors, preserving original matched-resolved branch's intentional skip of expected-fingerprint equality. Hashing the expected field is not permission to add an equality validation in that branch. Empty/missing nullable strings normalize with exact original nullif; workID mandatory; UUID alternate representation normalizes to typed UUID. Case/space canonicalization only follows original owners. There are16 issue fields, not an arbitrary replacement JSON body.

Root request SHA uses `BPAY_NEXT_QUERY_SYNC_REQUEST_V1`, actorUUID/cycleUUID/publicationUUID, complete cardinality, and sorted native `(work_event_id,hex(issue_sha256))` array. Compute it from pure parsed locals before entry and later recompute from immutable per-input rows. Native UUID ordering here affects proof only: do not change original body text ordering of work locks. No token or invented external requestUUID.

Historical-only matched facts SHA uses `BPAY_NEXT_QUERY_SYNC_RESOLVED_FACTS_V1` and exactly contractUUID, normalized family/presence and six typed timestamp/break fields. Compute once from actual parsed issue and once from matched immutable comparison and require equality at actual4882 branch. Retain that digest in the input certificate. Later assertion can recompute the comparison subset digest exactly; it cannot reconstruct all original input facts from a one-way issueSHA, so its trusted private preclock insertion certifies the relationship between complete issueSHA and matched-facts SHA. Do not claim the SHA alone is a reconstruction or recalculate current mutable work/timesheet/source data. Exact subset comparison remains the original boolean predicate, not this digest alone.

Input row digest domain `BPAY_NEXT_QUERY_SYNC_INPUT_COMPLETION_V1` binds completionUUID, workUUID, issueSHA, outcome, incidentUUID, episode, comparisonUUID, priorUUID, recheckUUID, resolvedDecisionUUID, matchedFactsSHA, in that fixed order. Omission row digest `BPAY_NEXT_QUERY_SYNC_OMISSION_COMPLETION_V1` binds completionUUID, incidentUUID, workUUID, episode, comparisonUUID, resolveUUID. Header child digests respectively bind sorted `(workUUID,inputRowSHA)` and sorted `(incidentUUID,omissionRowSHA)` with their domain/count, including canonical empty sets. Revised completion header digest includes both child aggregateSHA values. The root requestSHA need not include outcomes: it is original intent; completionSHA binds actual outcomes and evidence.

## Finite selector and validation fragments

### Original omitted selection, literally retained

```sql
  for v_incident in
    select incident.*
    from public.weekly_discrepancy_incidents incident
    join public.weekly_issue_comparison_revisions comparison
      on comparison.id=incident.current_comparison_revision_id
    join public.weekly_source_cycles origin_cycle
      on origin_cycle.id=incident.source_cycle_id
    where incident.source_group_id=v_group_id
      and incident.state='OPEN'
      and (nullif(v_guard->>'scope_client_id','') is null
        or incident.client_id=(v_guard->>'scope_client_id')::uuid)
      and not (incident.work_event_id=any(v_seen_work_events))
      and origin_cycle.finalisation_week_ending<=(v_guard->>'finalisation_week_ending')::date
      and (
        v_scope_kind='CYCLE'
        or (v_scope_kind='NHSP_REPORT_SCOPE' and incident.client_id=v_current_report_client_id)
      )
      and (
        v_omission_meaning='CANCEL_INSIDE_CONFIRMED_COVERAGE'
        or (
          v_final_authority_kind='NHSP_TRUST_BACKING_REPORT'
          and exists(
            select 1
            from public.weekly_work_event_source_links current_link
            join public.weekly_source_upload_rows current_row
              on current_row.id=current_link.upload_row_id
            join public.weekly_source_row_resolutions current_resolution
              on current_resolution.id=current_link.row_resolution_id
            where current_row.upload_id=v_upload_id
              and current_link.work_event_id=incident.work_event_id
              and current_link.link_kind in ('PROVISIONAL_SOURCE','POSITIVE_SOURCE')
              and current_row.row_finalisation_state='SOURCE_WORKED'
              and current_resolution.mapping_state='RESOLVED'
              and current_resolution.work_event_id=incident.work_event_id
              and current_resolution.candidate_id=incident.candidate_id
              and current_resolution.client_id=incident.client_id
              and current_resolution.contract_id=comparison.contract_id
              and current_row.start_at_local=comparison.candidate_start_at_local
              and current_row.end_at_local=comparison.candidate_end_at_local
              and current_row.break_minutes=comparison.candidate_break_minutes
          )
        )
      )
      and not exists(
        select 1
        from public.weekly_source_upload_rows current_row
        join public.weekly_source_row_resolutions current_resolution
          on current_resolution.upload_row_id=current_row.id
        where current_row.upload_id=v_upload_id
          and current_resolution.mapping_state='RESOLVED'
          and current_resolution.work_event_id=incident.work_event_id
          and (
            current_resolution.candidate_id is distinct from incident.candidate_id
            or current_resolution.client_id is distinct from incident.client_id
            or current_resolution.contract_id is distinct from comparison.contract_id
          )
      )
    order by incident.id
    for update of incident
```

Add expected-set accumulation immediately after original LOOP before UPDATE; append actual set only after the original update and exact real event retrieval. Capture comparisonUUID from this cursor's actual `v_incident.current_comparison_revision_id`, workUUID/episode from v_incident. Example private local seam (original statements between these additions stay full):

```sql
-- before original5066 update
v_expected_omitted := array_append(v_expected_omitted,v_incident.id);
-- original5066–5089 FULL BODY unchanged
select e.* into strict v_original_resolve
from public.weekly_discrepancy_events e
where e.event_kind='RESOLVED'
 and e.idempotency_key='SOURCE_MATCHED:'||v_incident.id::text||':'||v_publication_id::text;
-- assert incident,episode,publication exact; expected fingerprint NULL;
-- actor SYSTEM / actor_user NULL; resolution SOURCE_MATCHED.
v_actual_omitted := array_append(v_actual_omitted,v_incident.id);
v_omitted_comparison := array_append(v_omitted_comparison,v_incident.current_comparison_revision_id);
v_omitted_resolve := array_append(v_omitted_resolve,v_original_resolve.id);
-- append work and episode aligned with omitted IDs
```

Expected selector counts over1000 refuse before appending beyond cap; actual stage writer independently refuses transaction bound. Do not add early success/CONTINUE/BREAK or skip omitted rows on a metadata error. Original errors rollback whole action. After original5093–5116tail:

```sql
select coalesce(array_agg(x order by x),'{}'::uuid[]),count(*),count(distinct x)
 into v_sorted_seen,v_seen_count,v_seen_distinct
from unnest(v_seen_work_events) x;
-- Require v_seen_count=v_seen_distinct and v_sorted_seen=p_input_work_event_ids.
-- Same independently for expected and actual omitted sets.
```

Canonical omission set digest is domain `BPAY_NEXT_QUERY_SYNC_OMITTED_IDS_V1`, count and native sorted UUID[]; input-ID-only digest `BPAY_NEXT_QUERY_SYNC_INPUT_IDS_V1` count and native sorted work UUID[]. Root helper uses exact same constants later; keep these separate from child certificate aggregate digests.

### Genuine historical resolution selector

After original boolean matches, fetch original latest incident/comparison under its exact original nine-fact predicate; no current publication comparison is added. Then select the actual historical RESOLVED evidence from that matched episode:

```sql
select e.id
from public.weekly_discrepancy_events e
where e.incident_id=v_matched_incident.id
 and e.issue_episode=v_matched_incident.episode_number
 and e.event_kind='RESOLVED'
 and e.expected_comparison_fingerprint=v_matched_comparison.material_comparison_fingerprint
 and (
  (v_matched_incident.resolution_kind='OFFICE_ACCEPTED_SYSTEM_HOURS'
   and e.actor_kind='OFFICE'
   and e.bounded_payload_json->>'resolution'='OFFICE_ACCEPTED_SYSTEM_HOURS'
   and e.idempotency_key='OFFICE_ACCEPTED_RESOLVED:'||v_matched_incident.id::text||':'||
       encode(v_matched_comparison.material_comparison_fingerprint,'hex')
   and exists(select 1 from public.weekly_discrepancy_events accepted
    where accepted.event_kind='OFFICE_ACCEPTED'
     and accepted.idempotency_key='OFFICE_ACCEPTED:'||v_matched_incident.id::text||':'||
         encode(v_matched_comparison.material_comparison_fingerprint,'hex')
     and accepted.incident_id=e.incident_id and accepted.issue_episode=e.issue_episode
     and accepted.projection_publication_id is not distinct from e.projection_publication_id
     and accepted.expected_comparison_fingerprint=e.expected_comparison_fingerprint
     and accepted.actor_kind='OFFICE' and accepted.actor_user_id is not distinct from e.actor_user_id
     and accepted.bounded_payload_json->>'resolution'='OFFICE_ACCEPTED_SYSTEM_HOURS'))
  or (v_matched_incident.resolution_kind='MANAGER_CONFIRMED_SYSTEM_HOURS'
   and e.actor_kind='MANAGER' and e.bounded_payload_json->>'resolution'='MANAGER_CONFIRMED_SYSTEM_HOURS'
   and exists(select 1 from public.weekly_discrepancy_events answer
    where answer.event_kind='MANAGER_RESPONDED' and answer.actor_kind='MANAGER'
     and e.idempotency_key='MANAGER_CONFIRMED:'||answer.id::text
     and answer.incident_id=e.incident_id and answer.issue_episode=e.issue_episode
     and answer.projection_publication_id is not distinct from e.projection_publication_id
     and answer.expected_comparison_fingerprint=e.expected_comparison_fingerprint
     and answer.bounded_payload_json->>'response_kind' in ('SYSTEM_CORRECT','CANDIDATE_DID_NOT_WORK')
     and answer.idempotency_key='MANAGER_RESPONSE:'||
         (answer.bounded_payload_json->>'request_idempotency_key')||':'||
         (answer.bounded_payload_json->>'review_item_id')
     and (answer.bounded_payload_json->>'request_semantic_hash')~'^[0-9a-f]{64}$'
     and (answer.bounded_payload_json->>'response_fingerprint')~'^[0-9a-f]{64}$'
     and (answer.bounded_payload_json->>'request_item_count')~'^[1-9][0-9]*$'))
  or (v_matched_incident.resolution_kind='CANDIDATE_CORRECTED'
   and e.actor_kind='CANDIDATE' and e.bounded_payload_json->>'resolution'='CANDIDATE_CORRECTED'
   and exists(select 1 from public.weekly_discrepancy_events answer
    where answer.event_kind='CANDIDATE_RESPONDED' and answer.actor_kind='CANDIDATE'
     and e.idempotency_key='CANDIDATE_CORRECTED:'||answer.id::text
     and answer.incident_id=e.incident_id and answer.issue_episode=e.issue_episode
     and answer.projection_publication_id is not distinct from e.projection_publication_id
     and answer.expected_comparison_fingerprint=e.expected_comparison_fingerprint
     and answer.bounded_payload_json->>'choice'='CANDIDATE_WRONG'
     and answer.idempotency_key='CANDIDATE_RESPONSE:'||
         (answer.bounded_payload_json->>'request_idempotency_key')||':'||answer.incident_id::text
     and (answer.bounded_payload_json->>'request_semantic_hash')~'^[0-9a-f]{64}$'
     and (answer.bounded_payload_json->>'response_fingerprint')~'^[0-9a-f]{64}$'
     and (answer.bounded_payload_json->>'request_item_count')~'^[1-9][0-9]*$'))
 )
order by e.id;
```

Require count exactly1; no arbitrary latest event fallback. Original request UUID/review UUID fields must additionally be well-formed typed UUID; include real complete response-set hash/count/unique identities if claiming an original manager/Candidate request completion, using current0017 response owner. Historical Sync only needs original parent response that actually caused its RESOLVED event, not claim the old whole request is today's root. Never re-run today's candidate action state to decide whether an old manager response should have resolved: actual immutable resolution proves the earlier conditional branch. Office/Candidate historical event publication may legitimately differ from matched comparison's publication after an unchanged repeat import; verify parent/resolve publications agree with each other, not forcibly with current Sync or comparison publication.

Need not append parent MANAGER_RESPONDED/CANDIDATE_RESPONDED as an unsupported0020 witness kind. Resolve's immutable exact key binds genuine parentUUID; transitive later qualification checks that immutable row. Existing0030 original-event digest includes idempotency_key, expected fingerprint and resolution, binding the reference. If a dedicated later parent digest is desired, add an explicit typed column/kind instead of pretending RESOLVED_DECISION accepts response eventKind.

## Exact stage/evidence closure and later immutable assertion

Current0017 attaches the incident hook. No current saved comparison-stage trigger/function or inline comparison call was found in bounded `supabase/repeatable` search. QuerySync needs real comparison-insert capture at4968 before finish; otherwise raw digest omits a canonical relation and3/2 count equations do not hold. At this original insertion owner, all relevant values are already local (`v_event.candidate_id`, actual comparison row); root_timesheet_id stays genuinely NULL because candidate_timesheet_id is not proof of a canonical root. An inline private stage call using original returned comparison row is finite for this owner, while a future shared trigger must cover all actual comparison writers from the census and their earliest owner entry. Do not install a global hook while silently treating unsupported other comparison writers as complete.

Canonical comparison tuple from census: incident_id, revision_number, contract_id, issue_family, material_comparison_fingerprint (row identity separately e.owner_row_id=actual comparisonUUID). Stage is old_present=false/new_present=true, OLD={}, new typed exact canonical values; old/new SHA computed by2357. Preserve all immutable comparison fields/business INSERT, ACL and fingerprint owner. Incident tuple is current0017 seven tuple keys (row identity retained separately): candidate/client/work/source cycle/group/state/current comparison. Identity remains protected before tuple-no-op return. No root discovery or late SELECT in row hook.

Finite expected stage matrix for this full original body:

| Certified outcome | Exact raw material stages |
|---|---|
|NEW|One incident INSERT: absent→OPEN with NULL current comparison; one actual comparison INSERT; one incident UPDATE OPEN→OPEN with NULL→actual comparison. Identity fields identical across insert/pointer.|
|CHANGED|One actual comparison INSERT; one incident UPDATE OPEN→OPEN with actual retained prior comparison→actual new comparison. Other tuple fields unchanged.|
|UNCHANGED_OPEN / UNCHANGED_RESOLVED|No incident/comparison material stage for this input; existing immutable proof still retained.|
|omission|One incident UPDATE OPEN→RESOLVED with identical current comparison and all other canonical tuple fields unchanged.|

Require exact bijection between every producer-owned stage and these certified operations, not merely matching total count. Then total `3*new_incidents + 2*changed_comparisons + resolved_incidents` equals actual_stage_count. No stage from audit/cohort/outreach/notices is invented; those original writes are genuine tail obligations but not canonical financial gate tuple. If another actual canonical writer appears in the full tail, census it and extend explicit matrix before accepting; never discard an extra event to match the formula.

Typical finite checks:

```sql
-- validate raw owner join completeness first, using LEFT JOIN to expose omissions
if exists (
 select 1 from private.bpay_next_query_event_owner eo
 left join private.bpay_next_input_version_event e on e.id=eo.event_id
 left join private.bpay_next_query_input_version q on q.event_id=eo.event_id
 where eo.producer_id=p_producer_id and (
  e.id is null or q.event_id is null or e.input_kind<>'PAY_QUERY'
  or e.preparing_transaction_id<>owned.preparing_transaction_id
  or e.module_epoch<>owned.module_epoch
  or e.owner_relation<>q.owner_relation or e.owner_row_id<>q.owner_row_id
  or e.owner_relation not in ('public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions')
  or e.old_sha256<>sha256(convert_to(q.old_gate_tuple::text,'UTF8'))
  or e.new_sha256<>sha256(convert_to(q.new_gate_tuple::text,'UTF8'))
  or (not e.old_present and q.old_gate_tuple<>'{}'::jsonb)
  or (not e.new_present and q.new_gate_tuple<>'{}'::jsonb)
 )) then raise exception 'BPAY_NEXT_QUERY_SYNC_STAGED_SET_INVALID'; end if;
```

Also require every e in current xid/epoch PAY_QUERY has this exact eo, no extra non-PAY_QUERY staged event, distinct ordinals and exact all-count match to raw helper. For each certified expected operation require exactly1 matching stage; each stage matches exactly1 operation. Incident compare proof uses immutable raw OLD/NEW currentcomparison IDs plus retained certificates; do not query today's incident current_comparison to classify an old completion.

Original witness set is exact UNION DISTINCT of:

* COMPARISON: every input.comparison, CHANGED prior comparison, every omission.comparison.
* SOURCE_RECHECKED: every NEW/CHANGED input.recheck event.
* RESOLVED_DECISION: every UNCHANGED_RESOLVED input historical resolution.
* SOURCE_MATCHED: every omission resolve event.

No unrelated publication-wide events, no abandoned older revision, no omitted parent and no arbitrary one-event witness. Deduplicate(kind,actualUUID), deterministically order by finite kind-rank then nativeUUID, assign contiguous witness_ordinal1..count. All original evidence digest values come from current0030 helper; validate each event against its branch record before computing aggregate. Source recheck event must bind exact actual comparisonUUID/payload family/presence/fingerprint/incident/episode/pub and genuine original key; SOURCE_MATCHED uses exact original current Sync pub, null fingerprint, SYSTEM actor, resolution/key and captured original episode. UNCHANGED_OPEN needs its immutable comparison; do not demand an unrelated recheck event when original comparison was created elsewhere or earlier.

Later `completion_witness_assert_v1` QUERY_SYNC branch (keep manual/response/app full current branches):

1. p_witness_id=owned.id and header.id=owned.id; header matches original producer xid/epoch/owner/cause/request SHA, QUERY_SYNC variant, child_action=NULL. It need not require today's module ACTIVE or producer BUSINESS_OPEN when revalidating old retained evidence; insertion/finish own those lifecycle requirements. Completion SHA must equal p_witnessSHA and the revised typed header digest.
2. Exact certificate cardinalities equal original header input/omission counts; sorted exact IDs equal header arrays; no NULL/duplicates or input/omission work overlap. Recompute each input/omission typed row SHA and both child aggregate SHAs. Recompute root requestSHA from actor/cycle/pub and native sorted(workID,issueSHA). Outcome counts exactly equal original new/changed/unchanged/resolved, and all sum/equality laws hold.
3. Recompute exact child original witness UNION; compare every kind/actualID/digest, full count, contiguous ordinals and aggregate SHA to header. Check same branch-qualified immutable comparison/event relationships, including historical real original response/Office pair. Later historical matched-facts SHA recomputes from retained immutable comparison fields; no current incident lifecycle, work, policy, source mapping, current timesheet or publication guards are reread.
4. Recompute0030 raw stages and bijection/count formula from retained immutable stage tuples plus typed certificates; verify header count/min/max/raw SHA. Do not demand raw stage has no metadata receipt after root finish: later assertion normally occurs after those receipts exist. No future business lock/FK insertion; read immutable original IDs and private retained metadata only.
5. Source current body route/auth/locks/omission/restart/outreach/audit completion is certified by the sole private same-xid root helper at the actual5116 tail, not inferred from public mutable view state or the number of events. No service caller can author certificates directly.

Original root no-event cases: zero issues and no selected omissions; all supplied work unchanged open; all supplied work historically resolved matched; or mixes of these with no material change. Full original outreach/audit tail still runs. Helper inserts bounded private completion+typed certs/witnesses even when raw stages empty (`actual_stage_count=0`, first/last NULL, domain digest of empty array).0002 finish validates witness, updates private producer FINISHED/input CLOCK_SEALED, then returns at135–139 before receive_command. No QUERY_INPUT_VERSION command, agency sequence, job, new business reply or fake SOURCE_RECHECKED event is created. A repeated unchanged Sync is an actual new no-op action; do not return an invented prior idempotent response or widen original API response keys.

## Required concrete product sequence and honest limits

Additive schema before helpers; ordered full assertion/digest/helper successors before full original QuerySync successor. Final registry must designate actual public `(jsonb)` Sync full body and all added private signatures with exact physical paths. Keep public service-only ACL and private/browser denies. This note does not assign a file date root has not chosen, assert installation, or claim SQL compiles/behavior passes.

Typed header+cert helper closure is standalone public QuerySync only. Current2357 stack permits different nested SOURCE_UPLOAD/ROWS_APPLY/CANDIDATE_MATERIALISE owner kinds, but actual public Sync has no saved SQL caller. Do not attach a nested this-function path whose root Source completion certificate remains unbuilt. A genuine future nested call leaves only and contributes typed records to its actual Source root certificate; it must not create a standalone QUERY_SYNC header under a different root producer UUID/kind. Named unsupported refusal is honest before an unsupported owner mutates business; completing this slice is not wholeF25 closure.

Critical source constraints: original resolved-decision match runs before normal fingerprint and expected-fingerprint check; do not strengthen it to full current issue equality. Original text issue lock order and native proof sort are distinct. Actual omissions use source profile/report/mapping predicate above; later immutable assertion cannot reconstruct an earlier live omission population from a current view and must not try. New certificate FKs are business locks before clock. Fresh zero-stage action still executes original tail. Preserve original counters, append-only business evidence and response exactly; no journal/view audit side effects masquerade as economic receipt.

## Saved source provenance

SHA-256 below is fresh saved raw bytes at this investigation checkpoint; no installed-body or runtime claim. Function declaration/body hash is decoded UTF-8 normalized LF from the actual original lines, distinguished from raw file hash.

| Saved source | Raw SHA-256 |
|---|---|
| `docs/bpay-next/design/F-25-query-delivery-completion-witnesses.md` | `92fdff767cd7f24896946516102093d387dbf31a6c16020c1811735a4f45a83e` |
| `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql` | `bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf` |
| `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql` | `4be599c83fe18bb3b8761fecebb3e0a9785d2d69a716b83225787f8cc431dd54` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `supabase/repeatable/09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| `supabase/repeatable/10102026_0002_banking_pay_next_query_metadata_owner_v1.sql` | `a253e956f0f55ed3fc0e03dafbe97c690d800a05bf532f402c51c8e81641b759` |
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |

Original QuerySync full declaration/body, lines4691–5123: normalized-LF SHA-256 `045a26502b8a3f4b8dca5c4c0b2e836cb31bffe9c3de9629fb5830e9e428b994` (21024 bytes).

Original resolved-decision match, lines4663–4687: normalized-LF SHA-256 `12bf7cd6b6c1267cc52158afa31e26500962c5ed7741998615a2ca8614310cf9` (1907 bytes).

Original material fingerprint owner, lines4626–4657: normalized-LF SHA-256 `e25bf7975010ffc1cdc36fb33449fccbefb0e31f25fe94fd477c93a531aa88fc` (1614 bytes).

## Root implementation reconciliation —10 October2026

0057 succeeds the installed0020 witness caps at4000 after complete maximum proof:1000 historical-resolved inputs plus1000 distinct omissions retain4000 actual comparison/decision witnesses. Stage/input/omission caps remain1000. Do not truncate.

0059 uses bounded named SQL table-composite arrays for the0036 input/omission rows instead of anonymous JSON or parallel nullable-field arrays. Their exact typed layout/constraints and original pure requested IDs/hashes, actual seen IDs, original selected omission IDs and counters all bind one immutable header; no raw request or financial snapshot is retained.

The original historical resolved-match branch returns before validating expected_material_comparison_fingerprint.0038 therefore hashes its exact optional nonempty text without early hex validation; genuine lowercase valid values retain the same digest representation. Original nonhistorical body still calls its original lowercase hex parser and equality check in the same place. This preserves previously accepted historical matches with ignored malformed/uppercase expected values while binding the complete submitted input. No original parsed candidate hash/live signed Timesheet validation changes.
