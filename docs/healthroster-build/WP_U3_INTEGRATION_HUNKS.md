# WP-U3 exact adjacent integration hunks

Source-only, unmounted. Root owns the Banking Checklist union, SQL closure, ACL, contract, API/Office and native acceptance. This lane changes none of those owners and does not inspect the unavailable Banking overlay.

Include order: submission context → signed submission read context → check-only query rows → client-week context. These four `.sqlinc` pages in `supabase/repeatable/healthroster_wp_u3_source_proposals/` define complete private readers, logical postgres owner/fixed search paths/no browser or service EXECUTE. Include only from the new root-owned chronological repeatable after F1/CN/D10/Report schema and qualified WAIT prerequisites. Keep `supabase/verification/support/wp_u3_native_read_vectors.sqlinc` in the verification closure only, after real native fixtures exist. Both locations preserve the existing release tooling's include envelope; `.sqlinc` pages cannot become independent canonical `.sql` roots. Do not add canonical top-level roots from this lane.

The installed single native Checklist discovery needs an additive actual-submission arm before native dedup/retention/permission selection and keyset slicing. The following is an exact relational hunk, not a second public query or a cycle creator. Root supplies its actual actor/filter bounding and canonical cycle-resolution context:

```sql
with actual_submitted as materialized (
  select t.timesheet_id,t.week_ending_date,
    (submission.witness->>'client_id')::uuid client_id,
    (submission.witness->>'work_date')::date work_date
  from public.timesheets t
  cross join lateral (
    select private.weekly_source_actual_timesheet_submission_v1(t.timesheet_id) witness
  ) submission
  where (submission.witness->>'actual_submitted')::boolean is true
), actual_signed_own_weeks as (
  select distinct member.source_group_id,a.client_id,a.week_ending_date
  from actual_submitted a
  join public.weekly_source_group_clients member on member.client_id=a.client_id
    and a.work_date between member.valid_from and coalesce(member.valid_to,'infinity'::date)
    and a.week_ending_date between member.valid_from and coalesce(member.valid_to,'infinity'::date)
  cross join lateral (
    select private.weekly_source_signed_submission_read_context_v1(a.timesheet_id,member.source_group_id) read_context
  ) signed_read
  where (signed_read.read_context->>'eligible')::boolean is true
)
select * from actual_signed_own_weeks;
```

Union those identities into the existing native expected-work discovery. Do not require a Contract expectation/import/weekly publication and do not use source-only information as admission. The private Source leaf requires one real cycle with `cycleSunday-7 < ownWeek <= cycleSunday`, correct group/Client scope and real own week. The root native canonical cycle/prepare owner resolves/creates that bookkeeping outside these reads; missing/ambiguous canonical cycle is a held integration gate, never a fabricated UUID or guessed grouping. Preserve current Source scopes' offset cursor. This discovery has no fixed lookback: dated membership and actual submission control the set; late older submission can return an open earlier own-week.

For each native discovered client-week, call `weekly_source_healthroster_checklist_leaf_v1` with actual actor/group/Client/cycle/own-week and CONTEXT. `applies:false,preserve_native:true` preserves the unchanged native NHSP/nonpreset branch. For signed contexts, latest actual ended week stays when complete; earlier completed weeks drop and earlier real open weeks remain. Root applies this retention before slicing and exposes the actual earlier-open-week count/grey line. Expected Source rows use existing native completion/coverage/rule semantics and all actual final scope history; the private leaf does not classify Ready/Paid/Holds.

For signed Queries/Checklist items, reuse `weekly_source_check_only_query_rows_v1` with the same exact scope and actual actor. It is a private raw traversal. Root uses native reason/subject/action/withholding owners to flatten and select final public items, including native reason rank, before final public pagination. Neither `office_query_groups_v1` nor existing bulk Ask Candidate/Send Manager/Accept System Hours gains these rows. Invoice query provenance comes only from current native review decisions and existing HR issue/delivery/outbox records. Strict issued D10 is read only, and all validation/ref/invoice mutations retain their existing fences.

Pending Daily has `submission_read_context.route_state=SUBMITTED_PENDING_NATIVE_MATERIALISATION`, `query_route_available:false`, `actions_held:true`; copy its exact immutable workflow/generation/Candidate/Client/TS/date/SHA witness into the optional closed service pending fragment. Root must prove these identities from the private witness before serialization. Show actual submitted/pending native state, no finalized/READY claim and no actions. Do not route this read predicate into correction Send, manager authority, paired Authorise, ordinary query eligibility or financial admission. Those owners keep unchanged F1/native prerequisites. Actual native materialisation later clears the pending read state through the normal native witness, not through a client flag.

Waiting correction raw context owns one read item: count T0 once, suppress ordinary unauthorised duplication/reminders, and use only exact P0 for the existing Timesheet workspace action. Applied request counts genuine P0 and excludes displaced T0/R0. The read projection supplies the exact request/P0/workflow witness; the native authority supplies alignment/paired-authorise/same-draft-return subject permissions. Reduced pay actors get those permitted subjects as text only, no invoice-only editing/credit/issue/hold-release subject and no query recipients/history. Root must apply the subject filter before public slicing.

Import the standalone module only after callback binding is native-proved. Inject the existing native actor guard, native public Checklist page owner and authoritative Source/NEXT/ordinary evidence callback; preserve exact actor/scope/cursor/error order. Inject existing Timesheet presentation and `applyNextPaidEvidence` decoder for informational paid schedules. No network/provider/payment/mutation callback is added. Preserve existing Office infinite-scroll/session/modal entry points; each expansion has its own observer/root/cursor and child-return range refresh. Admin No one worked and Undo are only descriptors for the actual installed native attest/superseding-CAS/audit owner; no mutator is supplied here.

Native proof remains required: all four readers compile/ACL/caller coupling, actual submitted states and legacy assets, pending Daily order/identity/actions, nullable-Contract owner proof, request member selection, D10 issued query/provenance, root discovery/cycle mapping, all main sorts and native item rank/keysets/anchors, 500+ rows/independent 230/120 expansions, native No one worked/Undo, Source/NEXT holds/known-zero/unknown pence, NHSP/nonpreset pass-through and real API/Office screenshots. Source callbacks and packaging tests do not close those gates.
