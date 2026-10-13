# Correction contract v1 — source contract for review

Authority: SEND_5_TO_HEALTHROSTER_BUILD_STAGE_NONSTOP.md, owner R-240/R-248/R-250/R-255/R-258. Credit lane owns the request/WHOLE SQL producers. Integrator owns Worker/public-broker/detail exposure, generated client contracts, flags, sealing and releases. This is a frozen proposed source contract; installed/API/phone acceptance has not run. Generic Authorise/Unauthorise, Candidate financial owners, Policy X, OR-44 and the 63 Candidate operations remain pinned/call-only.

## Storage and why a request is necessary

`private.timesheet_correction_requests` is one new justified request store, inaccessible to browser roles and directly to `service_role`. Existing `weekly_timesheet_submission_requests` requires a cycle/cohort/current upload and has reminder/deadline semantics; `candidate_approval_requests` is the existing signature authority for an already-created workflow; `sourcecorrection_sessions` requires a final Source revision/cycle. None represents an ordinary signed weekly or native DAILY/null-Contract request before its workflow, exact cancellation, A0 triple or same-draft return. They keep their existing purposes. No second schedule, signature, comparison, query, financial ledger, authoriser, renderer or queue is created.

| Column | Type and authority |
|---|---|
| id | UUID, server allocated, stable draft ownership |
| correction_id | text, server allocated `awc:<root UUID>:<request UUID>`; unique |
| origin | text, closed `AGENCY_INVOICE_CORRECTION` or `SIGNED_TIMESHEET_CORRECTION`; server-proved dated client policy |
| state | text, closed WAITING/APPLIED/CANCELLED |
| state_revision | bigint >=1; monotonic CAS, every semantic transition |
| proposal_revision | bigint fixed at 1; immutable initial Office/HealthRoster proposal; excluded from draft identity |
| root_timesheet_id / original_booking_id | UUID/text, immutable logical original and its business booking (`timesheets.booking_id` is text) |
| reversed_timesheet_id / reversed_version / reversed_financial_id | UUID/integer/UUID, immutable latest positive X and frozen exact finance identity |
| candidate_id / client_id / contract_id / work_date / week_ending_date | UUID/UUID/nullable UUID/date/date, immutable actual business scope; DAILY never borrows a Contract |
| replacement_timesheet_id / replacement_booking_id | UUID/text, one server-owned physical P0 family; unique and distinct from every original family |
| replacement_week_id | nullable UUID, exact adjustment Contract week for weekly; DAILY has none |
| reversal_timesheet_id / reversal_booking_id / reversal_week_id | nullable UUID/text/UUID until atomic approval; R0 never created by preview or waiting signed request |
| request_key / request_fingerprint | text/text, unique end-user command receipt and immutable Send input fingerprint |
| target_revision | text, hash of original id/version/business scope/frozen evidence and request identity |
| reason | text, required normal Office correction reason, 1..1000 characters; latest additive named constraint preserves the initially locked migration |
| frozen_original_json / proposal_json | Internal JSON objects containing frozen native schedule/evidence and initial proposal arrays; the public serializer projects to existing closed CandidateScheduleSegment, with no separate schedule authority |
| original_invoice_id / return_invoice_id | nullable UUIDs, exact issued original or same draft D1; D1 id and number retained |
| workflow_id / workflow_generation | nullable UUID/integer; exact existing P0 workflow target, never T0's workflow |
| terminal_receipt_json | nullable JSON object; immutable APPLIED/CANCELLED receipt, including exact member IDs/delegate postconditions |
| created_by_user_id / created_at_utc / updated_at_utc / applied_at_utc / cancelled_at_utc | actor UUID/timestamps, server-owned audit facts |

All immutable identity, Send fingerprint, original evidence, original schedule and initial Office/HealthRoster proposal columns stay equal after INSERT. Proposal revision is 1 for the lifetime of a request. Worker edits use only existing P0 actual_schedule/workflow stores; they never rewrite proposal_json. A different Office proposal requires explicit Cancel and a new request/P0. WAITING may bind the existing workflow and transition once to APPLIED or CANCELLED. Terminal rows retain audit and return the same receipt on exact replay. Different command/payload for an existing key fails. One WAITING request per root/latest X; repeated correction starts only from the latest APPLIED positive member and remains one root tree. APPLIED retains both reversal/replacement IDs. No automatic reminders or silent record deletion.

## Optional TimesheetDetail.correction

`correction` is omitted for unrelated records. If supplied it is an object with these declared fields; no undeclared key is admitted by generated contracts. Original and proposal arrays use the existing `CandidateScheduleSegment` type, including its existing `break_entry` union.

```ts
type TimesheetCorrectionV1 = {
  schema_version: 1;
  request_id: string;
  correction_id: string;
  origin: 'AGENCY_INVOICE_CORRECTION' | 'SIGNED_TIMESHEET_CORRECTION';
  state: 'WAITING' | 'APPLIED' | 'CANCELLED';
  state_revision: number;
  proposal_revision: number;
  target_revision: string;
  root_timesheet_id: string;
  reversed_timesheet_id: string;
  original_booking_id: string;
  replacement_timesheet_id: string;
  replacement_booking_id: string;
  reason: string;
  changed_dates: string[];
  original_schedule: CandidateScheduleSegment[];
  proposed_schedule: CandidateScheduleSegment[];
  draft_scope: {
    kind: 'TIMESHEET_CORRECTION';
    request_id: string;
    replacement_timesheet_id: string;
    allowed_timesheet_aliases: string[];
    allowed_booking_aliases: string[];
  };
};
```

The stable draft owner is agency + Candidate + request_id + replacement_timesheet_id. The existing local canonical recordId is the exact, unique replacement_timesheet_id; correction HOURS context also binds request_id and immutable target/proposal witnesses. Only P0's exact Timesheet/booking aliases are allowed; T0/root/reversed/cancelled/sibling IDs never become writable aliases. State/action generation, proposal revision and target revision are CAS/readiness facts, excluded from that stable draft key. A harmless server refresh keeps the same P0 draft; cancellation revokes it. Historical read-only detail may show the terminal request without enabling an editor.

Use `segment_id`, else `row_key`, else a unique exact original/proposed value match to identify a part. Multiple anonymous indistinguishable parts fail closed; no pairing by array index or random draft key. Highlight only changed date/start/finish/break fields. `start_time`/`end_time` and `work_date` are preferred existing canonical fields; retained aliases follow existing parsers. Every multipart change preserves its segment identity. The public changed-date summary is recomputed from normalized immutable comparison; any raw legacy proposal.changed_dates is informational and ignored. `daily_shift` keeps its existing required booking_id/work_date/week_ending_date/hospital/ward/job_title/shift_type/shift_starts_at/shift_ends_at shape, frozen from original business facts and supplied independently of the active Rota window. No new Daily schedule type is introduced.

The raw service projection includes INTERNAL candidate_id, replacement_week_id, frozen_daily_receipt, original_manager_approved_at_utc and frozen_daily_shift. The latter uses only frozen_original_json.timesheet original business booking, work/week dates, hospital_norm/ward_norm/job_title_norm/shift_label_norm/scheduled_start_iso/scheduled_end_iso. Missing frozen facts remain missing; no current Rota fallback. Raw native schedule fields may include break_mins/break_minutes/breaks/ref_num and existing row/segment identifiers. Worker uses the existing canonical break converters to preserve explicit no-break, duration, timed overnight breaks and multipart identity before producing the closed public type. These internal facts are never copied blindly into TimesheetDetail.correction.

## Exact Candidate and manager purpose reads

All three public helpers are readonly service RPCs, browser-denied. `timesheet_correction_request_projection_v1(p_timesheet_id uuid,p_candidate_id uuid)` selects a request for owned detail. `timesheet_correction_workflow_projection_v1(p_workflow_id uuid,p_candidate_id uuid)` first proves the existing workflow's actual Candidate and exact P0 target/anchor, then projects by that request id; a newer request cannot hijack an older workflow. Unrelated/missing/foreign context returns null.

`timesheet_correction_create_projection_v1(p_candidate_id uuid,p_contract_week_id uuid default null,p_target_timesheet_id uuid default null,p_anchor_timesheet_id uuid default null)` requires at least one supplied selector matching the owned physical P0 or its exact adjustment week. Every other supplied selector must agree or raises CANDIDATE_CONTEXT_STALE. A matched CANCELLED request still returns its terminal context to deny admission after lawful P0 removal. For other states, the physical P0 must be current, unarchived and match request/book/root/Contract/kind. Ordinary missing/foreign selectors return null, preserving native CREATE's errors. None of these helpers grants client authority or changes ordinary replay ordering.

`timesheet_correction_manager_summary_v1(p_workflow_id uuid,p_workflow_generation integer,p_approval_request_id uuid,p_manifest_sha256 text)` is keyed by the existing authenticated manager channel. It returns schema_version/request/correction/reason/original_schedule/reviewed_schedule plus review_witness(workflow_id,workflow_generation,approval_request_id,manifest_sha256,hours_component_id,review_render_input_sha256,review_content_sha256). reviewed_schedule is the exact final worker immutable submission, distinct from the initial Office proposal. It verifies the existing immutable submission hash, manifest/approval, required immutable HOURS component and existing render-input/content hashes. Both approved origins are supported only when this real manager workflow exists; no signature or extra unsigned-manager requirement is invented. Integration freezes the normalized before/after metadata into the existing official_presentation under HEALTHROSTER_CORRECTION_V1 before worker submission, then compares that frozen metadata against this exact witness. No new renderer or cross-auth fetch.

## Actions, checks and atomicity

Proposed service RPCs use `timesheet_correction_*` names, a typed request JSON, authenticated end-user actor UUID and explicit idempotency/CAS. No new Candidate operation number is added. Worker adapter uses the existing operation and resolves the exact request/P0 target before invoking its normal edit/submission/signature route. Exposure waits for a compatible transport and app.

SEND validates active actor/action, dated check-only/preset client scope, exact current latest X, original immutable booking/root, signed electronic manager/candidate witnesses where required, proposal shape and invoice history. It allocates only P0's physical family. Signed T0 remains current/signed/paid/locked while P0 waits. Preview creates nothing. AUTHORISE resolves P0/request before R0 exists, locks request/root/business booking families/current TSFIN/workflow/signature generation in a consistent order, then rechecks all facts, creates one R0, and calls the unchanged complete-unit authorisation owner in one transaction. A0 includes T0/R0/P0 with no earlier standalone T0 approval. It checks returned success and every member postcondition; a returned partial/failed result raises and rolls back R0, P0 approval and request transition together. Mixed selections partition legal complete units before delegation and de-duplicate request identity. T0 selected alone retains its warned ordinary Authorise path.

CANCEL uses the same authority/CAS lock and an owner-created backend/transaction-bound temp capability for the exact WAITING P0/workflow/generation/own week. It refuses any P0 authorisation/payment/invoice/segment lock history, unrelated workflow, expense claim side effect or ordinary reset successor. The adjacent native retirement arm accepts this exact manager-pending/approved/finalized waiting P0, preserves immutable signature audit, clears authority before the unchanged lawful delete/archive owner removes the target. It records CANCELLED only after exact P0/week/workflow postconditions and full original TS/all-TSFIN equality. Actual R2 asset retention through the native GC owner remains a required root proof.

Deferred WHOLE invariants reject incomplete pairs, half-authorised units, missing original finance or any TS-only source/signature/schedule or finance identity/economic changes after APPLIED. Authorisation/invoice/cash/notice/document bookkeeping remains separate from frozen economics; HR crosscheck status/issues/external rows may refresh for normal validation, while original issued support comes only from the immutable member map. Same-draft removal/return delegates to invoice_apply_edits and holds D1 until complete P0 component placement is proved, including replay. The derived hold helper returns an array of {request_id,replacement_timesheet_id,return_invoice_id,blocker_code:"AGENCY_INVOICE_TIMESHEET_RETURNING"}; root joins it to existing issue/delete/send owners. No return queue is added, D1 keeps its id/number and unrelated lines, and an empty P0 has no line. Issued corrections use scoped credit and never write I1.

## Prepared finance and complete approval

`timesheet_correction_prepare_v1(p_request_id uuid,p_actor_user_id uuid)` is readonly and allocates nothing. Its internal contract_version TIMESHEET_CORRECTION_PREPARE_V1 contains prepare_fingerprint, authority(request/state/target/proposal/root/origin, original TS/version/finance id/hash, replacement TS/version/finance id/hash/schedule/signed witness, reserved R0/week ids, original/return invoice ids), candidate_id/client_id, frozen_original, replacement_timesheet, original_current_snapshot/replacement_current_snapshot and original_materialisation_required/original_authorisation_required. Missing exact F1 scope or active PP actor fails closed. Signed scope additionally needs the genuine final manager/candidate pair.

`timesheet_correction_authorise_atomic_v1(p_request_id uuid,p_expected_state_revision bigint,p_prepared jsonb,p_actor_user_id uuid,p_now_utc timestamptz)` requires p_prepared.prepare_fingerprint plus replacement_snapshot/reversal_snapshot and original_snapshot only for missing, unauthorised A0 finance. Root's producer/preloader supplies existing normal canonical outputs from frozen bounded authority; paid/authorised historical T0 is never repriced or made ready by writing it. The mutation locks invoice/root/request/workflow/week/TS/TSFIN and re-runs prepare/CAS. The newly materialized actual A0 snapshot is the reversal authority. Existing T0 uses its exact frozen snapshot. R0 must preserve historic policy/rates/identity and negate all hours/pence/mileage/additional units and every nested component, including daily quantities; unknown component identity must stay exact.

The single bounded writer remains unchanged for ordinary original/P0 and deliberately clamps mileage units. R0 instead delegates existing enqueue_ts_financials(R0,MANUAL), exact tsfin_dequeue_specific([R0],1), and unchanged tsfin_write_snapshots_and_complete with one real {outbox_id,timesheet_id,snapshot}. The newly inserted R0/queue are transaction-private. Exact native claim ownership, one success, zero failures, empty errors, cleared exact queue and actual stored negative components are checked. Any exception, extra/missing claim, returned partial failure or pence normalization drift rolls back the full unit. No second financial calculator or fake queue receipt exists.

The terminal APPLIED receipt contains exact original/R0/P0 financial identities and financial/TS authority hashes, prepare fingerprint, state revision and original_authorisation_required. The sole ordinary_notification_timesheet_ids entry is P0, allowing the existing notification owner to follow up only on the replacement. The unchanged pinned generic Authorise performs the complete 2-member pair or A0 triple, and its returned successes plus actual TS/TSFIN postconditions must all pass before commit. The root Worker adapter owns this prepare/result transport and exact notification context; this SQL source does not send messages.

## Release and proof boundaries

Schema migration preserves RLS/service-owner policy and denies direct service/browser table access. New private helpers are owner-only, new public RPCs service-only; existing routine ACL/config is retained. All 60 unique affected routines have final explicit owner/revoke/grant source expectations in SOURCE_ROUTINE_ACL_INVENTORY.json (24 service-executable, 36 owner-only; 40 new, 20 existing). This inventory is not installed catalogue proof. The final recursive closure reloads PostgREST; root generates/reviews the contract from a mechanical PG17 build before publication and runs serial rollback first-use plus real TEST/Office/document/app acceptance. Source code alone passes none of P-1 through P-18. P-16 money/recovery joins published NEXT frozen settlement receipts; no payment/provider/settlement effect is introduced here.
