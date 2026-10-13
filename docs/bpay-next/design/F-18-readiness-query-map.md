# F-18 readiness query map

9 October 2026. Source-only design for the builder. This document contains data-free SELECT designs, not an installation or an observed census. No SQL, runtime, tests, fixtures, hosted reads, payment/provider/email action or product edit was performed. No count, owner or external quiet window is asserted.

## Controlling contract

Re-read F-18's full card, F-18-D01/D02/D03, its F-02/F-06 dependencies, F-17's consuming contract and the R3 start sheet §§2–6. Start-sheet Q1/OD-F5 and Q5/OD-F1 establish clean launch with no carry-over; F-19/F-20/conditional DR-10 arise only from positively observed genuine old obligations. Q2/OD-F3 rejects Golden Key, permission/switch screens and reauthentication; activation uses the existing database owner-transition authority and the module-control setting. This census does not implement a new switch route, screen, credential or approval. NEXT/DISABLED never returns to LEGACY.

The sources are the companion `F-15-owner-status-and-F-18-census.md` and `.schema.json`, saved `supabase/release/current-contract.json`, the exact claim/scope/receipt owners cited below, and current Worker code. Metadata represents the repository contract, not hosted observations. HEAD at this read was `fee38677b21e3fab50030d57fb57bebd633ef6cd`; the builder's uncommitted saved work is authoritative too.

## Public read boundary and bounded response

Author one final repeatable for `public.bpay_next_cutover_readiness_page_v1(p_request jsonb) RETURNS jsonb`, STABLE, SECURITY DEFINER, fixed `search_path=pg_catalog,private`; fully qualify every relation/helper. Use the same in-function service JWT role guard and corrupt/missing owner refusal as `bpay_next_owner_status_v1()`. Revoke all from PUBLIC, anon, authenticated and service_role; grant only service_role; logical owner postgres; notify pgrst after final definitions/grants. Do not call owner-lock/claim/recovery/mapping/release helpers, use FOR UPDATE/SHARE, write a census cache, or change existing business rows.

Proposed exact request keys: `section`, `category`, `after_id`, `limit`, all optional with defaults ALL/ALL/null/25. Section is exactly ALL/LEGACY/NEXT; category is one fixed name below or ALL. Unknown keys, JSON nonobjects, nonstring/null section/category, unknown combination, malformed UUID, noninteger limit or limit outside 1..100 refuse with a typed 22023 error. ALL is summary-only: after_id must be null, return three first IDs per category, and use the explicit category page for continuation. Do not silently clamp an invalid limit or accept arbitrary table/column/predicate/sort names. Bound input to 4096 bytes and complete serialized output to 65536 bytes; byte overflow is a typed refusal, never a truncated success.

All selected roots below have a UUID cursor, but use their actual key: Source local receipt is `publication_request_id`, not nonexistent `id`; all listed other roots use `id`. The stored-credit-origin mapping has PK `command_id` and unique legacy identity; it is an anti-join, not the category root. Compare/order native UUIDs, not text. Accept only canonical UUID text (case can be normalized once). Return UUID text, count/blocking_count as decimal bigint text, booleans, fixed classification/owner strings and bounded status strings; no amounts, bank details, mail bodies/addresses, snapshots, provider payloads or raw evidence. Parent relation IDs may be included as UUID text for links, bounded to the documented fields.

Response concept: owner/epoch from the same read snapshot, fixed category summaries `{category, count, blocking_count, blocking, classification, items, has_more, next_after_id}`, and `database_ready` computed over whole-category blocking counts, never only the returned page. Classification is EMPTY for zero, OBSOLETE_BOOKKEEPING/ESTABLISHED_NO_CARRY for proved unused old controls, GENUINE_OBLIGATION for positive money/communication facts, UNRESOLVED_EXTERNAL for uncertain admitted action, SOURCE_PENDING_MAPPING for retained Source pending, and INTEGRITY_REVIEW for contradictory/missing ownership proof. `UNDECIDED (OD-F5)` is reserved for an actual genuine obligation needing a new disposition; it is not the default for every nonzero category. Mixed categories return MIXED with fixed per-class counts. NEXT history is HISTORY/nonblocking; unfinished NEXT owners are STOP_RESUME_REVIEW, evaluated for pause/resume separately from LEGACY activation.

Counts and pages within one call need one consistent STABLE statement snapshot. Separate HTTP pages are separate snapshots; do not promise snapshot-pinned exports. F-17 must run the final whole-category census under its real transition/epoch authority. Exact whole-table counts can require scans even though returned pages/bytes are bounded; do not claim constant database work or call the expensive family freeze census on an agency-wide aggregate array. If the statement times out, readiness is unavailable, never zero.

```sql
-- Documentation template only. eligible is ONE fixed category SELECT below;
-- v_after is a validated UUID, v_limit a validated integer.
with eligible as materialized (
  /* fixed SELECT returning id uuid, status text, classification text,
     blocking boolean; optional bounded parent UUID identities */
), totals as (
  select count(*)::text as count,
         count(*) filter (where blocking)::text as blocking_count
  from eligible
), page_plus_one as (
  select * from eligible
  where v_after is null or id > v_after
  order by id limit (v_limit + 1)
), page as (
  select * from page_plus_one order by id limit v_limit
)
select /* bounded JSON over page only; totals covers all eligible */;
-- has_more = count(page_plus_one) > v_limit;
-- next_after_id = the LAST returned page.id when has_more, otherwise null.
-- Do not use min(id), the extra row or offset pagination as the next cursor.
```

## Fixed category roots, statuses and disposition

The following expanded categories implement all card categories while avoiding a union cursor across unrelated tables. They can be grouped under the card headings in the response. Every nonzero informational row stays linkable; the census never discards, adopts or repairs it.

| Fixed category | Root/key (UUID) | Exact eligibility | Classification rule / owner |
|---|---|---|---|
| LEGACY_BATCHES | public.pay_batches.id | status NOT IN SETTLED,CANCELLED **or** execution_commit_state SUBMITTED_NOT_COMMITTED **or** companion open financial/external evidence | An unused DRAFT/DRAFT_CREATED/PREVIEW shell alone is bookkeeping. Authorised/scheduled execution, actual submit, open reservation, partial settlement or a required advice outcome is genuine. FAILED/PARTIAL never proves no remaining money. Owner: batch execution/settlement and companions. |
| LEGACY_OPERATIONS | public.banking_pay_operations.id | status NOT IN COMPLETE,FAILED,CANCELLED,REVIEW_REQUIRED, or active lease, or unresolved companion scope/provider/settlement | DRAFT_CREATE/PREVIEW_REFRESH alone is obsolete engine work. WAITING_PROVIDER, admitted transfer or pending settlement remains genuine even if runner terminal. Owner: operation/scope/settlement. |
| LEGACY_CORRECTION_REQUESTS | public.pay_payment_correction_requests.id | status NOT IN APPLIED,REJECTED,CANCELLED, or an unresolved child, or incomplete tracked cancellation notice | Planning alone without affected real financial/external facts is old workflow. AUTHORISED/PROCESSING/APPLIED_WITH_BLOCKERS/BLOCKED/FAILED with real affected transfer/item remain genuine. Owner: correction work, separate advice reconciliation. |
| LEGACY_CORRECTION_WORK | public.pay_payment_correction_work_items.id | status IN PENDING,PROCESSING,BLOCKED,FAILED_RETRYABLE,FAILED_FINAL | A runner failure is not an approved disposition. Bind actual request/transfer/candidate identities; orphan/contradiction requires integrity review. |
| LEGACY_SNOOZES | public.pay_item_snoozes.id | cleared_at_utc IS NULL AND cancelled_at_utc IS NULL | Distinguish expired date from still-active indefinite/date control. No status column exists. Old unpaid-work preview/control without money/debt reservation is established no-carry; actual retained debt/payment reservation needs its owner's disposition. No new Credit-payout snooze. |
| LEGACY_FINANCE_CASES | public.pay_advances.id | positive outstanding, positive pending principal/component or active reservation, with anti-join to already bound stored-credit origin | LOAN is Payment Advance, not a separate loans/pay_finance_cases table. A positive remaining balance with a cleared/write-off marker is an integrity contradiction, not an excuse to hide the balance. Already bound exact origin is preserved NEXT ownership/history, not a second adoption obligation. |
| LEGACY_RESERVATIONS | public.pay_advance_reservations.id | status IN RESERVED,COMMITTED | Positive reserved_amount/source amount is genuine. Released/settled excluded; malformed zero/contradictory rows are integrity facts. No release/settle call. |
| LEGACY_PAYE_NETS | public.pay_batch_candidates.id | uncommitted nonterminal batch and awaiting_net_amount, PENDING_NET or captured pay_batch_paye_net_inputs | Candidate root avoids duplicate candidate per input history and also sees missing-net pending state. Exactly captured zero is still a captured input, not absence. Pure no-input old Draft pending placeholder is bookkeeping, not a fake net obligation. |
| LEGACY_MAIL_UNSENT | public.mail_outbox.id | exact positive financial scope and no sent/delivered/read evidence or successful terminal status | Failed/queued remittance/payout/cancellation notice remains visible. Payment-auth/processing/scheduled control notices without genuine financial exposure are retired control bookkeeping; accepted/in-flight rows appear below regardless. Owner: actual outbox/result/advice. |
| LEGACY_MAIL_ATTEMPTS | public.mail_outbox.id | exact positive financial scope, unresolved acceptance/result or attempt lease identity; also unresolved tracked cancellation notice source | Lease expiry alone does not prove no send. Provider message identity/accepted status without sent acknowledgement needs result reconciliation. Historical sent rows without outstanding cancellation reconciliation are history. |
| LEGACY_COMMS_UNSENT | public.comms_outbox.id | exact comms Banking scope and no sent/delivered/read evidence/status | Never classify every WhatsApp/SMS channel as Banking. Use scope/reference/deterministic identity below. |
| LEGACY_COMMS_ATTEMPTS | public.comms_outbox.id | scoped Banking row with unresolved leased/accepted result | Identity/acceptance rules as mail; sent/delivered/read history is not open merely because provider_message_id persists. |
| LEGACY_REMITTANCE_TARGETS | public.pay_batch_candidates.id | genuine settled/transfer-backed advice target, remittance_sent_at_utc IS NULL and no exact successful advice evidence | QUEUED_TO_MAIL_OUTBOX/MAIL_OUTBOX_DUPLICATE_EXISTS prove queueing, not sending; include target when outbox insertion was lost. No blanket every historical candidate lacking advice timestamp. |
| LEGACY_COMPLETION_NOTICES | public.pay_batches.id | completion_notice_* marker exists, but actual corresponding outbox result is unresolved | completion_notice_queued_at_utc proves queued, not sent. Marker with no row cannot be silently closed. Old skipped/not-eligible action with no financial terminal outcome is bookkeeping. |
| LEGACY_CANCELLATION_NOTICES | public.pay_payment_correction_requests.id | cancel_notice_tracked AND applied_at_utc IS NOT NULL AND cancel_notice_reconciled_applied_at_utc IS DISTINCT FROM applied_at_utc | Reconciliation-complete is not advice-delivered; unsent resulting notice remains in outbox category. Source mail cancel_notice_tracked and reconciled_sent_at must also be included in MAIL_ATTEMPTS. |
| LEGACY_WORKBENCH_SESSIONS | public.banking_pay_workbench_sessions.id | status OPEN | Unused preview/rebuild/session is obsolete. Only a companion genuine batch/operation/reservation/external obligation blocks, covered separately. Do not revive/repair sessions. |
| LEGACY_WORKBENCH_JOBS | public.banking_pay_workbench_jobs.id | status IN QUEUED,RUNNING,FAILED,DEAD | Preview/source-build/certificate queue bookkeeping is nonblocking after admission retirement and bounded old invocation proof; RUNNING durable identity alone is not proof an old invocation ended. Linked real execution belongs exact financial owner. |
| LEGACY_PROVIDER_TRANSFERS | public.pay_bank_transfers.id | status IN PENDING,PROCESSING,UNKNOWN, or unresolved admitted provider-scope/event proof | PENDING can be only prepared and unsent: distinguish real admission with exact scope/rail proof. SUBMITTED is NOT allowed transfer.status; it is event/provider/operation state. Completed/returned/reverted cash history can still have separate correction obligations. |
| LEGACY_PROVIDER_SCOPES | public.banking_pay_operation_transfer_scope.id | admitted or ambiguous submit state/timestamps/identity, plus claimed active attempt needing bounded-lifetime proof | The table is singular `transfer_scope`, not `transfer_scopes`. Terminal transfer/outcome and exact same request/transaction identity required to close exposure. PROVIDER_REJECTED/CHUNK_FINALISED is stage completion, not unconditional no-money proof. |
| LEGACY_PROVIDER_ATTEMPTS | public.banking_pay_operation_provider_attempts.id | persisted provider attempt linked to unresolved/missing scope identity | Historical attempts linked to positively reconciled scope are history. Do not group all NULL scope IDs together or let a later unrelated row close an earlier admitted request. |
| LEGACY_PROVIDER_EVENTS | public.pay_bank_transfer_events.id | unresolved mapping/correction disposition or currently latest uncertain event for exact transfer/request identity | Mapping AMBIGUOUS/UNMATCHED, correction ACTION_REQUIRED/AMBIGUOUS/BLOCKED/FAILED/AUTO_PROCESSING block. Old SUBMITTED followed by exact authoritative terminal transfer outcome is history. LEGACY_NO_ARTIFACT needs actual no-money proof, not automatic empty classification. |
| LEGACY_PROVIDER_WEBHOOKS | public.bank_provider_webhook_receipts.id | verified/possibly verified unresolved ingestion or matching/review/failure | SIGNATURE_INVALID/IGNORED_EVENT_TYPE are terminal nonfinancial; RECEIVED/VERIFIED/NORMALISED/UNMATCHED_REVIEW_REQUIRED/FAILED_RETRYABLE/FAILED_FINAL need exact disposition. DUPLICATE only closes when original exact event is terminal; INGESTED can have an unresolved event companion. |
| LEGACY_UNBOUND_STORED_CREDITS | public.pay_advances.id | unbound pending positive manual non-taxable oneoff stored credit | Census reports supported candidate shape and disqualifiers without calling bind; noneligible still-positive credit remains genuine in finance category. No general Loan/debt/credit adoption. |
| SOURCE_LOCAL_PENDING | private.weekly_source_local_protected_decision_receipts.publication_request_id | state IN PENDING_FREEZE,PREPARING | Durable PENDING_FREEZE requires exact mapping; PREPARING visible in committed snapshot is integrity review. C1 request RETIRED and orchestration COMPLETE must not filter it out. |
| SOURCE_PENDING_BUNDLES | public.weekly_source_pending_entitlement_bundles.id | state IN PENDING,RELEASING,MANUAL_REVIEW | Exact Source release owner; no automatic exclusion from cleaner C1 or generation state. Include linkage to local receipt by verified pending-bundle identity, not just same candidate. |
| C1_PUBLICATION_PENDING | public.weekly_exceptional_c1_publication_requests.id | state IN READY,SUBMITTED,PENDING,RECOVERY_REQUIRED | READY never submitted with no C1 financial/publication identity is reusable Source-local request bookkeeping. Actual C1/unknown exposure needs reconciliation. source_mode is NHSP_WEEKLY/HEALTHROSTER_WEEKLY, **not** routing C1/SOURCE_LOCAL/NEXT. |
| C1_UNKNOWN | public.weekly_exceptional_c1_unknown_outcomes.id | state RECOVERY_REQUIRED | Durable actual unknown; RECOVERED_COMMITTED/RECOVERED_REPLAYED/REFUSED retain history. Never invoke record/recover owners. |
| NEXT_RUNS | private.bpay_next_pay_run.id | status IN PREPARING,REVIEW,DRAFT,CANCELLING,EXECUTING | Actual unfinished NEXT work; CANCELLED/COMPLETE terminal. |
| NEXT_TRANSFERS_EXTERNAL | private.bpay_next_transfer.id | status IN ISSUED_CSV,SUBMITTED,UNKNOWN | CSV issue is external exposure even without provider submit. |
| NEXT_TRANSFERS_UNFINISHED | private.bpay_next_transfer.id | status IN BUILDING,MEMBERS_READY,DRAFT,SCHEDULED,INTERNAL_PROCESSING | Internal processing is unfinished money, separately from external uncertainty. |
| NEXT_EFFECTS | private.bpay_next_financial_effect.id | all rows | Immutable realised history, no invented REALISED/status column. |
| NEXT_CASE_EVENTS | private.bpay_next_case_event.id | all rows | APPROVED,FUNDED,RECOVERED,PAID,SHORTFALL,WRITTEN_OFF,CORRECTED are history kinds, not open balances. |
| NEXT_WORK_HOLDS | private.bpay_next_hold.id | status ACTIVE | Separate from case holds. |
| NEXT_CASE_HOLDS | private.bpay_next_case_hold.id | status ACTIVE | REALISED/RELEASED terminal; do not release. |

## Data-free fixed SQL predicate blocks

These blocks are SELECT source designs. Shared CTE names are descriptive local names, not proposed database objects. Root must place them into fixed branches/CTEs, close the exact response shape and preserve source provenance. Never interpolate a request category into SQL.

### Direct owner categories

```sql
-- LEGACY_RESERVATIONS
select r.id, r.status from public.pay_advance_reservations r
where r.status in ('RESERVED','COMMITTED');

-- LEGACY_SNOOZES (expiry is a returned classification, not a repair)
select s.id,
       case when s.snooze_until_date is not null
                      and s.snooze_until_date < current_date
            then 'EXPIRED_DATE_UNCLEARED' else 'ACTIVE' end as status
from public.pay_item_snoozes s
where s.cleared_at_utc is null and s.cancelled_at_utc is null;
-- Supported kinds: DO_NOT_PAY,BLOCKED_TIMESHEET,TIMESHEET_PAYMENT,
-- OVERPAYMENT_RECOVERY,PAYMENT_ADVANCE_REPAYMENT,MANUAL_DEBT_RECOVERY.

-- LEGACY_CORRECTION_WORK
select w.id, w.status, w.correction_request_id
from public.pay_payment_correction_work_items w
where w.status in ('PENDING','PROCESSING','BLOCKED','FAILED_RETRYABLE','FAILED_FINAL');

-- LEGACY_CANCELLATION_NOTICES
select r.id, r.status from public.pay_payment_correction_requests r
where r.cancel_notice_tracked is true and r.applied_at_utc is not null
  and r.cancel_notice_reconciled_applied_at_utc is distinct from r.applied_at_utc;

-- SOURCE_LOCAL_PENDING -- DO NOT INNER JOIN requests with nonterminal state
select r.publication_request_id as id, r.state as status,
       r.family_id, r.generation_id, r.common_decision_bundle_id
from private.weekly_source_local_protected_decision_receipts r
where r.state in ('PENDING_FREEZE','PREPARING');

-- SOURCE_PENDING_BUNDLES
select p.id, p.state as status, p.decision_bundle_id
from public.weekly_source_pending_entitlement_bundles p
where p.state in ('PENDING','RELEASING','MANUAL_REVIEW');

-- C1_PUBLICATION_PENDING and C1_UNKNOWN, independently paged
select r.id, r.state as status from public.weekly_exceptional_c1_publication_requests r
where r.state in ('READY','SUBMITTED','PENDING','RECOVERY_REQUIRED');
select u.id, u.state as status, u.publication_request_id
from public.weekly_exceptional_c1_unknown_outcomes u
where u.state='RECOVERY_REQUIRED';

-- LEGACY_WORKBENCH_SESSIONS/JOBS, informational absent genuine companions
select s.id, s.status from public.banking_pay_workbench_sessions s where s.status='OPEN';
select j.id, j.status, j.session_id from public.banking_pay_workbench_jobs j
where j.status in ('QUEUED','RUNNING','FAILED','DEAD');

-- NEXT direct fixed branches
select r.id, r.status from private.bpay_next_pay_run r
where r.status in ('PREPARING','REVIEW','DRAFT','CANCELLING','EXECUTING');
select t.id, t.status from private.bpay_next_transfer t
where t.status in ('ISSUED_CSV','SUBMITTED','UNKNOWN');
select t.id, t.status from private.bpay_next_transfer t
where t.status in ('BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','INTERNAL_PROCESSING');
select e.id, e.effect_kind as status from private.bpay_next_financial_effect e;
select e.id, e.event_kind as status from private.bpay_next_case_event e;
select h.id, h.status from private.bpay_next_hold h where h.status='ACTIVE';
select h.id, h.status from private.bpay_next_case_hold h where h.status='ACTIVE';
```

### Finance and PAYE inputs

```sql
-- LEGACY_FINANCE_CASES. Positive stored balance survives a contradictory marker.
select a.id, a.status::text as status from public.pay_advances a
where not exists (select 1 from private.bpay_next_stored_credit_origin o
                  where o.legacy_case_id=a.id)
  and (a.outstanding_amount>0
       or (a.payout_status::text='PENDING' and a.original_amount>0
           and a.cleared_at_utc is null and a.written_off_at_utc is null)
       or exists (select 1 from public.pay_finance_case_components c
                  where c.finance_case_id=a.id and c.remaining_source_amount>0
                    and c.closed_at_utc is null)
       or exists (select 1 from public.pay_advance_reservations r
                  where r.finance_case_id=a.id and r.status in ('RESERVED','COMMITTED')));
-- LOAN/PAYMENT_ADVANCE; OVERPAYMENT/LEGACY_ADVANCE/UNDERPAYMENT and manual
-- debt/credit are distinguished by actual advance_kind/case_type, not aliases.

-- LEGACY_PAYE_NETS, root = candidate UUID; input existence is not net_amount>0.
select c.id, coalesce(c.paye_state,'UNSPECIFIED') as status, c.pay_batch_id
from public.pay_batch_candidates c join public.pay_batches b on b.id=c.pay_batch_id
where b.execution_commit_state='NOT_SUBMITTED'
  and b.status not in ('SETTLED','CANCELLED')
  and (c.awaiting_net_amount is true or c.paye_state='PENDING_NET'
       or exists (select 1 from public.pay_batch_paye_net_inputs n
                  where n.pay_batch_candidate_id=c.id));
-- SUBMITTED_NOT_COMMITTED candidates remain represented under batch/provider
-- exposure; COMMITTED/settled captured inputs are history, not new net input.

-- LEGACY_UNBOUND_STORED_CREDITS: discover genuine candidate credits broadly;
-- eligibility for the existing narrow bind owner is separate, never inferred.
select a.id, a.payout_status::text as status from public.pay_advances a
where a.case_type::text='MANUAL_CREDIT_ADJUSTMENT'
  and a.taxability::text='NON_TAXABLE'
  and a.routing_kind::text='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
  and a.payout_status::text='PENDING'
  and (a.outstanding_amount>0 or a.original_amount>0)
  and a.cleared_at_utc is null and a.written_off_at_utc is null
  and not exists (select 1 from private.bpay_next_stored_credit_origin o
                  where o.legacy_case_id=a.id);
```

Exact bind eligibility must copy *all* read facts of `04102026_1827_banking_pay_next_candidate_destination_group_v1.sql:31–64`: active candidate currently PAYE; ACTIVE untouched positive principal; oneoff bank required and matching bank record/hash; no payout batch/transfer/clear/write-off; exactly one component, original UMBRELLA REIMBURSEMENT_GROSS_FIXED CASE_TOTAL/TOTAL, family `case:<id>`, no linked timesheet/closed component, remaining=source=original, matching source-basis type/taxability/routing; no RESERVED/COMMITTED reservation; genuine COMPONENT_CREATED event with MANUAL_CREDIT_ADJUSTMENT_CREATE; finite creation/bank dates. Use these only for a fixed `supported_existing_owner` boolean if wanted; do not return bank data or call `_bank_hash`/bind merely to manufacture receipts. Unsupported positive credit is not obsolete. An existing origin is evidence of actual prior adoption; do not subtract reservations/provider/corrections merely because origin exists.

### Exact mail/comms scope and external identity

`private.bpay_next_legacy_email_is_factual_v1(text,text,text)` in migration `04102026_0504` is IMMUTABLE/non-STRICT and safe for read-only metadata. Its complete trim set, ASCII folding and C collation match the Worker `broker/src/bpay-next-legacy-email.js`. **Its negation means held/unidentifiable, not necessarily financial.** Positive financial markers are type REMITTANCE/PAYMENT_CANCELLATION; contexts PAY_BATCH/PAY_BATCHES/PAY_BATCH_REMITTANCE/PAY_PAYMENT_CORRECTION_REQUESTS; or reference prefix REMITTANCE, PAYOUT_NOTICE:, PAY_AUTH_INVITE:, PAY_BATCH_PROCESSING:, PAY_BATCH_SCHEDULED:, PAY_BATCH_COMPLETED:, PAYMENT_CANCELLATION_NOTICE_V1:. A marker wins even with missing context_id. Unknown metadata without a marker needs an integrity/held-mail report; do not call all such Source/Invoice/Candidate rows Banking obligations. `payment_scope_json IS NOT NULL` is forbidden as a global classifier.

```sql
-- Same exact normalization for every string used in the fixed mail classifier.
-- Documented expression N(x), NOT a new function:
pg_catalog.translate(pg_catalog.btrim(coalesce(x,''),
  U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF'),
  'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ') collate "C"

-- Positive marker P(type,context,reference), expanded as fixed expressions:
N(type) in ('REMITTANCE','PAYMENT_CANCELLATION')
or N(context) in ('PAY_BATCH','PAY_BATCHES','PAY_BATCH_REMITTANCE',
                 'PAY_PAYMENT_CORRECTION_REQUESTS')
or pg_catalog.starts_with(N(reference),'REMITTANCE')
or pg_catalog.starts_with(N(reference),'PAYOUT_NOTICE:')
or pg_catalog.starts_with(N(reference),'PAY_AUTH_INVITE:')
or pg_catalog.starts_with(N(reference),'PAY_BATCH_PROCESSING:')
or pg_catalog.starts_with(N(reference),'PAY_BATCH_SCHEDULED:')
or pg_catalog.starts_with(N(reference),'PAY_BATCH_COMPLETED:')
or pg_catalog.starts_with(N(reference),'PAYMENT_CANCELLATION_NOTICE_V1:')

-- mail scoped CTE: include P(m.type,m.context_kind,m.reference), plus exact
-- deterministic_outbox_key prefixes REMITTANCE:BATCH:, REMIT:PAY_BATCH:,
-- PAYOUT_NOTICE:PAY_BATCH: and PAYMENT_CANCELLATION_NOTICE_V1:.
-- Historical summary owner additionally links payment_scope_json.pay_batch_id
-- only through EXISTS real pay_batches b WHERE b.id::text=that text.
-- Match text to the UUID column rendered as text; do not cast unchecked JSON.
-- payment_scope_json presence alone is never sufficient.

-- Unsatisfied result predicate on financial mail/comms:
sent_at is null and delivered_at is null and read_at is null
and status::text not in ('SENT','DELIVERED','READ')

-- Unresolved external admission evidence (independent of current lease expiry):
(attempt_lease_token is not null or attempt_leased_at_utc is not null
 or attempt_lease_expires_at_utc is not null
 or nullif(pg_catalog.btrim(provider_message_id),'') is not null)
-- mail adds N(provider_status) IN ('ACCEPTED','SENT','SUCCESS','OK').
-- Combine with unresolved result above; accepted identity without result blocks.
-- For sent financial source mail, also retain:
cancel_notice_tracked is true and sent_at is not null
and cancel_notice_reconciled_sent_at_utc is distinct from sent_at
```

For `comms_outbox`, do not use channel as type. Extract optional financial `provider_payload_json.type` / email_type metadata only with `jsonb_typeof(...)=string`; `context_kind` is a real text column. Reference is `coalesce(provider_payload_json->>'reference', provider_payload_json->>'mail_reference','')`. Deterministic identity is `coalesce(deterministic_outbox_key, provider_payload_json->>'deterministic_outbox_key', provider_payload_json->>'deterministicOutboxKey','')`. Also use an exact real pay_batches join for `coalesce(provider_payload_json#>>'{payment_scope_json,pay_batch_id}',provider_payload_json->>'pay_batch_id','')`. These are the current batch summary owner's paths at `26052026_2100HRS_NEW_FUNCTIONS.sql:7921–8005`; missing JSON type/corrupt id is a scope-review fact, not a safe cast or fictitious batch. Reference/control-vs-advice classification must preserve the same positive marker priority.

Pending advice targets must preserve current item-flags semantics at `26052026_2100HRS_NEW_FUNCTIONS.sql:7843–7918`: real nonvoided items excluding DEBT_CREATED; no applied PRE_BANK_CANCEL/NO_MONEY_UNWIND/SETTLED_REVERSAL correction; captured generates_candidate_payment_advice/routing and finance-register classification; actual umbrella transfer/candidate remittance targets. Do not take every candidate with null remittance_sent_at as a due advice target. Apply successful exact remittance evidence from target/result owners to distinguish missing marker vs missing send. Candidate `remittance_trigger_status='SENT_VIA_MAIL_OUTBOX'` is also explicit retained successful result evidence (`26052026_2100HRS_NEW_FUNCTIONS.sql:4262–4283`), whereas QUEUED_TO_MAIL_OUTBOX/MAIL_OUTBOX_DUPLICATE_EXISTS are queue-only. `SENT` status is historical terminal evidence even if redundant timestamps are missing; contradictory status+active attempt needs integrity review. No call to drain, queue, send, markSent, cancellation reconciliation or retry.

The exact cancellation-source reconciliation owner requires **all** `cancel_notice_tracked=true`, type REMITTANCE, status SENT, sent_at nonnull, provider_status ACCEPTED, and reconciled_sent_at IS DISTINCT FROM sent_at (`07092026_2301:338–347,494–502`). The broader tracked/accepted discrepancy predicate above deliberately retains contradictions for integrity review; only this complete exact owner shape is a genuine actionable cancellation-source mapping. Do not filter out future next_attempt dates: due time changes scheduling, not whether the obligation exists.

### Provider admission and outcome owners

```sql
-- Positive admitted/ambiguous scope evidence A(s); static, exact saved columns.
s.provider_submit_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL',
 'PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED',
 'CHUNK_FINALISED')
or s.provider_request_sending_at_utc is not null
or s.provider_request_sent_at_utc is not null
or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null
or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null
or s.provider_submit_attempt_count>0
-- CLAIMED / REQUEST_PREPARING / claimed_at identity also requires external
-- lifetime proof, but alone is not proof a provider received a request.

-- Provider transfer candidate root (do not invent SUBMITTED transfer.status).
select t.id,t.status,t.pay_batch_id from public.pay_bank_transfers t
where t.status in ('PENDING','PROCESSING','UNKNOWN')
   or exists (select 1 from public.banking_pay_operation_transfer_scope s
              where s.pay_bank_transfer_id=t.id and /* A(s) and unresolved */);

-- Scope root: positively reconciled terminal outcome uses SAME exact transfer
-- and request/transaction identities. Scope status FAILED/SKIPPED, response_at,
-- PROVIDER_REJECTED or CHUNK_FINALISED alone is not financial finality.
select s.id,s.provider_submit_state as status,s.operation_id,s.pay_bank_transfer_id
from public.banking_pay_operation_transfer_scope s
where /* A(s) without exact terminal financial reconciliation */
   or s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING');

-- Transfer events needing actual owner disposition regardless transfer status.
select e.id,e.normalised_state as status,e.pay_bank_transfer_id,
       e.provider_webhook_receipt_id from public.pay_bank_transfer_events e
where e.mapping_status in ('AMBIGUOUS','UNMATCHED')
   or e.correction_disposition in ('ACTION_REQUIRED','AMBIGUOUS','BLOCKED',
                                  'FAILED','AUTO_PROCESSING')
   or (e.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
       and /* no later exact authoritative outcome for same transfer/request */);

-- Webhook persisted roots. No raw signature/header/payload leaves the RPC.
select w.id,w.status from public.bank_provider_webhook_receipts w
where w.status in ('RECEIVED','VERIFIED','NORMALISED','UNMATCHED_REVIEW_REQUIRED',
                   'FAILED_RETRYABLE','FAILED_FINAL')
   or (w.status='DUPLICATE' and /* original exact provider event unresolved */)
   or (w.status='INGESTED' and exists (
       select 1 from public.pay_bank_transfer_events e
       where e.provider_webhook_receipt_id=w.id
         and (e.mapping_status in ('AMBIGUOUS','UNMATCHED')
              or e.correction_disposition in ('ACTION_REQUIRED','AMBIGUOUS',
                                              'BLOCKED','FAILED','AUTO_PROCESSING'))));
```

`COMPLETED`, `RETURNED`, `REVERTED` are positive cash-history outcomes, never absence of payment. A no-money disposition must use the actual event movement classification (PRE_BANK_CANCEL/NO_MONEY_UNWIND/PROVIDER_CANCELLED_NO_MONEY/PROVIDER_FAILED_NO_MONEY), exact scope/transfer identity and no uncertain companion. An old accepted event must not be reopened just because it exists, but failure/decline/reject alone is not sufficient evidence to discard a submission. Exact terminal identity comparison must tolerate real owner-approved missing-provider-id cases, using that owner's already persisted matching method; it cannot invent an id or accept amount-only ambiguity. `pay_bank_event_ingest` is VOLATILE and prohibited here.

Use `banking_pay_operation_provider_attempts.transfer_scope_id -> banking_pay_operation_transfer_scope.id` and exact request/transaction/idempotency identity. Read source states `previous_state/new_state` as stage facts. An attempt whose scope is missing/mismatched is INTEGRITY_REVIEW; a scope's actual terminal outcome can close its historical attempts. Do not use DISTINCT ON nullable scope/request without a tie-breaking actual identity, group all NULL attempts together, or assume highest UUID/newest unrelated attempt supersedes all older actions. Expose outstanding settlement scope rows (`banking_pay_operation_settlement_scope.status IN PENDING,FAILED`) under their parent operation; root may add a fixed separate UUID category when helpful. This prevents runner COMPLETE from hiding a pending financial settlement.

The following exact positive closure is a conservative read design for terminal **external exposure**, not settlement/correction/advice completion. It requires an already authoritative persisted matching event for the actual transfer and exact admitted identities. A terminal transfer with no matching event can still be genuine cash history; classify its missing proof as INTEGRITY_REVIEW instead of inventing an event. This avoids treating every historical provider attempt as open while retaining contradictory or differently identified attempts. No fresh provider query or writer is called.

```sql
-- scope_terminal_proof: fixed SELECT, all IDs/columns in current-contract.
select s.id as scope_id
from public.banking_pay_operation_transfer_scope s
join public.pay_bank_transfers t
  on t.id=s.pay_bank_transfer_id and t.pay_batch_id=s.pay_batch_id
where exists (
  select 1 from public.pay_bank_transfer_events e
  where e.pay_bank_transfer_id=t.id and e.pay_batch_id=t.pay_batch_id
    and e.mapping_status='MATCHED'
    and (e.event_source<>'PROVIDER_WEBHOOK' or e.provider_signature_valid is true)
    and (nullif(pg_catalog.btrim(s.provider_request_id),'') is null
         or e.provider_request_id=s.provider_request_id)
    and (nullif(pg_catalog.btrim(s.provider_transaction_id),'') is null
         or e.provider_transaction_id=s.provider_transaction_id)
    and (s.provider_request_sending_at_utc is null
         or e.received_at_utc>=s.provider_request_sending_at_utc)
    and (
      (t.status='COMPLETED' and t.completed_at_utc is not null
       and e.normalised_state='COMPLETED')
      or (t.status in ('RETURNED','REVERTED')
          and e.normalised_state=t.status)
      or (t.status in ('FAILED','DECLINED','REJECTED','CANCELLED','VOIDED',
                       'SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
          and e.normalised_state in ('FAILED','DECLINED','REJECTED','CANCELLED')
          and e.movement_classification in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND',
            'PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY'))
    )
    -- A later exact unknown/pending event must not be hidden by an older final.
    and not exists (
      select 1 from public.pay_bank_transfer_events later
      where later.pay_bank_transfer_id=t.id and later.id<>e.id
        and later.received_at_utc>=e.received_at_utc
        and later.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
        and (nullif(pg_catalog.btrim(s.provider_request_id),'') is null
             or later.provider_request_id=s.provider_request_id)
        and (nullif(pg_catalog.btrim(s.provider_transaction_id),'') is null
             or later.provider_transaction_id=s.provider_transaction_id)
    )
);

-- An attempt is history only if its own exact scope AND identity are covered.
-- root can use this NOT EXISTS proof for LEGACY_PROVIDER_ATTEMPTS eligibility.
select a.id,a.new_state as status,a.transfer_scope_id
from public.banking_pay_operation_provider_attempts a
where not exists (
  select 1 from public.banking_pay_operation_transfer_scope s
  join scope_terminal_proof p on p.scope_id=s.id
  where s.id=a.transfer_scope_id and s.operation_id=a.operation_id
    and s.pay_batch_id=a.pay_batch_id
    and (nullif(pg_catalog.btrim(a.provider_request_id),'') is null
         or a.provider_request_id=s.provider_request_id)
    and (nullif(pg_catalog.btrim(a.provider_transaction_id),'') is null
         or a.provider_transaction_id=s.provider_transaction_id)
    and (nullif(pg_catalog.btrim(a.provider_idempotency_key),'') is null
         or a.provider_idempotency_key=s.provider_idempotency_key)
);
```

`public._pay_rail_state_money_movement_classify` is a real owner-derived classifier, but its saved declaration defaults VOLATILE (no STABLE/IMMUTABLE declaration) and SECURITY DEFINER `search_path=public`. Do not call it from this new STABLE census or alter its historical contract to make the design compile. The persisted event normalised_state/movement_classification is the correct read seam. Its derived UNKNOWN remains uncertain; an accepted/provider-success status is not a paid result.

A same-timestamp uncertain event above deliberately defeats terminal proof: UUID ordering can make a deterministic page, but cannot establish authoritative event chronology. Null/contradictory timestamps or identities remain integrity facts. Current unresolved correction disposition is evaluated independently even when a money outcome positively closes external exposure.

### Parent categories and positive shell separation

```sql
-- Required batch root eligibility, regardless declared terminal status:
select b.id,b.status from public.pay_batches b
where b.status not in ('SETTLED','CANCELLED')
   or b.execution_commit_state='SUBMITTED_NOT_COMMITTED'
   or exists (select 1 from public.pay_advance_reservations r
              where r.pay_batch_id=b.id and r.status in ('RESERVED','COMMITTED'))
   or exists (select 1 from public.pay_batch_candidates c
              where c.pay_batch_id=b.id and c.settlement_status in ('PARTIAL','FAILED'))
   or /* unresolved exact transfer/scope/correction/advice companions */;

select o.id,o.status from public.banking_pay_operations o
where o.status not in ('COMPLETE','FAILED','CANCELLED','REVIEW_REQUIRED')
   or (o.lease_owner is not null and o.lease_expires_at_utc>statement_timestamp())
   or exists (select 1 from public.banking_pay_operation_settlement_scope s
              where s.operation_id=o.id and s.status in ('PENDING','FAILED'))
   or /* unresolved exact provider scopes/attempts belonging to o.id */;

select r.id,r.status from public.pay_payment_correction_requests r
where r.status not in ('APPLIED','REJECTED','CANCELLED')
   or exists (select 1 from public.pay_payment_correction_work_items w
              where w.correction_request_id=r.id
                and w.status in ('PENDING','PROCESSING','BLOCKED',
                                 'FAILED_RETRYABLE','FAILED_FINAL'))
   or (r.cancel_notice_tracked and r.applied_at_utc is not null
       and r.cancel_notice_reconciled_applied_at_utc is distinct from r.applied_at_utc);
```

Before assigning OBSOLETE_BOOKKEEPING to a batch/preview operation/session/job, positively establish all of: no SUBMITTED_NOT_COMMITTED/real execution commit; no external submit evidence (scope sending/sent/accepted/ambiguous, provider attempts/events or CSV issue); no RESERVED/COMMITTED finance reservation; no unresolved settlement/correction; no actual captured PAYE net; no paid/partial candidate evidence or required payout/advice; no associated pending Source obligation. Merely `total_bank_out=0`, FAILED runner, expired lease, empty NEXT or no provider_tx_id is inadequate. Preview totals/nonvoided Draft items alone are not cash debt. Authorised/scheduled work with genuine committed inputs and no completed disposition is reported for its real owner, without reviving Draft. A parent may be informational while its separately paged companion blocks; aggregate database_ready must see the companion whole count.

Exact additional authorisation proof is `public.pay_batch_auth_requests.state='AUTHORISED'`, finalised_at_utc and its actual execution_intent_json; the real states are AWAITING/AUTHORISED/REJECTED/CANCELLED. Do not assume an old live token or an auth request in AWAITING means money was submitted, and never return/read token values. An AUTHORISED request, existing CSV export or real scheduled execution is enough to retain a row for genuine owner review even before submission; no old auth/Golden Key flow is to be operated. For a provably unused DRAFT/DRAFT_CREATED shell require NOT_SUBMITTED, no authoritative auth/CSV/schedule/executing/monzo-confirm evidence and **every** listed companion absence. Never assign this label to an arbitrary FAILED/SETTLED/CANCELLED row solely by status.

Completion-notice eligibility must also cover a failed/missing queue insertion, even with `completion_notice_queued_at_utc IS NULL`: a nonnull last_attempt/next_attempt/reference/result marker with completion_notice_result OUTBOX_QUEUE_INCOMPLETE retains an obligation. Successful QUEUED/ALREADY_COVERED_BY_DETERMINISTIC_KEY means coverage only. Match actual outbox rows by context_kind pay_batches/context_id and exact completion_notice_reference/deterministic key; all corresponding required rows must have a terminal sent result. One sent recipient does not prove all recipients sent. The current marker includes recipient_count, outbox_rows_covered_count and bounded inserted IDs; if the expected recipient set cannot be proved from durable rows/marker, classify INTEGRITY_REVIEW, never assume no remaining advice.

## Source identity and pending integrity

Local PENDING_FREEZE is tied to publication_request_id, request_sha256, generation_id/family_id, common_decision_bundle_id/revision, root_timesheet_id and saved result. For bounded page enrichment use the exact saved `result_json#>>'{publication,pending_bundle_verified}'` text to join `weekly_source_pending_entitlement_bundles.id::text`; never cast unchecked result JSON. Then verify bundle decision/revision, candidate, deciding actor and root/version mapping against saved request fields. Missing/mismatched bundle or a RELEASED bundle whose receipt hook did not complete the local receipt is integrity review. A SUPERSEDED bundle can still be allowed saved-status evidence and is not permission to erase its pending local receipt. The existing trigger `weekly_source_local_protected_pending_released_v1` is the release bookkeeping writer; the census must not execute it.

Current Source local completion uses a RETIRED publication request and COMPLETE orchestration run while the private receipt is PENDING_FREEZE. The release hook updates that exact receipt to COMPLETE only after a real common-head release receipt. Public request/run terminal flags cannot substitute for it. Current owner/routing is DB-selected; `source_mode` on C1 publication requests only means NHSP_WEEKLY/HEALTHROSTER_WEEKLY. C1 READY rows with no admitted C1 identity can be completed by the preserved Source-local branch; actual old unknown recovery is independently blocking until its real owner settles it. `weekly_exceptional_c1_source_records` is evidence storage, not unknown ownership.

Shared Source finalisation run/task READY/RUNNING/RECOVERY_REQUIRED/ACTION_REQUIRED and pending reconciliation targets ACTIVE may be useful supplemental informational summaries. They are continuing shared Source owner work and are not automatically legacy Banking obligations merely because their old names mention C1. Their exact admitted C1/unknown or retained local pending companions remain blocking in the explicit categories. Preserve the newer Source-local first-authorise/completion/projection branches and PT409 refusal; never call prepare/stage/complete/recover/write functions from F-18.

## External quiet-window coverage and limits

The accepted OR-45 builder decision is a reviewed deploy-before-switch release that permanently closes retired legacy *new* external admission while the database remains LEGACY, preserving only narrowly proven result reconciliation for already admitted identities. No new configuration, switch screen, credential, queue or intermediate LEGACY→DISABLED is introduced. F-18 is one factual input, not the external barrier itself.

| Persisted identity | What this census can show | What is still required outside F-18 |
|---|---|---|
| operation/root/chunk/transfer-scope UUID, provider request/idempotency/transaction IDs, sending/sent/response timestamps | Real admitted/claimed attempt, pending/unknown cash, unacknowledged settlement, exact terminal reconciliation | Version/admission closure and bounded old request/scheduled/queue/waitUntil/outbound lifetime, or per-identity terminal result proof. Claim/lease expiry is not death proof. |
| mail/comms outbox UUID, deterministic key, attempt token/leased/expiry, provider message/accepted status, sent/delivered/read result | Unsent genuine advice vs accepted-but-unrecorded send; cancellation source/reconcile ownership | Close all automatic/manual/direct/batch/retry/new send paths; reconcile exact already admitted sends without sending a second advice. Queued counts alone do not cover unpersisted send. |
| webhook receipt UUID, provider event key/ID, signature-valid fact and matched event UUID | Exact received/unmatched/failed/review ingestion and its money/correction companion | Continue signature-verified result ingestion only for genuine prior admitted transfers; prohibit branches that generate new payments/reissue/mail. Receipt acceptance itself is not cash finality. |
| Workbench session/job, operation/chunk/settlement UUID | Durable queue-owned unfinished work and positive shell/financial separation | Pending queue-only messages with no durable root and old active consumers are invisible to SQL. Admission/queue lifecycle must prove them retired/terminal under actual existing authorities. |
| Source local publication request/family/generation/bundle UUID | Genuine pending accepted local target and actual C1 recovery facts | Explicit mapping only if observed real obligations exist. No old fixtures, guessed adoption, lost paid truth or premature switch. |

The real sites include normalized legacy financial HTTP admission; banking/workbench operation cron plus minute/certificate/queue wakes and direct fallbacks; final rail submission backstop; per-row/batch/manual payment mail plus retry/markOutcome/purpose paths; provider polling/replay/webhook reconciliation. A fresh owner read immediately before a send cannot revoke an already admitted network action. The database's FOR SHARE legacy write fence serializes owner changes with admitted DB transactions; it does not recall outbound provider/mail actions or unseen queue messages.

No arbitrary sleep duration is accepted as a quiet-window proof. A `/pay` 1–120-second fetch timeout alone is not the lifetime of its enclosing operation, retries, waitUntil, scheduled or email action. Prove a finite bound for **every** admitted old surface, including durable retries/DLQ/queue continuations, or close the exact persisted admitted identity through its real result owner. Deploy success is not proof old invocations ended. Expose `external_quiet_window: UNPROVEN` (or equivalent fixed non-success fact) until the separately authored F-17/release boundary has actual proof; `database_ready=true` must never silently mean activation ready.

F-17's owner transition must compare expected epoch, serialize against the actual module-control authority, and re-run full relevant readiness before changing the setting. The exact process must avoid presenting a STABLE RPC snapshot taken before waiting on a writer lock as a fresh post-barrier census: use the existing transaction/command boundary that obtains the lock first and then a fresh query snapshot. This is an implementation/release proof, not a new approval flow. D05 remains explicitly partial until F-17/F-18 source and the external-boundary proof are complete. No hosted activation or reconciliation is authorised by this design.

## Source evidence and build follow-through

Contract SHA-256 `2e8c1a727c022b403fb7663ca1c5724bf28b262f462de7c3e079cfd7abce12e2`; companion schema SHA-256 `bfbaaa79ad339c4ca4a9caaa5af14ef89d2489663ff03f64711afb242bf164e5`; exact mail metadata migration SHA-256 `dc5fac7e18364a698ff35acbbc1656b76fc5077e532946d5a920b27d4a7dd308`. These are raw saved-file identities, not catalogue definition hashes or installed proof.

Root authors the repeatable, canonical final-definer entry and source contract; resolves each explicitly marked companion/disposition predicate through the cited existing real owner; then obtains the permitted local compile/catalogue/source-gate checks itself. The SELECT designs containing symbolic N/P/A or marked companion comments are intentionally documentation, not claimed runnable SQL. Review the completed source for closed JSON/service ACL, static allowlists, actual column types, whole blocking counts, bounded IDs/bytes, no mutation helpers, pending receipt ownership, terminal evidence and quiet-window limits. Installed/app-tested/hosted counts remain unset until their separately authorised real observation. Empty categories return actual zero/EMPTY without requiring a fixture or carry-over answer.

## Finite companion closure appendix — supersedes earlier symbolic companion comments

Source-only, 9 October 2026. These are complete data-free SELECT designs, never executed. The names `private.bpay_next_readiness_marker_v1`, `scope_terminal_v1`, `event_open_v1`, `event_terminal_v1` and `unused_batch_v1` refer to the corresponding full `private.bpay_next_readiness_*` helpers in the builder's saved 2109 repeatable, not installed functions. Each dependency's exact proof must retain the review corrections below. No unspecified N/P/A or generic companion placeholder is used here.

### Provider scope and attempt classes

Sending/sent timestamps, accepted/rejected/unknown provider states or a transaction identity retain an external admission. A claimed/preparing lease, request ID or attempt count alone retains integrity review. The actual untouched states are NOT_READY/READY (`01062026_0957_changes_to_banking.sql:615–640`), not PENDING/NOT_STARTED/UNPREPARED. Expired leases are not death proof.

```sql
with scopes as (
 select s.*,
   (s.provider_request_sending_at_utc is not null
    or s.provider_request_sent_at_utc is not null
    or nullif(btrim(s.provider_transaction_id),'') is not null
    or s.provider_submit_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL',
       'PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN')) as admitted,
   private.bpay_next_readiness_scope_terminal_v1(s.id) as closed
 from public.banking_pay_operation_transfer_scope s
), selected as (
 select s.* from scopes s where not s.closed and
  (s.admitted or s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING',
    'REVIEW_REQUIRED','CHUNK_FINALISED')
   or nullif(btrim(s.provider_request_id),'') is not null
   or s.provider_submit_attempt_count>0
   or s.provider_submit_claimed_at_utc is not null
   or s.provider_response_at_utc is not null)
)
select s.id,s.provider_submit_state as status,
 case when s.admitted then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end
   as classification,true as blocking
from selected s;

select a.id,a.new_state as status,
 case when exists (
   select 1 from public.banking_pay_operation_transfer_scope s
   where s.id=a.transfer_scope_id and s.operation_id=a.operation_id
     and s.pay_batch_id=a.pay_batch_id
     and (nullif(btrim(a.provider_request_id),'') is null
          or a.provider_request_id=s.provider_request_id)
     and (nullif(btrim(a.provider_transaction_id),'') is null
          or a.provider_transaction_id=s.provider_transaction_id)
     and (nullif(btrim(a.provider_idempotency_key),'') is null
          or a.provider_idempotency_key=s.provider_idempotency_key)
     and (s.provider_request_sending_at_utc is not null
       or s.provider_request_sent_at_utc is not null
       or nullif(btrim(s.provider_transaction_id),'') is not null
       or s.provider_submit_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL',
          'PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN'))
 ) then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end
   as classification,true as blocking
from public.banking_pay_operation_provider_attempts a
where not exists (
 select 1 from public.banking_pay_operation_transfer_scope s
 where s.id=a.transfer_scope_id and s.operation_id=a.operation_id
   and s.pay_batch_id=a.pay_batch_id
   and private.bpay_next_readiness_scope_terminal_v1(s.id)
   and (nullif(btrim(a.provider_request_id),'') is null
        or a.provider_request_id=s.provider_request_id)
   and (nullif(btrim(a.provider_transaction_id),'') is null
        or a.provider_transaction_id=s.provider_transaction_id)
   and (nullif(btrim(a.provider_idempotency_key),'') is null
        or a.provider_idempotency_key=s.provider_idempotency_key)
);
```

### Event integrity must not disappear behind an absence test

The saved event_open helper already covers uncertain history and its strictly later positive terminal witness. Add the following fixed integrity branch for terminal-looking events that cannot prove the exact existing transfer/batch outcome. A webhook is an additional owner, not a prerequisite for retaining a corrupt event. LEGACY_NO_ARTIFACT no-money evidence is allowed only for no-money terminal normalised states; it cannot make a contradictory COMPLETED/RETURNED/REVERTED event disappear.

```sql
select e.id,e.normalised_state as status,
 case when e.mapping_status='MATCHED'
        and (e.event_source<>'PROVIDER_WEBHOOK'
             or e.provider_signature_valid is true)
        and exists(select 1 from public.pay_bank_transfers t
          where t.id=e.pay_bank_transfer_id and t.pay_batch_id=e.pay_batch_id)
        and e.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
   then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end
   as classification,true as blocking
from public.pay_bank_transfer_events e
where private.bpay_next_readiness_event_open_v1(e.id)
 or (e.mapping_status='MATCHED'
     and e.normalised_state in ('COMPLETED','RETURNED','REVERTED','FAILED',
       'DECLINED','REJECTED','CANCELLED')
     and not private.bpay_next_readiness_event_terminal_v1(e.id))
 or (e.mapping_status='LEGACY_NO_ARTIFACT'
     and e.normalised_state in ('COMPLETED','RETURNED','REVERTED'))
 or (e.event_source='PROVIDER_WEBHOOK'
     and e.provider_signature_valid is distinct from true);
```

### Whole webhook constituent and result coverage

The Worker key fallback is provider_event_key/idempotency_key/event_key, in that order after trimming (`broker/src/index.js:56635–56642`). The unique delivery row is not a constituent event. The following fixed read design retains malformed/conflicting expected events and missing/nonterminal ingest results as integrity review. Add the positive terminal bank-event witness independently: INGESTED/DUPLICATE is only transport ingestion, not paid/no-money completion. All comparisons are text, avoiding casts of unchecked provider JSON.

```sql
with expected as (
 select w.id as receipt_id,n.value as event_json,
  coalesce(nullif(btrim(n.value->>'provider_event_key'),''),
           nullif(btrim(n.value->>'idempotency_key'),''),
           nullif(btrim(n.value->>'event_key'),'')) as event_key
 from public.bank_provider_webhook_receipts w
 cross join lateral jsonb_array_elements(case
  when jsonb_typeof(w.normalised_events_json)='array'
  then w.normalised_events_json else '[]'::jsonb end) n(value)
 where n.value->'ignored' is distinct from 'true'::jsonb
), results as (
 select w.id as receipt_id,r.value as result_json,
  coalesce(nullif(btrim(r.value->>'provider_event_key'),''),
           nullif(btrim(r.value->>'idempotency_key'),''),
           nullif(btrim(r.value->>'event_key'),'')) as event_key
 from public.bank_provider_webhook_receipts w
 cross join lateral jsonb_array_elements(case
  when jsonb_typeof(w.ingest_results_json)='array'
  then w.ingest_results_json else '[]'::jsonb end) r(value)
)
select w.id,w.status,'INTEGRITY_REVIEW'::text as classification,true as blocking
from public.bank_provider_webhook_receipts w
where w.status in ('RECEIVED','VERIFIED','NORMALISED',
 'UNMATCHED_REVIEW_REQUIRED','FAILED_RETRYABLE','FAILED_FINAL')
 or (w.status in ('INGESTED','DUPLICATE') and (
   w.signature_valid is distinct from true or w.processed_at_utc is null
   or jsonb_typeof(w.normalised_events_json) is distinct from 'array'
   or jsonb_typeof(w.ingest_results_json) is distinct from 'array'
   or exists(select 1 from expected n where n.receipt_id=w.id and (
      jsonb_typeof(n.event_json)<>'object' or n.event_key is null
      or (select count(*) from expected dup where dup.receipt_id=w.id
             and dup.event_key=n.event_key)<>1
      or not exists(select 1 from results r
        where r.receipt_id=w.id and r.event_key=n.event_key
          and r.result_json->'ok'='true'::jsonb
          and upper(btrim(r.result_json->>'status')) in ('INGESTED','DUPLICATE')
          and (n.event_json->>'provider_request_id' is null
            or r.result_json->>'provider_request_id'=n.event_json->>'provider_request_id')
          and (n.event_json->>'provider_transaction_id' is null
            or r.result_json->>'provider_transaction_id'=n.event_json->>'provider_transaction_id'))
      or not exists(select 1 from public.pay_bank_transfer_events e
        where e.provider_key=w.provider_key and e.rail_env=w.rail_env
          and e.provider_event_key=n.event_key
          and private.bpay_next_readiness_event_terminal_v1(e.id)
          and (n.event_json->>'provider_request_id' is null
            or e.provider_request_id=n.event_json->>'provider_request_id')
          and (n.event_json->>'provider_transaction_id' is null
            or e.provider_transaction_id=n.event_json->>'provider_transaction_id'))
   ))
   or exists(select 1 from public.pay_bank_transfer_events e
     where e.provider_webhook_receipt_id=w.id
       and private.bpay_next_readiness_event_open_v1(e.id))
 ));
```

An empty expected-event set is not a financial receipt. Only an exact verified IGNORED_EVENT_TYPE owner disposition with processed_at and genuinely ignored/nonfinancial events establishes that branch; malformed JSON, absent arrays or an INGESTED label alone remains integrity review. Normalized request/transaction fields must also have string/null JSON shape, not silently accept an object converted to text. Two distinct results with one key or a stored failed result contradicted by another success require integrity review. No extra unique delivery row is invented.

### Batch/operation, PAYE and mail substitutions to the saved repeatable

Use these exact finite substitutions, preserving whole-category eligibility/counts:

- Captured PAYE net proves unfinished input only with `b.execution_commit_state='NOT_SUBMITTED'` and `b.status NOT IN ('SETTLED','CANCELLED')`. This includes an actual captured zero. Historical input on a committed/closed batch defeats “unused”, but is not current money. The saved PAYE candidate branch correctly uses a real input, net_bank_amount IS NOT NULL, AUTHORISED/scheduled/executing/reserved/actual candidate transfer for the input/no-input split; a no-input contradiction is INTEGRITY_REVIEW.
- Parent provider positive is the admitted scope predicate above AND NOT scope_terminal, exact open admitted attempt, or trusted matched uncertain real transfer event. CLAIMED/REQUEST_PREPARING/request-only audit or WAITING_PROVIDER alone is INTEGRITY_REVIEW. RESERVED/COMMITTED reservations, PENDING/FAILED settlement, PARTIAL/FAILED candidate settlement, unexecuted AUTHORISED intent and real unsent required advice remain genuine owner work. SENT mail/APPLIED corrections/completed scopes and transfers are history. A declared COMPLETE operation does not suppress its independent open companions.
- For a DRAFT_CREATE/PREVIEW_REFRESH operation, OBSOLETE_BOOKKEEPING requires no admitted/integrity scope or attempt belonging to that operation, no PENDING/FAILED settlement scope, and its actual pay_batch_id either NULL or positively unused through unused_batch. A null batch with a reserved correction/finance companion is not proved unused merely by operation_type. An active lease is retained for the external lifetime census even if its remaining database work is no-carry.
- Mail/comms eligibility uses the exact marker helper and actual batch identity, never channel as email type or payment_scope_json presence alone. Attempt lease evidence is INTEGRITY_REVIEW until a nonblank provider message ID/actual accepted provider outcome proves admitted external work. Unsent CONTROL is obsolete. An unidentifiable batch-scoped message is integrity review; actual REMITTANCE/cancellation/completion owner advice is genuine.
- The separate cancellation-source mail obligation is exactly `m.cancel_notice_tracked IS TRUE AND m.type='REMITTANCE' AND m.status::text='SENT' AND m.sent_at IS NOT NULL AND upper(btrim(coalesce(m.provider_status,'')))='ACCEPTED' AND m.cancel_notice_reconciled_sent_at_utc IS DISTINCT FROM m.sent_at` (`07092026_2301…:338–347,494–502`). Do not gate census on next_attempt/due time. The request cancellation obligation uses tracked + actual applied timestamp + unreconciled applied timestamp, independently.
- Legitimate completion skip is exact persisted successful SKIPPED, queued=false/skipped=true, expected=covered=0, no corresponding mail/comms. It includes actual `completion_notice_skip_reason='NO_AUTHORISER_RECIPIENTS'` even when `outcome.eligible_to_send=true` (`broker/src/index.js:89225–89237`); lookup/derivation failures have ok=false and remain integrity. Require valid actual no-eligible disposition for other skipped reasons. Never synthesize a recipient or re-run the old authorisation flow.
- The remittance target set contains separate UMBRELLA_REMITTANCE (umbrella identity), CANDIDATE_UMBRELLA_COPY_REMITTANCE (candidate + umbrella identity), and each real candidate payout-advice target. The existing snapshot flags and transfer-only union above determine eligibility. Exact SENT typed/scoped candidate attribution is necessary but does not make one candidate advice cover the other umbrella target. Keep one bounded UUID candidate row blocking if any required target lacks its exact deterministic/reference/job-kind/recipient witness. Candidate-wide remittance_sent_at/SENT_VIA_MAIL_OUTBOX is owner aggregate evidence only where that owner proves all relevant target kinds; unknown old aggregate coverage is integrity review.

All these conclusions are source design. They assert no observed counts, terminal payments, actual mail deliveries, hosted readiness or external quiet-window completion.

## Final source decision BD007 (9 October 2026)
36 fixed categories now distinguish actual retained remittance_scope UUIDs and every exact scope/type/key/recipient SENT witness from inferred current-policy targets. Inferred target coverage missing historical admission/economic scope is INTEGRITY_REVIEW, not a invented genuine debt. Candidate copy/PAYE honor actual overrides/default/email; frozen payout routing/carry witnesses and exact item-paid membership are retained. Persisted suppression applies to item and transfer-only targets. Retained scopes/outbox remain independently evaluated after later preference changes; SKIPPED scope is an established disposition. Returned/reverted history does not manufacture new advice. Genuine positive finance balances and unbound supported credits remain UNDECIDED (OD-F5) only where their disposition is not established. Simple old snooze bookkeeping has established no-carry; separate genuine financial case/reserve categories cannot be hidden by that classification.
Verified nonempty all-ignored webhook deliveries require exact successful result multiset (including identity/type/reason), signature, processed time, valid shapes and no independently open linked bank events; they are closed nonfinancial history. Empty/malformed/contradictory/failed constituents remain integrity. Actual cancellation-source mail predicate is tracked REMITTANCE, SENT, ACCEPTED, exact sent timestamp unreconciled. No unrelated sent completion message is an outstanding cancellation source.
All counts are queried at RPC invocation; no hosted absence or positive obligation is asserted by source compilation. External quiet-window remains UNPROVEN and activation_ready false.
