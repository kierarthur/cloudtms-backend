-- Banking Pay NEXT: fixed read-only readiness census, F-18.
-- Counts are whole-category facts. Pages have native UUID keysets; no money,
-- bank, token, provider payload, mail recipient/body or source snapshots escape.
-- Database readiness is never proof of the separately required external barrier.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_readiness_marker_v1(p_type text,p_context text,p_reference text,p_key text)
returns text language sql immutable security invoker
set search_path=pg_catalog,private
as $marker$
 with n as (
  select pg_catalog.translate(pg_catalog.btrim(coalesce(x,''),
    U&'\0009\000A\000B\000C\000D\0020\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000\FEFF'),
    'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ') collate "C" as value, ord
  from pg_catalog.unnest(array[p_type,p_context,p_reference,p_key]) with ordinality a(x,ord)
 ), f as (
  select (array_agg(value order by ord))[1] as t,(array_agg(value order by ord))[2] as c,
    (array_agg(value order by ord))[3] as r,(array_agg(value order by ord))[4] as k from n
 )
 select case
  when t in ('REMITTANCE','PAYMENT_CANCELLATION')
    or pg_catalog.starts_with(r,'REMITTANCE') or pg_catalog.starts_with(r,'PAYOUT_NOTICE:')
    or pg_catalog.starts_with(r,'PAYMENT_CANCELLATION_NOTICE_V1:')
    or pg_catalog.starts_with(k,'REMITTANCE:BATCH:') or pg_catalog.starts_with(k,'REMIT:PAY_BATCH:')
    or pg_catalog.starts_with(k,'PAYOUT_NOTICE:PAY_BATCH:')
    or pg_catalog.starts_with(k,'PAYMENT_CANCELLATION_NOTICE_V1:') then 'ADVICE'
  when pg_catalog.starts_with(r,'PAY_AUTH_INVITE:') or pg_catalog.starts_with(r,'PAY_BATCH_PROCESSING:')
    or pg_catalog.starts_with(r,'PAY_BATCH_SCHEDULED:') then 'CONTROL'
  when c in ('PAY_BATCH','PAY_BATCHES','PAY_BATCH_REMITTANCE','PAY_PAYMENT_CORRECTION_REQUESTS')
    or pg_catalog.starts_with(r,'PAY_BATCH_COMPLETED:') then 'SCOPED'
  else 'UNSCOPED' end from f;
$marker$;

create or replace function private.bpay_next_readiness_scope_terminal_v1(p_scope_id uuid)
returns boolean language sql stable security invoker
set search_path=pg_catalog,private
as $scope$
 select exists (
  select 1 from public.banking_pay_operation_transfer_scope s
  join public.pay_bank_transfers t on t.id=s.pay_bank_transfer_id and t.pay_batch_id=s.pay_batch_id
  where s.id=p_scope_id and exists (
   select 1 from public.pay_bank_transfer_events e
   where e.pay_bank_transfer_id=t.id and e.pay_batch_id=t.pay_batch_id
     and e.mapping_status='MATCHED'
     and (e.event_source<>'PROVIDER_WEBHOOK' or e.provider_signature_valid is true)
     and (nullif(pg_catalog.btrim(s.provider_request_id),'') is null or e.provider_request_id=s.provider_request_id)
     and (nullif(pg_catalog.btrim(s.provider_transaction_id),'') is null or e.provider_transaction_id=s.provider_transaction_id)
     and (s.provider_request_sending_at_utc is null or e.received_at_utc>=s.provider_request_sending_at_utc)
     and ((t.status='COMPLETED' and t.completed_at_utc is not null and e.normalised_state='COMPLETED')
       or (t.status in ('RETURNED','REVERTED') and e.normalised_state=t.status)
       or (t.status in ('FAILED','DECLINED','REJECTED','CANCELLED','VOIDED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
         and e.normalised_state in ('FAILED','DECLINED','REJECTED','CANCELLED')
         and e.movement_classification in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY')))
     and not exists (
      select 1 from public.pay_bank_transfer_events later
      where later.pay_bank_transfer_id=t.id and later.id<>e.id
        and (later.received_at_utc is null or later.received_at_utc>=e.received_at_utc)
        and later.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
        and (nullif(pg_catalog.btrim(s.provider_request_id),'') is null or later.provider_request_id=s.provider_request_id)
        and (nullif(pg_catalog.btrim(s.provider_transaction_id),'') is null or later.provider_transaction_id=s.provider_transaction_id)
     )
  )
 );
$scope$;

create or replace function private.bpay_next_readiness_event_open_v1(p_event_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $event$
 select exists(select 1 from public.pay_bank_transfer_events e where e.id=p_event_id and (e.mapping_status in ('AMBIGUOUS','UNMATCHED') or e.correction_disposition in ('ACTION_REQUIRED','AMBIGUOUS','BLOCKED','FAILED','AUTO_PROCESSING')
    or (e.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN') and not exists(select 1 from public.pay_bank_transfer_events terminal
    join public.pay_bank_transfers t on t.id=terminal.pay_bank_transfer_id and t.pay_batch_id=terminal.pay_batch_id
    where terminal.pay_bank_transfer_id=e.pay_bank_transfer_id and terminal.mapping_status='MATCHED'
    and (terminal.event_source<>'PROVIDER_WEBHOOK' or terminal.provider_signature_valid)
    and terminal.received_at_utc>e.received_at_utc
    and ((terminal.normalised_state=t.status and t.status in ('COMPLETED','RETURNED','REVERTED'))
      or (t.status in ('FAILED','DECLINED','REJECTED','CANCELLED','VOIDED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
        and terminal.normalised_state in ('FAILED','DECLINED','REJECTED','CANCELLED')
        and terminal.movement_classification in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY')))
    and not exists(select 1 from public.pay_bank_transfer_events later where later.pay_bank_transfer_id=terminal.pay_bank_transfer_id and later.id<>terminal.id
      and (later.received_at_utc is null or later.received_at_utc>=terminal.received_at_utc)
      and later.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
      and (nullif(btrim(e.provider_request_id),'') is null or later.provider_request_id=e.provider_request_id)
      and (nullif(btrim(e.provider_transaction_id),'') is null or later.provider_transaction_id=e.provider_transaction_id))
    and (nullif(btrim(e.provider_request_id),'') is null or terminal.provider_request_id=e.provider_request_id)
    and (nullif(btrim(e.provider_transaction_id),'') is null or terminal.provider_transaction_id=e.provider_transaction_id)))
 or (e.mapping_status='LEGACY_NO_ARTIFACT' and coalesce(e.movement_classification,'UNKNOWN') not in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY'))));
$event$;

create or replace function private.bpay_next_readiness_event_terminal_v1(p_event_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $terminal$
 select exists(select 1 from public.pay_bank_transfer_events e
 join public.pay_bank_transfers t on t.id=e.pay_bank_transfer_id and t.pay_batch_id=e.pay_batch_id
 where e.id=p_event_id and e.mapping_status='MATCHED'
 and (e.event_source<>'PROVIDER_WEBHOOK' or e.provider_signature_valid is true)
 and ((t.status='COMPLETED' and t.completed_at_utc is not null and e.normalised_state='COMPLETED')
   or (t.status in ('RETURNED','REVERTED') and e.normalised_state=t.status)
   or (t.status in ('FAILED','DECLINED','REJECTED','CANCELLED','VOIDED','SUBMISSION_FAILED','FAILED_BEFORE_COMMIT')
     and e.normalised_state in ('FAILED','DECLINED','REJECTED','CANCELLED')
     and e.movement_classification in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','PROVIDER_CANCELLED_NO_MONEY','PROVIDER_FAILED_NO_MONEY')))
 and not private.bpay_next_readiness_event_open_v1(e.id)
 and not exists(select 1 from public.pay_bank_transfer_events later where later.pay_bank_transfer_id=t.id and later.id<>e.id
   and (later.received_at_utc is null or later.received_at_utc>=e.received_at_utc)
   and later.normalised_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN')
   and (nullif(btrim(e.provider_request_id),'') is null or later.provider_request_id=e.provider_request_id)
   and (nullif(btrim(e.provider_transaction_id),'') is null or later.provider_transaction_id=e.provider_transaction_id)));
$terminal$;

create or replace function private.bpay_next_readiness_unused_batch_v1(p_batch_id uuid)
returns boolean language sql stable security invoker set search_path=pg_catalog,private
as $unused$
 select exists(select 1 from public.pay_batches b where b.id=p_batch_id
 and b.status in ('DRAFT','DRAFT_CREATED') and b.execution_commit_state='NOT_SUBMITTED'
 and b.bank_csv_export_json is null and b.scheduled_at_utc is null and b.executing_started_at_utc is null
 and b.execution_committed_at_utc is null and b.monzo_confirmed_at_utc is null
 and not exists(select 1 from public.pay_batch_auth_requests a where a.pay_batch_id=b.id and a.state='AUTHORISED')
 and not exists(select 1 from public.pay_advance_reservations r where r.pay_batch_id=b.id and r.status in ('RESERVED','COMMITTED'))
 and not exists(select 1 from public.pay_batch_candidates c where c.pay_batch_id=b.id and (c.settled_at_utc is not null or c.settlement_status in ('PARTIAL','FAILED','SETTLED') or c.net_bank_amount is not null))
 and not exists(select 1 from public.pay_batch_paye_net_inputs n join public.pay_batch_candidates c on c.id=n.pay_batch_candidate_id where c.pay_batch_id=b.id)
 and not exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and
   (t.status<>'PENDING' or nullif(btrim(t.rail_tx_id),'') is not null or nullif(btrim(t.request_id),'') is not null or nullif(btrim(t.rail_state),'') is not null))
 and not exists(select 1 from public.banking_pay_operation_transfer_scope x where x.pay_batch_id=b.id and
   (x.provider_submit_state not in ('NOT_READY','READY') or x.provider_submit_attempt_count>0
    or x.provider_request_sending_at_utc is not null or x.provider_request_sent_at_utc is not null
    or x.provider_submit_claimed_at_utc is not null or x.provider_response_at_utc is not null
    or nullif(btrim(x.provider_transaction_id),'') is not null or nullif(btrim(x.provider_request_id),'') is not null))
 and not exists(select 1 from public.banking_pay_operation_provider_attempts a where a.pay_batch_id=b.id)
 and not exists(select 1 from public.pay_bank_transfer_events e where e.pay_batch_id=b.id)
 and not exists(select 1 from public.banking_pay_operation_settlement_scope x where x.pay_batch_id=b.id and x.status in ('PENDING','FAILED'))
 and not exists(select 1 from public.pay_payment_correction_requests r where r.pay_batch_id=b.id and r.status not in ('REJECTED','CANCELLED'))
 and not exists(select 1 from public.mail_outbox m where (m.context_id=b.id or m.payment_scope_json->>'pay_batch_id'=b.id::text)
   and private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key) in ('ADVICE','SCOPED')));
$unused$;

create or replace function private.bpay_next_readiness_rows_v1(p_category text)
returns table(id uuid,status text,classification text,blocking boolean)
language sql stable security invoker set search_path=pg_catalog,private
as $rows$
 select q.id,q.status::text,q.classification::text,q.blocking from (
   with candidates as (
    select b.*, (b.execution_commit_state='SUBMITTED_NOT_COMMITTED' or ((b.bank_csv_export_json is not null or b.scheduled_at_utc is not null or b.executing_started_at_utc is not null or b.monzo_confirmed_at_utc is not null)
      and (not exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id)
        or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and not exists(select 1 from public.pay_bank_transfer_events e where e.pay_bank_transfer_id=t.id and private.bpay_next_readiness_event_terminal_v1(e.id)))))
    or exists(select 1 from public.pay_batch_auth_requests a where a.pay_batch_id=b.id and a.state='AUTHORISED' and b.execution_commit_state='NOT_SUBMITTED')
    or exists(select 1 from public.pay_advance_reservations r where r.pay_batch_id=b.id and r.status in ('RESERVED','COMMITTED'))
    or exists(select 1 from public.pay_batch_paye_net_inputs n join public.pay_batch_candidates c on c.id=n.pay_batch_candidate_id where c.pay_batch_id=b.id and b.execution_commit_state='NOT_SUBMITTED' and c.settled_at_utc is null and coalesce(c.settlement_status,'')<>'SETTLED')
    or exists(select 1 from public.pay_batch_candidates c where c.pay_batch_id=b.id and c.settlement_status in ('PARTIAL','FAILED'))
    or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and (t.status in ('PROCESSING','UNKNOWN') or (t.rail_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN') and not exists(select 1 from public.pay_bank_transfer_events e where e.pay_bank_transfer_id=t.id and private.bpay_next_readiness_event_terminal_v1(e.id)))))
    or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.pay_batch_id=b.id and not private.bpay_next_readiness_scope_terminal_v1(s.id) and (s.provider_submit_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null ))
    or exists(select 1 from public.banking_pay_operation_provider_attempts a where a.pay_batch_id=b.id and a.new_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') and not exists(select 1 from public.banking_pay_operation_transfer_scope s where s.id=a.transfer_scope_id and private.bpay_next_readiness_scope_terminal_v1(s.id)))
    or exists(select 1 from public.pay_bank_transfer_events e where e.pay_batch_id=b.id and private.bpay_next_readiness_event_open_v1(e.id))
    or exists(select 1 from public.banking_pay_operation_settlement_scope s where s.pay_batch_id=b.id and s.status in ('PENDING','FAILED'))
    or exists(select 1 from public.pay_payment_correction_requests r where r.pay_batch_id=b.id and r.status not in ('APPLIED','REJECTED','CANCELLED'))
    or exists(select 1 from public.mail_outbox m where m.context_id=b.id and private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key) in ('ADVICE','SCOPED') and m.sent_at is null and m.delivered_at is null and m.read_at is null and m.status::text not in ('SENT','DELIVERED','READ'))) as positive
    from public.pay_batches b where b.status not in ('SETTLED','CANCELLED') or b.execution_commit_state='SUBMITTED_NOT_COMMITTED'
      or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.pay_batch_id=b.id and ((s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null or s.provider_submit_attempt_count>0) and not private.bpay_next_readiness_scope_terminal_v1(s.id)))
      or exists(select 1 from public.pay_advance_reservations r where r.pay_batch_id=b.id and r.status in ('RESERVED','COMMITTED'))
      or exists(select 1 from public.banking_pay_operation_settlement_scope s where s.pay_batch_id=b.id and s.status in ('PENDING','FAILED'))
   )
   select b.id,b.status,case when b.positive then 'GENUINE_OBLIGATION'
    when private.bpay_next_readiness_unused_batch_v1(b.id) then 'OBSOLETE_BOOKKEEPING'
    else 'INTEGRITY_REVIEW' end,
    b.positive or not private.bpay_next_readiness_unused_batch_v1(b.id) from candidates b
 ) q(id,status,classification,blocking) where p_category='LEGACY_BATCHES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   with candidates as (
    select o.*, (false
      or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.operation_id=o.id and ((s.provider_submit_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null ) and not private.bpay_next_readiness_scope_terminal_v1(s.id)))
      or exists(select 1 from public.banking_pay_operation_settlement_scope s where s.operation_id=o.id and s.status in ('PENDING','FAILED'))) as positive
    from public.banking_pay_operations o where o.status not in ('COMPLETE','FAILED','CANCELLED','REVIEW_REQUIRED')
    or (o.lease_owner is not null and o.lease_expires_at_utc>statement_timestamp())
    or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.operation_id=o.id and ((s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null or s.provider_submit_attempt_count>0) and not private.bpay_next_readiness_scope_terminal_v1(s.id)))
    or exists(select 1 from public.banking_pay_operation_settlement_scope s where s.operation_id=o.id and s.status in ('PENDING','FAILED'))
   )
   select o.id,o.status,case when o.positive then 'GENUINE_OBLIGATION'
    when o.operation_type in ('DRAFT_CREATE','PREVIEW_REFRESH') and (o.pay_batch_id is null or private.bpay_next_readiness_unused_batch_v1(o.pay_batch_id)) and not exists(select 1 from public.banking_pay_operation_transfer_scope x where x.operation_id=o.id) and not exists(select 1 from public.banking_pay_operation_provider_attempts x where x.operation_id=o.id) and not exists(select 1 from public.banking_pay_operation_settlement_scope x where x.operation_id=o.id and x.status in ('PENDING','FAILED')) then 'OBSOLETE_BOOKKEEPING' else 'INTEGRITY_REVIEW' end,
    o.positive or not(o.operation_type in ('DRAFT_CREATE','PREVIEW_REFRESH') and (o.pay_batch_id is null or private.bpay_next_readiness_unused_batch_v1(o.pay_batch_id)) and not exists(select 1 from public.banking_pay_operation_transfer_scope x where x.operation_id=o.id) and not exists(select 1 from public.banking_pay_operation_provider_attempts x where x.operation_id=o.id) and not exists(select 1 from public.banking_pay_operation_settlement_scope x where x.operation_id=o.id and x.status in ('PENDING','FAILED'))) from candidates o
 ) q(id,status,classification,blocking) where p_category='LEGACY_OPERATIONS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,
    case when r.authorised_at_utc is not null or r.source_bank_event_id is not null or exists(select 1 from public.pay_payment_correction_work_items w where w.correction_request_id=r.id and w.pay_bank_transfer_id is not null)
    then 'GENUINE_OBLIGATION' else 'OBSOLETE_BOOKKEEPING' end,
    r.authorised_at_utc is not null or r.source_bank_event_id is not null or exists(select 1 from public.pay_payment_correction_work_items w where w.correction_request_id=r.id and w.pay_bank_transfer_id is not null)
    from public.pay_payment_correction_requests r where r.status not in ('APPLIED','REJECTED','CANCELLED')
    or exists(select 1 from public.pay_payment_correction_work_items w where w.correction_request_id=r.id and w.status in ('PENDING','PROCESSING','BLOCKED','FAILED_RETRYABLE','FAILED_FINAL'))
    or (r.cancel_notice_tracked and r.applied_at_utc is not null and r.cancel_notice_reconciled_applied_at_utc is distinct from r.applied_at_utc)
 ) q(id,status,classification,blocking) where p_category='LEGACY_CORRECTION_REQUESTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select w.id,w.status,case when w.pay_bank_transfer_id is not null then 'GENUINE_OBLIGATION' else 'INTEGRITY_REVIEW' end,true
    from public.pay_payment_correction_work_items w where w.status in ('PENDING','PROCESSING','BLOCKED','FAILED_RETRYABLE','FAILED_FINAL')
 ) q(id,status,classification,blocking) where p_category='LEGACY_CORRECTION_WORK'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select s.id,case when s.snooze_until_date<current_date then 'EXPIRED_DATE_UNCLEARED' else 'ACTIVE' end,
    'ESTABLISHED_NO_CARRY'::text,false from public.pay_item_snoozes s where s.cleared_at_utc is null and s.cancelled_at_utc is null
 ) q(id,status,classification,blocking) where p_category='LEGACY_SNOOZES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select a.id,a.status::text,
    case when a.outstanding_amount>0 and (a.cleared_at_utc is not null or a.written_off_at_utc is not null) then 'INTEGRITY_REVIEW' else 'UNDECIDED (OD-F5)' end,true
    from public.pay_advances a where not exists(select 1 from private.bpay_next_stored_credit_origin o where o.legacy_case_id=a.id)
    and (a.outstanding_amount>0 or (a.payout_status::text='PENDING' and a.original_amount>0 and a.cleared_at_utc is null and a.written_off_at_utc is null)
    or exists(select 1 from public.pay_finance_case_components c where c.finance_case_id=a.id and c.remaining_source_amount>0 and c.closed_at_utc is null)
    or exists(select 1 from public.pay_advance_reservations r where r.finance_case_id=a.id and r.status in ('RESERVED','COMMITTED')))
 ) q(id,status,classification,blocking) where p_category='LEGACY_FINANCE_CASES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'GENUINE_OBLIGATION'::text,true from public.pay_advance_reservations r where r.status in ('RESERVED','COMMITTED')
 ) q(id,status,classification,blocking) where p_category='LEGACY_RESERVATIONS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select c.id,coalesce(c.paye_state,'UNSPECIFIED'),
   case when exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id) then 'GENUINE_OBLIGATION'
     when c.net_bank_amount is not null or b.scheduled_at_utc is not null or b.executing_started_at_utc is not null
       or exists(select 1 from public.pay_batch_auth_requests a where a.pay_batch_id=b.id and a.state='AUTHORISED')
       or exists(select 1 from public.pay_advance_reservations r where r.pay_batch_candidate_id=c.id and r.status in ('RESERVED','COMMITTED'))
       or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and t.candidate_id=c.candidate_id)
     then 'INTEGRITY_REVIEW' else 'OBSOLETE_BOOKKEEPING' end,
   exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id)
     or c.net_bank_amount is not null or b.scheduled_at_utc is not null or b.executing_started_at_utc is not null
     or exists(select 1 from public.pay_batch_auth_requests a where a.pay_batch_id=b.id and a.state='AUTHORISED')
     or exists(select 1 from public.pay_advance_reservations r where r.pay_batch_candidate_id=c.id and r.status in ('RESERVED','COMMITTED'))
     or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and t.candidate_id=c.candidate_id)
   from public.pay_batch_candidates c join public.pay_batches b on b.id=c.pay_batch_id
   where b.execution_commit_state='NOT_SUBMITTED' and b.status not in ('SETTLED','CANCELLED')
   and (c.awaiting_net_amount or c.paye_state='PENDING_NET' or c.net_bank_amount is not null or exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id))
 ) q(id,status,classification,blocking) where p_category='LEGACY_PAYE_NETS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select m.id,m.status::text,case when private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key)='CONTROL' then 'OBSOLETE_BOOKKEEPING' else 'GENUINE_OBLIGATION' end,private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key)<>'CONTROL'
    from public.mail_outbox m where (private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key)<>'UNSCOPED' or exists(select 1 from public.pay_batches b where b.id::text=m.payment_scope_json->>'pay_batch_id')) and (m.sent_at is null and m.delivered_at is null and m.read_at is null and m.status::text not in ('SENT','DELIVERED','READ'))
 ) q(id,status,classification,blocking) where p_category='LEGACY_MAIL_UNSENT'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select m.id,m.status::text,case when m.cancel_notice_tracked and m.type='REMITTANCE' and m.status::text='SENT' and m.sent_at is not null and upper(btrim(coalesce(m.provider_status,'')))='ACCEPTED' then 'GENUINE_OBLIGATION'
     when nullif(btrim(m.provider_message_id),'') is not null or upper(btrim(coalesce(m.provider_status,''))) in ('ACCEPTED','SENT','SUCCESS','OK') then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end,true from public.mail_outbox m where (private.bpay_next_readiness_marker_v1(m.type,m.context_kind,m.reference,m.deterministic_outbox_key)<>'UNSCOPED' or exists(select 1 from public.pay_batches b where b.id::text=m.payment_scope_json->>'pay_batch_id'))
    and (((m.attempt_lease_token is not null or m.attempt_leased_at_utc is not null or m.attempt_lease_expires_at_utc is not null or nullif(pg_catalog.btrim(m.provider_message_id),'') is not null or upper(coalesce(m.provider_status,'')) in ('ACCEPTED','SENT','SUCCESS','OK')) and (m.sent_at is null and m.delivered_at is null and m.read_at is null and m.status::text not in ('SENT','DELIVERED','READ'))) or (m.cancel_notice_tracked is true and m.type='REMITTANCE' and m.status::text='SENT' and m.sent_at is not null and m.provider_status='ACCEPTED' and m.cancel_notice_reconciled_sent_at_utc is distinct from m.sent_at))
 ) q(id,status,classification,blocking) where p_category='LEGACY_MAIL_ATTEMPTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select m.id,m.status::text,case when private.bpay_next_readiness_marker_v1(coalesce(m.provider_payload_json->>'type',m.provider_payload_json->>'email_type'),m.context_kind,coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference'),coalesce(m.deterministic_outbox_key,m.provider_payload_json->>'deterministic_outbox_key',m.provider_payload_json->>'deterministicOutboxKey'))='CONTROL' then 'OBSOLETE_BOOKKEEPING' else 'GENUINE_OBLIGATION' end,private.bpay_next_readiness_marker_v1(coalesce(m.provider_payload_json->>'type',m.provider_payload_json->>'email_type'),m.context_kind,coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference'),coalesce(m.deterministic_outbox_key,m.provider_payload_json->>'deterministic_outbox_key',m.provider_payload_json->>'deterministicOutboxKey'))<>'CONTROL'
    from public.comms_outbox m where (private.bpay_next_readiness_marker_v1(coalesce(m.provider_payload_json->>'type',m.provider_payload_json->>'email_type'),m.context_kind,coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference'),coalesce(m.deterministic_outbox_key,m.provider_payload_json->>'deterministic_outbox_key',m.provider_payload_json->>'deterministicOutboxKey'))<>'UNSCOPED' or exists(select 1 from public.pay_batches b where b.id::text=coalesce(m.provider_payload_json#>>'{payment_scope_json,pay_batch_id}',m.provider_payload_json->>'pay_batch_id'))) and (m.sent_at is null and m.delivered_at is null and m.read_at is null and m.status::text not in ('SENT','DELIVERED','READ'))
 ) q(id,status,classification,blocking) where p_category='LEGACY_COMMS_UNSENT'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select m.id,m.status::text,case when nullif(btrim(m.provider_message_id),'') is not null then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end,true from public.comms_outbox m where (private.bpay_next_readiness_marker_v1(coalesce(m.provider_payload_json->>'type',m.provider_payload_json->>'email_type'),m.context_kind,coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference'),coalesce(m.deterministic_outbox_key,m.provider_payload_json->>'deterministic_outbox_key',m.provider_payload_json->>'deterministicOutboxKey'))<>'UNSCOPED' or exists(select 1 from public.pay_batches b where b.id::text=coalesce(m.provider_payload_json#>>'{payment_scope_json,pay_batch_id}',m.provider_payload_json->>'pay_batch_id')))
    and (((m.attempt_lease_token is not null or m.attempt_leased_at_utc is not null or m.attempt_lease_expires_at_utc is not null or nullif(pg_catalog.btrim(m.provider_message_id),'') is not null) and (m.sent_at is null and m.delivered_at is null and m.read_at is null and m.status::text not in ('SENT','DELIVERED','READ'))))
 ) q(id,status,classification,blocking) where p_category='LEGACY_COMMS_ATTEMPTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   with item_flags as (
    select
      pbc.id as pay_batch_candidate_id,
      pbc.candidate_id as candidate_id,
      pbi.finance_case_id as finance_case_id,
      pbi.item_type as item_type,
      upper(btrim(coalesce(pbi.pay_channel::text,''))) as pay_channel,
      pbi.umbrella_id as umbrella_id,
      case
        when jsonb_typeof(pbi.payout_instruction_snapshot_json) = 'object'
         and pbi.payout_instruction_snapshot_json ? 'appears_on_umbrella_remittance'
          then lower(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'appears_on_umbrella_remittance','false'))) in ('true','1','yes','y','on')
        when upper(coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind','')), ''), '')) = 'UMBRELLA_COMPANY' then true
        when upper(coalesce(nullif(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind','')), ''), '')) in ('NORMAL_PAY_ROUTE','ONE_OFF_SPECIFIED_BANK_ACCOUNT') then false
        when upper(coalesce(pbi.pay_channel::text,'')) = 'PAYE' then false
        when upper(coalesce(pbi.pay_channel::text,'')) = 'UMBRELLA' then true
        else coalesce(vfcr.appears_on_umbrella_remittance, false)
      end as appears_on_umbrella_remittance,
      (pbi.item_type in ('LOAN_PAYOUT','MANUAL_CREDIT_PAYOUT','MANUAL_DEBT_RECOVERY') and (
        (pbi.finance_case_id is not null and jsonb_typeof(pbi.payout_instruction_snapshot_json)='object'
          and upper(coalesce(pbi.payout_instruction_snapshot_json->>'routing_kind',''))='ONE_OFF_SPECIFIED_BANK_ACCOUNT'
          and lower(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'is_candidate_directed_oneoff_payout','false'))) in ('true','1','yes','y','on')
          and lower(btrim(coalesce(pbi.payout_instruction_snapshot_json->>'generates_candidate_payment_advice','false'))) in ('true','1','yes','y','on'))
        or (pbi.item_type in ('MANUAL_CREDIT_PAYOUT','MANUAL_DEBT_RECOVERY') and (
          nullif(btrim(coalesce(pbi.source_ref,'')),'') ~* '^carry_forward:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or nullif(btrim(coalesce(pbi.operation_source_key,'')),'') ~* '^carry_forward:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or nullif(btrim(coalesce(pbi.frozen_source_basis_json->>'manual_adjustment_carry_forward_id','')),'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or nullif(btrim(coalesce(pbi.frozen_component_snapshot_json->>'manual_adjustment_carry_forward_id','')),'') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))
      )) as generates_candidate_payment_advice
    from public.pay_batch_candidates pbc
    join public.pay_batch_items pbi
      on pbi.pay_batch_candidate_id = pbc.id
    left join public.v_finance_cases_register vfcr
      on vfcr.finance_case_id = pbi.finance_case_id
    where ((pbi.pay_bank_transfer_id is null and pbc.settled_at_utc is not null and pbc.settlement_status='SETTLED')
       or exists(select 1 from public.pay_bank_transfers paid where paid.id=pbi.pay_bank_transfer_id
         and paid.pay_batch_id=pbc.pay_batch_id and paid.candidate_id=pbc.candidate_id
         and paid.status='COMPLETED' and paid.completed_at_utc is not null))
      and not exists(select 1 from public.pay_batches b where b.id=pbc.pay_batch_id and (
        lower(btrim(coalesce(b.execution_intent_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.execution_intent_json->>'suppress_remittances_pending','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.settlement_confirmation_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.settlement_confirmation_json->>'suppress_remittances_pending','false'))) in ('true','1','yes','y','on')))
      and pbi.item_type <> 'DEBT_CREATED'
      and coalesce(pbi.is_voided, false) = false
      and not exists (
        select 1
        from public.pay_payment_correction_items as active_item_corrections
        where active_item_corrections.pay_batch_item_id = pbi.id
          and active_item_corrections.status = 'APPLIED'
          and active_item_corrections.correction_item_kind in ('PRE_BANK_CANCEL','NO_MONEY_UNWIND','SETTLED_REVERSAL')
      )
  ),
   rem_targets as (
    select f.pay_batch_candidate_id,f.candidate_id,f.umbrella_id,'UMBRELLA'::text as target_kind
      from item_flags f join public.umbrellas u on u.id=f.umbrella_id where f.appears_on_umbrella_remittance and f.umbrella_id is not null and nullif(btrim(coalesce(u.remittance_email,'')),'') is not null
    union
    select c.id,c.candidate_id,t.umbrella_id,'UMBRELLA'::text from public.pay_bank_transfers t
      join public.pay_batch_candidates c on c.pay_batch_id=t.pay_batch_id and c.candidate_id=t.candidate_id
      join public.umbrellas u on u.id=t.umbrella_id
      where t.pay_channel='UMBRELLA' and t.umbrella_id is not null and t.status='COMPLETED' and t.completed_at_utc is not null and nullif(btrim(coalesce(u.remittance_email,'')),'') is not null
      and not exists(select 1 from public.pay_batches b where b.id=t.pay_batch_id and (
        lower(btrim(coalesce(b.execution_intent_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.execution_intent_json->>'suppress_remittances_pending','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.settlement_confirmation_json->>'suppress_remittances','false'))) in ('true','1','yes','y','on')
        or lower(btrim(coalesce(b.settlement_confirmation_json->>'suppress_remittances_pending','false'))) in ('true','1','yes','y','on')))
   ), targets as (
    select r.* from rem_targets r
    union
    select r.pay_batch_candidate_id,r.candidate_id,null::uuid,'CANDIDATE_UMBRELLA_COPY_REMITTANCE'::text from rem_targets r
      join public.candidates cc on cc.id=r.candidate_id left join public.settings_defaults sd on sd.id=1
      where nullif(btrim(coalesce(cc.email,'')),'') is not null and
        case when coalesce(cc.remittance_overrides_enabled,false) then coalesce(cc.remittance_receive_when_umbrella_paid,false)
          else coalesce(sd.remittance_receive_when_umbrella_paid,false) end
    union
    select f.pay_batch_candidate_id,f.candidate_id,null::uuid,'PAYE_REMITTANCE'::text from item_flags f
      join public.candidates cc on cc.id=f.candidate_id left join public.settings_defaults sd on sd.id=1
      where f.pay_channel='PAYE' and not f.generates_candidate_payment_advice
        and nullif(btrim(coalesce(cc.email,'')),'') is not null and
        case when coalesce(cc.remittance_overrides_enabled,false) then coalesce(cc.remittance_receive_enabled,false)
          else coalesce(sd.paye_remittances_enabled,false) end
    union
    select f.pay_batch_candidate_id,f.candidate_id,null::uuid,'PAYOUT_NOTICE_CANDIDATE'::text from item_flags f
      join public.candidates cc on cc.id=f.candidate_id where f.generates_candidate_payment_advice
      and nullif(btrim(coalesce(cc.email,'')),'') is not null
   )
   -- Current preferences are not historical admission evidence. Missing
   -- recomputed advice proof is integrity review; retained scopes/mail below
   -- separately establish genuine admitted advice, independent of preferences.
   select c.id,coalesce(c.remittance_trigger_status,'UNRECORDED'),'INTEGRITY_REVIEW'::text,true from public.pay_batch_candidates c
   where exists(select 1 from targets t where t.pay_batch_candidate_id=c.id and not exists(
    select 1 from public.mail_outbox m where upper(btrim(coalesce(m.status::text,'')))='SENT'
      and upper(btrim(coalesce(m.provider_status,'ACCEPTED'))) in ('ACCEPTED','SENT','SUCCESS','OK','')
      and ((t.target_kind='UMBRELLA' and m.type='REMITTANCE' and upper(btrim(coalesce(m.recipient_kind,'')))='UMBRELLA' and m.recipient_id=t.umbrella_id)
        or (t.target_kind='CANDIDATE_UMBRELLA_COPY_REMITTANCE' and m.type='REMITTANCE'
          and upper(btrim(coalesce(m.recipient_kind,'')))='CANDIDATE' and m.recipient_id=t.candidate_id
          and (m.payment_scope_json->>'job_kind'='CANDIDATE_UMBRELLA_COPY_REMITTANCE'
            or m.payment_scope_json#>>'{queue_context,job_kind}'='CANDIDATE_UMBRELLA_COPY_REMITTANCE'))
        or (t.target_kind='PAYE_REMITTANCE' and m.type='REMITTANCE'
          and upper(btrim(coalesce(m.recipient_kind,'')))='CANDIDATE' and m.recipient_id=t.candidate_id
          and (m.payment_scope_json->>'job_kind'='PAYE_REMITTANCE'
            or m.payment_scope_json#>>'{queue_context,job_kind}'='PAYE_REMITTANCE'
            or (m.payment_scope_json->>'remittance_type'='CANDIDATE_REMITTANCE'
              and m.payment_scope_json->>'message_kind' is distinct from 'PAYOUT_NOTICE'
              and m.payment_scope_json->>'notice_scope' is distinct from 'PAYOUT_NOTICE_CANDIDATE'))) 
        or (t.target_kind='PAYOUT_NOTICE_CANDIDATE' and upper(btrim(coalesce(m.recipient_kind,'')))='CANDIDATE' and m.recipient_id=t.candidate_id
          and starts_with(m.reference,'payout_notice:pay_batch:'||c.pay_batch_id::text||':')))
      and (starts_with(m.deterministic_outbox_key,'remittance:batch:'||c.pay_batch_id::text||':')
        or starts_with(m.reference,'remittance:batch:'||c.pay_batch_id::text||':')
        or starts_with(m.reference,'remit:pay_batch:'||c.pay_batch_id::text||':')
        or m.reference='pay_batch:'||c.pay_batch_id::text||':candidate:'||c.candidate_id::text
        or starts_with(m.reference,'payout_notice:pay_batch:'||c.pay_batch_id::text||':')
        or (lower(btrim(coalesce(m.context_kind,''))) in ('pay_batch','pay_batches') and m.context_id=c.pay_batch_id)
        or m.payment_scope_json->>'pay_batch_id'=c.pay_batch_id::text)
   ))

 ) q(id,status,classification,blocking) where p_category='LEGACY_REMITTANCE_TARGETS'

 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select s.id,s.status,case when s.status in ('PENDING','QUEUED','FAILED')
     and s.remittance_type in ('CANDIDATE_REMITTANCE','UMBRELLA_REMITTANCE')
     and exists(select 1 from public.pay_batch_candidates c where c.id=s.pay_batch_candidate_id
       and c.pay_batch_id=s.pay_batch_id and c.candidate_id=s.candidate_id)
     and ((s.remittance_type='CANDIDATE_REMITTANCE' and s.recipient_kind='CANDIDATE' and s.recipient_id=s.candidate_id)
       or (s.remittance_type='UMBRELLA_REMITTANCE' and s.recipient_kind='UMBRELLA' and s.recipient_id is not null))
     then 'GENUINE_OBLIGATION' else 'INTEGRITY_REVIEW' end,true
   from public.banking_pay_operation_remittance_scope s where s.status<>'SKIPPED' and not exists(
    select 1 from public.mail_outbox m where m.type='REMITTANCE' and m.status::text='SENT'
      and (m.provider_status is null or upper(btrim(m.provider_status)) in ('ACCEPTED','SENT','SUCCESS','OK',''))
      and upper(btrim(coalesce(m.recipient_kind,'')))=s.recipient_kind and m.recipient_id=s.recipient_id
      and m.context_kind='pay_batches' and m.context_id=s.pay_batch_id
      and m.deterministic_outbox_key=s.deterministic_outbox_key and m.reference=s.deterministic_outbox_key
      and m.payment_scope_json->>'pay_batch_id'=s.pay_batch_id::text
      and m.payment_scope_json->>'remittance_scope_id'=s.id::text
      and m.payment_scope_json->>'remittance_type'=s.remittance_type
      and m.payment_scope_json->>'message_kind' is distinct from 'PAYOUT_NOTICE'
      and m.payment_scope_json->>'notice_scope' is distinct from 'PAYOUT_NOTICE_CANDIDATE'
      and (s.outbox_id is null or m.id=s.outbox_id or exists(select 1 from public.mail_outbox payout
        where payout.id=s.outbox_id and payout.context_id=s.pay_batch_id
          and payout.payment_scope_json->>'remittance_scope_id'=s.id::text
          and payout.payment_scope_json->>'message_kind'='PAYOUT_NOTICE'))
   )
 ) q(id,status,classification,blocking) where p_category='LEGACY_REMITTANCE_SCOPES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   with candidates as (
    select b.*,
     case when jsonb_typeof(b.completion_notice_last_result_json->'recipient_count')='number'
       and (b.completion_notice_last_result_json->>'recipient_count') ~ '^[0-9]{1,9}$'
       then (b.completion_notice_last_result_json->>'recipient_count')::integer else null end as expected_count,
     case when jsonb_typeof(b.completion_notice_last_result_json->'outbox_rows_covered_count')='number'
       and (b.completion_notice_last_result_json->>'outbox_rows_covered_count') ~ '^[0-9]{1,9}$'
       then (b.completion_notice_last_result_json->>'outbox_rows_covered_count')::integer else null end as covered_count
    from public.pay_batches b where b.completion_notice_queued_at_utc is not null or b.completion_notice_last_attempt_at_utc is not null
     or b.completion_notice_next_attempt_at_utc is not null or b.completion_notice_reference is not null or b.completion_notice_last_result_json is not null
   ), proof as (
    select b.*,
     (b.expected_count=0 and b.covered_count=0 and b.completion_notice_last_result_json->'ok'='true'::jsonb
      and b.completion_notice_last_result_json->'completion_notice_skipped'='true'::jsonb
      and b.completion_notice_last_result_json->>'completion_notice_result'='SKIPPED'
      and (b.completion_notice_last_result_json#>'{outcome,eligible_to_send}'='false'::jsonb
        or b.completion_notice_last_result_json->>'completion_notice_skip_reason'='NO_AUTHORISER_RECIPIENTS')
      and not exists(select 1 from public.mail_outbox m where m.context_id=b.id and m.reference=b.completion_notice_reference)
      and not exists(select 1 from public.comms_outbox m where m.context_id=b.id and coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference')=b.completion_notice_reference)) as legitimate_skip,
     (b.expected_count>0 and b.covered_count=b.expected_count
      and b.completion_notice_last_result_json->'ok'='true'::jsonb
      and b.completion_notice_last_result_json->>'completion_notice_result' in ('QUEUED','ALREADY_COVERED_BY_DETERMINISTIC_KEY')
      and b.completion_notice_last_result_json->'completion_notice_queued'='true'::jsonb
      and b.completion_notice_last_result_json->>'recipients_total'=b.expected_count::text
      and (select count(*) from public.mail_outbox m where m.context_id=b.id and m.reference=b.completion_notice_reference
        and lower(btrim(coalesce(m.context_kind,'')))='pay_batches'
        and starts_with(m.deterministic_outbox_key,'pay_batch_completed:'||b.id::text||':to:'))=b.expected_count
      and (select count(distinct m.deterministic_outbox_key) from public.mail_outbox m where m.context_id=b.id and m.reference=b.completion_notice_reference
        and lower(btrim(coalesce(m.context_kind,'')))='pay_batches'
        and starts_with(m.deterministic_outbox_key,'pay_batch_completed:'||b.id::text||':to:')
        and (m.sent_at is not null or m.delivered_at is not null or m.read_at is not null or m.status::text in ('SENT','DELIVERED','READ')))=b.expected_count
      and not exists(select 1 from public.comms_outbox m where m.context_id=b.id
        and coalesce(m.provider_payload_json->>'reference',m.provider_payload_json->>'mail_reference')=b.completion_notice_reference)
      and not exists(select 1 from jsonb_array_elements(case when jsonb_typeof(b.completion_notice_last_result_json->'inserted')='array'
          then b.completion_notice_last_result_json->'inserted' else '[]'::jsonb end) x
        where jsonb_typeof(x->'id') is distinct from 'string' or jsonb_typeof(x->'deterministic_outbox_key') is distinct from 'string'
        or not exists(select 1 from public.mail_outbox m where m.id::text=x->>'id'
          and m.deterministic_outbox_key=x->>'deterministic_outbox_key' and m.context_id=b.id and m.reference=b.completion_notice_reference
          and (m.sent_at is not null or m.delivered_at is not null or m.read_at is not null or m.status::text in ('SENT','DELIVERED','READ'))))) as complete
    from candidates b
   )
   select b.id,b.status,case when b.legitimate_skip is true then 'OBSOLETE_BOOKKEEPING' else 'INTEGRITY_REVIEW' end,
     b.legitimate_skip is distinct from true from proof b where b.complete is distinct from true
 ) q(id,status,classification,blocking) where p_category='LEGACY_COMPLETION_NOTICES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'GENUINE_OBLIGATION'::text,true from public.pay_payment_correction_requests r where r.cancel_notice_tracked and r.applied_at_utc is not null and r.cancel_notice_reconciled_applied_at_utc is distinct from r.applied_at_utc
 ) q(id,status,classification,blocking) where p_category='LEGACY_CANCELLATION_NOTICES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select s.id,s.status,'OBSOLETE_BOOKKEEPING'::text,false from public.banking_pay_workbench_sessions s where s.status='OPEN'
 ) q(id,status,classification,blocking) where p_category='LEGACY_WORKBENCH_SESSIONS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select j.id,j.status,'OBSOLETE_BOOKKEEPING'::text,false from public.banking_pay_workbench_jobs j where j.status in ('QUEUED','RUNNING','FAILED','DEAD')
 ) q(id,status,classification,blocking) where p_category='LEGACY_WORKBENCH_JOBS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select t.id,t.status,case when t.status='PENDING' and private.bpay_next_readiness_unused_batch_v1(t.pay_batch_id) then 'OBSOLETE_BOOKKEEPING' when t.status in ('PROCESSING','UNKNOWN') or t.rail_state in ('SUBMITTED','PENDING','PROCESSING','UNKNOWN') or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.pay_bank_transfer_id=t.id and ((s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null or s.provider_submit_attempt_count>0) and not private.bpay_next_readiness_scope_terminal_v1(s.id)))
    then 'UNRESOLVED_EXTERNAL' else 'INTEGRITY_REVIEW' end,not(t.status='PENDING' and private.bpay_next_readiness_unused_batch_v1(t.pay_batch_id)) from public.pay_bank_transfers t where t.status in ('PENDING','PROCESSING','UNKNOWN')
    or exists(select 1 from public.banking_pay_operation_transfer_scope s where s.pay_bank_transfer_id=t.id and ((s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null or s.provider_submit_attempt_count>0) and not private.bpay_next_readiness_scope_terminal_v1(s.id)))
 ) q(id,status,classification,blocking) where p_category='LEGACY_PROVIDER_TRANSFERS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select s.id,s.provider_submit_state,case when s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING') then 'INTEGRITY_REVIEW' else 'UNRESOLVED_EXTERNAL' end,true
    from public.banking_pay_operation_transfer_scope s where (s.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED') or s.provider_request_sending_at_utc is not null or s.provider_request_sent_at_utc is not null or nullif(pg_catalog.btrim(s.provider_transaction_id),'') is not null or nullif(pg_catalog.btrim(s.provider_request_id),'') is not null or s.provider_submit_attempt_count>0) and not private.bpay_next_readiness_scope_terminal_v1(s.id)
 ) q(id,status,classification,blocking) where p_category='LEGACY_PROVIDER_SCOPES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select a.id,a.new_state,case when a.transfer_scope_id is null then 'INTEGRITY_REVIEW' else 'UNRESOLVED_EXTERNAL' end,true
    from public.banking_pay_operation_provider_attempts a where not exists(select 1 from public.banking_pay_operation_transfer_scope s
    where s.id=a.transfer_scope_id and s.operation_id=a.operation_id and s.pay_batch_id=a.pay_batch_id and private.bpay_next_readiness_scope_terminal_v1(s.id)
    and (nullif(btrim(a.provider_request_id),'') is null or a.provider_request_id=s.provider_request_id)
    and (nullif(btrim(a.provider_transaction_id),'') is null or a.provider_transaction_id=s.provider_transaction_id)
    and (nullif(btrim(a.provider_idempotency_key),'') is null or a.provider_idempotency_key=s.provider_idempotency_key))
 ) q(id,status,classification,blocking) where p_category='LEGACY_PROVIDER_ATTEMPTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select e.id,e.normalised_state,case when e.mapping_status='MATCHED' and e.normalised_state in ('COMPLETED','RETURNED','REVERTED','FAILED','DECLINED','REJECTED','CANCELLED')
     and not private.bpay_next_readiness_event_terminal_v1(e.id) then 'INTEGRITY_REVIEW' else 'UNRESOLVED_EXTERNAL' end,true
   from public.pay_bank_transfer_events e where private.bpay_next_readiness_event_open_v1(e.id)
   or (e.mapping_status='MATCHED' and e.normalised_state in ('COMPLETED','RETURNED','REVERTED','FAILED','DECLINED','REJECTED','CANCELLED')
     and not private.bpay_next_readiness_event_terminal_v1(e.id))
   or (e.event_source='PROVIDER_WEBHOOK' and e.provider_signature_valid is distinct from true)

 ) q(id,status,classification,blocking) where p_category='LEGACY_PROVIDER_EVENTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   with normalized as (
    select w.id as receipt_id,n.value as event_json
    from public.bank_provider_webhook_receipts w cross join lateral jsonb_array_elements(case when jsonb_typeof(w.normalised_events_json)='array'
     then w.normalised_events_json else '[]'::jsonb end) n(value)
   ), expected as (
    select n.receipt_id,n.event_json,
     coalesce(nullif(btrim(n.event_json->>'provider_event_key'),''),nullif(btrim(n.event_json->>'idempotency_key'),''),nullif(btrim(n.event_json->>'event_key'),'')) as event_key
    from normalized n where n.event_json->'ignored' is distinct from 'true'::jsonb
   ), results as (
    select w.id as receipt_id,r.value as result_json,
     coalesce(nullif(btrim(r.value->>'provider_event_key'),''),nullif(btrim(r.value->>'idempotency_key'),''),nullif(btrim(r.value->>'event_key'),'')) as event_key
    from public.bank_provider_webhook_receipts w cross join lateral jsonb_array_elements(case when jsonb_typeof(w.ingest_results_json)='array'
     then w.ingest_results_json else '[]'::jsonb end) r(value)
   )
   select w.id,w.status,'INTEGRITY_REVIEW'::text,true from public.bank_provider_webhook_receipts w
   where w.status in ('RECEIVED','VERIFIED','NORMALISED','UNMATCHED_REVIEW_REQUIRED','FAILED_RETRYABLE','FAILED_FINAL')
   or (w.status in ('DUPLICATE','INGESTED') and (
    w.signature_valid is distinct from true or w.processed_at_utc is null
    or jsonb_typeof(w.normalised_events_json) is distinct from 'array' or jsonb_typeof(w.ingest_results_json) is distinct from 'array'
    or not exists(select 1 from normalized n where n.receipt_id=w.id)
    or exists(select 1 from normalized n where n.receipt_id=w.id and (
      jsonb_typeof(n.event_json) is distinct from 'object'
      or (n.event_json ? 'ignored' and jsonb_typeof(n.event_json->'ignored')<>'boolean')
      or exists(select 1 from jsonb_each(case when jsonb_typeof(n.event_json)='object' then n.event_json else '{}'::jsonb end) f where f.key in ('provider_event_key','idempotency_key','event_key','provider_event_type','provider_request_id','provider_transaction_id','ignored_reason') and jsonb_typeof(f.value) not in ('string','null'))))
    -- Compare ignored constituents as a multiset: supported nonfinancial
    -- normalizers may have no provider identity and may repeat an event type.
    -- A successful, completely covered all-ignored delivery is closed history.
    or exists(
      (select jsonb_build_array(coalesce(nullif(n.event_json->>'provider_event_key',''),nullif(n.event_json->>'idempotency_key','')),n.event_json->>'provider_event_type',n.event_json->>'provider_request_id',n.event_json->>'provider_transaction_id',coalesce(nullif(n.event_json->>'ignored_reason',''),'IGNORED_EVENT_TYPE')) as ignored_identity
       from normalized n where n.receipt_id=w.id and n.event_json->'ignored'='true'::jsonb
       except all
       select jsonb_build_array(r.result_json->>'provider_event_key',r.result_json->>'provider_event_type',r.result_json->>'provider_request_id',r.result_json->>'provider_transaction_id',r.result_json->>'ignored_reason')
       from results r where r.receipt_id=w.id and r.result_json->>'status'='IGNORED' and r.result_json->'ok'='true'::jsonb)
      union all
      (select jsonb_build_array(r.result_json->>'provider_event_key',r.result_json->>'provider_event_type',r.result_json->>'provider_request_id',r.result_json->>'provider_transaction_id',r.result_json->>'ignored_reason')
       from results r where r.receipt_id=w.id and r.result_json->>'status'='IGNORED' and r.result_json->'ok'='true'::jsonb
       except all
       select jsonb_build_array(coalesce(nullif(n.event_json->>'provider_event_key',''),nullif(n.event_json->>'idempotency_key','')),n.event_json->>'provider_event_type',n.event_json->>'provider_request_id',n.event_json->>'provider_transaction_id',coalesce(nullif(n.event_json->>'ignored_reason',''),'IGNORED_EVENT_TYPE'))
       from normalized n where n.receipt_id=w.id and n.event_json->'ignored'='true'::jsonb)
    )
    or exists(select 1 from expected n where n.receipt_id=w.id and (
      jsonb_typeof(n.event_json) is distinct from 'object' or n.event_key is null
      or (n.event_json ? 'provider_event_key' and jsonb_typeof(n.event_json->'provider_event_key') not in ('string','null'))
      or (n.event_json ? 'idempotency_key' and jsonb_typeof(n.event_json->'idempotency_key') not in ('string','null'))
      or (n.event_json ? 'event_key' and jsonb_typeof(n.event_json->'event_key') not in ('string','null'))
      or (n.event_json ? 'provider_request_id' and jsonb_typeof(n.event_json->'provider_request_id') not in ('string','null'))
      or (n.event_json ? 'provider_transaction_id' and jsonb_typeof(n.event_json->'provider_transaction_id') not in ('string','null'))
      or (select count(*) from expected dup where dup.receipt_id=w.id and dup.event_key=n.event_key)<>1
      or (select count(*) from results r where r.receipt_id=w.id and r.event_key=n.event_key)<>1
      or not exists(select 1 from results r where r.receipt_id=w.id and r.event_key=n.event_key
        and jsonb_typeof(r.result_json)='object' and r.result_json->'ok'='true'::jsonb
        and upper(btrim(r.result_json->>'status')) in ('INGESTED','DUPLICATE')
        and (n.event_json->>'provider_request_id' is null or r.result_json->>'provider_request_id'=n.event_json->>'provider_request_id')
        and (n.event_json->>'provider_transaction_id' is null or r.result_json->>'provider_transaction_id'=n.event_json->>'provider_transaction_id'))
      or not exists(select 1 from public.pay_bank_transfer_events e where e.provider_key=w.provider_key and e.rail_env=w.rail_env and e.provider_event_key=n.event_key
        and private.bpay_next_readiness_event_terminal_v1(e.id)
        and (n.event_json->>'provider_request_id' is null or e.provider_request_id=n.event_json->>'provider_request_id')
        and (n.event_json->>'provider_transaction_id' is null or e.provider_transaction_id=n.event_json->>'provider_transaction_id'))
    ))
    or exists(select 1 from results r where r.receipt_id=w.id and (
      jsonb_typeof(r.result_json) is distinct from 'object' or r.result_json->'ok' is distinct from 'true'::jsonb
      or upper(btrim(coalesce(r.result_json->>'status',''))) not in ('INGESTED','DUPLICATE','IGNORED')
      or (upper(btrim(r.result_json->>'status'))<>'IGNORED' and not exists(select 1 from expected n where n.receipt_id=w.id and n.event_key=r.event_key))))
    or exists(select 1 from public.pay_bank_transfer_events e where e.provider_webhook_receipt_id=w.id and private.bpay_next_readiness_event_open_v1(e.id))
   ))
 ) q(id,status,classification,blocking) where p_category='LEGACY_PROVIDER_WEBHOOKS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select s.id,s.status,'GENUINE_OBLIGATION'::text,true from public.banking_pay_operation_settlement_scope s where s.status in ('PENDING','FAILED')
 ) q(id,status,classification,blocking) where p_category='LEGACY_SETTLEMENT_SCOPE'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select a.id,a.payout_status::text,'UNDECIDED (OD-F5)'::text,true from public.pay_advances a where a.case_type::text='MANUAL_CREDIT_ADJUSTMENT'
    and a.taxability::text='NON_TAXABLE' and a.routing_kind::text='ONE_OFF_SPECIFIED_BANK_ACCOUNT' and a.payout_status::text='PENDING'
    and (a.outstanding_amount>0 or a.original_amount>0) and a.cleared_at_utc is null and a.written_off_at_utc is null
    and not exists(select 1 from private.bpay_next_stored_credit_origin o where o.legacy_case_id=a.id)
 ) q(id,status,classification,blocking) where p_category='LEGACY_UNBOUND_STORED_CREDITS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.publication_request_id,r.state,case when r.state='PENDING_FREEZE' then 'SOURCE_PENDING_MAPPING' else 'INTEGRITY_REVIEW' end,true from private.weekly_source_local_protected_decision_receipts r where r.state in ('PENDING_FREEZE','PREPARING')
 ) q(id,status,classification,blocking) where p_category='SOURCE_LOCAL_PENDING'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select b.id,b.state,'SOURCE_PENDING_MAPPING'::text,true from public.weekly_source_pending_entitlement_bundles b where b.state in ('PENDING','RELEASING','MANUAL_REVIEW')
 ) q(id,status,classification,blocking) where p_category='SOURCE_PENDING_BUNDLES'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.state,case when exists(select 1 from private.weekly_source_local_protected_decision_receipts l where l.publication_request_id=r.id and l.state='COMPLETE') then 'INTEGRITY_REVIEW' when r.state='READY' and r.c1_publication_id is null and r.c1_operation_id is null and r.c1_receipt_sha256 is null
    then 'OBSOLETE_BOOKKEEPING' else 'UNRESOLVED_EXTERNAL' end,exists(select 1 from private.weekly_source_local_protected_decision_receipts l where l.publication_request_id=r.id and l.state='COMPLETE') or not(r.state='READY' and r.c1_publication_id is null and r.c1_operation_id is null and r.c1_receipt_sha256 is null)
    from public.weekly_exceptional_c1_publication_requests r where r.state in ('READY','SUBMITTED','PENDING','RECOVERY_REQUIRED')
 ) q(id,status,classification,blocking) where p_category='C1_PUBLICATION_PENDING'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select u.id,u.state,'UNRESOLVED_EXTERNAL'::text,true from public.weekly_exceptional_c1_unknown_outcomes u where u.state='RECOVERY_REQUIRED'
 ) q(id,status,classification,blocking) where p_category='C1_UNKNOWN'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'STOP_RESUME_REVIEW'::text,true from private.bpay_next_pay_run r where r.status in ('PREPARING','REVIEW','DRAFT','CANCELLING','EXECUTING')
 ) q(id,status,classification,blocking) where p_category='NEXT_RUNS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'UNRESOLVED_EXTERNAL'::text,true from private.bpay_next_transfer r where r.status in ('ISSUED_CSV','SUBMITTED','UNKNOWN')
 ) q(id,status,classification,blocking) where p_category='NEXT_TRANSFERS_EXTERNAL'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'STOP_RESUME_REVIEW'::text,true from private.bpay_next_transfer r where r.status in ('BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','INTERNAL_PROCESSING')
 ) q(id,status,classification,blocking) where p_category='NEXT_TRANSFERS_UNFINISHED'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.effect_kind,'HISTORY'::text,false from private.bpay_next_financial_effect r where true
 ) q(id,status,classification,blocking) where p_category='NEXT_EFFECTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.event_kind,'HISTORY'::text,false from private.bpay_next_case_event r where true
 ) q(id,status,classification,blocking) where p_category='NEXT_CASE_EVENTS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'STOP_RESUME_REVIEW'::text,true from private.bpay_next_hold r where r.status='ACTIVE'
 ) q(id,status,classification,blocking) where p_category='NEXT_WORK_HOLDS'
 union all
 select q.id,q.status::text,q.classification::text,q.blocking from (
   select r.id,r.status,'STOP_RESUME_REVIEW'::text,true from private.bpay_next_case_hold r where r.status='ACTIVE'
 ) q(id,status,classification,blocking) where p_category='NEXT_CASE_HOLDS';
$rows$;

create or replace function public.bpay_next_cutover_readiness_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog,private
as $page$
declare
 v_role text; v_owner jsonb; v_section text; v_category text; v_after uuid; v_limit integer;
 v_categories text[]:=array['LEGACY_BATCHES','LEGACY_OPERATIONS','LEGACY_CORRECTION_REQUESTS','LEGACY_CORRECTION_WORK','LEGACY_SNOOZES','LEGACY_FINANCE_CASES','LEGACY_RESERVATIONS','LEGACY_PAYE_NETS','LEGACY_MAIL_UNSENT','LEGACY_MAIL_ATTEMPTS','LEGACY_COMMS_UNSENT','LEGACY_COMMS_ATTEMPTS','LEGACY_REMITTANCE_TARGETS','LEGACY_REMITTANCE_SCOPES','LEGACY_COMPLETION_NOTICES','LEGACY_CANCELLATION_NOTICES','LEGACY_WORKBENCH_SESSIONS','LEGACY_WORKBENCH_JOBS','LEGACY_PROVIDER_TRANSFERS','LEGACY_PROVIDER_SCOPES','LEGACY_PROVIDER_ATTEMPTS','LEGACY_PROVIDER_EVENTS','LEGACY_PROVIDER_WEBHOOKS','LEGACY_SETTLEMENT_SCOPE','LEGACY_UNBOUND_STORED_CREDITS','SOURCE_LOCAL_PENDING','SOURCE_PENDING_BUNDLES','C1_PUBLICATION_PENDING','C1_UNKNOWN','NEXT_RUNS','NEXT_TRANSFERS_EXTERNAL','NEXT_TRANSFERS_UNFINISHED','NEXT_EFFECTS','NEXT_CASE_EVENTS','NEXT_WORK_HOLDS','NEXT_CASE_HOLDS'];
 v_name text; v_categories_json jsonb:='[]'::jsonb; v_summary jsonb; v_result jsonb;
 v_count bigint; v_blocking bigint; v_total_blocking bigint:=0;
 v_items jsonb; v_more boolean; v_next uuid; v_classes jsonb; v_page_limit integer;
begin
 begin
  v_role:=coalesce(nullif(current_setting('request.jwt.claim.role',true),''),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','');
 exception when invalid_text_representation then
  raise exception using errcode='42501',message='BPAY_NEXT_READINESS_FORBIDDEN';
 end;
 if v_role<>'service_role' then raise exception using errcode='42501',message='BPAY_NEXT_READINESS_FORBIDDEN'; end if;
 v_owner:=public.bpay_next_owner_status_v1();
 if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>4096
 or exists(select 1 from jsonb_object_keys(p_request) k where k not in ('section','category','after_id','limit')) then
  raise exception using errcode='22023',message='BPAY_NEXT_READINESS_REQUEST_INVALID';
 end if;
 if (p_request ? 'section' and jsonb_typeof(p_request->'section')<>'string')
 or (p_request ? 'category' and jsonb_typeof(p_request->'category')<>'string') then
  raise exception using errcode='22023',message='BPAY_NEXT_READINESS_REQUEST_INVALID';
 end if;
 v_section:=coalesce(p_request->>'section','ALL');v_category:=coalesce(p_request->>'category','ALL');
 if v_section not in ('ALL','LEGACY','NEXT') or (v_category<>'ALL' and not(v_category=any(v_categories)))
 or (v_category<>'ALL' and v_section='NEXT' and left(v_category,5)<>'NEXT_')
 or (v_category<>'ALL' and v_section='LEGACY' and left(v_category,5)='NEXT_') then
  raise exception using errcode='22023',message='BPAY_NEXT_READINESS_CATEGORY_INVALID';
 end if;
 if p_request ? 'after_id' and p_request->'after_id'<>'null'::jsonb then
  if jsonb_typeof(p_request->'after_id')<>'string' or (p_request->>'after_id') !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_READINESS_CURSOR_INVALID';
  end if;
  v_after:=(p_request->>'after_id')::uuid;
 end if;
 if v_category='ALL' and v_after is not null then raise exception using errcode='22023',message='BPAY_NEXT_READINESS_CURSOR_INVALID';end if;
 v_limit:=25;
 if p_request ? 'limit' then
  if jsonb_typeof(p_request->'limit')<>'number' or (p_request->>'limit') !~ '^[1-9][0-9]{0,2}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_READINESS_LIMIT_INVALID';
  end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit>100 then raise exception using errcode='22023',message='BPAY_NEXT_READINESS_LIMIT_INVALID';end if;
 end if;
 v_page_limit:=case when v_category='ALL' then 3 else v_limit end;
 foreach v_name in array v_categories loop
  if v_section<>'ALL' and not((v_section='NEXT' and left(v_name,5)='NEXT_') or (v_section='LEGACY' and left(v_name,5)<>'NEXT_')) then continue;end if;
  -- Readiness always covers the whole requested section, including off-page
  -- categories. A page containing no blocking rows can never clear a section.
  if v_category='ALL' or v_name=v_category then
   with eligible as materialized (select r.* from private.bpay_next_readiness_rows_v1(v_name) r),
   classes as (select r.classification,count(*) n from eligible r group by r.classification),
   page_plus_one as materialized (
    select r.* from eligible r where v_after is null or r.id>v_after order by r.id limit (v_page_limit+1)
   ), page as (select r.* from page_plus_one r order by r.id limit v_page_limit)
   select (select count(*) from eligible),(select count(*) from eligible r where r.blocking),
    coalesce((select jsonb_object_agg(c.classification,c.n::text) from classes c),'{}'::jsonb),
    coalesce((select jsonb_agg(jsonb_build_object('id',p.id::text,'status',p.status,'classification',p.classification,'blocking',p.blocking) order by p.id) from page p),'[]'::jsonb),
    (select count(*)>v_page_limit from page_plus_one), (select p.id from page p order by p.id desc limit 1)
    into v_count,v_blocking,v_classes,v_items,v_more,v_next;
   v_summary:=jsonb_build_object('category',v_name,'count',v_count::text,'blocking_count',v_blocking::text,'blocking',v_blocking>0,
    'classification',case when v_count=0 then 'EMPTY' when (select count(*) from jsonb_object_keys(v_classes))=1 then (select k from jsonb_object_keys(v_classes) k limit 1) else 'MIXED' end,
    'class_counts',v_classes,'items',v_items,'has_more',v_more,'next_after_id',case when v_more then v_next::text else null end);
   v_categories_json:=v_categories_json||jsonb_build_array(v_summary);
  else
   select count(*),count(*) filter(where r.blocking) into v_count,v_blocking from private.bpay_next_readiness_rows_v1(v_name) r;
  end if;
  v_total_blocking:=v_total_blocking+v_blocking;
 end loop;
 v_result:=jsonb_build_object('active_owner',v_owner->>'active_owner','owner_epoch',v_owner->>'owner_epoch','section',v_section,
  'category',v_category,'categories',v_categories_json,'database_ready',v_total_blocking=0,'blocking_count',v_total_blocking::text,
  'external_quiet_window','UNPROVEN','activation_ready',false);
 if octet_length(v_result::text)>65536 then raise exception using errcode='54000',message='BPAY_NEXT_READINESS_RESPONSE_TOO_LARGE';end if;
 return v_result;
end;
$page$;

alter function private.bpay_next_readiness_marker_v1(text,text,text,text) owner to postgres;
revoke all on function private.bpay_next_readiness_marker_v1(text,text,text,text) from public,anon,authenticated,service_role;

alter function private.bpay_next_readiness_scope_terminal_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_readiness_scope_terminal_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_readiness_unused_batch_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_readiness_unused_batch_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_readiness_event_terminal_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_readiness_event_terminal_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_readiness_event_open_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_readiness_event_open_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_readiness_rows_v1(text) owner to postgres;
revoke all on function private.bpay_next_readiness_rows_v1(text) from public,anon,authenticated,service_role;

alter function public.bpay_next_cutover_readiness_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_cutover_readiness_page_v1(jsonb) from public,anon,authenticated,service_role;

grant execute on function public.bpay_next_cutover_readiness_page_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
