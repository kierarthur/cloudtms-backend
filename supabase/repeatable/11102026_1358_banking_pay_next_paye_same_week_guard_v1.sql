-- Repeatable CloudTMS function/view authority: banking_pay_next_paye_same_week_guard_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- DR-10 / BD111. Context is re-derived by the existing service-only HEADER.
create or replace function private.bpay_next_paye_guard_context_v1(p_actor uuid,p_context jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare n jsonb;h jsonb;k text;
begin
 if p_actor is null or jsonb_typeof(p_context) is distinct from 'object'
   or not(p_context ?& array['namespace','workbench_revision','adoption_revision'])
   or p_context-array['namespace','workbench_revision','adoption_revision']<>'{}'::jsonb
   or octet_length(p_context::text)>2048 then
  raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 n:=p_context->'namespace';
 if jsonb_typeof(n) is distinct from 'object'
   or not(n ?& array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope'])
   or n-array['agency_id','owner_epoch','actor_user_id','official_pay_date','scope']<>'{}'::jsonb
   or jsonb_typeof(n->'scope') is distinct from 'object'
   or not(n->'scope' ?& array['candidate_id','client_id','channel'])
   or (n->'scope')-array['candidate_id','client_id','channel']<>'{}'::jsonb
   or jsonb_typeof(n->'scope'->'channel') is distinct from 'string'
   or n->'scope'->>'channel' not in ('ALL','PAYE','UMBRELLA') then
  raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 foreach k in array array['agency_id','actor_user_id'] loop
  if jsonb_typeof(n->k) is distinct from 'string' or n->>k!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
   raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 end loop;
 foreach k in array array['candidate_id','client_id'] loop
  if n->'scope'->k<>'null'::jsonb and (jsonb_typeof(n->'scope'->k) is distinct from 'string'
    or n->'scope'->>k!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
   raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 end loop;
 foreach k in array array['workbench_revision','adoption_revision'] loop
  if jsonb_typeof(p_context->k) is distinct from 'string' or p_context->>k!~'^(0|[1-9][0-9]{0,18})$' then
   raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 end loop;
 if jsonb_typeof(n->'owner_epoch') is distinct from 'string' or n->>'owner_epoch'!~'^[1-9][0-9]{0,18}$'
   or jsonb_typeof(n->'official_pay_date') is distinct from 'string' or n->>'official_pay_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
  raise exception using errcode='22023',message='BPAY_NEXT_REAUTH_REQUEST_INVALID';end if;
 h:=public.bpay_next_workbench_page_v1(jsonb_build_object('actor_user_id',p_actor,'kind','HEADER',
   'candidate_id',n->'scope'->'candidate_id','client_id',n->'scope'->'client_id','channel',n->'scope'->>'channel',
   'section','READY','search','','sort','NAME_ASC','group_key',null,'item_id',null,'reason_code',null,
   'after',null,'limit',100,'count_selection',null));
 if n->>'actor_user_id' is distinct from p_actor::text
   or n->>'agency_id' is distinct from h->'context'->>'agency_id'
   or n->>'owner_epoch' is distinct from h->'context'->>'owner_epoch'
   or n->>'official_pay_date' is distinct from h->'context'->>'official_pay_date'
   or p_context->>'workbench_revision' is distinct from h->'context'->>'workbench_revision'
   or p_context->>'adoption_revision' is distinct from h->'context'->>'adoption_revision' then
  raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
 return p_context;
end
$function$;

-- Native indexed official-week seek. No current Candidate channel inference.
-- Reuse C15's exact original-leg receipt/disposition reader, never its writers.
create or replace function private.bpay_next_paye_same_week_facts_v1(p_pay_date date,p_run_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare v_start date;v_rows jsonb:='[]';r record;w record;t record;s record;
 v_block boolean;v_paid boolean;v_relevant boolean;v_has boolean;v_n integer:=0;v_ref text;
begin
 if p_pay_date is null or not isfinite(p_pay_date) or p_run_id is null then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 v_start:=date_trunc('week',p_pay_date::timestamp)::date;
 for r in select pr.id,pr.status,pr.confirmed_at_utc from private.bpay_next_pay_run pr
   where pr.pay_date>=v_start and pr.pay_date<v_start+7 and pr.id<>p_run_id
     and exists(select 1 from private.bpay_next_selection_candidate sc where sc.run_id=pr.id and sc.target_pay_channel='PAYE')
   order by pr.created_at_utc,pr.id loop
  v_block:=false;v_paid:=false;v_relevant:=false;
  for w in select sc.candidate_id,rw.id as worker_id,rw.status,rw.preparation_revision,rw.captured_preparation_revision
    from private.bpay_next_selection_candidate sc left join private.bpay_next_run_worker rw
      on rw.run_id=sc.run_id and rw.candidate_id=sc.candidate_id
    where sc.run_id=r.id and sc.target_pay_channel='PAYE' order by sc.candidate_id loop
   -- Only an exact completed native release can remove a pre-Draft member.
   if w.status='CANCELLED' and exists(select 1 from private.bpay_next_worker_reprepare_request q
     where q.run_id=r.id and q.run_worker_id=w.worker_id and q.action='RELEASE' and q.status='COMPLETE'
       and q.target_preparation_revision=w.preparation_revision
       and q.captured_preparation_revision=w.captured_preparation_revision
       and q.released_work_count=q.expected_work_count and q.released_case_hold_count=q.expected_case_hold_count)
     and not exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=w.worker_id) then continue;end if;
   -- DR09 completed per-Candidate disposition, including PARTIAL survivors.
   if exists(select 1 from private.bpay_next_preparation_discard_worker x
     join private.bpay_next_preparation_discard_request q on q.command_id=x.command_id and q.run_id=x.run_id
     where x.run_id=r.id and x.candidate_id=w.candidate_id and x.run_worker_id is not distinct from w.worker_id
       and x.status='DISCARDED' and x.stage='COMPLETE' and x.finished_at_utc is not null
       and x.outcome in ('REMOVED','ALREADY_REMOVED') and x.blocked_code is null
       and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count
       and (w.worker_id is null or w.status='CANCELLED' and x.expected_execution_revision=w.preparation_revision
         and x.preparation_revision=w.captured_preparation_revision)
       and q.status in ('DISCARDED','PARTIAL') and q.finished_at_utc is not null
       and q.completed_candidate_count=q.expected_candidate_count and q.enrolled_candidate_count=q.expected_candidate_count)
     and not exists(select 1 from private.bpay_next_transfer tr where tr.run_worker_id=w.worker_id) then continue;end if;
   if exists(select 1 from private.bpay_next_preparation_expiry_worker x
     join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
     where x.run_id=r.id and x.candidate_id=w.candidate_id and x.run_worker_id is not distinct from w.worker_id
       and x.status='EXPIRED' and x.stage='COMPLETE' and x.finished_at_utc is not null
       and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count
       and (w.worker_id is null or w.status='CANCELLED' and coalesce(x.expected_execution_revision,x.preparation_revision)=w.preparation_revision
         and x.preparation_revision=w.captured_preparation_revision)
       and q.status='EXPIRED' and q.finished_at_utc is not null
       and q.completed_candidate_count=q.expected_candidate_count and q.enrolled_candidate_count=q.expected_candidate_count)
     and not exists(select 1 from private.bpay_next_transfer tr where tr.run_worker_id=w.worker_id) then continue;end if;
   if w.worker_id is null then
    if r.status='CANCELLED' and exists(select 1 from private.bpay_next_preparation_discard_request q
      where q.run_id=r.id and q.status='DISCARDED' and q.finished_at_utc is not null
       and q.blocked_candidate_count=0 and q.completed_candidate_count=q.expected_candidate_count and q.enrolled_candidate_count=q.expected_candidate_count) then continue;end if;
    v_relevant:=true;v_block:=true;continue;
   end if;
   select * into strict s from private.bpay_next_original_reservation_v1(w.worker_id,null);
   if s.reservation_state='CANCELLED' and not exists(select 1 from private.bpay_next_transfer_outcome o
     join private.bpay_next_transfer t on t.id=o.transfer_id where t.run_worker_id=w.worker_id) then continue;end if;
   v_relevant:=true;v_has:=false;
   if r.confirmed_at_utc is null then v_block:=true;continue;end if;
   for t in select tr.id from private.bpay_next_transfer tr where tr.run_worker_id=w.worker_id
     and tr.candidate_id=w.candidate_id and tr.original_transfer_id is null and tr.return_cash_id is null
     order by tr.transfer_no,tr.id loop
    v_has:=true;
    select * into strict s from private.bpay_next_original_reservation_v1(w.worker_id,t.id);
    if s.reservation_state not in ('POSTED','RETURNED','CLOSED_INTERNAL') then v_block:=true;
    else v_paid:=true;end if;
   end loop;
   if not v_has then v_block:=true;end if;
   if exists(select 1 from private.bpay_next_destination_group g where g.run_worker_id=w.worker_id
     and (g.stage<>'COMPLETE' or g.completed_at_utc is null or g.created_leg_count<>g.expected_leg_count
       or g.sealed_leg_count<>g.expected_leg_count or g.expected_leg_count<>(select count(*)
         from private.bpay_next_destination_group_leg l where l.anchor_transfer_id=g.anchor_transfer_id))) then
    v_block:=true;end if;
  end loop;
  if v_relevant then
   v_n:=v_n+1;if v_n>100 then raise exception using errcode='54000',message='BPAY_NEXT_PAYE_GUARD_FACTS_UNAVAILABLE';end if;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('run_id',r.id,'reference',null,'legacy',false,
     'unexecuted',v_block,'status',case when v_paid and not v_block then 'Paid' else 'Not paid yet' end));
  end if;
 end loop;
 -- Conditional genuine retained old facts only; unused preview shells are proved
 -- by F18's existing positive helper. This is not an observed-zero census.
 for r in select b.* from public.pay_batches b where
   ((b.authoritative_payment_date>=v_start and b.authoritative_payment_date<v_start+7)
     or (b.authoritative_payment_date is null and b.pay_date>=v_start and b.pay_date<v_start+7))
   and coalesce(b.batch_kind_fixed,'')<>'LOANS'
   and (b.batch_kind_fixed='PAYE' or exists(select 1 from public.pay_batch_items i
     join public.pay_batch_candidates c on c.id=i.pay_batch_candidate_id
     where c.pay_batch_id=b.id and i.pay_channel='PAYE' and not coalesce(i.is_voided,false))
     or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=b.id and t.pay_channel='PAYE')
     or exists(select 1 from public.pay_batch_paye_net_inputs n join public.pay_batch_candidates c
       on c.id=n.pay_batch_candidate_id where c.pay_batch_id=b.id)
     or exists(select 1 from public.pay_advance_reservations x where x.pay_batch_id=b.id
       and x.frozen_target_pay_method='PAYE' and x.status in ('RESERVED','COMMITTED'))
     or exists(select 1 from public.banking_pay_operation_transfer_scope x where x.pay_batch_id=b.id and x.pay_channel='PAYE'
       and (x.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED')
         or x.provider_request_sending_at_utc is not null or x.provider_request_sent_at_utc is not null
         or nullif(btrim(x.provider_request_id),'') is not null or nullif(btrim(x.provider_transaction_id),'') is not null or x.provider_submit_attempt_count>0)))
   and private.bpay_next_readiness_unused_batch_v1(b.id) is not true
   order by b.created_at_utc,b.id loop
  -- No silent 'terminal=paid': require actual settled payroll members, or exact
  -- matched/signature-qualified COMPLETED transfers via F18's terminal reader.
  v_paid:=exists(select 1 from public.pay_batch_candidates c where c.pay_batch_id=r.id
    and c.settlement_status='SETTLED' and c.settled_at_utc is not null
    and (exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id)
      or exists(select 1 from public.pay_batch_items i where i.pay_batch_candidate_id=c.id
        and i.pay_channel='PAYE' and not coalesce(i.is_voided,false))));
  v_paid:=v_paid or exists(select 1 from public.pay_bank_transfers tr where tr.pay_batch_id=r.id and tr.pay_channel='PAYE'
    and tr.status='COMPLETED' and tr.completed_at_utc is not null and exists(select 1 from public.pay_bank_transfer_events e
      where e.pay_bank_transfer_id=tr.id and e.pay_batch_id=r.id and e.normalised_state='COMPLETED'
        and private.bpay_next_readiness_event_terminal_v1(e.id)));
  v_block:=not v_paid or exists(select 1 from public.pay_batch_candidates c where c.pay_batch_id=r.id
    and (exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id)
      or exists(select 1 from public.pay_batch_items i where i.pay_batch_candidate_id=c.id
        and i.pay_channel='PAYE' and not coalesce(i.is_voided,false)))
    and (c.settlement_status is distinct from 'SETTLED' or c.settled_at_utc is null))
    or exists(select 1 from public.pay_bank_transfers t where t.pay_batch_id=r.id and t.pay_channel='PAYE'
      and (t.status is distinct from 'COMPLETED' or t.completed_at_utc is null or not exists(
        select 1 from public.pay_bank_transfer_events e where e.pay_bank_transfer_id=t.id
          and private.bpay_next_readiness_event_terminal_v1(e.id))))
    or exists(select 1 from public.banking_pay_operation_settlement_scope q where q.pay_batch_id=r.id and q.pay_channel='PAYE' and q.status in ('PENDING','FAILED'))
    or exists(select 1 from public.pay_advance_reservations reservation where reservation.pay_batch_id=r.id and reservation.status in ('RESERVED','COMMITTED')
      and (r.batch_kind_fixed='PAYE' or reservation.frozen_target_pay_method='PAYE'
        or not exists(select 1 from public.pay_batch_candidates c where c.id=reservation.pay_batch_candidate_id and c.pay_batch_id=reservation.pay_batch_id
          and (reservation.pay_batch_item_id is null or exists(select 1 from public.pay_batch_items i where i.id=reservation.pay_batch_item_id and i.pay_batch_candidate_id=c.id)))
        or exists(select 1 from public.pay_batch_candidates c where c.id=reservation.pay_batch_candidate_id and c.pay_batch_id=reservation.pay_batch_id
        and (exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=c.id)
          or exists(select 1 from public.pay_batch_items i where i.pay_batch_candidate_id=c.id and i.pay_channel='PAYE' and not coalesce(i.is_voided,false))))))
    or exists(select 1 from public.banking_pay_operation_transfer_scope scope left join public.pay_bank_transfers tr on tr.id=scope.pay_bank_transfer_id
      where scope.pay_batch_id=r.id and (scope.pay_channel='PAYE' or tr.pay_channel='PAYE'
        or scope.pay_channel is null or (scope.pay_bank_transfer_id is not null and (tr.id is null or tr.pay_batch_id is distinct from scope.pay_batch_id or tr.pay_channel is distinct from scope.pay_channel)))
        and (scope.provider_submit_state in ('CLAIMED','REQUEST_PREPARING','REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED')
          or scope.provider_request_sending_at_utc is not null or scope.provider_request_sent_at_utc is not null
          or nullif(btrim(scope.provider_transaction_id),'') is not null or nullif(btrim(scope.provider_request_id),'') is not null or scope.provider_submit_attempt_count>0)
        and (private.bpay_next_readiness_scope_terminal_v1(scope.id) is distinct from true
          or tr.pay_batch_id is distinct from scope.pay_batch_id or tr.pay_channel is distinct from scope.pay_channel))
    or exists(select 1 from public.banking_pay_operation_provider_attempts a
      left join public.banking_pay_operation_transfer_scope scope on scope.id=a.transfer_scope_id
      left join public.pay_bank_transfers tr on tr.id=scope.pay_bank_transfer_id
      where a.pay_batch_id=r.id and (scope.pay_channel='PAYE' or tr.pay_channel='PAYE' or scope.id is null
        or scope.operation_id is distinct from a.operation_id or scope.pay_batch_id is distinct from a.pay_batch_id)
        and a.new_state in ('REQUEST_SENDING','REQUEST_SENT_LOCAL','PROVIDER_ACCEPTED','PROVIDER_REJECTED','PROVIDER_UNKNOWN','REVIEW_REQUIRED','CHUNK_FINALISED')
        and not exists(select 1 from public.banking_pay_operation_transfer_scope done
          join public.pay_bank_transfers dt on dt.id=done.pay_bank_transfer_id and dt.pay_batch_id=done.pay_batch_id and dt.pay_channel=done.pay_channel
          where done.id=a.transfer_scope_id
          and done.operation_id=a.operation_id and done.pay_batch_id=a.pay_batch_id and done.pay_channel='PAYE'
          and (nullif(btrim(a.provider_request_id),'') is null or a.provider_request_id=done.provider_request_id)
          and (nullif(btrim(a.provider_transaction_id),'') is null or a.provider_transaction_id=done.provider_transaction_id)
          and (nullif(btrim(a.provider_idempotency_key),'') is null or a.provider_idempotency_key=done.provider_idempotency_key)
          and private.bpay_next_readiness_scope_terminal_v1(done.id)))
    or exists(select 1 from public.pay_bank_transfer_events e left join public.pay_bank_transfers tr on tr.id=e.pay_bank_transfer_id
      where e.pay_batch_id=r.id and (tr.pay_channel='PAYE' or tr.id is null or tr.pay_batch_id is distinct from e.pay_batch_id)
        and (tr.id is null or tr.pay_batch_id is distinct from e.pay_batch_id or private.bpay_next_readiness_event_open_v1(e.id)
          or (e.mapping_status='MATCHED' and e.normalised_state in ('COMPLETED','RETURNED','REVERTED','FAILED','DECLINED','REJECTED','CANCELLED')
            and private.bpay_next_readiness_event_terminal_v1(e.id) is distinct from true)
          or (e.event_source='PROVIDER_WEBHOOK' and e.provider_signature_valid is distinct from true)))
    or exists(select 1 from public.pay_payment_correction_requests correction
      left join public.pay_bank_transfer_events source_event on source_event.id=correction.source_bank_event_id
      left join public.pay_bank_transfers source_transfer on source_transfer.id=source_event.pay_bank_transfer_id
      where correction.pay_batch_id=r.id
        and (correction.authorised_at_utc is not null or correction.source_bank_event_id is not null
          or exists(select 1 from public.pay_payment_correction_work_items x where x.correction_request_id=correction.id and x.pay_bank_transfer_id is not null))
        and (correction.status not in ('APPLIED','REJECTED','CANCELLED') or exists(
          select 1 from public.pay_payment_correction_work_items x where x.correction_request_id=correction.id
            and x.status in ('PENDING','PROCESSING','BLOCKED','FAILED_RETRYABLE','FAILED_FINAL')))
        and (r.batch_kind_fixed='PAYE' or source_transfer.pay_channel='PAYE'
          or (correction.source_bank_event_id is not null and (source_transfer.id is null or source_event.pay_batch_id is distinct from r.id or source_transfer.pay_batch_id is distinct from r.id))
          or exists(select 1 from public.pay_payment_correction_work_items x
            left join public.pay_bank_transfers xt on xt.id=x.pay_bank_transfer_id
            left join public.pay_batch_candidates xc on xc.id=x.pay_batch_candidate_id
            where x.correction_request_id=correction.id and (xt.pay_channel='PAYE'
              or x.pay_batch_id is distinct from correction.pay_batch_id
              or (x.pay_bank_transfer_id is not null and (xt.id is null or xt.pay_batch_id is distinct from x.pay_batch_id))
              or (x.pay_batch_candidate_id is not null and (xc.id is null or xc.pay_batch_id is distinct from x.pay_batch_id or xc.candidate_id is distinct from x.candidate_id))
              or (xc.pay_batch_id=x.pay_batch_id and xc.candidate_id=x.candidate_id and (
                exists(select 1 from public.pay_batch_paye_net_inputs n where n.pay_batch_candidate_id=xc.id)
                or exists(select 1 from public.pay_batch_items i where i.pay_batch_candidate_id=xc.id and i.pay_channel='PAYE' and not coalesce(i.is_voided,false))))))
          or (correction.authorised_at_utc is not null and correction.source_bank_event_id is null
            and not exists(select 1 from public.pay_payment_correction_work_items x where x.correction_request_id=correction.id))));
  v_ref:=nullif(btrim(r.bulk_reference),'');
  if length(v_ref)>80 or v_ref~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then v_ref:=null;end if;
  v_n:=v_n+1;if v_n>100 then raise exception using errcode='54000',message='BPAY_NEXT_PAYE_GUARD_FACTS_UNAVAILABLE';end if;
  v_rows:=v_rows||jsonb_build_array(jsonb_build_object('run_id',r.id,'reference',v_ref,'legacy',true,
    'unexecuted',v_block,'status',case when v_paid and not v_block then 'Paid' else 'Not paid yet' end));
 end loop;
 return v_rows;
end
$function$;

create or replace function private.bpay_next_paye_guard_page_v1(p_actor uuid,p_run_id uuid,p_descriptor jsonb default null)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private,public
as $function$
declare r private.bpay_next_pay_run%rowtype;ctx jsonb;facts jsonb;d date;p bigint:=0;u bigint:=0;baseline uuid;header jsonb;memory jsonb;
begin
 if p_actor is null or p_run_id is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 select src.* into r from private.bpay_next_pay_run src where src.id=p_run_id;
 if found and r.accepted_choice_descriptor is not null then
  if r.actor_user_id is distinct from p_actor or (p_descriptor is not null and p_descriptor is distinct from r.accepted_choice_descriptor) then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_DESCRIPTOR_REPLAY_CONFLICT';end if;
  d:=r.pay_date;ctx:=r.accepted_choice_descriptor-array['intent','revision','memory_baseline_run_id'];
  select count(*) filter(where target_pay_channel='PAYE'),count(*) filter(where target_pay_channel='UMBRELLA')
    into p,u from private.bpay_next_selection_candidate where run_id=p_run_id;
 else
  if jsonb_typeof(p_descriptor) is distinct from 'object' or octet_length(p_descriptor::text)>32768
    or not(p_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision'])
    or p_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb
    or not private.bpay_next_selection_memory_intent_valid_v1(p_descriptor->'intent') then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  ctx:=private.bpay_next_paye_guard_context_v1(p_actor,p_descriptor-array['intent','revision','memory_baseline_run_id']);
  if jsonb_typeof(p_descriptor->'revision') is distinct from 'number' or p_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$'
    or (p_descriptor ? 'memory_baseline_run_id' and p_descriptor->'memory_baseline_run_id'<>'null'::jsonb
      and (jsonb_typeof(p_descriptor->'memory_baseline_run_id') is distinct from 'string'
        or p_descriptor->>'memory_baseline_run_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
   raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_DESCRIPTOR_INVALID';end if;
  memory:=jsonb_build_object('run_id',coalesce(p_descriptor->'memory_baseline_run_id','null'::jsonb),'scope',ctx->'namespace'->'scope');
  baseline:=private.bpay_next_selection_memory_resolve_v1(ctx->'namespace',memory);
  header:=public.bpay_next_workbench_page_v1(jsonb_build_object('actor_user_id',p_actor,'kind','HEADER',
    'candidate_id',ctx->'namespace'->'scope'->'candidate_id','client_id',ctx->'namespace'->'scope'->'client_id',
    'channel',ctx->'namespace'->'scope'->>'channel','section','READY','search','','sort','NAME_ASC',
    'group_key',null,'item_id',null,'reason_code',null,'after',null,'limit',100,'selection_memory',memory,
    'count_selection',jsonb_build_object('intent',p_descriptor->'intent','memory_baseline_run_id',baseline)));
  if header->'complete' is distinct from 'true'::jsonb then
   raise exception using errcode='55000',message='BPAY_NEXT_PAYE_GUARD_FACTS_UNAVAILABLE';end if;
  if header->'context'->>'workbench_revision' is distinct from ctx->>'workbench_revision'
    or header->'context'->>'adoption_revision' is distinct from ctx->>'adoption_revision' then
   raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_CONTEXT_CHANGED';end if;
  d:=(ctx->'namespace'->>'official_pay_date')::date;
  with chosen as materialized(select distinct candidate_id from private.bpay_next_selection_memory_choices_v1(
    ctx->'namespace'->'scope',baseline,p_descriptor->'intent') where selected and direction='PAYMENT')
  select count(*) filter(where upper(c.pay_method)='PAYE'),count(*) filter(where upper(c.pay_method)='UMBRELLA')
    into p,u from chosen join public.candidates c on c.id=chosen.candidate_id;
 end if;
 facts:=private.bpay_next_paye_same_week_facts_v1(d,p_run_id);
 return jsonb_build_object('run_id',p_run_id,'guard_applies',p>0 and jsonb_array_length(facts)>0,
   'unexecuted',exists(select 1 from jsonb_array_elements(facts) f where f->'unexecuted'='true'::jsonb),
   'existing',facts,'paye_candidates',p::text,'umbrella_candidates',u::text,'context',ctx,
   'decision',case when r.accepted_choice_descriptor is not null then r.paye_guard_decision else 'NONE' end,
   'channel_scope',case when r.accepted_choice_descriptor is not null then r.channel_scope else 'ALL' end);
end
$function$;

alter function private.bpay_next_paye_guard_context_v1(uuid,jsonb) owner to postgres;
alter function private.bpay_next_paye_same_week_facts_v1(date,uuid) owner to postgres;
alter function private.bpay_next_paye_guard_page_v1(uuid,uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_paye_guard_context_v1(uuid,jsonb),
 private.bpay_next_paye_same_week_facts_v1(date,uuid),private.bpay_next_paye_guard_page_v1(uuid,uuid,jsonb)
 from public,anon,authenticated,service_role;
notify pgrst,'reload schema';

commit;
