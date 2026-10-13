-- Repeatable CloudTMS function/view authority: banking_pay_next_case_change_guard_v1
-- FC-16: factual accepted-input / exact ACTIVE hold protection, never allocation.

\set ON_ERROR_STOP on

begin;

-- VOLATILE is deliberate: a writer calls this after its existing component
-- lock may have waited for concurrent Create. Do not retain a pre-wait snapshot.
create or replace function private.bpay_next_case_protected_bindings_v1(
 p_case_id uuid,p_after jsonb default null,p_limit integer default null)
returns table(binding_kind text,run_id uuid,case_component_id uuid,native_id uuid,
  candidate_id uuid,run_worker_id uuid,channel text,run_status text,worker_status text,
  actor_user_id uuid,preparation_revision bigint,protected_amount numeric,next_action text)
language sql volatile security invoker set search_path=pg_catalog,private
as $function$
 with accepted as (
  select m.run_id,m.candidate_id,m.case_component_id,m.member_id as native_id
  from private.bpay_next_case_component c
  join private.bpay_next_run_choice_member m on m.case_component_id=c.id
    and m.member_kind='CASE_COMPONENT' and m.selected
  join private.bpay_next_pay_run pr on pr.id=m.run_id and pr.accepted_choice_descriptor is not null
  where c.case_id=p_case_id
  union all
  select si.run_id,si.candidate_id,si.case_component_id,si.case_component_id
  from private.bpay_next_case_component component
  join private.bpay_next_case_selection_item si on si.case_component_id=component.id
  join private.bpay_next_case_selection cs on cs.run_id=si.run_id and cs.candidate_id=si.candidate_id
    and cs.selection_revision=si.selection_revision and cs.status='SEALED'
  left join private.bpay_next_run_worker rw on rw.run_id=si.run_id and rw.candidate_id=si.candidate_id
  where component.case_id=p_case_id and si.case_id=component.case_id and si.is_selected
    and (rw.id is null or rw.case_selection_revision=si.selection_revision)
    and not exists(select 1 from private.bpay_next_run_choice_member m
      where m.run_id=si.run_id and m.member_kind='CASE_COMPONENT' and m.member_id=si.case_component_id)
 ), pending as (
  select a.*,pr.status as run_status,pr.confirmed_at_utc,pr.actor_user_id,
    rw.id as worker_id,rw.status as worker_status,rw.preparation_revision,
    rw.captured_preparation_revision,rw.case_selection_revision
  from accepted a join private.bpay_next_pay_run pr on pr.id=a.run_id
  left join private.bpay_next_run_worker rw on rw.run_id=a.run_id and rw.candidate_id=a.candidate_id
  where not exists(select 1 from private.bpay_next_case_hold h
      where h.case_id=p_case_id and h.case_component_id=a.case_component_id
        and h.run_worker_id=rw.id and h.status='ACTIVE')
    -- Exact completed original disposition, not a CANCELLED flag or deadline.
    and not (pr.confirmed_at_utc is null and rw.id is not null
      and private.bpay_next_worker_release_complete_v1(rw.id,pg_catalog.clock_timestamp()))
    and not exists(select 1 from private.bpay_next_preparation_discard_worker x
      join private.bpay_next_preparation_discard_request q on q.command_id=x.command_id and q.run_id=x.run_id
      join private.bpay_next_job j on j.id=x.job_id and j.command_id=x.command_id and j.candidate_id=x.candidate_id
        and j.job_kind='PREPARATION_DISCARD' and j.status='DONE' and j.phase='DISCARDED'
        and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      join private.bpay_next_command cmd on cmd.id=q.command_id and cmd.command_kind='PREPARATION_DISCARD'
        and cmd.module_epoch=j.module_epoch
      where pr.confirmed_at_utc is null and x.run_id=a.run_id and x.candidate_id=a.candidate_id
        and x.run_worker_id is not distinct from rw.id and x.status='DISCARDED' and x.stage='COMPLETE'
        and x.finished_at_utc is not null and x.outcome in ('REMOVED','ALREADY_REMOVED') and x.blocked_code is null
        and x.released_work_count=x.expected_work_count and x.released_case_hold_count=x.expected_case_hold_count
        and (rw.id is null or rw.status='CANCELLED' and x.expected_execution_revision=rw.preparation_revision
          and x.preparation_revision=rw.captured_preparation_revision)
        and q.status in ('CANCELLING','DISCARDED','PARTIAL')
        and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command oc
          on oc.id=rc.command_id and oc.command_kind='PREPARE' and oc.module_epoch=j.module_epoch
          where rc.run_id=a.run_id and rc.command_id=q.original_prepare_command_id)
        and not exists(select 1 from private.bpay_next_transfer tr where tr.run_worker_id=rw.id))
    and not exists(select 1 from private.bpay_next_preparation_expiry_worker x
      join private.bpay_next_preparation_expiry_request q on q.command_id=x.command_id and q.run_id=x.run_id
      join private.bpay_next_job j on j.id=x.job_id and j.command_id=x.command_id and j.candidate_id=x.candidate_id
        and j.job_kind='PREPARATION_EXPIRY' and j.status='DONE' and j.phase='EXPIRED'
        and j.cursor_key is not distinct from nullif(x.checkpoint,0)::text
      join private.bpay_next_command cmd on cmd.id=q.command_id and cmd.command_kind='PREPARATION_EXPIRY'
        and cmd.module_epoch=j.module_epoch
      where pr.confirmed_at_utc is null and x.run_id=a.run_id and x.candidate_id=a.candidate_id
        and x.run_worker_id is not distinct from rw.id and x.status='EXPIRED' and x.stage='COMPLETE'
        and x.finished_at_utc is not null and x.released_work_count=x.expected_work_count
        and x.released_case_hold_count=x.expected_case_hold_count
        and (rw.id is null or rw.status='CANCELLED'
          and coalesce(x.expected_execution_revision,x.preparation_revision)=rw.preparation_revision
          and x.preparation_revision=rw.captured_preparation_revision)
        and q.status in ('CANCELLING','EXPIRED')
        and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command oc
          on oc.id=rc.command_id and oc.command_kind='PREPARE' and oc.module_epoch=j.module_epoch
          where rc.run_id=a.run_id and rc.command_id=q.original_prepare_command_id)
        and not exists(select 1 from private.bpay_next_transfer tr where tr.run_worker_id=rw.id))
    and not exists(select 1 from private.bpay_next_cancel_request cr
      join private.bpay_next_case_cancel_binding cb on cb.command_id=cr.command_id
        and cb.run_worker_id=cr.run_worker_id and cb.candidate_id=cr.candidate_id
        and cb.status='CANCELLED' and cb.stage='COMPLETE' and cb.finished_at_utc=cr.finished_at_utc
        and cb.blocked_code is null and cb.released_work_count=cb.expected_work_count
        and cb.released_case_hold_count=cb.expected_case_hold_count
        and cb.preparation_revision=rw.captured_preparation_revision and cb.selection_revision=rw.case_selection_revision
      join private.bpay_next_job j on j.command_id=cr.command_id and j.candidate_id=cr.candidate_id
        and j.job_kind='SIMPLE_CANCEL' and j.status='DONE' and j.phase='CANCELLED'
        and j.cursor_key=cb.checkpoint::text
      join private.bpay_next_command cmd on cmd.id=cr.command_id and cmd.command_kind='SIMPLE_CANCEL'
        and cmd.status='COMPLETE' and cmd.module_epoch=j.module_epoch
      where cr.run_worker_id=rw.id and cr.candidate_id=a.candidate_id and cr.status='CANCELLED'
        and cr.finished_at_utc is not null and cr.blocked_code is null
        and cr.released_line_count=cr.expected_line_count and rw.status='CANCELLED'
        and rw.active_case_hold_count=0 and rw.realised_effect_count=0
        and not exists(select 1 from private.bpay_next_case_hold h where h.run_worker_id=rw.id and h.status='ACTIVE')
        and not exists(select 1 from private.bpay_next_transfer tr where tr.run_worker_id=rw.id and tr.status<>'CANCELLED'))
    -- Completed zero is different from an unallocated binding. Positive
    -- completed results require their actual typed hold disposition as well.
    and not exists(select 1 from private.bpay_next_run_case_instruction i
      join private.bpay_next_case_allocation_result ar on ar.instruction_id=i.id
      join private.bpay_next_case_allocation_state st on st.id=ar.state_id
        and st.run_worker_id=rw.id and st.status='COMPLETE' and st.completed_at_utc is not null
        and st.processed_instruction_count=st.expected_instruction_count
        and st.weekly_binding_state<>'UNBOUND'
      where i.run_worker_id=rw.id and i.case_component_id=a.case_component_id and i.is_selected
        and i.preparation_revision=rw.captured_preparation_revision
        and i.selection_revision=rw.case_selection_revision
        and st.preparation_revision=i.preparation_revision and st.selection_revision=i.selection_revision
        and ar.preparation_revision=i.preparation_revision and ar.selection_revision=i.selection_revision
        and ((i.target_pay_channel='PAYE' and i.payroll_stage='NET_DEDUCT'
            and ar.pass_kind='NET' and st.pass_kind='NET'
            and st.net_stage='COMPLETE' and st.net_issue_code is null
            and st.projection_no=rw.net_projection_revision and rw.net_projection_revision>0
            and rw.net_request_revision=rw.net_projection_revision
            and exists(select 1 from private.bpay_next_net_projection np
              join private.bpay_next_paye_net_request nr on nr.command_id=np.request_command_id
                and nr.run_worker_id=rw.id and nr.request_no=np.projection_no
                and nr.preparation_revision=i.preparation_revision and nr.selection_revision=i.selection_revision
              join private.bpay_next_job nj on nj.command_id=nr.command_id and nj.candidate_id=rw.candidate_id
                and nj.job_kind='PAYE_NET_ENTRY' and nj.status='DONE' and nj.phase='PROJECTED'
                and nj.id=st.job_id and st.command_id=nr.command_id
              where np.id=st.projection_id and np.run_worker_id=rw.id
                and np.projection_no=st.projection_no and np.retired_at_utc is null))
          or (not(i.target_pay_channel='PAYE' and i.payroll_stage='NET_DEDUCT')
            and ar.pass_kind='DRAFT' and st.pass_kind='DRAFT' and st.projection_no=0
            and st.prepare_stage='COMPLETE' and st.captured_instruction_count=st.expected_instruction_count))
        and (ar.allocated_source_ex_vat=0 or exists(select 1 from private.bpay_next_case_hold h
          join private.bpay_next_case_capacity_use cu on cu.case_hold_id=h.id
            and cu.case_component_id=h.case_component_id and cu.case_id=h.case_id and cu.candidate_id=h.candidate_id
            and cu.pay_week_start=h.pay_week_start and cu.purpose=h.purpose and cu.status='REALISED'
            and cu.source_amount_ex_vat=h.source_reserved_ex_vat
          join private.bpay_next_case_event ce on ce.id=cu.realisation_event_id
            and ce.case_id=h.case_id and ce.case_component_id=h.case_component_id and ce.candidate_id=h.candidate_id
            and ce.pay_week_start=h.pay_week_start and ce.hold_purpose=h.purpose
            and ce.source_amount_ex_vat=h.source_reserved_ex_vat and ce.event_kind in ('FUNDED','RECOVERED','PAID')
          where h.allocation_result_id=ar.id and h.instruction_id=i.id
            and h.case_component_id=a.case_component_id and h.run_worker_id=rw.id
            and h.candidate_id=rw.candidate_id and h.source_reserved_ex_vat=ar.allocated_source_ex_vat
            and h.target_amount_ex_vat=ar.allocated_target_ex_vat and h.target_amount_vat=ar.allocated_target_vat
            and h.target_amount_inc_vat=ar.allocated_target_inc_vat and h.purpose=ar.hold_purpose
            and h.status='REALISED' and h.finished_at_utc is not null)))
 ), refs(binding_kind,run_id,case_component_id,native_id,candidate_id,run_worker_id,
   channel,run_status,worker_status,actor_user_id,preparation_revision,protected_amount,next_action) as (
 select 'HOLD'::text,pr.id,h.case_component_id,h.id,h.candidate_id,rw.id,rw.target_pay_channel,
   pr.status,rw.status,pr.actor_user_id,rw.preparation_revision,h.source_reserved_ex_vat::numeric,
   case when pr.confirmed_at_utc is not null then 'CANCEL_CANDIDATE_PAYMENT'
     else 'REMOVE_CANDIDATE_OR_DISCARD' end
 from private.bpay_next_case_hold h
 join private.bpay_next_run_worker rw on rw.id=h.run_worker_id and rw.candidate_id=h.candidate_id
 join private.bpay_next_pay_run pr on pr.id=rw.run_id
 where h.case_id=p_case_id and h.status='ACTIVE'
 union all
 select 'PENDING'::text,p.run_id,p.case_component_id,p.native_id,p.candidate_id,p.worker_id,
   coalesce(rw.target_pay_channel,sc.target_pay_channel),p.run_status,p.worker_status,p.actor_user_id,
   p.preparation_revision,null::numeric,
   case when p.confirmed_at_utc is not null then 'CANCEL_CANDIDATE_PAYMENT'
     when p.worker_id is null then 'DISCARD_PREPARATION' else 'REMOVE_CANDIDATE_OR_DISCARD' end
 from pending p left join private.bpay_next_run_worker rw on rw.id=p.worker_id
 left join private.bpay_next_selection_candidate sc on sc.run_id=p.run_id and sc.candidate_id=p.candidate_id
 )
 select r.* from refs r where p_after is null
   or (r.binding_kind,r.run_id,r.native_id)>
      (p_after->>'binding_kind',(p_after->>'run_id')::uuid,(p_after->>'native_id')::uuid)
 order by r.binding_kind,r.run_id,r.native_id limit p_limit
$function$;

create or replace function private.bpay_next_case_reserved_slices_v1(
 p_case_id uuid,p_cursor jsonb default null,p_limit integer default 100)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_case private.bpay_next_finance_case%rowtype;r record;v_rows jsonb:='[]';
 v_after jsonb;v_kind text;v_run uuid;v_native uuid;v_row jsonb;
 v_next jsonb;v_count integer:=0;v_more boolean:=false;v_bytes integer:=512;
begin
 if p_case_id is null or p_limit is null or p_limit not between 1 and 100 then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_INPUT_INVALID';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id;
 if p_cursor is not null then
  if jsonb_typeof(p_cursor) is distinct from 'object' or octet_length(p_cursor::text)>1024
    or not p_cursor ?& array['case_revision','after']
    or p_cursor-array['case_revision','after']<>'{}'::jsonb
    or jsonb_typeof(p_cursor->'case_revision') is distinct from 'string'
    or p_cursor->>'case_revision'!~'^(0|[1-9][0-9]{0,18})$' then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_CURSOR_INVALID';end if;
  if (p_cursor->>'case_revision')::numeric<>v_case.case_revision then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_CHANGED';end if;
  v_after:=p_cursor->'after';
  if v_after<>'null'::jsonb then
   if jsonb_typeof(v_after) is distinct from 'object'
     or not v_after ?& array['binding_kind','run_id','native_id']
     or v_after-array['binding_kind','run_id','native_id']<>'{}'::jsonb
     or v_after->>'binding_kind' not in ('HOLD','PENDING')
     or jsonb_typeof(v_after->'binding_kind') is distinct from 'string'
     or jsonb_typeof(v_after->'run_id') is distinct from 'string'
     or jsonb_typeof(v_after->'native_id') is distinct from 'string' then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_CURSOR_INVALID';end if;
   v_kind:=v_after->>'binding_kind';v_run:=(v_after->>'run_id')::uuid;
   v_native:=(v_after->>'native_id')::uuid;
  end if;
 end if;
 for r in select b.*,u.display_name as actor_display_name from private.bpay_next_case_protected_bindings_v1(
   p_case_id,case when v_kind is null then null else v_after end,p_limit+1) b
   left join public.tms_users u on u.id=b.actor_user_id
   order by b.binding_kind,b.run_id,b.native_id loop
  if v_count=p_limit then v_more:=true;exit;end if;
  v_row:=jsonb_build_object('binding_kind',r.binding_kind,
   'run_id',r.run_id,'case_component_id',r.case_component_id,'native_id',r.native_id,
   'candidate_id',r.candidate_id,'run_worker_id',r.run_worker_id,'channel',r.channel,
   'run_status',r.run_status,'worker_status',r.worker_status,'actor_user_id',r.actor_user_id,
   'actor_display_name',r.actor_display_name,
   'preparation_revision',r.preparation_revision::text,'protected_amount',r.protected_amount::text,
   'next_action',r.next_action);
  if v_bytes+octet_length(v_row::text)+1>120000 then
   if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_PROTECTION_ROW_TOO_LARGE';end if;
   v_more:=true;exit;end if;
  v_bytes:=v_bytes+octet_length(v_row::text)+1;v_rows:=v_rows||jsonb_build_array(v_row);
  v_after:=jsonb_build_object('binding_kind',r.binding_kind,'run_id',r.run_id,
    'native_id',r.native_id);v_count:=v_count+1;
 end loop;
 if v_more then v_next:=jsonb_build_object('case_revision',v_case.case_revision::text,'after',v_after);end if;
 return jsonb_build_object('case_id',p_case_id,'case_revision',v_case.case_revision::text,
   'rows',v_rows,'next_cursor',v_next);
exception when invalid_text_representation then
 raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_CURSOR_INVALID';
end
$function$;

-- FUTURE_ONLY does not authorise rewriting any accepted rule/period. Later
-- callers append their own explicitly versioned future authority. Whole-slice
-- change is refused; the native whole-Candidate release is a separate command.
create or replace function private.bpay_next_case_change_assert_v1(
 p_case_id uuid,p_expected_case_revision bigint,p_mode text default 'FUTURE_ONLY',p_amount numeric default null,
 p_capacity_purpose text default null)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_case private.bpay_next_finance_case%rowtype;v_count bigint;v_pending boolean;
 v_protected numeric;v_capacity numeric;v_summary jsonb;v_purpose text;
begin
 if p_case_id is null or p_expected_case_revision is null or p_expected_case_revision<0
   or p_mode is null or p_mode not in ('FUTURE_ONLY','UNRESERVED_AMOUNT','WHOLE_CASE')
   or (p_mode='UNRESERVED_AMOUNT' and (p_amount is null or p_amount::text in ('NaN','Infinity','-Infinity')
      or p_amount<=0 or p_amount<>trunc(p_amount,2)))
   or (p_mode<>'UNRESERVED_AMOUNT' and p_amount is not null) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_INPUT_INVALID';end if;
 if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
  raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if p_capacity_purpose is not null and p_capacity_purpose not in ('PAYOUT','RECOVERY') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_INPUT_INVALID';end if;
 select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id;
 if v_case.case_revision<>p_expected_case_revision then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_CHANGED';end if;
 select count(*),coalesce(bool_or(b.binding_kind='PENDING' or b.protected_amount is null),false),coalesce(sum(b.protected_amount),0)
   into v_count,v_pending,v_protected from private.bpay_next_case_protected_bindings_v1(p_case_id,null,null) b;
 if (not v_pending and v_protected<>v_case.active_hold_amount) or v_protected>v_case.active_hold_amount then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID';end if;
 -- Reuse the exact purpose-specific factual balances of the existing plan.
 -- Mixed funding/recovery Case authority is not a generic payout capacity.
 v_purpose:=coalesce(p_capacity_purpose,case when v_case.case_kind='CREDIT' then 'PAYOUT'
   when v_case.case_kind in ('OVERPAYMENT','MANUAL_DEBT') then 'RECOVERY' end);
 if (v_purpose='PAYOUT' and v_case.case_kind not in ('LOAN','ADVANCE','CREDIT'))
   or (v_purpose='RECOVERY' and v_case.case_kind='CREDIT') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_PURPOSE_INVALID';end if;
 if p_mode='UNRESERVED_AMOUNT' and v_purpose is null then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PROTECTION_PURPOSE_REQUIRED';end if;
 if v_purpose='PAYOUT' then
  v_capacity:=v_case.principal_approved-v_case.principal_funded-v_case.principal_paid_credit
    -v_case.principal_written_off-v_case.active_payout_hold_amount;
 elsif v_purpose='RECOVERY' then
  v_capacity:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
    else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off
    -v_case.principal_paid_credit-v_case.active_recovery_hold_amount;
 end if;
 if v_capacity is not null and v_capacity<0 then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID';end if;
 if v_pending then v_protected:=null;v_capacity:=null;end if;
 v_summary:=jsonb_build_object('unreserved_capacity',v_capacity::text,'protected_count',v_count::text,
   'protected_amount',v_protected::text,'next_cursor',case when v_count>0 then
     jsonb_build_object('case_revision',v_case.case_revision::text,'after',null) else null end);
 if octet_length(v_summary::text)>120000 then
  raise exception using errcode='54000',message='BPAY_NEXT_CASE_PROTECTION_ROW_TOO_LARGE';end if;
 if (p_mode='WHOLE_CASE' and v_count>0) or (p_mode='UNRESERVED_AMOUNT' and (v_pending or p_amount>v_capacity)) then
  raise exception using errcode='23514',
   message=case when v_pending then 'BPAY_NEXT_CASE_PROTECTION_PENDING' else 'BPAY_NEXT_CASE_PROTECTED_SLICE' end,
   detail=v_summary::text;end if;
 return v_summary;
end
$function$;

create or replace function private.bpay_next_apply_write_off_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_write_off_request%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_basis private.bpay_next_work_collection%rowtype;
  v_remaining numeric;v_amount numeric;v_phase text;v_issue text;v_receipt jsonb;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_WRITE_OFF_APPLY_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select r.* into strict v_request from private.bpay_next_write_off_request r where r.command_id=v_job.command_id for update;
  if v_job.job_kind<>'CASE_WRITE_OFF' or v_job.module_epoch<>v_epoch or v_request.candidate_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_JOB_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_job.cursor_key is null then
    if v_request.status='REQUESTED' or not exists(select 1 from private.bpay_next_command c where c.id=v_job.command_id
      and c.command_kind='CASE_WRITE_OFF' and c.module_epoch=v_epoch
      and c.status=case when v_request.status='WRITTEN_OFF' then 'COMPLETE' else 'FAILED' end)
      or (v_request.status='WRITTEN_OFF' and not exists(select 1 from private.bpay_next_case_event e
        where e.id=v_request.event_id and e.case_id=v_request.case_id and e.operation_id=v_request.command_id
          and e.operation_item_id=v_request.work_collection_id and e.event_kind='WRITTEN_OFF' and e.written_off_delta=v_request.applied_amount)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_TERMINAL_EVIDENCE_INVALID';end if;
    v_receipt:=private.bpay_next_write_off_receipt_v1(v_job.command_id,true);
    return v_receipt-array['command_id','sequence','requested_scope'];
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.cursor_key is not null
     or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or not private.bpay_next_write_off_owner_v1(v_job.command_id,v_candidate)
     or v_request.status<>'REQUESTED' then
    raise exception using errcode='55000',message='BPAY_NEXT_WRITE_OFF_LEASE_STALE';end if;
  select b.* into strict v_basis from private.bpay_next_work_collection b where b.id=v_request.work_collection_id;
  perform 1 from private.bpay_next_position where work_id=v_basis.work_id and component_key=v_basis.component_key for update;
  select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_request.case_id for update;
  select x.* into strict v_component from private.bpay_next_case_component x where x.id=v_request.case_component_id for update;
  v_basis:=private.bpay_next_work_collection_origin_v1(v_case.id,v_component.id,v_candidate);
  v_remaining:=v_case.principal_approved-v_case.principal_recovered-v_case.principal_written_off;
  if (v_case.principal_approved,v_case.principal_recovered,v_case.principal_written_off,v_case.active_recovery_hold_amount)
     is distinct from (v_component.approved_source_ex_vat,v_component.recovered_source_ex_vat,
       v_component.written_off_source_ex_vat,v_component.active_recovery_source_ex_vat)
     or v_remaining<v_case.active_recovery_hold_amount then
    raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_BALANCE_INVALID';end if;
  v_phase:='WRITTEN_OFF';
  if v_component.component_revision<>v_request.expected_component_revision or v_case.case_revision<>v_request.basis_case_revision then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_COMPONENT_CHANGED';
  elsif v_basis.id is null or v_case.status not in ('OPEN','PAUSED') then
    v_phase:='REVIEW';v_issue:='BPAY_NEXT_WRITE_OFF_ORIGIN_UNBOUND';
  elsif v_remaining=0 then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_NO_BALANCE';
  else
    v_amount:=case when v_request.requested_scope='ALL' then v_remaining else v_request.requested_amount end;
    if v_amount>v_remaining then v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_AMOUNT_EXCEEDS_BALANCE';
    elsif v_amount>v_remaining-v_case.active_recovery_hold_amount then
      v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY';end if;
  end if;
  if v_phase='WRITTEN_OFF' then
    -- FC-16: fresh post-component-lock reproof; never acquire a run header
    -- here or release a Candidate recursively from this money owner.
    begin
      perform private.bpay_next_case_change_assert_v1(v_case.id,v_case.case_revision,
        case when v_request.requested_scope='ALL' then 'WHOLE_CASE' else 'UNRESERVED_AMOUNT' end,
        case when v_request.requested_scope='ALL' then null else v_amount end,'RECOVERY');
    exception when check_violation then
      if sqlerrm='BPAY_NEXT_CASE_PROTECTION_PENDING' then
        v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_INPUT_PENDING';
      elsif sqlerrm='BPAY_NEXT_CASE_PROTECTED_SLICE' then
        v_phase:='BLOCKED';v_issue:='BPAY_NEXT_WRITE_OFF_PROTECTED_CAPACITY';
      else raise;end if;
    end;
  end if;
  if v_phase='WRITTEN_OFF' then
    insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,written_off_delta)
      values(v_request.command_id,v_case.id,v_request.command_id,v_request.work_collection_id,'WRITTEN_OFF',v_amount);
    update private.bpay_next_case_component set written_off_source_ex_vat=written_off_source_ex_vat+v_amount,
      component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
    update private.bpay_next_finance_case set principal_written_off=principal_written_off+v_amount,case_revision=case_revision+1,
      status=case when v_remaining-v_amount=0 then 'WRITTEN_OFF' when status='PAUSED' then 'PAUSED' else 'OPEN' end,
      updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
    v_remaining:=v_remaining-v_amount;
    update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
      updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  else v_amount:=0;end if;
  update private.bpay_next_write_off_request set status=v_phase,applied_amount=v_amount,remaining_amount=v_remaining,
    protected_amount=v_case.active_recovery_hold_amount,event_id=case when v_phase='WRITTEN_OFF' then command_id else null end,
    issue_code=v_issue,completed_at_utc=pg_catalog.transaction_timestamp() where command_id=v_request.command_id;
  update private.bpay_next_job set status='DONE',phase='DONE',lease_nonce=null,lease_until_utc=null where id=p_job_id;
  update private.bpay_next_command set status=case when v_phase='WRITTEN_OFF' then 'COMPLETE' else 'FAILED' end where id=v_job.command_id;
  v_receipt:=private.bpay_next_write_off_receipt_v1(v_job.command_id,false);
  return v_receipt-array['command_id','sequence','requested_scope'];
end
$function$;

alter function private.bpay_next_apply_write_off_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_apply_write_off_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;

alter function private.bpay_next_case_protected_bindings_v1(uuid,jsonb,integer) owner to postgres;
alter function private.bpay_next_case_reserved_slices_v1(uuid,jsonb,integer) owner to postgres;
alter function private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text) owner to postgres;
revoke all on function private.bpay_next_case_protected_bindings_v1(uuid,jsonb,integer),
 private.bpay_next_case_reserved_slices_v1(uuid,jsonb,integer),
 private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text)
from public,anon,authenticated,service_role;

commit;
