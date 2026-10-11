-- FC09 / BD119: exact ordered preactivity amendment. Complete current successors only.
\set ON_ERROR_STOP on
begin;

-- FC09 exact immutable amendment owners; no money formula or funding successor.
create or replace function private.bpay_next_case_edit_origin_v1(p_rule_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 r private.bpay_next_case_rule%rowtype;approved private.bpay_next_case_rule%rowtype;
 q private.bpay_next_case_edit_request%rowtype;original private.bpay_next_case_create_request%rowtype;
 e private.bpay_next_case_event%rowtype;i private.bpay_next_run_case_instruction%rowtype;
begin
 select case_point.* into strict r from private.bpay_next_case_rule case_point where id=p_rule_id;approved:=r;
 if r.original_funding_event_id is not null then
  select case_point.* into strict e from private.bpay_next_case_event case_point where id=r.original_funding_event_id;
  select case_point.* into strict i from private.bpay_next_run_case_instruction case_point where id=e.instruction_id;
  select case_point.* into strict approved from private.bpay_next_case_rule case_point where id=i.rule_id;
  if (e.case_id,e.candidate_id,e.event_kind,e.occurred_at_utc,i.case_id,i.candidate_id,i.instruction_kind,
       i.rule_revision,i.case_kind,i.case_subtype,i.tax_treatment)
   is distinct from (r.case_id,r.candidate_id,'FUNDED'::text,r.original_funded_at_utc,r.case_id,r.candidate_id,
       'PAYOUT'::text,approved.rule_revision,r.case_kind,r.case_subtype,r.tax_treatment)
   or (r.case_id,r.candidate_id,r.case_kind,r.case_subtype,r.tax_treatment,r.case_created_at_utc,
       r.minimum_earnings_threshold,r.take_home_floor_override,r.weekly_due_source_ex_vat,
       r.schedule_start_monday,r.next_due_monday,r.schedule_week_count)
    is distinct from (approved.case_id,approved.candidate_id,approved.case_kind,approved.case_subtype,
       approved.tax_treatment,approved.case_created_at_utc,approved.minimum_earnings_threshold,
       approved.take_home_floor_override,approved.weekly_due_source_ex_vat,approved.schedule_start_monday,
       approved.next_due_monday,approved.schedule_week_count)
   or approved.original_funding_event_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
 end if;
 select case_point.* into q from private.bpay_next_case_edit_request case_point where new_rule_id=approved.id;
 if not found then return null;end if;
 select case_point.* into strict original from private.bpay_next_case_create_request case_point where command_id=q.original_command_id;
 if (q.case_id,q.candidate_id,q.new_rule_revision,q.input->>'case_kind',q.input->>'case_subtype',q.input->>'tax_treatment',
     (q.terms->>'weekly_due')::numeric,(q.terms->>'week_count')::bigint,(q.input->>'start_monday')::date,
     (q.input->>'minimum_earnings_threshold')::numeric,(q.input->>'take_home_floor_override')::numeric)
  is distinct from (approved.case_id,approved.candidate_id,approved.rule_revision,approved.case_kind,approved.case_subtype,
     approved.tax_treatment,approved.weekly_due_source_ex_vat,approved.schedule_week_count,approved.schedule_start_monday,
     approved.minimum_earnings_threshold,approved.take_home_floor_override)
  or (original.case_id,original.candidate_id,original.case_kind,original.case_subtype,original.accepted_at_utc)
   is distinct from (approved.case_id,approved.candidate_id,approved.case_kind,approved.case_subtype,approved.case_created_at_utc)
  or approved.next_due_monday is distinct from approved.schedule_start_monday
  or not exists(select 1 from private.bpay_next_case_event event where event.id=q.command_id
     and event.case_id=q.case_id and event.operation_id=q.command_id and event.operation_item_id=original.primary_component_id
     and event.event_kind='APPROVED' and event.approved_delta=q.principal-q.previous_principal
     and event.occurred_at_utc=q.accepted_at_utc)
  or (r.original_funding_event_id is not null and i.case_component_id is distinct from original.primary_component_id) then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
 return to_jsonb(q);
end
$function$;

create or replace function private.bpay_next_case_edit_state_v1(p_case_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
 c private.bpay_next_finance_case%rowtype;r private.bpay_next_case_rule%rowtype;
 o private.bpay_next_case_create_request%rowtype;amendment jsonb;input jsonb;binding record;
 code text;ids uuid[];protection jsonb:=null;
begin
 select case_point.* into strict c from private.bpay_next_finance_case case_point where id=p_case_id and candidate_id=p_candidate_id;
 select case_point.* into strict r from private.bpay_next_case_rule case_point where id=c.current_rule_id and case_id=c.id and candidate_id=c.candidate_id;
 select case_point.* into o from private.bpay_next_case_create_request case_point where case_id=c.id and candidate_id=c.candidate_id;
 select array_agg(id order by component_ordinal) into ids from
  (select id,component_ordinal from private.bpay_next_case_component where case_id=c.id order by component_ordinal limit 3) bounded;
 if o.command_id is null or c.case_kind not in ('LOAN','ADVANCE','CREDIT','MANUAL_DEBT')
  or ids is distinct from (case when o.recovery_component_id is null then array[o.primary_component_id]
    else array[o.primary_component_id,o.recovery_component_id] end) then code:='BPAY_NEXT_CASE_EDIT_UNSUPPORTED';
 else
  amendment:=private.bpay_next_case_edit_origin_v1(r.id);
  input:=case when amendment is not null then amendment->'input' else jsonb_build_object(
   'candidate_id',o.candidate_id::text,'case_kind',o.case_kind,'case_subtype',o.case_subtype,'tax_treatment',o.tax_treatment,
   'principal',o.principal_source_ex_vat::text,'due_date',o.due_date::text,'start_monday',o.input_start_monday::text,
   'weekly_due',o.input_weekly_due::text,'week_count',o.input_week_count::text,
   'minimum_earnings_threshold',o.minimum_earnings_threshold::text,'take_home_floor_override',o.take_home_floor_override::text,
   'reason',o.approval_reason,'note',o.note,'original_liability_reference',o.original_liability_reference) end;
  if c.status<>'OPEN' or r.original_funding_event_id is not null
   or (c.principal_funded,c.principal_recovered,c.principal_written_off,c.principal_paid_credit)
    is distinct from (0::numeric,0::numeric,0::numeric,0::numeric)
   or exists(select 1 from private.bpay_next_case_event where case_id=c.id and event_kind<>'APPROVED')
   or exists(select 1 from private.bpay_next_case_component x where x.case_id=c.id and
     (x.funded_source_ex_vat,x.recovered_source_ex_vat,x.paid_credit_source_ex_vat,x.written_off_source_ex_vat)
      is distinct from (0::numeric,0::numeric,0::numeric,0::numeric))
   or exists(select 1 from private.bpay_next_case_period p where p.case_component_id=any(ids)
     and (p.realised_recovery_source_ex_vat<>0 or p.active_unrealised_recovery_source_ex_vat<>0 or p.active_payout_source_ex_vat<>0))
   or exists(select 1 from private.bpay_next_case_hold h join private.bpay_next_transfer_member tm on tm.case_hold_id=h.id
     join private.bpay_next_transfer t on t.id=tm.transfer_id where h.case_id=c.id and t.status<>'CANCELLED') then code:='BPAY_NEXT_CASE_HAS_ACTIVITY';end if;
 end if;
 select * into binding from private.bpay_next_case_protected_bindings_v1(c.id,null,1);
 if found then
  protection:=jsonb_build_object('run_id',binding.run_id,'run_worker_id',binding.run_worker_id,
   'channel',binding.channel,'run_status',binding.run_status,'worker_status',binding.worker_status,
   'protected_amount',binding.protected_amount::text,'next_action',binding.next_action);
  code:='BPAY_NEXT_CASE_HELD_BY_DRAFT';
 elsif c.active_hold_amount<>0 or exists(select 1 from private.bpay_next_case_hold where case_id=c.id and status='ACTIVE')
  or exists(select 1 from private.bpay_next_case_component x where x.case_id=c.id and
   (x.active_payout_source_ex_vat,x.active_recovery_source_ex_vat) is distinct from (0::numeric,0::numeric)) then
  code:=coalesce(code,'BPAY_NEXT_CASE_HAS_ACTIVITY');
 end if;
 return jsonb_build_object('case_id',c.id,'candidate_id',c.candidate_id,'case_revision',c.case_revision::text,
  'rule_id',r.id,'rule_revision',r.rule_revision::text,'status',c.status,'editable',code is null,
  'issue_code',code,'input',input,'protection',protection,
  'applied_edit_command_id',case when amendment is null then null else amendment->>'command_id' end);
end
$function$;

create or replace function private.bpay_next_case_edit_assert_v1(p_case_id uuid,p_candidate_id uuid,p_expected_revision bigint)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare state jsonb;
begin
 state:=private.bpay_next_case_edit_state_v1(p_case_id,p_candidate_id);
 if p_expected_revision is null or p_expected_revision<1 or (state->>'case_revision')::bigint<>p_expected_revision then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_CHANGED';end if;
 -- The FC16 native assertion is always called under component UPDATE locks.
 begin perform private.bpay_next_case_change_assert_v1(p_case_id,p_expected_revision,'WHOLE_CASE',null,null);
 exception when check_violation then
  if sqlerrm in ('BPAY_NEXT_CASE_PROTECTION_PENDING','BPAY_NEXT_CASE_PROTECTED_SLICE') then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_HELD_BY_DRAFT',detail=(state->'protection')::text;
  else raise;end if;
 end;
 if state->>'issue_code' is not null then
  raise exception using errcode='23514',message=state->>'issue_code',detail=(state->'protection')::text;end if;
 return state;
end
$function$;

create or replace function private.bpay_next_case_edit_receipt_v1(p_command_id uuid,p_case_id uuid,p_candidate_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare q private.bpay_next_case_edit_request%rowtype;j private.bpay_next_job%rowtype;phase text;
begin
 if p_command_id is null then return null;end if;
 select case_point.* into strict q from private.bpay_next_case_edit_request case_point where command_id=p_command_id and case_id=p_case_id and candidate_id=p_candidate_id;
 select case_point.* into j from private.bpay_next_job case_point where command_id=p_command_id and candidate_id=p_candidate_id and job_kind='CASE_EDIT';
 phase:=case when j.status='DONE' and j.phase='DONE' then case when j.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end else 'PENDING' end;
 if phase='CASE_EDITED' and private.bpay_next_case_edit_origin_v1(q.new_rule_id) is null then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_TERMINAL_EVIDENCE_MISSING';end if;
 return jsonb_build_object('command_id',q.command_id,'case_id',q.case_id,'candidate_id',q.candidate_id,
  'case_revision',(q.expected_case_revision+1)::text,'rule_id',q.new_rule_id,'phase',phase,'issue_code',j.last_error_code);
end
$function$;

create or replace function private.bpay_next_case_edit_preview_v1(p_actor_user_id uuid,p_case_id uuid,p_expected_revision bigint,p_input jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare result jsonb;state jsonb;context jsonb;
begin
 result:=private.bpay_next_case_create_preview_v1(p_actor_user_id,p_input);
 state:=private.bpay_next_case_edit_state_v1(p_case_id,(p_input->>'candidate_id')::uuid);
 if (state->>'case_revision')::bigint is distinct from p_expected_revision then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_CHANGED';end if;
 if state->>'issue_code' is not null then
  raise exception using errcode='23514',message=state->>'issue_code',detail=(state->'protection')::text;end if;
 if (p_input->>'case_kind',p_input->>'case_subtype') is distinct from
  (state->'input'->>'case_kind',state->'input'->>'case_subtype') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_IDENTITY_INVALID';end if;
 context:=jsonb_build_object('actor_user_id',result->'preview_token'->'context'->'actor_user_id',
   'owner_epoch',result->'preview_token'->'context'->'owner_epoch',
   'financial_view_revision',result->'preview_token'->'context'->'financial_view_revision',
   'pay_channel',result->'preview_token'->'context'->'pay_channel',
   'candidate_default_floor',result->'preview_token'->'context'->'candidate_default_floor',
   'valuation_policy_id',result->'preview_token'->'context'->'valuation_policy_id','case_id',p_case_id,
   'case_revision',p_expected_revision::text,'rule_id',state->'rule_id','rule_revision',state->'rule_revision');
 return jsonb_set(result,'{preview_token,context}',context);
end
$function$;

create or replace function private.bpay_next_accept_case_edit_v1(p_actor_user_id uuid,p_args jsonb)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
#variable_conflict use_variable
declare command_id uuid;candidate_id uuid;case_id uuid;expected bigint;input jsonb;terms jsonb;token jsonb;
 prior private.bpay_next_case_edit_request%rowtype;c private.bpay_next_finance_case%rowtype;
 o private.bpay_next_case_create_request%rowtype;epoch bigint;sequence bigint;state jsonb;
begin
 if jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>49152
  or not(p_args ?& array['command_id','case_id','expected_case_revision','preview_token'])
  or jsonb_typeof(p_args->'command_id') is distinct from 'string'
  or jsonb_typeof(p_args->'case_id') is distinct from 'string'
  or p_args->>'command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
  or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
  or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
 command_id:=(p_args->>'command_id')::uuid;case_id:=(p_args->>'case_id')::uuid;
 expected:=(p_args->>'expected_case_revision')::bigint;
 input:=p_args-array['command_id','case_id','expected_case_revision','preview_token'];
 terms:=private.bpay_next_case_create_terms_v1(input);candidate_id:=(input->>'candidate_id')::uuid;token:=p_args->'preview_token';
 if token->'input' is distinct from input then raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
 select owner_epoch into epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 if epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin' for share) then
  raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';end if;
 perform 1 from public.candidates c where c.id=candidate_id for share;
 insert into private.bpay_next_worker_control(candidate_id) values(candidate_id) on conflict on constraint bpay_next_worker_control_pkey do nothing;
 perform 1 from private.bpay_next_worker_control w where w.candidate_id=candidate_id for update;
 perform pg_advisory_xact_lock(hashtextextended('bpay-next-case-edit:'||command_id::text,0));
 select q.* into prior from private.bpay_next_case_edit_request q where q.command_id=command_id;
 if found then
  if (prior.candidate_id,prior.case_id,prior.actor_user_id,prior.expected_case_revision,prior.input,prior.preview_token)
    is distinct from (candidate_id,case_id,p_actor_user_id,expected,input,token) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_REPLAY_CONFLICT';end if;
  select agency_sequence into strict sequence from private.bpay_next_command c where c.id=command_id
    and c.command_kind='CASE_EDIT' and c.module_epoch=epoch;
  return jsonb_build_object('case_id',prior.case_id,'command_id',command_id,'sequence',sequence::text,
    'case_revision',(prior.expected_case_revision+1)::text,'rule_id',prior.new_rule_id,'phase','ACCEPTED_PENDING_EDIT','replay',true);
 end if;
 select c2.* into strict c from private.bpay_next_finance_case c2 where c2.id=case_id and c2.candidate_id=candidate_id for update;
 perform 1 from private.bpay_next_case_component x where x.case_id=c.id order by x.component_ordinal for update;
 state:=private.bpay_next_case_edit_assert_v1(c.id,candidate_id,expected);
 if (private.bpay_next_case_edit_preview_v1(p_actor_user_id,c.id,expected,input)->'preview_token') is distinct from token then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREVIEW_CHANGED';end if;
 select q.* into strict o from private.bpay_next_case_create_request q where q.case_id=c.id and q.candidate_id=candidate_id;
 sequence:=private.bpay_next_receive_command_v1(command_id,'CASE_EDIT');
 insert into private.bpay_next_command_member(command_id,candidate_id,member_no) values(command_id,candidate_id,1);
 insert into private.bpay_next_case_edit_request(command_id,candidate_id,actor_user_id,case_id,original_command_id,
  expected_case_revision,previous_rule_id,previous_rule_revision,new_rule_id,new_rule_revision,previous_principal,principal,
  input,terms,preview_token,accepted_at_utc)
 values(command_id,candidate_id,p_actor_user_id,c.id,o.command_id,expected,c.current_rule_id,c.current_rule_revision,
  command_id,c.current_rule_revision+1,c.principal_approved,(input->>'principal')::numeric,input,terms,token,transaction_timestamp());
 update private.bpay_next_command cmd set expected_member_count=1,status='SEALED',sealed_at_utc=transaction_timestamp()
  where cmd.id=command_id and cmd.status='RECEIVED';
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_COMMAND_INVALID';end if;
 return jsonb_build_object('case_id',c.id,'command_id',command_id,'sequence',sequence::text,
  'case_revision',(expected+1)::text,'rule_id',command_id,'phase','ACCEPTED_PENDING_EDIT','replay',false);
end
$function$;

create or replace function private.bpay_next_apply_case_period_amendment_v1(p_edit_id uuid,p_before jsonb,p_after jsonb)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_old private.bpay_next_case_period%rowtype;v_new private.bpay_next_case_period%rowtype;v_actual private.bpay_next_case_period%rowtype;
 v_old_rule private.bpay_next_case_rule%rowtype;v_new_rule private.bpay_next_case_rule%rowtype;
 v_edit jsonb;v_old_edit jsonb;v_origin private.bpay_next_case_create_request%rowtype;
begin
 v_old:=jsonb_populate_record(null::private.bpay_next_case_period,p_before);
 v_new:=jsonb_populate_record(null::private.bpay_next_case_period,p_after);
 select case_point.* into strict v_actual from private.bpay_next_case_period case_point
  where case_component_id=v_old.case_component_id and pay_week_start=v_old.pay_week_start for update;
 if to_jsonb(v_actual) is not distinct from p_after and exists(select 1 from private.bpay_next_case_period_amendment a
  where a.case_component_id=v_new.case_component_id and a.pay_week_start=v_new.pay_week_start
   and a.period_revision=v_new.period_revision and a.edit_command_id=p_edit_id and a.rule_id=v_new.rule_id
   and a.previous_rule_id=v_old.rule_id and a.previous_period_revision=v_old.period_revision
   and (a.previous_opening_outstanding,a.previous_opening_due,a.opening_outstanding,a.opening_due)
    is not distinct from (v_old.opening_outstanding_source_ex_vat,v_old.opening_due_source_ex_vat,
      v_new.opening_outstanding_source_ex_vat,v_new.opening_due_source_ex_vat)) then return;end if;
 select case_point.* into strict v_old_rule from private.bpay_next_case_rule case_point where id=v_old.rule_id;
 select case_point.* into strict v_new_rule from private.bpay_next_case_rule case_point where id=v_new.rule_id;
 v_edit:=private.bpay_next_case_edit_origin_v1(v_new.rule_id);
 select case_point.* into strict v_origin from private.bpay_next_case_create_request case_point where case_id=v_old.case_id and candidate_id=v_old.candidate_id;
 if v_old_rule.id<>v_origin.rule_id then v_old_edit:=private.bpay_next_case_edit_origin_v1(v_old_rule.id);end if;
 if v_edit is null or (v_edit->>'command_id')::uuid is distinct from p_edit_id
  or (v_old_rule.id<>v_origin.rule_id and v_old_edit is null)
  or v_old_rule.rule_revision>=v_new_rule.rule_revision
  or v_old_rule.original_funding_event_id is not null
  or (v_old.case_component_id,v_old.case_id,v_old.candidate_id,v_old.pay_week_start,v_old.work_collection_id)
    is distinct from (v_new.case_component_id,v_new.case_id,v_new.candidate_id,v_new.pay_week_start,null::uuid)
  or v_old.work_collection_id is not null
  or v_old.case_component_id not in (v_origin.primary_component_id,coalesce(v_origin.recovery_component_id,v_origin.primary_component_id))
  or (v_old_rule.case_id,v_old_rule.candidate_id,v_new_rule.case_id,v_new_rule.candidate_id,v_new_rule.case_created_at_utc)
    is distinct from (v_old.case_id,v_old.candidate_id,v_old.case_id,v_old.candidate_id,v_origin.accepted_at_utc)
  or (v_old.realised_recovery_source_ex_vat,v_old.active_unrealised_recovery_source_ex_vat,v_old.active_payout_source_ex_vat,
      v_new.realised_recovery_source_ex_vat,v_new.active_unrealised_recovery_source_ex_vat,v_new.active_payout_source_ex_vat)
   is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric,0::numeric)
  or v_new.period_revision<>v_old.period_revision+1 or to_jsonb(v_actual) is distinct from p_before then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
 insert into private.bpay_next_case_period_amendment(edit_command_id,case_component_id,case_id,candidate_id,pay_week_start,
  previous_period_revision,period_revision,previous_rule_id,rule_id,previous_opening_outstanding,previous_opening_due,opening_outstanding,opening_due)
 values(p_edit_id,v_old.case_component_id,v_old.case_id,v_old.candidate_id,v_old.pay_week_start,v_old.period_revision,v_new.period_revision,
  v_old.rule_id,v_new.rule_id,v_old.opening_outstanding_source_ex_vat,v_old.opening_due_source_ex_vat,
  v_new.opening_outstanding_source_ex_vat,v_new.opening_due_source_ex_vat);
 update private.bpay_next_case_period set rule_id=v_new.rule_id,period_revision=v_new.period_revision,
  opening_outstanding_source_ex_vat=v_new.opening_outstanding_source_ex_vat,opening_due_source_ex_vat=v_new.opening_due_source_ex_vat
  where case_component_id=v_old.case_component_id and pay_week_start=v_old.pay_week_start and period_revision=v_old.period_revision;
 if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
end
$function$;

create or replace function private.bpay_next_apply_case_edit_v1(p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
 v_epoch bigint;v_candidate uuid;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
 v_q private.bpay_next_case_edit_request%rowtype;v_case private.bpay_next_finance_case%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_origin private.bpay_next_case_create_request%rowtype;
 v_component private.bpay_next_case_component%rowtype;v_period private.bpay_next_case_period%rowtype;
 v_new_period private.bpay_next_case_period%rowtype;v_code text;v_week date;v_input jsonb;
begin
 select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 select candidate_id into strict v_candidate from private.bpay_next_job where id=p_job_id;
 select case_point.* into strict v_control from private.bpay_next_worker_control case_point where candidate_id=v_candidate for update;
 select case_point.* into strict v_job from private.bpay_next_job case_point where id=p_job_id for update;
 select case_point.* into strict v_q from private.bpay_next_case_edit_request case_point where command_id=v_job.command_id and candidate_id=v_candidate;
 if v_job.job_kind<>'CASE_EDIT' or v_job.module_epoch<>v_epoch then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_JOB_SCOPE_INVALID';end if;
 if v_job.status='DONE' and v_job.phase='DONE' then
  if v_job.last_error_code is null and private.bpay_next_case_edit_origin_v1(v_q.new_rule_id) is null then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_TERMINAL_EVIDENCE_MISSING';end if;
  return jsonb_build_object('phase',case when v_job.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end,
   'case_id',v_q.case_id,'case_revision',(v_q.expected_case_revision+1)::text,'rule_id',v_q.new_rule_id,
   'issue_code',v_job.last_error_code,'replay',true);
 end if;
 if v_job.status<>'LEASED' or v_job.phase<>'NEW' or v_job.cursor_key is not null
  or v_job.lease_nonce is distinct from p_lease_nonce or v_job.owner_epoch is distinct from p_owner_epoch
  or v_control.active_owner_epoch is distinct from p_owner_epoch or v_job.lease_until_utc<=clock_timestamp() then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_EDIT_LEASE_STALE';end if;
 -- Known lifecycle refusal is a terminal typed outcome. No partial amendment escapes this block.
 begin
  select case_point.* into strict v_case from private.bpay_next_finance_case case_point where id=v_q.case_id and candidate_id=v_candidate for update;
  perform 1 from private.bpay_next_case_component where case_id=v_case.id order by component_ordinal for update;
  perform private.bpay_next_case_edit_assert_v1(v_case.id,v_candidate,v_q.expected_case_revision);
  if (v_case.current_rule_id,v_case.current_rule_revision,v_case.principal_approved)
    is distinct from (v_q.previous_rule_id,v_q.previous_rule_revision,v_q.previous_principal) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_PROTECTION_CHANGED';end if;
  select case_point.* into strict v_rule from private.bpay_next_case_rule case_point where id=v_q.previous_rule_id;
  select case_point.* into strict v_origin from private.bpay_next_case_create_request case_point where command_id=v_q.original_command_id;
  v_input:=v_q.input;
  insert into private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,case_created_at_utc,
   minimum_earnings_threshold,take_home_floor_override,weekly_due_source_ex_vat,schedule_start_monday,next_due_monday,schedule_week_count)
  values(v_q.new_rule_id,v_case.id,v_candidate,v_q.new_rule_revision,v_rule.case_kind,v_rule.case_subtype,v_input->>'tax_treatment',
   v_rule.case_created_at_utc,(v_input->>'minimum_earnings_threshold')::numeric,(v_input->>'take_home_floor_override')::numeric,
   (v_q.terms->>'weekly_due')::numeric,(v_input->>'start_monday')::date,(v_input->>'start_monday')::date,(v_q.terms->>'week_count')::bigint);
  insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,approved_delta,occurred_at_utc)
   values(v_q.command_id,v_case.id,v_q.command_id,v_origin.primary_component_id,'APPROVED',v_q.principal-v_q.previous_principal,v_q.accepted_at_utc);
  update private.bpay_next_finance_case set principal_approved=v_q.principal,tax_treatment=v_input->>'tax_treatment',
   current_rule_id=v_q.new_rule_id,current_rule_revision=v_q.new_rule_revision,due_date=(v_input->>'due_date')::date,
   case_revision=case_revision+1,updated_at_utc=transaction_timestamp() where id=v_case.id;
  update private.bpay_next_case_component set approved_source_ex_vat=v_q.principal,rule_id=v_q.new_rule_id,
   tax_treatment=v_input->>'tax_treatment',payroll_stage=case when instruction_kind='PAYOUT' then 'NET_ADD'
    when instruction_kind='CREDIT' then case when v_input->>'tax_treatment'='TAXABLE' then 'GROSS_ADD' else 'NET_ADD' end
    else case when v_input->>'tax_treatment'='TAXABLE' then 'GROSS_DEDUCT' else 'NET_DEDUCT' end end,
   component_revision=component_revision+1,resolution_state='REVIEW',target_pay_channel=null,target_ex_vat=null,target_vat=null,
   target_inc_vat=null,valuation_policy_id=null,valuation_window_id=null,resolution_ref=null,updated_at_utc=transaction_timestamp()
   where case_id=v_case.id;
  -- At most two native creation anchors; no period-history enumeration.
  for v_component in select * from private.bpay_next_case_component where case_id=v_case.id order by component_ordinal loop
   v_week:=case when v_component.id=v_origin.recovery_component_id or v_origin.case_kind='MANUAL_DEBT'
     then v_origin.input_start_monday else v_origin.opening_pay_week_start end;
   select case_point.* into strict v_period from private.bpay_next_case_period case_point
    where case_component_id=v_component.id and pay_week_start=v_week for update;
   v_new_period:=v_period;v_new_period.rule_id:=v_q.new_rule_id;v_new_period.period_revision:=v_period.period_revision+1;
   v_new_period.opening_outstanding_source_ex_vat:=case when v_component.instruction_kind='RECOVERY'
     and v_origin.case_kind in ('LOAN','ADVANCE') then 0 else v_q.principal end;
   v_new_period.opening_due_source_ex_vat:=case when v_origin.case_kind='MANUAL_DEBT'
      and v_week>=(v_input->>'start_monday')::date then least((v_q.terms->>'weekly_due')::numeric,v_q.principal) else 0 end;
   perform private.bpay_next_apply_case_period_amendment_v1(v_q.command_id,to_jsonb(v_period),to_jsonb(v_new_period));
  end loop;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=transaction_timestamp()
   where candidate_id=v_candidate;
  perform private.bpay_next_workbench_refresh_case_v1(v_case.id);
 exception when check_violation or sqlstate '55000' then
  if sqlerrm not in ('BPAY_NEXT_CASE_HAS_ACTIVITY','BPAY_NEXT_CASE_HELD_BY_DRAFT','BPAY_NEXT_CASE_PROTECTION_CHANGED',
   'BPAY_NEXT_CASE_EDIT_UNSUPPORTED','BPAY_NEXT_CASE_PROTECTION_BALANCE_INVALID') then raise;end if;
  v_code:=sqlerrm;
 end;
 update private.bpay_next_job set status='DONE',phase='DONE',last_error_code=v_code,lease_nonce=null,lease_until_utc=null where id=p_job_id;
 update private.bpay_next_command set status=case when v_code is null then 'COMPLETE' else 'FAILED' end where id=v_job.command_id;
 return jsonb_build_object('phase',case when v_code is null then 'CASE_EDITED' else 'BLOCKED' end,
  'case_id',v_q.case_id,'case_revision',(v_q.expected_case_revision+1)::text,'rule_id',v_q.new_rule_id,'issue_code',v_code,'replay',false);
end
$function$;


create or replace function private.bpay_next_claim_case_edit_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare v_epoch bigint;v_candidate uuid;v_job private.bpay_next_job%rowtype;v_owner bigint;v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.candidate_id into strict v_candidate from private.bpay_next_job j where j.id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'CASE_EDIT'
     or v_job.status not in ('READY','LEASED') or v_job.phase<>'NEW'
     or (v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job e where e.candidate_id=v_candidate
       and e.command_sequence<v_job.command_sequence and e.status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_EDIT_JOB_NOT_CLAIMABLE';
  end if;
  if not exists(select 1 from private.bpay_next_case_edit_request r where r.command_id=v_job.command_id
      and r.candidate_id=v_candidate) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_REQUEST_MISSING';
  end if;
  update private.bpay_next_worker_control set active_owner_epoch=active_owner_epoch+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate returning active_owner_epoch into v_owner;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job set status='LEASED',owner_epoch=v_owner,lease_nonce=v_nonce,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>p_lease_seconds),attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce;owner_epoch:=v_owner;return next;
end
$function$;

create or replace function public.bpay_next_financial_intake_v1(
  p_actor_user_id uuid,p_action text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];
  v_key text;
  v_command_id uuid;
  v_worker_id uuid;
  v_transfer_id uuid;
  v_instruction_id uuid;
  v_return_cash_id uuid;
  v_run_id uuid;
  v_amount numeric;
  v_occurred_at timestamptz;
  v_result jsonb;
  v_output jsonb;
  v_sequence text;
  v_projection_revision bigint;
  v_case_selection_revision bigint;
  v_case_component_id uuid;
  v_component_revision bigint;
  v_proof_id uuid;
  v_reauth_scope jsonb;
  v_protected boolean;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
     or p_actor_user_id is null
     or not exists(select 1 from public.tms_users u
                   where u.id=p_actor_user_id and u.is_active is true
                     and u.role::text='admin' for share) then
    raise exception using errcode='42501',message='BPAY_NEXT_COMMAND_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_action='CASE_EDIT' then
    return private.bpay_next_accept_case_edit_v1(p_actor_user_id,p_args);
  end if;
  if p_action='CASE_CREATE' then
    if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>32768
      or (select count(*) from jsonb_object_keys(p_args))<>16
      or not(p_args ? 'command_id') or not(p_args ? 'preview_token')
      or jsonb_typeof(p_args->'command_id') is distinct from 'string'
      or (p_args->>'command_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->'preview_token' is null or p_args->'preview_token'='null'::jsonb
      or (p_args-'command_id'-'preview_token') is distinct from (p_args->'preview_token'->'input') then
      raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    perform private.bpay_next_case_create_terms_v1(p_args-'command_id'-'preview_token');
    v_result:=private.bpay_next_accept_case_create_v1((p_args->>'command_id')::uuid,(p_args->>'candidate_id')::uuid,p_actor_user_id,
      p_args->>'case_kind',p_args->>'case_subtype',p_args->>'tax_treatment',(p_args->>'principal')::numeric,
      (p_args->>'due_date')::date,(p_args->>'start_monday')::date,(p_args->>'weekly_due')::numeric,(p_args->>'week_count')::bigint,
      (p_args->>'minimum_earnings_threshold')::numeric,(p_args->>'take_home_floor_override')::numeric,
      p_args->>'reason',p_args->>'note',p_args->>'original_liability_reference',p_args->'preview_token');
    v_output:=pg_catalog.jsonb_build_object('case_id',v_result->'case_id','command_id',v_result->'command_id',
      'sequence',v_result->'sequence','phase',v_result->'phase','replay',v_result->'replay');
    return v_output;
  end if;
  if p_action='RETRY_WORK_JOB' then
    return jsonb_build_object('action',p_action,'result',
      private.bpay_next_retry_work_job_v1(p_actor_user_id,p_args,false));
  end if;
  if p_action is null or p_action not in
      ('PAYE_NET','CASE_PAYOUT','TRANSFER_BEGIN','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL',
       'CSV_ISSUE','CSV_SETTLEMENT','CSV_RETURN','INTERNAL_SETTLE','PREPARATION_EXPIRE','WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  v_keys:=case p_action
    when 'WRITE_OFF' then array['command_id','case_component_id','expected_component_revision','scope','reason']
      || case when p_args->>'scope'='AMOUNT' then array['amount'] else array[]::text[] end
    when 'PAYE_NET' then array['command_id','run_worker_id','entered_paye_net']
      || case when p_args ? 'expected_projection_revision' then array['expected_projection_revision'] else array[]::text[] end
    when 'CASE_PAYOUT' then array['command_id','run_worker_id','expected_projection_revision']
    when 'TRANSFER_BEGIN' then array['command_id','run_worker_id']
    when 'CANCEL_REQUEST' then array['command_id','run_worker_id']
      || case when p_args ? 'run_id' or p_args ? 'pay_channel'
        then array['run_id','pay_channel'] else array[]::text[] end
    when 'CASH_REISSUE' then array['command_id','return_cash_id']
    when 'REISSUE_CANCEL' then array['command_id','transfer_id']
    when 'CSV_ISSUE' then array['instruction_id','transfer_id']
    when 'INTERNAL_SETTLE' then array['command_id','transfer_id']
    when 'PREPARATION_EXPIRE' then array['command_id','run_id','expected_deadline']
    else array['command_id','transfer_id','receipt_id','amount','occurred_at'] end;
  v_protected:=p_action in ('WRITE_OFF','CANCEL_REQUEST','CASH_REISSUE','REISSUE_CANCEL','CSV_ISSUE','CSV_RETURN');
  if v_protected then
    if p_args ? 'reauth_proof_id' then v_keys:=v_keys||array['reauth_proof_id'];
    elsif p_action<>'CSV_ISSUE' then
      raise exception using errcode='42501',message='BPAY_NEXT_REAUTH_REQUIRED';end if;
  end if;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k
               where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
  end if;
  foreach v_key in array v_keys loop
    if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key in ('command_id','run_worker_id','transfer_id','instruction_id','return_cash_id','run_id','case_component_id','reauth_proof_id')
       and (p_args->>v_key)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    if v_key='pay_channel' and (p_args->>v_key) not in ('PAYE','UMBRELLA') then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end loop;
  v_command_id:=(p_args->>'command_id')::uuid;
  v_worker_id:=(p_args->>'run_worker_id')::uuid;
  v_transfer_id:=(p_args->>'transfer_id')::uuid;
  v_instruction_id:=(p_args->>'instruction_id')::uuid;
  v_return_cash_id:=(p_args->>'return_cash_id')::uuid;
  if p_action='WRITE_OFF' then
    v_case_component_id:=(p_args->>'case_component_id')::uuid;
    if (p_args->>'expected_component_revision')!~'^[1-9][0-9]{0,18}$'
       or (p_args->>'expected_component_revision')::numeric>9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_component_revision:=(p_args->>'expected_component_revision')::bigint;
  end if;
  if p_action='PREPARATION_EXPIRE' then
    v_run_id:=(p_args->>'run_id')::uuid;
    if (p_args->>'expected_deadline')!~
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'expected_deadline')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_args ? 'expected_projection_revision' then
    if (p_args->>'expected_projection_revision')!~'^(0|[1-9][0-9]{0,18})$'
       or (p_args->>'expected_projection_revision')::numeric>=9223372036854775807 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_projection_revision:=(p_args->>'expected_projection_revision')::bigint;
  end if;
  if p_action in ('PAYE_NET','CSV_SETTLEMENT','CSV_RETURN') then
    v_key:=case when p_action='PAYE_NET' then 'entered_paye_net' else 'amount' end;
    -- Current money columns are numeric(18,2). Explicit finite, ordinary
    -- decimal syntax prevents exponent/NaN, narrowing and JSON-number coercion.
    if (p_args->>v_key)!~'^(0|[1-9][0-9]{0,15})([.][0-9]{1,2})?$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_amount:=(p_args->>v_key)::numeric;
    if p_action<>'PAYE_NET' and v_amount<=0 then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;
  if p_action in ('CSV_SETTLEMENT','CSV_RETURN') then
    if pg_catalog.octet_length(p_args->>'receipt_id') not between 1 and 256
       or (p_args->>'occurred_at')!~
         '^[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$' then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
    v_occurred_at:=(p_args->>'occurred_at')::timestamptz;
    if not pg_catalog.isfinite(v_occurred_at) then
      raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
    end if;
  end if;

  if v_protected then
    -- Shared finite resolver takes the native business locks before recomputing
    -- scope. Same original receipt qualifies consumed replay, never live repricing.
    v_proof_id:=(p_args->>'reauth_proof_id')::uuid;
    v_reauth_scope:=private.bpay_next_reauth_scope_v1(p_actor_user_id,p_action,p_args-'reauth_proof_id',true);
    if not (p_action='CSV_ISSUE' and (v_reauth_scope->>'accepted')::boolean and v_proof_id is null) then
      perform private.bpay_next_reauth_consume_v1(v_proof_id,p_actor_user_id,p_action,
        v_reauth_scope->>'scope_sha256',coalesce(v_command_id,v_instruction_id));
    end if;
  end if;

  -- Fixed branches and exact typed IDs only. No dynamic identifier, query,
  -- caller actor, arbitrary RPC, live finance/history reconstruction or drain.
  case p_action
    when 'WRITE_OFF' then
      v_result:=private.bpay_next_accept_write_off_v1(v_command_id,v_case_component_id,p_actor_user_id,
        v_component_revision,p_args->>'scope',p_args->>'amount',p_args->>'reason');
      v_output:=pg_catalog.jsonb_build_object('command_id',v_result->>'command_id',
        'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase','requested_scope',v_result->>'requested_scope',
        'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
        'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id',
        'issue_code',v_result->>'issue_code','replay',v_result->'replay');
    when 'PREPARATION_EXPIRE' then
      -- NULL clock selects the real server clock AFTER acquiring the header.
      -- Office cannot supply SYSTEM authority, now, or a renewal duration.
      v_result:=private.bpay_next_accept_preparation_expiry_v1(
        v_command_id,v_run_id,p_actor_user_id,v_occurred_at,null);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','run_id',v_result->>'run_id',
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'expected_candidates',v_result->>'expected_candidates',
        'completed_candidates',v_result->>'completed_candidates',
        'replay',v_result->'replay');
    when 'PAYE_NET' then
      -- Route by the worker's FROZEN selection, never by live Candidate cases.
      -- Explicit prior projection protects resaves from overwriting a newer one.
      select w.case_selection_revision into strict v_case_selection_revision
        from private.bpay_next_run_worker w where w.id=v_worker_id;
      if v_case_selection_revision>0 then
        if v_projection_revision is null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_case_paye_net_v1(
          v_command_id,v_worker_id,p_actor_user_id,v_amount,v_projection_revision);
      else
        if v_projection_revision is not null then
          raise exception using errcode='22023',message='BPAY_NEXT_COMMAND_REQUEST_INVALID';
        end if;
        v_result:=private.bpay_next_accept_simple_paye_net_v1(v_command_id,v_worker_id,v_amount);
      end if;
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CASE_PAYOUT' then
      v_result:=private.bpay_next_accept_case_payout_projection_v1(
        v_command_id,v_worker_id,p_actor_user_id,v_projection_revision);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','request_no',v_result->>'request_no',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'TRANSFER_BEGIN' then
      v_result:=private.bpay_next_begin_simple_transfer_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','transfer_id',v_result->>'transfer_id',
        'phase',v_result->>'phase','replay',v_result->'replay');
    when 'CANCEL_REQUEST' then
      -- Optional address is strictly paired and independently bound before accepting intent.
      if p_args ? 'run_id' and not exists(select 1 from private.bpay_next_run_worker w
        where w.id=v_worker_id and w.run_id=(p_args->>'run_id')::uuid
          and w.target_pay_channel=p_args->>'pay_channel') then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_CHANNEL_SCOPE_INVALID';
      end if;
      v_result:=private.bpay_next_accept_simple_cancel_v1(v_command_id,v_worker_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'run_worker_id',v_worker_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase',
        'cursor',v_result->>'cursor','released_line_count',v_result->>'released_line_count',
        'blocked_code',v_result->>'blocked_code','replay',v_result->'replay');
    when 'CASH_REISSUE' then
      v_result:=private.bpay_next_receive_simple_reissue_v1(v_command_id,v_return_cash_id,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c where c.id=v_command_id and c.command_kind='CASH_REISSUE';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_command_id,'return_cash_id',v_return_cash_id,
        'sequence',v_sequence,'transfer_id',v_result->>'transfer_id',
        'phase',case when v_result->>'transfer_id' is null
          then 'ACCEPTED_PENDING_BUILD' else 'TRANSFER_CREATED' end,
        'replay',v_result->'replay');
    when 'REISSUE_CANCEL' then
      v_result:=private.bpay_next_accept_reissue_cancel_v1(v_command_id,v_transfer_id,p_actor_user_id);
      select r.blocked_code into v_key from private.bpay_next_reissue_cancel_request r where r.command_id=v_command_id;
      v_output:=pg_catalog.jsonb_build_object('command_id',v_command_id,'transfer_id',v_transfer_id,
        'sequence',v_result->>'sequence','phase',v_result->>'phase','blocked_code',v_key,'replay',v_result->'replay');
    when 'CSV_ISSUE' then
      -- A new CSV issue first binds its current approved destination using the
      -- existing owner. Already-issued replay never rebinds or changes bytes.
      -- Both calls use the same transaction and parent run mutex; any issue
      -- permission/state failure rolls the destination preparation back too.
      -- Acquire that mutex BEFORE checking instruction existence: concurrent
      -- same-ID retry must see the first committed issue and skip rebinding.
      select w.run_id into strict v_run_id
        from private.bpay_next_transfer t
        join private.bpay_next_run_worker w on w.id=t.run_worker_id
        where t.id=v_transfer_id;
      perform 1 from private.bpay_next_pay_run where id=v_run_id for update;
      if not exists(select 1 from private.bpay_next_csv_instruction i where i.id=v_instruction_id) then
        perform private.bpay_next_bind_simple_csv_destination_v1(v_transfer_id);
      end if;
      v_result:=private.bpay_next_issue_simple_csv_v1(v_instruction_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'instruction_id',v_result->>'instruction_id','transfer_id',v_result->>'transfer_id',
        'file_name',v_result->>'file_name','csv_sha256',v_result->>'csv_sha256',
        'csv_text',v_result->>'csv_text','phase','ISSUED_CSV',
        'payment_recorded',v_result->'payment_recorded','replay',v_result->'replay');
    when 'CSV_SETTLEMENT' then
      v_result:=private.bpay_next_receive_simple_csv_settlement_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_SETTLEMENT';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
    when 'INTERNAL_SETTLE' then
      -- The server creates an internal receipt for an exact zero-cash frozen
      -- envelope. There is no caller amount, bank receipt or CSV instruction.
      v_result:=private.bpay_next_receive_internal_settlement_v1(
        v_command_id,v_transfer_id,p_actor_user_id);
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_result->>'transfer_id',
        'internal_receipt_id',v_result->>'internal_receipt_id','sequence',v_result->>'sequence',
        'posting_complete',v_result->'posting_complete','phase',v_result->>'phase','replay',v_result->'replay');
    when 'CSV_RETURN' then
      v_result:=private.bpay_next_receive_simple_csv_return_v1(
        v_command_id,v_transfer_id,p_args->>'receipt_id',v_amount,v_occurred_at,p_actor_user_id);
      select c.agency_sequence::text into strict v_sequence
        from private.bpay_next_command c
        where c.id=(v_result->>'command_id')::uuid and c.command_kind='CSV_RETURN';
      v_output:=pg_catalog.jsonb_build_object(
        'command_id',v_result->>'command_id','transfer_id',v_transfer_id,
        'outcome_id',v_result->>'outcome_id','sequence',v_sequence,
        'posting_complete',v_result->'posting_complete','replay',v_result->'replay',
        'phase',case when (v_result->>'posting_complete')::boolean
          then 'POSTED' else 'ACCEPTED_PENDING_POSTING' end);
  end case;
  if pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean'
     or pg_catalog.octet_length(v_output::text)>120000 then
    -- Exception aborts the whole request transaction; no partial issue or
    -- accepted financial request is committed without its bounded receipt.
    raise exception using errcode='54000',message='BPAY_NEXT_COMMAND_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function private.bpay_next_enroll_one_v1()
returns uuid
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_last bigint;
  v_command private.bpay_next_command%rowtype;
  v_member private.bpay_next_command_member%rowtype;
  v_job_id uuid;
  v_epoch bigint;
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_selected_candidate uuid;
  v_expired_preparation boolean;
  v_discarded_preparation boolean;
begin
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select last_enrolled_sequence into strict v_last
    from private.bpay_next_enrollment_clock where id=1 for update;
  select dr09_row.* into v_command from private.bpay_next_command dr09_row where agency_sequence=v_last+1 for update;
  if not found then
    return null;
  end if;
  -- Native completed snooze controls own no financial job. Advance exactly
  -- the next accepted sequence only after its original immutable receipt.
  if v_command.command_kind in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')
    and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not (exists(select 1 from private.bpay_next_snooze_request r
        where r.request_id=v_command.id and r.action=v_command.command_kind
          and (r.request_body->>'request_id')::uuid=r.request_id)
        or private.bpay_next_snooze_due_receipt_valid_v1(v_command.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';
    end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  -- WB-38 parent records frozen native scope; original single child commands own all controls.
  if v_command.command_kind in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') and v_command.status='COMPLETE' then
    if v_command.module_epoch is distinct from v_epoch or v_command.expected_member_count is distinct from 0
      or v_command.enrolled_member_count<>0 or v_command.sealed_at_utc is null
      or exists(select 1 from private.bpay_next_command_member m where m.command_id=v_command.id)
      or exists(select 1 from private.bpay_next_job j where j.command_id=v_command.id)
      or not exists(select 1 from private.bpay_next_snooze_many_request r
        join private.bpay_next_snooze_many_page p on p.request_id=r.request_id and p.page_no=1
        where r.request_id=v_command.id and r.action=v_command.command_kind and r.module_epoch=v_epoch
          and (r.request_body->>'request_id')::uuid=r.request_id and p.result_body->'received'='true'::jsonb) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ENROLLMENT_RECEIPT_INVALID';end if;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return null;
  end if;
  if v_command.command_kind='PREPARE' and exists(select 1 from private.bpay_next_worker_reprepare_request x
      where x.command_id=v_command.id and x.initiator_kind='OFFICE') then
    v_job_id:=private.bpay_next_enroll_worker_action_v1(v_command.id);
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARE' then
    select rc.run_id into strict v_run_id
      from private.bpay_next_run_command rc
      where rc.command_id=v_command.id;
if v_command.status in ('RECEIVED','COMPLETE') then
  -- NO FOR SHARE/UPDATE here. Seal/expiry acquire run then command;
  -- waiting/retired metadata must not add command -> run locking.
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=v_run_id;
  if v_command.status='RECEIVED' then
    if v_command.module_epoch is distinct from v_epoch
       or v_run.status is distinct from 'PREPARING'
       or v_run.selection_state is distinct from 'OPEN'
       or v_run.confirmed_at_utc is not null
       or v_run.accepted_choice_descriptor is null
       or v_run.accepted_choice_command_id is distinct from v_command.id
       or v_command.expected_member_count is not null
       or v_command.enrolled_member_count<>0
       or v_command.sealed_at_utc is not null
       or exists(select 1 from private.bpay_next_command_member cm
         where cm.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_job j
         where j.command_id=v_command.id)
       or exists(select 1 from private.bpay_next_run_worker rw
         where rw.run_id=v_run_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    return null;
  end if;
  -- Only an exact expired, never-enrolled positive CREATE may retire0.
  if v_command.module_epoch is distinct from v_epoch
     or v_command.expected_member_count is distinct from 0::bigint
     or v_command.enrolled_member_count<>0
     or v_command.sealed_at_utc is null
     or v_run.selection_state is distinct from 'OPEN'
     or v_run.status not in ('CANCELLING','CANCELLED')
     or v_run.confirmed_at_utc is not null
     or v_run.accepted_choice_descriptor is null
     or v_run.accepted_choice_command_id is distinct from v_command.id
     or not(private.bpay_next_preparation_expiry_fenced_v1(v_run_id,v_command.id)
       or private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id))
     or exists(select 1 from private.bpay_next_command_member cm
       where cm.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_job j
       where j.command_id=v_command.id)
     or exists(select 1 from private.bpay_next_run_worker rw
       where rw.run_id=v_run_id) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence
    where id=1 and last_enrolled_sequence=v_last
      and v_command.agency_sequence=v_last+1;
  if not found then
    raise exception using errcode='23514',
      message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
  end if;
  return null;
end if;
    select dr09_row.* into strict v_run from private.bpay_next_pay_run dr09_row where id=v_run_id for share nowait;
    v_expired_preparation:=private.bpay_next_preparation_expiry_fenced_v1(
      v_run_id,v_command.id);
    v_discarded_preparation:=private.bpay_next_preparation_discard_fenced_v1(v_run_id,v_command.id);
    if v_command.module_epoch<>v_epoch
       or v_command.status<>'ENROLLING'
       or (v_run.status<>'PREPARING' and not
           (v_run.status='CANCELLING' and(v_expired_preparation or v_discarded_preparation)))
       or v_run.selection_state<>'SEALED'
       or v_command.expected_member_count<>v_run.selected_candidate_count
       or v_command.enrolled_member_count>=v_command.expected_member_count then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PREPARE_ENROLLMENT_INVALID';
    end if;
    select sc.candidate_id into strict v_selected_candidate
      from private.bpay_next_selection_candidate sc
      where sc.run_id=v_run_id
        and sc.member_no=v_command.enrolled_member_count+1;
    insert into private.bpay_next_command_member
      (command_id,candidate_id,member_no)
      values(v_command.id,v_selected_candidate,
             v_command.enrolled_member_count+1);
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_selected_candidate,'PREPARE',
             case when v_expired_preparation or v_discarded_preparation then 'DONE' else 'READY' end,
             case when v_expired_preparation then 'EXPIRED' when v_discarded_preparation then 'DISCARDED' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command
      set enrolled_member_count=enrolled_member_count+1,
          status=case when enrolled_member_count+1=expected_member_count
            then 'SEALED' else 'ENROLLING' end,
          sealed_at_utc=case when enrolled_member_count+1=expected_member_count
            then pg_catalog.transaction_timestamp() else null end
      where id=v_command.id;
    if v_command.enrolled_member_count+1=v_command.expected_member_count then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_EXPIRY' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_expiry_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.command_kind='PREPARATION_DISCARD' then
    -- Exact header fence permits only metadata enrollment here. The helper
    -- retires unfinished original PREPARE jobs and binds ONE cleanup member;
    -- financial holds are released later by its normally ordered worker.
    -- The helper, not this caller, owns member counts and command sealing.
    v_job_id:=private.bpay_next_enroll_preparation_discard_one_v1(v_command.id);
    if exists(select 1 from private.bpay_next_command c
        where c.id=v_command.id
          and c.enrolled_member_count=c.expected_member_count
          and c.status in ('SEALED','COMPLETE')) then
      update private.bpay_next_enrollment_clock
        set last_enrolled_sequence=v_command.agency_sequence where id=1;
    end if;
    return v_job_id;
  end if;
  if v_command.status <> 'SEALED' then
    return null;
  end if;
  if v_command.command_kind='CASE_WRITE_OFF' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_write_off_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_WRITE_OFF_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_WRITE_OFF','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_EDIT' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_edit_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_EDIT_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_EDIT','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='CASE_CREATE' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_case_create_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREATE_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'CASE_CREATE','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='REISSUE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_reissue_cancel_request r
       where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_REISSUE_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'REISSUE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind in ('CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','INTERNAL_SETTLEMENT') then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if v_command.command_kind='INTERNAL_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is null and r.internal_receipt_id is not null)
       or v_command.command_kind='CSV_SETTLEMENT' and not exists(select 1 from private.bpay_next_outcome_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id
                    and r.outcome_id is not null and r.internal_receipt_id is null)
       or v_command.command_kind='CSV_RETURN' and not exists(select 1 from private.bpay_next_return_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id)
       or v_command.command_kind='CASH_REISSUE' and not exists(select 1 from private.bpay_next_reissue_request r
                  where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_OUTCOME_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,v_command.command_kind,'READY','MEMBERS',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='SIMPLE_CANCEL' then
    if v_command.module_epoch<>v_epoch or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_cancel_request r
      where r.command_id=v_command.id and r.candidate_id=v_member.candidate_id
        and r.status='REQUESTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,v_member.candidate_id,
        'SIMPLE_CANCEL','READY','NEW',1) returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1 where id=v_command.id;
    update private.bpay_next_enrollment_clock set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='PAYE_NET_ENTRY' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_paye_net_request r
                  where r.command_id=v_command.id
                    and r.candidate_id=v_member.candidate_id) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_PAYE_NET_REQUEST_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'PAYE_NET_ENTRY','READY',
             case when exists(select 1 from private.bpay_next_paye_net_request r
               where r.command_id=v_command.id and r.case_draft_state_id is not null)
               then 'FINANCE_ALLOCATE' else 'NEW' end,1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.command_kind='TRANSFER_BUILD' then
    if v_command.module_epoch<>v_epoch
       or v_command.expected_member_count<>1
       or v_command.enrolled_member_count<>0 then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_ENROLLMENT_INVALID';
    end if;
    select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
    if not exists(select 1 from private.bpay_next_transfer t
                  where t.build_command_id=v_command.id
                    and t.candidate_id=v_member.candidate_id
                    and t.status='BUILDING') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_MISSING';
    end if;
    insert into private.bpay_next_job
      (command_id,command_sequence,module_epoch,candidate_id,job_kind,
       status,phase,owner_epoch)
      values(v_command.id,v_command.agency_sequence,v_epoch,
             v_member.candidate_id,'TRANSFER_BUILD','READY','WORK',1)
      returning id into v_job_id;
    update private.bpay_next_command set enrolled_member_count=1
      where id=v_command.id;
    update private.bpay_next_enrollment_clock
      set last_enrolled_sequence=v_command.agency_sequence where id=1;
    return v_job_id;
  end if;
  if v_command.module_epoch<>v_epoch
     or v_command.command_kind<>'POSITION_APPLY'
     or v_command.expected_member_count<>1
     or v_command.enrolled_member_count<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_COMMAND_INVALID';
  end if;
  select dr09_row.* into strict v_member from private.bpay_next_command_member dr09_row where command_id=v_command.id and member_no=1;
  if not exists (select 1 from private.bpay_next_publication p
                 where p.command_id=v_command.id and p.candidate_id=v_member.candidate_id) then
    raise exception using errcode='23514', message='BPAY_NEXT_ENROLLMENT_PUBLICATION_MISSING';
  end if;
  insert into private.bpay_next_job
    (command_id,command_sequence,module_epoch,candidate_id,job_kind,
     status,phase,owner_epoch)
    values (v_command.id,v_command.agency_sequence,v_epoch,
            v_member.candidate_id,'POSITION_APPLY','READY','NEW',1)
    returning id into v_job_id;
  update private.bpay_next_command set enrolled_member_count=1
    where id=v_command.id;
  update private.bpay_next_enrollment_clock
    set last_enrolled_sequence=v_command.agency_sequence where id=1;
  return v_job_id;
end
$function$;

create or replace function public.bpay_next_financial_job_v1(
  p_action text,p_job_id uuid,p_job_kind text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_keys text[];
  v_key text;
  v_paged boolean;
  v_integer_cursor boolean;
  v_phase text;
  v_lease_seconds integer;
  v_nonce uuid;
  v_owner_epoch bigint;
  v_cursor bigint;
  v_text_cursor text;
  v_work_cursor uuid;
  v_component_cursor text;
  v_limit integer;
  v_claim record;
  v_claim_state record;
  v_page record;
  v_worker private.bpay_next_run_worker%rowtype;
  v_result jsonb;
  v_output jsonb;
  v_lease_until text;
  v_claim_failure text;
  v_case_net boolean:=false;
  v_expired_prepare boolean:=false;
  v_discarded_prepare boolean:=false;
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_EDIT','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_phase:=p_args->>'expected_phase';
  if p_action='STEP' and (p_job_kind in ('POSITION_APPLY','PREPARE')
      or (p_job_kind='PAYE_NET_ENTRY' and p_args ? 'expected_phase'))
     and (pg_catalog.jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
       or not(v_phase=any(case p_job_kind when 'POSITION_APPLY' then array['NEW','REMOVED']
         when 'PAYE_NET_ENTRY' then array['FINANCE_ALLOCATE']
         else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_keys:=case when p_action='CLAIM' then array['lease_seconds']
    when p_job_kind='POSITION_APPLY' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then array['lease_nonce','owner_epoch','expected_phase','expected_cursor']
    when p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then array['lease_nonce','owner_epoch','expected_phase','expected_work_cursor','expected_component_cursor','limit']
    when p_job_kind='PREPARE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when v_paged then array['lease_nonce','owner_epoch','expected_cursor','limit']
    else array['lease_nonce','owner_epoch'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='CLAIM' then
    if pg_catalog.jsonb_typeof(p_args->'lease_seconds') is distinct from 'number'
       or (p_args->>'lease_seconds')!~'^[1-9][0-9]{0,2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    v_lease_seconds:=(p_args->>'lease_seconds')::integer;
    if v_lease_seconds not between 1 and 120 then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  else
    if pg_catalog.jsonb_typeof(p_args->'lease_nonce') is distinct from 'string'
       or (p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    foreach v_key in array array['owner_epoch','expected_cursor'] loop
      if v_key='expected_cursor' and (not coalesce(v_integer_cursor,false) or p_args->v_key='null'::jsonb) then
        continue;
      end if;
      if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string'
         or (p_args->>v_key)!~'^[1-9][0-9]{0,18}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      if (p_args->>v_key)::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end loop;
    v_nonce:=(p_args->>'lease_nonce')::uuid;
    v_owner_epoch:=(p_args->>'owner_epoch')::bigint;
    if v_integer_cursor then v_cursor:=(p_args->>'expected_cursor')::bigint; end if;
    if p_job_kind='POSITION_APPLY' then
      if p_args->'expected_cursor'<>'null'::jsonb
         and (pg_catalog.jsonb_typeof(p_args->'expected_cursor') is distinct from 'string'
              or pg_catalog.char_length(p_args->>'expected_cursor') not between 1 and 256) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_text_cursor:=p_args->>'expected_cursor';
    elsif p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then
      -- A cursor is the complete (UUID work, opaque retained component TEXT)
      -- pair, or both NULL. Component bounds are CHARACTERS, not bytes.
      if (p_args->'expected_work_cursor'='null'::jsonb) is distinct from
          (p_args->'expected_component_cursor'='null'::jsonb)
         or (p_args->'expected_work_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_work_cursor') is distinct from 'string'
                  or (p_args->>'expected_work_cursor')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
         or (p_args->'expected_component_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_component_cursor') is distinct from 'string'
                  or pg_catalog.char_length(p_args->>'expected_component_cursor') not between 1 and 256)) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_work_cursor:=(p_args->>'expected_work_cursor')::uuid;
      v_component_cursor:=p_args->>'expected_component_cursor';
    end if;
    if v_paged then
      if pg_catalog.jsonb_typeof(p_args->'limit') is distinct from 'number'
         or (p_args->>'limit')!~'^[1-9][0-9]{0,2}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_limit:=(p_args->>'limit')::integer;
      if v_limit not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end if;
  end if;

  -- Immutable dispatch identity, without an outer job lock. Owners acquire
  -- Candidate -> job for CLAIM, parent run -> Candidate -> job for STEP.
  -- Taking job FOR UPDATE here would invert that established order.
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>p_job_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';
  end if;
  -- Query admission is outside even the public module SHARE. Plain dispatch
  -- discovery is not lease/module authority; every owner rechecks under locks.
  -- Definite terminal/advanced/cursor/stale replies must not newly contend on
  -- the query key. Their independent owner locks/validates the module before
  -- any write, and this boundary reasserts it immediately after that call.
  if p_action='STEP' and p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then
    if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
       and v_job.lease_nonce is not distinct from v_nonce
       and v_job.owner_epoch is not distinct from v_owner_epoch
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()
       and v_job.cursor_key::bigint is not distinct from v_cursor then
      perform private.weekly_source_pay_query_admit_v2();
    else
      v_defer_pin_module:=true;
    end if;
  end if;
  if not v_defer_pin_module and not exists(
      select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_job_kind='PAYE_NET_ENTRY' then
    select r.case_draft_state_id is not null into strict v_case_net
      from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id;
    if p_action='STEP' and v_case_net is distinct from (p_args ? 'expected_phase') then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  end if;
  if p_action='CLAIM' then
    if (p_job_kind='POSITION_APPLY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['DONE'] else array['NEW','REMOVED'] end)))
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED','DISCARDED','CANCELLED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind='PREPARATION_DISCARD' and not(v_job.phase=any(case when v_job.status='DONE' then array['DISCARDED','BLOCKED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind in ('CASE_CREATE','CASE_EDIT','CASE_WRITE_OFF') and (v_job.phase is distinct from
          case when v_job.status='DONE' then 'DONE' else 'NEW' end or v_job.cursor_key is not null)) then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
    end if;
    -- Distinguish terminal and busy claims; never return the stored nonce.
    -- These read-only early checks are not ownership: owners recheck/lock.
    if v_job.status='DONE' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
    end if;
    if v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
    end if;
    begin
      case p_job_kind
        when 'POSITION_APPLY' then
          select c.* into strict v_claim from private.bpay_next_claim_position_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARE' then
          select c.* into strict v_claim from private.bpay_next_claim_prepare_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_EXPIRY' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_expiry_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_DISCARD' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_discard_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_CREATE' then
          select c.* into strict v_claim from private.bpay_next_claim_case_create_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_EDIT' then
          select c.* into strict v_claim from private.bpay_next_claim_case_edit_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_WRITE_OFF' then
          select c.* into strict v_claim from private.bpay_next_claim_write_off_job_v1(p_job_id,v_lease_seconds) c;
        when 'PAYE_NET_ENTRY' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_paye_net_job_v1(p_job_id,v_lease_seconds) c;
        when 'TRANSFER_BUILD' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_transfer_job_v1(p_job_id,v_lease_seconds) c;
        when 'SIMPLE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_cancel_job_v1(p_job_id,v_lease_seconds) c;
        when 'REISSUE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_reissue_cancel_job_v1(p_job_id,v_lease_seconds) c;
        else
          select c.* into strict v_claim from private.bpay_next_claim_simple_outcome_job_v1(p_job_id,v_lease_seconds) c;
      end case;
    exception when sqlstate '55000' then
      get stacked diagnostics v_claim_failure=message_text;
      if v_claim_failure not in
          ('BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE','BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE','BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_EDIT_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD') then raise; end if;
      -- A loser may have read READY, then waited behind the winning owner.
      -- The failed subtransaction released its owner locks. Classify its
      -- now-visible exact job without acquiring job ahead of Candidate/run,
      -- retrying any owner or adopting the winner's stored lease nonce.
      -- Job and admission predicates share one fresh read snapshot. A
      -- predecessor finishing during separate reads must not manufacture a
      -- permanent eligibility failure for an already accepted financial job.
      select j.*,
        exists(select 1 from private.bpay_next_pay_run r
          where r.id=private.bpay_next_prepare_job_run_v1(j.id) and r.status='PREPARING' and r.selection_state='SEALED'
            and private.bpay_next_prepare_job_active_v1(j.id)
            and exists(select 1 from private.bpay_next_run_worker w where w.run_id=r.id and w.candidate_id=j.candidate_id
              and (w.status='PREPARING' or (j.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and w.status in ('READY','REVIEW','CANCELLED'))
                or (j.phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') and w.status='CANCELLING')))) as prepare_admissible,
        exists(select 1 from private.bpay_next_cancel_request r
               where r.command_id=j.command_id and r.candidate_id=j.candidate_id
                 and r.status in ('REQUESTED','CANCELLING')) as cancel_request_pending,
        exists(select 1 from private.bpay_next_preparation_expiry_request r
          join private.bpay_next_pay_run run on run.id=r.run_id
          where r.command_id=j.command_id and r.status='CANCELLING'
            and run.status='CANCELLING' and run.confirmed_at_utc is null) as expiry_request_pending,
        exists(select 1 from private.bpay_next_preparation_discard_request q join private.bpay_next_pay_run r on r.id=q.run_id
          where q.command_id=j.command_id and q.status='CANCELLING' and r.status='CANCELLING' and r.confirmed_at_utc is null) as discard_request_pending,
        exists(select 1 from private.bpay_next_case_create_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_create_request_present,
        exists(select 1 from private.bpay_next_case_edit_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_edit_request_present,
        exists(select 1 from private.bpay_next_write_off_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.status='REQUESTED') as write_off_request_pending,
        exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=j.candidate_id
                 and earlier.command_sequence<j.command_sequence
                 and earlier.status<>'DONE') as predecessor_pending
      into strict v_claim_state from private.bpay_next_job j where j.id=p_job_id;
      if v_claim_state.job_kind<>p_job_kind or v_claim_state.module_epoch is distinct from
          (select m.owner_epoch from private.bpay_next_module_control m
           where m.id=1 and m.active_owner='NEXT') then
        raise;
      end if;
      if v_claim_state.status='DONE' then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
      end if;
      if not(v_claim_state.phase=any(case p_job_kind
          when 'PAYE_NET_ENTRY' then array['NEW','FINANCE_ALLOCATE']
          when 'TRANSFER_BUILD' then array['WORK']
          when 'SIMPLE_CANCEL' then array['NEW','RELEASE']
          when 'REISSUE_CANCEL' then array['NEW']
          when 'POSITION_APPLY' then array['NEW','REMOVED']
          when 'PREPARE' then array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL']
          when 'PREPARATION_EXPIRY' then array['NEW','RELEASE']
          when 'PREPARATION_DISCARD' then array['NEW','RELEASE']
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_EDIT' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='PREPARATION_DISCARD' and not v_claim_state.discard_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_EDIT' and (not v_claim_state.case_edit_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_WRITE_OFF' and (not v_claim_state.write_off_request_pending
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='SIMPLE_CANCEL'
             and (v_claim_state.available_at_utc>pg_catalog.clock_timestamp()
                  or not v_claim_state.cancel_request_pending)) then
        raise;
      end if;
      if v_claim_state.status='LEASED' and v_claim_state.lease_until_utc>pg_catalog.clock_timestamp() then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
      end if;
      if v_claim_state.status in ('READY','LEASED') and v_claim_state.predecessor_pending then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR';
      end if;
      if v_claim_state.status in ('READY','LEASED') and not v_claim_state.predecessor_pending
         and (v_claim_state.status='READY'
              or v_claim_state.lease_until_utc<=pg_catalog.clock_timestamp()) then
        -- The predecessor just completed or the short lease just expired.
        -- Only signal a fresh CLAIM; never invoke the owner a second time.
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_CLAIM_RETRY';
      end if;
      -- Other permanent scope/phase/request admission failures retain the
      -- exact original owner rejection, not a general automatic retry.
      raise;
    end;
    -- Only the owner-returned newly acquired token crosses this boundary.
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from v_claim.lease_nonce
       or v_job.owner_epoch is distinct from v_claim.owner_epoch then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_CLAIM_RESULT_INVALID';
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase,
      'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text,
      'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key);
    if p_job_kind='PREPARE' then
      v_output:=v_output||pg_catalog.jsonb_build_object(
        'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
        'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
        'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end);
    end if;
  else
    -- Do not reject cleared DONE leases here. The owners' scoped DONE replay
    -- branches are authoritative and return the immutable retained result.
    case p_job_kind
      when 'POSITION_APPLY' then
        select p.* into strict v_page from private.bpay_next_apply_position_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_phase,v_text_cursor,v_limit) p;
        v_result:=pg_catalog.jsonb_build_object('phase',v_page.next_phase,
          'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done);
      when 'PREPARE' then
        -- Terminal authority comes from the retained owner, never a fabricated
        -- READY row or an adopted active cursor/nonce. One owner per STEP.
        select private.bpay_next_preparation_expiry_fenced_v1(
          rc.run_id,rc.command_id) into v_expired_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        select private.bpay_next_preparation_discard_fenced_v1(rc.run_id,rc.command_id) into v_discarded_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        v_discarded_prepare:=coalesce(v_discarded_prepare,false) or(v_job.status='DONE' and v_job.phase='DISCARDED'
          and exists(select 1 from private.bpay_next_worker_reprepare_request x where x.previous_job_id=p_job_id
            and x.candidate_id=v_job.candidate_id and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'));
        if coalesce(v_discarded_prepare,false) then
          if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DISCARDING';end if;
          v_result:=private.bpay_next_discarded_prepare_receipt_v1(p_job_id);
        elsif coalesce(v_expired_prepare,false) then
          if v_job.status<>'DONE' then
            raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';
          end if;
          -- May have been retired before its first CLAIM: no worker is invented.
          -- Only the exact expiry owner validates and returns this old receipt.
          v_result:=private.bpay_next_expired_prepare_receipt_v1(p_job_id);
        elsif v_job.status='DONE' and v_job.phase='CANCELLED' then
          if not exists(select 1 from private.bpay_next_worker_reprepare_request x
            join private.bpay_next_run_worker w on w.id=x.run_worker_id
            where x.job_id=p_job_id and x.action='RELEASE' and x.status='COMPLETE'
              and x.target_preparation_revision=w.preparation_revision and w.status='CANCELLED'
              and x.module_epoch=v_job.module_epoch) then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
          v_result:=pg_catalog.jsonb_build_object('phase','CANCELLED','cursor',v_job.cursor_key,'rows_visited','0','replay',true);
        elsif v_job.status='DONE' and (exists(select 1 from private.bpay_next_case_allocation_state s
          join private.bpay_next_run_worker w on w.id=s.run_worker_id
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id
            and s.preparation_revision=w.captured_preparation_revision and s.pass_kind='DRAFT')
          or exists(select 1 from private.bpay_next_run_worker w
            where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id and w.status='REVIEW'
              and w.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
                'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT'))) then
          -- An original NEW/PINNING request has no page limit. Terminal
          -- read-back does no work, but the owner still requires a valid bound.
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce(v_limit,1));
        elsif v_job.status='DONE' and v_job.phase='READY' then
          v_result:=private.bpay_next_complete_no_case_worker_v1(p_job_id,v_nonce,v_owner_epoch);
        elsif v_job.status='DONE' and v_job.phase='DONE' then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,null);
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited','0');
        elsif v_phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then
          v_result:=private.bpay_next_worker_release_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
          v_result:=private.bpay_next_prepare_projection_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('NEW','PINNING') then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor);
          -- This owner examines at most one selected work. It exposes no page
          -- count, but its exact work/replay receipt identifies zero vs one.
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited',case
            when v_result->>'rows_visited' is not null then v_result->>'rows_visited'
            when v_result->>'replay'='true' then '0'
            when v_result->>'work_id' is not null then '1' else '0' end);
        elsif v_phase='RESERVE' then
          v_result:=private.bpay_next_capture_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='ALLOCATE' then
          v_result:=private.bpay_next_hold_fresh_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='FINANCE_ALLOCATE' then
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
        end if;
      when 'PAYE_NET_ENTRY' then
        if v_case_net then
          v_result:=private.bpay_next_apply_case_paye_net_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          v_result:=private.bpay_next_apply_simple_paye_net_v1(p_job_id,v_nonce,v_owner_epoch);
        end if;
      when 'TRANSFER_BUILD' then
        v_result:=private.bpay_next_build_simple_transfer_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'INTERNAL_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_RETURN' then
        v_result:=private.bpay_next_post_simple_return_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASH_REISSUE' then
        v_result:=private.bpay_next_build_simple_reissue_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'SIMPLE_CANCEL' then
        v_result:=private.bpay_next_cancel_simple_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'REISSUE_CANCEL' then
        v_result:=private.bpay_next_apply_reissue_cancel_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'PREPARATION_EXPIRY' then
        v_result:=private.bpay_next_expire_preparation_worker_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'PREPARATION_DISCARD' then
        v_result:=private.bpay_next_discard_preparation_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CASE_CREATE' then
        -- One exact original approval application. The retained 0330 owner
        -- alone governs lease validation and DONE/DONE immutable replay.
        v_result:=private.bpay_next_apply_case_create_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_EDIT' then
        v_result:=private.bpay_next_apply_case_edit_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_WRITE_OFF' then
        v_result:=private.bpay_next_apply_write_off_v1(p_job_id,v_nonce,v_owner_epoch);
    end case;
    if v_defer_pin_module and not exists(
        select 1 from private.bpay_next_module_control m
        where m.id=1 and m.active_owner='NEXT' for share) then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
    end if;
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status not in ('LEASED','DONE')
       or (p_job_kind<>'POSITION_APPLY' and pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean')
       or (p_job_kind='POSITION_APPLY' and ((v_result->>'done')::boolean is distinct from (v_job.status='DONE')
            or v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
      or (p_job_kind='PREPARE' and v_result->>'phase' is distinct from
            case when v_discarded_prepare then 'DISCARDED' when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind in ('PREPARATION_EXPIRY','PREPARATION_DISCARD') and (v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
       or (p_job_kind='CASE_CREATE' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or v_result->>'phase' is distinct from 'CASE_CREATED'
            or not exists(select 1 from private.bpay_next_case_create_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.case_id::text=v_result->>'case_id')))
       or (p_job_kind='CASE_EDIT' and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or v_result->>'phase' is distinct from case when v_job.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end
          or v_result->>'issue_code' is distinct from v_job.last_error_code
          or not exists(select 1 from private.bpay_next_case_edit_request q where q.command_id=v_job.command_id
            and q.candidate_id=v_job.candidate_id and q.case_id::text=v_result->>'case_id'
            and q.new_rule_id::text=v_result->>'rule_id' and (q.expected_case_revision+1)::text=v_result->>'case_revision')))
       or (p_job_kind='CASE_WRITE_OFF' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or not exists(select 1 from private.bpay_next_write_off_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW') and r.status=v_result->>'phase'
                and r.case_id::text=v_result->>'case_id' and r.case_component_id::text=v_result->>'case_component_id'
                and r.applied_amount is not distinct from (v_result->>'applied_amount')::numeric
                and r.remaining_amount is not distinct from (v_result->>'remaining_amount')::numeric
                and r.protected_amount is not distinct from (v_result->>'protected_amount')::numeric
                and r.event_id::text is not distinct from v_result->>'event_id'
                and r.issue_code is not distinct from v_result->>'issue_code')))
       or (v_case_net and (v_result->>'phase' is distinct from
            case when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end
          or v_result->>'cursor' is distinct from v_job.cursor_key
          or (v_result->>'run_worker_id')::uuid is distinct from
            (select r.run_worker_id from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id))) then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
    end if;
    -- Yield only this successfully validated page's own lease. The next ID
    -- notification takes a fresh CLAIM from the committed native checkpoint.
    if v_job.status='LEASED' then
      if v_job.lease_nonce is distinct from v_nonce or v_job.owner_epoch is distinct from v_owner_epoch then
        raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
      end if;
      update private.bpay_next_job set status='READY',lease_nonce=null,lease_until_utc=null
        where id=p_job_id and status='LEASED' and lease_nonce=v_nonce and owner_epoch=v_owner_epoch
        returning * into strict v_job;
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text,
      'lease_until_utc',v_lease_until,'done',v_job.status='DONE');
    if p_job_kind<>'POSITION_APPLY' then
      v_output:=v_output||pg_catalog.jsonb_build_object('replay',v_result->'replay');
    end if;
    case p_job_kind
      when 'POSITION_APPLY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited');
      when 'PREPARE' then
        if coalesce(v_expired_prepare,false) or coalesce(v_discarded_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase',case when v_discarded_prepare then 'DISCARDED' else 'EXPIRED' end,
            'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor',
            'component_cursor',v_result->>'component_cursor',
            'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status',
            'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id',
            'rows_visited',v_result->>'rows_visited');
        else
        select w.* into strict v_worker from private.bpay_next_run_worker w
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id;
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
          'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
          'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end,
          'run_worker_id',v_worker.id,'worker_status',v_worker.status,
          'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id,
          -- NULL means the exact owner did not report a count; do not fabricate
          -- zero for a REVIEW reached after earlier rows in the same page.
          'rows_visited',v_result->>'rows_visited');
        end if;
      when 'PAYE_NET_ENTRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','projection_id',v_result->>'projection_id');
        if v_case_net then
          v_output:=v_output||pg_catalog.jsonb_build_object(
            'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
            'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code');
        end if;
      when 'TRANSFER_BUILD' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id',
          'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'),
          'cursor',v_job.cursor_key,'member_count',v_result->>'member_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'INTERNAL_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_RETURN' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id');
      when 'CASH_REISSUE' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','transfer_id',v_result->>'transfer_id');
      when 'REISSUE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code');
      when 'SIMPLE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id',
          'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'),
          'holds_released',coalesce(v_result->>'holds_released','0'),
          'blocked_code',v_result->>'blocked_code');
      when 'PREPARATION_EXPIRY','PREPARATION_DISCARD' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
          'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released',
          'run_worker_id',v_result->>'run_worker_id');
      when 'CASE_CREATE' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id');
      when 'CASE_EDIT' then
        v_output:=v_output||jsonb_build_object('phase',v_result->>'phase','case_id',v_result->>'case_id',
          'case_revision',v_result->>'case_revision','rule_id',v_result->>'rule_id','issue_code',v_result->>'issue_code');
      when 'CASE_WRITE_OFF' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
          'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
          'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code');
    end case;
  end if;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_JOB_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;

create or replace function private.bpay_next_case_allocation_plan_v1(p_scope jsonb)
returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private,public
as $function$
declare
 v_mode text;v_candidate uuid;v_run_id uuid;v_run private.bpay_next_pay_run%rowtype;
 v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_period_rule private.bpay_next_case_rule%rowtype;
 v_origin private.bpay_next_case_create_request%rowtype;v_collection private.bpay_next_work_collection%rowtype;
 v_state private.bpay_next_case_allocation_state%rowtype;v_worker private.bpay_next_run_worker%rowtype;
 v_period private.bpay_next_case_period%rowtype;v_value record;v_row record;
 v_amendment jsonb;v_old_amendment jsonb;v_period_before jsonb;v_approved_principal numeric;
 v_instruction private.bpay_next_run_case_instruction%rowtype;v_instruction_json jsonb;v_rows jsonb:='[]';
 v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_status text;v_case_due date;
 v_automatic boolean;v_explanation text;v_outstanding numeric;v_case_outstanding numeric;
 v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_purpose text;v_age timestamptz;v_age_key bigint;
 v_period_operation text;v_resolution_ref text;v_e numeric;v_h numeric;v_taken numeric;v_next_e numeric;v_next_h numeric;
 v_cap_reason text;v_affordability_reason text;v_capacity_reason text;v_keys text[];
begin
 if jsonb_typeof(p_scope) is distinct from 'object' then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
 v_mode:=p_scope->>'mode';
 if v_mode='DERIVE' then
  v_keys:=array['mode','candidate_id','case_id','case_component_id','pay_date','pay_week_start',
   'target_pay_channel','captured_default_floor','captured_default_floor_revision','weekly_binding_state','accepted_run_id'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_candidate:=(p_scope->>'candidate_id')::uuid;v_run_id:=(p_scope->>'accepted_run_id')::uuid;
  v_run.pay_date:=(p_scope->>'pay_date')::date;
  v_state.pay_week_start:=(p_scope->>'pay_week_start')::date;
  v_worker.target_pay_channel:=p_scope->>'target_pay_channel';
  v_state.captured_default_floor:=(p_scope->>'captured_default_floor')::numeric;
  v_state.captured_default_floor_revision:=(p_scope->>'captured_default_floor_revision')::bigint;
  v_state.weekly_binding_state:=p_scope->>'weekly_binding_state';
  if v_candidate is null or v_run.pay_date is null or not isfinite(v_run.pay_date)
   or v_state.pay_week_start is distinct from v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1)
   or v_state.captured_default_floor is null or v_state.captured_default_floor<0
   or v_state.captured_default_floor::text in ('NaN','Infinity','-Infinity')
   or v_state.captured_default_floor<>trunc(v_state.captured_default_floor,2)
   or coalesce(v_state.captured_default_floor_revision,0)<1
   or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if v_run_id is not null then
   select r.accepted_choice_descriptor into strict v_run.accepted_choice_descriptor from private.bpay_next_pay_run r
    where r.id=v_run_id and r.pay_date=v_run.pay_date and r.selection_state='SEALED';
  end if;
  select c.* into strict v_case from private.bpay_next_finance_case c
   where c.id=(p_scope->>'case_id')::uuid and c.candidate_id=v_candidate;
  select c.* into strict v_component from private.bpay_next_case_component c
   where c.id=(p_scope->>'case_component_id')::uuid and c.case_id=v_case.id and c.candidate_id=v_candidate;
        -- WB26: immutable rule/control choice from this original accepted cut.
        -- Balances below are still live and protected by the original money locks.
        v_memory_cut:=v_run_id is not null and v_run.accepted_choice_descriptor is not null
          and v_run.accepted_choice_descriptor ? 'memory_baseline_run_id';
        v_case_cut:=null;v_case_status:=v_case.status;v_case_due:=v_case.due_date;
        if v_memory_cut then
          select retained.basis into v_case_cut from private.bpay_next_selection_mode_memory retained
            where retained.run_id=v_run_id and retained.row_kind='LEAF'
              and retained.leaf_key='CASE:'||v_component.id::text
              and retained.candidate_id=v_candidate and retained.case_component_id=v_component.id;
          if v_case_cut is null or not(v_case_cut ?& array['case_id','case_component_id','case_revision',
              'case_state','case_due_date','case_kind','case_subtype','case_tax_treatment','case_instruction_kind',
              'case_component_ordinal','case_control_sequence','case_component_revision','rule_id','case_rule_revision'])
            or (v_case_cut->>'case_id',v_case_cut->>'case_component_id',v_case_cut->>'case_kind',v_case_cut->>'case_subtype',
                v_case_cut->>'case_tax_treatment',v_case_cut->>'case_instruction_kind',v_case_cut->>'direction',
                v_case_cut->>'payroll_stage',v_case_cut->>'source_pay_channel',v_case_cut->>'case_component_ordinal')
              is distinct from (v_case.id::text,v_component.id::text,v_component.case_kind,v_component.case_subtype,
                v_component.tax_treatment,v_component.instruction_kind,v_component.direction,v_component.payroll_stage,
                v_component.source_pay_channel,v_component.component_ordinal::text)
            or coalesce((v_case_cut->>'case_revision')::bigint,0)<1 or coalesce((v_case_cut->>'case_component_revision')::bigint,0)<1
            or v_case_cut->>'case_state' is distinct from 'OPEN'
            or v_case_cut->>'rule_id' is null or coalesce((v_case_cut->>'case_rule_revision')::bigint,0)<1
            or coalesce((v_case_cut->>'case_control_sequence')::bigint,-1)<0 then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          v_case_status:=v_case_cut->>'case_state';v_case_due:=(v_case_cut->>'case_due_date')::date;
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=(v_case_cut->>'rule_id')::uuid
            and r.case_id=v_case.id and r.candidate_id=v_candidate
            and r.rule_revision=(v_case_cut->>'case_rule_revision')::bigint
            and (r.case_kind,r.case_subtype,r.tax_treatment)
              is not distinct from (v_component.case_kind,v_component.case_subtype,v_component.tax_treatment);
        else
          select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id
            and r.case_id=v_case.id and r.candidate_id=v_candidate and r.rule_revision=v_case.current_rule_revision;
        end if;
        select r.* into v_origin from private.bpay_next_case_create_request r where r.case_id=v_case.id and r.candidate_id=v_candidate;
        -- Refuse the immutable selected identity before qualification: a stale
        -- Source collection must not disappear into an ordinary review path.
        if exists(select 1 from private.bpay_next_work_collection c
          join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=c.candidate_id
          where c.case_id=v_case.id and c.case_component_id=v_component.id
            and c.candidate_id=v_candidate and w.work_kind='SOURCE') then
          raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
        end if;
        -- The exact original read qualification, in this STABLE snapshot.
  select b.* into v_collection from private.bpay_next_work_collection b
    join private.bpay_next_position p on p.work_id=b.work_id and p.component_key=b.component_key
    join private.bpay_next_work_revision r on r.id=p.applied_revision_id and r.work_id=b.work_id
    join private.bpay_next_work_revision original on original.id=b.original_revision_id and original.work_id=b.work_id
    join private.bpay_next_work work on work.id=b.work_id and work.candidate_id=b.candidate_id
    join private.bpay_next_approved_line original_line on original_line.id=b.original_approved_line_id
      and original_line.revision_id=original.id and original_line.component_key=b.component_key
    join private.bpay_next_finance_case c on c.id=b.case_id and c.candidate_id=b.candidate_id
    join private.bpay_next_case_component x on x.id=b.case_component_id and x.case_id=c.id and x.candidate_id=c.candidate_id
    join private.bpay_next_case_rule rule on rule.id=x.rule_id and rule.case_id=c.id and rule.candidate_id=c.candidate_id
    where b.case_id=v_case.id and b.case_component_id=v_component.id and b.candidate_id=v_candidate
      and b.id=b.case_id and b.id=b.case_component_id and b.paid_basis_qualified
      and b.qualification_state='READY' and b.original_source_pay_channel='PAYE' and b.original_tax_treatment='TAXABLE'
      and (b.observed_revision_id,b.observed_approved_source_ex_vat,b.observed_realised_source_ex_vat)
        is not distinct from (p.applied_revision_id,p.approved_source_ex_vat,p.realised_source_ex_vat)
      and p.source_basis_channel='PAYE' and p.approved_source_ex_vat>=0 and p.realised_source_ex_vat>=0
      and (p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat)
        is not distinct from (p.realised_source_ex_vat,0::numeric,p.realised_source_ex_vat)
      and ((work.work_kind='ORDINARY' and original.source_kind='ORDINARY' and r.source_kind='ORDINARY')
        or (work.work_kind='SOURCE' and original_line.component_kind='WORK'
          and original_line.source_component_id is not null
          and b.component_key='SOURCE:'||original_line.source_component_id::text
          and ((original.source_kind='SOURCE' and original.source_event_id is not null
            and (original.source_head_id is null or original.source_event_id=original.source_head_id)) or (original.source_kind='PROTECTED'
            and original.source_head_id is not null and original.source_event_id=original.source_head_id))
          and ((r.source_kind='SOURCE' and r.source_event_id is not null
            and (r.source_head_id is null or r.source_event_id=r.source_head_id)) or (r.source_kind='PROTECTED'
            and r.source_head_id is not null and r.source_event_id=r.source_head_id))))
      and r.source_pay_channel='PAYE' and r.sealed_at_utc is not null
      and ((r.detail_kind=original.detail_kind and exists(select 1 from private.bpay_next_approved_line a
        where a.revision_id=r.id and a.component_key=b.component_key and a.component_kind='WORK'
          and a.tax_treatment='TAXABLE' and a.source_pay_ex_vat=p.approved_source_ex_vat
          and (work.work_kind='ORDINARY' or a.source_component_id=original_line.source_component_id)))
        or (r.certified_zero and r.approved_source_ex_vat=0 and r.expected_line_count=0 and p.approved_source_ex_vat=0)
        or (b.segment_attribution_complete and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and (not b.segment_attribution_complete or (b.observed_segment_negative_ex_vat is not distinct from
        (private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'negative_source_ex_vat')::numeric
        and coalesce((private.bpay_next_segment_collection_basis_v1(b.work_id,b.component_key,r.id)->>'complete')::boolean,false)))
      and c.case_kind='OVERPAYMENT' and c.tax_treatment='TAXABLE' and c.principal_funded=0
      and x.case_kind='OVERPAYMENT' and x.case_subtype='OVERPAYMENT' and x.tax_treatment='TAXABLE'
      and x.source_pay_channel='PAYE' and x.instruction_kind='RECOVERY' and x.direction='DEDUCTION'
      and x.payroll_stage='GROSS_DEDUCT' and x.component_key='RECOVERY' and x.component_ordinal=1
      and c.current_rule_id=rule.id and c.current_rule_revision=rule.rule_revision and rule.id=b.id
      and rule.weekly_due_source_ex_vat is null
      and (c.principal_approved,c.principal_recovered,c.principal_written_off,c.active_recovery_hold_amount)
        is not distinct from (x.approved_source_ex_vat,x.recovered_source_ex_vat,x.written_off_source_ex_vat,x.active_recovery_source_ex_vat);

        v_automatic:=v_collection.id is not null;
        if v_state.weekly_binding_state='NOT_AFFECTED'
          and (v_case.case_kind,v_component.case_kind,v_component.instruction_kind,v_component.payroll_stage)
            is distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND';
        end if;
        if v_rule.id is null or v_case.case_revision<1 or (not v_memory_cut and v_component.rule_id<>v_rule.id) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if not v_automatic then
          v_amendment:=private.bpay_next_case_edit_origin_v1(v_rule.id);
          v_approved_principal:=case when v_amendment is null then v_origin.principal_source_ex_vat
            else (v_amendment->>'principal')::numeric end;
          if v_amendment is not null and ((v_amendment->>'original_command_id')::uuid is distinct from v_origin.command_id
              or (v_amendment->>'case_id')::uuid is distinct from v_case.id
              or (v_amendment->>'candidate_id')::uuid is distinct from v_candidate
              or v_amendment->'input'->>'tax_treatment' is distinct from v_component.tax_treatment) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if v_automatic then
          -- Genuine paid WORK origin, not a fabricated CASE_CREATE receipt.
          -- The immutable original paid policy and the exact applied position
          -- are authority; a newer queued WORK approval is not a reprice.
          if v_origin.command_id is not null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          v_policy_id:=v_collection.original_valuation_policy_id;
        elsif v_origin.command_id is null
           or (not v_memory_cut and v_component.rule_id<>v_rule.id) or v_origin.case_kind<>v_case.case_kind
           or v_origin.source_pay_channel<>v_component.source_pay_channel
           or v_approved_principal<>v_component.approved_source_ex_vat
           or (v_component.id<>v_origin.primary_component_id
             and (v_origin.recovery_component_id is null or v_component.id<>v_origin.recovery_component_id))
           or (v_component.id=v_origin.primary_component_id and v_component.instruction_kind<>
             case when v_case.case_kind in ('LOAN','ADVANCE') then 'PAYOUT'
               when v_case.case_kind='CREDIT' then 'CREDIT' else 'RECOVERY' end)
           or (v_component.id=v_origin.recovery_component_id and v_component.instruction_kind<>'RECOVERY') then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        else
          v_policy_id:=v_origin.valuation_policy_id;
        end if;
        if v_component.source_pay_channel<>'PAYE' or v_worker.target_pay_channel<>'PAYE' then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_TARGET_UNSUPPORTED';
        end if;
        v_explanation:='Exact approved case; same-channel PAYE current basis captured';
        v_outstanding:=case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
        if v_component.instruction_kind='RECOVERY' then
          if v_automatic then
            -- Retained automatic OVERPAYMENT policy has no instalment. Its
            -- remaining principal already excludes realised recovery and W;
            -- capacity below subtracts each disjoint active hold only once.
            v_nominal:=least(v_outstanding,v_case_outstanding);
            v_explanation:='Exact original ordinary PAYE collection; remaining outstanding, no instalment';
          else
          if v_rule.weekly_due_source_ex_vat is null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded>0
             and (v_rule.original_funding_event_id is null or v_rule.original_funded_at_utc is null) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          v_nominal:=case when v_state.pay_week_start>=v_rule.next_due_monday
            then least(v_rule.weekly_due_source_ex_vat,v_outstanding,v_case_outstanding) else 0 end;
          if v_case.case_kind in ('LOAN','ADVANCE') and v_case.principal_funded=0 then
            v_nominal:=0;v_explanation:='Approved loan/advance is unfunded; repayment not eligible';
          elsif v_state.pay_week_start<v_rule.next_due_monday then
            v_explanation:='Repayment schedule is not yet due in the pay-date week';
          end if;
          end if;
          v_purpose:=case when v_component.payroll_stage='GROSS_DEDUCT' then 'GROSS_RECOVERY' else 'NET_RECOVERY_CAPACITY' end;
        else
          v_outstanding:=v_component.approved_source_ex_vat-v_component.funded_source_ex_vat
            -v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat;
          v_case_outstanding:=v_case.principal_approved-v_case.principal_funded
            -v_case.principal_paid_credit-v_case.principal_written_off;
          v_nominal:=least(v_outstanding,v_case_outstanding);v_purpose:='PAYOUT';
        end if;
        -- Weekly due exists independently of this particular arrangement's
        -- eligibility. An early/paused selection must not make the whole
        -- week's legitimate opening due permanently zero for later pay dates.
        v_opening_due:=case when v_component.instruction_kind='RECOVERY' then v_nominal else 0 end;
        if v_case_status<>'OPEN' or (v_case_due is not null and v_case_due>v_run.pay_date) then
          v_nominal:=0;v_explanation:='Selected case is not currently open/due; no reservation';
        end if;
        if v_outstanding<0 or v_case_outstanding<0 or v_nominal<0 then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        -- Opening due is independent of later principal. Never subtract the
        -- same realised recovery twice or use max(overlapping counters).
        select p.* into v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start;
        v_period_operation:='KEEP';
        if not found then
          -- Prospective opening operands only; the writable owner persists them.
          v_period_operation:='INSERT';
          v_period.case_component_id:=v_component.id;v_period.case_id:=v_case.id;
          v_period.candidate_id:=v_candidate;v_period.pay_week_start:=v_state.pay_week_start;
          v_period.rule_id:=v_rule.id;v_period.period_revision:=1;
          v_period.opening_outstanding_source_ex_vat:=v_outstanding;
          v_period.opening_due_source_ex_vat:=v_opening_due;
          v_period.realised_recovery_source_ex_vat:=0;
          v_period.active_unrealised_recovery_source_ex_vat:=0;v_period.active_payout_source_ex_vat:=0;
          v_period.work_collection_id:=case when v_automatic then v_collection.id else null end;
        end if;
        if v_period.work_collection_id is distinct from (case when v_automatic then v_collection.id else null end) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        if v_period.rule_id<>v_rule.id then
          -- Genuine original funding advances an immutable loan rule solely
          -- to bind its age/evidence. It is NOT a schedule/policy reprice.
          select r.* into strict v_period_rule from private.bpay_next_case_rule r where r.id=v_period.rule_id;
          v_old_amendment:=case when v_period_rule.id=v_origin.rule_id then null
            else private.bpay_next_case_edit_origin_v1(v_period_rule.id) end;
          if v_amendment is not null and v_period_rule.original_funding_event_id is null
             and v_period_rule.rule_revision<v_rule.rule_revision
             and (v_period_rule.id=v_origin.rule_id or v_old_amendment is not null)
             and (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_created_at_utc)
                is not distinct from (v_case.id,v_candidate,v_origin.accepted_at_utc)
             and (v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric) then
            v_period_before:=to_jsonb(v_period);v_period_operation:='CASE_EDIT_OPENING';
            v_period.rule_id:=v_rule.id;v_period.period_revision:=v_period.period_revision+1;
            v_period.opening_outstanding_source_ex_vat:=v_outstanding;v_period.opening_due_source_ex_vat:=v_opening_due;
          else
          if v_case.case_kind not in ('LOAN','ADVANCE') or v_rule.original_funding_event_id is null
             or (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
               v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
               v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
               v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
               is distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
                 v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
                 v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
             or not exists(select 1 from private.bpay_next_case_event e where e.id=v_rule.original_funding_event_id
               and e.case_id=v_case.id and e.event_kind='FUNDED' and e.occurred_at_utc=v_rule.original_funded_at_utc)
             or (v_period_rule.original_funding_event_id is not null and
               (v_period_rule.original_funding_event_id,v_period_rule.original_funded_at_utc)
                 is distinct from (v_rule.original_funding_event_id,v_rule.original_funded_at_utc)) then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          end if;
          if v_component.instruction_kind='RECOVERY' and v_period_rule.original_funding_event_id is null then
            if (v_period.opening_outstanding_source_ex_vat,v_period.opening_due_source_ex_vat,
                v_period.realised_recovery_source_ex_vat,v_period.active_unrealised_recovery_source_ex_vat,v_period.active_payout_source_ex_vat)
                is distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
              raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
            end if;
            -- Only this exact zero/unconsumed pay-week is made ready. Its
            -- frozen old zero instructions/results remain immutable and zero.
            v_period_operation:='FUNDING_ZERO';
            v_period.rule_id:=v_rule.id;v_period.opening_outstanding_source_ex_vat:=v_outstanding;
            v_period.opening_due_source_ex_vat:=v_opening_due;v_period.period_revision:=v_period.period_revision+1;
          end if;
          end if; -- exact approved amendment or retained original funding-only branch
          -- A consumed funded period (or original payout period) retains its
          -- recorded opening rule and counters; only the new instruction's
          -- captured_rule_id/age advance to the genuine current funded rule.
        end if;
        v_capacity:=case when v_automatic then
          least(v_nominal,greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          when v_component.instruction_kind='RECOVERY' then
          least(v_nominal,greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat,0),
            greatest(v_outstanding-v_component.active_recovery_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_recovery_hold_amount,0))
          else least(v_nominal,greatest(v_outstanding-v_component.active_payout_source_ex_vat,0),
            greatest(v_case_outstanding-v_case.active_payout_hold_amount,0)) end;
        begin
          select x.* into strict v_value from private.bpay_next_value_source_for_target_v1(
            v_component.approved_source_ex_vat,'PAYE','PAYE',null,v_policy_id,v_run.pay_date) x;
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING','BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS') then
            raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
          else raise;end if;
        end;
        if v_value.target_ex_vat<>v_component.approved_source_ex_vat or v_value.target_vat<>0
           or v_value.target_inc_vat<>v_value.target_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';
        end if;
        v_resolution_ref:=case when v_automatic then 'WORK_COLLECTION:'||v_collection.id::text||':PAYE'
            when v_amendment is not null then 'CASE_EDIT:'||(v_amendment->>'command_id')||':PAYE'
            else 'CASE_CREATE:'||v_origin.command_id::text||':PAYE' end;
        -- Current estimate admission only. Accepted Draft eligibility is frozen;
        -- WB41 owns subsequent accepted WAIT branches in the master chain.
        if v_run_id is null and v_automatic and private.bpay_next_work_wait_state_v1(v_collection.work_id) then
          v_nominal:=0;v_capacity:=0;v_explanation:='Awaiting reauthorisation';
        end if;
        v_age:=coalesce(v_rule.original_funded_at_utc,v_rule.case_created_at_utc);
        -- A genuinely bound stored credit keeps its original creator age. The
        -- exact point sidecar is immutable, not a live legacy money read.
        if not v_automatic then
          select coalesce(o.original_created_at_utc,v_age) into v_age
            from (select 1) s left join private.bpay_next_stored_credit_origin o
              on o.command_id=v_component.id and o.candidate_id=v_candidate;
        end if;
        v_age_key:=(extract(epoch from v_age)*1000000)::bigint;
  return jsonb_build_object('period_operation',v_period_operation,'period',to_jsonb(v_period),
   'period_amendment',case when v_period_operation='CASE_EDIT_OPENING' then
     jsonb_build_object('edit_command_id',v_amendment->'command_id','before',v_period_before) else null end,
   'resolution',jsonb_build_object('target_ex_vat',v_value.target_ex_vat,'target_vat',0,
    'target_inc_vat',v_value.target_inc_vat,'valuation_policy_id',v_policy_id,
    'valuation_window_id',v_value.policy_window_id,'resolution_ref',v_resolution_ref),
   'instruction',jsonb_build_object('case_component_id',v_component.id,
'case_id',v_case.id,
'candidate_id',v_candidate,
'captured_case_revision',v_case.case_revision,
'rule_id',v_rule.id,
'rule_revision',v_rule.rule_revision,
'case_kind',v_component.case_kind,
'case_subtype',v_component.case_subtype,
'tax_treatment',v_component.tax_treatment,
'instruction_kind',v_component.instruction_kind,
'direction',v_component.direction,
'payroll_stage',v_component.payroll_stage,
'hold_purpose',v_purpose,
'age_key',v_age_key,
'debt_age_at_utc',v_age,
'component_ordinal',v_component.component_ordinal,
'pay_week_start',v_state.pay_week_start,
'source_pay_channel','PAYE',
'target_pay_channel','PAYE',
'allocation_channel','PAYE')||
jsonb_build_object('currency','GBP',
'nominal_source_ex_vat',v_nominal,
'nominal_target_ex_vat',v_nominal,
'nominal_target_vat',0,
'nominal_target_inc_vat',v_nominal,
'capacity_source_ex_vat',v_capacity,
'capacity_target_ex_vat',v_capacity,
'capacity_target_vat',0,
'capacity_target_inc_vat',v_capacity,
'minimum_earnings_threshold',v_rule.minimum_earnings_threshold,
'take_home_floor_override',v_rule.take_home_floor_override,
'captured_default_floor',v_state.captured_default_floor,
'captured_default_floor_revision',v_state.captured_default_floor_revision,
'resolved_take_home_floor',coalesce(v_rule.take_home_floor_override,v_state.captured_default_floor),
'valuation_policy_id',v_policy_id,
'valuation_window_id',v_value.policy_window_id,
'resolution_ref',v_resolution_ref,
'explanation',v_explanation,
'work_collection_id',case when v_automatic then v_collection.id else null end));
 elsif v_mode='ALLOCATE' then
  v_keys:=array['mode','instructions','channel_headroom','worker_take_home'];
  if not(p_scope ?& v_keys) or p_scope-v_keys<>'{}'::jsonb
   or jsonb_typeof(p_scope->'instructions') is distinct from 'array'
   or jsonb_array_length(p_scope->'instructions')>1000 then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  v_e:=(p_scope->>'channel_headroom')::numeric;v_h:=(p_scope->>'worker_take_home')::numeric;
  if v_e is null or v_h is null or v_e<0 or v_e::text in ('NaN','Infinity','-Infinity')
   or v_h::text in ('NaN','Infinity','-Infinity') or v_e<>trunc(v_e,2) or v_h<>trunc(v_h,2) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
  if exists(select 1 from jsonb_array_elements(p_scope->'instructions') x
    group by x->>'case_component_id' having count(*)>1) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_DUPLICATE';end if;
  for v_instruction in select i.* from jsonb_populate_recordset(null::private.bpay_next_run_case_instruction,p_scope->'instructions') i
   order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id loop
   if v_instruction.case_component_id is null or v_instruction.case_id is null or v_instruction.age_key is null
    or v_instruction.component_ordinal is null or v_instruction.allocation_channel is distinct from 'PAYE'
    or v_instruction.nominal_target_ex_vat is null or v_instruction.capacity_target_ex_vat is null
    or v_instruction.nominal_target_ex_vat<0 or v_instruction.capacity_target_ex_vat<0 then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_INSTRUCTION_INVALID';end if;
   if v_instruction.hold_purpose='PAYOUT' then
    v_taken:=least(v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat);
    v_cap_reason:=null;v_affordability_reason:=null;
    v_capacity_reason:=case when v_instruction.capacity_target_ex_vat<v_instruction.nominal_target_ex_vat then 'CASE_CAPACITY' else null end;
    v_next_e:=v_e;v_next_h:=v_h;
   else
    select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
     v_instruction.nominal_target_ex_vat,v_instruction.capacity_target_ex_vat,v_e,v_h,
     v_instruction.minimum_earnings_threshold,v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
    v_taken:=v_row.taken_amount;v_cap_reason:=v_row.cap_reason;v_affordability_reason:=v_row.affordability_reason;
    v_capacity_reason:=v_row.capacity_reason;v_next_e:=v_row.next_channel_headroom;v_next_h:=v_row.next_worker_take_home;
   end if;
   v_rows:=v_rows||jsonb_build_array(jsonb_build_object('instruction',to_jsonb(v_instruction),
    'taken_amount',v_taken,'cap_reason',v_cap_reason,'affordability_reason',v_affordability_reason,'capacity_reason',v_capacity_reason,
    'channel_headroom_before',v_e,'channel_headroom_after',v_next_e,'worker_take_home_before',v_h,'worker_take_home_after',v_next_h));
   v_e:=v_next_e;v_h:=v_next_h;
  end loop;
  return jsonb_build_object('rows',v_rows,'channel_headroom_after',v_e,'worker_take_home_after',v_h);
 else raise exception using errcode='22023',message='BPAY_NEXT_CASE_PLAN_SCOPE_INVALID';end if;
end
$function$;

create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_plan jsonb;v_plan_period jsonb;v_plan_row jsonb;v_plan_instructions jsonb;
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_saved_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_case_selection%rowtype;
  v_state private.bpay_next_case_allocation_state%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_period_rule private.bpay_next_case_rule%rowtype;
  v_origin private.bpay_next_case_create_request%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_due date;v_case_status text;
  v_automatic boolean;v_week_not_affected boolean;
  v_period private.bpay_next_case_period%rowtype;
  v_item record;v_prior record;v_value record;v_row record;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_floor numeric;v_outstanding numeric;v_case_outstanding numeric;
  v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_taken numeric;v_e numeric;v_h numeric;
  v_next_e numeric;v_next_h numeric;
  v_gross numeric;v_week_start date;v_age timestamptz;v_age_key bigint;
  v_purpose text;v_explanation text;v_issue text;v_seen bigint:=0;
  v_result_id uuid;v_hold_id uuid;v_instruction_id uuid;v_has_more boolean;v_terminal boolean:=false;
  v_cap_reason text;v_affordability_reason text;v_capacity_reason text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_expected_cursor<0 or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PREPARE_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id),j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update nowait;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update nowait;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update nowait;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update nowait;
  if v_job.module_epoch<>v_epoch then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_OWNER_STALE';
  end if;
  if v_job.status='DONE' and v_job.phase='READY' and v_state.status='COMPLETE'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_worker.status='REVIEW'
     and v_worker.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_control.active_owner_epoch<>p_owner_epoch
     or v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_STATE_INVALID';
  end if;
  if v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
     or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
     or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
       and work_collection_id is not null and not managed_capture_completed) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_LINE_COUNT_MISMATCH';
  end if;
  if v_job.cursor_key is not null and v_job.cursor_key !~ '^[0-9]{1,19}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CURSOR_CORRUPT';
  end if;
  v_saved_cursor:=coalesce(v_job.cursor_key::bigint,0);
  if coalesce(p_expected_cursor,0)<v_saved_cursor then
    -- Lost STEP reply: read back the persisted checkpoint without a second
    -- capture, hold, cursor spend or financial-view bump. Ahead is not replay.
    return pg_catalog.jsonb_build_object('done',false,'phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,
      'rows_visited','0','replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if coalesce(p_expected_cursor,0)>v_saved_cursor then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_CURSOR_STALE';
  end if;
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate)
     and (not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
       or (v_run.accepted_choice_descriptor is not null
         and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_candidate)
         and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
           and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_candidate))) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    -- A resumed weekly/capture/allocation page uses its exact frozen selection
    -- revision. Refuse Source origins before any stage writes, including when
    -- the collection's current qualification reader would return no row.
    if v_state.id is not null and exists(
      select 1 from private.bpay_next_case_selection_item si
      join private.bpay_next_work_collection c on c.case_id=si.case_id
        and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
      where si.run_id=v_run_id and si.candidate_id=v_candidate
        and si.selection_revision=v_state.selection_revision and si.is_selected
        and w.work_kind='SOURCE') then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
    end if;
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      -- Comparison is owned by the later CM chain. Existing immutable Source
      -- origin identity is enough to refuse it; no absent pack column/type or
      -- fabricated financial certificate is consulted or created here.
      if exists(select 1 from private.bpay_next_case_selection_item si
        join private.bpay_next_work_collection c on c.case_id=si.case_id
          and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
        join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
        where si.run_id=v_run_id and si.candidate_id=v_candidate
          and si.selection_revision=v_selection.selection_revision and si.is_selected
          and w.work_kind='SOURCE') then
        raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
      end if;
      if v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
         or v_worker.gross_ex_vat<>v_worker.gross_inc_vat then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select selected.min_take_home_wtd into strict v_floor from private.bpay_next_selection_candidate selected
        where selected.run_id=v_run_id and selected.candidate_id=v_candidate;
      if v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity') or v_floor<0
         or v_floor<>pg_catalog.trunc(v_floor,2) then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      v_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
      -- OVERPAYMENT gross recovery does not consume the protected-debt H/floor
      -- operand (0290). Prove the exact nonempty SEALED selected set, not merely
      -- that no protected row happened to be found. Missing identities/counts
      -- remain week-dependent; instruction capture and allocation re-prove it.
      select count(*)=v_selection.selected_count and count(*)>0
        and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
        into v_week_not_affected
        from private.bpay_next_case_selection_item si
        left join private.bpay_next_case_component cc
          on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
        left join private.bpay_next_finance_case fc on fc.id=si.case_id and fc.candidate_id=si.candidate_id
        where si.run_id=v_selection.run_id and si.candidate_id=v_selection.candidate_id
          and si.selection_revision=v_selection.selection_revision and si.is_selected;
      -- Exact indexed metadata absence is required BEFORE establishing a new
      -- zero week. Missing prior NEW facts are unresolved, not zero/history SUM.
      if not v_week_not_affected and exists(select 1 from private.bpay_next_run_worker w
        join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_week_start and r.pay_date<v_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
      end if;
      insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
        default_floor,default_floor_revision,resolved_arranged_take_home)
        values(v_candidate,v_week_start,1,v_floor,1,0) on conflict(candidate_id,pay_week_start) do nothing;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_week_start for update;
      if not v_week_not_affected and v_week.unresolved_contribution_count<>0 then v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;end if;
      if v_week.default_floor<>v_floor then
        update private.bpay_next_worker_week set default_floor=v_floor,default_floor_revision=default_floor_revision+1,
          period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_week_start returning * into v_week;
      end if;
      update private.bpay_next_run_worker set case_selection_revision=v_selection.selection_revision where id=v_worker.id;
      v_worker.case_selection_revision:=v_selection.selection_revision;
      insert into private.bpay_next_case_allocation_state
        (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,pass_kind,projection_no,
         command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
         expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,prepare_stage,
         captured_default_floor,captured_default_floor_revision,pay_week_start,captured_week_revision,captured_work_gross)
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.captured_preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.captured_preparation_revision
         or v_state.selection_revision<>v_worker.case_selection_revision or v_state.prepare_stage='NOT_CASE_PREPARE' then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_BINDING_MISMATCH';
      end if;
      -- Lease takeover does not reset the captured economic cursors/headroom.
      if v_state.owner_epoch<>p_owner_epoch then
        update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
        v_state.owner_epoch:=p_owner_epoch;
      end if;
    end if;

    if v_state.prepare_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            <(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or
            (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
              >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID')
            then 'CASE_RETURN_FLOOR_UNBOUND' else 'CASE_WEEK_BASIS_UNBOUND' end;
          exit prepare_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;
        v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_has_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_has_more then 'UNBOUND' else 'BOUND' end,
        prepare_stage=case when v_has_more then 'WEEK_CAPTURE' else 'INSTRUCTION_CAPTURE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.prepare_stage='INSTRUCTION_CAPTURE' then
      for v_item in select i.* from private.bpay_next_case_selection_item i
        where i.run_id=v_run_id and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision
          and i.is_selected and i.selection_no>v_state.instruction_capture_cursor order by i.selection_no limit p_limit
      loop
        v_seen:=v_seen+1;
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        -- Original Case/component locks precede the original period lock.
        perform 1 from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        begin
          v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
            'candidate_id',v_candidate,'case_id',v_case.id,'case_component_id',v_component.id,
            'pay_date',v_run.pay_date,'pay_week_start',v_state.pay_week_start,
            'target_pay_channel',v_worker.target_pay_channel,'captured_default_floor',v_state.captured_default_floor,
            'captured_default_floor_revision',v_state.captured_default_floor_revision,
            'weekly_binding_state',v_state.weekly_binding_state,'accepted_run_id',v_run_id));
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_CASE_INPUT_UNBOUND','BPAY_NEXT_CASE_TARGET_UNSUPPORTED','BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND') then
            v_issue:=substring(sqlerrm from 11);exit prepare_page;
          else raise;end if;
        end;
        v_plan_period:=v_plan->'period';
        if v_plan->>'period_operation'='INSERT' then
          insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
            period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
            values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,(v_plan_period->>'rule_id')::uuid,
              (v_plan_period->>'period_revision')::bigint,(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
              (v_plan_period->>'opening_due_source_ex_vat')::numeric,(v_plan_period->>'work_collection_id')::uuid)
            on conflict(case_component_id,pay_week_start) do nothing;
        end if;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_plan->>'period_operation'='CASE_EDIT_OPENING' then
          perform private.bpay_next_apply_case_period_amendment_v1(
            (v_plan->'period_amendment'->>'edit_command_id')::uuid,v_plan->'period_amendment'->'before',v_plan_period);
          select amended_period.* into strict v_period from private.bpay_next_case_period amended_period
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start;
        end if;
        if v_plan->>'period_operation'='FUNDING_ZERO' then
          update private.bpay_next_case_period set rule_id=(v_plan_period->>'rule_id')::uuid,
            opening_outstanding_source_ex_vat=(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
            opening_due_source_ex_vat=(v_plan_period->>'opening_due_source_ex_vat')::numeric,period_revision=period_revision+1
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start
              and period_revision=(v_plan_period->>'period_revision')::bigint-1
              and (opening_outstanding_source_ex_vat,opening_due_source_ex_vat,realised_recovery_source_ex_vat,
                active_unrealised_recovery_source_ex_vat,active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) returning * into v_period;
          if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if to_jsonb(v_period) is distinct from v_plan_period then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=(v_plan->'resolution'->>'target_ex_vat')::numeric,target_vat=(v_plan->'resolution'->>'target_vat')::numeric,
          target_inc_vat=(v_plan->'resolution'->>'target_inc_vat')::numeric,
          valuation_policy_id=(v_plan->'resolution'->>'valuation_policy_id')::uuid,
          valuation_window_id=(v_plan->'resolution'->>'valuation_window_id')::uuid,
          resolution_ref=v_plan->'resolution'->>'resolution_ref',component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_plan:=jsonb_set(v_plan,'{instruction}',(v_plan->'instruction')||jsonb_build_object(
          'run_worker_id',v_worker.id,'run_id',v_run_id,'candidate_id',v_candidate,
          'selection_revision',v_state.selection_revision,'preparation_revision',v_worker.captured_preparation_revision,
          'captured_case_revision',v_case.case_revision,'captured_component_revision',v_component.component_revision,
          'captured_period_revision',v_period.period_revision));
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id)
          select planned.run_worker_id,planned.run_id,planned.candidate_id,planned.selection_revision,planned.preparation_revision,planned.case_component_id,planned.case_id,planned.captured_case_revision,planned.captured_component_revision,planned.captured_period_revision,planned.rule_id,planned.rule_revision,planned.case_kind,planned.case_subtype,planned.tax_treatment,planned.instruction_kind,planned.direction,planned.payroll_stage,planned.hold_purpose,planned.age_key,planned.debt_age_at_utc,planned.component_ordinal,planned.pay_week_start,planned.source_pay_channel,planned.target_pay_channel,planned.allocation_channel,planned.currency,planned.nominal_source_ex_vat,planned.nominal_target_ex_vat,planned.nominal_target_vat,planned.nominal_target_inc_vat,planned.capacity_source_ex_vat,planned.capacity_target_ex_vat,planned.capacity_target_vat,planned.capacity_target_inc_vat,planned.minimum_earnings_threshold,planned.take_home_floor_override,planned.captured_default_floor,planned.captured_default_floor_revision,planned.resolved_take_home_floor,planned.valuation_policy_id,planned.valuation_window_id,planned.resolution_ref,planned.explanation,planned.work_collection_id from jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan->'instruction') planned
          returning id into v_instruction_id;
        perform private.bpay_next_capture_instruction_destination_v1(v_instruction_id);
        v_state.instruction_capture_cursor:=v_item.selection_no;
        v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
        if v_component.payroll_stage='NET_ADD' then v_state.payout_instruction_count:=v_state.payout_instruction_count+1;end if;
        update private.bpay_next_run_worker set case_instruction_count=case_instruction_count+1 where id=v_worker.id;
      end loop;
      select exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=v_run_id
        and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision and i.is_selected
        and i.selection_no>v_state.instruction_capture_cursor) into v_has_more;
      if not v_has_more and v_state.captured_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CAPTURE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_case_allocation_state set instruction_capture_cursor=v_state.instruction_capture_cursor,
        captured_instruction_count=v_state.captured_instruction_count,payout_instruction_count=v_state.payout_instruction_count,
        prepare_stage=case when v_has_more then 'INSTRUCTION_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.weekly_binding_state='NOT_AFFECTED' then
      -- Only frozen instructions are authoritative from this point. This
      -- scalar metadata proof is scoped to this exact run worker/revisions;
      -- it never reconstructs money or reads prior payment history.
      select count(*)=v_state.expected_instruction_count and count(*)>0
        and coalesce(bool_and((i.case_kind,i.instruction_kind,i.payroll_stage,i.hold_purpose)
          is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
          and si.case_id is not null),false)
        into v_week_not_affected
        from private.bpay_next_run_case_instruction i
        left join private.bpay_next_case_selection_item si
          on si.run_id=i.run_id and si.candidate_id=i.candidate_id and si.selection_revision=i.selection_revision
            and si.case_id=i.case_id and si.case_component_id=i.case_component_id and si.is_selected
        where i.run_worker_id=v_worker.id and i.run_id=v_run_id and i.candidate_id=v_candidate
          and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision;
      if not v_week_not_affected
        or v_state.captured_instruction_count<>v_state.expected_instruction_count
        or v_worker.case_instruction_count<>v_state.expected_instruction_count
        or not exists(select 1 from private.bpay_next_case_selection cs where cs.run_id=v_run_id
          and cs.candidate_id=v_candidate and cs.selection_revision=v_state.selection_revision
          and cs.status='SEALED' and cs.selected_count=v_state.expected_instruction_count) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
      end if;
    end if;
    if v_state.prepare_stage<>'ALLOCATE' or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
    end if;
    select c.* into strict v_channel from private.bpay_next_case_allocation_channel c
      where c.state_id=v_state.id and c.allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    select coalesce(jsonb_agg(page.value),'[]'::jsonb) into v_plan_instructions from (
      select to_jsonb(i) as value from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.captured_preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    ) page;
    v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
      'instructions',v_plan_instructions,'channel_headroom',v_e,'worker_take_home',v_h));
    for v_plan_row in select value from jsonb_array_elements(v_plan->'rows') loop
      v_instruction:=jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan_row->'instruction');
      v_seen:=v_seen+1;
      v_taken:=(v_plan_row->>'taken_amount')::numeric;v_cap_reason:=v_plan_row->>'cap_reason';
      v_affordability_reason:=v_plan_row->>'affordability_reason';v_capacity_reason:=v_plan_row->>'capacity_reason';
      v_next_e:=(v_plan_row->>'channel_headroom_after')::numeric;v_next_h:=(v_plan_row->>'worker_take_home_after')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.captured_preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT','PAYE',v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,
         v_cap_reason,v_affordability_reason,v_capacity_reason,v_e,v_next_e,v_h,v_next_h)
        returning id into v_result_id;
      if v_taken>0 then
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_instruction.case_id for update;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_instruction.case_component_id for update;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_instruction.pay_week_start for update;
        insert into private.bpay_next_case_hold
          (run_worker_id,candidate_id,case_id,amount,status,instruction_id,allocation_result_id,allocation_pass_kind,
           case_component_id,purpose,pay_week_start,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_worker.id,v_candidate,v_case.id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_taken,v_taken,0,v_taken) returning id into v_hold_id;
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
      elsif v_instruction.payroll_stage='NET_ADD' then v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.captured_preparation_revision,'GBP','CANDIDATE',v_candidate,
            v_state.payout_instruction_count,v_state.payout_instruction_count,v_state.allocated_net_additions,
            v_state.allocated_net_additions,0,v_state.allocated_net_additions,'SEALED');
      end if;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_state.pay_week_start for update;
      insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,original_run_worker_id,
        original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,
        payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision)
        values(v_candidate,v_state.pay_week_start,v_worker.id,v_run.pay_date,v_run.created_at_utc,1,'GROSS_FALLBACK',
          v_gross,'ARRANGED',case when v_gross>0 then 'ELIGIBLE' else 'EXCLUDED' end,v_gross,1);
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home+v_gross,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_case_allocation_state set status='COMPLETE',prepare_stage='COMPLETE',
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=0,gross_inc_vat=v_gross,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;

      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set retained_prepare_stage=v_state.prepare_stage,retained_weekly_binding_state=v_state.weekly_binding_state,
        prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;

    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  if v_terminal then
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen::integer);
  end if;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  if p_kind in ('CASE_LIST','CASE_SUMMARY') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_list_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args ? 'case_id' or p_args ? 'expected_case_revision' then
      if jsonb_typeof(p_args) is distinct from 'object' or p_args-array['input','case_id','expected_case_revision']<>'{}'::jsonb
        or not(p_args ?& array['input','case_id','expected_case_revision'])
        or jsonb_typeof(p_args->'case_id') is distinct from 'string'
        or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
        or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
        or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      return jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id,
        (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true);
    end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
      ||case when p_args ? 'edit_command_id' then array['edit_command_id'] else array[]::text[] end
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='PROGRESS' then
    v_progress:=private.bpay_next_preparation_progress_v1((p_args->>'run_id')::uuid);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    v_progress:=private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
    if p_args ? 'edit_command_id' then
      if jsonb_typeof(p_args->'edit_command_id') is distinct from 'string'
        or p_args->>'edit_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_edit_receipt_v1((p_args->>'edit_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('edit_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    return v_progress;
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code,
        'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        left join private.bpay_next_preparation_discard_worker x on x.run_id=(p_args->>'run_id')::uuid and x.candidate_id=w.candidate_id
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;

create or replace function private.bpay_next_current_case_component_page_v1(
  p_candidate_id uuid,p_case_id uuid,p_after_component_ordinal bigint,p_limit integer
) returns jsonb language plpgsql volatile security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_row record;v_rows jsonb:='[]'::jsonb;v_count integer:=0;
  v_bytes integer:=512;v_more boolean:=false;v_pay_week date;v_date_context jsonb;
begin
  if p_candidate_id is null or p_case_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_component_ordinal is not null and p_after_component_ordinal<0) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if not exists(select 1 from private.bpay_next_finance_case c where c.id=p_case_id
      and c.candidate_id=p_candidate_id) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';
  end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DATE_UNAVAILABLE';
  end if;
  v_pay_week:=pg_catalog.date_trunc('week',(v_date_context->>'current_official_pay_date')::date)::date;
  for v_row in
    select pg_catalog.to_jsonb(page) as row_value from (
      select c.id as case_component_id,c.case_id,c.candidate_id,c.component_ordinal::text,
        c.component_revision::text,c.case_kind,c.case_subtype,c.tax_treatment,
        c.instruction_kind,c.direction,c.payroll_stage,c.source_pay_channel,c.resolution_state,
        c.approved_source_ex_vat::text,c.funded_source_ex_vat::text,c.recovered_source_ex_vat::text,
        c.written_off_source_ex_vat::text,c.active_payout_source_ex_vat::text,c.active_recovery_source_ex_vat::text,
        c.rule_id,fc.current_rule_id,r.rule_revision::text,
        fc.principal_approved::text as case_principal_approved,
        r.weekly_due_source_ex_vat::text,r.minimum_earnings_threshold::text,r.take_home_floor_override::text,
        r.schedule_start_monday,r.next_due_monday,r.schedule_week_count::text,
        (case when c.direction='DEDUCTION' then
          case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end
          -c.recovered_source_ex_vat-c.written_off_source_ex_vat
          else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end)::text as outstanding_source_ex_vat,
        (p.opening_due_source_ex_vat-p.realised_recovery_source_ex_vat-p.active_unrealised_recovery_source_ex_vat)::text as amount_due,
        null::text as recoverable_this_run,
        private.bpay_next_case_edit_state_v1(c.case_id,c.candidate_id) as edit_state,null::jsonb as edit_receipt,
        (select coalesce(amended.input->>'original_liability_reference',cr.original_liability_reference) from private.bpay_next_case_create_request cr
         left join private.bpay_next_case_edit_request amended on amended.new_rule_id=fc.current_rule_id
         where cr.case_id=c.case_id and cr.candidate_id=c.candidate_id
          and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT'
          and c.case_kind='MANUAL_DEBT' and c.case_subtype='MANUAL_DEBT') as original_liability_reference
      from private.bpay_next_case_component c
      join private.bpay_next_finance_case fc on fc.id=c.case_id and fc.candidate_id=c.candidate_id
      left join private.bpay_next_case_rule r on r.id=fc.current_rule_id and r.case_id=c.case_id
        and r.candidate_id=c.candidate_id and r.rule_revision=fc.current_rule_revision
      left join private.bpay_next_case_period p on p.case_component_id=c.id and p.case_id=c.case_id
        and p.candidate_id=c.candidate_id and p.pay_week_start=v_pay_week and p.rule_id=c.rule_id
      where c.case_id=p_case_id and c.candidate_id=p_candidate_id
        and c.component_ordinal>coalesce(p_after_component_ordinal,0)
      order by c.component_ordinal limit p_limit
    ) page
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;

create or replace function private.bpay_next_case_list_row_v1(p_case_id uuid,p_context jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_case private.bpay_next_finance_case%rowtype;v_key private.bpay_next_case_list_key%rowtype;
 v_rule private.bpay_next_case_rule%rowtype;v_component private.bpay_next_case_component%rowtype;
 v_component_rule private.bpay_next_case_rule%rowtype;v_period private.bpay_next_case_period%rowtype;
 v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_binding record;
 v_quote record;v_origin_policy uuid;v_basis bytea;v_snooze record;v_result jsonb;v_component_ids uuid[];
 v_today date:=(p_context->>'local_date')::date;v_week date:=(p_context->>'pay_week_start')::date;
 v_remaining numeric;v_due numeric:=0;v_payout numeric:=0;v_outstanding numeric;v_hold numeric:=0;
 v_components integer:=0;v_unknown_due boolean:=false;v_open_tax boolean:=false;v_open_non_tax boolean:=false;
 v_unresolved boolean:=false;v_stale boolean:=false;v_stale_unknown boolean:=false;v_gap text;
 v_active_snoozes integer:=0;v_stale_snoozes integer:=0;v_indefinite integer:=0;v_until date;
 v_pending boolean:=false;v_hold_text text;v_run uuid;v_channel text;
begin
 select c.* into strict v_case from private.bpay_next_finance_case c where c.id=p_case_id;
 select k.* into strict v_key from private.bpay_next_case_list_key k where k.case_id=p_case_id;
 select r.* into v_rule from private.bpay_next_case_rule r where r.id=v_case.current_rule_id and r.case_id=v_case.id;
 select m.* into v_metadata from private.bpay_next_workbench_candidate_metadata m where m.candidate_id=v_case.candidate_id;
 if v_key.created_at_utc is null then v_gap:='CASE_CREATED_UNAVAILABLE';
 elsif v_key.case_subtype is null or v_key.case_subtype not in ('LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT') then v_gap:='CASE_TYPE_UNMAPPED';
 elsif v_rule.id is null then v_gap:='CASE_RULE_UNAVAILABLE';end if;
 if v_key.client_association='UNKNOWN' then v_gap:=coalesce(v_gap,'CASE_CLIENT_UNAVAILABLE');end if;
 v_outstanding:=case when v_case.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
  else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off-v_case.principal_paid_credit;
 select original_valuation_policy_id into v_origin_policy from private.bpay_next_work_collection where case_id=p_case_id;
 if not found then select valuation_policy_id into v_origin_policy from private.bpay_next_case_create_request where case_id=p_case_id;end if;
 -- Current native producers create one component, or the two instructions of
 -- one Payment Advance. Inspect three keys to detect unsupported membership;
 -- never calculate or publish a prefix total.
 select array_agg(child.id order by child.component_ordinal) into v_component_ids from
  (select id,component_ordinal from private.bpay_next_case_component where case_id=p_case_id order by component_ordinal limit 3) child;
 if cardinality(v_component_ids)>2 then
  v_gap:=coalesce(v_gap,'CASE_COMPONENT_MEMBERSHIP_UNAVAILABLE');
  v_unknown_due:=true;v_payout:=null;v_hold:=v_case.active_hold_amount;
  v_open_tax:=null;v_open_non_tax:=null;v_unresolved:=null;v_stale_unknown:=true;
  v_active_snoozes:=null;v_stale_snoozes:=null;v_indefinite:=null;
 end if;
 for v_component in select component.* from private.bpay_next_case_component component
  where component.case_id=p_case_id and cardinality(v_component_ids)<=2 and component.id=any(v_component_ids)
  order by component.component_ordinal
 loop
  v_components:=v_components+1;
  v_remaining:=case when v_component.direction='DEDUCTION' then
    case when v_component.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat else v_component.approved_source_ex_vat end
      -v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat
   else v_component.approved_source_ex_vat-v_component.funded_source_ex_vat-v_component.paid_credit_source_ex_vat-v_component.written_off_source_ex_vat end;
  v_hold:=v_hold+v_component.active_payout_source_ex_vat+v_component.active_recovery_source_ex_vat;
  if v_component.direction='PAYMENT' then v_payout:=v_payout+greatest(v_remaining-v_component.active_payout_source_ex_vat,0);
  else
   select period.* into v_period from private.bpay_next_case_period period where period.case_component_id=v_component.id
    and period.case_id=p_case_id and period.candidate_id=v_case.candidate_id and period.pay_week_start=v_week and period.rule_id=v_component.rule_id;
   if not found and v_remaining>0 then v_unknown_due:=true;
   elsif found then v_due:=v_due+greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat-v_period.active_unrealised_recovery_source_ex_vat,0);end if;
  end if;
  if v_remaining>0 and v_case.status in ('OPEN','PAUSED') then
   if v_component.tax_treatment='TAXABLE' then
    v_open_tax:=true;
    if v_component.resolution_state='REVIEW' then v_unresolved:=true;
    elsif v_component.resolution_state='RESOLVED' then
     if v_origin_policy is null or v_metadata.target_pay_channel is null
      or (v_metadata.target_pay_channel='UMBRELLA' and (v_metadata.target_umbrella_enabled is distinct from true or v_metadata.target_umbrella_vat_chargeable is null)) then
      v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');
     else
      begin
       select * into strict v_quote from private.bpay_next_value_source_for_target_v1(v_component.approved_source_ex_vat,
        v_component.source_pay_channel,v_metadata.target_pay_channel,v_metadata.target_umbrella_vat_chargeable,
        v_origin_policy,(p_context->>'official_pay_date')::date);
       v_stale:=v_stale or (v_component.target_pay_channel,v_component.valuation_policy_id,v_component.valuation_window_id,
         v_component.target_ex_vat,v_component.target_vat,v_component.target_inc_vat)
        is distinct from (v_metadata.target_pay_channel,v_origin_policy,v_quote.policy_window_id,
         v_quote.target_ex_vat,v_quote.target_vat,v_quote.target_inc_vat);
      exception when check_violation or no_data_found or too_many_rows then
       v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');
      end;
     end if;
    else v_stale_unknown:=true;v_gap:=coalesce(v_gap,'CASE_RESOLUTION_UNAVAILABLE');end if;
   elsif v_component.tax_treatment='NON_TAXABLE' then v_open_non_tax:=true;
   elsif v_component.tax_treatment<>'NOT_APPLICABLE' then v_gap:=coalesce(v_gap,'CASE_TAX_UNMAPPED');end if;
  end if;
  select r.* into v_component_rule from private.bpay_next_case_rule r where r.id=v_component.rule_id and r.case_id=p_case_id;
  if v_component_rule.id is not null then
   -- Exact existing WB36 Case basis recipe; not a new resolution proof.
   v_basis:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(v_case.candidate_id,v_component.case_id,v_component.id,
    v_component.case_kind,v_component.case_subtype,v_component.tax_treatment,v_component.instruction_kind,v_component.direction,
    v_component.payroll_stage,v_component.source_pay_channel,v_component.approved_source_ex_vat,v_component.funded_source_ex_vat,
    v_component_rule.minimum_earnings_threshold,v_component_rule.take_home_floor_override,v_component_rule.weekly_due_source_ex_vat,
    v_component_rule.schedule_start_monday,v_component_rule.next_due_monday,v_component_rule.schedule_week_count)::text,'UTF8'));
   for v_snooze in select * from private.bpay_next_snooze where case_component_id=v_component.id and case_id=p_case_id
    and candidate_id=v_case.candidate_id and scope_identity=v_component.id::text
    and scope_kind='CASE' and active and (until_date is null or until_date>=v_today)
   loop
    if v_snooze.worker_pay_basis=v_basis then
     v_active_snoozes:=v_active_snoozes+1;
     if v_snooze.until_date is null then v_indefinite:=v_indefinite+1;else v_until:=least(v_until,v_snooze.until_date);end if;
    else v_stale_snoozes:=v_stale_snoozes+1;end if;
   end loop;
  end if;
 end loop;
 if v_components=0 then v_gap:=coalesce(v_gap,'CASE_COMPONENTS_UNAVAILABLE');v_unknown_due:=true;end if;
 select * into v_binding from private.bpay_next_case_protected_bindings_v1(p_case_id,null,1);
 if found then
  v_pending:=v_binding.binding_kind='PENDING';v_run:=v_binding.run_id;v_channel:=v_binding.channel;
  v_hold_text:=case when v_pending then 'Being prepared' when v_channel='PAYE' then 'In PAYE Draft'
   when v_channel='UMBRELLA' then 'In Umbrella Draft' else 'In a Draft' end;
 elsif v_hold>0 then v_hold_text:='Reserved for payment';end if;
 v_result:=jsonb_build_object('case_id',v_case.id,'candidate_id',v_case.candidate_id,
  'candidate_name',v_key.candidate_display_name,'candidate_reference',v_key.candidate_reference,
  'client_id',v_key.client_id,'client_name',v_key.client_display_name,'client_association',v_key.client_association,
  'case_kind',v_case.case_kind,'case_subtype',v_key.case_subtype,'case_type_label',v_key.case_type_label,
  'original_liability_reference',(select coalesce(amended.input->>'original_liability_reference',cr.original_liability_reference) from private.bpay_next_case_create_request cr
   left join private.bpay_next_case_edit_request amended on amended.new_rule_id=v_case.current_rule_id
   where cr.case_id=v_case.id and cr.candidate_id=v_case.candidate_id
    and cr.case_kind='MANUAL_DEBT' and cr.case_subtype='MANUAL_DEBT'
    and v_case.case_kind='MANUAL_DEBT' and v_key.case_subtype='MANUAL_DEBT'),
  'tax_treatment',v_case.tax_treatment,'tax_label',case when v_key.case_subtype is null or v_key.case_type_label='Unmapped case' then 'Tax treatment unavailable' when v_open_tax and v_open_non_tax then 'Mixed tax treatment'
   when v_case.tax_treatment='TAXABLE' then 'Taxable' when v_case.tax_treatment='NON_TAXABLE' then 'Non-taxable'
   when v_case.tax_treatment='NOT_APPLICABLE' then 'Not applicable' else 'Tax treatment unavailable' end,
  'status',v_case.status,'created_at_utc',v_key.created_at_utc,'case_revision',v_case.case_revision::text,
  'principal_approved',v_case.principal_approved::text,'principal_funded',v_case.principal_funded::text,
  'principal_recovered',v_case.principal_recovered::text,'principal_written_off',v_case.principal_written_off::text,
  'outstanding',v_outstanding::text,'pending_payout',v_payout::text,
  'amount_due',case when v_unknown_due then null else v_due::text end,'recoverable_this_run',null,
  'active_hold_amount',v_hold::text,'active_hold_explanation',v_hold_text,'protected_run_id',v_run,
  'protected_channel',v_channel,'input_pending',v_pending,
  'due_date',v_case.due_date,'weekly_due',v_rule.weekly_due_source_ex_vat::text,
  'schedule_start_monday',v_rule.schedule_start_monday,'next_due_monday',v_rule.next_due_monday,
  'schedule_week_count',v_rule.schedule_week_count::text,
  'minimum_earnings_threshold',v_rule.minimum_earnings_threshold::text,'take_home_floor',v_rule.take_home_floor_override::text,
  'has_active_snooze',v_active_snoozes>0,'active_snooze_count',v_active_snoozes::text,
  'indefinite_snooze_count',v_indefinite::text,'snooze_until_date',v_until,'stale_snooze_count',v_stale_snoozes::text,
  'is_mixed_case',v_open_tax and v_open_non_tax,'unresolved_taxable',v_unresolved,
  'resolution_stale',case when v_stale then true when v_stale_unknown then null else false end,'gap_code',v_gap);
 if pg_catalog.octet_length(v_result::text)>90000 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
 return v_result;
end
$function$;

alter function private.bpay_next_case_edit_origin_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_case_edit_origin_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_edit_state_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_case_edit_state_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_edit_assert_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_case_edit_assert_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_edit_preview_v1(uuid,uuid,bigint,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_edit_preview_v1(uuid,uuid,bigint,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_accept_case_edit_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_accept_case_edit_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_period_amendment_v1(uuid,jsonb,jsonb) owner to postgres;
revoke all on function private.bpay_next_apply_case_period_amendment_v1(uuid,jsonb,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_edit_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_apply_case_edit_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_claim_case_edit_job_v1(uuid,integer) owner to postgres;
revoke all on function private.bpay_next_claim_case_edit_job_v1(uuid,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_intake_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_intake_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_enroll_one_v1() owner to postgres;
revoke all on function private.bpay_next_enroll_one_v1() from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
alter function private.bpay_next_case_allocation_plan_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_case_allocation_plan_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
alter function private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_current_case_component_page_v1(uuid,uuid,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_row_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_list_row_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_edit_receipt_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_case_edit_receipt_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

drop trigger if exists bpay_next_case_edit_immutable_v1 on private.bpay_next_case_edit_request;
create trigger bpay_next_case_edit_immutable_v1 before update or delete on private.bpay_next_case_edit_request
 for each row execute function private.bpay_next_effect_immutable_v1();
drop trigger if exists bpay_next_case_period_amendment_immutable_v1 on private.bpay_next_case_period_amendment;
create trigger bpay_next_case_period_amendment_immutable_v1 before update or delete on private.bpay_next_case_period_amendment
 for each row execute function private.bpay_next_effect_immutable_v1();
notify pgrst,'reload schema';
commit;
