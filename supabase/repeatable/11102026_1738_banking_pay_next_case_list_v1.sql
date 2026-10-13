-- FC-01 bounded native Case register; maintained keys only, never money authority.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_case_list_refresh_key_v1(p_case_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare v_origin jsonb;v_collection private.bpay_next_work_collection%rowtype;
begin
 select c.* into v_collection from private.bpay_next_work_collection c where c.case_id=p_case_id;
 if found then
  v_origin:=private.bpay_next_payment_client_origin_v1(v_collection.original_revision_id);
 end if;
 insert into private.bpay_next_case_list_key(case_id,candidate_id,created_at_utc,candidate_sort_name,
  candidate_display_name,candidate_reference,case_subtype,case_type_label,client_id,client_display_name,client_association)
 select c.id,c.candidate_id,r0.case_created_at_utc,
  lower(coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text)),
  coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text),
  coalesce(m.tms_ref,ca.tms_ref),r.case_subtype,
  case r.case_subtype when 'LOAN' then 'Payment Advance' when 'PAYMENT_ADVANCE' then 'Payment Advance'
   when 'OVERPAYMENT' then 'Overpayment' when 'UNDERPAYMENT' then 'Underpayment'
   when 'MANUAL_CREDIT' then 'Manual Credit' when 'MANUAL_DEBT' then 'Manual Debt' else 'Unmapped case' end,
  case when v_origin->>'classification'='CLASSIFIED' then (v_origin->>'client_id')::uuid end,
  v_origin->>'client_display_name',
  case when v_collection.id is null then 'UNASSOCIATED' when v_origin->>'classification'='CLASSIFIED' then 'FROZEN' else 'UNKNOWN' end
 from private.bpay_next_finance_case c join public.candidates ca on ca.id=c.candidate_id
 left join private.bpay_next_case_rule r0 on r0.case_id=c.id and r0.candidate_id=c.candidate_id and r0.rule_revision=1
 left join private.bpay_next_case_rule r on r.id=c.current_rule_id and r.case_id=c.id and r.candidate_id=c.candidate_id
  and r.rule_revision=c.current_rule_revision
 left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=c.candidate_id
 where c.id=p_case_id
 on conflict(case_id) do update set candidate_id=excluded.candidate_id,created_at_utc=excluded.created_at_utc,
  candidate_sort_name=excluded.candidate_sort_name,candidate_display_name=excluded.candidate_display_name,candidate_reference=excluded.candidate_reference,
  case_subtype=excluded.case_subtype,case_type_label=excluded.case_type_label,client_id=excluded.client_id,
  client_display_name=excluded.client_display_name,client_association=excluded.client_association;
end
$function$;

-- Deferred maintenance obtains its read-key/revision locks after original money
-- locks. The same transaction invalidates every filter/sort/count operand.
-- Hold/progress explanations remain live point facts, never count operands.
create or replace function private.bpay_next_case_list_native_changed_v1()
returns trigger language plpgsql volatile security definer set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_candidate uuid;
begin
 if tg_table_schema<>'private' then raise exception using errcode='42501',message='BPAY_NEXT_CASE_LIST_TRIGGER_SCOPE';end if;
 update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
  updated_at_utc=pg_catalog.transaction_timestamp() where id=1;
 if tg_table_name='bpay_next_workbench_candidate_metadata' then
  v_candidate:=case when tg_op='DELETE' then old.candidate_id else new.candidate_id end;
  if tg_op='UPDATE' and (old.candidate_sort_name,old.tms_ref)
    is not distinct from (new.candidate_sort_name,new.tms_ref) then return null;end if;
  update private.bpay_next_case_list_key k set
   candidate_sort_name=lower(coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),
    nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text)),
   candidate_display_name=coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),
    nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text),
   candidate_reference=coalesce(m.tms_ref,ca.tms_ref)
  from public.candidates ca left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=ca.id
  where k.candidate_id=v_candidate and ca.id=v_candidate;
 elsif tg_table_name='bpay_next_finance_case' then
  v_id:=case when tg_op='DELETE' then old.id else new.id end;
  perform private.bpay_next_case_list_refresh_key_v1(v_id);
 elsif tg_table_name in ('bpay_next_case_rule','bpay_next_case_component','bpay_next_case_period','bpay_next_snooze','bpay_next_work_collection') then
  v_id:=case when tg_op='DELETE' then old.case_id else new.case_id end;
  if v_id is null then return null;end if;
  perform private.bpay_next_case_list_refresh_key_v1(v_id);
 else raise exception using errcode='42501',message='BPAY_NEXT_CASE_LIST_TRIGGER_SCOPE';end if;

 return null;
end
$function$;

create or replace function private.bpay_next_case_list_context_v1(p_actor_user_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare v_date jsonb;v_result jsonb;v_revision private.bpay_next_workbench_revision%rowtype;
begin
 v_date:=public.pay_banking_official_date_context_v1(null::timestamptz);
 if v_date->>'ok' is distinct from 'true' or v_date->>'current_official_pay_date' is null then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_LIST_DATE_UNAVAILABLE';end if;
 select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
 if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
  or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
  raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT';end if;
 select jsonb_build_object('agency_id',d.installation_id,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
  'local_date',(pg_catalog.clock_timestamp() at time zone 'Europe/London')::date,
  'official_pay_date',v_date->>'current_official_pay_date',
  'pay_week_start',pg_catalog.date_trunc('week',(v_date->>'current_official_pay_date')::date)::date,
  'workbench_revision',r.workbench_revision::text,'adoption_revision',r.adoption_revision::text,
  'valuation_policy_id',p.current_policy_id)
 into v_result from private.bpay_next_module_control m
 cross join private.bpay_next_workbench_revision r cross join private.bpay_next_valuation_policy_control p
 cross join private.cloudtms_database_identity d
 where m.id=1 and m.active_owner='NEXT' and r.id=1 and p.id=1 and d.singleton;
 if v_result is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 return v_result;
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

create or replace function private.bpay_next_case_list_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_filters jsonb;v_context jsonb;v_after jsonb;v_sort text;v_limit integer;v_row record;v_value jsonb;
 v_previous_last jsonb;
 v_rows jsonb:='[]'::jsonb;v_complete boolean:=true;v_next jsonb;v_last jsonb;v_scanned integer:=0;v_matched integer:=0;
 v_summary jsonb:=jsonb_build_object('payment_advances_active_count','0','overpayments_active_count','0',
  'underpayments_active_count','0','manual_debt_adjustments_active_count','0','manual_credit_adjustments_count','0',
  'mixed_finance_cases_count','0','unresolved_finance_cases_count','0','stale_finance_cases_count','0','finance_cases_with_active_snooze_count','0');
 v_category text;v_gap_count integer:=0;v_key text;
 v_after_id uuid;v_after_date timestamptz;v_after_sort text;v_bytes integer:=24576;v_result jsonb;v_cursor_last jsonb;
 v_candidate uuid;v_client uuid;v_subtype text;v_view text;v_snooze text;v_hide boolean;
begin
 if p_kind not in ('CASE_LIST','CASE_SUMMARY') or p_args is null or jsonb_typeof(p_args)<>'object'
  or octet_length(p_args::text)>32768 or exists(select 1 from jsonb_object_keys(p_args) k where k not in ('filters','after','sort','limit'))
  or (p_kind='CASE_SUMMARY' and (p_args ? 'sort' or p_args ? 'limit')) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 if p_args ? 'filters' and (jsonb_typeof(p_args->'filters') is distinct from 'object'
  or exists(select 1 from jsonb_object_keys(p_args->'filters') k where k not in ('candidate_id','client_id','case_subtype','view','snooze','hide_completed'))) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 v_filters:=jsonb_build_object('candidate_id',null,'client_id',null,'case_subtype','ALL','view','ALL','snooze','ALL','hide_completed',true)
  ||coalesce(p_args->'filters','{}'::jsonb);
 if jsonb_typeof(v_filters->'candidate_id') not in ('string','null') or jsonb_typeof(v_filters->'client_id') not in ('string','null')
  or (v_filters->>'candidate_id' is not null and v_filters->>'candidate_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  or (v_filters->>'client_id' is not null and v_filters->>'client_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  or jsonb_typeof(v_filters->'case_subtype') is distinct from 'string'
  or v_filters->>'case_subtype' not in ('ALL','LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT','UNMAPPED')
  or jsonb_typeof(v_filters->'view') is distinct from 'string' or v_filters->>'view' not in ('ALL','ACTIVE','SNOOZED','HISTORY')
  or jsonb_typeof(v_filters->'snooze') is distinct from 'string' or v_filters->>'snooze' not in ('ALL','FINITE','INDEFINITE')
  or jsonb_typeof(v_filters->'hide_completed') is distinct from 'boolean' then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 v_candidate:=(v_filters->>'candidate_id')::uuid;v_client:=(v_filters->>'client_id')::uuid;
 v_subtype:=v_filters->>'case_subtype';v_view:=v_filters->>'view';v_snooze:=v_filters->>'snooze';v_hide:=(v_filters->>'hide_completed')::boolean;
 v_sort:=coalesce(p_args->>'sort','CREATED_DESC');
 if v_sort not in ('CREATED_ASC','CREATED_DESC','CANDIDATE_ASC','CANDIDATE_DESC','TYPE_ASC','TYPE_DESC')
  or (p_args ? 'sort' and jsonb_typeof(p_args->'sort') is distinct from 'string') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 if p_kind='CASE_LIST' then
  if p_args ? 'limit' and jsonb_typeof(p_args->'limit') is distinct from 'number' then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  v_limit:=coalesce((p_args->>'limit')::integer,10);
  if v_limit not in (5,10,20,50) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 else v_limit:=100;end if;
 v_context:=private.bpay_next_case_list_context_v1(p_actor_user_id);
 v_after:=nullif(p_args->'after','null'::jsonb);
 if v_after is not null then
  if jsonb_typeof(v_after) is distinct from 'object' or octet_length(v_after::text)>16384
   or (v_after-array['context','filters','sort','last']) is distinct from '{}'::jsonb
   or not(v_after ?& array['context','filters','sort','last']) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  if v_after->'context' is distinct from v_context or v_after->'filters' is distinct from v_filters
   or v_after->>'sort' is distinct from (case when p_kind='CASE_SUMMARY' then 'CASE_ID_ASC' else v_sort end) then
   raise exception using errcode='55000',message='BPAY_NEXT_CASE_LIST_CONTEXT_CHANGED';end if;
  v_last:=v_after->'last';
  if jsonb_typeof(v_last) is distinct from 'object' or (v_last-array['case_id','created_at_utc','sort_value']) is distinct from '{}'::jsonb
   or not(v_last ?& array['case_id','created_at_utc','sort_value']) or jsonb_typeof(v_last->'case_id') is distinct from 'string'
   or v_last->>'case_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or jsonb_typeof(v_last->'created_at_utc') not in ('string','null') or jsonb_typeof(v_last->'sort_value') not in ('string','null') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  v_after_id:=(v_last->>'case_id')::uuid;v_after_date:=(v_last->>'created_at_utc')::timestamptz;
  if v_last->>'sort_value' is not null then
   v_after_sort:=convert_from(decode(v_last->>'sort_value','base64'),'UTF8');
   if replace(encode(convert_to(v_after_sort,'UTF8'),'base64'),chr(10),'') is distinct from v_last->>'sort_value' then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
   v_last:=jsonb_set(v_last,'{sort_value}',to_jsonb(v_after_sort));
  end if;
  if v_after_date is not null and not pg_catalog.isfinite(v_after_date) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  if (p_kind='CASE_SUMMARY' and (v_after_date is not null or v_after_sort is not null))
   or (p_kind='CASE_LIST' and ((v_sort like 'CREATED_%' and v_after_sort is not null)
     or (v_sort not like 'CREATED_%' and (v_after_sort is null or octet_length(v_after_sort)>8192)))) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 end if;
 -- Seven static, independently limited native index paths. Selection filters
 -- are applied after the raw window; no caller SQL or dynamic identifier.
 for v_row in select page.* from (
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_SUMMARY' and (v_after_id is null or k.case_id>v_after_id)
  order by k.case_id asc limit v_limit+1)
 union all
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CREATED_ASC' and (v_after_id is null or ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc > v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))
  order by (k.created_at_utc is null) asc,k.created_at_utc asc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CREATED_DESC' and (v_after_id is null or ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))
  order by (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.candidate_sort_name as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CANDIDATE_ASC' and (v_after_id is null or (k.candidate_sort_name> v_after_sort collate "C" or (k.candidate_sort_name=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by left(k.candidate_sort_name,512) collate "C" asc,k.candidate_sort_name asc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.candidate_sort_name as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CANDIDATE_DESC' and (v_after_id is null or (k.candidate_sort_name< v_after_sort collate "C" or (k.candidate_sort_name=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by left(k.candidate_sort_name,512) collate "C" desc,k.candidate_sort_name desc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.case_type_label as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='TYPE_ASC' and (v_after_id is null or (k.case_type_label> v_after_sort collate "C" or (k.case_type_label=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by k.case_type_label asc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.case_type_label as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='TYPE_DESC' and (v_after_id is null or (k.case_type_label< v_after_sort collate "C" or (k.case_type_label=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by k.case_type_label desc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 ) page order by
 case when p_kind='CASE_SUMMARY' then page.case_id end asc,
 case when v_sort in ('CANDIDATE_ASC','TYPE_ASC') then page.sort_value end collate "C" asc,
 case when v_sort in ('CANDIDATE_DESC','TYPE_DESC') then page.sort_value end collate "C" desc,
 (page.created_at_utc is null) asc,
 case when v_sort='CREATED_ASC' then page.created_at_utc end asc nulls last,
 case when v_sort<>'CREATED_ASC' then page.created_at_utc end desc nulls last,page.case_id asc
 loop
  if v_scanned=v_limit then v_complete:=false;exit;end if;
  v_scanned:=v_scanned+1;v_previous_last:=v_last;
  v_last:=jsonb_build_object('case_id',v_row.case_id,'created_at_utc',case when p_kind='CASE_SUMMARY' then null else v_row.created_at_utc end,
   'sort_value',case when p_kind='CASE_SUMMARY' then null else v_row.sort_value end);
  if (v_candidate is not null and v_row.candidate_id<>v_candidate)
   or (v_client is not null and v_row.client_id is distinct from v_client)
   or (v_subtype<>'ALL' and not(
    (v_subtype='UNMAPPED' and (v_row.case_subtype is null or v_row.case_subtype not in ('LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT')))
    or (v_subtype='PAYMENT_ADVANCE' and v_row.case_subtype in ('LOAN','PAYMENT_ADVANCE'))
    or v_row.case_subtype=v_subtype)) then continue;end if;
  v_value:=private.bpay_next_case_list_row_v1(v_row.case_id,v_context);
  if p_kind='CASE_LIST' then
   v_value:=v_value||jsonb_build_object('sort_key',v_last);
   if octet_length(v_value::text)>90000 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
  end if;
  if (v_hide and v_value->>'status' in ('CLOSED','WRITTEN_OFF'))
   or (v_view='ACTIVE' and (v_value->>'status' not in ('OPEN','PAUSED') or v_value->'has_active_snooze' is distinct from 'false'::jsonb))
   or (v_view='HISTORY' and v_value->>'status' not in ('CLOSED','WRITTEN_OFF'))
   or (v_view='SNOOZED' and v_value->'has_active_snooze' is distinct from 'true'::jsonb)
   or (v_snooze='FINITE' and coalesce((v_value->>'active_snooze_count')::integer<=(v_value->>'indefinite_snooze_count')::integer,true))
   or (v_snooze='INDEFINITE' and coalesce((v_value->>'indefinite_snooze_count')::integer=0,true)) then continue;end if;
  if p_kind='CASE_LIST' then
   if v_bytes+octet_length(v_value::text)>120000 then
    if v_matched=0 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
    -- Reissue the current raw member rather than silently skip its output.
    v_last:=v_previous_last;v_scanned:=v_scanned-1;v_complete:=false;exit;
   end if;
   v_rows:=v_rows||jsonb_build_array(v_value);v_bytes:=v_bytes+octet_length(v_value::text);
  else
   v_category:=case v_value->>'case_subtype' when 'LOAN' then 'payment_advances_active_count' when 'PAYMENT_ADVANCE' then 'payment_advances_active_count'
    when 'OVERPAYMENT' then 'overpayments_active_count' when 'UNDERPAYMENT' then 'underpayments_active_count'
    when 'MANUAL_DEBT' then 'manual_debt_adjustments_active_count' when 'MANUAL_CREDIT' then 'manual_credit_adjustments_count' end;
   if v_category is not null then v_summary:=jsonb_set(v_summary,array[v_category],to_jsonb(((v_summary->>v_category)::integer+1)::text));end if;
   foreach v_category in array array['mixed_finance_cases_count','unresolved_finance_cases_count','stale_finance_cases_count','finance_cases_with_active_snooze_count']
   loop
    v_key:=case v_category when 'mixed_finance_cases_count' then 'is_mixed_case' when 'unresolved_finance_cases_count' then 'unresolved_taxable'
     when 'stale_finance_cases_count' then 'resolution_stale' else 'has_active_snooze' end;
    if v_value->v_key='true'::jsonb and v_summary->v_category<>'null'::jsonb then
     v_summary:=jsonb_set(v_summary,array[v_category],to_jsonb(((v_summary->>v_category)::integer+1)::text));
    elsif v_value->v_key='null'::jsonb then v_summary:=jsonb_set(v_summary,array[v_category],'null'::jsonb);end if;
   end loop;
   if v_value->>'gap_code' is not null then v_gap_count:=v_gap_count+1;end if;
  end if;
  v_matched:=v_matched+1;v_next:=v_last;
 end loop;
 if private.bpay_next_case_list_context_v1(p_actor_user_id) is distinct from v_context then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_LIST_CONTEXT_CHANGED';end if;
 v_cursor_last:=case when v_last->>'sort_value' is null then v_last else jsonb_set(v_last,'{sort_value}',
  to_jsonb(replace(encode(convert_to(v_last->>'sort_value','UTF8'),'base64'),chr(10),''))) end;
 v_next:=case when v_complete then null else jsonb_build_object('context',v_context,'filters',v_filters,
  'sort',case when p_kind='CASE_SUMMARY' then 'CASE_ID_ASC' else v_sort end,'last',v_cursor_last) end;
 if v_next is not null and octet_length(v_next::text)>16384 then
  raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_RESPONSE_TOO_LARGE';end if;
 v_result:=case when p_kind='CASE_LIST' then jsonb_build_object('context',v_context,'filters',v_filters,'sort',v_sort,
  'rows',v_rows,'scanned_count',v_scanned::text,'complete',v_complete,'next_cursor',v_next)
 else jsonb_build_object('context',v_context,'filters',v_filters,'counts',v_summary,'scanned_count',v_scanned::text,
  'matched_count',v_matched::text,'gap_case_count',v_gap_count::text,'complete',v_complete,'next_cursor',v_next) end;
 if octet_length(v_result::text)>119936 then
  raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_RESPONSE_TOO_LARGE';end if;
 return v_result;
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
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
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

alter function private.bpay_next_case_list_refresh_key_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_case_list_refresh_key_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_native_changed_v1() owner to postgres;
revoke all on function private.bpay_next_case_list_native_changed_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_context_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_case_list_context_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_row_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_list_row_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_list_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_finance_case;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_finance_case
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_case_rule;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_case_rule
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_case_component;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_case_component
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_case_period;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_case_period
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_snooze;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_snooze
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_work_collection;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_work_collection
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
drop trigger if exists bpay_next_case_list_native_changed on private.bpay_next_workbench_candidate_metadata;
create constraint trigger bpay_next_case_list_native_changed after insert or update or delete on private.bpay_next_workbench_candidate_metadata
deferrable initially deferred for each row execute function private.bpay_next_case_list_native_changed_v1();
-- Derived initial index fill only. Every original NEXT Case, including history,
-- stays in the register; no legacy financial records or business rows created.
do $derived_keys$
declare v_id uuid;
begin
 for v_id in select id from private.bpay_next_finance_case order by id loop
  perform private.bpay_next_case_list_refresh_key_v1(v_id);
 end loop;
end
$derived_keys$;
notify pgrst,'reload schema';
commit;
