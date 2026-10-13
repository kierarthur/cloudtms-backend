-- WB-39 · BD129. Complete current 0454 reader successor; no producer/money rewrite.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_snooze_management_row_v1(p_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare s private.bpay_next_snooze%rowtype;w private.bpay_next_work%rowtype;
 r private.bpay_next_work_revision%rowtype;c private.bpay_next_case_component%rowtype;
 k private.bpay_next_case_list_key%rowtype;g private.bpay_next_approved_segment%rowtype;
 a private.bpay_next_approved_line%rowtype;v_label text;v_amend boolean:=false;v_basis bytea;
begin
 select x.* into s from private.bpay_next_snooze x where x.id=p_id and x.active;
 if not found then return null;end if;
 if s.scope_kind='CASE' then
  select x.* into c from private.bpay_next_case_component x where x.id=s.case_component_id
   and x.case_id=s.case_id and x.candidate_id=s.candidate_id;
  select x.* into k from private.bpay_next_case_list_key x where x.case_id=s.case_id and x.candidate_id=s.candidate_id;
  if c.id is null or k.case_id is null then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CASE_AUTHORITY_MISMATCH';end if;
  v_amend:=c.instruction_kind='RECOVERY' and c.case_kind=s.case_kind
   and ((s.snooze_kind='PAYMENT_ADVANCE_REPAYMENT' and c.case_kind in ('LOAN','ADVANCE'))
    or (s.snooze_kind='MANUAL_DEBT_RECOVERY' and c.case_kind='MANUAL_DEBT')
    or (s.snooze_kind='OVERPAYMENT_RECOVERY' and c.case_kind='OVERPAYMENT'));
  v_label:=case c.case_kind when 'LOAN' then 'Payment Advance repayment' when 'ADVANCE' then 'Payment Advance repayment'
   when 'MANUAL_DEBT' then 'Manual Debt recovery' when 'OVERPAYMENT' then 'Overpayment recovery' else 'Payment' end;
 else
  select x.* into strict w from private.bpay_next_work x where x.id=s.work_id and x.candidate_id=s.candidate_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.id=coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id)
   and x.work_id=s.work_id and x.sealed_at_utc is not null;
  if s.scope_kind='SEGMENT' then
   select x.* into g from private.bpay_next_approved_segment x where x.revision_id=r.id and x.segment_key=s.segment_key;
   if not found then select x.* into g from private.bpay_next_approved_segment x where x.id=s.approved_segment_id;end if;
  elsif s.scope_kind='EXPENSE_LINE' then
   select x.* into a from private.bpay_next_approved_line x where x.revision_id=r.id and x.component_key=s.component_key;
   if not found then select x.* into a from private.bpay_next_approved_line x where x.id=s.expense_approved_line_id;end if;
  end if;
  if w.approval_state='APPROVED' and w.current_revision_id=r.id then
   v_basis:=private.bpay_next_snooze_revision_basis_v1(r.id,s.scope_kind,case s.scope_kind when 'SEGMENT' then s.segment_key else s.component_key end);
   v_amend:=v_basis is not null and v_basis=s.worker_pay_basis;
  end if;
  v_label:=case s.scope_kind when 'WHOLE_TIMESHEET' then 'Whole Timesheet' when 'SEGMENT' then 'Shift'
   when 'EXPENSES_ALL' then 'All expenses and mileage' when 'EXPENSE_LINE' then case when a.component_kind='MILEAGE' then 'Mileage'
    when a.component_key like '%TRAVEL%' then 'Travel' when a.component_key like '%ACCOMMODATION%' then 'Accommodation' else 'Expense' end end;
 end if;
 return jsonb_build_object('snooze_id',s.id,'candidate_id',s.candidate_id,'work_id',s.work_id,
  'expected_revision_id',s.expected_revision_id,'revision_id',case when w.current_revision_id=r.id then r.id else null end,
  'physical_timesheet_id',coalesce(r.physical_timesheet_id,s.physical_timesheet_id),'case_id',s.case_id,'case_component_id',s.case_component_id,
  'case_kind',s.case_kind,'case_component_revision',c.component_revision::text,'scope_kind',s.scope_kind,'scope_identity',s.scope_identity,
  'segment_key',s.segment_key,'component_key',s.component_key,'snooze_kind',s.snooze_kind,'version',s.version::text,
  'control_sequence',s.control_sequence::text,'active',s.active,'until_date',s.until_date,'note',s.note,'created_at_utc',s.created_at_utc,
  'candidate_display_name',coalesce(r.candidate_display_name,k.candidate_display_name),'candidate_reference',coalesce(r.candidate_reference,k.candidate_reference),
  'client_id',coalesce(w.client_id,k.client_id),'client_display_name',coalesce(r.client_display_name,k.client_display_name),
  'timesheet_reference',r.timesheet_reference,'week_ending_date',r.week_ending_date,'label',v_label,
  'work_date',g.work_date,'shift_start_local',g.shift_start_local,'shift_end_local',g.shift_end_local,
  'can_amend',v_amend,'can_unsnooze',true,'amend_unavailable_reason',case when v_amend then null else 'PAYMENT_CHANGED' end);
end
$function$;

create or replace function private.bpay_next_snooze_management_page_v1(p_actor uuid,p_args jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_scope jsonb;v_context jsonb;v_after jsonb;v_key jsonb;v_last jsonb;v_rows jsonb:='[]';
 v_row jsonb;v_rec record;v_epoch bigint;v_agency uuid;v_revision private.bpay_next_workbench_revision%rowtype;
 v_clock bigint;v_limit integer;v_sort text;v_complete boolean:=true;v_seen integer:=0;
begin
 if p_actor is null or not exists(select 1 from public.tms_users u where u.id=p_actor and u.is_active and u.role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
 select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if p_args is null or jsonb_typeof(p_args)<>'object' or not(p_args?&array['candidate_id','client_id','scope_kind','filter','snooze_id','sort','after','limit'])
  or p_args-array['candidate_id','client_id','scope_kind','filter','snooze_id','sort','after','limit']<>'{}'::jsonb or octet_length(p_args::text)>32768
  or p_args->>'scope_kind' is null or p_args->>'scope_kind' not in ('ALL','WORK','CASE')
  or p_args->>'filter' is null or p_args->>'filter' not in ('ACTIVE','INDEFINITE','DATED')
  or p_args->>'sort' is null or p_args->>'sort' not in ('CREATED_ASC','CREATED_DESC')
  or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]?$' then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 if exists(select 1 from unnest(array['candidate_id','client_id','snooze_id']) q(k) where p_args->k<>'null'::jsonb
  and (jsonb_typeof(p_args->k)<>'string' or p_args->>k!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 v_limit:=(p_args->>'limit')::integer;v_sort:=p_args->>'sort';
 if v_limit>50 or (p_args->'snooze_id'<>'null'::jsonb and p_args->'after'<>'null'::jsonb) then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
 select x.* into strict v_revision from private.bpay_next_workbench_revision x where x.id=1;
 select x.last_sequence into strict v_clock from private.bpay_next_command_clock x where x.id=1;
 v_scope:=p_args-array['after','limit'];
 v_context:=jsonb_build_object('actor_user_id',p_actor,'agency_id',v_agency,'owner_epoch',v_epoch::text,
  'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,'control_cut',v_clock::text,'scope',v_scope);
 v_after:=nullif(p_args->'after','null'::jsonb);
 if v_after is not null then
  if jsonb_typeof(v_after)<>'object' or not(v_after?&array['context','key']) or v_after-array['context','key']<>'{}'::jsonb
   or v_after->'context' is distinct from v_context then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  v_key:=v_after->'key';
  if jsonb_typeof(v_key)<>'object' or not(v_key?&array['created_at_utc','id']) or v_key-array['created_at_utc','id']<>'{}'::jsonb
   or jsonb_typeof(v_key->'id')<>'string' or v_key->>'id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_key->'created_at_utc')<>'string' or v_key->>'created_at_utc'!~'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+]00:00)$'
   or not isfinite((v_key->>'created_at_utc')::timestamptz) then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 end if;
 for v_rec in select s.id,s.created_at_utc from private.bpay_next_snooze s
  left join private.bpay_next_work w on w.id=s.work_id
  left join private.bpay_next_case_list_key k on k.case_id=s.case_id
  where s.active and (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
   and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
   and (p_args->'snooze_id'='null'::jsonb or s.id=(p_args->>'snooze_id')::uuid)
   and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
   and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null))
   and (v_key is null or (v_sort='CREATED_DESC' and s.created_at_utc<(v_key->>'created_at_utc')::timestamptz)
    or (v_sort='CREATED_ASC' and s.created_at_utc>(v_key->>'created_at_utc')::timestamptz)
    or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))
  order by case when v_sort='CREATED_DESC' then s.created_at_utc end desc,
   case when v_sort='CREATED_ASC' then s.created_at_utc end asc,s.id limit v_limit+1
 loop
  v_seen:=v_seen+1;if v_seen>v_limit then v_complete:=false;exit;end if;
  v_row:=private.bpay_next_snooze_management_row_v1(v_rec.id);
  if v_row is null then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  if octet_length((v_rows||jsonb_build_array(v_row))::text)>110000 then
   if jsonb_array_length(v_rows)=0 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
   v_complete:=false;exit;end if;
  v_rows:=v_rows||jsonb_build_array(v_row);v_last:=jsonb_build_object('created_at_utc',v_rec.created_at_utc,'id',v_rec.id);
 end loop;
 return jsonb_build_object('ok',true,'kind','SNOOZES','context',v_context,'rows',v_rows,'complete',v_complete,
  'next_cursor',case when v_complete then null else jsonb_build_object('context',v_context,'key',v_last) end);
end
$function$;

create or replace function public.bpay_next_workbench_page_v1(p_request jsonb)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
set statement_timeout='5s'
as $function$
declare
  v_source_receipt private.weekly_source_entitlement_publication_receipts%rowtype;v_source_member record;v_source_no integer;v_source_count integer;
  v_initial_a public.weekly_source_root_authorisations%rowtype;v_initial_t public.timesheets%rowtype;
  v_initial_f public.timesheets_financials%rowtype;v_initial_w private.bpay_next_work%rowtype;
  v_initial_r private.bpay_next_work_revision%rowtype;v_initial_p private.bpay_next_publication%rowtype;
  v_initial_c public.contracts%rowtype;v_initial_line private.bpay_next_approved_line%rowtype;
  v_initial_inventory jsonb;v_initial_basis jsonb;v_initial_origin jsonb;v_initial_details jsonb;
  v_initial_cert jsonb;v_initial_component jsonb;v_initial_n integer;
  v_est jsonb;v_descriptor jsonb;v_est_intent jsonb;v_est_scope jsonb;v_versions jsonb;v_est_leaves jsonb;v_est_instructions jsonb:='[]';
  v_est_plan jsonb;v_est_rows jsonb:='[]';v_est_row jsonb;v_est_key jsonb;v_est_last jsonb;v_est_bound jsonb;
  v_est_candidate public.candidates%rowtype;v_est_position private.bpay_next_position%rowtype;
  v_est_week private.bpay_next_worker_week%rowtype;v_est_binding private.bpay_next_source_component_binding_version%rowtype;
  v_est_record record;v_est_value record;v_est_source numeric:=0;v_est_gross numeric:=0;v_est_h numeric:=0;
  v_est_gross_add numeric:=0;v_est_gross_deduct numeric:=0;v_est_net_add numeric:=0;v_est_net_deduct numeric:=0;
  v_est_financial bigint;v_est_week_start date;v_est_count bigint;v_est_seen integer:=0;
  v_est_work_ids uuid[];v_est_components uuid[];v_est_all_overpayment boolean;v_est_state text:='ESTIMATE';
  v_actor uuid;v_kind text;v_scope jsonb;v_context jsonb;v_after jsonb;v_cursor jsonb;
  v_page jsonb;v_response jsonb;v_read_scope jsonb;v_ns jsonb;v_memory jsonb;v_baseline uuid;v_revision private.bpay_next_workbench_revision%rowtype;
  v_epoch bigint;v_agency uuid;v_date_context jsonb;v_limit integer;v_error text;
  v_key_count integer;v_key jsonb;v_sort text;v_text text;v_keys text[];v_count_selection jsonb;v_progress jsonb;
begin
  -- STABLE reads share a single statement snapshot. No volatile authority or
  -- FOR SHARE helper; these are the existing service/active-admin predicates.
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  -- WB-39 read-only native snooze management; original financial owners retained.
  if p_request->>'kind'='SNOOZES' then
    if jsonb_typeof(p_request)<>'object' or not(p_request?&array['actor_user_id','kind'])
      or jsonb_typeof(p_request->'actor_user_id')<>'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    return private.bpay_next_snooze_management_page_v1((p_request->>'actor_user_id')::uuid,p_request-array['actor_user_id','kind']);
  end if;
  -- WB-38 true read: context only; acknowledgement issuance remains W2.
  if p_request->>'kind'='SNOOZE_CHECK' then
    if jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>8192
      or not(p_request ?& array['actor_user_id','kind','snooze'])
      or p_request-array['actor_user_id','kind','snooze']<>'{}'::jsonb
      or jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
      or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';end if;
    v_actor:=(p_request->>'actor_user_id')::uuid;
    return jsonb_build_object('ok',true,'kind','SNOOZE_CHECK',
      'context',private.bpay_next_snooze_context_v1(v_actor,p_request->'snooze'));
  end if;
  p_request:=p_request||jsonb_build_object('count_selection',coalesce(p_request->'count_selection','null'::jsonb),
    'progress_after',coalesce(p_request->'progress_after','null'::jsonb),'selection_memory',coalesce(p_request->'selection_memory','null'::jsonb),'estimate',coalesce(p_request->'estimate','null'::jsonb));
  v_keys:=array['actor_user_id','kind','candidate_id','client_id','channel','section',
    'search','sort','group_key','item_id','reason_code','after','limit','count_selection','progress_after','selection_memory','estimate'];
  if p_request is null or jsonb_typeof(p_request)<>'object' or octet_length(p_request::text)>32768
    or not(p_request ?& v_keys) or p_request-v_keys<>'{}'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if jsonb_typeof(p_request->'actor_user_id')<>'string'
    or p_request->>'actor_user_id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  v_actor:=(p_request->>'actor_user_id')::uuid;
  if not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active is true and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select r.* into strict v_revision from private.bpay_next_workbench_revision r where r.id=1;
  select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
  v_kind:=p_request->>'kind';v_sort:=p_request->>'sort';
  if jsonb_typeof(p_request->'kind')<>'string'
    or v_kind not in ('HEADER','CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL',
      'CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE','PICK_CANDIDATES','PICK_CLIENTS','EXPORT_ROWS')
    or jsonb_typeof(p_request->'channel')<>'string' or p_request->>'channel' not in ('ALL','PAYE','UMBRELLA')
    or jsonb_typeof(p_request->'section')<>'string' or p_request->>'section' not in ('ALL','READY','ACTION','BLOCKED','WAITING')
    or jsonb_typeof(p_request->'sort')<>'string' or (v_kind='EXPORT_ROWS' and v_sort not in ('C_NAME_ASC','C_NAME_DESC','C_AMOUNT_ASC','C_AMOUNT_DESC','C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC','C_STATUS_ASC','C_STATUS_DESC','G_PAYMENT_ASC','G_PAYMENT_DESC','G_CLIENT_ASC','G_CLIENT_DESC','G_CHANNEL_ASC','G_CHANNEL_DESC','G_AMOUNT_ASC','G_AMOUNT_DESC','G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC','G_REASON_ASC','G_REASON_DESC'))
    or (v_kind<>'EXPORT_ROWS' and (false or v_sort not in ('NAME_ASC','NAME_DESC','AMOUNT_ASC','AMOUNT_DESC','DEDUCTIONS_ASC','DEDUCTIONS_DESC','PAYMENT_ASC','PAYMENT_DESC','STATUS_ASC','STATUS_DESC','REASON_ASC','REASON_DESC','CLIENT_ASC','CLIENT_DESC','CHANNEL_ASC','CHANNEL_DESC')
    or (v_kind='CANDIDATES' and v_sort like 'PAYMENT_%')
    or (v_kind<>'CANDIDATES' and v_sort like 'STATUS_%')
    or (v_kind not in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort like 'REASON_%')
    or (v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') and v_sort like 'DEDUCTIONS_%')
    or (v_kind not in ('CANDIDATES','ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL','CANDIDATE_GROUPS') and v_sort<>'NAME_ASC')
    or (v_sort like 'CLIENT_%' or v_sort like 'CHANNEL_%') and v_kind<>'CANDIDATE_GROUPS'
    or (v_kind='CANDIDATE_GROUPS' and v_sort='NAME_DESC')
))
    or jsonb_typeof(p_request->'search')<>'string' or length(p_request->>'search')>200
    or octet_length(p_request->>'search')>800
    or btrim(p_request->>'search')<>p_request->>'search'
    or jsonb_typeof(p_request->'limit')<>'number' or p_request->>'limit'!~'^[1-9][0-9]{0,2}$' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_limit:=(p_request->>'limit')::integer;
  if v_limit not between 1 and 100 or (v_kind='CANDIDATE_GROUPS' and v_limit>10) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  foreach v_text in array array['candidate_id','client_id','item_id'] loop
    if p_request->v_text<>'null'::jsonb and (jsonb_typeof(p_request->v_text)<>'string'
      or p_request->>v_text!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end loop;
  if p_request->'group_key'<>'null'::jsonb and (jsonb_typeof(p_request->'group_key')<>'string'
    or p_request->>'group_key'!~'^(WORK|CASE|ROW):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->>'group_key' like 'ROW:%' and v_kind not in ('GROUP_LINES','CANDIDATE_GROUPS') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if p_request->'reason_code'<>'null'::jsonb and (jsonb_typeof(p_request->'reason_code')<>'string'
    or p_request->>'reason_code'!~'^[A-Z][A-Z0-9_]{0,255}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if (v_kind in ('CANDIDATE_GROUPS','GROUP_LINES','CANDIDATE_ESTIMATE') and p_request->'candidate_id'='null'::jsonb)
    or (v_kind in ('GROUP_LINES','ISSUE_DETAIL') and p_request->'group_key'='null'::jsonb)
    or (v_kind='ISSUE_DETAIL' and p_request->'reason_code'='null'::jsonb)
    or (v_kind not in ('GROUP_LINES','ISSUE_DETAIL','CANDIDATE_GROUPS') and (p_request->'group_key'<>'null'::jsonb or p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)
    or (v_kind='CANDIDATE_GROUPS' and (p_request->'item_id'<>'null'::jsonb or p_request->'reason_code'<>'null'::jsonb)))
    or (v_kind not in ('CANDIDATES','PICK_CANDIDATES','PICK_CLIENTS','ACTION_REQUIRED','BLOCKED','EXPORT_ROWS') and p_request->>'search'<>'')
    or (v_kind='HEADER' and p_request->'after'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_count_selection:=p_request->'count_selection';
  if v_count_selection ? 'intent' then
   if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
    or not(v_count_selection ?& array['intent','memory_baseline_run_id'])
    or v_count_selection-array['intent','memory_baseline_run_id']<>'{}'::jsonb
    or not private.bpay_next_selection_memory_intent_valid_v1(v_count_selection->'intent') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  else
  if v_count_selection<>'null'::jsonb then
    if v_kind<>'HEADER' or p_request->>'section'<>'READY' or jsonb_typeof(v_count_selection)<>'object'
      or not(v_count_selection ?& array['mode','candidate_ids'])
      or v_count_selection-array['mode','candidate_ids']<>'{}'::jsonb
      or jsonb_typeof(v_count_selection->'mode')<>'string' or v_count_selection->>'mode' not in ('ALL','NONE')
      or jsonb_typeof(v_count_selection->'candidate_ids')<>'array' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if exists(select 1 from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)
      where jsonb_typeof(a.value)<>'string' or (a.value#>>'{}')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (select count(*)<>count(distinct a.value) from jsonb_array_elements(v_count_selection->'candidate_ids') a(value)) then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  end if;
  end if;
  if p_request->'progress_after'<>'null'::jsonb and v_kind<>'HEADER' then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  v_scope:=p_request-array['actor_user_id','kind','after','limit','progress_after','estimate'];
  -- Fixed effective sections, explicit normalization; client cannot silently
  -- continue a cursor issued for a different list.
  if v_kind='CANDIDATES' then v_scope:=jsonb_set(v_scope,'{section}','"READY"'::jsonb);
  elsif v_kind='ACTION_REQUIRED' then v_scope:=jsonb_set(v_scope,'{section}','"ACTION"'::jsonb);
  elsif v_kind='BLOCKED' then v_scope:=jsonb_set(v_scope,'{section}','"BLOCKED"'::jsonb);end if;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  if v_date_context->>'ok'<>'true' or v_date_context->>'current_official_pay_date' is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
  v_context:=jsonb_build_object('kind',v_kind,'agency_id',v_agency,'actor_user_id',v_actor,
    'owner_epoch',v_epoch::text,'official_pay_date',v_date_context->>'current_official_pay_date',
    'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,
    'scope',v_scope);
  if v_kind='HEADER' then
    v_progress:=private.bpay_next_workbench_job_progress_v1(v_context,p_request->'progress_after',v_limit);
  end if;
  if v_revision.adoption_state<>'CURRENT' or v_revision.dirty_candidate_count<>0
    or v_revision.dirty_group_count<>0 or v_revision.pending_task_count<>0 then
    v_response:=jsonb_build_object('ok',false,'error_code','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT',
      'retryable',true,'reload_required',false,'workbench_revision',v_revision.workbench_revision::text,
      'complete',false,'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
      'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
    if v_kind='HEADER' then v_response:=v_response||jsonb_build_object('job_progress',v_progress);end if;
    if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
    return v_response;
  end if;
  v_ns:=jsonb_build_object('agency_id',v_agency,'owner_epoch',v_epoch::text,'actor_user_id',v_actor,
   'official_pay_date',v_date_context->>'current_official_pay_date','scope',jsonb_build_object(
    'candidate_id',v_scope->'candidate_id','client_id',v_scope->'client_id','channel',v_scope->'channel'));
  v_baseline:=private.bpay_next_selection_memory_resolve_v1(v_ns,p_request->'selection_memory');
  v_memory:=jsonb_build_object('run_id',v_baseline,'scope',coalesce(nullif(p_request->'selection_memory'->'scope','null'::jsonb),v_ns->'scope'));
  if v_count_selection ? 'intent' and v_count_selection->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
   raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  if v_count_selection ? 'intent' then
   -- Same native identities as the current memory-aware Create owner.
   -- Component-only edits inherit defaults; supplied Work parents must match.
   if exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'candidates') chosen
     where not exists(select 1 from public.candidates candidate where candidate.id=(chosen->>'candidate_id')::uuid))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'components') c(value)
     where case when exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
       where work->>'work_id'=c.value->>'work_id') then not exists(
        select 1 from jsonb_array_elements(v_count_selection->'intent'->'works') work
         where work->>'work_id'=c.value->>'work_id' and work->>'candidate_id'=c.value->>'candidate_id'
          and work->>'expected_revision_id'=c.value->>'expected_revision_id')
      else not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision r on r.work_id=w.id
       join private.bpay_next_workbench_item i on i.work_id=w.id and i.revision_id=r.id
       join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
       where w.id=(c.value->>'work_id')::uuid and w.candidate_id=(c.value->>'candidate_id')::uuid
        and r.id=(c.value->>'expected_revision_id')::uuid and w.current_revision_id=r.id and w.applied_revision_id=r.id
        and i.approved_line_id=(c.value->>'approved_line_id')::uuid and i.candidate_id=w.candidate_id
        and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_memory->'scope'->>'candidate_id' is null or i.candidate_id=(v_memory->'scope'->>'candidate_id')::uuid)
        and (v_memory->'scope'->>'client_id' is null or i.client_id=(v_memory->'scope'->>'client_id')::uuid)
        and (v_memory->'scope'->>'channel'='ALL' or i.target_pay_channel=v_memory->'scope'->>'channel')) end) then
    raise exception using errcode='22023',message='BPAY_NEXT_CHOICE_NATIVE_IDENTITY_INVALID';end if;
   if exists(select 1 from unnest(array['works','components']) kind
    cross join lateral jsonb_array_elements(v_count_selection->'intent'->kind) chosen
    where not exists(select 1 from private.bpay_next_work w join private.bpay_next_work_revision wr on wr.work_id=w.id
     where w.id=(chosen->>'work_id')::uuid and w.candidate_id=(chosen->>'candidate_id')::uuid
      and wr.id=(chosen->>'expected_revision_id')::uuid and w.current_revision_id=wr.id and w.applied_revision_id=wr.id
      and (kind='works' or exists(select 1 from private.bpay_next_approved_line l where l.id=(chosen->>'approved_line_id')::uuid and l.revision_id=wr.id))))
    or exists(select 1 from jsonb_array_elements(v_count_selection->'intent'->'cases') chosen
     where not exists(select 1 from private.bpay_next_case_component cc where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=(chosen->>'candidate_id')::uuid)) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  end if;
  v_context:=v_context||jsonb_build_object('selection_memory',v_memory);
  if v_kind='CANDIDATE_ESTIMATE' then
    v_est:=p_request->'estimate';v_descriptor:=v_est->'descriptor';
    if p_request->>'section'<>'READY' or p_request->'count_selection'<>'null'::jsonb
      or jsonb_typeof(v_est) is distinct from 'object'
      or not(v_est ?& array['descriptor','expected_financial_view_revision','versions','verify_only'])
      or v_est-array['descriptor','expected_financial_view_revision','versions','verify_only']<>'{}'::jsonb
      or jsonb_typeof(v_est->'verify_only') is distinct from 'boolean'
      or (v_est->'versions'='null'::jsonb and (v_est->'verify_only'='true'::jsonb or p_request->'after'<>'null'::jsonb))
      or jsonb_typeof(v_est->'expected_financial_view_revision') is distinct from 'string'
      or v_est->>'expected_financial_view_revision'!~'^(0|[1-9][0-9]{0,18})$'
      or (v_est->'versions'<>'null'::jsonb and (jsonb_typeof(v_est->'versions')<>'array' or jsonb_array_length(v_est->'versions')>8000))
      or jsonb_typeof(v_descriptor) is distinct from 'object'
      or not(v_descriptor ?& array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id'])
      or v_descriptor-array['namespace','intent','revision','workbench_revision','adoption_revision','memory_baseline_run_id']<>'{}'::jsonb
      or not private.bpay_next_selection_memory_intent_valid_v1(v_descriptor->'intent')
      or jsonb_typeof(v_descriptor->'revision') is distinct from 'number'
      or v_descriptor->>'revision'!~'^(0|[1-9][0-9]{0,15})$' then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_descriptor->'namespace' is distinct from jsonb_set(v_ns,'{scope}',v_memory->'scope')
      or v_descriptor->'memory_baseline_run_id' is distinct from v_memory->'run_id' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    select c.* into strict v_est_candidate from public.candidates c where c.id=(p_request->>'candidate_id')::uuid;
    select c.financial_view_revision into strict v_est_financial from private.bpay_next_worker_control c where c.candidate_id=v_est_candidate.id;
    v_est_week_start:=(v_date_context->>'current_official_pay_date')::date-
      (extract(isodow from (v_date_context->>'current_official_pay_date')::date)::integer-1);
    if v_descriptor->>'workbench_revision' is distinct from v_revision.workbench_revision::text
      or v_descriptor->>'adoption_revision' is distinct from v_revision.adoption_revision::text
      or v_est->>'expected_financial_view_revision' is distinct from v_est_financial::text then v_est_state:='OUTDATED';end if;
    -- Bound the current indexed Candidate membership BEFORE choices materialises LEAF.
    select count(*) into v_est_count from (select 1 from private.bpay_next_workbench_item i
      join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
      where i.candidate_id=v_est_candidate.id and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
        and g.progress_state='CURRENT' and i.progress_state='CURRENT'
        and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
        and (v_scope->>'client_id' is null or i.client_id=(v_scope->>'client_id')::uuid)
        and (v_scope->>'channel'='ALL' or i.target_pay_channel=v_scope->>'channel') limit 1001) bounded;
    if v_est_count>1000 then v_est_state:='PENDING';end if;
    if v_est_state='ESTIMATE' then
      begin
        -- Only this Candidate's native overrides can affect their estimate.
        v_est_intent:=jsonb_build_object('mode',v_descriptor->'intent'->'mode');
        for v_est_record in select unnest(array['candidates','works','components','cases']) kind loop
          v_est_intent:=v_est_intent||jsonb_build_object(v_est_record.kind,(select coalesce(jsonb_agg(x.value),'[]'::jsonb)
            from jsonb_array_elements(v_descriptor->'intent'->v_est_record.kind) x(value) where x.value->>'candidate_id'=v_est_candidate.id::text));
        end loop;
        with chosen as materialized(select * from private.bpay_next_selection_memory_choices_v1(
          v_scope,v_baseline,v_est_intent) where selected),
        leaves as materialized(select * from private.bpay_next_selection_memory_leaves_v1(v_scope))
        select coalesce(jsonb_agg(jsonb_build_object('leaf_key',l.leaf_key,'candidate_id',l.candidate_id,'work_id',l.work_id,'revision_id',l.revision_id,'approved_line_id',l.approved_line_id,'case_component_id',l.case_component_id,'basis',l.basis) order by l.leaf_key),'[]'::jsonb) into v_est_leaves from leaves l
          join chosen c on c.candidate_id=l.candidate_id and
            (c.work_id=l.work_id and c.revision_id=l.revision_id and c.approved_line_id=l.approved_line_id
              or c.case_component_id=l.case_component_id);
        if jsonb_array_length(v_est_leaves)>1000 then
          raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        select coalesce(array_agg(distinct (l->>'work_id')::uuid) filter(where l->>'work_id' is not null),'{}'),
          coalesce(array_agg(distinct (l->>'case_component_id')::uuid) filter(where l->>'case_component_id' is not null),'{}')
          into v_est_work_ids,v_est_components from jsonb_array_elements(v_est_leaves) l;
        select count(*) into v_est_count from (select 1 from private.bpay_next_worker_week_contribution x
          where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
            and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') chosen
          where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
            where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
              and w.current_revision_id=(chosen->>'expected_revision_id')::uuid and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_work w
              join private.bpay_next_approved_line l on l.revision_id=w.current_revision_id
              where w.id=(chosen->>'work_id')::uuid and w.candidate_id=v_est_candidate.id
                and l.id=(chosen->>'approved_line_id')::uuid and w.current_revision_id=(chosen->>'expected_revision_id')::uuid
                and w.applied_revision_id=w.current_revision_id))
          or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'cases') chosen
            where chosen->>'candidate_id'=v_est_candidate.id::text and not exists(select 1 from private.bpay_next_case_component cc
              where cc.id=(chosen->>'case_component_id')::uuid and cc.candidate_id=v_est_candidate.id)) then
          raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        select count(*) into v_est_count from (select 1 from private.bpay_next_approved_line l
          join private.bpay_next_work w on w.current_revision_id=l.revision_id where w.id=any(v_est_work_ids) limit 1001) bounded;
        if v_est_count>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
        if exists(select 1 from private.bpay_next_valuation_policy_window pw
          where pw.policy_id in (select r.valuation_policy_id from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
            union select o.valuation_policy_id from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
            union select o.original_valuation_policy_id from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components))
          and (v_date_context->>'current_official_pay_date')::date>=pw.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
          and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
          group by pw.policy_id having count(*)>1) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        -- Native identities and versions only; no monetary carry is returned to the browser.
        if exists(select 1 from private.bpay_next_source_component_binding b
          join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
            and cardinality(receipt.head_ids) not between 1 and 2) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
        with source_receipts as materialized(select distinct receipt.*,receipt.xmin::text as receipt_version
          from private.bpay_next_source_component_binding b join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
          join private.weekly_source_entitlement_publication_receipts receipt on receipt.id=bv.source_receipt_id
          where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)),
        source_members as materialized(select point.* from source_receipts receipt cross join lateral (
    select h.id,h.xmin::text as head_version,a.id as auth_id,a.xmin::text as auth_version,w.xmin::text as work_version,r.xmin::text as revision_version,p.xmin::text as publication_version,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id 
      from generate_subscripts(receipt.head_ids,1) member(member_no) join public.weekly_source_entitlement_heads h on h.id=receipt.head_ids[member.member_no]
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=any(receipt.head_ids) and h.state='COMMITTED_CURRENT'
        and h.candidate_id=receipt.candidate_id and h.root_timesheet_id=receipt.member_root_ids[member.member_no]
        and h.root_family_booking_id=receipt.member_family_booking_ids[member.member_no]
        and h.root_timesheet_version=receipt.member_root_versions[member.member_no]
        and h.decision_bundle_id=receipt.decision_bundle_id and h.bundle_revision=receipt.bundle_revision
        and h.publication_receipt_digest=receipt.request_digest
        and h.scope_change_tx_token=receipt.scope_change_tx_token limit 3
        ) point),native as (
          select jsonb_build_array('C',c.id,c.xmin::text) value from public.candidates c where c.id=v_est_candidate.id
          union all select jsonb_build_array('U',u.id,u.xmin::text) from public.umbrellas u where u.id=v_est_candidate.umbrella_id
          union all select jsonb_build_array('W',w.id,w.xmin::text) from private.bpay_next_work w where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('R',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work w on w.current_revision_id=r.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('P',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            where p.work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'work_id'=p.work_id::text and l->'basis'->>'component_key'=p.component_key)
          union all select jsonb_build_array('I',i.id,i.xmin::text) from private.bpay_next_workbench_item i
            where i.candidate_id=v_est_candidate.id and exists(select 1 from jsonb_array_elements(v_est_leaves) l
              where l->>'leaf_key'=case when i.work_id is not null then 'WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
                else 'CASE:'||i.case_component_id::text end) and i.portion_kind in ('AVAILABLE','ZERO')
          union all select jsonb_build_array('FC',c.id,c.xmin::text) from private.bpay_next_finance_case c
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CC',cc.id,cc.xmin::text) from private.bpay_next_case_component cc where cc.id=any(v_est_components)
          union all select jsonb_build_array('CR',r.id,r.xmin::text) from private.bpay_next_case_rule r
            join private.bpay_next_finance_case c on c.current_rule_id=r.id
            join private.bpay_next_case_component cc on cc.case_id=c.id where cc.id=any(v_est_components)
          union all select jsonb_build_array('CP',p.case_component_id,p.pay_week_start,p.xmin::text) from private.bpay_next_case_period p
            where p.case_component_id=any(v_est_components) and p.pay_week_start=v_est_week_start
          union all select jsonb_build_array('CO',o.id,o.xmin::text) from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OP',p.work_id,p.component_key,p.xmin::text) from private.bpay_next_position p
            join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OW',w.id,w.xmin::text) from private.bpay_next_work w
            join private.bpay_next_work_collection o on o.work_id=w.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('SN',sn.id,sn.xmin::text) from private.bpay_next_snooze sn
            where sn.candidate_id=v_est_candidate.id and (sn.work_id=any(v_est_work_ids) or sn.case_component_id=any(v_est_components))
              and sn.control_sequence=(select max(s.control_sequence::bigint) from private.bpay_next_snooze s
                where (sn.work_id is not null and s.work_id=sn.work_id or sn.case_component_id is not null and s.case_component_id=sn.case_component_id))
          union all select jsonb_build_array('SB',b.canonical_work_id,b.canonical_component_key,b.xmin::text)
            from private.bpay_next_source_component_binding b where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('SV',bv.id,bv.xmin::text) from private.bpay_next_source_component_binding_version bv
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('PL',p.command_id,p.xmin::text) from private.bpay_next_publication p
            join private.bpay_next_source_component_binding_version bv on p.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BC',c.id,c.xmin::text) from private.bpay_next_command c
            join private.bpay_next_source_component_binding_version bv on c.id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BJ',j.id,j.xmin::text) from private.bpay_next_job j
            join private.bpay_next_source_component_binding_version bv on j.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('BM',m.command_id,m.candidate_id,m.xmin::text) from private.bpay_next_command_member m
            join private.bpay_next_source_component_binding_version bv on m.command_id in (bv.publication_command_id_1,bv.publication_command_id_2)
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('EB',d.decision_bundle_id,d.xmin::text) from public.weekly_source_entitlement_decision_bundles d
            join private.bpay_next_source_component_binding_version bv on bv.decision_bundle_id=d.decision_bundle_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('RA',a.id,a.xmin::text) from public.weekly_source_root_authorisations a
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_authorisation_id=a.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TS',t.timesheet_id,t.xmin::text) from public.timesheets t
            join private.bpay_next_source_component_binding_version bv on bv.initial_root_timesheet_id=t.timesheet_id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('TF',f.id,f.xmin::text) from public.timesheets_financials f
            join private.bpay_next_source_component_binding_version bv on bv.initial_financial_snapshot_id=f.id
            join private.bpay_next_source_component_binding b on b.current_binding_version_id=bv.id where b.canonical_work_id=any(v_est_work_ids) and exists(select 1 from jsonb_array_elements(v_est_leaves) l where l->>'work_id'=b.canonical_work_id::text and l->'basis'->>'component_key'=b.canonical_component_key)
          union all select jsonb_build_array('CT',c.id,c.xmin::text) from public.contracts c
            join private.bpay_next_work w on w.contract_id=c.id where w.id=any(v_est_work_ids)
          union all select jsonb_build_array('SR',receipt.id,receipt.receipt_version) from source_receipts receipt
          union all select jsonb_build_array('SH',m.id,m.head_version) from source_members m
          union all select jsonb_build_array('SA',m.auth_id,m.auth_version) from source_members m
          union all select jsonb_build_array('SW',m.work_id,m.work_version) from source_members m
          union all select jsonb_build_array('SVR',m.revision_id,m.revision_version) from source_members m
          union all select jsonb_build_array('SP',m.command_id,m.publication_version) from source_members m
          union all select jsonb_build_array('OR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_work_collection o on r.id=o.original_revision_id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AR',r.id,r.xmin::text) from private.bpay_next_work_revision r
            join private.bpay_next_position p on p.applied_revision_id=r.id join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('OL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_work_collection o on o.original_approved_line_id=l.id where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('AL',l.id,l.xmin::text) from private.bpay_next_approved_line l
            join private.bpay_next_position p on p.applied_revision_id=l.revision_id and p.component_key=l.component_key join private.bpay_next_work_collection o on o.work_id=p.work_id and o.component_key=p.component_key where o.case_component_id=any(v_est_components)
          union all select jsonb_build_array('CQ',o.command_id,o.xmin::text) from private.bpay_next_case_create_request o
            join private.bpay_next_case_component cc on cc.case_id=o.case_id where cc.id=any(v_est_components)
          union all select jsonb_build_array('PW',pw.policy_id,pw.source_window_id,pw.xmin::text) from private.bpay_next_valuation_policy_window pw
            where (v_date_context->>'current_official_pay_date')::date>=pw.date_from
              and (v_date_context->>'current_official_pay_date')::date<=coalesce(pw.date_to,'infinity'::date)
              and pw.date_from=(select max(winner.date_from::date) from private.bpay_next_valuation_policy_window winner where winner.policy_id=pw.policy_id and (v_date_context->>'current_official_pay_date')::date>=winner.date_from and (v_date_context->>'current_official_pay_date')::date<=coalesce(winner.date_to,'infinity'::date))
              and (exists(select 1 from private.bpay_next_work_revision r join private.bpay_next_work w on w.current_revision_id=r.id
                where w.id=any(v_est_work_ids) and r.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_case_create_request o join private.bpay_next_case_component cc on cc.case_id=o.case_id
                  where cc.id=any(v_est_components) and o.valuation_policy_id=pw.policy_id)
                or exists(select 1 from private.bpay_next_work_collection o where o.case_component_id=any(v_est_components)
                  and o.original_valuation_policy_id=pw.policy_id))
          union all select jsonb_build_array('WK',w.candidate_id,w.pay_week_start,w.xmin::text) from private.bpay_next_worker_week w
            where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start
          union all select jsonb_build_array('WC',x.id,x.xmin::text) from private.bpay_next_worker_week_contribution x
            where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
              and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
        ) select coalesce(jsonb_agg(value order by value::text),'[]'::jsonb) into v_versions from (select distinct value from native) exact_versions;
        if v_est->'versions'<>'null'::jsonb and v_est->'versions' is distinct from v_versions then
          v_est_state:='OUTDATED';
        else
          for v_est_record in select (l->>'work_id')::uuid work_id,(l->>'revision_id')::uuid revision_id,
              l->'basis'->>'component_key' component_key,(l->>'approved_line_id')::uuid approved_line_id,
              sum((l->'basis'->>'included_source_ex_vat')::numeric) included_source,
              count(*) segment_count,bool_and(coalesce((l->'basis'->>'pay_hold')::boolean,false)=false) not_held
            from jsonb_array_elements(v_est_leaves) l where l->>'work_id' is not null
            group by l->>'work_id',l->>'revision_id',l->'basis'->>'component_key',l->>'approved_line_id' loop
            select p.* into strict v_est_position from private.bpay_next_position p
              join private.bpay_next_work w on w.id=p.work_id and w.current_revision_id=v_est_record.revision_id
                and w.applied_revision_id=v_est_record.revision_id and w.approval_state='APPROVED'
              where p.work_id=v_est_record.work_id and p.component_key=v_est_record.component_key
                and p.applied_revision_id=v_est_record.revision_id;
            if private.bpay_next_work_wait_state_v1(v_est_record.work_id) or not v_est_record.not_held
              or v_est_record.included_source is null or v_est_record.included_source<0
              or exists(select 1 from private.bpay_next_workbench_item i
                join private.bpay_next_workbench_group g on g.group_key=i.group_key and g.candidate_id=i.candidate_id
                where i.work_id=v_est_record.work_id and i.component_key=v_est_record.component_key
                  and i.portion_kind in ('AVAILABLE','ZERO') and (i.financial_view_revision<>v_est_financial
                    or g.financial_view_revision<>v_est_financial or not g.complete or g.dirty or i.progress_state<>'CURRENT'))
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'works') w
                where w->>'work_id'=v_est_record.work_id::text and w->>'expected_revision_id'<>v_est_record.revision_id::text)
              or exists(select 1 from jsonb_array_elements(v_descriptor->'intent'->'components') c
                where c->>'work_id'=v_est_record.work_id::text and c->>'expected_revision_id'<>v_est_record.revision_id::text) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            if exists(select 1 from private.bpay_next_work w where w.id=v_est_record.work_id and w.work_kind='SOURCE') then
              select bv.* into strict v_est_binding from private.bpay_next_source_component_binding b
                join private.bpay_next_source_component_binding_version bv on bv.id=b.current_binding_version_id
                where b.canonical_work_id=v_est_record.work_id and b.canonical_component_key=v_est_record.component_key
                  and bv.approval_work_id=v_est_record.work_id and bv.approval_revision_id=v_est_record.revision_id
                  and bv.approved_line_id=v_est_record.approved_line_id and bv.component_state='PRESENT'
                  and bv.candidate_id=v_est_candidate.id and bv.approval_component_key=v_est_record.component_key;
              -- Exact existing bound snapshot admission, without locks or a new certificate owner.
              if v_est_binding.origin_kind='INITIAL_AUTHORISED_TSFIN_V1' then
  select x.* into strict v_initial_a from public.weekly_source_root_authorisations x where x.id=v_est_binding.initial_root_authorisation_id;
  select x.* into strict v_initial_t from public.timesheets x where x.timesheet_id=v_initial_a.root_timesheet_id;
  select x.* into strict v_initial_c from public.contracts x where x.id=v_initial_t.contract_id;
  select x.* into strict v_initial_r from private.bpay_next_work_revision x where x.source_event_id=v_initial_a.id;
  select x.* into strict v_initial_w from private.bpay_next_work x where x.id=v_initial_r.work_id;
  select x.* into strict v_initial_f from public.timesheets_financials x where x.id=v_initial_r.financial_snapshot_id;
  select x.* into strict v_initial_p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(v_initial_a.id);
  if v_initial_a.withdrawn_at_utc is not null or v_initial_a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(v_initial_t.booking_id))
     or v_initial_t.is_current is distinct from true or v_initial_t.authorised_at_server is null
     or v_initial_t.revoked_at is not null or v_initial_t.archived_at_utc is not null
     or (v_initial_a.family_booking_id,v_initial_a.timesheet_version) is distinct from (v_initial_t.booking_id,v_initial_t.version)
     or (v_initial_w.work_kind,v_initial_w.candidate_id,v_initial_w.contract_id,v_initial_w.original_timesheet_id,v_initial_w.booking_id,v_initial_w.week_ending_date,v_initial_w.current_revision_id)
       is distinct from ('SOURCE'::text,v_initial_c.candidate_id,v_initial_c.id,v_initial_t.timesheet_id,v_initial_t.booking_id,v_initial_t.week_ending_date,v_initial_r.id)
     or v_initial_w.approval_state is distinct from 'APPROVED'
     or (v_initial_r.source_kind,v_initial_r.source_head_id,v_initial_r.source_event_id,v_initial_r.physical_timesheet_id,v_initial_r.physical_timesheet_version,v_initial_r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,v_initial_a.id,v_initial_t.timesheet_id,v_initial_t.version,v_initial_t.week_ending_date)
     or v_initial_r.approved_at_utc is null or v_initial_r.sealed_at_utc is null
     or v_initial_f.is_current is distinct from true or v_initial_f.is_stale is distinct from false or v_initial_f.authorised_at_utc is null
     or (v_initial_f.timesheet_id,v_initial_f.timesheet_version,v_initial_f.candidate_id,v_initial_f.client_id)
       is distinct from (v_initial_t.timesheet_id,v_initial_t.version,v_initial_w.candidate_id,v_initial_c.client_id)
     or (v_initial_p.work_id,v_initial_p.revision_id,v_initial_p.candidate_id,v_initial_p.revision_no)
       is distinct from (v_initial_w.id,v_initial_r.id,v_initial_w.candidate_id,v_initial_r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=v_initial_w.candidate_id
       where command.id=v_initial_p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=v_epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and v_initial_p.status='QUEUED' and v_initial_p.phase='NEW' and v_initial_p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and v_initial_p.status='APPLIED' and v_initial_p.phase='DONE' and v_initial_p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=v_initial_w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

  select x.* into strict v_initial_line from private.bpay_next_approved_line x where x.id=v_est_binding.approved_line_id;
  v_initial_details:=jsonb_build_object('segments',v_initial_f.invoice_breakdown_json->'segments','additional_units',v_initial_f.additional_units_json,
    'actual_schedule',v_initial_f.actual_schedule_json,'rate_source_refs',v_initial_f.rate_source_refs_json);
  if (v_est_binding.candidate_id,v_est_binding.canonical_work_id,v_est_binding.approval_work_id,v_est_binding.approval_revision_id,
      v_est_binding.initial_root_authorisation_id,v_est_binding.initial_financial_snapshot_id,v_est_binding.initial_root_timesheet_id,
      v_est_binding.initial_root_version,v_est_binding.initial_authorisation_generation,v_est_binding.initial_authorised_row_signature)
    is distinct from (v_initial_w.candidate_id,v_initial_w.id,v_initial_w.id,v_initial_r.id,v_initial_a.id,v_initial_f.id,
      v_initial_t.timesheet_id,v_initial_t.version,v_initial_a.authorisation_generation,v_initial_a.authorised_row_signature)
    or v_initial_r.source_inventory_digest is distinct from v_est_binding.initial_inventory_digest
    or (v_est_binding.approved_line_id,v_est_binding.classification_line_id,v_est_binding.canonical_component_key,v_est_binding.approval_component_key,
        v_est_binding.approval_source_channel,v_est_binding.tax_treatment,v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
      is distinct from (v_initial_line.id,v_initial_line.id,v_initial_line.component_key,v_initial_line.component_key,v_initial_r.source_pay_channel,
        v_initial_line.tax_treatment,v_initial_p.command_id,v_initial_w.id,v_initial_r.id)
    or (v_initial_line.revision_id,v_initial_line.source_component_id,v_initial_line.component_key,v_initial_line.evidence_ref)
      is distinct from (v_initial_r.id,v_est_binding.source_component_id,'SOURCE:'||v_est_binding.source_component_id::text,
        'source-component:'||v_est_binding.source_component_id::text||'#'||encode(v_est_binding.component_sha256,'hex'))
    or v_est_binding.initial_detail_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_initial_details)
    or v_est_binding.initial_policy_digest is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',v_initial_f.policy_snapshot_json)
    or v_est_binding.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_est_binding.canonical_work_id,
      v_est_binding.canonical_component_key,v_est_binding.approval_revision_id,v_est_binding.approved_line_id,v_est_binding.initial_root_authorisation_id,
      encode(v_est_binding.initial_certificate_digest,'hex'),v_est_binding.publication_command_id_1::text,encode(v_est_binding.component_sha256,'hex'))::text,'UTF8'))
  then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;

              else
                select x.* into strict v_source_receipt from private.weekly_source_entitlement_publication_receipts x where x.id=v_est_binding.source_receipt_id;
                if cardinality(v_source_receipt.head_ids) not between 1 and 2 or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
                  where b.decision_bundle_id=v_source_receipt.decision_bundle_id and b.bundle_revision=v_source_receipt.bundle_revision
                    and b.candidate_id=v_source_receipt.candidate_id and b.state='COMMITTED' and b.request_digest=v_source_receipt.request_digest
                    and b.decision_id=v_source_receipt.decision_id and b.decided_by_user_id=v_source_receipt.decided_by_user_id
                    and b.decision_bundle_id=v_est_binding.decision_bundle_id and b.bundle_revision=v_est_binding.bundle_revision) then
                  raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
                v_source_count:=0;
                for v_source_no in 1..cardinality(v_source_receipt.head_ids) loop
    select h.id,h.prior_head_id,w.id as work_id,r.id as revision_id,
           p.command_id,coalesce(p.predecessor_revision_id,
             case when p.command_id is null and w.current_revision_id is distinct from r.id
               then w.current_revision_id end) as predecessor_revision_id into strict v_source_member
      from public.weekly_source_entitlement_heads h
      join public.weekly_source_root_authorisations a on a.root_timesheet_id=h.root_timesheet_id
        and a.withdrawn_at_utc is null and a.current_entitlement_head_id=h.id
      join private.bpay_next_work w on w.original_timesheet_id=h.root_timesheet_id
        and w.booking_id=h.root_family_booking_id and w.candidate_id=h.candidate_id
        and w.contract_id=h.contract_id and w.week_ending_date=h.week_ending_date and w.work_kind='SOURCE'
      join private.bpay_next_work_revision r on r.work_id=w.id and r.source_head_id=h.id and r.source_event_id=h.id
        and r.physical_timesheet_id=h.root_timesheet_id and r.physical_timesheet_version=h.root_timesheet_version
        and r.source_kind in ('SOURCE','PROTECTED')
      left join private.bpay_next_publication p on p.work_id=w.id and p.revision_id=r.id
        and p.candidate_id=w.candidate_id
      where h.id=v_source_receipt.head_ids[v_source_no] and h.state='COMMITTED_CURRENT'
        and h.candidate_id=v_source_receipt.candidate_id and h.root_timesheet_id=v_source_receipt.member_root_ids[v_source_no]
        and h.root_family_booking_id=v_source_receipt.member_family_booking_ids[v_source_no]
        and h.root_timesheet_version=v_source_receipt.member_root_versions[v_source_no]
        and h.decision_bundle_id=v_source_receipt.decision_bundle_id and h.bundle_revision=v_source_receipt.bundle_revision
        and h.publication_receipt_digest=v_source_receipt.request_digest
        and h.scope_change_tx_token=v_source_receipt.scope_change_tx_token;

                  if (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (v_source_member.command_id,v_source_member.work_id,v_source_member.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2)) then v_source_count:=v_source_count+1;end if;
                end loop;
                if v_source_count<>(case when v_est_binding.publication_command_id_2 is null then 1 else 2 end) then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              end if;
              if (select count(*) from private.bpay_next_publication p
                join private.bpay_next_work w on w.id=p.work_id
                where p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
                  and w.current_revision_id=p.revision_id and w.applied_revision_id=p.revision_id
                  and ((p.command_id,p.work_id,p.revision_id) is not distinct from
                    (v_est_binding.publication_command_id_1,v_est_binding.publication_work_id_1,v_est_binding.publication_revision_id_1)
                    or (v_est_binding.publication_command_id_2 is not null and (p.command_id,p.work_id,p.revision_id) is not distinct from
                      (v_est_binding.publication_command_id_2,v_est_binding.publication_work_id_2,v_est_binding.publication_revision_id_2))))
                is distinct from (case when v_est_binding.publication_command_id_2 is null then 1 else 2 end)::bigint then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
              if v_est_position.source_book_state is distinct from 'READY' or coalesce(v_est_position.source_book_revision,0)<1
                or v_est_candidate.pay_method::text is distinct from v_est_position.source_basis_channel
                or v_est_position.source_basis_channel is distinct from v_est_position.source_book_seed_channel
                or v_est_binding.approval_source_channel is distinct from v_est_position.source_basis_channel
                or v_est_position.approved_source_ex_vat is distinct from (select l.source_pay_ex_vat from private.bpay_next_approved_line l where l.id=v_est_record.approved_line_id)
                or (v_est_position.native_realised_paye,v_est_position.native_held_paye,v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella) is distinct from
                  (case when v_est_position.source_basis_channel='PAYE' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='PAYE' then v_est_position.held_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.realised_source_ex_vat else 0::numeric end,
                   case when v_est_position.source_basis_channel='UMBRELLA' then v_est_position.held_source_ex_vat else 0::numeric end)
                or v_est_binding.tax_treatment is distinct from 'TAXABLE' or v_est_binding.classification_line_id is null
                or (v_est_position.source_basis_channel='PAYE' and (v_est_position.native_realised_paye,v_est_position.native_held_paye)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat))
                or (v_est_position.source_basis_channel='UMBRELLA' and (v_est_position.native_realised_umbrella,v_est_position.native_held_umbrella)
                  is distinct from (v_est_position.realised_source_ex_vat,v_est_position.held_source_ex_vat)) then
                raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_INPUT_UNBOUND';end if;
            end if;
            select x.* into strict v_est_value from private.bpay_next_work_revision r
              cross join lateral private.bpay_next_value_source_for_target_v1(v_est_record.included_source,r.source_pay_channel,
                v_est_candidate.pay_method::text,case when v_est_candidate.pay_method='UMBRELLA' then
                  (select u.vat_chargeable from public.umbrellas u where u.id=v_est_candidate.umbrella_id) else false end,
                r.valuation_policy_id,(v_date_context->>'current_official_pay_date')::date) x where r.id=v_est_record.revision_id;
            v_est_source:=v_est_source+v_est_record.included_source;v_est_gross:=v_est_gross+v_est_value.target_ex_vat;
          end loop;
          select cardinality(v_est_components)>0 and coalesce(bool_and((cc.case_kind,cc.instruction_kind,cc.payroll_stage)
            is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false) into v_est_all_overpayment
            from private.bpay_next_case_component cc where cc.id=any(v_est_components);
          v_est_h:=v_est_gross;
          select w.* into v_est_week from private.bpay_next_worker_week w where w.candidate_id=v_est_candidate.id and w.pay_week_start=v_est_week_start;
          if cardinality(v_est_components)>0 and not v_est_all_overpayment then
            if coalesce(v_est_week.unresolved_contribution_count,0)<>0 or exists(select 1 from private.bpay_next_run_worker w
              join private.bpay_next_pay_run r on r.id=w.run_id where w.candidate_id=v_est_candidate.id
                and w.status in ('READY','DRAFT','ISSUED','COMPLETE') and r.pay_date>=v_est_week_start
                and r.pay_date<=(v_date_context->>'current_official_pay_date')::date
                and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
              raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
            v_est_seen:=0;
            for v_est_record in select x.* from private.bpay_next_worker_week_contribution x
              where x.candidate_id=v_est_candidate.id and x.pay_week_start=v_est_week_start
                and x.original_pay_date<=(v_date_context->>'current_official_pay_date')::date
              order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit 1001 loop
              v_est_seen:=v_est_seen+1;
              if v_est_seen>1000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_BUDGET';end if;
              if v_est_record.eligibility_state='UNBOUND' then raise exception using errcode='23514',message='BPAY_NEXT_ESTIMATE_WEEK_UNBOUND';end if;
              v_est_h:=v_est_h+v_est_record.eligible_arranged_amount;
            end loop;
          end if;
          for v_est_record in select cc.* from private.bpay_next_case_component cc where cc.id=any(v_est_components)
            order by cc.case_id,cc.component_ordinal,cc.id loop
            v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
              'candidate_id',v_est_candidate.id,'case_id',v_est_record.case_id,'case_component_id',v_est_record.id,
              'pay_date',v_date_context->>'current_official_pay_date','pay_week_start',v_est_week_start,
              'target_pay_channel',v_est_candidate.pay_method::text,'captured_default_floor',v_est_candidate.min_take_home_wtd,
              'captured_default_floor_revision',coalesce(v_est_week.default_floor_revision,1),
              'weekly_binding_state',case when v_est_all_overpayment then 'NOT_AFFECTED' else 'BOUND' end,'accepted_run_id',null));
            v_est_instructions:=v_est_instructions||jsonb_build_array(v_est_plan->'instruction');
          end loop;
          v_est_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
            'instructions',v_est_instructions,'channel_headroom',v_est_gross,'worker_take_home',v_est_h));
          v_est_scope:=jsonb_build_object('descriptor',v_descriptor,'expected_financial_view_revision',v_est_financial::text,'versions',v_versions);
          v_context:=v_context||jsonb_build_object('estimate_scope',v_est_scope);
          v_after:=null;v_est_last:=null;v_est_seen:=0;
          if p_request->'after'<>'null'::jsonb then
            if jsonb_typeof(p_request->'after')<>'object' or not(p_request->'after' ?& array['context','key'])
              or (p_request->'after')-array['context','key']<>'{}'::jsonb or p_request->'after'->'context' is distinct from v_context
              or (p_request->'after'->'key'<>'null'::jsonb and (jsonb_typeof(p_request->'after'->'key')<>'array' or jsonb_array_length(p_request->'after'->'key')<>4))
              or (p_request->'after'->'key'='null'::jsonb and (jsonb_array_length(v_est_plan->'rows')<>0 or v_est->'verify_only'<>'true'::jsonb)) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
            v_after:=nullif(p_request->'after'->'key','null'::jsonb);
            if v_after is not null and not exists(select 1 from jsonb_array_elements(v_est_plan->'rows') r where
              jsonb_build_array(r->'instruction'->>'age_key',r->'instruction'->>'case_id',r->'instruction'->>'component_ordinal',r->'instruction'->>'case_component_id')=v_after) then
              raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          end if;
          for v_est_row in select value from jsonb_array_elements(v_est_plan->'rows') loop
            if v_est_row->'instruction'->>'payroll_stage'='GROSS_ADD' then v_est_gross_add:=v_est_gross_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='GROSS_DEDUCT' then v_est_gross_deduct:=v_est_gross_deduct+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_ADD' then v_est_net_add:=v_est_net_add+(v_est_row->>'taken_amount')::numeric;
            elsif v_est_row->'instruction'->>'payroll_stage'='NET_DEDUCT' then v_est_net_deduct:=v_est_net_deduct+(v_est_row->>'taken_amount')::numeric;end if;
            v_est_key:=jsonb_build_array(v_est_row->'instruction'->>'age_key',v_est_row->'instruction'->>'case_id',
              v_est_row->'instruction'->>'component_ordinal',v_est_row->'instruction'->>'case_component_id');
            if v_after is not null and ((v_est_key->>0)::bigint,(v_est_key->>1)::uuid,(v_est_key->>2)::bigint,(v_est_key->>3)::uuid)
              <=((v_after->>0)::bigint,(v_after->>1)::uuid,(v_after->>2)::bigint,(v_after->>3)::uuid) then continue;end if;
            v_est_seen:=v_est_seen+1;
            if v_est_seen<=v_limit and v_est->'verify_only'='false'::jsonb then
              v_est_last:=v_est_key;
              v_est_rows:=v_est_rows||jsonb_build_array(jsonb_build_object('case_id',v_est_row->'instruction'->'case_id',
                'case_component_id',v_est_row->'instruction'->'case_component_id','age_key',v_est_key->>0,
                'component_ordinal',v_est_key->>2,'recoverable_this_run',v_est_row->>'taken_amount',
                'cap_reason',v_est_row->'cap_reason','affordability_reason',v_est_row->'affordability_reason',
                'capacity_reason',v_est_row->'capacity_reason','payroll_stage',v_est_row->'instruction'->'payroll_stage'));
            end if;
          end loop;
          if v_est->'verify_only'='true'::jsonb and v_est_seen<>0 then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
          v_est_bound:=jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after));
          -- Conservative URL encoding bound checked again exactly by Worker/Office.
          if octet_length((p_request||jsonb_build_object('after',v_est_bound,'estimate',v_est||jsonb_build_object('versions',v_versions)))::text)*3>32768 then
            raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          v_response:=jsonb_build_object('ok',true,'kind',v_kind,'state','ESTIMATE','context',v_context,
            'workbench_revision',v_revision.workbench_revision::text,'complete',v_est_seen<=v_limit,'rows',v_est_rows,
            'next_cursor',case when v_est_seen>v_limit then v_est_bound else null end,
            'verification_cursor',jsonb_build_object('context',v_context,'key',coalesce(v_est_last,v_after)),
            'estimated_gross_source_ex_vat',v_est_source::text,'estimated_gross_target_ex_vat',(v_est_gross+v_est_gross_add-v_est_gross_deduct)::text,
            'estimated_gross_deductions',v_est_gross_deduct::text,'estimated_net_additions',v_est_net_add::text,'estimated_net_deductions',v_est_net_deduct::text,
            'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
            'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_est_last,'row_count',jsonb_array_length(v_est_rows)::text));
          if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_ESTIMATE_SCOPE_TOO_LARGE';end if;
          return v_response;
        end if;
      exception when check_violation or no_data_found or too_many_rows or program_limit_exceeded then
        v_est_state:='PENDING';
      when object_not_in_prerequisite_state then
        if sqlerrm='BPAY_NEXT_COMPARISON_NOT_INSTALLED' then v_est_state:='PENDING';else raise;end if;
      end;
    end if;
    v_context:=v_context-'estimate_scope';
    return jsonb_build_object('ok',true,'kind',v_kind,'state',v_est_state,'context',v_context,
      'workbench_revision',v_revision.workbench_revision::text,'complete',true,'rows','[]'::jsonb,
      'next_cursor',null,'verification_cursor',null,'estimated_gross_source_ex_vat',null,'estimated_gross_target_ex_vat',null,
      'estimated_gross_deductions',null,'estimated_net_additions',null,'estimated_net_deductions',null,
      'financial_view_revision_before',v_est_financial::text,'financial_view_revision_after',v_est_financial::text,
      'affected_candidate_count','1','page_receipt',jsonb_build_object('context',v_context,'start_key',null,'end_key',null,'row_count','0'));
  elsif p_request->'estimate'<>'null'::jsonb then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';
  end if;

  v_read_scope:=v_scope||jsonb_build_object('_selection_memory',v_memory);
  v_cursor:=p_request->'after';
  if v_cursor<>'null'::jsonb then
    if jsonb_typeof(v_cursor)<>'object' or not(v_cursor ?& array['context','key'])
      or v_cursor-array['context','key']<>'{}'::jsonb or v_cursor->'context' is distinct from v_context then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
    v_after:=v_cursor->'key';
    if v_kind='EXPORT_ROWS' then
      if jsonb_typeof(v_after)<>'array' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      if v_sort in ('C_NAME_ASC','C_NAME_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'string' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_AMOUNT_ASC','C_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_DEDUCTIONS_ASC','C_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'boolean' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('C_STATUS_ASC','C_STATUS_DESC') then
        if jsonb_array_length(v_after)<>6 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'boolean' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or octet_length(v_after->>3)>8192 or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->5)<>'string' or v_after->>5!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_PAYMENT_ASC','G_PAYMENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CLIENT_ASC','G_CLIENT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_CHANNEL_ASC','G_CHANNEL_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_AMOUNT_ASC','G_AMOUNT_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$' or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_DEDUCTIONS_ASC','G_DEDUCTIONS_DESC') then
        if jsonb_array_length(v_after)<>4 or jsonb_typeof(v_after->0)<>'boolean' or octet_length(v_after->>0)>8192 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or v_after->>2!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      elsif v_sort in ('G_REASON_ASC','G_REASON_DESC') then
        if jsonb_array_length(v_after)<>5 or jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string' or octet_length(v_after->>1)>8192 or jsonb_typeof(v_after->2)<>'string' or octet_length(v_after->>2)>8192 or jsonb_typeof(v_after->3)<>'string' or v_after->>3!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' or jsonb_typeof(v_after->4)<>'string' or v_after->>4!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
      else raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    else
    v_key_count:=case when v_kind in ('CANDIDATES','PICK_CANDIDATES') then case when v_sort like 'DEDUCTIONS_%' or v_sort like 'STATUS_%' then 5 when v_sort like 'AMOUNT_%' then 3 else 4 end
      when v_kind='CANDIDATE_GROUPS' then case when v_sort like 'DEDUCTIONS_%' then 3 else 4 end when v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then case when v_sort like 'PAYMENT_%' then 2 else 3 end else 1 end;
    if jsonb_typeof(v_after)<>'array' or jsonb_array_length(v_after)<>v_key_count then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    if v_kind in ('CANDIDATES','PICK_CANDIDATES') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string'
        or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'DEDUCTIONS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or jsonb_typeof(v_after->2)<>'boolean' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'STATUS_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string' or jsonb_typeof(v_after->3)<>'string'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'boolean' or jsonb_typeof(v_after->2)<>'string')) then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind='CANDIDATE_GROUPS' then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(v_after->(v_key_count-2))<>'string' or v_after->>(v_key_count-2) not in ('WORK','CASE','ROW')
        or (v_sort like 'DEDUCTIONS_%' and jsonb_typeof(v_after->0)<>'boolean')
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^[1-9][0-9]{0,2}$' or (v_after->>0)::numeric>200 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort not like 'DEDUCTIONS_%' and v_sort not like 'REASON_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string'))
        or ((v_sort='NAME_ASC' or v_sort like 'PAYMENT_%') and ((v_after->>0='true' and v_after->>1<>'') or (v_after->>0='false' and v_after->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')))
        or (v_sort like 'AMOUNT_%' and v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif v_kind in ('ACTION_REQUIRED','BLOCKED','ISSUE_DETAIL') then
      if jsonb_typeof(v_after->(v_key_count-1))<>'string' or v_after->>(v_key_count-1)!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or (v_sort like 'AMOUNT_%' and (jsonb_typeof(v_after->0)<>'boolean' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^-?(0|[1-9][0-9]*)(\.[0-9]+)?$'))
        or (v_sort like 'NAME_%' and (jsonb_typeof(v_after->0)<>'string' or jsonb_typeof(v_after->1)<>'string' or v_after->>1!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
        or (v_sort like 'REASON_%' and (jsonb_typeof(v_after->0)<>'number' or v_after->>0!~'^(0|[1-9][0-9]*)$'
          or (v_after->>0)::numeric>32767 or jsonb_typeof(v_after->1)<>'string'))
        or (v_sort like 'PAYMENT_%' and jsonb_typeof(v_after->0)<>'string') then
        raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    elsif jsonb_typeof(v_after->0)<>'string' or (v_kind='CANDIDATE_GROUPS'
      and v_after->>0!~'^(WORK|CASE):[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
      or (v_kind<>'CANDIDATE_GROUPS' and v_after->>0!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
    end if;
  end if;
  case v_kind
    when 'HEADER' then v_page:=private.bpay_next_workbench_header_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATES' then v_page:=private.bpay_next_workbench_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'ACTION_REQUIRED' then v_page:=private.bpay_next_workbench_action_required_page_v1(v_read_scope,v_after,v_limit);
    when 'BLOCKED' then v_page:=private.bpay_next_workbench_blocked_page_v1(v_read_scope,v_after,v_limit);
    when 'ISSUE_DETAIL' then v_page:=private.bpay_next_workbench_issue_detail_page_v1(v_read_scope,v_after,v_limit);
    when 'CANDIDATE_GROUPS' then v_page:=private.bpay_next_workbench_candidate_groups_page_v1(v_read_scope,v_after,v_limit);
    when 'GROUP_LINES' then v_page:=private.bpay_next_workbench_group_lines_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CANDIDATES' then v_page:=private.bpay_next_workbench_pick_candidates_page_v1(v_read_scope,v_after,v_limit);
    when 'PICK_CLIENTS' then v_page:=private.bpay_next_workbench_pick_clients_page_v1(v_read_scope,v_after,v_limit);
    when 'EXPORT_ROWS' then v_page:=private.bpay_next_workbench_export_rows_page_v1(v_read_scope,v_after,v_limit);
  end case;
  v_response:=jsonb_build_object('ok',true,'kind',v_kind,'workbench_revision',v_revision.workbench_revision::text,
    'complete',v_page->'complete','context',v_context,'rows',v_page->'rows',
    'affected_candidate_count',v_page->'affected_candidate_count',
    'next_cursor',case when v_page->>'complete'='true' then null else jsonb_build_object('context',v_context,'key',v_page->'last_key') end,
    'page_receipt',jsonb_build_object('context',v_context,'start_key',v_after,'end_key',v_page->'last_key',
      'row_count',jsonb_array_length(v_page->'rows')::text));
  if v_kind='HEADER' then
    v_response:=v_response||jsonb_build_object('job_progress',v_progress);
    v_response:=jsonb_set(v_response,'{rows,0}',(v_response#>'{rows,0}')||
      jsonb_build_object('oldest_pending_age_seconds',v_progress->'oldest_pending_age_seconds','can_create_draft',true));
  end if;
  if octet_length(v_response::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return v_response;
exception when others then
  if p_request->>'kind'='SNOOZE_CHECK' then raise;end if;
  v_error:=case
    when sqlstate='42501' then 'BPAY_NEXT_WORKBENCH_FORBIDDEN'
    when sqlstate in ('22023','22P02','22003','22007','22008') then 'BPAY_NEXT_WORKBENCH_REQUEST_INVALID'
    when sqlstate='54000' then 'BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE'
    when sqlerrm in ('BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_WORKBENCH_CURSOR_STALE',
      'BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT','BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE') then sqlerrm
    else 'BPAY_NEXT_WORKBENCH_UNAVAILABLE' end;
  return jsonb_build_object('ok',false,'error_code',v_error,'retryable',v_error in
    ('BPAY_NEXT_WORKBENCH_UNAVAILABLE','BPAY_NEXT_WORKBENCH_PROJECTION_NOT_CURRENT'),
    'reload_required',v_error='BPAY_NEXT_WORKBENCH_CURSOR_STALE',
    'workbench_revision',v_revision.workbench_revision::text,'complete',false,
    'projection_state',v_revision.adoption_state,'dirty_candidate_count',v_revision.dirty_candidate_count::text,
    'dirty_group_count',v_revision.dirty_group_count::text,'pending_task_count',v_revision.pending_task_count::text);
end
$function$;

alter function private.bpay_next_snooze_management_row_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_snooze_management_row_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_management_page_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_management_page_v1(uuid,jsonb) from public,anon,authenticated,service_role;
alter function public.bpay_next_workbench_page_v1(jsonb) owner to postgres;
revoke all on function public.bpay_next_workbench_page_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_workbench_page_v1(jsonb) to service_role;
commit;
