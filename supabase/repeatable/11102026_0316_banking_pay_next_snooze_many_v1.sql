-- WB-38 card-named MANY actions. Native single-child writes keep 0255 guards unchanged.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_snooze_scope_at_cut_v1(p_work_id uuid,p_scope text,p_identity text,p_cut bigint)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select e.after_state from private.bpay_next_snooze_event e
    where e.after_state->>'work_id'=p_work_id::text and e.after_state->>'scope_kind'=p_scope
      and e.after_state->>'scope_identity'=p_identity and e.control_sequence<=p_cut
    order by e.control_sequence desc,e.id desc limit 1
$function$;
alter function private.bpay_next_snooze_scope_at_cut_v1(uuid,text,text,bigint) owner to postgres;
revoke all on function private.bpay_next_snooze_scope_at_cut_v1(uuid,text,text,bigint) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_snooze_many_v1(p_actor uuid,p_action text,p_args jsonb,p_readback boolean)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
set statement_timeout='5s'
as $function$
declare
  v_parent private.bpay_next_snooze_many_request%rowtype;v_page private.bpay_next_snooze_many_page%rowtype;
  v_line private.bpay_next_snooze_many_line%rowtype;v_work private.bpay_next_work%rowtype;
  v_request uuid;v_candidate uuid;v_work_id uuid;v_revision uuid;v_epoch bigint;v_page_no bigint;v_cut bigint;v_sequence bigint;
  v_after integer;v_retry jsonb;v_body jsonb;v_context jsonb;v_args jsonb;v_check jsonb;v_ack jsonb;v_result jsonb;
  v_rows jsonb:='[]';v_row jsonb;v_shared jsonb;v_control jsonb;v_active jsonb;v_code text;
  v_last integer;v_more boolean;v_new boolean:=false;v_total bigint;v_success bigint;v_failed bigint;
  v_line_nos integer[];v_key text;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor is null or not exists(select 1 from public.tms_users u where u.id=p_actor and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_FORBIDDEN';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  if p_action is null or p_action not in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') or p_readback is null
    or p_args is null or jsonb_typeof(p_args)<>'object' or octet_length(p_args::text)>24576
    or not(p_args ?& array['request_id','candidate_id','work_id','expected_revision_id','worker_pay_basis','control_cut',
      'until_date','indefinite','note','warning_context','page_no','after_line_no','retry_line_nos'])
    or p_args-array['request_id','candidate_id','work_id','expected_revision_id','worker_pay_basis','control_cut',
      'until_date','indefinite','note','warning_context','page_no','after_line_no','retry_line_nos']<>'{}'::jsonb
    or p_args->>'page_no'!~'^[1-9][0-9]{0,18}$' or p_args->>'control_cut'!~'^(0|[1-9][0-9]{0,18})$'
    or jsonb_typeof(p_args->'retry_line_nos')<>'array' or jsonb_array_length(p_args->'retry_line_nos')>100
    or exists(select 1 from jsonb_array_elements(p_args->'retry_line_nos') n where jsonb_typeof(n)<>'string' or n#>>'{}'!~'^[1-9][0-9]{0,9}$')
    or (select count(*) from jsonb_array_elements(p_args->'retry_line_nos'))<>(select count(distinct n) from jsonb_array_elements(p_args->'retry_line_nos') n)
    or (p_args->'after_line_no'<>'null'::jsonb and p_args->>'after_line_no'!~'^(0|[1-9][0-9]{0,9})$')
    or (jsonb_array_length(p_args->'retry_line_nos')>0 and p_args->'after_line_no'<>'null'::jsonb) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID';end if;
  foreach v_key in array array['request_id','candidate_id','work_id','expected_revision_id'] loop
    if jsonb_typeof(p_args->v_key) is distinct from 'string' or p_args->>v_key!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID';end if;
  end loop;
  foreach v_key in array array['page_no','control_cut'] loop
    if jsonb_typeof(p_args->v_key) is distinct from 'string' or length(p_args->>v_key)>19
      or (length(p_args->>v_key)=19 and p_args->>v_key>'9223372036854775807') then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID';end if;
  end loop;
  if jsonb_typeof(p_args->'worker_pay_basis') is distinct from 'string' or p_args->>'worker_pay_basis'!~'^[0-9a-f]{64}$'
    or jsonb_typeof(p_args->'indefinite') is distinct from 'boolean' or jsonb_typeof(p_args->'note') is distinct from 'string'
    or length(p_args->>'note')>500 or jsonb_typeof(p_args->'warning_context') is distinct from 'object'
    or jsonb_typeof(p_args->'until_date') not in ('string','null')
    or (p_args->'until_date'<>'null'::jsonb and p_args->>'until_date'!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    or (p_args->>'indefinite')::boolean is distinct from (p_args->'until_date'='null'::jsonb)
    or (p_args->'after_line_no'<>'null'::jsonb and (jsonb_typeof(p_args->'after_line_no')<>'string' or (p_args->>'after_line_no')::numeric>2147483647))
    or exists(select 1 from jsonb_array_elements_text(p_args->'retry_line_nos') n where n::numeric>2147483647) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID';end if;
  v_request:=(p_args->>'request_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  v_work_id:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_page_no:=(p_args->>'page_no')::bigint;v_cut:=(p_args->>'control_cut')::bigint;
  v_after:=coalesce(nullif(p_args->>'after_line_no','')::integer,0);v_retry:=p_args->'retry_line_nos';
  v_body:=p_args-array['page_no','after_line_no','retry_line_nos'];
  select r.* into v_parent from private.bpay_next_snooze_many_request r where r.request_id=v_request;
  if found and (v_parent.actor_id,v_parent.action,v_parent.module_epoch,v_parent.request_body)
    is distinct from (p_actor,p_action,v_epoch,v_body) then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
  select p.* into v_page from private.bpay_next_snooze_many_page p where p.request_id=v_request and p.page_no=v_page_no;
  if found then
    if v_page.request_body is distinct from p_args then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
    return v_page.result_body||jsonb_build_object('replay',true);
  end if;
  if p_readback then return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'page_no',v_page_no::text,
    'received',false,'replay',false,'result',null);end if;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select w.* into v_work from private.bpay_next_work w where w.id=v_work_id and w.candidate_id=v_candidate for update;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WORK_AUTHORITY_MISMATCH';end if;
  select r.* into v_parent from private.bpay_next_snooze_many_request r where r.request_id=v_request for update;
  if found then
    if (v_parent.actor_id,v_parent.action,v_parent.module_epoch,v_parent.request_body) is distinct from (p_actor,p_action,v_epoch,v_body) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
    select p.* into v_page from private.bpay_next_snooze_many_page p where p.request_id=v_request and p.page_no=v_page_no;
    if found then
      if v_page.request_body is distinct from p_args then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';end if;
      return v_page.result_body||jsonb_build_object('replay',true);
    end if;
  else
    if v_page_no<>1 or v_after<>0 or jsonb_array_length(v_retry)<>0 then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_FIRST_PAGE_REQUIRED';end if;
    v_args:=jsonb_build_object('candidate_id',v_candidate,'work_id',v_work_id,'expected_revision_id',v_revision,
      'scope_kind','EXPENSES_ALL','snooze_kind','DO_NOT_PAY','until_date',p_args->'until_date',
      'indefinite',p_args->'indefinite','note',p_args->'note','phase','PRE_SAVE');
    v_context:=private.bpay_next_snooze_context_v1(p_actor,v_args);
    if p_args->>'worker_pay_basis' is distinct from v_context->>'worker_pay_basis'
      or p_args->'warning_context' is distinct from v_context->'warning_context'
      or v_cut<0 or v_cut>(v_context->>'control_cut')::bigint
      or (p_action='SNOOZE_CLEAR_MANY' and (p_args->'until_date'<>'null'::jsonb or p_args->'indefinite'<>'true'::jsonb or p_args->>'note'<>'')) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE';end if;
    v_sequence:=private.bpay_next_receive_command_v1(v_request,p_action);
    insert into private.bpay_next_snooze_many_request(request_id,actor_id,candidate_id,work_id,revision_id,
      physical_timesheet_id,module_epoch,action,request_body,control_cut,worker_pay_basis,warning_context,aggregate_control,whole_control,total_count)
    values(v_request,p_actor,v_candidate,v_work_id,v_revision,(v_context->>'physical_timesheet_id')::uuid,v_epoch,p_action,
      v_body,v_cut,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context',
      private.bpay_next_snooze_scope_at_cut_v1(v_work_id,'EXPENSES_ALL',v_work_id::text,v_cut),
      private.bpay_next_snooze_scope_at_cut_v1(v_work_id,'WHOLE_TIMESHEET',v_work_id::text,v_cut),(v_context->>'expense_count')::bigint) returning * into v_parent;
    v_new:=true;
  end if;
  if jsonb_array_length(v_retry)>0 and exists(select 1 from jsonb_array_elements_text(v_retry) n
    where not exists(select 1 from private.bpay_next_snooze_many_line l where l.request_id=v_request and l.line_no=n::integer
      and not l.successful and l.last_result is not null)) then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_MANY_RETRY_NOT_FAILED';end if;
  if v_page_no<>v_parent.next_page_no or (jsonb_array_length(v_retry)=0 and v_after is distinct from v_parent.next_after_line_no) then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_MANY_CURSOR_INVALID';end if;
  -- Sealed revision + native all-expense basis freeze COMPLETE membership once.
  -- Materialize only this <=100-line slice, using its original indexed control cut.
  if jsonb_array_length(v_retry)=0 then
    insert into private.bpay_next_snooze_many_line(request_id,line_no,approved_line_id,component_key,label,amount,worker_pay_basis,initial_control)
      select v_request,a.line_no,a.id,a.component_key,coalesce(nullif(a.unit_label,''),initcap(a.component_kind)),a.source_pay_ex_vat::text,
        private.bpay_next_line_pay_basis_v1(a.id),private.bpay_next_snooze_scope_at_cut_v1(v_work_id,'EXPENSE_LINE',v_work_id::text||':'||a.component_key,v_cut)
      from (select a.* from private.bpay_next_approved_line a where a.revision_id=v_revision and a.component_kind in ('EXPENSE','MILEAGE')
        and a.line_no>v_after order by a.line_no limit 100) a on conflict(request_id,line_no) do nothing;
  end if;
  if jsonb_array_length(v_retry)=0 then
    select array_agg(x.line_no) into v_line_nos from (select l.line_no from private.bpay_next_snooze_many_line l
      where l.request_id=v_request and l.line_no>v_after order by l.line_no limit 100) x;
  else
    select array_agg(n::integer) into v_line_nos from jsonb_array_elements_text(v_retry) n;
  end if;
  for v_line in select l.* from private.bpay_next_snooze_many_line l where l.request_id=v_request and l.line_no=any(v_line_nos)
    order by l.line_no limit 100 for update loop
    v_last:=v_line.line_no;
    if v_line.successful then v_rows:=v_rows||jsonb_build_array(v_line.last_result);continue;end if;
    v_code:=null;v_result:=null;
    begin
      if v_work.current_revision_id is distinct from v_revision or v_work.approval_state<>'APPROVED'
        or private.bpay_next_line_pay_basis_v1(v_line.approved_line_id) is distinct from v_line.worker_pay_basis then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_STALE';end if;
      v_control:=v_parent.aggregate_control;
      select to_jsonb(s) into v_active from private.bpay_next_snooze s where s.work_id=v_work_id and s.active and s.scope_kind='EXPENSES_ALL';
      if coalesce((v_control->>'active')::boolean,false) then
        if v_parent.aggregate_result is null then
          if v_active is null or v_active->>'id' is distinct from v_control->>'id' or v_active->>'version' is distinct from v_control->>'version' then
            raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';end if;
          if p_action='SNOOZE_SET_MANY' then
            v_args:=jsonb_build_object('candidate_id',v_candidate,'work_id',v_work_id,'expected_revision_id',v_revision,'scope_kind','EXPENSES_ALL',
              'snooze_kind','DO_NOT_PAY','until_date',v_body->'until_date','indefinite',v_body->'indefinite','note',v_body->'note','phase','PRE_SAVE',
              'expected_version',v_control->>'version','worker_pay_basis',encode(v_parent.worker_pay_basis,'hex'));
            v_check:=private.bpay_next_snooze_context_v1(p_actor,v_args);
            if v_check->'warning_context' is distinct from v_parent.warning_context then
              raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WARNING_ACK_REQUIRED_OR_STALE';end if;
            v_ack:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_WARNING_ACK_ISSUE',v_args||jsonb_build_object('request_id',v_parent.aggregate_ack_request_id));
            v_shared:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_SET',v_args||jsonb_build_object('request_id',v_parent.aggregate_mutation_request_id,
              'acknowledgement_token',v_ack->'acknowledgement_token'));
          else
            v_shared:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_CLEAR',jsonb_build_object('request_id',v_parent.aggregate_mutation_request_id,
              'candidate_id',v_candidate,'snooze_id',v_control->'id','expected_version',v_control->>'version'));
          end if;
          update private.bpay_next_snooze_many_request r set aggregate_result=v_shared where r.request_id=v_request;
          v_parent.aggregate_result:=v_shared;
        elsif p_action='SNOOZE_SET_MANY' and (v_active is null or v_active->>'id' is distinct from v_parent.aggregate_result->>'snooze_id'
          or v_active->>'version' is distinct from v_parent.aggregate_result->>'version')
          or p_action='SNOOZE_CLEAR_MANY' and v_active is not null then
          raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';
        end if;
      elsif v_active is not null then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';end if;
      v_control:=v_line.initial_control;
      if p_action='SNOOZE_CLEAR_MANY' or v_parent.aggregate_result is not null then
        if coalesce((v_control->>'active')::boolean,false) then
          v_result:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_CLEAR',jsonb_build_object('request_id',v_line.mutation_request_id,
            'candidate_id',v_candidate,'snooze_id',v_control->'id','expected_version',v_control->>'version'));
        else
          if exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work_id and s.active and s.scope_kind='EXPENSE_LINE'
            and s.scope_identity=v_work_id::text||':'||v_line.component_key) then
            raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';end if;
          v_result:=v_parent.aggregate_result;
        end if;
      else
        v_args:=jsonb_build_object('candidate_id',v_candidate,'work_id',v_work_id,'expected_revision_id',v_revision,'scope_kind','EXPENSE_LINE',
          'component_key',v_line.component_key,'snooze_kind','DO_NOT_PAY','until_date',v_body->'until_date','indefinite',v_body->'indefinite',
          'note',v_body->'note','phase','PRE_SAVE','expected_version',case when coalesce((v_control->>'active')::boolean,false) then v_control->>'version' else '0' end,
          'worker_pay_basis',encode(v_line.worker_pay_basis,'hex'));
        v_check:=private.bpay_next_snooze_context_v1(p_actor,v_args);
        if v_check->'warning_context' is distinct from v_parent.warning_context then
          raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WARNING_ACK_REQUIRED_OR_STALE';end if;
        v_ack:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_WARNING_ACK_ISSUE',v_args||jsonb_build_object('request_id',v_line.ack_request_id));
        v_result:=private.bpay_next_snooze_action_v1(p_actor,'SNOOZE_SET',v_args||jsonb_build_object('request_id',v_line.mutation_request_id,'acknowledgement_token',v_ack->'acknowledgement_token'));
      end if;
    exception when others then
      get stacked diagnostics v_code=message_text;
      if v_code!~'^[A-Z][A-Z0-9_]{0,240}$' then v_code:='BPAY_NEXT_SNOOZE_MANY_ITEM_FAILED';end if;
      -- Subtransaction rolled back all native child writes. Do not retain an uncommitted shared result.
      select r.* into strict v_parent from private.bpay_next_snooze_many_request r where r.request_id=v_request;
    end;
    if v_code is null then
      v_parent.successful_count:=v_parent.successful_count+1;
      if v_line.last_result is not null then v_parent.failed_count:=v_parent.failed_count-1;end if;
    elsif v_line.last_result is null then v_parent.failed_count:=v_parent.failed_count+1;end if;
    update private.bpay_next_snooze_many_request r set successful_count=v_parent.successful_count,failed_count=v_parent.failed_count where r.request_id=v_request;
    v_row:=jsonb_build_object('line_no',v_line.line_no::text,'approved_line_id',v_line.approved_line_id,'component_key',v_line.component_key,
      'label',v_line.label,'amount',v_line.amount,'successful',v_code is null,'reason_code',v_code,
      'whole_timesheet_snoozed',exists(select 1 from private.bpay_next_snooze s join private.bpay_next_revision_worker_basis b on b.revision_id=v_revision
        where s.work_id=v_work_id and s.scope_kind='WHOLE_TIMESHEET' and s.active and s.worker_pay_basis=b.whole_basis
          and (s.until_date is null or s.until_date>=(current_timestamp at time zone 'Europe/London')::date)));
    update private.bpay_next_snooze_many_line l set successful=v_code is null,last_result=v_row where l.request_id=v_request and l.line_no=v_line.line_no;
    v_rows:=v_rows||jsonb_build_array(v_row);
  end loop;
  v_total:=v_parent.total_count;v_success:=v_parent.successful_count;v_failed:=v_parent.failed_count;
  v_more:=jsonb_array_length(v_retry)=0 and v_last is not null and exists(select 1 from private.bpay_next_approved_line a
    where a.revision_id=v_revision and a.component_kind in ('EXPENSE','MILEAGE') and a.line_no>v_last);
  v_result:=jsonb_build_object('rows',v_rows,'complete',not v_more,'next_after_line_no',case when v_more then v_last::text end,
    'total_count',v_total::text,'successful_count',v_success::text,'failed_count',v_failed::text);
  v_result:=jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'page_no',v_page_no::text,'received',true,'replay',false,'result',v_result);
  insert into private.bpay_next_snooze_many_page(request_id,page_no,request_body,result_body) values(v_request,v_page_no,p_args,v_result);
  update private.bpay_next_snooze_many_request r set next_page_no=r.next_page_no+1,
    next_after_line_no=case when jsonb_array_length(v_retry)>0 then r.next_after_line_no when v_more then v_last end where r.request_id=v_request;
  if v_new then update private.bpay_next_command set status='COMPLETE',expected_member_count=0,sealed_at_utc=clock_timestamp() where id=v_request;end if;
  return v_result;
exception when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
  raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_MANY_REQUEST_INVALID';
end
$function$;
alter function private.bpay_next_snooze_many_v1(uuid,text,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_snooze_many_v1(uuid,text,jsonb,boolean) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_snooze_many_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_parent private.bpay_next_snooze_many_request%rowtype;v_command private.bpay_next_command%rowtype;
begin
  if tg_op in ('DELETE','TRUNCATE') or coalesce(current_setting('request.jwt.claim.role',true),
    nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_IMMUTABLE';end if;
  select c.* into strict v_command from private.bpay_next_command c where c.id=new.request_id;
  if v_command.command_kind not in ('SNOOZE_SET_MANY','SNOOZE_CLEAR_MANY') or v_command.status not in ('RECEIVED','COMPLETE')
    or not exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_OWNER_INVALID';end if;
  if tg_table_name='bpay_next_snooze_many_request' then
    if tg_op='INSERT' then
      if new.module_epoch<>v_command.module_epoch or new.action<>v_command.command_kind
        or (new.request_body->>'request_id')::uuid is distinct from new.request_id
        or (new.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (new.request_body->>'work_id')::uuid is distinct from new.work_id
        or (new.request_body->>'expected_revision_id')::uuid is distinct from new.revision_id
        or new.worker_pay_basis is distinct from decode(new.request_body->>'worker_pay_basis','hex')
        or new.control_cut is distinct from (new.request_body->>'control_cut')::bigint
        or new.warning_context is distinct from new.request_body->'warning_context'
        or new.successful_count<>0 or new.failed_count<>0 or new.next_page_no<>1 or new.next_after_line_no is distinct from 0
        or new.aggregate_result is not null or not exists(select 1 from public.tms_users u where u.id=new.actor_id and u.is_active and u.role::text='admin') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_MANY_FROZEN_SCOPE_INVALID';end if;
    else
      if (to_jsonb(new)-array['aggregate_result','successful_count','failed_count','next_page_no','next_after_line_no'])
        is distinct from (to_jsonb(old)-array['aggregate_result','successful_count','failed_count','next_page_no','next_after_line_no'])
        or old.aggregate_result is not null and new.aggregate_result is distinct from old.aggregate_result
        or new.successful_count-old.successful_count not between 0 and 1 or new.failed_count-old.failed_count not between -1 and 1
        or new.next_page_no-old.next_page_no not between 0 and 1
        or (new.next_page_no=old.next_page_no and new.next_after_line_no is distinct from old.next_after_line_no)
        or (new.next_page_no>old.next_page_no and not exists(select 1 from private.bpay_next_snooze_many_page p
          where p.request_id=new.request_id and p.page_no=old.next_page_no)) then
        raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_IMMUTABLE';end if;
    end if;
  else
    select r.* into strict v_parent from private.bpay_next_snooze_many_request r where r.request_id=new.request_id;
    if tg_table_name='bpay_next_snooze_many_page' then
      if tg_op<>'INSERT' or new.page_no<>v_parent.next_page_no
        or new.request_body-array['page_no','after_line_no','retry_line_nos'] is distinct from v_parent.request_body
        or new.result_body->>'request_id' is distinct from new.request_id::text
        or new.result_body->>'candidate_id' is distinct from v_parent.candidate_id::text
        or new.result_body->>'page_no' is distinct from new.page_no::text
        or new.result_body->'received' is distinct from 'true'::jsonb or new.result_body->'replay' is distinct from 'false'::jsonb then
        raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_IMMUTABLE';end if;
    elsif tg_table_name='bpay_next_snooze_many_line' then
      if tg_op='INSERT' then
        if new.successful or new.last_result is not null or not exists(select 1 from private.bpay_next_approved_line a
          where a.id=new.approved_line_id and a.revision_id=v_parent.revision_id and a.line_no=new.line_no
            and a.component_kind in ('EXPENSE','MILEAGE') and a.component_key=new.component_key
            and a.source_pay_ex_vat::text=new.amount and private.bpay_next_line_pay_basis_v1(a.id)=new.worker_pay_basis) then
          raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_MANY_FROZEN_SCOPE_INVALID';end if;
      elsif old.successful or (to_jsonb(new)-array['successful','last_result']) is distinct from (to_jsonb(old)-array['successful','last_result'])
        or new.last_result is null or new.last_result->>'line_no' is distinct from new.line_no::text
        or new.last_result->>'approved_line_id' is distinct from new.approved_line_id::text
        or new.last_result->>'component_key' is distinct from new.component_key
        or new.last_result->>'label' is distinct from new.label or new.last_result->>'amount' is distinct from new.amount
        or new.last_result->'successful' is distinct from to_jsonb(new.successful) then
        raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_IMMUTABLE';end if;
    else raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_MANY_RECEIPT_IMMUTABLE';end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_snooze_many_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_many_guard_v1() from public,anon,authenticated,service_role;

create or replace trigger bpay_next_snooze_many_request_guard before insert or update or delete on private.bpay_next_snooze_many_request
  for each row execute function private.bpay_next_snooze_many_guard_v1();
create or replace trigger bpay_next_snooze_many_request_truncate_guard before truncate on private.bpay_next_snooze_many_request
  for each statement execute function private.bpay_next_snooze_many_guard_v1();
create or replace trigger bpay_next_snooze_many_line_guard before insert or update or delete on private.bpay_next_snooze_many_line
  for each row execute function private.bpay_next_snooze_many_guard_v1();
create or replace trigger bpay_next_snooze_many_line_truncate_guard before truncate on private.bpay_next_snooze_many_line
  for each statement execute function private.bpay_next_snooze_many_guard_v1();
create or replace trigger bpay_next_snooze_many_page_guard before insert or update or delete on private.bpay_next_snooze_many_page
  for each row execute function private.bpay_next_snooze_many_guard_v1();
create or replace trigger bpay_next_snooze_many_page_truncate_guard before truncate on private.bpay_next_snooze_many_page
  for each statement execute function private.bpay_next_snooze_many_guard_v1();

commit;
