-- WB-36 private W2 snooze owner. WB-19 attaches the single public dispatcher.
-- CHECK is read-only; ACK_ISSUE, SET and CLEAR own exact metadata receipts.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_snooze_context_v1(p_actor_id uuid,p_args jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
  v_candidate uuid; v_work_id uuid; v_revision_id uuid; v_scope text; v_key text;
  v_case_id uuid; v_case_component_id uuid; v_kind text; v_identity text; v_basis bytea;
  v_revision private.bpay_next_work_revision%rowtype; v_work private.bpay_next_work%rowtype;
  v_segment private.bpay_next_approved_segment%rowtype; v_line private.bpay_next_approved_line%rowtype;
  v_case private.bpay_next_case_component%rowtype; v_snooze private.bpay_next_snooze%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_module_epoch bigint; v_until date; v_date_context jsonb; v_warning text; v_phase text;
  v_conflict text; v_business_date date; v_indefinite boolean; v_note text; v_financial_revision text;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_id is null or not exists(select 1 from public.tms_users u
      where u.id=p_actor_id and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_FORBIDDEN';
  end if;
  select m.owner_epoch into v_module_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE'; end if;
  if p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>4096
    or exists(select 1 from jsonb_object_keys(p_args) k where k not in ('request_id','candidate_id','work_id',
      'expected_revision_id','scope_kind','segment_key','component_key','case_id','case_component_id','snooze_kind',
      'until_date','indefinite','note','expected_version','worker_pay_basis','acknowledgement_token','phase','expected_case_revision')) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_REQUEST_INVALID';
  end if;
  v_candidate:=(p_args->>'candidate_id')::uuid; v_work_id:=nullif(p_args->>'work_id','')::uuid;
  v_revision_id:=nullif(p_args->>'expected_revision_id','')::uuid;
  v_case_id:=nullif(p_args->>'case_id','')::uuid; v_case_component_id:=nullif(p_args->>'case_component_id','')::uuid;
  v_scope:=p_args->>'scope_kind'; v_key:=case v_scope when 'SEGMENT' then p_args->>'segment_key'
    when 'EXPENSE_LINE' then p_args->>'component_key' end;
  if (v_scope<>'SEGMENT' and p_args ? 'segment_key')
    or (v_scope<>'EXPENSE_LINE' and p_args ? 'component_key')
    or (v_scope<>'CASE' and (p_args ? 'case_id' or p_args ? 'case_component_id' or p_args ? 'expected_case_revision'))
    or (v_scope='CASE' and (p_args ? 'work_id' or p_args ? 'expected_revision_id')) then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_FOREIGN_SCOPE_FIELD';
  end if;
  v_kind:=case upper(btrim(p_args->>'snooze_kind')) when 'BLOCKED' then 'BLOCKED_TIMESHEET'
    when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT' else upper(btrim(p_args->>'snooze_kind')) end;
  v_phase:=coalesce(p_args->>'phase','PRE_SAVE'); v_note:=coalesce(p_args->>'note','');
  v_until:=nullif(p_args->>'until_date','')::date;
  if v_scope is null or v_scope not in ('WHOLE_TIMESHEET','SEGMENT','EXPENSE_LINE','EXPENSES_ALL','CASE')
    or v_kind is null or v_kind not in ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT',
      'OVERPAYMENT_RECOVERY','PAYMENT_ADVANCE_REPAYMENT','MANUAL_DEBT_RECOVERY')
    or v_phase not in ('PRE_OPEN','PRE_SAVE') or v_candidate is null or length(v_note)>500
    or jsonb_typeof(p_args->'indefinite') is distinct from 'boolean'
    or (p_args->>'indefinite')::boolean is distinct from (v_until is null)
    or (p_args ? 'until_date' and jsonb_typeof(p_args->'until_date') not in ('string','null'))
    or (v_until is not null and p_args->>'until_date' !~ '^\d{4}-\d{2}-\d{2}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_SCOPE_OR_VALUE_INVALID';
  end if;
  if v_scope='CASE' then
    select x.* into v_case from private.bpay_next_case_component x
      where x.id=v_case_component_id and x.case_id=v_case_id and x.candidate_id=v_candidate;
    if not found or v_work_id is not null or v_revision_id is not null
      or not (case v_kind when 'OVERPAYMENT_RECOVERY' then v_case.case_kind='OVERPAYMENT'
        when 'PAYMENT_ADVANCE_REPAYMENT' then v_case.case_kind in ('LOAN','ADVANCE')
        when 'MANUAL_DEBT_RECOVERY' then v_case.case_kind='MANUAL_DEBT' else false end)
      or v_case.instruction_kind<>'RECOVERY' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CASE_AUTHORITY_MISMATCH';
    end if;
    v_identity:=v_case.id::text; v_financial_revision:=v_case.component_revision::text;
    select r.* into strict v_rule from private.bpay_next_case_rule r
      where r.id=v_case.rule_id and r.case_id=v_case.case_id and r.candidate_id=v_candidate;
    v_basis:=pg_catalog.sha256(pg_catalog.convert_to(jsonb_build_array(v_candidate,v_case.case_id,v_case.id,
      v_case.case_kind,v_case.case_subtype,v_case.tax_treatment,v_case.instruction_kind,v_case.direction,
      v_case.payroll_stage,v_case.source_pay_channel,v_case.approved_source_ex_vat,v_case.funded_source_ex_vat,
      v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,v_rule.weekly_due_source_ex_vat,
      v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)::text,'UTF8'));
  else
    select w.* into v_work from private.bpay_next_work w where w.id=v_work_id and w.candidate_id=v_candidate;
    if not found or v_case_id is not null or v_case_component_id is not null or v_kind not in
      ('DO_NOT_PAY','BLOCKED_TIMESHEET','TIMESHEET_PAYMENT') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WORK_AUTHORITY_MISMATCH';
    end if;
    select r.* into v_revision from private.bpay_next_work_revision r where r.id=v_work.current_revision_id
      and r.work_id=v_work.id and r.sealed_at_utc is not null;
    if not found or v_revision_id is distinct from v_revision.id or v_work.approval_state<>'APPROVED' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_STALE';
    end if;
    v_financial_revision:=v_revision.revision_no::text;
    v_identity:=v_work.id::text;
    if v_scope='SEGMENT' then
      select s.* into v_segment from private.bpay_next_approved_segment s
        where s.revision_id=v_revision.id and s.segment_key=v_key;
      if not found then
        if exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=v_revision.id) then
          raise exception using errcode='23514',message='SNOOZE_SEGMENT_STALE';
        end if;
        raise exception using errcode='23514',message='SNOOZE_SEGMENT_SCOPE_UNSUPPORTED';
      end if;
      v_identity:=v_identity||':'||v_key; v_basis:=private.bpay_next_segment_pay_basis_v1(v_segment.id);
    elsif v_scope='EXPENSE_LINE' then
      select a.* into v_line from private.bpay_next_approved_line a
        where a.revision_id=v_revision.id and a.component_key=v_key and a.component_kind in ('EXPENSE','MILEAGE');
      if not found or v_kind<>'DO_NOT_PAY' then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EXPENSE_IDENTITY_INVALID';
      end if;
      v_identity:=v_identity||':'||v_key; v_basis:=private.bpay_next_line_pay_basis_v1(v_line.id);
    else
      if v_scope='EXPENSES_ALL' and (v_kind<>'DO_NOT_PAY' or not exists(select 1 from private.bpay_next_approved_line a
          where a.revision_id=v_revision.id and a.component_kind in ('EXPENSE','MILEAGE'))) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EXPENSE_SCOPE_UNSUPPORTED';
      end if;
      select case when v_scope='EXPENSES_ALL' then b.expense_basis else b.whole_basis end into v_basis
        from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id;
    end if;
    if v_scope='WHOLE_TIMESHEET' and exists(select 1 from private.bpay_next_snooze s
      where s.work_id=v_work.id and s.active and s.scope_kind='SEGMENT'
        and (s.until_date is null or s.until_date>=(current_timestamp at time zone 'Europe/London')::date)
        and exists(select 1 from private.bpay_next_approved_segment a where a.revision_id=v_revision.id
          and a.segment_key=s.segment_key and private.bpay_next_segment_pay_basis_v1(a.id)=s.worker_pay_basis)) then
      v_conflict:='SEGMENT_SNOOZES_ALREADY_EXIST';
    elsif v_scope='SEGMENT' and exists(select 1 from private.bpay_next_snooze s
      join private.bpay_next_revision_worker_basis b on b.revision_id=v_revision.id and b.whole_basis=s.worker_pay_basis
      where s.work_id=v_work.id and s.active and s.scope_kind='WHOLE_TIMESHEET'
        and (s.until_date is null or s.until_date>=(current_timestamp at time zone 'Europe/London')::date)) then
      v_conflict:='WHOLE_TIMESHEET_ALREADY_SNOOZED';
    end if;
  end if;
  if v_basis is null then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WORKER_BASIS_UNAVAILABLE'; end if;
  select s.* into v_snooze from private.bpay_next_snooze s where s.candidate_id=v_candidate
    and s.scope_kind=v_scope and s.scope_identity=v_identity and s.active;
  v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
  v_business_date:=(v_date_context->>'business_date')::date;
  if v_until<v_business_date then raise exception using errcode='22023',message='SNOOZE_DATE_IN_PAST'; end if;
  -- WB-45 adds its genuine resolved-rate context here at its chain turn.
  if v_phase='PRE_SAVE' and v_until is not null then
    v_warning:=case when v_until<(v_date_context->>'next_official_pay_date')::date
      then 'SNOOZE_DATE_BEFORE_NEXT_PAY_RUN' else 'SNOOZE_INCLUDES_NEXT_PAY_RUN' end;
  end if;
  v_indefinite:=exists(select 1 from private.bpay_next_snooze s where s.candidate_id=v_candidate and s.active
    and s.until_date is null and ((s.scope_kind=v_scope and s.scope_identity=v_identity and s.worker_pay_basis=v_basis)
      or (v_work_id is not null and s.work_id=v_work_id and ((s.scope_kind='WHOLE_TIMESHEET'
        and s.worker_pay_basis=(select b.whole_basis from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id))
        or (v_scope='EXPENSE_LINE' and s.scope_kind='EXPENSES_ALL'
          and s.worker_pay_basis=(select b.expense_basis from private.bpay_next_revision_worker_basis b where b.revision_id=v_revision.id))))));
  return jsonb_build_object('candidate_id',v_candidate,'work_id',v_work_id,'expected_revision_id',v_revision_id,
    'physical_timesheet_id',v_revision.physical_timesheet_id,'scope_kind',v_scope,'scope_identity',v_identity,
    'segment_key',v_segment.segment_key,'approved_segment_id',v_segment.id,
    'component_key',coalesce(v_segment.component_key,v_line.component_key),'expense_approved_line_id',v_line.id,
    'case_id',v_case.case_id,'case_component_id',v_case.id,'case_kind',v_case.case_kind,'snooze_kind',v_kind,
    'worker_pay_basis',encode(v_basis,'hex'),'snooze_id',v_snooze.id,'expected_version',coalesce(v_snooze.version,0),
    'financial_revision',v_financial_revision,'stale_control',v_snooze.id is not null and v_snooze.worker_pay_basis<>v_basis,
    'module_epoch',v_module_epoch,'conflict',v_conflict,'indefinite_wins',v_indefinite,'until_date',v_until,'note',v_note,
    'warning_context',jsonb_build_object('phase',v_phase,'until_date',v_until,'business_date',v_business_date,
      'next_official_pay_date',v_date_context->'next_official_pay_date',
      'configuration_fingerprint',v_date_context->'configuration_fingerprint','warning_code',v_warning,
      'warning_required',v_warning is not null));
end
$function$;
alter function private.bpay_next_snooze_context_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_context_v1(uuid,jsonb) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_snooze_action_v1(p_actor_id uuid,p_action text,p_args jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare
  v_request uuid; v_receipt private.bpay_next_snooze_request%rowtype;
  v_command private.bpay_next_command%rowtype; v_snooze private.bpay_next_snooze%rowtype;
  v_ack private.bpay_next_snooze_warning_ack%rowtype;
  v_sequence bigint;
  v_context jsonb; v_result jsonb; v_candidate uuid; v_work uuid; v_before jsonb; v_event text;
  v_expected bigint; v_token uuid; v_now timestamptz; v_epoch bigint;
begin
  if p_action in ('SNOOZE_CHECK','SNOOZE_CONTEXT') then
    return private.bpay_next_snooze_context_v1(p_actor_id,p_args);
  end if;
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_id is null or not exists(select 1 from public.tms_users u
      where u.id=p_actor_id and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('SNOOZE_WARNING_ACK_ISSUE','SNOOZE_SET','SNOOZE_CLEAR')
    or p_args is null or jsonb_typeof(p_args) is distinct from 'object' or octet_length(p_args::text)>4096 then
    raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_ACTION_INVALID';
  end if;
  v_request:=(p_args->>'request_id')::uuid;
  -- Exact replay is read before token consumption or any new control write.
  select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  if found then
    if (v_receipt.actor_id,v_receipt.action,v_receipt.request_body) is distinct from (p_actor_id,p_action,p_args) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';
    end if;
    return v_receipt.result_body;
  end if;
  if p_action='SNOOZE_CLEAR' then
    if exists(select 1 from jsonb_object_keys(p_args) k where k not in ('request_id','snooze_id','candidate_id','expected_version')) then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_CLEAR_ARGS_INVALID';
    end if;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=(p_args->>'snooze_id')::uuid;
    v_candidate:=v_snooze.candidate_id; v_work:=v_snooze.work_id;
    if v_candidate is distinct from (p_args->>'candidate_id')::uuid then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CANDIDATE_MISMATCH';
    end if;
  else
    v_candidate:=(p_args->>'candidate_id')::uuid; v_work:=nullif(p_args->>'work_id','')::uuid;
  end if;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  if v_work is not null then perform 1 from private.bpay_next_work w where w.id=v_work and w.candidate_id=v_candidate for update; end if;
  if p_action<>'SNOOZE_CLEAR' and p_args->>'scope_kind'='CASE' then
    perform 1 from private.bpay_next_case_component x where x.id=(p_args->>'case_component_id')::uuid
      and x.candidate_id=v_candidate for update;
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(v_request,p_action);
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request and c.agency_sequence=v_sequence;
  -- A concurrent identical retry may have completed while this call waited
  -- on the Candidate lock. Recheck its original body before any token write.
  select r.* into v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  if found then
    if (v_receipt.actor_id,v_receipt.action,v_receipt.request_body) is distinct from (p_actor_id,p_action,p_args) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_REPLAY_MISMATCH';
    end if;
    return v_receipt.result_body;
  end if;
  v_expected:=(p_args->>'expected_version')::bigint;
  if v_expected is null or v_expected<0 then raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_VERSION_REQUIRED'; end if;
  if p_action='SNOOZE_CLEAR' then
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=(p_args->>'snooze_id')::uuid for update;
    if not v_snooze.active or v_snooze.version<>v_expected then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_VERSION_STALE';
    end if;
    v_before:=to_jsonb(v_snooze);
    update private.bpay_next_snooze s set active=false,version=s.version+1,control_sequence=v_command.agency_sequence,
      updated_at_utc=clock_timestamp(),updated_by=p_actor_id,last_request_id=v_request
      where s.id=v_snooze.id returning s.* into v_snooze;
    v_event:='CLEARED';
    -- CLEAR remains available on stale financial facts. This pure point read
    -- only reports valid remaining dominance; it does not readmit the old scope.
    select jsonb_build_object('indefinite_wins',exists(select 1 from private.bpay_next_snooze s
      join private.bpay_next_work w on w.id=s.work_id
      join private.bpay_next_revision_worker_basis b on b.revision_id=w.current_revision_id
      left join private.bpay_next_approved_segment seg on seg.revision_id=w.current_revision_id and seg.segment_key=s.segment_key
      left join private.bpay_next_approved_line line on line.revision_id=w.current_revision_id and line.component_key=s.component_key
      where s.work_id=v_snooze.work_id and s.active and s.until_date is null and
        ((s.scope_kind=v_snooze.scope_kind and s.scope_identity=v_snooze.scope_identity
          and s.worker_pay_basis=case s.scope_kind when 'WHOLE_TIMESHEET' then b.whole_basis when 'EXPENSES_ALL' then b.expense_basis
            when 'SEGMENT' then private.bpay_next_segment_pay_basis_v1(seg.id) when 'EXPENSE_LINE' then private.bpay_next_line_pay_basis_v1(line.id) end)
          or (s.scope_kind='WHOLE_TIMESHEET' and s.worker_pay_basis=b.whole_basis)
          or (s.scope_kind='EXPENSES_ALL' and v_snooze.scope_kind='EXPENSE_LINE' and s.worker_pay_basis=b.expense_basis)))) into v_context;
  else
    -- CASE authority is locked separately; no due/pause/schedule is rewritten.
    if p_args->>'scope_kind'='CASE' then
      perform 1 from private.bpay_next_case_component x where x.id=(p_args->>'case_component_id')::uuid
        and x.candidate_id=v_candidate for update;
    end if;
    v_context:=private.bpay_next_snooze_context_v1(p_actor_id,p_args);
    if p_action='SNOOZE_SET' and v_context#>>'{warning_context,phase}'<>'PRE_SAVE' then
      raise exception using errcode='22023',message='BPAY_NEXT_SNOOZE_SET_REQUIRES_PRE_SAVE';
    end if;
    if v_context->>'conflict' is not null then raise exception using errcode='23514',message=v_context->>'conflict'; end if;
    if v_expected is distinct from (v_context->>'expected_version')::bigint
      or p_args->>'worker_pay_basis' is distinct from v_context->>'worker_pay_basis'
      or (p_args->>'scope_kind'='CASE' and p_args->>'expected_case_revision' is distinct from v_context->>'financial_revision') then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_FINANCIAL_VERSION_STALE';
    end if;
    if p_action='SNOOZE_WARNING_ACK_ISSUE' then
      v_now:=clock_timestamp();
      insert into private.bpay_next_snooze_warning_ack(request_id,actor_id,candidate_id,module_epoch,
        scope_kind,scope_identity,expected_revision_id,expected_version,worker_pay_basis,warning_context,issued_at_utc,expires_at_utc)
      values(v_request,p_actor_id,v_candidate,v_command.module_epoch,v_context->>'scope_kind',v_context->>'scope_identity',
        nullif(v_context->>'expected_revision_id','')::uuid,v_expected,decode(v_context->>'worker_pay_basis','hex'),
        v_context->'warning_context',v_now,v_now+interval '30 minutes') returning token into v_token;
      v_result:=v_context||jsonb_build_object('acknowledgement_token',v_token,'acknowledgement_expires_at_utc',v_now+interval '30 minutes');
    else
      if coalesce((v_context#>>'{warning_context,warning_required}')::boolean,false) or p_args ? 'acknowledgement_token' then
        select a.* into v_ack from private.bpay_next_snooze_warning_ack a
          where a.token=nullif(p_args->>'acknowledgement_token','')::uuid for update;
        if not found or v_ack.consumed_by_request_id is not null or v_ack.expires_at_utc<=clock_timestamp()
          or (v_ack.actor_id,v_ack.candidate_id,v_ack.module_epoch,v_ack.scope_kind,v_ack.scope_identity,
              v_ack.expected_revision_id,v_ack.expected_version,v_ack.worker_pay_basis,v_ack.warning_context)
            is distinct from (p_actor_id,v_candidate,v_command.module_epoch,v_context->>'scope_kind',v_context->>'scope_identity',
              nullif(v_context->>'expected_revision_id','')::uuid,v_expected,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context') then
          raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_WARNING_ACK_REQUIRED_OR_STALE';
        end if;
        update private.bpay_next_snooze_warning_ack set consumed_by_request_id=v_request,consumed_at_utc=clock_timestamp() where token=v_ack.token;
      end if;
      select s.* into v_snooze from private.bpay_next_snooze s where s.id=nullif(v_context->>'snooze_id','')::uuid for update;
      if found then
        v_before:=to_jsonb(v_snooze); v_event:='AMENDED';
        update private.bpay_next_snooze s set expected_revision_id=nullif(v_context->>'expected_revision_id','')::uuid,
          physical_timesheet_id=nullif(v_context->>'physical_timesheet_id','')::uuid,
          approved_segment_id=nullif(v_context->>'approved_segment_id','')::uuid,
          expense_approved_line_id=nullif(v_context->>'expense_approved_line_id','')::uuid,
          component_key=v_context->>'component_key',snooze_kind=v_context->>'snooze_kind',
          worker_pay_basis=decode(v_context->>'worker_pay_basis','hex'),until_date=nullif(v_context->>'until_date','')::date,
          note=v_context->>'note',version=s.version+1,control_sequence=v_command.agency_sequence,
          updated_at_utc=clock_timestamp(),updated_by=p_actor_id,last_request_id=v_request
          where s.id=v_snooze.id returning s.* into v_snooze;
      else
        v_event:='SET';
        insert into private.bpay_next_snooze(candidate_id,work_id,expected_revision_id,physical_timesheet_id,scope_kind,scope_identity,
          component_key,segment_key,approved_segment_id,expense_approved_line_id,case_id,case_component_id,case_kind,
          snooze_kind,worker_pay_basis,until_date,note,control_sequence,created_by,updated_by,last_request_id)
        values(v_candidate,v_work,nullif(v_context->>'expected_revision_id','')::uuid,nullif(v_context->>'physical_timesheet_id','')::uuid,
          v_context->>'scope_kind',v_context->>'scope_identity',v_context->>'component_key',v_context->>'segment_key',
          nullif(v_context->>'approved_segment_id','')::uuid,nullif(v_context->>'expense_approved_line_id','')::uuid,
          nullif(v_context->>'case_id','')::uuid,nullif(v_context->>'case_component_id','')::uuid,v_context->>'case_kind',
          v_context->>'snooze_kind',decode(v_context->>'worker_pay_basis','hex'),nullif(v_context->>'until_date','')::date,
          v_context->>'note',v_command.agency_sequence,p_actor_id,p_actor_id,v_request) returning * into v_snooze;
      end if;
    end if;
  end if;
  if v_event is not null then
    insert into private.bpay_next_snooze_event(snooze_id,candidate_id,event_kind,version,control_sequence,actor_id,request_id,reason,before_state,after_state)
      values(v_snooze.id,v_candidate,v_event,v_snooze.version,v_command.agency_sequence,p_actor_id,v_request,v_event,v_before,to_jsonb(v_snooze));
    if p_action='SNOOZE_SET' then v_context:=private.bpay_next_snooze_context_v1(p_actor_id,p_args); end if;
    v_result:=jsonb_build_object('snooze_id',v_snooze.id,'version',v_snooze.version,'active',v_snooze.active,
      'control_sequence',v_command.agency_sequence,'indefinite_wins',coalesce((v_context->>'indefinite_wins')::boolean,false));
  end if;
  insert into private.bpay_next_snooze_request(request_id,actor_id,action,request_body,result_body)
    values(v_request,p_actor_id,p_action,p_args,v_result);
  update private.bpay_next_command set status='COMPLETE',expected_member_count=0,sealed_at_utc=clock_timestamp() where id=v_request;
  return v_result;
end
$function$;
alter function private.bpay_next_snooze_action_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_action_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;

-- Append-only receipts/audit. ACK token can transition once, with its binding immutable.
create or replace function private.bpay_next_snooze_metadata_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_command private.bpay_next_command%rowtype;v_snooze private.bpay_next_snooze%rowtype;
  v_previous private.bpay_next_snooze_event%rowtype;v_context jsonb;v_args jsonb;v_request uuid;v_actor uuid;
  v_ack private.bpay_next_snooze_warning_ack%rowtype;
begin
  if tg_op in ('DELETE','TRUNCATE') then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;v_actor:=new.updated_by;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    v_request:=case when tg_op='UPDATE' then new.consumed_by_request_id else new.request_id end;v_actor:=new.actor_id;
  else v_request:=new.request_id;v_actor:=new.actor_id;end if;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  if v_command.status<>'RECEIVED' or not exists(select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch)
    or not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active and u.role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_METADATA_OWNER_INVALID';
  end if;
  if tg_table_name='bpay_next_snooze' then
    if new.control_sequence<>v_command.agency_sequence or v_command.command_kind not in ('SNOOZE_SET','SNOOZE_CLEAR')
      or (tg_op='INSERT' and (new.version<>1 or not new.active or new.created_by<>new.updated_by or v_command.command_kind<>'SNOOZE_SET'))
      or (tg_op='UPDATE' and (not old.active or new.version<>old.version+1 or new.control_sequence<=old.control_sequence
        or (new.id,new.candidate_id,new.work_id,new.scope_kind,new.scope_identity,new.segment_key,new.case_id,new.case_component_id,
          new.created_at_utc,new.created_by) is distinct from (old.id,old.candidate_id,old.work_id,old.scope_kind,old.scope_identity,
          old.segment_key,old.case_id,old.case_component_id,old.created_at_utc,old.created_by))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_VERSION_INVALID';
    end if;
    if not new.active then
      if tg_op<>'UPDATE' or v_command.command_kind<>'SNOOZE_CLEAR'
        or (to_jsonb(new)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id'])
          is distinct from (to_jsonb(old)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id']) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CLEAR_FINANCIAL_TUPLE_CHANGED';
      end if;
      return new;
    end if;
    if v_command.command_kind<>'SNOOZE_SET' then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_KIND_INVALID';end if;
    v_args:=jsonb_strip_nulls(jsonb_build_object('candidate_id',new.candidate_id,'work_id',new.work_id,
      'expected_revision_id',new.expected_revision_id,'scope_kind',new.scope_kind,'segment_key',new.segment_key,
      'component_key',case when new.scope_kind='EXPENSE_LINE' then new.component_key end,'case_id',new.case_id,
      'case_component_id',new.case_component_id,'snooze_kind',new.snooze_kind,'until_date',new.until_date,
      'indefinite',new.until_date is null,'note',new.note));
    v_context:=private.bpay_next_snooze_context_v1(new.updated_by,v_args);
    if (new.scope_identity,new.expected_revision_id,new.physical_timesheet_id,new.approved_segment_id,
        new.expense_approved_line_id,new.component_key,new.case_kind,new.worker_pay_basis)
      is distinct from (v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
        (v_context->>'physical_timesheet_id')::uuid,(v_context->>'approved_segment_id')::uuid,
        (v_context->>'expense_approved_line_id')::uuid,v_context->>'component_key',v_context->>'case_kind',
        decode(v_context->>'worker_pay_basis','hex')) or v_context->>'conflict' is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_event' then
    if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=new.snooze_id;
    select e.* into v_previous from private.bpay_next_snooze_event e where e.snooze_id=new.snooze_id and e.version=new.version-1;
    if (new.candidate_id,new.version,new.control_sequence,new.actor_id,new.request_id,new.after_state)
      is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,v_snooze.updated_by,
        v_snooze.last_request_id,to_jsonb(v_snooze)) or new.control_sequence<>v_command.agency_sequence
      or (new.version=1 and (new.event_kind<>'SET' or new.before_state is not null or not v_snooze.active))
      or (new.version>1 and (v_previous.id is null or new.before_state is distinct from v_previous.after_state
        or new.event_kind is distinct from (case when v_snooze.active then 'AMENDED'::text else 'CLEARED'::text end)))
      or v_command.command_kind is distinct from (case when new.event_kind='CLEARED' then 'SNOOZE_CLEAR'::text else 'SNOOZE_SET'::text end) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_command.command_kind<>'SNOOZE_WARNING_ACK_ISSUE' or new.module_epoch<>v_command.module_epoch
        or new.consumed_by_request_id is not null or jsonb_typeof(new.warning_context)<>'object' then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUE_INVALID';
      end if;
    elsif tg_op<>'UPDATE' or old.consumed_by_request_id is not null or new.consumed_by_request_id is null
      or new.consumed_at_utc is null or new.consumed_at_utc>=old.expires_at_utc or new.consumed_at_utc<old.issued_at_utc
      or v_command.command_kind<>'SNOOZE_SET' or v_command.module_epoch<>old.module_epoch
      or (to_jsonb(new)-array['consumed_by_request_id','consumed_at_utc'])
        is distinct from (to_jsonb(old)-array['consumed_by_request_id','consumed_at_utc']) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMPTION_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_request' and tg_op='INSERT' then
    if new.action<>v_command.command_kind or (new.request_body->>'request_id')::uuid is distinct from new.request_id
      or jsonb_typeof(new.request_body)<>'object' or octet_length(new.request_body::text)>4096
      or jsonb_typeof(new.result_body)<>'object' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_COMMAND_INVALID';
    end if;
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      select a.* into strict v_ack from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id;
      v_context:=private.bpay_next_snooze_context_v1(new.actor_id,new.request_body);
      if (v_ack.actor_id,v_ack.candidate_id,v_ack.module_epoch,v_ack.scope_kind,v_ack.scope_identity,
          v_ack.expected_revision_id,v_ack.expected_version,v_ack.worker_pay_basis,v_ack.warning_context)
        is distinct from (new.actor_id,(v_context->>'candidate_id')::uuid,(v_context->>'module_epoch')::bigint,
          v_context->>'scope_kind',v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
          (v_context->>'expected_version')::bigint,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUED_SCOPE_MISMATCH';
      end if;
    end if;
    return new;
  end if;
  raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';
end
$function$;alter function private.bpay_next_snooze_metadata_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_metadata_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_snooze_guard on private.bpay_next_snooze;
create trigger bpay_next_snooze_guard before insert or update or delete on private.bpay_next_snooze for each row execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_truncate_guard on private.bpay_next_snooze;
create trigger bpay_next_snooze_truncate_guard before truncate on private.bpay_next_snooze for each statement execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_event_guard on private.bpay_next_snooze_event;
create trigger bpay_next_snooze_event_guard before insert or update or delete on private.bpay_next_snooze_event for each row execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_event_truncate_guard on private.bpay_next_snooze_event;
create trigger bpay_next_snooze_event_truncate_guard before truncate on private.bpay_next_snooze_event for each statement execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_ack_guard on private.bpay_next_snooze_warning_ack;
create trigger bpay_next_snooze_ack_guard before insert or update or delete on private.bpay_next_snooze_warning_ack for each row execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_ack_truncate_guard on private.bpay_next_snooze_warning_ack;
create trigger bpay_next_snooze_ack_truncate_guard before truncate on private.bpay_next_snooze_warning_ack for each statement execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_request_guard on private.bpay_next_snooze_request;
create trigger bpay_next_snooze_request_guard before insert or update or delete on private.bpay_next_snooze_request for each row execute function private.bpay_next_snooze_metadata_guard_v1();
drop trigger if exists bpay_next_snooze_request_truncate_guard on private.bpay_next_snooze_request;
create trigger bpay_next_snooze_request_truncate_guard before truncate on private.bpay_next_snooze_request for each statement execute function private.bpay_next_snooze_metadata_guard_v1();

-- Resolve only exact applicable metadata at the Create click's ordered cut.
-- Later wake/amend/clear events cannot change an already captured offer.
create or replace function private.bpay_next_snooze_at_cut_v1(
  p_work_id uuid,p_revision_id uuid,p_segment_key text,p_component_key text,p_sequence bigint,p_business_date date)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
  select e.after_state from private.bpay_next_snooze s
  join lateral(select ev.after_state from private.bpay_next_snooze_event ev
    where ev.snooze_id=s.id and ev.control_sequence<=p_sequence order by ev.control_sequence desc limit 1) e on true
  join private.bpay_next_approved_line a on a.revision_id=p_revision_id and a.component_key=p_component_key
  left join private.bpay_next_approved_segment seg on seg.revision_id=p_revision_id and seg.segment_key=p_segment_key and seg.component_key=a.component_key
  join private.bpay_next_revision_worker_basis b on b.revision_id=p_revision_id
  where s.work_id=p_work_id and (e.after_state->>'active')::boolean
    and ((e.after_state->>'until_date') is null or (e.after_state->>'until_date')::date>=p_business_date)
    and ((s.scope_kind='WHOLE_TIMESHEET' and (e.after_state->>'worker_pay_basis')::bytea=b.whole_basis)
      or (s.scope_kind='SEGMENT' and s.segment_key=p_segment_key
        and (e.after_state->>'worker_pay_basis')::bytea=private.bpay_next_segment_pay_basis_v1(seg.id))
      or (s.scope_kind='EXPENSES_ALL' and a.component_kind in ('EXPENSE','MILEAGE')
        and (e.after_state->>'worker_pay_basis')::bytea=b.expense_basis)
      or (s.scope_kind='EXPENSE_LINE' and s.component_key=p_component_key
        and (e.after_state->>'worker_pay_basis')::bytea=private.bpay_next_line_pay_basis_v1(a.id)))
  order by (e.after_state->>'until_date') is null desc,s.scope_kind,s.id limit 1
$function$;
alter function private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date) owner to postgres;
revoke all on function private.bpay_next_snooze_at_cut_v1(uuid,uuid,text,text,bigint,date) from public,anon,authenticated,service_role;

-- Commands become COMPLETE only after the owner wrote its exact receipt.
-- Deferred checks bind the immutable children without reordering that owner.
create or replace function private.bpay_next_snooze_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request uuid;v_receipt private.bpay_next_snooze_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_context jsonb;v_ack private.bpay_next_snooze_warning_ack%rowtype;
begin
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;
  elsif tg_table_name='bpay_next_snooze_warning_ack' and tg_op='UPDATE' then v_request:=new.consumed_by_request_id;
  else v_request:=new.request_id;end if;
  select r.* into strict v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  if v_command.status<>'COMPLETE' or v_command.expected_member_count is distinct from 0 or v_command.sealed_at_utc is null
    or v_command.command_kind<>v_receipt.action then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_COMMAND_RECEIPT_INCOMPLETE';
  end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event') then
    if not exists(select 1 from private.bpay_next_snooze_event e
      where e.snooze_id=case when tg_table_name='bpay_next_snooze' then (to_jsonb(new)->>'id')::uuid else (to_jsonb(new)->>'snooze_id')::uuid end
        and e.version=new.version and e.request_id=v_request and e.actor_id=v_receipt.actor_id
        and e.candidate_id=(v_receipt.request_body->>'candidate_id')::uuid
        and (v_receipt.result_body->>'snooze_id')::uuid=e.snooze_id
        and (v_receipt.result_body->>'version')::bigint=e.version
        and (v_receipt.result_body->>'control_sequence')::bigint=e.control_sequence
        and (v_receipt.request_body->>'expected_version')::bigint=e.version-1
        and (v_receipt.action<>'SNOOZE_CLEAR' or (v_receipt.request_body->>'snooze_id')::uuid=e.snooze_id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_RECEIPT_MISMATCH';
    end if;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_receipt.action<>'SNOOZE_WARNING_ACK_ISSUE' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.result_body->>'acknowledgement_token')::uuid is distinct from new.token
        or (v_receipt.result_body->>'acknowledgement_expires_at_utc')::timestamptz is distinct from new.expires_at_utc
        or (v_receipt.result_body->>'scope_kind',v_receipt.result_body->>'scope_identity',
          (v_receipt.result_body->>'expected_revision_id')::uuid,(v_receipt.result_body->>'expected_version')::bigint,
          decode(v_receipt.result_body->>'worker_pay_basis','hex'),v_receipt.result_body->'warning_context')
          is distinct from (new.scope_kind,new.scope_identity,new.expected_revision_id,new.expected_version,new.worker_pay_basis,new.warning_context) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISMATCH';
      end if;
    else
      if v_receipt.action<>'SNOOZE_SET' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.request_body->>'acknowledgement_token')::uuid is distinct from new.token
        or v_receipt.request_body->>'scope_kind' is distinct from new.scope_kind
        or (v_receipt.request_body->>'expected_revision_id')::uuid is distinct from new.expected_revision_id
        or (v_receipt.request_body->>'expected_version')::bigint is distinct from new.expected_version
        or decode(v_receipt.request_body->>'worker_pay_basis','hex') is distinct from new.worker_pay_basis
        or not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=v_request
          and e.actor_id=new.actor_id and e.after_state->>'scope_identity'=new.scope_identity
          and (e.after_state->>'candidate_id')::uuid=new.candidate_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMED_RECEIPT_MISMATCH';
      end if;
    end if;
  elsif tg_table_name='bpay_next_snooze_request' then
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      if not exists(select 1 from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id
        and a.actor_id=new.actor_id and a.token=(new.result_body->>'acknowledgement_token')::uuid) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISSING';
      end if;
    elsif not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=new.request_id
      and e.actor_id=new.actor_id and e.candidate_id=(new.request_body->>'candidate_id')::uuid
      and e.snooze_id=(new.result_body->>'snooze_id')::uuid and e.version=(new.result_body->>'version')::bigint
      and e.control_sequence=(new.result_body->>'control_sequence')::bigint
      and (e.after_state->>'active')::boolean is not distinct from (new.result_body->>'active')::boolean
      and (new.request_body->>'expected_version')::bigint=e.version-1
      and ((new.action='SNOOZE_CLEAR' and e.event_kind='CLEARED' and e.snooze_id=(new.request_body->>'snooze_id')::uuid)
        or (new.action='SNOOZE_SET' and e.event_kind in ('SET','AMENDED')
          and (e.after_state->>'work_id')::uuid is not distinct from (new.request_body->>'work_id')::uuid
          and (e.after_state->>'expected_revision_id')::uuid is not distinct from (new.request_body->>'expected_revision_id')::uuid
          and e.after_state->>'scope_kind'=new.request_body->>'scope_kind'
          and e.after_state->>'segment_key' is not distinct from new.request_body->>'segment_key'
          and (new.request_body->>'scope_kind'<>'EXPENSE_LINE' or e.after_state->>'component_key'=new.request_body->>'component_key')
          and (e.after_state->>'case_id')::uuid is not distinct from (new.request_body->>'case_id')::uuid
          and (e.after_state->>'case_component_id')::uuid is not distinct from (new.request_body->>'case_component_id')::uuid
          and e.after_state->>'snooze_kind'=case upper(btrim(new.request_body->>'snooze_kind'))
            when 'BLOCKED' then 'BLOCKED_TIMESHEET' when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT'
            else upper(btrim(new.request_body->>'snooze_kind')) end
          and (e.after_state->>'worker_pay_basis')::bytea=decode(new.request_body->>'worker_pay_basis','hex')
          and (e.after_state->>'until_date')::date is not distinct from (new.request_body->>'until_date')::date
          and e.after_state->>'note'=coalesce(new.request_body->>'note','')))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_EVENT_MISMATCH';
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_snooze_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_receipt_guard_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_snooze_receipt_guard on private.bpay_next_snooze;
create constraint trigger bpay_next_snooze_receipt_guard after insert or update on private.bpay_next_snooze
  deferrable initially deferred for each row execute function private.bpay_next_snooze_receipt_guard_v1();
drop trigger if exists bpay_next_snooze_event_receipt_guard on private.bpay_next_snooze_event;
create constraint trigger bpay_next_snooze_event_receipt_guard after insert on private.bpay_next_snooze_event
  deferrable initially deferred for each row execute function private.bpay_next_snooze_receipt_guard_v1();
drop trigger if exists bpay_next_snooze_ack_receipt_guard on private.bpay_next_snooze_warning_ack;
create constraint trigger bpay_next_snooze_ack_receipt_guard after insert or update on private.bpay_next_snooze_warning_ack
  deferrable initially deferred for each row execute function private.bpay_next_snooze_receipt_guard_v1();
drop trigger if exists bpay_next_snooze_request_receipt_guard on private.bpay_next_snooze_request;
create constraint trigger bpay_next_snooze_request_receipt_guard after insert on private.bpay_next_snooze_request
  deferrable initially deferred for each row execute function private.bpay_next_snooze_receipt_guard_v1();

commit;
