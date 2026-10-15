set local jit=off;
set local pg_show_plans.is_enabled=off;
do $vacancy$ begin if to_regclass('private.timesheet_correction_a0_transaction_fences') is not null or exists(select 1 from unnest(array['private.timesheet_correction_a0_source_token_assert_v1(jsonb)','private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean)','private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean)','private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean)','private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)']) id where to_regprocedure(id) is not null) then raise exception 'HR_A0_KERNEL_NATIVE_PREREQUISITE_NOT_VACANT';end if;end $vacancy$;
-- SOURCE successor only. One physical arbiter; no business authority store.
-- No frozen predecessor was rewritten or assumed installed.
do $native_fence_domains$
declare r record;v_count integer:=0;
begin
  if current_setting('block_size')::integer<>8192 or not exists(
    select 1 from pg_catalog.pg_extension e join pg_catalog.pg_namespace n on n.oid=e.extnamespace
      where e.extname='btree_gist' and n.nspname='public') then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_PROFILE_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation
    and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_JSONB_DEDUP_EQUALITY_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_bool_ops'
      and c.opcintype='boolean'::regtype and a.amname='gist') then
    raise exception 'CORRECTION_A0_SOURCE_COVERAGE_OPCLASS_UNPROVED' using errcode='55000';end if;
  for r in select a.atttypid,c.collisdeterministic from pg_catalog.pg_attribute a
    join pg_catalog.pg_class t on t.oid=a.attrelid join pg_catalog.pg_namespace n on n.oid=t.relnamespace
    join pg_catalog.pg_collation c on c.oid=a.attcollation
    where n.nspname='public' and a.attnum>0 and not a.attisdropped and (t.relname,a.attname) in(values
      ('timesheets','booking_id'),('invoice_lines','booking_id'),('timesheets','occupant_key_norm'),('candidates','key_norm'),
      ('weekly_source_row_timesheet_lineages','family_booking_id'))
  loop
    v_count:=v_count+1;
    if r.atttypid<>'text'::regtype or r.collisdeterministic is distinct from true then
      raise exception 'CORRECTION_A0_NATIVE_TEXT_EQUALITY_UNPROVED' using errcode='55000';end if;
  end loop;
  if v_count<>5 or not exists(select 1 from pg_catalog.pg_attribute a where a.attrelid='public.candidates'::regclass
    and a.attname='nhsp_hr_name_aliases' and not a.attisdropped and a.atttypid='jsonb'::regtype)
    or to_regclass('private.timesheet_correction_a0_transaction_fences') is not null then
    raise exception 'CORRECTION_A0_NATIVE_TOKEN_SCHEMA_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_uuid_ops'
    and c.opcintype='uuid'::regtype and a.amname='gist') or not exists(
    select 1 from pg_catalog.pg_opclass c join pg_catalog.pg_namespace n on n.oid=c.opcnamespace
    join pg_catalog.pg_am a on a.oid=c.opcmethod where n.nspname='public' and c.opcname='gist_text_ops'
    and c.opcintype='text'::regtype and a.amname='gist') then
    raise exception 'CORRECTION_A0_NATIVE_EXCLUSION_OPCLASS_UNPROVED' using errcode='55000';end if;
end;
$native_fence_domains$;
create table private.timesheet_correction_a0_transaction_fences(
  token_kind text collate "C" not null,
  uuid_identity uuid,
  text_identity text collate "C",
  retained boolean not null,
  intent_span numrange not null,
  coverage_span daterange,
  touch_bit boolean not null default false,
  constraint timesheet_correction_a0_fences_identity_shape check(
    (coverage_span is null and (
      (token_kind in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
        and uuid_identity is not null and text_identity is null)
      or(token_kind in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT',
        'SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') and uuid_identity is null
        and text_identity is not null and octet_length(text_identity)<=1536)))
    or(token_kind in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') and uuid_identity is not null and text_identity is null
      and coverage_span is not null and not isempty(coverage_span))),
  constraint timesheet_correction_a0_fences_span_shape check(
    not isempty(intent_span) and ((retained and lower_inf(intent_span) and upper_inf(intent_span))
      or(not retained and not lower_inf(intent_span) and not upper_inf(intent_span)
        and lower_inc(intent_span) and not upper_inc(intent_span) and lower(intent_span)>=0
        and trunc(lower(intent_span))=lower(intent_span) and upper(intent_span)=lower(intent_span)+1))),
  constraint timesheet_correction_a0_fences_uuid_excl exclude using gist(
    token_kind public.gist_text_ops with =,uuid_identity public.gist_uuid_ops with =,intent_span with &&)
    where(uuid_identity is not null and coverage_span is null),
  constraint timesheet_correction_a0_fences_text_excl exclude using gist(
    token_kind public.gist_text_ops with =,text_identity public.gist_text_ops with =,intent_span with &&)
    where(text_identity is not null),
  constraint timesheet_correction_a0_fences_coverage_excl exclude using gist(
    token_kind public.gist_text_ops with =,uuid_identity public.gist_uuid_ops with =,
    coverage_span with &&,intent_span with &&,retained public.gist_bool_ops with <>)
    where(coverage_span is not null)
);
-- Retained scopes stay exact. Different overlapping scopes are not merged or
-- client-wide serialized; an ordinary range conflicts with either real scope.
create unique index timesheet_correction_a0_fences_coverage_retained_unique
  on private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,coverage_span)
  where(retained and coverage_span is not null);
alter table private.timesheet_correction_a0_transaction_fences OWNER TO CURRENT_USER;
alter table private.timesheet_correction_a0_transaction_fences enable row level security;
create policy cloudtms_miget_service_owner_all on private.timesheet_correction_a0_transaction_fences
  for all to CURRENT_USER,service_role using(true) with check(true);
revoke all on table private.timesheet_correction_a0_transaction_fences from public,anon,authenticated,service_role;
-- Ordinary spans use only the actual top-level XID and are immediately deleted.
-- Correction span is unbounded. No ordinary store, retirement timer or history bump.

-- Closed grammar for actual Source dependencies in the SAME proposed A0 arbiter.
-- No caller SQL, fake family identity or monetary permission is introduced.
create or replace function private.timesheet_correction_a0_source_token_assert_v1(p_token jsonb)
returns void language plpgsql immutable security definer set search_path='pg_catalog','pg_temp'
as $source_token$
declare kind text:=p_token->>'kind';identity jsonb; i integer; expected jsonb; span daterange;
begin
 if kind not in('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') then
  if p_token ? 'coverage' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if kind in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') then
  if jsonb_typeof(p_token->'coverage') is distinct from 'array' or jsonb_array_length(p_token->'coverage')<>2
   or exists(select 1 from jsonb_array_elements(p_token->'coverage') x where jsonb_typeof(x) not in('string','null')
    or(jsonb_typeof(x)='string' and pg_input_is_valid(x#>>'{}','date') is distinct from true)) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  span:=daterange((p_token->'coverage'->>0)::date,(p_token->'coverage'->>1)::date,'[]');
  if isempty(span) then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 if p_token ? 'coverage' or jsonb_typeof(p_token->'json') is distinct from 'string' then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_GLOBAL_SETTINGS' then
  if p_token->>'json' is distinct from 'true' then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  return;
 end if;
 begin identity:=(p_token->>'json')::jsonb;
 exception when invalid_text_representation then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end;
 if jsonb_typeof(identity) is distinct from 'array' or jsonb_array_length(identity)<>case when kind='SOURCE_RESOLUTION_SLOT' then 3 else 2 end then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if kind='SOURCE_RESOLUTION_SLOT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or pg_input_is_valid(identity->>0,'uuid') is distinct from true
   or jsonb_typeof(identity->1) is distinct from 'number' or pg_input_is_valid(identity->>1,'bigint') is distinct from true
   or jsonb_typeof(identity->2) is distinct from 'string' or pg_input_is_valid(identity->>2,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::bigint,(identity->>2)::uuid);
 elsif kind='SOURCE_OBJECT' then
  if jsonb_typeof(identity->0) is distinct from 'string' or identity->>0 not in('public.weekly_source_groups','public.weekly_source_cycles','public.weekly_source_uploads','public.weekly_source_upload_rows','public.weekly_source_projection_publications','public.weekly_source_row_resolutions','public.weekly_source_row_economic_snapshots','public.weekly_source_row_expense_policy_snapshots','public.weekly_expense_authority_generations','public.weekly_source_expense_cell_evidence','public.weekly_source_charge_checks','public.weekly_source_charge_acceptances','public.weekly_source_invoice_hold_releases','public.weekly_source_state_transitions','public.weekly_source_report_scopes','public.weekly_source_self_bill_reports','public.weekly_source_self_bill_report_line_bindings','public.weekly_work_events','public.weekly_source_final_revisions','public.weekly_source_final_snapshot_lines','public.weekly_source_row_timesheet_lineages','public.weekly_source_ordinary_pay_projection_receipts','public.weekly_discrepancy_incidents','public.weekly_issue_comparison_revisions','public.weekly_source_billing_movements','public.weekly_source_group_clients','public.weekly_source_client_policies','public.weekly_source_contract_policies','public.weekly_source_group_rule_assignments','public.weekly_source_rule_set_versions','public.weekly_source_rule_sets','public.weekly_source_format_profiles','public.clients','public.client_settings','public.contracts','public.timesheets','public.timesheets_financials')
   or jsonb_typeof(identity->1) is distinct from 'string' or pg_input_is_valid(identity->>1,'uuid') is distinct from true then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  expected:=jsonb_build_array(identity->>0,(identity->>1)::uuid);
 else
  for i in 0..1 loop
   if jsonb_typeof(identity->i) is distinct from 'string' or pg_input_is_valid(identity->>i,'uuid') is distinct from true then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  end loop;
  expected:=jsonb_build_array((identity->>0)::uuid,(identity->>1)::uuid);
 end if;
 if p_token->>'json' is distinct from expected::text then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
end;
$source_token$;
alter function private.timesheet_correction_a0_source_token_assert_v1(jsonb) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_token_assert_v1(jsonb) from public,anon,authenticated,service_role;

create or replace function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens jsonb,p_retain boolean)
returns void language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare t jsonb;k text;u uuid;s text;v_inserted integer;v_count integer;
  v_timeout text:=current_setting('lock_timeout');v_xid numeric;v_span numrange;
begin
  if p_retain is null or jsonb_typeof(p_tokens) is distinct from 'array' or exists(
    select 1 from jsonb_array_elements(p_tokens) x(value) where jsonb_typeof(x.value) is distinct from 'object'
      or x.value->>'kind' is null or x.value->>'kind' not in('FAMILY_UUID','PHYSICAL_UUID','BOOKING','KEY_NORM',
        'ALIAS_JSON_STRING','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID',
        'SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')
      or exists(select 1 from jsonb_object_keys(x.value) n(key) where n.key not in('kind','uuid','json'))
      or(x.value->>'kind' in('FAMILY_UUID','PHYSICAL_UUID','WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID')
        and(pg_input_is_valid(x.value->>'uuid','uuid') is distinct from true or x.value ? 'json'))
      or(x.value->>'kind' in('BOOKING','KEY_NORM','ALIAS_JSON_STRING','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')
        and(jsonb_typeof(x.value->'json') is distinct from 'string' or x.value ? 'uuid'))) then
    raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  if p_retain and exists(select 1 from jsonb_array_elements(p_tokens) x(value)
    where x.value ? 'json' and octet_length(x.value->'json'#>>'{}')>1536) then
    raise exception 'CORRECTION_A0_TOKEN_IDENTITY_UNPROVED' using errcode='55000';end if;
  if jsonb_array_length(p_tokens)=0 then return;end if;
  if p_retain then v_span:=numrange(null,null,'()');else
    -- xid8 is converted through exact decimal text, never narrowed to bigint.
    -- Native API returns the top-level XID even inside a subtransaction.
    v_xid:=pg_current_xact_id()::text::numeric;v_span:=numrange(v_xid,v_xid+1,'[)');
  end if;
  begin
    -- One call orders PHYSICAL identities before parent/dependency identities.
    -- Owner callers still supply Candidate/I1 -> existing family -> dependency
    -- -> genuinely allocated phases, with the full native CAS after retention.
    for t in select d.value from(select distinct x.value from jsonb_array_elements(p_tokens) x(value)
      where not(x.value ? 'json') or octet_length(x.value->'json'#>>'{}')<=1536) d
      order by case when d.value->>'kind' in('WORKFLOW_UUID','CONTRACT_WEEK_UUID','SOURCE_CYCLE_UUID','SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT') then 1 else 0 end,d.value
    loop
      k:=t->>'kind';u:=case when t ? 'uuid' then(t->>'uuid')::uuid end;
      s:=case when t ? 'json' then t->'json'#>>'{}' end;
      perform set_config('lock_timeout','1ms',true);
      if p_retain then
        perform 1 from private.timesheet_correction_a0_transaction_fences f
          where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
            or(s is not null and f.text_identity=s collate "C")) for update nowait;
        if found then
          update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
            where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
              or(s is not null and f.text_identity=s collate "C"));
          get diagnostics v_count=row_count;
          if v_count<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
        else
          v_inserted:=null;
          if u is not null then
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,u,null,true,v_span,true) on conflict on constraint timesheet_correction_a0_fences_uuid_excl
              do nothing returning 1 into v_inserted;
          else
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,null,s,true,v_span,true) on conflict on constraint timesheet_correction_a0_fences_text_excl
              do nothing returning 1 into v_inserted;
          end if;
          if v_inserted is distinct from 1 then
            perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
              and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for update nowait;
            if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
            update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
              where f.retained and f.token_kind=k and((u is not null and f.uuid_identity=u)
                or(s is not null and f.text_identity=s collate "C"));
            get diagnostics v_count=row_count;
            if v_count<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
          end if;
        end if;
      else
        perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
          and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for share nowait;
        if not found then
          v_inserted:=null;
          if u is not null then
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,u,null,false,v_span,false) on conflict on constraint timesheet_correction_a0_fences_uuid_excl
              do nothing returning 1 into v_inserted;
          else
            insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,text_identity,retained,intent_span,touch_bit)
              values(k,null,s,false,v_span,false) on conflict on constraint timesheet_correction_a0_fences_text_excl
              do nothing returning 1 into v_inserted;
          end if;
          if v_inserted=1 then
            -- Delete only this top-level transaction's own newly inserted span.
            -- Other ordinary spans are disjoint. This never deletes a retained token.
            delete from private.timesheet_correction_a0_transaction_fences f where not f.retained and f.intent_span=v_span
              and f.token_kind=k and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C"));
          else
            perform 1 from private.timesheet_correction_a0_transaction_fences f where f.retained and f.token_kind=k
              and((u is not null and f.uuid_identity=u) or(s is not null and f.text_identity=s collate "C")) for share nowait;
            if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
          end if;
        end if;
      end if;
    end loop;
    perform set_config('lock_timeout',v_timeout,true);
  exception when others then perform set_config('lock_timeout',v_timeout,true);raise;end;
end;
$function$;
alter function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean) from public,anon,authenticated,service_role;
-- The same arbiter, native date overlap and opposite intent classes.
-- Engine contention/dirty visibility/old-RR and whole caller order remain UNRUN.
create or replace function private.timesheet_correction_a0_source_coverage_acquire_core_v1(p_kind text,p_client uuid,p_coverage daterange,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $coverage$
declare old_timeout text:=current_setting('lock_timeout');span numrange;xid numeric;inserted integer;changed integer;
begin
 if p_kind is null or p_kind not in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') or p_client is null or p_coverage is null or isempty(p_coverage) or p_retain is null then
  raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 if p_retain then span:=numrange(null,null,'()');else
  xid:=pg_current_xact_id()::text::numeric;span:=numrange(xid,xid+1,'[)');end if;
 begin
  perform set_config('lock_timeout','1ms',true);
  if p_retain then
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
      and f.coverage_span=p_coverage for update nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values(p_kind,p_client,true,span,p_coverage,true)
     on conflict(token_kind,uuid_identity,coverage_span) where retained and coverage_span is not null
      do nothing returning 1 into inserted;
    if inserted is distinct from 1 then
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span=p_coverage for update nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
   if inserted is distinct from 1 then
    update private.timesheet_correction_a0_transaction_fences f set touch_bit=not f.touch_bit
     where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client and f.coverage_span=p_coverage;
    get diagnostics changed=row_count;
    if changed<>1 then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
   end if;
  else
   perform 1 from private.timesheet_correction_a0_transaction_fences f
    where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
     and f.coverage_span && p_coverage for share nowait;
   if not found then
    insert into private.timesheet_correction_a0_transaction_fences(token_kind,uuid_identity,retained,intent_span,coverage_span,touch_bit)
     values(p_kind,p_client,false,span,p_coverage,false)
     on conflict on constraint timesheet_correction_a0_fences_coverage_excl do nothing returning 1 into inserted;
    if inserted=1 then
     delete from private.timesheet_correction_a0_transaction_fences f
      where not f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span=p_coverage and f.intent_span=span;
    else
     perform 1 from private.timesheet_correction_a0_transaction_fences f
      where f.retained and f.token_kind=p_kind and f.uuid_identity=p_client
       and f.coverage_span && p_coverage for share nowait;
     if not found then raise exception 'CORRECTION_A0_TOKEN_CHANGED' using errcode='40001';end if;
    end if;
   end if;
  end if;
  perform set_config('lock_timeout',old_timeout,true);
 exception when others then perform set_config('lock_timeout',old_timeout,true);raise;end;
end;
$coverage$;
alter function private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean) from public,anon,authenticated,service_role;

-- Preserve the original Contract-coverage helper ABI and its exact domain.
create or replace function private.timesheet_correction_a0_source_coverage_acquire_v1(p_client uuid,p_coverage daterange,p_retain boolean)
returns void language sql volatile security definer set search_path='pg_catalog','pg_temp'
as $coverage_contract$
 select private.timesheet_correction_a0_source_coverage_acquire_core_v1('SOURCE_CONTRACT_COVERAGE',p_client,p_coverage,p_retain);
$coverage_contract$;
alter function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean) from public,anon,authenticated,service_role;

-- Preserve the exact original public-to-private caller signature. No Source
-- token means one unchanged native scalar call, preserving its old validation.
create or replace function private.timesheet_correction_a0_fence_acquire_v1(p_tokens jsonb,p_retain boolean)
returns void language plpgsql volatile security definer set search_path='pg_catalog','pg_temp'
as $source_acquire$
declare t jsonb;scalar_tokens jsonb;
begin
 if jsonb_typeof(p_tokens) is distinct from 'array' then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if not exists(select 1 from jsonb_array_elements(p_tokens) x where x->>'kind' in
  ('SOURCE_OBJECT','SOURCE_GROUP_CLIENT','SOURCE_CYCLE_CLIENT','SOURCE_GLOBAL_SETTINGS','SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE','SOURCE_REVISION_CLIENT','SOURCE_REVISION_EVENT','SOURCE_RESOLUTION_SLOT','SOURCE_LINEAGE_RESOLUTION_CLIENT','SOURCE_LINEAGE_EVENT_CLIENT','SOURCE_GROUP_REPORT_REVISION','SOURCE_GROUP_REVISION_EVENT')) then
  perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(p_tokens,p_retain);return;end if;
 if p_retain is null then raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 -- Validate the entire new envelope BEFORE an acquisition. Neither a malformed
 -- coverage token nor mixed native token may partially admit an owner.
 for t in select value from jsonb_array_elements(p_tokens) loop
  if jsonb_typeof(t) is distinct from 'object' or exists(select 1 from jsonb_object_keys(t) key
    where key not in('kind','uuid','json','coverage')) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
  perform private.timesheet_correction_a0_source_token_assert_v1(t);
  if t->>'kind' in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') and (t ? 'json' or jsonb_typeof(t->'uuid') is distinct from 'string'
    or pg_input_is_valid(t->>'uuid','uuid') is distinct from true) then
   raise exception 'CORRECTION_A0_TOKEN_SHAPE_INVALID' using errcode='55000';end if;
 end loop;
 select coalesce(jsonb_agg(value order by value),'[]'::jsonb) into scalar_tokens
  from jsonb_array_elements(p_tokens) where coalesce(value->>'kind','') not in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE');
 perform private.timesheet_correction_a0_fence_scalar_acquire_core_v1(scalar_tokens,p_retain);
 for t in select distinct value from jsonb_array_elements(p_tokens)
   where value->>'kind' in('SOURCE_CONTRACT_COVERAGE','SOURCE_POLICY_MEMBERSHIP_COVERAGE') order by value loop
  perform private.timesheet_correction_a0_source_coverage_acquire_core_v1(t->>'kind',(t->>'uuid')::uuid,
   daterange((t->'coverage'->>0)::date,(t->'coverage'->>1)::date,'[]'),p_retain);
 end loop;
end;
$source_acquire$;
alter function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) OWNER TO CURRENT_USER;
revoke all on function private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean) from public,anon,authenticated,service_role;


set local lock_timeout='1379ms';
do $kernel$
declare c uuid:='62d7638d-5bbb-4549-b4dc-082d3b1e3276';other_c uuid:='23e2a7ba-ad08-4c1c-aa81-784ff23a6310';n integer;caught boolean;before_touch jsonb;t jsonb;
begin
 -- Identical retained ranges touch exactly one row; overlap is not unioned.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>1 then raise exception 'HR_A0_KERNEL_RETAINED_DEDUP_FAILED';end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-03','2026-09-09'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>2 then raise exception 'HR_A0_KERNEL_RETAINED_RANGES_MERGED';end if;
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>3 then raise exception 'HR_A0_KERNEL_RANGE_DOMAIN_COLLISION';end if;
 select jsonb_agg(to_jsonb(f) order by f.token_kind,f.coverage_span) into before_touch from private.timesheet_correction_a0_transaction_fences f;
 -- Ordinary overlap acquires existing rows without rewriting retained scopes.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-04','2026-09-05'))),false);
 if (select jsonb_agg(to_jsonb(f) order by f.token_kind,f.coverage_span) from private.timesheet_correction_a0_transaction_fences f) is distinct from before_touch then raise exception 'HR_A0_KERNEL_ORDINARY_CHANGED_RETAINED';end if;
 -- Disjoint native date or Client leaves no ordinary intent row behind.
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-10-01','2026-10-07'))),false);
 perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_CONTRACT_COVERAGE','uuid',other_c,'coverage',jsonb_build_array('2026-09-01','2026-09-07'))),false);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>3 or exists(select 1 from private.timesheet_correction_a0_transaction_fences where not retained) then raise exception 'HR_A0_KERNEL_ORDINARY_INTENT_LEAK';end if;
 -- Scalar dispatch uses the exact existing acquisition core in the same table.
 t:=jsonb_build_array(jsonb_build_object('kind','SOURCE_GLOBAL_SETTINGS','json','true'),jsonb_build_object('kind','SOURCE_OBJECT','json',jsonb_build_array('public.contracts',c)::text));
 perform private.timesheet_correction_a0_fence_acquire_v1(t,true);perform private.timesheet_correction_a0_fence_acquire_v1(t,true);
 if (select count(*) from private.timesheet_correction_a0_transaction_fences)<>5 then raise exception 'HR_A0_KERNEL_SCALAR_DISPATCH_FAILED';end if;
 -- Invalid mixed envelopes must refuse before any earlier token is admitted.
 caught:=false;begin
  perform private.timesheet_correction_a0_fence_acquire_v1(jsonb_build_array(jsonb_build_object('kind','SOURCE_OBJECT','json',jsonb_build_array('public.contracts',other_c)::text),jsonb_build_object('kind','SOURCE_POLICY_MEMBERSHIP_COVERAGE','uuid',c,'coverage',jsonb_build_array('2026-09-01'))),true);
 exception when sqlstate '55000' then caught:=true;end;
 if not caught or (select count(*) from private.timesheet_correction_a0_transaction_fences)<>5 then raise exception 'HR_A0_KERNEL_PARTIAL_INVALID_ADMISSION';end if;
 caught:=false;begin perform private.timesheet_correction_a0_source_coverage_acquire_core_v1('FOREIGN_KIND',c,daterange('2026-09-01','2026-09-07','[]'),true);exception when sqlstate '55000' then caught:=true;end;
 if not caught then raise exception 'HR_A0_KERNEL_KIND_WHITELIST_FAILED';end if;
 if current_setting('lock_timeout')<>'1379ms' then raise exception 'HR_A0_KERNEL_TIMEOUT_NOT_RESTORED';end if;
 if exists(select 1 from unnest(array['private.timesheet_correction_a0_source_token_assert_v1(jsonb)','private.timesheet_correction_a0_fence_scalar_acquire_core_v1(jsonb,boolean)','private.timesheet_correction_a0_source_coverage_acquire_core_v1(text,uuid,daterange,boolean)','private.timesheet_correction_a0_source_coverage_acquire_v1(uuid,daterange,boolean)','private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)']) id cross join unnest(array['anon','authenticated','service_role']) r where has_function_privilege(r,id,'EXECUTE')) then raise exception 'HR_A0_KERNEL_PRIVATE_ACL_FAILED';end if;
end $kernel$;
select current_database() as physical_database,true as actual_scalar_coverage_kernel_assertions_passed,
 (select count(*) from private.timesheet_correction_a0_transaction_fences) as retained_native_intents;
