-- Repeatable CloudTMS function/view authority: banking_pay_next_initial_tsfin_daily_evidence_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- Owner-only metadata qualification. No caller money, position, hold or effect write.
-- The existing I-1 and immutable Banking revision remain the financial authorities.
create or replace function private.bpay_next_initial_tsfin_certificate_v1(p_root_authorisation_id uuid)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  a public.weekly_source_root_authorisations%rowtype;
  t public.timesheets%rowtype;f public.timesheets_financials%rowtype;
  w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  p private.bpay_next_publication%rowtype;c public.contracts%rowtype;
  inventory jsonb;basis jsonb;origin jsonb;details jsonb;n integer;epoch bigint;
begin
  if p_root_authorisation_id is null then
    raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  select m.owner_epoch into epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if epoch is null then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select x.* into strict a from public.weekly_source_root_authorisations x where x.id=p_root_authorisation_id;
  select x.* into strict t from public.timesheets x where x.timesheet_id=a.root_timesheet_id;
  select x.* into strict c from public.contracts x where x.id=t.contract_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.source_event_id=a.id;
  select x.* into strict w from private.bpay_next_work x where x.id=r.work_id;
  select x.* into strict f from public.timesheets_financials x where x.id=r.financial_snapshot_id;
  select x.* into strict p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(a.id);
  if a.withdrawn_at_utc is not null or a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(t.booking_id))
     or t.is_current is distinct from true or t.authorised_at_server is null
     or t.revoked_at is not null or t.archived_at_utc is not null
     or (a.family_booking_id,a.timesheet_version) is distinct from (t.booking_id,t.version)
     or (w.work_kind,w.candidate_id,w.contract_id,w.original_timesheet_id,w.booking_id,w.week_ending_date,w.current_revision_id)
       is distinct from ('SOURCE'::text,c.candidate_id,c.id,t.timesheet_id,t.booking_id,t.week_ending_date,r.id)
     or w.approval_state is distinct from 'APPROVED'
     or (r.source_kind,r.source_head_id,r.source_event_id,r.physical_timesheet_id,r.physical_timesheet_version,r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,a.id,t.timesheet_id,t.version,t.week_ending_date)
     or r.approved_at_utc is null or r.sealed_at_utc is null
     or f.is_current is distinct from true or f.is_stale is distinct from false or f.authorised_at_utc is null
     or (f.timesheet_id,f.timesheet_version,f.candidate_id,f.client_id)
       is distinct from (t.timesheet_id,t.version,w.candidate_id,c.client_id)
     or (p.work_id,p.revision_id,p.candidate_id,p.revision_no)
       is distinct from (w.id,r.id,w.candidate_id,r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=w.candidate_id
       where command.id=p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and p.status='QUEUED' and p.phase='NEW' and p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception 'BPAY_NEXT_INITIAL_AUTH_NOT_CURRENT' using errcode='55000';end if;
  inventory:=private.weekly_source_effective_inventory_v1(t.timesheet_id);
  basis:=inventory->'approval_basis';origin:=basis->'origin';
  details:=jsonb_build_object('segments',f.invoice_breakdown_json->'segments','additional_units',f.additional_units_json,
    'actual_schedule',f.actual_schedule_json,'rate_source_refs',f.rate_source_refs_json);
  if inventory->>'ok' is distinct from 'true' or inventory->>'authority' is distinct from 'TSFIN'
     or inventory->>'head_id' is not null or jsonb_typeof(inventory->'components') is distinct from 'array'
     or jsonb_typeof(basis) is distinct from 'object' or basis->'coverage_complete' is distinct from 'true'::jsonb
     or basis is distinct from private.weekly_source_inventory_approval_basis_v2(t.timesheet_id,inventory)
     or origin is distinct from jsonb_build_object('kind','INITIAL_AUTHORISED_TSFIN_V1',
       'root_authorisation_id',a.id,'financial_snapshot_id',f.id,'authorisation_generation',a.authorisation_generation,
       'root_timesheet_id',t.timesheet_id,'root_version',t.version::text,'authorised_row_signature',a.authorised_row_signature,
       'financial_snapshot_digest',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',
         jsonb_build_object('policy',f.policy_snapshot_json,'details',details,'components',inventory->'components')),'hex'),
       'inventory_digest',basis->>'inventory_digest','entitlement_digest',basis->>'entitlement_digest')
     or basis->>'detail_digest' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',details),'hex')
     or basis->>'policy_fingerprint' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',f.policy_snapshot_json),'hex')
     or r.source_inventory_digest is distinct from decode(basis->>'inventory_digest','hex')
     or r.approved_source_ex_vat is distinct from (basis->>'approved_pay_ex_vat')::numeric
     or r.expected_line_count is distinct from jsonb_array_length(inventory->'components')
  then raise exception 'BPAY_NEXT_INITIAL_I1_CERTIFICATE_REQUIRED' using errcode='55000';end if;
  select count(*) into n from private.bpay_next_approved_line l where l.revision_id=r.id;
  if n<>r.expected_line_count or exists(select 1 from jsonb_array_elements(inventory->'components') component
      left join private.bpay_next_approved_line l on l.revision_id=r.id
        and l.source_component_id=(component.value->>'component_id')::uuid
        and l.component_key='SOURCE:'||(component.value->>'component_id')
      where l.id is null or l.line_no is distinct from (component.value->>'component_ordinal')::integer
        or l.evidence_ref is distinct from 'source-component:'||(component.value->>'component_id')||'#'||(component.value->>'component_sha256')
        or l.source_pay_ex_vat is distinct from case when (component.value->>'exclude_from_pay')::boolean then 0 else (component.value->>'pay_ex_vat')::numeric end
        or l.component_kind is distinct from case component.value->>'component_kind' when 'WORKED_TIME' then 'WORK' when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end
        or l.tax_treatment is distinct from case when component.value->>'component_kind'='WORKED_TIME' then 'TAXABLE'::text else null::text end)
  then raise exception 'BPAY_NEXT_INITIAL_SEALED_COMPONENTS_MISMATCH' using errcode='23514';end if;
  return jsonb_build_object('candidate_id',w.candidate_id,'work_id',w.id,'revision_id',r.id,'command_id',p.command_id,
    'source_channel',r.source_pay_channel,'origin',origin,'basis',basis,'components',inventory->'components',
    'certificate_digest',encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis),'hex'));
end
$function$;

create or replace function private.bpay_next_initial_tsfin_bind_v1(p_root_authorisation_id uuid)
returns table(binding_version_id uuid,canonical_work_id uuid,canonical_component_key text,binding_revision bigint,qualification_state text)
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_candidate uuid;v_work uuid;cert jsonb;origin jsonb;basis jsonb;component jsonb;
  l private.bpay_next_approved_line%rowtype;b private.bpay_next_source_component_binding%rowtype;
  v private.bpay_next_source_component_binding_version%rowtype;revision private.bpay_next_work_revision%rowtype;
  id_new uuid;digest bytea;n integer;
begin
  if p_root_authorisation_id is null then raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select w.candidate_id,w.id into v_candidate,v_work
    from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id
    where r.source_event_id=p_root_authorisation_id;
  if not found then raise exception 'BPAY_NEXT_INITIAL_REVISION_REQUIRED' using errcode='55000';end if;
  select r.* into strict revision from private.bpay_next_work_revision r where r.source_event_id=p_root_authorisation_id;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception 'BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED' using errcode='55000';end if;
  perform 1 from private.bpay_next_work where id=v_work and candidate_id=v_candidate for update;
  perform 1 from private.bpay_next_work_revision where id=revision.id and work_id=v_work for share;
  -- Exact stored replay precedes current I-1 discovery and NEVER advances the pointer.
  select count(*) into n from private.bpay_next_source_component_binding_version x
    where x.initial_root_authorisation_id=p_root_authorisation_id;
  if n>0 then
    if n<>revision.expected_line_count then raise exception 'BPAY_NEXT_INITIAL_PARTIAL_REPLAY' using errcode='23514';end if;
    for v in select x.* from private.bpay_next_source_component_binding_version x
      where x.initial_root_authorisation_id=p_root_authorisation_id order by x.canonical_component_key collate "C"
    loop
      if v.origin_kind<>'INITIAL_AUTHORISED_TSFIN_V1' or v.canonical_work_id<>v_work or v.approval_revision_id<>revision.id then
        raise exception 'BPAY_NEXT_INITIAL_REPLAY_CONFLICT' using errcode='23514';end if;
      select x.* into strict b from private.bpay_next_source_component_binding x
        where x.canonical_work_id=v.canonical_work_id and x.canonical_component_key=v.canonical_component_key;
      binding_version_id:=v.id;canonical_work_id:=v.canonical_work_id;canonical_component_key:=v.canonical_component_key;
      binding_revision:=v.binding_revision;qualification_state:=case when b.current_binding_version_id=v.id then 'REGISTERED' else 'SUPERSEDED' end;
      return next;
    end loop;return;
  end if;
  cert:=private.bpay_next_initial_tsfin_certificate_v1(p_root_authorisation_id);
  origin:=cert->'origin';basis:=cert->'basis';
  -- Empty genuine I-1 inventory creates no invented component/binding/position.
  for component in select value from jsonb_array_elements(cert->'components') order by value->>'component_id'
  loop
    select x.* into strict l from private.bpay_next_approved_line x
      where x.revision_id=revision.id and x.source_component_id=(component->>'component_id')::uuid;
    if exists(select 1 from private.bpay_next_source_component_binding x where x.candidate_id=v_candidate and x.source_component_id=l.source_component_id) then
      raise exception 'BPAY_NEXT_INITIAL_COMPONENT_ALREADY_BOUND' using errcode='23514';end if;
    id_new:=gen_random_uuid();
    digest:=sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_work,l.component_key,
      revision.id,l.id,p_root_authorisation_id,cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'));
    insert into private.bpay_next_source_component_binding_version
      (id,canonical_work_id,canonical_component_key,candidate_id,source_component_id,binding_revision,
       approval_work_id,approval_revision_id,approval_component_key,approved_line_id,component_state,
       approval_source_channel,tax_treatment,classification_line_id,component_sha256,
       publication_command_id_1,publication_work_id_1,publication_revision_id_1,binding_digest,
       origin_kind,initial_root_authorisation_id,initial_financial_snapshot_id,initial_root_timesheet_id,
       initial_root_version,initial_authorisation_generation,initial_authorised_row_signature,
       initial_financial_snapshot_digest,initial_inventory_digest,initial_entitlement_digest,
       initial_detail_digest,initial_policy_digest,initial_certificate_digest)
    values(id_new,v_work,l.component_key,v_candidate,l.source_component_id,1,v_work,revision.id,l.component_key,l.id,'PRESENT',
      revision.source_pay_channel,l.tax_treatment,l.id,decode(component->>'component_sha256','hex'),
      (cert->>'command_id')::uuid,v_work,revision.id,digest,'INITIAL_AUTHORISED_TSFIN_V1',p_root_authorisation_id,
      (origin->>'financial_snapshot_id')::uuid,(origin->>'root_timesheet_id')::uuid,(origin->>'root_version')::integer,
      (origin->>'authorisation_generation')::integer,origin->>'authorised_row_signature',
      decode(origin->>'financial_snapshot_digest','hex'),decode(basis->>'inventory_digest','hex'),decode(basis->>'entitlement_digest','hex'),
      decode(basis->>'detail_digest','hex'),decode(basis->>'policy_fingerprint','hex'),decode(cert->>'certificate_digest','hex'));
    insert into private.bpay_next_source_component_binding
      (canonical_work_id,canonical_component_key,candidate_id,source_component_id,canonical_revision_id,canonical_line_id,current_binding_version_id,binding_revision)
    values(v_work,l.component_key,v_candidate,l.source_component_id,revision.id,l.id,id_new,1);
    binding_version_id:=id_new;canonical_work_id:=v_work;canonical_component_key:=l.component_key;binding_revision:=1;
    qualification_state:='REGISTERED';return next;
  end loop;
end
$function$;

-- Only a current initial version calls I-1. A superseded initial version is
-- handled before this dispatch by the existing snapshot's pointer check.
create or replace function private.bpay_next_initial_tsfin_bound_assert_v1(p_binding_version_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v private.bpay_next_source_component_binding_version%rowtype;l private.bpay_next_approved_line%rowtype;
  cert jsonb;origin jsonb;basis jsonb;component jsonb;
begin
  select x.* into strict v from private.bpay_next_source_component_binding_version x where x.id=p_binding_version_id;
  if v.origin_kind is distinct from 'INITIAL_AUTHORISED_TSFIN_V1' then
    raise exception 'BPAY_NEXT_INITIAL_ORIGIN_REQUIRED' using errcode='23514';end if;
  cert:=private.bpay_next_initial_tsfin_certificate_v1(v.initial_root_authorisation_id);origin:=cert->'origin';basis:=cert->'basis';
  select value into strict component from jsonb_array_elements(cert->'components') x(value)
    where (value->>'component_id')::uuid=v.source_component_id;
  select x.* into strict l from private.bpay_next_approved_line x
    where x.revision_id=(cert->>'revision_id')::uuid and x.source_component_id=v.source_component_id;
  if (v.candidate_id,v.canonical_work_id,v.approval_work_id,v.approval_revision_id,v.publication_command_id_1,
      v.initial_financial_snapshot_id,v.initial_root_timesheet_id,v.initial_root_version,v.initial_authorisation_generation,
      v.initial_authorised_row_signature,v.initial_financial_snapshot_digest,v.initial_inventory_digest,v.initial_entitlement_digest,
      v.initial_detail_digest,v.initial_policy_digest,v.initial_certificate_digest,v.component_sha256)
    is distinct from ((cert->>'candidate_id')::uuid,(cert->>'work_id')::uuid,(cert->>'work_id')::uuid,(cert->>'revision_id')::uuid,
      (cert->>'command_id')::uuid,(origin->>'financial_snapshot_id')::uuid,(origin->>'root_timesheet_id')::uuid,
      (origin->>'root_version')::integer,(origin->>'authorisation_generation')::integer,origin->>'authorised_row_signature',
      decode(origin->>'financial_snapshot_digest','hex'),decode(basis->>'inventory_digest','hex'),decode(basis->>'entitlement_digest','hex'),
      decode(basis->>'detail_digest','hex'),decode(basis->>'policy_fingerprint','hex'),decode(cert->>'certificate_digest','hex'),
      decode(component->>'component_sha256','hex'))
     or (v.approved_line_id,v.classification_line_id,v.canonical_component_key,v.approval_component_key,
         v.approval_source_channel,v.tax_treatment,v.publication_work_id_1,v.publication_revision_id_1)
       is distinct from (l.id,l.id,l.component_key,l.component_key,cert->>'source_channel',l.tax_treatment,
         (cert->>'work_id')::uuid,(cert->>'revision_id')::uuid)
     or v.binding_digest is distinct from sha256(convert_to(jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v.canonical_work_id,
       v.canonical_component_key,v.approval_revision_id,v.approved_line_id,v.initial_root_authorisation_id,
       cert->>'certificate_digest',cert->>'command_id',component->>'component_sha256')::text,'UTF8'))
  then raise exception 'BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED' using errcode='23514';end if;
end
$function$;

alter function private.bpay_next_initial_tsfin_certificate_v1(uuid) owner to postgres;
alter function private.bpay_next_initial_tsfin_bind_v1(uuid) owner to postgres;
alter function private.bpay_next_initial_tsfin_bound_assert_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid),private.bpay_next_initial_tsfin_bind_v1(uuid),
  private.bpay_next_initial_tsfin_bound_assert_v1(uuid) from public,anon,authenticated,service_role;

-- Read and freeze actual DAILY authority; this function calculates no money.
create or replace function private.bpay_next_daily_approval_evidence_v1(p_financial_id uuid)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_tf public.timesheets_financials%rowtype;
  v_ts public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_context record;
  v_date date;
  v_rates jsonb;
  v_selected jsonb;
  v_paye jsonb;
  v_umbrella jsonb;
  v_rows jsonb;
  v_bucket record;
begin
  select tf.* into strict v_tf from public.timesheets_financials tf where tf.id=p_financial_id;
  select t.* into strict v_ts from public.timesheets t where t.timesheet_id=v_tf.timesheet_id;
  if v_ts.sheet_scope is distinct from 'DAILY'::public.timesheet_scope_enum
     or v_ts.line_type::text is distinct from 'HOURS'
     or v_ts.is_current is distinct from true or v_ts.archived_at_utc is not null or v_ts.revoked_at is not null
     or v_ts.is_adjustment is true or v_ts.parent_timesheet_id is not null or v_ts.correction_id is not null
     or nullif(btrim(v_ts.adjustment_origin),'') is not null or nullif(btrim(v_ts.correction_kind),'') is not null
     or nullif(btrim(v_ts.booking_id),'') is null
     or v_tf.is_current is distinct from true or v_tf.is_stale is distinct from false
     or v_tf.timesheet_version is distinct from v_ts.version
     or v_tf.authorised_at_utc is null or v_ts.authorised_at_server is null
     or v_tf.basis::text is distinct from 'SELF_REPORTED'
     or v_tf.candidate_id is null or v_tf.client_id is null or nullif(btrim(v_tf.role),'') is null
     or upper(v_tf.pay_method) not in ('PAYE','UMBRELLA') or v_tf.pay_method is null
     or v_tf.has_rate_issue is distinct from false or v_tf.has_pay_channel_issue is distinct from false
     or v_tf.invoice_breakdown_json->>'mode' is distinct from 'AGGREGATE'
     or jsonb_typeof(v_tf.policy_snapshot_json) is distinct from 'object'
     or jsonb_typeof(v_tf.rate_source_refs_json) is distinct from 'object'
     or v_tf.worked_start_iso is null or v_tf.worked_end_iso is null or v_tf.worked_end_iso<=v_tf.worked_start_iso
     or (v_tf.worked_start_iso,v_tf.worked_end_iso,v_tf.break_start_iso,v_tf.break_end_iso,v_tf.break_minutes)
        is distinct from (v_ts.worked_start_iso,v_ts.worked_end_iso,v_ts.break_start_iso,v_ts.break_end_iso,v_ts.break_minutes)
     or (v_tf.break_start_iso is null)<>(v_tf.break_end_iso is null)
     or (v_tf.break_start_iso is not null and v_tf.break_end_iso<=v_tf.break_start_iso)
     or v_tf.break_minutes<0
     or v_tf.additional_units_json is distinct from '{}'::jsonb
     or v_tf.additional_pay_ex_vat is distinct from 0::numeric
     or coalesce(v_tf.expenses_pay_ex_vat,0)<>0 or coalesce(v_tf.mileage_pay_ex_vat,0)<>0
     or coalesce(v_tf.mileage_units,0)<>0 or coalesce(v_tf.travel_pay_ex_vat,0)<>0
     or coalesce(v_tf.accommodation_pay_ex_vat,0)<>0 or coalesce(v_tf.other_pay_ex_vat,0)<>0 then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_APPROVAL_PROVENANCE_REQUIRED';
  end if;
  -- A real contracted DAILY retains its existing Contract identity. This
  -- does not turn a Contract edit into authorisation or manufacture a Contract
  -- for a contractless Daily. Both paths require the same authorised TF below.
  if v_ts.contract_id is not null then
    select c.* into strict v_contract from public.contracts c where c.id=v_ts.contract_id;
    if v_contract.candidate_id is distinct from v_tf.candidate_id
       or v_contract.client_id is distinct from v_tf.client_id then
      raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CONTRACT_IDENTITY_CONFLICT';
    end if;
  end if;
  -- The existing canonical context resolves the factual Candidate and Office
  -- financial Client. No latest-history lookup or Rota re-admission.
  select ctx.* into strict v_context
    from public.tsfin_load_context_batch(array[v_ts.timesheet_id]) ctx;
  if v_context.effective_timesheet_id is distinct from v_ts.timesheet_id
     or (v_context.out_cur_fin->>'id') is distinct from v_tf.id::text
     or (v_context.out_candidate->>'id') is distinct from v_tf.candidate_id::text
     or v_context.out_client_id is distinct from v_tf.client_id then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CONTEXT_IDENTITY_CONFLICT';
  end if;
  v_date:=(v_tf.worked_start_iso at time zone 'Europe/London')::date;
  if v_ts.week_ending_date is distinct from v_date+(7-extract(isodow from v_date)::integer) then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_WORK_DATE_CONFLICT';
  end if;
  -- Both channels use the SAME existing dated override/default resolver.
  -- The opposite channel remains unavailable when that authority returns NULL.
  select jsonb_object_agg(r.rate_type,to_jsonb(r)) into v_rates
    from public.tsfin_resolve_rates_batch(jsonb_build_array(
      jsonb_build_object('k',v_ts.timesheet_id,'candidate_id',v_tf.candidate_id,
        'client_id',v_tf.client_id,'role',v_tf.role,'band',v_tf.band,'date',v_date,'rate_type','PAYE'),
      jsonb_build_object('k',v_ts.timesheet_id,'candidate_id',v_tf.candidate_id,
        'client_id',v_tf.client_id,'role',v_tf.role,'band',v_tf.band,'date',v_date,'rate_type','UMBRELLA')
    )) r;
  v_selected:=v_rates->upper(v_tf.pay_method);
  if v_selected is null or coalesce(v_selected->>'source_kind','') not in ('CANDIDATE_OVERRIDE','CLIENT_DEFAULT')
     or private._candidate_daily_rate_contract_v1(v_selected) is distinct from
        private._candidate_daily_rate_contract_v1(jsonb_build_object(
          'source_kind',v_tf.rate_source_refs_json->>'kind',
          'override_id',v_tf.rate_source_refs_json->>'override_id',
          'default_id',v_tf.rate_source_refs_json->>'default_id',
          'rate_type',v_tf.rate_source_refs_json->>'rate_type',
          'pay_day',v_tf.pay_day,'pay_night',v_tf.pay_night,'pay_sat',v_tf.pay_sat,'pay_sun',v_tf.pay_sun,'pay_bh',v_tf.pay_bh,
          'charge_day',v_tf.charge_day,'charge_night',v_tf.charge_night,'charge_sat',v_tf.charge_sat,
          'charge_sun',v_tf.charge_sun,'charge_bh',v_tf.charge_bh
        )) then
    raise exception using errcode='40001',message='BPAY_NEXT_DAILY_RATE_AUTHORITY_STALE';
  end if;
  v_paye:=v_rates->'PAYE'; v_umbrella:=v_rates->'UMBRELLA';
  if (v_paye-array['k','rate_type','source_kind','override_id','pay_day','pay_night','pay_sat','pay_sun','pay_bh'])
     is distinct from
     (v_umbrella-array['k','rate_type','source_kind','override_id','pay_day','pay_night','pay_sat','pay_sun','pay_bh']) then
    raise exception using errcode='23514',message='BPAY_NEXT_DAILY_CHANNEL_CONTEXT_CONFLICT';
  end if;
  for v_bucket in select x.value from jsonb_each(v_rates) channel
    cross join lateral jsonb_each(channel.value) x
    where x.key in ('pay_day','pay_night','pay_sat','pay_sun','pay_bh',
                    'charge_day','charge_night','charge_sat','charge_sun','charge_bh')
  loop
    if v_bucket.value<>'null'::jsonb and
       ((v_bucket.value#>>'{}')::numeric<>round((v_bucket.value#>>'{}')::numeric,6)
        or (v_bucket.value#>>'{}')::numeric<0
        or abs((v_bucket.value#>>'{}')::numeric)>=1000000000000) then
      raise exception using errcode='22003',message='BPAY_NEXT_DAILY_AVAILABLE_RATE_PRECISION_INVALID';
    end if;
  end loop;
  select coalesce(jsonb_agg(jsonb_build_object('rate_family','STANDARD','rate_code',b.code,
      'unit_label',nullif(btrim(v_contract.bucket_labels_json->>lower(b.code)),''),
      'paye_rate',(v_paye->>b.pay_key)::numeric,
      'umbrella_rate',(v_umbrella->>b.pay_key)::numeric,'charge_rate',(v_paye->>b.charge_key)::numeric)
      order by b.code),'[]'::jsonb) into v_rows
    from (values ('DAY'::text,'pay_day'::text,'charge_day'::text),
      ('NIGHT','pay_night','charge_night'),('SAT','pay_sat','charge_sat'),
      ('SUN','pay_sun','charge_sun'),('BH','pay_bh','charge_bh')) b(code,pay_key,charge_key)
    where (v_paye->>b.pay_key) is not null or (v_umbrella->>b.pay_key) is not null
       or (v_paye->>b.charge_key) is not null;
  return jsonb_build_object('contract_version','BPAY_NEXT_DAILY_APPROVAL_EVIDENCE_V1',
    'timesheet_id',v_ts.timesheet_id,'timesheet_version',v_ts.version,'financial_id',v_tf.id,
    'booking_id',v_ts.booking_id,'candidate_id',v_tf.candidate_id,'client_id',v_tf.client_id,
    'work_date',v_date,'role',v_tf.role,'band',v_tf.band,
    'policy_snapshot_json',v_tf.policy_snapshot_json,'rate_source_refs_json',v_tf.rate_source_refs_json,
    'resolved_channels',v_rates,'rates',v_rows);
end
$function$;
alter function private.bpay_next_daily_approval_evidence_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_daily_approval_evidence_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
